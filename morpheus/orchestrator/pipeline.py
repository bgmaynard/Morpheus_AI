"""
Signal Pipeline - Orchestrates the full decision flow from market data to approved signals.

This is the central coordinator that wires all components together:
- Market Data Events → Feature Engineering → Regime Detection
- Regime → Strategy Selection → Signal Generation
- Signal → Scoring → MetaGate → Risk Evaluation
- Approved Signals → Decision Support Panel (for human confirmation)

OBSERVATIONAL ONLY: This pipeline does NOT execute trades.
It only emits events for UI consumption. Human confirmation is always required.

All processing is:
- Event-driven (no polling)
- Deterministic (same input → same output)
- Stateless where possible (state only for warmup data)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Awaitable

from morpheus.core.events import Event, EventType, create_event

# Features & Regime
from morpheus.features.feature_engine import FeatureEngine, FeatureContext
from morpheus.features.indicators import OHLCV
from morpheus.regime.regime_detector import (
    RegimeDetector,
    RegimeClassification,
    update_feature_context_with_regime,
)
from morpheus.regime.strategy_router import (
    StrategyRouter,
    RoutingResult,
    update_feature_context_with_strategies,
)

# Strategies
from morpheus.strategies.base import (
    Strategy,
    StrategyRunner,
    StrategyContext,
    SignalCandidate,
    SignalDirection,
)
from morpheus.strategies.momentum import get_momentum_strategies
from morpheus.strategies.mean_reversion import get_mean_reversion_strategies

# Scoring & Gate
from morpheus.scoring.base import Scorer, ScoredSignal, GateResult, GateDecision
from morpheus.scoring.stub_scorer import StubScorer
from morpheus.scoring.meta_gate import StandardMetaGate, PermissiveMetaGate

# Risk
from morpheus.risk.base import (
    AccountState,
    PositionSize,
    RiskResult,
    RiskDecision,
)
from morpheus.risk.position_sizer import StandardPositionSizer, PositionSizerConfig
from morpheus.risk.risk_manager import PermissiveRiskManager, StandardRiskManager
from morpheus.risk.kill_switch import KillSwitch, KillSwitchResult

# Data
from morpheus.data.market_snapshot import MarketSnapshot

logger = logging.getLogger(__name__)


@dataclass
class PipelineConfig:
    """
    Configuration for the signal pipeline.

    Controls component selection and behavior.
    """

    # Feature engine settings
    max_bars_history: int = 200
    min_bars_warmup: int = 50

    # Use permissive mode for testing (approves more signals)
    permissive_mode: bool = True

    # Minimum confidence to emit SIGNAL_CANDIDATE events
    min_signal_confidence: float = 0.0  # 0 = emit all signals

    # Position sizing config
    risk_per_trade_pct: float = 0.01  # 1%
    max_position_pct: float = 0.10  # 10%

    # Kill switch respected even in permissive mode
    respect_kill_switch: bool = True


@dataclass
class PipelineState:
    """
    Internal state for the pipeline.

    Tracks warmup status and recent signals.
    """

    # Symbols being tracked
    active_symbols: set[str] = field(default_factory=set)

    # Kill switch state
    kill_switch_active: bool = False

    # Recent signals per symbol (for deduplication)
    # symbol -> (timestamp, direction)
    recent_signals: dict[str, tuple[datetime, SignalDirection]] = field(default_factory=dict)

    # Signal cooldown in seconds (avoid duplicate signals)
    signal_cooldown_seconds: float = 60.0


class SignalPipeline:
    """
    Orchestrates the full signal generation pipeline.

    Wires together:
    - FeatureEngine (computes indicators from market data)
    - RegimeDetector (classifies market conditions)
    - StrategyRouter (selects appropriate strategies)
    - StrategyRunner (generates signal candidates)
    - Scorer (assigns confidence scores)
    - MetaGate (approves/rejects signals)
    - RiskManager (evaluates account-level constraints)

    Events emitted:
    - FEATURES_COMPUTED
    - REGIME_DETECTED
    - SIGNAL_CANDIDATE
    - SIGNAL_SCORED
    - META_APPROVED / META_REJECTED
    - RISK_APPROVED / RISK_VETO

    Does NOT:
    - Execute trades
    - Modify account state
    - Send orders to broker

    Human confirmation is always required for execution.
    """

    def __init__(
        self,
        config: PipelineConfig | None = None,
        emit_event: Callable[[Event], Awaitable[None]] | None = None,
    ):
        """
        Initialize the signal pipeline.

        Args:
            config: Pipeline configuration
            emit_event: Async callback to emit events (for server integration)
        """
        self.config = config or PipelineConfig()
        self._emit_event = emit_event
        self._state = PipelineState()

        # Initialize components
        self._feature_engine = FeatureEngine(
            max_bars=self.config.max_bars_history,
            min_bars=self.config.min_bars_warmup,
        )

        self._regime_detector = RegimeDetector()
        self._strategy_router = StrategyRouter()

        # Strategy runner with registered strategies
        self._strategy_runner = StrategyRunner()
        self._register_strategies()

        # Scorer
        self._scorer = StubScorer()

        # MetaGate (permissive for testing, standard for production)
        if self.config.permissive_mode:
            self._meta_gate = PermissiveMetaGate()
        else:
            self._meta_gate = StandardMetaGate()

        # Position sizer
        sizer_config = PositionSizerConfig(
            risk_per_trade_pct=self.config.risk_per_trade_pct,
            max_position_pct=self.config.max_position_pct,
        )
        self._position_sizer = StandardPositionSizer(sizer_config)

        # Risk manager
        if self.config.permissive_mode:
            self._risk_manager = PermissiveRiskManager()
        else:
            self._risk_manager = StandardRiskManager()

        # Kill switch
        self._kill_switch = KillSwitch()

        logger.info(
            f"[PIPELINE] Initialized with permissive_mode={self.config.permissive_mode}"
        )

    def _register_strategies(self) -> None:
        """Register all available strategies."""
        # Momentum strategies
        for strategy in get_momentum_strategies():
            self._strategy_runner.register(strategy)
            logger.debug(f"[PIPELINE] Registered strategy: {strategy.name}")

        # Mean reversion strategies
        for strategy in get_mean_reversion_strategies():
            self._strategy_runner.register(strategy)
            logger.debug(f"[PIPELINE] Registered strategy: {strategy.name}")

        logger.info(
            f"[PIPELINE] Registered {len(self._strategy_runner.strategies)} strategies"
        )

    async def _emit(self, event: Event) -> None:
        """Emit an event through the callback."""
        if self._emit_event:
            await self._emit_event(event)
        logger.debug(f"[PIPELINE] Emitted {event.event_type.value} for {event.symbol}")

    # =========================================================================
    # PUBLIC API
    # =========================================================================

    def add_symbol(self, symbol: str) -> None:
        """Add a symbol to track."""
        self._state.active_symbols.add(symbol.upper())
        logger.info(f"[PIPELINE] Added symbol: {symbol}")

    def remove_symbol(self, symbol: str) -> None:
        """Remove a symbol from tracking."""
        self._state.active_symbols.discard(symbol.upper())
        self._feature_engine.reset_symbol(symbol.upper())
        logger.info(f"[PIPELINE] Removed symbol: {symbol}")

    def get_active_symbols(self) -> set[str]:
        """Get set of active symbols."""
        return self._state.active_symbols.copy()

    def set_kill_switch(self, active: bool) -> None:
        """Set kill switch state."""
        self._state.kill_switch_active = active
        logger.warning(f"[PIPELINE] Kill switch {'ACTIVATED' if active else 'deactivated'}")

    def is_kill_switch_active(self) -> bool:
        """Check if kill switch is active."""
        return self._state.kill_switch_active

    # =========================================================================
    # EVENT HANDLERS
    # =========================================================================

    async def on_candle(
        self,
        symbol: str,
        candle: OHLCV,
        timestamp: datetime | None = None,
    ) -> None:
        """
        Process a new candle bar.

        This is the main entry point for candle-based data.
        Triggers the full pipeline: features → regime → strategies → scoring → gate → risk

        Args:
            symbol: Stock symbol
            candle: OHLCV candle data
            timestamp: Optional timestamp (defaults to now)
        """
        symbol = symbol.upper()

        # Only process tracked symbols
        if symbol not in self._state.active_symbols:
            return

        # Add bar to feature engine history
        self._feature_engine.add_bar(symbol, candle)

        # Check warmup status
        if not self._feature_engine.warmup_complete(symbol):
            bars = self._feature_engine.bars_available(symbol)
            logger.debug(
                f"[PIPELINE] {symbol} warmup: {bars}/{self.config.min_bars_warmup} bars"
            )
            return

        # Create a minimal snapshot for feature computation
        snapshot = MarketSnapshot(
            symbol=symbol,
            last=candle.close,
            bid=candle.close,
            ask=candle.close,
            volume=candle.volume,
            timestamp=timestamp or datetime.now(timezone.utc),
        )

        # Run the pipeline
        await self._run_pipeline(symbol, snapshot)

    async def on_quote(
        self,
        symbol: str,
        last: float,
        bid: float | None = None,
        ask: float | None = None,
        volume: int | None = None,
        timestamp: datetime | None = None,
    ) -> None:
        """
        Process a quote update.

        For quote updates, we don't add to candle history.
        We re-evaluate with current features if warmup is complete.

        Args:
            symbol: Stock symbol
            last: Last trade price
            bid: Bid price
            ask: Ask price
            volume: Volume
            timestamp: Optional timestamp
        """
        symbol = symbol.upper()

        # Only process tracked symbols
        if symbol not in self._state.active_symbols:
            return

        # Need warmup complete for quote-based evaluation
        if not self._feature_engine.warmup_complete(symbol):
            return

        # Create snapshot
        snapshot = MarketSnapshot(
            symbol=symbol,
            last=last,
            bid=bid or last,
            ask=ask or last,
            volume=volume or 0,
            timestamp=timestamp or datetime.now(timezone.utc),
        )

        # Run pipeline (but with existing feature history)
        await self._run_pipeline(symbol, snapshot)

    async def on_market_snapshot(self, snapshot: MarketSnapshot) -> None:
        """
        Process a market snapshot.

        Convenience method that extracts data from MarketSnapshot.
        """
        await self.on_quote(
            symbol=snapshot.symbol,
            last=snapshot.last,
            bid=snapshot.bid,
            ask=snapshot.ask,
            volume=snapshot.volume,
            timestamp=snapshot.timestamp,
        )

    # =========================================================================
    # PIPELINE STAGES
    # =========================================================================

    async def _run_pipeline(
        self,
        symbol: str,
        snapshot: MarketSnapshot,
    ) -> None:
        """
        Run the full pipeline for a symbol.

        Stages:
        1. Compute features
        2. Detect regime
        3. Route strategies
        4. Generate signals
        5. Score signals
        6. Gate signals
        7. Evaluate risk
        8. Emit events
        """
        try:
            # Stage 1: Compute features
            feature_context = self._feature_engine.compute_features(symbol, snapshot)
            await self._emit(feature_context.to_event())

            # Stage 2: Detect regime
            regime = self._regime_detector.classify(feature_context)
            feature_context = update_feature_context_with_regime(feature_context, regime)

            # Emit regime event
            regime_event = create_event(
                EventType.REGIME_DETECTED,
                symbol=symbol,
                payload=regime.to_dict(),
            )
            await self._emit(regime_event)

            # Stage 3: Route strategies
            routing = self._strategy_router.route(regime)
            feature_context = update_feature_context_with_strategies(feature_context, routing)

            # Stage 4: Generate signals
            strategy_context = StrategyContext(
                symbol=symbol,
                snapshot=snapshot,
                features=feature_context,
                allowed_strategies=feature_context.allowed_strategies,
            )

            signals = self._strategy_runner.run_actionable(strategy_context)

            # Process each actionable signal
            for signal in signals:
                await self._process_signal(signal, feature_context, snapshot)

        except Exception as e:
            logger.error(f"[PIPELINE] Error processing {symbol}: {e}", exc_info=True)

    async def _process_signal(
        self,
        signal: SignalCandidate,
        features: FeatureContext,
        snapshot: MarketSnapshot,
    ) -> None:
        """
        Process a signal through scoring, gating, and risk evaluation.

        Args:
            signal: Generated signal candidate
            features: Feature context
            snapshot: Current market snapshot
        """
        symbol = signal.symbol

        # Check signal cooldown (avoid duplicate signals)
        if not self._check_signal_cooldown(signal):
            logger.debug(f"[PIPELINE] {symbol} signal in cooldown, skipping")
            return

        # Emit SIGNAL_CANDIDATE
        await self._emit(signal.to_event())
        logger.info(
            f"[PIPELINE] Signal: {symbol} {signal.direction.value} "
            f"from {signal.strategy_name} - {signal.rationale}"
        )

        # Stage 5: Score the signal
        scored_signal = self._scorer.score(signal, features)
        await self._emit(scored_signal.to_event())
        logger.info(
            f"[PIPELINE] Scored: {symbol} confidence={scored_signal.confidence:.2f} "
            f"- {scored_signal.score_rationale}"
        )

        # Skip low confidence signals if configured
        if scored_signal.confidence < self.config.min_signal_confidence:
            logger.debug(f"[PIPELINE] {symbol} below min confidence, skipping gate")
            return

        # Stage 6: Gate the signal
        gate_result = self._meta_gate.evaluate(scored_signal, features)
        await self._emit(gate_result.to_event())

        if gate_result.is_rejected:
            logger.info(
                f"[PIPELINE] Rejected: {symbol} - {gate_result.reason_details}"
            )
            return

        logger.info(f"[PIPELINE] Approved: {symbol} by {gate_result.gate_name}")

        # Check kill switch before risk evaluation
        if self.config.respect_kill_switch and self._state.kill_switch_active:
            logger.warning(f"[PIPELINE] {symbol} blocked by kill switch")
            # Emit a blocked event
            blocked_event = create_event(
                EventType.RISK_VETO,
                symbol=symbol,
                payload={
                    "reason": "kill_switch_active",
                    "details": "Kill switch is active, blocking all new signals",
                    "gate_result": gate_result.to_event_payload(),
                },
            )
            await self._emit(blocked_event)
            return

        # Stage 7: Risk evaluation
        # Create a minimal account state for risk evaluation
        # In production, this would come from the broker
        account = self._create_stub_account_state()

        # Compute position size
        entry_price = Decimal(str(snapshot.last))
        stop_price = (
            Decimal(str(signal.invalidation_reference))
            if signal.invalidation_reference
            else None
        )

        position_size = self._position_sizer.compute_size(
            gate_result=gate_result,
            account=account,
            entry_price=entry_price,
            stop_price=stop_price,
        )

        # Evaluate risk
        risk_result = self._risk_manager.evaluate(
            gate_result=gate_result,
            position_size=position_size,
            account=account,
        )

        await self._emit(risk_result.to_event())

        if risk_result.is_vetoed:
            logger.info(
                f"[PIPELINE] Risk veto: {symbol} - {risk_result.reason_details}"
            )
            return

        logger.info(
            f"[PIPELINE] Risk approved: {symbol} "
            f"{position_size.shares} shares @ ${entry_price:.2f}"
        )

        # Record signal for cooldown
        self._record_signal(signal)

        # Signal is fully approved - it will appear in Decision Support Panel
        # Human must confirm before any execution happens

    def _check_signal_cooldown(self, signal: SignalCandidate) -> bool:
        """
        Check if signal is in cooldown period.

        Returns True if signal should be processed, False if in cooldown.
        """
        symbol = signal.symbol
        recent = self._state.recent_signals.get(symbol)

        if recent is None:
            return True

        last_time, last_direction = recent
        now = datetime.now(timezone.utc)

        # Same direction within cooldown = skip
        if signal.direction == last_direction:
            elapsed = (now - last_time).total_seconds()
            if elapsed < self._state.signal_cooldown_seconds:
                return False

        return True

    def _record_signal(self, signal: SignalCandidate) -> None:
        """Record a signal for cooldown tracking."""
        self._state.recent_signals[signal.symbol] = (
            datetime.now(timezone.utc),
            signal.direction,
        )

    def _create_stub_account_state(self) -> AccountState:
        """
        Create a stub account state for risk evaluation.

        In production, this should come from the broker integration.
        For observational mode, we use conservative defaults.
        """
        return AccountState(
            total_equity=Decimal("100000"),  # $100k default
            cash_available=Decimal("50000"),
            buying_power=Decimal("100000"),
            open_position_count=0,
            total_exposure=Decimal("0"),
            exposure_pct=0.0,
            daily_pnl=Decimal("0"),
            daily_pnl_pct=0.0,
            peak_equity=Decimal("100000"),
            current_drawdown_pct=0.0,
            kill_switch_active=self._state.kill_switch_active,
            manual_halt=False,
        )

    # =========================================================================
    # LIFECYCLE
    # =========================================================================

    async def start(self) -> None:
        """Start the pipeline."""
        logger.info("[PIPELINE] Starting signal pipeline")
        # Emit system start event
        await self._emit(create_event(EventType.SYSTEM_START, payload={"component": "pipeline"}))

    async def stop(self) -> None:
        """Stop the pipeline."""
        logger.info("[PIPELINE] Stopping signal pipeline")
        self._state.active_symbols.clear()
        self._feature_engine.reset_all()
        await self._emit(create_event(EventType.SYSTEM_STOP, payload={"component": "pipeline"}))

    def get_status(self) -> dict[str, Any]:
        """Get pipeline status for diagnostics."""
        return {
            "active_symbols": list(self._state.active_symbols),
            "kill_switch_active": self._state.kill_switch_active,
            "permissive_mode": self.config.permissive_mode,
            "registered_strategies": [s.name for s in self._strategy_runner.strategies],
            "warmup_status": {
                symbol: {
                    "bars": self._feature_engine.bars_available(symbol),
                    "complete": self._feature_engine.warmup_complete(symbol),
                }
                for symbol in self._state.active_symbols
            },
        }
