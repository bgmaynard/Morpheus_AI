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
from morpheus.core.market_mode import (
    MarketMode, MarketPhase, PhaseRegime,
    get_market_mode, get_market_phase, get_phase_regime, get_time_et,
    PREMARKET_NORMAL,
)

# Features & Regime
from morpheus.features.feature_engine import FeatureEngine, FeatureContext
from morpheus.features.indicators import OHLCV
from morpheus.integrations.max_ai_scanner import update_feature_context_with_external
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
    SignalMode,
)
from morpheus.strategies.momentum import get_momentum_strategies
from morpheus.strategies.mean_reversion import get_mean_reversion_strategies
from morpheus.strategies.premarket_observer import get_premarket_strategies
from morpheus.strategies.mass_strategies import get_mass_strategies

# MASS (Morpheus Adaptive Strategy System)
from morpheus.core.mass_config import MASSConfig
from morpheus.structure.analyzer import StructureAnalyzer
from morpheus.structure.types import StructureGrade
from morpheus.classify.classifier import StrategyClassifier
from morpheus.classify.types import ClassificationResult
from morpheus.features.feature_engine import (
    update_feature_context_with_structure,
    update_feature_context_with_momentum,
)
from morpheus.regime.mass_regime import MASSRegimeMapper, MASSRegime
from morpheus.risk.mass_risk_governor import MASSRiskGovernor, MASSRiskConfig
from morpheus.evolve.tracker import PerformanceTracker
from morpheus.evolve.weight_manager import StrategyWeightManager
from morpheus.supervise.supervisor import AISupervisor
from morpheus.supervise.config import SupervisorConfig

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
from morpheus.risk.risk_manager import (
    PermissiveRiskManager,
    StandardRiskManager,
    RoomToProfitRiskManager,
)
from morpheus.risk.room_to_profit import RoomToProfitConfig
from morpheus.risk.kill_switch import KillSwitch, KillSwitchResult
from morpheus.core.runtime_config import get_runtime_config

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

    # Position sizing config (IBKR_Algo_BOT_V2 parity)
    risk_per_trade_pct: float = 0.02  # 2% - matches IBKR bot
    max_position_pct: float = 0.10  # 10%

    # Paper trading equity (for position sizing parity with IBKR_Algo_BOT_V2)
    paper_equity: float = 500.0  # $500 paper equity for comparable P&L

    # Kill switch respected even in permissive mode
    respect_kill_switch: bool = True

    # Room-to-profit checking (spread/slippage awareness)
    enable_room_to_profit: bool = True  # Check execution costs before approval
    min_net_rr_ratio: float = 1.5  # Minimum net R:R after costs
    max_spread_pct: float = 1.5  # Max spread % to allow trade


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
        external_context_provider: Callable[[str], Awaitable[dict[str, Any]]] | None = None,
        mass_config: MASSConfig | None = None,
        position_manager: Any = None,
        momentum_engine: Any = None,
    ):
        """
        Initialize the signal pipeline.

        Args:
            config: Pipeline configuration
            emit_event: Async callback to emit events (for server integration)
            external_context_provider: Async callback to fetch external context for a symbol
                                       (e.g., from MAX_AI_SCANNER for scanner_score, gap_pct, etc.)
            mass_config: MASS configuration (None = use defaults with MASS enabled)
            position_manager: PaperPositionManager for live position tracking (optional)
            momentum_engine: MomentumEngine for real-time momentum intelligence (optional)
        """
        self.config = config or PipelineConfig()
        self._emit_event = emit_event
        self._external_context_provider = external_context_provider
        self._state = PipelineState()
        self._mass_config = mass_config or MASSConfig()
        self._position_manager = position_manager
        self._momentum_engine = momentum_engine

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

        # Ignition Gate (hard momentum filter, Stage 5.5)
        self._ignition_gate = None
        rc = get_runtime_config()
        if getattr(rc, 'ignition_gate_enabled', False):
            from morpheus.scoring.ignition_gate import IgnitionGate, IgnitionGateConfig
            self._ignition_gate = IgnitionGate(IgnitionGateConfig.from_runtime_config(rc))
            logger.info("[PIPELINE] Ignition Gate enabled (hard momentum filter)")

        # MetaGate (permissive for testing, standard for production)
        if self.config.permissive_mode:
            self._meta_gate = PermissiveMetaGate()
        else:
            # Use standard gate with lowered threshold for stub_scorer
            # TODO: Replace with proper threshold when ML scorer is implemented
            from morpheus.scoring.meta_gate import StandardGateConfig
            self._meta_gate = StandardMetaGate(
                config=StandardGateConfig(min_confidence=0.3)
            )

        # Position sizer
        sizer_config = PositionSizerConfig(
            risk_per_trade_pct=self.config.risk_per_trade_pct,
            max_position_pct=self.config.max_position_pct,
        )
        self._position_sizer = StandardPositionSizer(sizer_config)

        # Risk manager
        if self.config.permissive_mode:
            base_risk_manager = PermissiveRiskManager()
        else:
            base_risk_manager = StandardRiskManager()

        # Wrap with room-to-profit checking if enabled
        if self.config.enable_room_to_profit:
            rtp_config = RoomToProfitConfig(
                min_net_rr_ratio=self.config.min_net_rr_ratio,
                max_spread_pct=self.config.max_spread_pct,
            )
            self._risk_manager = RoomToProfitRiskManager(
                base_manager=base_risk_manager,
                rtp_config=rtp_config,
            )
            logger.info("[PIPELINE] Room-to-profit checking enabled")
        else:
            self._risk_manager = base_risk_manager

        # Kill switch
        self._kill_switch = KillSwitch()

        # MASS Phase 1 components
        if self._mass_config.enabled:
            self._structure_analyzer = StructureAnalyzer(self._mass_config.structure)
            self._strategy_classifier = StrategyClassifier(self._mass_config.classifier)
            logger.info("[PIPELINE] MASS enabled: Structure Analyzer + Strategy Classifier active")
        else:
            self._structure_analyzer = None
            self._strategy_classifier = None

        # MASS Phase 2 components
        self._mass_regime_mapper = MASSRegimeMapper()
        self._performance_tracker = PerformanceTracker()
        self._weight_manager = StrategyWeightManager(
            tracker=self._performance_tracker,
            base_weights=self._mass_config.classifier.strategy_weights if self._mass_config else {},
        )
        self._supervisor = AISupervisor(
            tracker=self._performance_tracker,
            weight_manager=self._weight_manager,
            config=SupervisorConfig(),
        )
        self._current_mass_regime: MASSRegime | None = None
        self._current_phase_regime: PhaseRegime | None = None
        self._last_phase: MarketPhase | None = None  # Track for phase transition events

        # Wrap risk manager with MASS governor if enabled
        if self._mass_config.enabled and self._mass_config.regime_mapping_enabled:
            self._risk_manager = MASSRiskGovernor(
                base_manager=self._risk_manager,
                mass_config=MASSRiskConfig(),
                strategy_risk_overrides=self._mass_config.strategy_risk_overrides,
                position_manager=self._position_manager,
            )
            logger.info("[PIPELINE] MASS Risk Governor active")

        logger.info(
            f"[PIPELINE] Initialized with permissive_mode={self.config.permissive_mode}, "
            f"room_to_profit={self.config.enable_room_to_profit}, "
            f"mass_enabled={self._mass_config.enabled}"
        )

    def _register_strategies(self) -> None:
        """Register all available strategies."""
        # Premarket observer strategies (PREMARKET mode only, OBSERVE-only)
        for strategy in get_premarket_strategies():
            self._strategy_runner.register(strategy)
            logger.debug(
                f"[PIPELINE] Registered strategy: {strategy.name} "
                f"(modes: {strategy.allowed_market_modes})"
            )

        # Momentum strategies (RTH mode only)
        for strategy in get_momentum_strategies():
            self._strategy_runner.register(strategy)
            logger.debug(f"[PIPELINE] Registered strategy: {strategy.name}")

        # Mean reversion strategies (RTH mode only)
        for strategy in get_mean_reversion_strategies():
            self._strategy_runner.register(strategy)
            logger.debug(f"[PIPELINE] Registered strategy: {strategy.name}")

        # MASS strategies
        for strategy in get_mass_strategies():
            self._strategy_runner.register(strategy)
            logger.debug(
                f"[PIPELINE] Registered MASS strategy: {strategy.name} "
                f"(modes: {strategy.allowed_market_modes})"
            )

        # Log summary by mode
        premarket_strategies = self._strategy_runner.get_strategies_for_mode("PREMARKET")
        rth_strategies = self._strategy_runner.get_strategies_for_mode("RTH")

        logger.info(
            f"[PIPELINE] Registered {len(self._strategy_runner.strategies)} strategies: "
            f"{len(premarket_strategies)} for PREMARKET, {len(rth_strategies)} for RTH"
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

    def set_external_context_provider(
        self,
        provider: Callable[[str], Awaitable[dict[str, Any]]] | None,
    ) -> None:
        """
        Set the external context provider for scanner augmentation.

        This allows the server to wire the scanner integration after initialization.
        The provider should return scanner context dict for a given symbol.

        Args:
            provider: Async callable that takes a symbol and returns context dict
        """
        self._external_context_provider = provider
        if provider:
            logger.info("[PIPELINE] External context provider set (scanner augmentation enabled)")

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
        0. Get market mode (trading window gate)
        1. Compute features
        2. Detect regime
        3. Route strategies
        4. Generate signals (filtered by market mode)
        5. Score signals
        6. Gate signals
        7. Evaluate risk
        8. Emit events (ACTIVE vs OBSERVED based on mode)
        """
        try:
            # Stage 0: Get market mode (trading window gate)
            market_mode = get_market_mode()
            time_et = get_time_et()

            logger.debug(
                f"[PIPELINE] {symbol} mode={market_mode.name} "
                f"active_trading={market_mode.active_trading} time_et={time_et}"
            )

            # Stage 1: Compute features
            feature_context = self._feature_engine.compute_features(symbol, snapshot)

            # Stage 1.5: Augment with external context (from Scanner)
            # This is READ-ONLY data - strategies may reference but NOT recompute
            if self._external_context_provider:
                try:
                    external_data = await self._external_context_provider(symbol)
                    if external_data:
                        feature_context = update_feature_context_with_external(
                            feature_context, external_data
                        )
                        logger.debug(
                            f"[PIPELINE] {symbol} augmented with scanner context: "
                            f"score={external_data.get('scanner_score', 0)}"
                        )
                except Exception as e:
                    logger.warning(f"[PIPELINE] Failed to fetch external context for {symbol}: {e}")

            # Stage 1.7: Augment with momentum engine data (if available)
            if self._momentum_engine:
                try:
                    momentum_snap = self._momentum_engine.get_snapshot(symbol)
                    if momentum_snap:
                        feature_context = update_feature_context_with_momentum(
                            feature_context,
                            momentum_score=momentum_snap.momentum_score,
                            momentum_state=momentum_snap.momentum_state.value,
                            momentum_confidence=momentum_snap.confidence,
                            nofi=momentum_snap.nofi,
                            l2_pressure=momentum_snap.l2_pressure,
                            velocity=momentum_snap.velocity,
                            absorption=momentum_snap.absorption,
                            spread_dynamics=momentum_snap.spread_dynamics,
                        )
                        logger.debug(
                            f"[PIPELINE] {symbol} momentum: score={momentum_snap.momentum_score:.1f} "
                            f"state={momentum_snap.momentum_state.value}"
                        )
                except Exception as e:
                    logger.warning(f"[PIPELINE] Momentum engine error for {symbol}: {e}")

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

            # Stage 2.3: Phase-aware regime routing
            # Premarket and after-hours use phase-specific regimes, NOT RTH regime
            market_phase = get_market_phase()
            phase_regime = get_phase_regime(market_phase)

            # Emit phase transition event when phase changes
            if self._last_phase is not None and self._last_phase != market_phase:
                phase_event = create_event(
                    EventType.MARKET_PHASE_CHANGE,
                    payload={
                        "phase": market_phase.value,
                        "previous_phase": self._last_phase.value,
                        "regime": phase_regime.rationale if phase_regime else "RTH_COMPUTED",
                    },
                )
                await self._emit(phase_event)
                logger.info(
                    f"[PIPELINE] Phase transition: {self._last_phase.value} -> {market_phase.value}"
                )
            self._last_phase = market_phase

            if self._mass_config.enabled:
                if phase_regime is not None:
                    # NON-RTH: Use phase-specific regime, ignore computed RTH regime
                    self._current_phase_regime = phase_regime
                    # Tell risk governor we're in a phase regime (bypasses RTH limits)
                    if isinstance(self._risk_manager, MASSRiskGovernor):
                        self._risk_manager.set_phase_regime(phase_regime)
                    logger.debug(
                        f"[PIPELINE] {symbol} phase={market_phase.value} "
                        f"regime={phase_regime.rationale}"
                    )
                else:
                    # RTH: Use computed MASS regime as normal
                    self._current_phase_regime = None
                    mass_regime = self._mass_regime_mapper.map(regime)
                    self._current_mass_regime = mass_regime
                    if isinstance(self._risk_manager, MASSRiskGovernor):
                        self._risk_manager.set_regime_mode(mass_regime.mode.value)

            # Stage 2.5: MASS Structure Analysis (if enabled)
            if self._structure_analyzer and self._mass_config.enabled:
                structure_grade = self._structure_analyzer.classify(feature_context, snapshot)
                feature_context = update_feature_context_with_structure(
                    feature_context,
                    structure_grade=structure_grade.grade,
                    structure_score=structure_grade.score,
                    structure_data=structure_grade.to_event_payload(),
                )
                await self._emit(structure_grade.to_event(symbol))
                logger.debug(
                    f"[PIPELINE] {symbol} structure: grade={structure_grade.grade} "
                    f"score={structure_grade.score:.0f}"
                )

            # Stage 2.7: MASS Strategy Classification (if enabled)
            if self._strategy_classifier and self._mass_config.enabled:
                classification = self._strategy_classifier.classify(
                    features=feature_context,
                    structure=structure_grade,
                    regime=regime,
                    market_mode=market_mode,
                )
                if classification.assigned_strategies:
                    # Override allowed_strategies with MASS classification
                    from dataclasses import replace as dc_replace
                    feature_context = dc_replace(
                        feature_context,
                        allowed_strategies=tuple(
                            s.value for s in classification.assigned_strategies
                        ),
                    )
                await self._emit(classification.to_event())
            else:
                # Fallback: Use original StrategyRouter when MASS is disabled
                routing = self._strategy_router.route(regime)
                feature_context = update_feature_context_with_strategies(feature_context, routing)

            # Stage 4: Generate signals (filtered by market mode)
            strategy_context = StrategyContext(
                symbol=symbol,
                snapshot=snapshot,
                features=feature_context,
                allowed_strategies=feature_context.allowed_strategies,
                market_mode=market_mode,  # Inject market mode for filtering
            )

            # Log how many strategies are available for this mode
            eligible_strategies = self._strategy_runner.get_strategies_for_mode(market_mode.name)
            logger.debug(
                f"[PIPELINE] {symbol} {len(eligible_strategies)} strategies eligible in {market_mode.name}"
            )

            signals = self._strategy_runner.run_actionable(strategy_context)

            # Process each actionable signal
            for signal in signals:
                # Emit phase-allowed event for visibility
                if phase_regime is not None:
                    await self._emit(create_event(
                        EventType.STRATEGY_PHASE_ALLOWED,
                        symbol=symbol,
                        payload={
                            "strategy": signal.strategy_name,
                            "phase": market_phase.value,
                            "regime": phase_regime.rationale,
                        },
                    ))
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

        Respects signal_mode: ACTIVE signals go through full pipeline,
        OBSERVED signals are logged but not processed for execution.

        Args:
            signal: Generated signal candidate
            features: Feature context
            snapshot: Current market snapshot
        """
        symbol = signal.symbol
        is_observed = signal.signal_mode == SignalMode.OBSERVED

        # Check signal cooldown (avoid duplicate signals)
        if not self._check_signal_cooldown(signal):
            logger.debug(f"[PIPELINE] {symbol} signal in cooldown, skipping")
            return

        # Emit SIGNAL_CANDIDATE (includes market_mode, signal_mode, time_et)
        await self._emit(signal.to_event())

        # Log with mode context
        mode_tag = "[OBSERVED]" if is_observed else "[ACTIVE]"
        logger.info(
            f"[PIPELINE] {mode_tag} Signal: {symbol} {signal.direction.value} "
            f"from {signal.strategy_name} @ {signal.time_et} - {signal.rationale}"
        )

        # OBSERVED signals are logged but not processed further
        # They will appear in UI but not as actionable
        if is_observed:
            logger.info(
                f"[PIPELINE] {symbol} signal is OBSERVED (mode={signal.market_mode}) - "
                "skipping scoring/gate/risk"
            )
            return

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

        # Stage 5.5: Ignition Gate (hard momentum filter)
        if self._ignition_gate:
            daily_pnl_pct = 0.0
            if self._position_manager:
                try:
                    acct = self._position_manager.get_account_state(
                        base_equity=Decimal(str(self.config.paper_equity)),
                        kill_switch_active=self._state.kill_switch_active,
                    )
                    daily_pnl_pct = float(acct.daily_pnl_pct)
                except Exception:
                    pass

            ig_result = self._ignition_gate.evaluate(
                features=features.features,
                daily_pnl_pct=daily_pnl_pct,
            )

            if ig_result.passed:
                await self._emit(create_event(
                    EventType.IGNITION_APPROVED,
                    symbol=symbol,
                    payload=ig_result.to_event_payload(),
                ))
            else:
                await self._emit(create_event(
                    EventType.IGNITION_REJECTED,
                    symbol=symbol,
                    payload=ig_result.to_event_payload(),
                ))
                logger.info(
                    f"[PIPELINE] Ignition REJECTED: {symbol} - {ig_result.failures}"
                )
                return  # Stop processing

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

        # Block signals while already holding a position in this symbol
        if self._position_manager and self._position_manager.has_position(symbol):
            logger.debug(f"[PIPELINE] {symbol} blocked: position already open")
            return False

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
        Create account state for risk evaluation.

        If a position manager is available, returns real position/exposure data.
        Otherwise falls back to conservative defaults.

        Uses paper_equity from config ($500 default for IBKR parity).
        """
        # Get paper equity from config (default $500 for IBKR_Algo_BOT_V2 parity)
        paper_equity = Decimal(str(self.config.paper_equity))

        if self._position_manager:
            return self._position_manager.get_account_state(
                base_equity=paper_equity,
                kill_switch_active=self._state.kill_switch_active,
            )

        # Fallback stub for when no position manager is available
        return AccountState(
            total_equity=paper_equity,
            cash_available=paper_equity,
            buying_power=paper_equity,
            open_position_count=0,
            total_exposure=Decimal("0"),
            exposure_pct=0.0,
            daily_pnl=Decimal("0"),
            daily_pnl_pct=0.0,
            peak_equity=paper_equity,
            current_drawdown_pct=0.0,
            kill_switch_active=self._state.kill_switch_active,
            manual_halt=False,
            is_paper_mode=True,  # Deliverable 3 - Paper mode isolation
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
        # Get current market mode
        market_mode = get_market_mode()
        time_et = get_time_et()

        # Get strategies eligible for current mode
        strategies_for_mode = self._strategy_runner.get_strategies_for_mode(market_mode.name)

        return {
            # Market mode status (required for UI display)
            "market_mode": market_mode.name,
            "active_trading": market_mode.active_trading,
            "observe_only": market_mode.observe_only,
            "time_et": time_et,
            # Strategies
            "active_symbols": list(self._state.active_symbols),
            "kill_switch_active": self._state.kill_switch_active,
            "permissive_mode": self.config.permissive_mode,
            "registered_strategies": [s.name for s in self._strategy_runner.strategies],
            "strategies_for_current_mode": [s.name for s in strategies_for_mode],
            "warmup_status": {
                symbol: {
                    "bars": self._feature_engine.bars_available(symbol),
                    "complete": self._feature_engine.warmup_complete(symbol),
                }
                for symbol in self._state.active_symbols
            },
            # Phase-aware regime status
            "market_phase": get_market_phase().value,
            "phase_regime": self._current_phase_regime.to_dict() if self._current_phase_regime else None,
            # MASS status
            "mass_enabled": self._mass_config.enabled,
            "mass_structure_analyzer": self._structure_analyzer is not None,
            "mass_strategy_classifier": self._strategy_classifier is not None,
            "mass_regime": self._current_mass_regime.to_event_payload() if self._current_mass_regime else None,
            "mass_regime_mapping_enabled": self._mass_config.regime_mapping_enabled,
            "mass_feedback_enabled": self._mass_config.feedback_enabled,
            "mass_supervisor_enabled": self._mass_config.supervisor_enabled,
            # Shadow mode + Ignition gate
            "runtime_mode": getattr(get_runtime_config(), 'runtime_mode', 'SHADOW'),
            "ignition_gate": self._ignition_gate is not None,
        }
