"""
Strategy Base Contract - Defines signal candidate generation interface.

Strategies consume market data and features, producing signal candidates.
They do NOT make trade decisions, size positions, or execute orders.

All strategies are DETERMINISTIC: same input -> same output.

Phase 4 Scope:
- Signal candidate generation only
- No trade decisions
- No AI scoring
- No risk logic
- No execution
- No FSM mutations
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from morpheus.core.events import Event, EventType, create_event
from morpheus.core.market_mode import MarketMode, get_market_mode
from morpheus.data.market_snapshot import MarketSnapshot
from morpheus.features.feature_engine import FeatureContext


class SignalDirection(Enum):
    """Direction of a trading signal."""

    LONG = "long"
    SHORT = "short"
    NONE = "none"  # No signal generated


# Schema version for signal candidates
SIGNAL_SCHEMA_VERSION = "1.0"


class SignalMode(Enum):
    """Signal actionability mode based on market session."""

    ACTIVE = "ACTIVE"  # Signal is actionable (within active trading window)
    OBSERVED = "OBSERVED"  # Signal is logged but not actionable


@dataclass(frozen=True)
class SignalCandidate:
    """
    A candidate trading signal (not yet approved for execution).

    Contains:
    - Symbol and direction
    - Strategy that generated it
    - Supporting context (regime, features)
    - Optional reference levels
    - Market mode context for audit trail

    Immutable to ensure determinism.
    Does NOT contain:
    - Confidence scores (Phase 5)
    - Position sizing (Phase 6)
    - Approval status (Phase 5)
    """

    schema_version: str = SIGNAL_SCHEMA_VERSION

    # Core signal data
    symbol: str = ""
    direction: SignalDirection = SignalDirection.NONE
    strategy_name: str = ""

    # Timestamp
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # Context references (not raw data)
    regime: str = ""
    regime_confidence: float = 0.0

    # Feature references (names of features that triggered signal)
    triggering_features: tuple[str, ...] = field(default_factory=tuple)

    # Rationale / tags for debugging and analysis
    rationale: str = ""
    tags: tuple[str, ...] = field(default_factory=tuple)

    # Optional reference levels (informational, not orders)
    entry_reference: float | None = None
    invalidation_reference: float | None = None
    target_reference: float | None = None

    # Market mode context (for audit trail)
    market_mode: str = ""  # "PREMARKET", "RTH", "OFFHOURS"
    signal_mode: SignalMode = SignalMode.OBSERVED  # ACTIVE or OBSERVED
    time_et: str = ""  # ET timestamp for logging

    def to_event_payload(self) -> dict[str, Any]:
        """Convert to event payload dict."""
        return {
            "schema_version": self.schema_version,
            "symbol": self.symbol,
            "direction": self.direction.value,
            "strategy_name": self.strategy_name,
            "timestamp": self.timestamp.isoformat(),
            "regime": self.regime,
            "regime_confidence": self.regime_confidence,
            "triggering_features": list(self.triggering_features),
            "rationale": self.rationale,
            "tags": list(self.tags),
            "entry_reference": self.entry_reference,
            "invalidation_reference": self.invalidation_reference,
            "target_reference": self.target_reference,
            # Market mode context (mandatory for audit)
            "market_mode": self.market_mode,
            "signal_mode": self.signal_mode.value,
            "time_et": self.time_et,
        }

    def to_event(self) -> Event:
        """Create a SIGNAL_CANDIDATE or SIGNAL_OBSERVED event based on mode."""
        # Use SIGNAL_OBSERVED for observe-only signals
        event_type = (
            EventType.SIGNAL_OBSERVED
            if self.signal_mode == SignalMode.OBSERVED
            else EventType.SIGNAL_CANDIDATE
        )
        return create_event(
            event_type,
            payload=self.to_event_payload(),
            symbol=self.symbol,
            timestamp=self.timestamp,
        )

    def is_actionable(self) -> bool:
        """Check if this signal suggests a trade (LONG or SHORT)."""
        return self.direction in (SignalDirection.LONG, SignalDirection.SHORT)


@dataclass(frozen=True)
class StrategyContext:
    """
    Context passed to strategy evaluation.

    Bundles all inputs a strategy needs to generate signals.
    Immutable to ensure strategies cannot mutate state.
    """

    symbol: str
    snapshot: MarketSnapshot
    features: FeatureContext

    # From strategy router (Phase 3)
    allowed_strategies: tuple[str, ...] = field(default_factory=tuple)

    # Market mode context (for strategy filtering and event tagging)
    market_mode: MarketMode | None = None


class Strategy(ABC):
    """
    Abstract base class for trading strategies.

    Strategies:
    - Are pure functions (no side effects)
    - Are deterministic (same input -> same output)
    - Do NOT make trade decisions
    - Do NOT execute orders
    - Do NOT access external state

    Each strategy must implement:
    - name: Unique identifier
    - evaluate: Generate signal candidate from context
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique name identifying this strategy."""
        pass

    @property
    def description(self) -> str:
        """Human-readable description of the strategy."""
        return ""

    @property
    def required_features(self) -> tuple[str, ...]:
        """Feature names required for this strategy to evaluate."""
        return ()

    @property
    def compatible_regimes(self) -> tuple[str, ...]:
        """Regime patterns this strategy is designed for (empty = all)."""
        return ()

    @property
    def allowed_market_modes(self) -> frozenset[str]:
        """
        Market modes where this strategy is allowed to generate signals.

        Default: RTH only (Regular Trading Hours).
        Override to include "PREMARKET" for premarket-safe strategies.

        Returns:
            frozenset of allowed mode names ("PREMARKET", "RTH", "OFFHOURS")
        """
        return frozenset({"RTH"})

    def can_evaluate(self, context: StrategyContext) -> bool:
        """
        Check if strategy can evaluate given the context.

        Returns False if:
        - Required features are missing
        - Strategy not in allowed list
        - Regime is incompatible
        - Market mode not allowed for this strategy
        """
        # Check market mode first (safety gate)
        if context.market_mode:
            if context.market_mode.name not in self.allowed_market_modes:
                return False

        # Check if strategy is allowed by router
        if context.allowed_strategies and self.name not in context.allowed_strategies:
            return False

        # Check required features
        for feature in self.required_features:
            if not context.features.has_feature(feature):
                return False

        # Check regime compatibility (if specified)
        if self.compatible_regimes and context.features.regime:
            regime_match = any(
                pattern in context.features.regime
                for pattern in self.compatible_regimes
            )
            if not regime_match:
                return False

        return True

    @abstractmethod
    def evaluate(self, context: StrategyContext) -> SignalCandidate:
        """
        Evaluate the strategy and generate a signal candidate.

        Args:
            context: StrategyContext with all inputs

        Returns:
            SignalCandidate (may have direction=NONE if no signal)

        This method MUST:
        - Be deterministic
        - Not have side effects
        - Not access external state
        - Return a valid SignalCandidate (even if direction=NONE)
        """
        pass

    def create_signal(
        self,
        context: StrategyContext,
        direction: SignalDirection,
        rationale: str,
        triggering_features: tuple[str, ...] = (),
        tags: tuple[str, ...] = (),
        entry_reference: float | None = None,
        invalidation_reference: float | None = None,
        target_reference: float | None = None,
    ) -> SignalCandidate:
        """
        Helper to create a SignalCandidate with common fields populated.

        Args:
            context: StrategyContext
            direction: Signal direction
            rationale: Why this signal was generated
            triggering_features: Features that triggered the signal
            tags: Additional categorization tags
            entry_reference: Optional entry price reference
            invalidation_reference: Optional stop/invalidation level
            target_reference: Optional target price

        Returns:
            SignalCandidate with all fields populated
        """
        from morpheus.core.market_mode import get_time_et

        # Determine market mode and signal mode
        mode = context.market_mode or get_market_mode()
        signal_mode = SignalMode.ACTIVE if mode.active_trading else SignalMode.OBSERVED

        return SignalCandidate(
            schema_version=SIGNAL_SCHEMA_VERSION,
            symbol=context.symbol,
            direction=direction,
            strategy_name=self.name,
            timestamp=context.snapshot.timestamp,
            regime=context.features.regime or "",
            regime_confidence=context.features.regime_confidence,
            triggering_features=triggering_features,
            rationale=rationale,
            tags=tags,
            entry_reference=entry_reference,
            invalidation_reference=invalidation_reference,
            target_reference=target_reference,
            # Market mode context
            market_mode=mode.name,
            signal_mode=signal_mode,
            time_et=get_time_et(),
        )

    def create_no_signal(self, context: StrategyContext) -> SignalCandidate:
        """
        Helper to create a SignalCandidate with no signal (direction=NONE).

        Args:
            context: StrategyContext

        Returns:
            SignalCandidate with direction=NONE
        """
        from morpheus.core.market_mode import get_time_et

        mode = context.market_mode or get_market_mode()

        return SignalCandidate(
            schema_version=SIGNAL_SCHEMA_VERSION,
            symbol=context.symbol,
            direction=SignalDirection.NONE,
            strategy_name=self.name,
            timestamp=context.snapshot.timestamp,
            regime=context.features.regime or "",
            regime_confidence=context.features.regime_confidence,
            market_mode=mode.name,
            signal_mode=SignalMode.OBSERVED,  # No-signals are always observed
            time_et=get_time_et(),
        )


import logging

_logger = logging.getLogger(__name__)


class StrategyRunner:
    """
    Runs multiple strategies against a context.

    Collects signal candidates from all eligible strategies.
    Does NOT decide which signals to act on (that's Phase 5).
    """

    def __init__(self, strategies: list[Strategy] | None = None):
        """
        Initialize with a list of strategies.

        Args:
            strategies: List of Strategy instances to run
        """
        self._strategies: list[Strategy] = strategies or []

    def register(self, strategy: Strategy) -> None:
        """Register a strategy to run."""
        if strategy not in self._strategies:
            self._strategies.append(strategy)

    def unregister(self, strategy_name: str) -> None:
        """Unregister a strategy by name."""
        self._strategies = [s for s in self._strategies if s.name != strategy_name]

    @property
    def strategies(self) -> list[Strategy]:
        """Get registered strategies."""
        return list(self._strategies)

    def get_strategies_for_mode(self, market_mode: str) -> list[Strategy]:
        """
        Get strategies allowed for a specific market mode.

        Args:
            market_mode: "PREMARKET", "RTH", or "OFFHOURS"

        Returns:
            List of strategies allowed in that mode
        """
        return [s for s in self._strategies if market_mode in s.allowed_market_modes]

    def run(self, context: StrategyContext) -> list[SignalCandidate]:
        """
        Run all eligible strategies and collect signal candidates.

        Filters strategies by market mode, features, regime, etc.

        Args:
            context: StrategyContext with all inputs

        Returns:
            List of SignalCandidate from eligible strategies
            (includes NONE signals for completeness)
        """
        signals: list[SignalCandidate] = []
        mode_name = context.market_mode.name if context.market_mode else "UNKNOWN"

        for strategy in self._strategies:
            if strategy.can_evaluate(context):
                signal = strategy.evaluate(context)
                signals.append(signal)
            else:
                # Log why strategy was filtered (for debugging)
                if context.market_mode and context.market_mode.name not in strategy.allowed_market_modes:
                    _logger.debug(
                        f"[STRATEGY] {strategy.name} filtered: not allowed in {mode_name} "
                        f"(allowed: {strategy.allowed_market_modes})"
                    )

        return signals

    def run_actionable(self, context: StrategyContext) -> list[SignalCandidate]:
        """
        Run strategies and return only actionable signals (LONG/SHORT).

        Args:
            context: StrategyContext with all inputs

        Returns:
            List of actionable SignalCandidate (no NONE signals)
        """
        all_signals = self.run(context)
        return [s for s in all_signals if s.is_actionable()]
