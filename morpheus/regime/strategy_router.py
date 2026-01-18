"""
Strategy Router - Maps regimes to allowed strategies.

Determines which trading strategies are appropriate for the
current market regime.

All routing is DETERMINISTIC: same regime -> same strategies.

Phase 3 Scope:
- Routing logic only
- No trade decisions
- No state mutation
- Consumes RegimeClassification (read-only)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from morpheus.features.feature_engine import FeatureContext
from morpheus.regime.regime_detector import (
    RegimeClassification,
    TrendRegime,
    VolatilityRegime,
    MomentumRegime,
)


class Strategy(Enum):
    """Available trading strategies."""

    # Trend-following strategies
    TREND_FOLLOW_LONG = "trend_follow_long"
    TREND_FOLLOW_SHORT = "trend_follow_short"

    # Mean reversion strategies
    MEAN_REVERSION_LONG = "mean_reversion_long"
    MEAN_REVERSION_SHORT = "mean_reversion_short"

    # Breakout strategies
    BREAKOUT_LONG = "breakout_long"
    BREAKOUT_SHORT = "breakout_short"

    # Momentum strategies
    MOMENTUM_LONG = "momentum_long"
    MOMENTUM_SHORT = "momentum_short"

    # Range-bound strategies
    RANGE_FADE = "range_fade"

    # Volatility strategies
    VOLATILITY_EXPANSION = "volatility_expansion"
    VOLATILITY_CONTRACTION = "volatility_contraction"

    # Defensive
    CASH = "cash"  # No trading


@dataclass(frozen=True)
class RoutingResult:
    """
    Result of strategy routing.

    Contains:
    - Allowed strategies (in priority order)
    - Blocked strategies
    - Routing rationale
    """

    # Strategies allowed in current regime (priority order)
    allowed_strategies: tuple[Strategy, ...]

    # Strategies explicitly blocked
    blocked_strategies: tuple[Strategy, ...]

    # Rationale for routing decision
    rationale: str

    # Confidence in routing (inherits from regime confidence)
    confidence: float

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "allowed_strategies": [s.value for s in self.allowed_strategies],
            "blocked_strategies": [s.value for s in self.blocked_strategies],
            "rationale": self.rationale,
            "confidence": self.confidence,
        }

    def is_strategy_allowed(self, strategy: Strategy) -> bool:
        """Check if a strategy is allowed."""
        return strategy in self.allowed_strategies

    def get_primary_strategy(self) -> Strategy | None:
        """Get the highest priority allowed strategy."""
        if self.allowed_strategies:
            return self.allowed_strategies[0]
        return None


@dataclass(frozen=True)
class RoutingRules:
    """
    Configuration for strategy routing rules.

    Defines which strategies are allowed for each regime combination.
    """

    # Minimum confidence to allow strategies (below this -> CASH only)
    min_confidence_threshold: float = 0.3

    # Trend-following minimum trend strength
    trend_follow_min_strength: float = 0.4

    # Mean reversion maximum volatility (ATR%)
    mean_reversion_max_volatility: float = 4.0

    # Breakout minimum volatility
    breakout_min_volatility: float = 1.5


class StrategyRouter:
    """
    Routes market regimes to allowed trading strategies.

    Uses configurable rules to determine which strategies
    are appropriate for the current market conditions.

    All routing is deterministic - same regime always produces
    the same strategy set.
    """

    def __init__(self, rules: RoutingRules | None = None):
        """
        Initialize the strategy router.

        Args:
            rules: Optional custom routing rules
        """
        self.rules = rules or RoutingRules()

    def route(self, classification: RegimeClassification) -> RoutingResult:
        """
        Route a regime classification to allowed strategies.

        Args:
            classification: RegimeClassification from RegimeDetector

        Returns:
            RoutingResult with allowed and blocked strategies
        """
        allowed: list[Strategy] = []
        blocked: list[Strategy] = []
        rationale_parts: list[str] = []

        # Check confidence threshold
        if classification.confidence < self.rules.min_confidence_threshold:
            return RoutingResult(
                allowed_strategies=(Strategy.CASH,),
                blocked_strategies=tuple(s for s in Strategy if s != Strategy.CASH),
                rationale=f"Low regime confidence ({classification.confidence:.2f}), cash only",
                confidence=classification.confidence,
            )

        # Route based on trend
        trend_allowed, trend_blocked, trend_rationale = self._route_trend(
            classification.trend,
            classification.trend_confidence,
        )
        allowed.extend(trend_allowed)
        blocked.extend(trend_blocked)
        if trend_rationale:
            rationale_parts.append(trend_rationale)

        # Route based on volatility
        vol_allowed, vol_blocked, vol_rationale = self._route_volatility(
            classification.volatility,
            classification.volatility_confidence,
        )
        allowed.extend(vol_allowed)
        blocked.extend(vol_blocked)
        if vol_rationale:
            rationale_parts.append(vol_rationale)

        # Route based on momentum
        mom_allowed, mom_blocked, mom_rationale = self._route_momentum(
            classification.momentum,
            classification.momentum_confidence,
        )
        allowed.extend(mom_allowed)
        blocked.extend(mom_blocked)
        if mom_rationale:
            rationale_parts.append(mom_rationale)

        # Apply cross-regime filters
        allowed, blocked = self._apply_cross_filters(
            allowed, blocked, classification
        )

        # Remove duplicates while preserving order
        allowed = list(dict.fromkeys(allowed))
        blocked = list(dict.fromkeys(blocked))

        # Ensure allowed strategies are not in blocked
        allowed = [s for s in allowed if s not in blocked]

        # If nothing allowed, default to cash
        if not allowed:
            allowed = [Strategy.CASH]
            rationale_parts.append("No strategies passed filters, defaulting to cash")

        return RoutingResult(
            allowed_strategies=tuple(allowed),
            blocked_strategies=tuple(blocked),
            rationale="; ".join(rationale_parts) if rationale_parts else "Standard routing",
            confidence=classification.confidence,
        )

    def _route_trend(
        self, trend: TrendRegime, confidence: float
    ) -> tuple[list[Strategy], list[Strategy], str]:
        """Route based on trend regime."""
        allowed: list[Strategy] = []
        blocked: list[Strategy] = []
        rationale = ""

        if trend == TrendRegime.TRENDING_UP:
            allowed = [
                Strategy.TREND_FOLLOW_LONG,
                Strategy.MOMENTUM_LONG,
                Strategy.BREAKOUT_LONG,
            ]
            blocked = [
                Strategy.TREND_FOLLOW_SHORT,
                Strategy.MEAN_REVERSION_SHORT,
            ]
            rationale = f"Uptrend ({confidence:.0%} conf): favoring long strategies"

        elif trend == TrendRegime.TRENDING_DOWN:
            allowed = [
                Strategy.TREND_FOLLOW_SHORT,
                Strategy.MOMENTUM_SHORT,
                Strategy.BREAKOUT_SHORT,
            ]
            blocked = [
                Strategy.TREND_FOLLOW_LONG,
                Strategy.MEAN_REVERSION_LONG,
            ]
            rationale = f"Downtrend ({confidence:.0%} conf): favoring short strategies"

        elif trend == TrendRegime.RANGING:
            allowed = [
                Strategy.MEAN_REVERSION_LONG,
                Strategy.MEAN_REVERSION_SHORT,
                Strategy.RANGE_FADE,
            ]
            blocked = [
                Strategy.TREND_FOLLOW_LONG,
                Strategy.TREND_FOLLOW_SHORT,
            ]
            rationale = f"Ranging ({confidence:.0%} conf): favoring mean reversion"

        return allowed, blocked, rationale

    def _route_volatility(
        self, volatility: VolatilityRegime, confidence: float
    ) -> tuple[list[Strategy], list[Strategy], str]:
        """Route based on volatility regime."""
        allowed: list[Strategy] = []
        blocked: list[Strategy] = []
        rationale = ""

        if volatility == VolatilityRegime.HIGH_VOLATILITY:
            allowed = [
                Strategy.VOLATILITY_EXPANSION,
                Strategy.BREAKOUT_LONG,
                Strategy.BREAKOUT_SHORT,
            ]
            blocked = [
                Strategy.VOLATILITY_CONTRACTION,
            ]
            rationale = f"High vol ({confidence:.0%} conf): favoring volatility plays"

        elif volatility == VolatilityRegime.LOW_VOLATILITY:
            allowed = [
                Strategy.VOLATILITY_CONTRACTION,
                Strategy.MEAN_REVERSION_LONG,
                Strategy.MEAN_REVERSION_SHORT,
            ]
            blocked = [
                Strategy.VOLATILITY_EXPANSION,
            ]
            rationale = f"Low vol ({confidence:.0%} conf): favoring contraction plays"

        elif volatility == VolatilityRegime.NORMAL_VOLATILITY:
            # Normal vol doesn't restrict much
            rationale = f"Normal vol ({confidence:.0%} conf): standard routing"

        return allowed, blocked, rationale

    def _route_momentum(
        self, momentum: MomentumRegime, confidence: float
    ) -> tuple[list[Strategy], list[Strategy], str]:
        """Route based on momentum regime."""
        allowed: list[Strategy] = []
        blocked: list[Strategy] = []
        rationale = ""

        if momentum == MomentumRegime.STRONG_BULLISH:
            allowed = [Strategy.MOMENTUM_LONG]
            # Extremely overbought - might block new longs
            rationale = f"Strong bullish momentum ({confidence:.0%} conf)"

        elif momentum == MomentumRegime.BULLISH:
            allowed = [Strategy.MOMENTUM_LONG, Strategy.TREND_FOLLOW_LONG]
            rationale = f"Bullish momentum ({confidence:.0%} conf)"

        elif momentum == MomentumRegime.STRONG_BEARISH:
            allowed = [Strategy.MOMENTUM_SHORT]
            rationale = f"Strong bearish momentum ({confidence:.0%} conf)"

        elif momentum == MomentumRegime.BEARISH:
            allowed = [Strategy.MOMENTUM_SHORT, Strategy.TREND_FOLLOW_SHORT]
            rationale = f"Bearish momentum ({confidence:.0%} conf)"

        elif momentum == MomentumRegime.NEUTRAL:
            allowed = [Strategy.RANGE_FADE]
            rationale = f"Neutral momentum ({confidence:.0%} conf)"

        return allowed, blocked, rationale

    def _apply_cross_filters(
        self,
        allowed: list[Strategy],
        blocked: list[Strategy],
        classification: RegimeClassification,
    ) -> tuple[list[Strategy], list[Strategy]]:
        """
        Apply cross-regime filters.

        Some combinations of regimes should block certain strategies
        even if individual regime routing allowed them.
        """
        # High volatility + ranging = dangerous for mean reversion
        if (
            classification.volatility == VolatilityRegime.HIGH_VOLATILITY
            and classification.trend == TrendRegime.RANGING
        ):
            if Strategy.MEAN_REVERSION_LONG in allowed:
                allowed.remove(Strategy.MEAN_REVERSION_LONG)
                blocked.append(Strategy.MEAN_REVERSION_LONG)
            if Strategy.MEAN_REVERSION_SHORT in allowed:
                allowed.remove(Strategy.MEAN_REVERSION_SHORT)
                blocked.append(Strategy.MEAN_REVERSION_SHORT)

        # Low volatility + strong momentum = potential breakout
        if (
            classification.volatility == VolatilityRegime.LOW_VOLATILITY
            and classification.momentum in (MomentumRegime.STRONG_BULLISH, MomentumRegime.STRONG_BEARISH)
        ):
            if Strategy.VOLATILITY_CONTRACTION not in allowed:
                allowed.append(Strategy.VOLATILITY_CONTRACTION)

        return allowed, blocked


def route_strategies(
    classification: RegimeClassification,
    rules: RoutingRules | None = None,
) -> RoutingResult:
    """
    Convenience function to route strategies from a classification.

    Args:
        classification: RegimeClassification from RegimeDetector
        rules: Optional custom routing rules

    Returns:
        RoutingResult with allowed strategies
    """
    router = StrategyRouter(rules)
    return router.route(classification)


def update_feature_context_with_strategies(
    context: FeatureContext,
    routing: RoutingResult,
) -> FeatureContext:
    """
    Create a new FeatureContext with allowed strategies.

    Since FeatureContext is frozen, this creates a new instance.

    Args:
        context: FeatureContext (with regime already set)
        routing: RoutingResult from StrategyRouter

    Returns:
        New FeatureContext with allowed_strategies populated
    """
    from dataclasses import replace

    return replace(
        context,
        allowed_strategies=tuple(s.value for s in routing.allowed_strategies),
    )
