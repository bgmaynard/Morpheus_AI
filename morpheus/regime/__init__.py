"""Regime detection and strategy routing."""

from morpheus.regime.regime_detector import (
    TrendRegime,
    VolatilityRegime,
    MomentumRegime,
    RegimeClassification,
    RegimeThresholds,
    RegimeDetector,
    classify_regime,
    update_feature_context_with_regime,
)
from morpheus.regime.strategy_router import (
    Strategy,
    RoutingResult,
    RoutingRules,
    StrategyRouter,
    route_strategies,
    update_feature_context_with_strategies,
)

__all__ = [
    # Regime detection
    "TrendRegime",
    "VolatilityRegime",
    "MomentumRegime",
    "RegimeClassification",
    "RegimeThresholds",
    "RegimeDetector",
    "classify_regime",
    "update_feature_context_with_regime",
    # Strategy routing
    "Strategy",
    "RoutingResult",
    "RoutingRules",
    "StrategyRouter",
    "route_strategies",
    "update_feature_context_with_strategies",
]
