"""
Tests for Strategy Router.

Phase 3 test coverage:
- Strategy routing for different regimes
- Blocking inappropriate strategies
- Cross-regime filters
- Routing rules configuration
- Determinism
"""

import pytest

from morpheus.features.feature_engine import FeatureContext
from morpheus.regime.regime_detector import (
    TrendRegime,
    VolatilityRegime,
    MomentumRegime,
    RegimeClassification,
)
from morpheus.regime.strategy_router import (
    Strategy,
    RoutingResult,
    RoutingRules,
    StrategyRouter,
    route_strategies,
    update_feature_context_with_strategies,
)


class TestStrategyEnum:
    """Tests for Strategy enum."""

    def test_strategy_values(self):
        """Strategy should have expected values."""
        assert Strategy.TREND_FOLLOW_LONG.value == "trend_follow_long"
        assert Strategy.MEAN_REVERSION_LONG.value == "mean_reversion_long"
        assert Strategy.BREAKOUT_LONG.value == "breakout_long"
        assert Strategy.CASH.value == "cash"


class TestRoutingResult:
    """Tests for RoutingResult dataclass."""

    def test_routing_result_creation(self):
        """RoutingResult should store all fields."""
        result = RoutingResult(
            allowed_strategies=(Strategy.TREND_FOLLOW_LONG, Strategy.MOMENTUM_LONG),
            blocked_strategies=(Strategy.MEAN_REVERSION_SHORT,),
            rationale="Uptrend favors long strategies",
            confidence=0.85,
        )

        assert len(result.allowed_strategies) == 2
        assert Strategy.TREND_FOLLOW_LONG in result.allowed_strategies
        assert result.rationale == "Uptrend favors long strategies"

    def test_routing_result_is_frozen(self):
        """RoutingResult should be immutable."""
        result = RoutingResult(
            allowed_strategies=(Strategy.CASH,),
            blocked_strategies=(),
            rationale="Low confidence",
            confidence=0.2,
        )

        with pytest.raises(Exception):
            result.allowed_strategies = (Strategy.TREND_FOLLOW_LONG,)

    def test_is_strategy_allowed(self):
        """is_strategy_allowed should check allowed list."""
        result = RoutingResult(
            allowed_strategies=(Strategy.TREND_FOLLOW_LONG, Strategy.MOMENTUM_LONG),
            blocked_strategies=(Strategy.MEAN_REVERSION_SHORT,),
            rationale="",
            confidence=0.8,
        )

        assert result.is_strategy_allowed(Strategy.TREND_FOLLOW_LONG) is True
        assert result.is_strategy_allowed(Strategy.MEAN_REVERSION_SHORT) is False

    def test_get_primary_strategy(self):
        """get_primary_strategy should return first allowed."""
        result = RoutingResult(
            allowed_strategies=(Strategy.TREND_FOLLOW_LONG, Strategy.MOMENTUM_LONG),
            blocked_strategies=(),
            rationale="",
            confidence=0.8,
        )

        assert result.get_primary_strategy() == Strategy.TREND_FOLLOW_LONG

    def test_get_primary_strategy_empty(self):
        """get_primary_strategy should return None if empty."""
        result = RoutingResult(
            allowed_strategies=(),
            blocked_strategies=(),
            rationale="",
            confidence=0.0,
        )

        assert result.get_primary_strategy() is None

    def test_to_dict(self):
        """to_dict should serialize correctly."""
        result = RoutingResult(
            allowed_strategies=(Strategy.TREND_FOLLOW_LONG,),
            blocked_strategies=(Strategy.MEAN_REVERSION_SHORT,),
            rationale="Uptrend",
            confidence=0.85,
        )

        data = result.to_dict()

        assert data["allowed_strategies"] == ["trend_follow_long"]
        assert data["blocked_strategies"] == ["mean_reversion_short"]
        assert data["confidence"] == 0.85


class TestRoutingRules:
    """Tests for RoutingRules configuration."""

    def test_default_rules(self):
        """Default rules should have sensible values."""
        rules = RoutingRules()

        assert rules.min_confidence_threshold == 0.3
        assert rules.trend_follow_min_strength == 0.4
        assert rules.mean_reversion_max_volatility == 4.0

    def test_custom_rules(self):
        """Custom rules should be accepted."""
        rules = RoutingRules(
            min_confidence_threshold=0.5,
            mean_reversion_max_volatility=3.0,
        )

        assert rules.min_confidence_threshold == 0.5
        assert rules.mean_reversion_max_volatility == 3.0


class TestStrategyRouter:
    """Tests for StrategyRouter class."""

    def _create_classification(
        self,
        trend: TrendRegime = TrendRegime.RANGING,
        volatility: VolatilityRegime = VolatilityRegime.NORMAL_VOLATILITY,
        momentum: MomentumRegime = MomentumRegime.NEUTRAL,
        confidence: float = 0.7,
    ) -> RegimeClassification:
        """Create a test classification."""
        return RegimeClassification(
            primary_regime=f"{volatility.value}_{trend.value}",
            trend=trend,
            volatility=volatility,
            momentum=momentum,
            confidence=confidence,
            trend_confidence=confidence,
            volatility_confidence=confidence,
            momentum_confidence=confidence,
        )

    def test_router_initialization(self):
        """Router should initialize with default rules."""
        router = StrategyRouter()

        assert router.rules is not None

    def test_router_custom_rules(self):
        """Router should accept custom rules."""
        rules = RoutingRules(min_confidence_threshold=0.5)
        router = StrategyRouter(rules)

        assert router.rules.min_confidence_threshold == 0.5

    def test_route_low_confidence(self):
        """Low confidence should route to cash only."""
        router = StrategyRouter()

        classification = self._create_classification(confidence=0.2)

        result = router.route(classification)

        assert result.allowed_strategies == (Strategy.CASH,)
        assert "Low regime confidence" in result.rationale

    def test_route_trending_up(self):
        """Uptrend should favor long strategies."""
        router = StrategyRouter()

        classification = self._create_classification(
            trend=TrendRegime.TRENDING_UP,
            momentum=MomentumRegime.BULLISH,
            confidence=0.8,
        )

        result = router.route(classification)

        assert Strategy.TREND_FOLLOW_LONG in result.allowed_strategies
        assert Strategy.TREND_FOLLOW_SHORT in result.blocked_strategies

    def test_route_trending_down(self):
        """Downtrend should favor short strategies."""
        router = StrategyRouter()

        classification = self._create_classification(
            trend=TrendRegime.TRENDING_DOWN,
            momentum=MomentumRegime.BEARISH,
            confidence=0.8,
        )

        result = router.route(classification)

        assert Strategy.TREND_FOLLOW_SHORT in result.allowed_strategies
        assert Strategy.TREND_FOLLOW_LONG in result.blocked_strategies

    def test_route_ranging(self):
        """Ranging market should favor mean reversion."""
        router = StrategyRouter()

        classification = self._create_classification(
            trend=TrendRegime.RANGING,
            volatility=VolatilityRegime.NORMAL_VOLATILITY,
            confidence=0.7,
        )

        result = router.route(classification)

        assert Strategy.MEAN_REVERSION_LONG in result.allowed_strategies
        assert Strategy.MEAN_REVERSION_SHORT in result.allowed_strategies
        assert Strategy.TREND_FOLLOW_LONG in result.blocked_strategies

    def test_route_high_volatility(self):
        """High volatility should favor breakout strategies."""
        router = StrategyRouter()

        classification = self._create_classification(
            trend=TrendRegime.TRENDING_UP,
            volatility=VolatilityRegime.HIGH_VOLATILITY,
            confidence=0.8,
        )

        result = router.route(classification)

        assert Strategy.VOLATILITY_EXPANSION in result.allowed_strategies
        assert Strategy.BREAKOUT_LONG in result.allowed_strategies

    def test_route_low_volatility(self):
        """Low volatility should favor contraction strategies."""
        router = StrategyRouter()

        classification = self._create_classification(
            trend=TrendRegime.RANGING,
            volatility=VolatilityRegime.LOW_VOLATILITY,
            confidence=0.7,
        )

        result = router.route(classification)

        assert Strategy.VOLATILITY_CONTRACTION in result.allowed_strategies


class TestCrossRegimeFilters:
    """Tests for cross-regime filtering."""

    def test_high_vol_ranging_blocks_mean_reversion(self):
        """High vol + ranging should block mean reversion."""
        router = StrategyRouter()

        classification = RegimeClassification(
            primary_regime="volatile_ranging",
            trend=TrendRegime.RANGING,
            volatility=VolatilityRegime.HIGH_VOLATILITY,
            momentum=MomentumRegime.NEUTRAL,
            confidence=0.7,
            trend_confidence=0.7,
            volatility_confidence=0.8,
            momentum_confidence=0.6,
        )

        result = router.route(classification)

        # Mean reversion should be blocked in volatile ranging
        assert Strategy.MEAN_REVERSION_LONG in result.blocked_strategies
        assert Strategy.MEAN_REVERSION_SHORT in result.blocked_strategies

    def test_low_vol_strong_momentum_allows_contraction(self):
        """Low vol + strong momentum should allow contraction plays."""
        router = StrategyRouter()

        classification = RegimeClassification(
            primary_regime="quiet_trending_up",
            trend=TrendRegime.TRENDING_UP,
            volatility=VolatilityRegime.LOW_VOLATILITY,
            momentum=MomentumRegime.STRONG_BULLISH,
            confidence=0.8,
            trend_confidence=0.8,
            volatility_confidence=0.75,
            momentum_confidence=0.85,
        )

        result = router.route(classification)

        assert Strategy.VOLATILITY_CONTRACTION in result.allowed_strategies


class TestRouteStrategiesFunction:
    """Tests for route_strategies convenience function."""

    def test_route_strategies(self):
        """route_strategies should work like router.route."""
        classification = RegimeClassification(
            primary_regime="trending_up",
            trend=TrendRegime.TRENDING_UP,
            volatility=VolatilityRegime.NORMAL_VOLATILITY,
            momentum=MomentumRegime.BULLISH,
            confidence=0.8,
            trend_confidence=0.8,
            volatility_confidence=0.7,
            momentum_confidence=0.75,
        )

        result = route_strategies(classification)

        assert isinstance(result, RoutingResult)
        assert len(result.allowed_strategies) > 0

    def test_route_strategies_with_custom_rules(self):
        """route_strategies should accept custom rules."""
        classification = RegimeClassification(
            primary_regime="ranging",
            trend=TrendRegime.RANGING,
            volatility=VolatilityRegime.NORMAL_VOLATILITY,
            momentum=MomentumRegime.NEUTRAL,
            confidence=0.4,  # Above default 0.3 threshold
            trend_confidence=0.4,
            volatility_confidence=0.4,
            momentum_confidence=0.4,
        )

        # With higher threshold, should get cash only
        rules = RoutingRules(min_confidence_threshold=0.5)
        result = route_strategies(classification, rules)

        assert result.allowed_strategies == (Strategy.CASH,)


class TestUpdateFeatureContextWithStrategies:
    """Tests for update_feature_context_with_strategies."""

    def test_update_adds_strategies(self):
        """Update should add strategies to context."""
        ctx = FeatureContext(
            symbol="SPY",
            regime="trending_up",
            regime_confidence=0.8,
        )

        routing = RoutingResult(
            allowed_strategies=(Strategy.TREND_FOLLOW_LONG, Strategy.MOMENTUM_LONG),
            blocked_strategies=(Strategy.MEAN_REVERSION_SHORT,),
            rationale="Uptrend",
            confidence=0.8,
        )

        updated = update_feature_context_with_strategies(ctx, routing)

        assert "trend_follow_long" in updated.allowed_strategies
        assert "momentum_long" in updated.allowed_strategies

    def test_update_preserves_original(self):
        """Update should not modify original context."""
        ctx = FeatureContext(
            symbol="SPY",
            regime="ranging",
        )

        routing = RoutingResult(
            allowed_strategies=(Strategy.MEAN_REVERSION_LONG,),
            blocked_strategies=(),
            rationale="",
            confidence=0.7,
        )

        updated = update_feature_context_with_strategies(ctx, routing)

        # Original should be unchanged
        assert ctx.allowed_strategies == ()
        # Updated should have strategies
        assert len(updated.allowed_strategies) > 0


class TestStrategyRouterDeterminism:
    """Tests verifying strategy routing is deterministic."""

    def test_same_regime_same_strategies(self):
        """Same regime should always produce same strategies."""
        router = StrategyRouter()

        classification = RegimeClassification(
            primary_regime="volatile_trending_up",
            trend=TrendRegime.TRENDING_UP,
            volatility=VolatilityRegime.HIGH_VOLATILITY,
            momentum=MomentumRegime.STRONG_BULLISH,
            confidence=0.85,
            trend_confidence=0.9,
            volatility_confidence=0.8,
            momentum_confidence=0.85,
        )

        result1 = router.route(classification)
        result2 = router.route(classification)

        assert result1.allowed_strategies == result2.allowed_strategies
        assert result1.blocked_strategies == result2.blocked_strategies
        assert result1.rationale == result2.rationale


class TestStrategyRouterIsolation:
    """Tests verifying strategy router is isolated."""

    def test_strategy_router_has_no_trade_imports(self):
        """Strategy router should not import trade modules."""
        from morpheus.regime import strategy_router

        source = open(strategy_router.__file__).read()

        assert "trade_fsm" not in source
        assert "TradeLifecycle" not in source

    def test_strategy_router_has_no_broker_imports(self):
        """Strategy router should not import broker modules."""
        from morpheus.regime import strategy_router

        source = open(strategy_router.__file__).read()

        assert "schwab_auth" not in source
        assert "schwab_market" not in source
