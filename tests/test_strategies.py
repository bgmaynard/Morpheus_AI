"""
Tests for Strategy Signal Candidates.

Phase 4 test coverage:
- SignalCandidate creation and immutability
- Strategy base contract
- Momentum strategies (FirstPullback, HODContinuation)
- Mean reversion strategies (VWAPReclaim)
- Isolation (no execution/risk imports)
- Determinism (same input -> same output)
- Event emission
"""

import pytest
from datetime import datetime, timezone

from morpheus.data.market_snapshot import create_snapshot, MarketSnapshot
from morpheus.features.feature_engine import FeatureContext
from morpheus.strategies.base import (
    SIGNAL_SCHEMA_VERSION,
    SignalDirection,
    SignalCandidate,
    StrategyContext,
    Strategy,
    StrategyRunner,
)
from morpheus.strategies.momentum import (
    FirstPullbackStrategy,
    HighOfDayContinuationStrategy,
    get_momentum_strategies,
)
from morpheus.strategies.mean_reversion import (
    VWAPReclaimStrategy,
    get_mean_reversion_strategies,
)
from morpheus.strategies import get_all_strategies
from morpheus.core.events import EventType


class TestSignalDirection:
    """Tests for SignalDirection enum."""

    def test_direction_values(self):
        """SignalDirection should have expected values."""
        assert SignalDirection.LONG.value == "long"
        assert SignalDirection.SHORT.value == "short"
        assert SignalDirection.NONE.value == "none"


class TestSignalCandidate:
    """Tests for SignalCandidate dataclass."""

    def test_signal_creation(self):
        """SignalCandidate should store all fields."""
        signal = SignalCandidate(
            symbol="SPY",
            direction=SignalDirection.LONG,
            strategy_name="test_strategy",
            regime="trending_up",
            rationale="Test signal",
        )

        assert signal.symbol == "SPY"
        assert signal.direction == SignalDirection.LONG
        assert signal.strategy_name == "test_strategy"
        assert signal.regime == "trending_up"

    def test_signal_is_frozen(self):
        """SignalCandidate should be immutable."""
        signal = SignalCandidate(symbol="SPY")

        with pytest.raises(Exception):
            signal.symbol = "QQQ"

    def test_signal_default_values(self):
        """SignalCandidate should have sensible defaults."""
        signal = SignalCandidate()

        assert signal.schema_version == SIGNAL_SCHEMA_VERSION
        assert signal.symbol == ""
        assert signal.direction == SignalDirection.NONE
        assert signal.strategy_name == ""
        assert signal.triggering_features == ()
        assert signal.tags == ()

    def test_signal_is_actionable(self):
        """is_actionable should identify LONG/SHORT signals."""
        long_signal = SignalCandidate(direction=SignalDirection.LONG)
        short_signal = SignalCandidate(direction=SignalDirection.SHORT)
        none_signal = SignalCandidate(direction=SignalDirection.NONE)

        assert long_signal.is_actionable() is True
        assert short_signal.is_actionable() is True
        assert none_signal.is_actionable() is False

    def test_signal_to_event_payload(self):
        """to_event_payload should serialize correctly."""
        signal = SignalCandidate(
            symbol="SPY",
            direction=SignalDirection.LONG,
            strategy_name="test",
            regime="trending_up",
            triggering_features=("rsi_14", "ema_21"),
            tags=("momentum", "pullback"),
            entry_reference=450.0,
        )

        payload = signal.to_event_payload()

        assert payload["symbol"] == "SPY"
        assert payload["direction"] == "long"
        assert payload["strategy_name"] == "test"
        assert payload["triggering_features"] == ["rsi_14", "ema_21"]
        assert payload["entry_reference"] == 450.0

    def test_signal_to_event(self):
        """to_event should create SIGNAL_CANDIDATE event."""
        signal = SignalCandidate(symbol="SPY", direction=SignalDirection.LONG)

        event = signal.to_event()

        assert event.event_type == EventType.SIGNAL_CANDIDATE
        assert event.symbol == "SPY"


class TestStrategyContext:
    """Tests for StrategyContext."""

    def _create_snapshot(self) -> MarketSnapshot:
        """Create a test snapshot."""
        return create_snapshot(
            symbol="SPY",
            bid=450.0,
            ask=450.10,
            last=450.05,
            volume=5000000,
        )

    def _create_features(self, **features) -> FeatureContext:
        """Create a test feature context."""
        return FeatureContext(
            symbol="SPY",
            features=features,
            bars_available=100,
            warmup_complete=True,
            regime="trending_up",
            regime_confidence=0.8,
        )

    def test_context_creation(self):
        """StrategyContext should bundle inputs."""
        snapshot = self._create_snapshot()
        features = self._create_features(sma_20=448.0)

        ctx = StrategyContext(
            symbol="SPY",
            snapshot=snapshot,
            features=features,
            allowed_strategies=("first_pullback",),
        )

        assert ctx.symbol == "SPY"
        assert ctx.snapshot == snapshot
        assert ctx.features == features
        assert "first_pullback" in ctx.allowed_strategies

    def test_context_is_frozen(self):
        """StrategyContext should be immutable."""
        snapshot = self._create_snapshot()
        features = self._create_features()

        ctx = StrategyContext(
            symbol="SPY",
            snapshot=snapshot,
            features=features,
        )

        with pytest.raises(Exception):
            ctx.symbol = "QQQ"


class TestStrategyRunner:
    """Tests for StrategyRunner."""

    def _create_context(
        self,
        regime: str = "trending_up",
        allowed: tuple[str, ...] = (),
        **features,
    ) -> StrategyContext:
        """Create a test strategy context."""
        snapshot = create_snapshot(
            symbol="SPY",
            bid=450.0,
            ask=450.10,
            last=450.05,
            volume=5000000,
        )
        feat_ctx = FeatureContext(
            symbol="SPY",
            features=features,
            bars_available=100,
            warmup_complete=True,
            regime=regime,
            regime_confidence=0.8,
            allowed_strategies=allowed,
        )
        return StrategyContext(
            symbol="SPY",
            snapshot=snapshot,
            features=feat_ctx,
            allowed_strategies=allowed,
        )

    def test_runner_initialization(self):
        """Runner should initialize with empty strategy list."""
        runner = StrategyRunner()

        assert runner.strategies == []

    def test_runner_register(self):
        """Runner should register strategies."""
        runner = StrategyRunner()
        strategy = FirstPullbackStrategy()

        runner.register(strategy)

        assert strategy in runner.strategies

    def test_runner_unregister(self):
        """Runner should unregister strategies by name."""
        runner = StrategyRunner()
        strategy = FirstPullbackStrategy()

        runner.register(strategy)
        runner.unregister("first_pullback")

        assert strategy not in runner.strategies

    def test_runner_run_returns_signals(self):
        """Runner should return signals from strategies."""
        runner = StrategyRunner(get_all_strategies())

        # Create context with features needed by strategies
        ctx = self._create_context(
            regime="trending_up",
            ema_9=451.0,
            ema_21=449.0,
            price_vs_sma20=0.5,
            rsi_14=55.0,
            trend_strength=0.6,
            trend_direction=0.8,
        )

        signals = runner.run(ctx)

        # Should have at least one signal (even if NONE)
        assert len(signals) > 0
        assert all(isinstance(s, SignalCandidate) for s in signals)

    def test_runner_run_actionable(self):
        """run_actionable should filter out NONE signals."""
        runner = StrategyRunner(get_all_strategies())

        ctx = self._create_context(
            regime="ranging",  # Strategies might not match
            sma_20=450.0,
        )

        actionable = runner.run_actionable(ctx)

        # All should be LONG or SHORT
        assert all(s.is_actionable() for s in actionable)


class TestFirstPullbackStrategy:
    """Tests for FirstPullbackStrategy."""

    def _create_context(self, regime: str, **features) -> StrategyContext:
        """Create context for testing."""
        snapshot = create_snapshot(
            symbol="SPY",
            bid=450.0,
            ask=450.10,
            last=450.05,
            volume=5000000,
        )
        feat_ctx = FeatureContext(
            symbol="SPY",
            features=features,
            bars_available=100,
            warmup_complete=True,
            regime=regime,
            regime_confidence=0.8,
            allowed_strategies=("first_pullback",),
        )
        return StrategyContext(
            symbol="SPY",
            snapshot=snapshot,
            features=feat_ctx,
            allowed_strategies=("first_pullback",),
        )

    def test_strategy_name(self):
        """Strategy should have correct name."""
        strategy = FirstPullbackStrategy()
        assert strategy.name == "first_pullback"

    def test_strategy_required_features(self):
        """Strategy should list required features."""
        strategy = FirstPullbackStrategy()
        assert "ema_21" in strategy.required_features
        assert "rsi_14" in strategy.required_features

    def test_long_signal_in_uptrend(self):
        """Strategy should generate LONG in uptrend pullback."""
        strategy = FirstPullbackStrategy()

        # Price at 450.05, EMA21 at 449.0 (within 2%)
        ctx = self._create_context(
            regime="trending_up",
            ema_9=451.0,
            ema_21=449.0,  # Price near EMA21
            price_vs_sma20=0.5,
            rsi_14=55.0,
            trend_strength=0.6,
            trend_direction=0.8,
        )

        signal = strategy.evaluate(ctx)

        assert signal.direction == SignalDirection.LONG
        assert signal.strategy_name == "first_pullback"
        assert "pullback" in signal.tags

    def test_no_signal_wrong_regime(self):
        """Strategy should return NONE in ranging market."""
        strategy = FirstPullbackStrategy()

        ctx = self._create_context(
            regime="ranging",  # Not trending
            ema_9=450.0,
            ema_21=450.0,
            price_vs_sma20=0.0,
            rsi_14=50.0,
            trend_strength=0.2,
            trend_direction=0.0,
        )

        signal = strategy.evaluate(ctx)

        assert signal.direction == SignalDirection.NONE

    def test_determinism(self):
        """Same input should produce same output."""
        strategy = FirstPullbackStrategy()

        ctx = self._create_context(
            regime="trending_up",
            ema_9=451.0,
            ema_21=449.0,
            price_vs_sma20=0.5,
            rsi_14=55.0,
            trend_strength=0.6,
            trend_direction=0.8,
        )

        signal1 = strategy.evaluate(ctx)
        signal2 = strategy.evaluate(ctx)

        assert signal1.direction == signal2.direction
        assert signal1.rationale == signal2.rationale


class TestHighOfDayContinuationStrategy:
    """Tests for HighOfDayContinuationStrategy."""

    def _create_context(self, regime: str, **features) -> StrategyContext:
        """Create context for testing."""
        snapshot = create_snapshot(
            symbol="SPY",
            bid=454.0,
            ask=454.10,
            last=454.05,
            volume=5000000,
        )
        feat_ctx = FeatureContext(
            symbol="SPY",
            features=features,
            bars_available=100,
            warmup_complete=True,
            regime=regime,
            regime_confidence=0.8,
            allowed_strategies=("hod_continuation",),
        )
        return StrategyContext(
            symbol="SPY",
            snapshot=snapshot,
            features=feat_ctx,
            allowed_strategies=("hod_continuation",),
        )

    def test_strategy_name(self):
        """Strategy should have correct name."""
        strategy = HighOfDayContinuationStrategy()
        assert strategy.name == "hod_continuation"

    def test_long_signal_at_highs(self):
        """Strategy should generate LONG at high of day with volume."""
        strategy = HighOfDayContinuationStrategy()

        ctx = self._create_context(
            regime="trending_up",
            rsi_14=65.0,
            macd_histogram=0.5,
            relative_volume=1.5,
            price_in_range=95.0,  # At highs
            trend_direction=0.8,
        )

        signal = strategy.evaluate(ctx)

        assert signal.direction == SignalDirection.LONG
        assert "hod" in signal.tags

    def test_no_signal_low_volume(self):
        """Strategy should not signal with low volume."""
        strategy = HighOfDayContinuationStrategy()

        ctx = self._create_context(
            regime="trending_up",
            rsi_14=65.0,
            macd_histogram=0.5,
            relative_volume=0.8,  # Low volume
            price_in_range=95.0,
            trend_direction=0.8,
        )

        signal = strategy.evaluate(ctx)

        assert signal.direction == SignalDirection.NONE


class TestVWAPReclaimStrategy:
    """Tests for VWAPReclaimStrategy."""

    def _create_context(self, regime: str, **features) -> StrategyContext:
        """Create context for testing."""
        snapshot = create_snapshot(
            symbol="SPY",
            bid=450.0,
            ask=450.10,
            last=450.05,
            volume=5000000,
        )
        feat_ctx = FeatureContext(
            symbol="SPY",
            features=features,
            bars_available=100,
            warmup_complete=True,
            regime=regime,
            regime_confidence=0.7,
            allowed_strategies=("vwap_reclaim",),
        )
        return StrategyContext(
            symbol="SPY",
            snapshot=snapshot,
            features=feat_ctx,
            allowed_strategies=("vwap_reclaim",),
        )

    def test_strategy_name(self):
        """Strategy should have correct name."""
        strategy = VWAPReclaimStrategy()
        assert strategy.name == "vwap_reclaim"

    def test_long_signal_on_reclaim(self):
        """Strategy should generate LONG on VWAP reclaim from below."""
        strategy = VWAPReclaimStrategy()

        ctx = self._create_context(
            regime="ranging",
            vwap=449.5,
            price_vs_vwap=0.12,  # Just above VWAP
            rsi_14=42.0,  # Recovering
            mean_reversion_score=1.2,  # Was oversold
            bb_position=30.0,  # Low in bands
            atr_pct=1.5,  # Normal volatility
        )

        signal = strategy.evaluate(ctx)

        assert signal.direction == SignalDirection.LONG
        assert "vwap" in signal.tags
        assert "mean_reversion" in signal.tags

    def test_no_signal_high_volatility(self):
        """Strategy should not signal in high volatility."""
        strategy = VWAPReclaimStrategy()

        ctx = self._create_context(
            regime="ranging",
            vwap=449.5,
            price_vs_vwap=0.1,
            rsi_14=42.0,
            mean_reversion_score=1.2,
            bb_position=30.0,
            atr_pct=4.0,  # High volatility
        )

        signal = strategy.evaluate(ctx)

        assert signal.direction == SignalDirection.NONE


class TestStrategyIsolation:
    """Tests verifying strategies don't import forbidden modules."""

    def test_base_has_no_execution_imports(self):
        """Base module should not import execution modules."""
        from morpheus.strategies import base

        source = open(base.__file__).read()

        # Check for actual imports of forbidden modules
        # Words in comments/docstrings are allowed
        assert "from morpheus" not in source or "order_" not in source
        assert "import order" not in source.lower()
        assert "from morpheus.execution" not in source

    def test_base_has_no_risk_imports(self):
        """Base module should not import risk modules."""
        from morpheus.strategies import base

        source = open(base.__file__).read()

        assert "from morpheus.risk" not in source
        assert "import risk" not in source.lower()

    def test_base_has_no_fsm_imports(self):
        """Base module should not import FSM."""
        from morpheus.strategies import base

        source = open(base.__file__).read()

        assert "trade_fsm" not in source
        assert "TradeLifecycle" not in source

    def test_momentum_has_no_forbidden_imports(self):
        """Momentum module should not import forbidden modules."""
        from morpheus.strategies import momentum

        source = open(momentum.__file__).read()

        assert "trade_fsm" not in source
        assert "from morpheus.execution" not in source
        assert "from morpheus.risk" not in source

    def test_mean_reversion_has_no_forbidden_imports(self):
        """Mean reversion module should not import forbidden modules."""
        from morpheus.strategies import mean_reversion

        source = open(mean_reversion.__file__).read()

        assert "trade_fsm" not in source
        assert "from morpheus.execution" not in source
        assert "from morpheus.risk" not in source


class TestStrategyDeterminism:
    """Tests verifying strategies are deterministic."""

    def _create_context(self, regime: str, **features) -> StrategyContext:
        """Create deterministic context."""
        snapshot = create_snapshot(
            symbol="SPY",
            bid=450.0,
            ask=450.10,
            last=450.05,
            volume=5000000,
        )
        feat_ctx = FeatureContext(
            symbol="SPY",
            timestamp=datetime(2024, 6, 17, 15, 0, 0, tzinfo=timezone.utc),
            features=features,
            bars_available=100,
            warmup_complete=True,
            regime=regime,
            regime_confidence=0.8,
        )
        return StrategyContext(
            symbol="SPY",
            snapshot=snapshot,
            features=feat_ctx,
        )

    def test_all_strategies_deterministic(self):
        """All strategies should produce same output for same input."""
        strategies = get_all_strategies()

        features = {
            "ema_9": 451.0,
            "ema_21": 449.0,
            "price_vs_sma20": 0.5,
            "rsi_14": 55.0,
            "trend_strength": 0.6,
            "trend_direction": 0.8,
            "vwap": 449.5,
            "price_vs_vwap": 0.1,
            "mean_reversion_score": 0.5,
            "bb_position": 50.0,
            "atr_pct": 1.5,
            "macd_histogram": 0.2,
            "relative_volume": 1.2,
            "price_in_range": 60.0,
        }

        ctx = self._create_context(regime="trending_up", **features)

        for strategy in strategies:
            signal1 = strategy.evaluate(ctx)
            signal2 = strategy.evaluate(ctx)

            assert signal1.direction == signal2.direction, f"{strategy.name} not deterministic"
            assert signal1.rationale == signal2.rationale, f"{strategy.name} rationale differs"


class TestGetAllStrategies:
    """Tests for strategy collection functions."""

    def test_get_momentum_strategies(self):
        """get_momentum_strategies should return all momentum strategies."""
        strategies = get_momentum_strategies()

        assert len(strategies) == 2
        names = [s.name for s in strategies]
        assert "first_pullback" in names
        assert "hod_continuation" in names

    def test_get_mean_reversion_strategies(self):
        """get_mean_reversion_strategies should return all MR strategies."""
        strategies = get_mean_reversion_strategies()

        assert len(strategies) == 1
        assert strategies[0].name == "vwap_reclaim"

    def test_get_all_strategies(self):
        """get_all_strategies should return all strategies."""
        strategies = get_all_strategies()

        assert len(strategies) == 3
        names = [s.name for s in strategies]
        assert "first_pullback" in names
        assert "hod_continuation" in names
        assert "vwap_reclaim" in names
