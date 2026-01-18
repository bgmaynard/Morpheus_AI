"""
Tests for Feature Engine.

Phase 3 test coverage:
- FeatureContext creation and immutability
- Feature computation
- History management
- Warmup detection
- Event generation
"""

import pytest
from datetime import datetime, timezone

from morpheus.features.indicators import OHLCV
from morpheus.features.feature_engine import (
    FEATURE_SCHEMA_VERSION,
    MIN_BARS_REQUIRED,
    FeatureContext,
    FeatureEngine,
    create_feature_context,
    candle_to_ohlcv,
)
from morpheus.data.market_snapshot import create_snapshot, Candle
from morpheus.core.events import EventType


class TestFeatureContext:
    """Tests for FeatureContext dataclass."""

    def test_feature_context_creation(self):
        """FeatureContext should store all fields."""
        ctx = FeatureContext(
            symbol="SPY",
            features={"sma_20": 450.0, "rsi_14": 55.0},
            bars_available=100,
            warmup_complete=True,
        )

        assert ctx.symbol == "SPY"
        assert ctx.features["sma_20"] == 450.0
        assert ctx.bars_available == 100
        assert ctx.warmup_complete is True

    def test_feature_context_is_frozen(self):
        """FeatureContext should be immutable."""
        ctx = FeatureContext(symbol="SPY")

        with pytest.raises(Exception):
            ctx.symbol = "QQQ"

    def test_feature_context_default_values(self):
        """FeatureContext should have sensible defaults."""
        ctx = FeatureContext()

        assert ctx.schema_version == FEATURE_SCHEMA_VERSION
        assert ctx.symbol == ""
        assert ctx.features == {}
        assert ctx.regime is None
        assert ctx.regime_confidence == 0.0
        assert ctx.allowed_strategies == ()
        assert ctx.warmup_complete is False

    def test_feature_context_get_feature(self):
        """get_feature should return feature value or None."""
        ctx = FeatureContext(features={"sma_20": 450.0, "rsi_14": None})

        assert ctx.get_feature("sma_20") == 450.0
        assert ctx.get_feature("rsi_14") is None
        assert ctx.get_feature("nonexistent") is None

    def test_feature_context_has_feature(self):
        """has_feature should check for non-None value."""
        ctx = FeatureContext(features={"sma_20": 450.0, "rsi_14": None})

        assert ctx.has_feature("sma_20") is True
        assert ctx.has_feature("rsi_14") is False
        assert ctx.has_feature("nonexistent") is False

    def test_feature_context_to_event_payload(self):
        """to_event_payload should serialize correctly."""
        ctx = FeatureContext(
            symbol="SPY",
            features={"sma_20": 450.0},
            regime="trending_up",
            regime_confidence=0.85,
            bars_available=100,
        )

        payload = ctx.to_event_payload()

        assert payload["symbol"] == "SPY"
        assert payload["features"]["sma_20"] == 450.0
        assert payload["regime"] == "trending_up"
        assert payload["regime_confidence"] == 0.85

    def test_feature_context_to_event(self):
        """to_event should create FEATURES_COMPUTED event."""
        ctx = FeatureContext(symbol="SPY")

        event = ctx.to_event()

        assert event.event_type == EventType.FEATURES_COMPUTED
        assert event.symbol == "SPY"


class TestCandleToOHLCV:
    """Tests for candle conversion."""

    def test_candle_to_ohlcv(self):
        """candle_to_ohlcv should convert Candle to OHLCV."""
        candle = Candle(
            timestamp=datetime.now(timezone.utc),
            open=100.0,
            high=105.0,
            low=95.0,
            close=102.0,
            volume=1000000,
        )

        ohlcv = candle_to_ohlcv(candle)

        assert ohlcv.open == 100.0
        assert ohlcv.high == 105.0
        assert ohlcv.low == 95.0
        assert ohlcv.close == 102.0
        assert ohlcv.volume == 1000000


class TestFeatureEngine:
    """Tests for FeatureEngine class."""

    def _create_bar(self, close: float = 100.0, volume: int = 1000) -> OHLCV:
        """Create a test bar."""
        return OHLCV(
            open=close - 1,
            high=close + 2,
            low=close - 2,
            close=close,
            volume=volume,
        )

    def _create_bars(self, n: int, start_price: float = 100.0) -> list[OHLCV]:
        """Create n test bars."""
        return [self._create_bar(start_price + i * 0.1) for i in range(n)]

    def test_engine_initialization(self):
        """Engine should initialize with default values."""
        engine = FeatureEngine()

        assert engine.max_bars == 200
        assert engine.min_bars == MIN_BARS_REQUIRED

    def test_engine_custom_params(self):
        """Engine should accept custom parameters."""
        engine = FeatureEngine(max_bars=100, min_bars=25)

        assert engine.max_bars == 100
        assert engine.min_bars == 25

    def test_add_bar(self):
        """add_bar should store bars in history."""
        engine = FeatureEngine()
        bar = self._create_bar()

        engine.add_bar("SPY", bar)

        assert engine.bars_available("SPY") == 1
        assert engine.get_bars("SPY")[0] == bar

    def test_add_candle(self):
        """add_candle should convert and store candles."""
        engine = FeatureEngine()
        candle = Candle(
            timestamp=datetime.now(timezone.utc),
            open=100.0,
            high=105.0,
            low=95.0,
            close=102.0,
            volume=1000,
        )

        engine.add_candle("SPY", candle)

        assert engine.bars_available("SPY") == 1

    def test_history_trimming(self):
        """Engine should trim history to max_bars."""
        engine = FeatureEngine(max_bars=10)

        for i in range(20):
            engine.add_bar("SPY", self._create_bar(100.0 + i))

        assert engine.bars_available("SPY") == 10
        # Should keep most recent
        closes = engine.get_closes("SPY")
        assert closes[0] == 110.0  # Started at 110 (100+10)

    def test_get_closes(self):
        """get_closes should return close prices."""
        engine = FeatureEngine()
        bars = self._create_bars(5, 100.0)

        for bar in bars:
            engine.add_bar("SPY", bar)

        closes = engine.get_closes("SPY")
        assert len(closes) == 5

    def test_get_volumes(self):
        """get_volumes should return volumes."""
        engine = FeatureEngine()
        bars = self._create_bars(5)

        for bar in bars:
            engine.add_bar("SPY", bar)

        volumes = engine.get_volumes("SPY")
        assert len(volumes) == 5

    def test_warmup_detection(self):
        """warmup_complete should check min_bars."""
        engine = FeatureEngine(min_bars=10)

        for i in range(5):
            engine.add_bar("SPY", self._create_bar())

        assert engine.warmup_complete("SPY") is False

        for i in range(10):
            engine.add_bar("SPY", self._create_bar())

        assert engine.warmup_complete("SPY") is True

    def test_reset_symbol(self):
        """reset_symbol should clear history for symbol."""
        engine = FeatureEngine()

        for i in range(10):
            engine.add_bar("SPY", self._create_bar())
            engine.add_bar("QQQ", self._create_bar())

        engine.reset_symbol("SPY")

        assert engine.bars_available("SPY") == 0
        assert engine.bars_available("QQQ") == 10

    def test_reset_all(self):
        """reset_all should clear all history."""
        engine = FeatureEngine()

        for i in range(10):
            engine.add_bar("SPY", self._create_bar())
            engine.add_bar("QQQ", self._create_bar())

        engine.reset_all()

        assert engine.bars_available("SPY") == 0
        assert engine.bars_available("QQQ") == 0


class TestFeatureComputation:
    """Tests for feature computation."""

    def _populate_engine(self, engine: FeatureEngine, symbol: str = "SPY") -> None:
        """Populate engine with sufficient data."""
        for i in range(60):
            bar = OHLCV(
                open=100.0 + i * 0.1,
                high=102.0 + i * 0.1,
                low=98.0 + i * 0.1,
                close=101.0 + i * 0.1,
                volume=1000000 + i * 1000,
            )
            engine.add_bar(symbol, bar)

    def test_compute_features_basic(self):
        """compute_features should return FeatureContext."""
        engine = FeatureEngine()
        self._populate_engine(engine)

        ctx = engine.compute_features("SPY")

        assert isinstance(ctx, FeatureContext)
        assert ctx.symbol == "SPY"
        assert ctx.warmup_complete is True
        assert ctx.bars_available == 60

    def test_compute_features_sma(self):
        """compute_features should calculate SMAs."""
        engine = FeatureEngine()
        self._populate_engine(engine)

        ctx = engine.compute_features("SPY")

        assert ctx.has_feature("sma_20")
        assert ctx.has_feature("sma_50")
        assert ctx.features["sma_20"] is not None
        assert ctx.features["sma_50"] is not None

    def test_compute_features_momentum(self):
        """compute_features should calculate momentum indicators."""
        engine = FeatureEngine()
        self._populate_engine(engine)

        ctx = engine.compute_features("SPY")

        assert ctx.has_feature("rsi_14")
        assert ctx.has_feature("macd_line")
        assert ctx.has_feature("momentum_10")

        # RSI should be in valid range
        rsi = ctx.features["rsi_14"]
        assert rsi is not None
        assert 0 <= rsi <= 100

    def test_compute_features_volatility(self):
        """compute_features should calculate volatility indicators."""
        engine = FeatureEngine()
        self._populate_engine(engine)

        ctx = engine.compute_features("SPY")

        assert ctx.has_feature("atr_14")
        assert ctx.has_feature("atr_pct")
        assert ctx.has_feature("realized_vol_20")
        assert ctx.has_feature("bb_upper")

    def test_compute_features_insufficient_data(self):
        """compute_features should handle insufficient data."""
        engine = FeatureEngine()

        # Only add a few bars
        for i in range(5):
            engine.add_bar("SPY", OHLCV(100, 102, 98, 101, 1000))

        ctx = engine.compute_features("SPY")

        assert ctx.warmup_complete is False
        # Most features should be None
        assert ctx.features["sma_50"] is None
        assert ctx.features["rsi_14"] is None

    def test_compute_features_with_snapshot(self):
        """compute_features should use snapshot for current price."""
        engine = FeatureEngine()
        self._populate_engine(engine)

        snapshot = create_snapshot(
            symbol="SPY",
            bid=150.0,
            ask=150.10,
            last=150.05,
            volume=5000000,
        )

        ctx = engine.compute_features("SPY", snapshot)

        assert ctx.snapshot == snapshot
        assert ctx.timestamp == snapshot.timestamp

    def test_features_are_deterministic(self):
        """Same input should produce same features."""
        engine = FeatureEngine()
        self._populate_engine(engine)

        ctx1 = engine.compute_features("SPY")
        ctx2 = engine.compute_features("SPY")

        # All features should be identical
        for key in ctx1.features:
            assert ctx1.features[key] == ctx2.features[key]


class TestCreateFeatureContext:
    """Tests for create_feature_context factory."""

    def test_create_feature_context(self):
        """Factory should create valid FeatureContext."""
        snapshot = create_snapshot(
            symbol="SPY",
            bid=450.0,
            ask=450.10,
            last=450.05,
            volume=5000000,
        )

        ctx = create_feature_context(
            symbol="SPY",
            snapshot=snapshot,
            features={"sma_20": 448.0, "rsi_14": 55.0},
            bars_available=100,
            warmup_complete=True,
        )

        assert ctx.symbol == "SPY"
        assert ctx.snapshot == snapshot
        assert ctx.features["sma_20"] == 448.0
        assert ctx.bars_available == 100
        assert ctx.warmup_complete is True


class TestFeatureEngineIsolation:
    """Tests verifying feature engine is isolated from other concerns."""

    def test_feature_engine_has_no_trade_imports(self):
        """Feature engine should not import trade modules."""
        from morpheus.features import feature_engine

        source = open(feature_engine.__file__).read()

        assert "trade_fsm" not in source
        assert "TradeLifecycle" not in source

    def test_feature_engine_has_no_broker_imports(self):
        """Feature engine should not import broker modules."""
        from morpheus.features import feature_engine

        source = open(feature_engine.__file__).read()

        assert "schwab_auth" not in source
        assert "schwab_market" not in source
