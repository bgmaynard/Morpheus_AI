"""
Tests for Regime Detector.

Phase 3 test coverage:
- Trend regime classification
- Volatility regime classification
- Momentum regime classification
- Combined regime labels
- Confidence calculation
- Determinism
"""

import pytest
from datetime import datetime, timezone

from morpheus.features.feature_engine import FeatureContext
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


class TestRegimeEnums:
    """Tests for regime enums."""

    def test_trend_regime_values(self):
        """TrendRegime should have expected values."""
        assert TrendRegime.TRENDING_UP.value == "trending_up"
        assert TrendRegime.TRENDING_DOWN.value == "trending_down"
        assert TrendRegime.RANGING.value == "ranging"
        assert TrendRegime.UNKNOWN.value == "unknown"

    def test_volatility_regime_values(self):
        """VolatilityRegime should have expected values."""
        assert VolatilityRegime.HIGH_VOLATILITY.value == "high_volatility"
        assert VolatilityRegime.NORMAL_VOLATILITY.value == "normal_volatility"
        assert VolatilityRegime.LOW_VOLATILITY.value == "low_volatility"

    def test_momentum_regime_values(self):
        """MomentumRegime should have expected values."""
        assert MomentumRegime.STRONG_BULLISH.value == "strong_bullish"
        assert MomentumRegime.BEARISH.value == "bearish"
        assert MomentumRegime.NEUTRAL.value == "neutral"


class TestRegimeClassification:
    """Tests for RegimeClassification dataclass."""

    def test_classification_creation(self):
        """RegimeClassification should store all fields."""
        classification = RegimeClassification(
            primary_regime="trending_up",
            trend=TrendRegime.TRENDING_UP,
            volatility=VolatilityRegime.NORMAL_VOLATILITY,
            momentum=MomentumRegime.BULLISH,
            confidence=0.85,
            trend_confidence=0.9,
            volatility_confidence=0.8,
            momentum_confidence=0.85,
        )

        assert classification.primary_regime == "trending_up"
        assert classification.trend == TrendRegime.TRENDING_UP
        assert classification.confidence == 0.85

    def test_classification_is_frozen(self):
        """RegimeClassification should be immutable."""
        classification = RegimeClassification(
            primary_regime="ranging",
            trend=TrendRegime.RANGING,
            volatility=VolatilityRegime.LOW_VOLATILITY,
            momentum=MomentumRegime.NEUTRAL,
            confidence=0.7,
        )

        with pytest.raises(Exception):
            classification.primary_regime = "trending_up"

    def test_classification_to_dict(self):
        """to_dict should serialize correctly."""
        classification = RegimeClassification(
            primary_regime="volatile_trending_up",
            trend=TrendRegime.TRENDING_UP,
            volatility=VolatilityRegime.HIGH_VOLATILITY,
            momentum=MomentumRegime.STRONG_BULLISH,
            confidence=0.9,
        )

        data = classification.to_dict()

        assert data["primary_regime"] == "volatile_trending_up"
        assert data["trend"] == "trending_up"
        assert data["volatility"] == "high_volatility"
        assert data["momentum"] == "strong_bullish"
        assert data["confidence"] == 0.9


class TestRegimeThresholds:
    """Tests for RegimeThresholds configuration."""

    def test_default_thresholds(self):
        """Default thresholds should have sensible values."""
        thresholds = RegimeThresholds()

        assert thresholds.price_vs_sma_trend_threshold == 1.0
        assert thresholds.high_volatility_threshold == 3.0
        assert thresholds.low_volatility_threshold == 1.0
        assert thresholds.rsi_overbought == 70.0
        assert thresholds.rsi_oversold == 30.0

    def test_custom_thresholds(self):
        """Custom thresholds should be accepted."""
        thresholds = RegimeThresholds(
            price_vs_sma_trend_threshold=2.0,
            high_volatility_threshold=4.0,
        )

        assert thresholds.price_vs_sma_trend_threshold == 2.0
        assert thresholds.high_volatility_threshold == 4.0


class TestRegimeDetector:
    """Tests for RegimeDetector class."""

    def _create_context(self, **features) -> FeatureContext:
        """Create a FeatureContext with given features."""
        return FeatureContext(
            symbol="TEST",
            features=features,
            bars_available=100,
            warmup_complete=True,
        )

    def test_detector_initialization(self):
        """Detector should initialize with default thresholds."""
        detector = RegimeDetector()

        assert detector.thresholds is not None

    def test_detector_custom_thresholds(self):
        """Detector should accept custom thresholds."""
        thresholds = RegimeThresholds(rsi_overbought=75.0)
        detector = RegimeDetector(thresholds)

        assert detector.thresholds.rsi_overbought == 75.0

    def test_classify_trending_up(self):
        """Detector should identify uptrend."""
        detector = RegimeDetector()

        ctx = self._create_context(
            price_vs_sma20=2.5,  # Above threshold
            trend_strength=0.8,
            trend_direction=1.0,
            atr_pct=2.0,
            rsi_14=60.0,
        )

        result = detector.classify(ctx)

        assert result.trend == TrendRegime.TRENDING_UP
        assert "trending_up" in result.primary_regime

    def test_classify_trending_down(self):
        """Detector should identify downtrend."""
        detector = RegimeDetector()

        ctx = self._create_context(
            price_vs_sma20=-2.5,  # Below threshold
            trend_strength=0.8,
            trend_direction=-1.0,
            atr_pct=2.0,
            rsi_14=40.0,
        )

        result = detector.classify(ctx)

        assert result.trend == TrendRegime.TRENDING_DOWN
        assert "trending_down" in result.primary_regime

    def test_classify_ranging(self):
        """Detector should identify ranging market."""
        detector = RegimeDetector()

        ctx = self._create_context(
            price_vs_sma20=0.3,  # Within threshold
            trend_strength=0.2,
            trend_direction=0.1,
            atr_pct=2.0,
            rsi_14=50.0,
        )

        result = detector.classify(ctx)

        assert result.trend == TrendRegime.RANGING

    def test_classify_high_volatility(self):
        """Detector should identify high volatility."""
        detector = RegimeDetector()

        ctx = self._create_context(
            price_vs_sma20=1.5,
            atr_pct=5.0,  # Above high threshold
            rsi_14=55.0,
        )

        result = detector.classify(ctx)

        assert result.volatility == VolatilityRegime.HIGH_VOLATILITY
        assert "volatile" in result.primary_regime

    def test_classify_low_volatility(self):
        """Detector should identify low volatility."""
        detector = RegimeDetector()

        ctx = self._create_context(
            price_vs_sma20=0.5,
            atr_pct=0.5,  # Below low threshold
            rsi_14=50.0,
        )

        result = detector.classify(ctx)

        assert result.volatility == VolatilityRegime.LOW_VOLATILITY
        assert "quiet" in result.primary_regime

    def test_classify_strong_bullish_momentum(self):
        """Detector should identify strong bullish momentum."""
        detector = RegimeDetector()

        ctx = self._create_context(
            price_vs_sma20=2.0,
            atr_pct=2.0,
            rsi_14=85.0,  # Above strong overbought
            macd_histogram=0.5,
        )

        result = detector.classify(ctx)

        assert result.momentum == MomentumRegime.STRONG_BULLISH

    def test_classify_strong_bearish_momentum(self):
        """Detector should identify strong bearish momentum."""
        detector = RegimeDetector()

        ctx = self._create_context(
            price_vs_sma20=-2.0,
            atr_pct=2.0,
            rsi_14=15.0,  # Below strong oversold
            macd_histogram=-0.5,
        )

        result = detector.classify(ctx)

        assert result.momentum == MomentumRegime.STRONG_BEARISH

    def test_classify_neutral_momentum(self):
        """Detector should identify neutral momentum."""
        detector = RegimeDetector()

        ctx = self._create_context(
            price_vs_sma20=0.5,
            atr_pct=2.0,
            rsi_14=50.0,  # Right in the middle
        )

        result = detector.classify(ctx)

        assert result.momentum == MomentumRegime.NEUTRAL

    def test_classify_unknown_with_missing_features(self):
        """Detector should return unknown if key features missing."""
        detector = RegimeDetector()

        ctx = self._create_context()  # No features

        result = detector.classify(ctx)

        assert result.trend == TrendRegime.UNKNOWN
        assert result.primary_regime == "unknown"

    def test_confidence_calculation(self):
        """Confidence should be weighted average of components."""
        detector = RegimeDetector()

        ctx = self._create_context(
            price_vs_sma20=2.5,
            trend_strength=0.9,
            trend_direction=1.5,
            atr_pct=2.0,
            rsi_14=65.0,
        )

        result = detector.classify(ctx)

        # Confidence should be positive
        assert result.confidence > 0
        assert result.confidence <= 1.0

        # Component confidences should be tracked
        assert result.trend_confidence > 0
        assert result.volatility_confidence > 0
        assert result.momentum_confidence > 0


class TestClassifyRegimeFunction:
    """Tests for classify_regime convenience function."""

    def test_classify_regime(self):
        """classify_regime should work like detector.classify."""
        ctx = FeatureContext(
            symbol="SPY",
            features={
                "price_vs_sma20": 2.0,
                "atr_pct": 2.5,
                "rsi_14": 60.0,
            },
        )

        result = classify_regime(ctx)

        assert isinstance(result, RegimeClassification)

    def test_classify_regime_with_custom_thresholds(self):
        """classify_regime should accept custom thresholds."""
        ctx = FeatureContext(
            symbol="SPY",
            features={"price_vs_sma20": 1.5, "atr_pct": 2.0, "rsi_14": 55.0},
        )

        thresholds = RegimeThresholds(price_vs_sma_trend_threshold=0.5)
        result = classify_regime(ctx, thresholds)

        # With lower threshold, 1.5% should be trending
        assert result.trend == TrendRegime.TRENDING_UP


class TestUpdateFeatureContext:
    """Tests for update_feature_context_with_regime."""

    def test_update_adds_regime_info(self):
        """Update should add regime info to context."""
        ctx = FeatureContext(
            symbol="SPY",
            features={"sma_20": 450.0},
        )

        classification = RegimeClassification(
            primary_regime="volatile_trending_up",
            trend=TrendRegime.TRENDING_UP,
            volatility=VolatilityRegime.HIGH_VOLATILITY,
            momentum=MomentumRegime.BULLISH,
            confidence=0.85,
        )

        updated = update_feature_context_with_regime(ctx, classification)

        assert updated.regime == "volatile_trending_up"
        assert updated.regime_confidence == 0.85
        assert updated.regime_components["trend"] == "trending_up"
        assert updated.regime_components["volatility"] == "high_volatility"

    def test_update_preserves_original(self):
        """Update should not modify original context."""
        ctx = FeatureContext(
            symbol="SPY",
            features={"sma_20": 450.0},
        )

        classification = RegimeClassification(
            primary_regime="ranging",
            trend=TrendRegime.RANGING,
            volatility=VolatilityRegime.NORMAL_VOLATILITY,
            momentum=MomentumRegime.NEUTRAL,
            confidence=0.7,
        )

        updated = update_feature_context_with_regime(ctx, classification)

        # Original should be unchanged
        assert ctx.regime is None
        # Updated should have regime
        assert updated.regime == "ranging"


class TestRegimeDetectorDeterminism:
    """Tests verifying regime detection is deterministic."""

    def test_same_features_same_regime(self):
        """Same features should always produce same regime."""
        detector = RegimeDetector()

        features = {
            "price_vs_sma20": 2.5,
            "trend_strength": 0.8,
            "trend_direction": 1.2,
            "atr_pct": 2.5,
            "rsi_14": 65.0,
            "macd_histogram": 0.3,
        }

        ctx = FeatureContext(symbol="SPY", features=features)

        result1 = detector.classify(ctx)
        result2 = detector.classify(ctx)

        assert result1.primary_regime == result2.primary_regime
        assert result1.confidence == result2.confidence
        assert result1.trend == result2.trend
        assert result1.volatility == result2.volatility
        assert result1.momentum == result2.momentum


class TestRegimeDetectorIsolation:
    """Tests verifying regime detector is isolated."""

    def test_regime_detector_has_no_trade_imports(self):
        """Regime detector should not import trade modules."""
        from morpheus.regime import regime_detector

        source = open(regime_detector.__file__).read()

        assert "trade_fsm" not in source
        assert "TradeLifecycle" not in source

    def test_regime_detector_has_no_broker_imports(self):
        """Regime detector should not import broker modules."""
        from morpheus.regime import regime_detector

        source = open(regime_detector.__file__).read()

        assert "schwab_auth" not in source
        assert "schwab_market" not in source
