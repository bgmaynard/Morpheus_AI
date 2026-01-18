"""
Regime Detector - Classifies market regime from features.

Determines if market is trending, ranging, or volatile based on
computed features from the Feature Engine.

All classifications are DETERMINISTIC: same features -> same regime.

Phase 3 Scope:
- Classification only
- No trade decisions
- No state mutation
- Consumes FeatureContext (read-only)
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from morpheus.features.feature_engine import FeatureContext


class TrendRegime(Enum):
    """Trend direction regime."""

    TRENDING_UP = "trending_up"
    TRENDING_DOWN = "trending_down"
    RANGING = "ranging"
    UNKNOWN = "unknown"


class VolatilityRegime(Enum):
    """Volatility level regime."""

    HIGH_VOLATILITY = "high_volatility"
    NORMAL_VOLATILITY = "normal_volatility"
    LOW_VOLATILITY = "low_volatility"
    UNKNOWN = "unknown"


class MomentumRegime(Enum):
    """Momentum state regime."""

    STRONG_BULLISH = "strong_bullish"
    BULLISH = "bullish"
    NEUTRAL = "neutral"
    BEARISH = "bearish"
    STRONG_BEARISH = "strong_bearish"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class RegimeClassification:
    """
    Complete regime classification result.

    Contains:
    - Primary regime (combined label)
    - Component regimes (trend, volatility, momentum)
    - Confidence scores
    - Supporting feature values
    """

    # Primary combined regime label
    primary_regime: str

    # Component regimes
    trend: TrendRegime
    volatility: VolatilityRegime
    momentum: MomentumRegime

    # Confidence (0-1)
    confidence: float

    # Supporting data
    trend_confidence: float = 0.0
    volatility_confidence: float = 0.0
    momentum_confidence: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "primary_regime": self.primary_regime,
            "trend": self.trend.value,
            "volatility": self.volatility.value,
            "momentum": self.momentum.value,
            "confidence": self.confidence,
            "trend_confidence": self.trend_confidence,
            "volatility_confidence": self.volatility_confidence,
            "momentum_confidence": self.momentum_confidence,
        }


@dataclass(frozen=True)
class RegimeThresholds:
    """
    Configurable thresholds for regime detection.

    All thresholds are designed for deterministic classification.
    """

    # Trend thresholds
    price_vs_sma_trend_threshold: float = 1.0  # % above/below SMA for trend
    trend_strength_threshold: float = 0.3  # R-squared for strong trend
    trend_direction_threshold: float = 0.5  # Normalized slope threshold

    # Volatility thresholds (ATR%)
    high_volatility_threshold: float = 3.0  # ATR% above this = high vol
    low_volatility_threshold: float = 1.0  # ATR% below this = low vol

    # Momentum thresholds
    rsi_overbought: float = 70.0
    rsi_oversold: float = 30.0
    rsi_strong_overbought: float = 80.0
    rsi_strong_oversold: float = 20.0

    # Mean reversion thresholds (z-score based)
    mean_reversion_extreme: float = 2.0  # Std devs for extreme


class RegimeDetector:
    """
    Detects market regime from computed features.

    Uses rule-based classification with configurable thresholds.
    All logic is deterministic - same features always produce same regime.
    """

    def __init__(self, thresholds: RegimeThresholds | None = None):
        """
        Initialize the regime detector.

        Args:
            thresholds: Optional custom thresholds (defaults to standard)
        """
        self.thresholds = thresholds or RegimeThresholds()

    def classify(self, context: FeatureContext) -> RegimeClassification:
        """
        Classify the market regime from features.

        Args:
            context: FeatureContext with computed features

        Returns:
            RegimeClassification with all regime components
        """
        features = context.features

        # Classify each component
        trend, trend_conf = self._classify_trend(features)
        volatility, vol_conf = self._classify_volatility(features)
        momentum, mom_conf = self._classify_momentum(features)

        # Build primary regime label
        primary = self._build_primary_regime(trend, volatility, momentum)

        # Overall confidence is weighted average
        confidence = self._compute_confidence(trend_conf, vol_conf, mom_conf)

        return RegimeClassification(
            primary_regime=primary,
            trend=trend,
            volatility=volatility,
            momentum=momentum,
            confidence=confidence,
            trend_confidence=trend_conf,
            volatility_confidence=vol_conf,
            momentum_confidence=mom_conf,
        )

    def _classify_trend(
        self, features: dict[str, float | None]
    ) -> tuple[TrendRegime, float]:
        """
        Classify trend regime.

        Uses:
        - Price vs SMA (primary signal)
        - Trend strength (R-squared)
        - Trend direction (slope)
        """
        price_vs_sma = features.get("price_vs_sma20")
        trend_strength = features.get("trend_strength")
        trend_direction = features.get("trend_direction")

        if price_vs_sma is None:
            return TrendRegime.UNKNOWN, 0.0

        t = self.thresholds

        # Determine trend direction
        if price_vs_sma > t.price_vs_sma_trend_threshold:
            regime = TrendRegime.TRENDING_UP
        elif price_vs_sma < -t.price_vs_sma_trend_threshold:
            regime = TrendRegime.TRENDING_DOWN
        else:
            regime = TrendRegime.RANGING

        # Calculate confidence
        confidence = 0.5  # Base confidence

        # Boost confidence if trend strength supports it
        if trend_strength is not None and trend_strength > t.trend_strength_threshold:
            confidence += 0.25

        # Boost confidence if trend direction aligns
        if trend_direction is not None:
            if regime == TrendRegime.TRENDING_UP and trend_direction > t.trend_direction_threshold:
                confidence += 0.25
            elif regime == TrendRegime.TRENDING_DOWN and trend_direction < -t.trend_direction_threshold:
                confidence += 0.25
            elif regime == TrendRegime.RANGING and abs(trend_direction) < t.trend_direction_threshold:
                confidence += 0.25

        return regime, min(1.0, confidence)

    def _classify_volatility(
        self, features: dict[str, float | None]
    ) -> tuple[VolatilityRegime, float]:
        """
        Classify volatility regime.

        Uses:
        - ATR percentage (primary signal)
        - Realized volatility (secondary)
        """
        atr_pct = features.get("atr_pct")
        realized_vol = features.get("realized_vol_20")

        if atr_pct is None:
            return VolatilityRegime.UNKNOWN, 0.0

        t = self.thresholds

        # Classify based on ATR%
        if atr_pct > t.high_volatility_threshold:
            regime = VolatilityRegime.HIGH_VOLATILITY
            # Confidence based on how far above threshold
            confidence = min(1.0, 0.5 + (atr_pct - t.high_volatility_threshold) / 4.0)
        elif atr_pct < t.low_volatility_threshold:
            regime = VolatilityRegime.LOW_VOLATILITY
            # Confidence based on how far below threshold
            confidence = min(1.0, 0.5 + (t.low_volatility_threshold - atr_pct) / 2.0)
        else:
            regime = VolatilityRegime.NORMAL_VOLATILITY
            # Confidence highest when in middle of range
            mid = (t.high_volatility_threshold + t.low_volatility_threshold) / 2
            distance_from_mid = abs(atr_pct - mid)
            range_half = (t.high_volatility_threshold - t.low_volatility_threshold) / 2
            confidence = 0.5 + 0.5 * (1 - distance_from_mid / range_half)

        return regime, confidence

    def _classify_momentum(
        self, features: dict[str, float | None]
    ) -> tuple[MomentumRegime, float]:
        """
        Classify momentum regime.

        Uses:
        - RSI (primary signal)
        - MACD histogram (secondary)
        - Momentum score (tertiary)
        """
        rsi = features.get("rsi_14")
        macd_hist = features.get("macd_histogram")
        momentum_score = features.get("momentum_score")

        if rsi is None:
            return MomentumRegime.UNKNOWN, 0.0

        t = self.thresholds

        # Classify based on RSI
        if rsi >= t.rsi_strong_overbought:
            regime = MomentumRegime.STRONG_BULLISH
            confidence = 0.7
        elif rsi >= t.rsi_overbought:
            regime = MomentumRegime.BULLISH
            confidence = 0.6
        elif rsi <= t.rsi_strong_oversold:
            regime = MomentumRegime.STRONG_BEARISH
            confidence = 0.7
        elif rsi <= t.rsi_oversold:
            regime = MomentumRegime.BEARISH
            confidence = 0.6
        else:
            regime = MomentumRegime.NEUTRAL
            # Confidence highest at RSI 50
            confidence = 0.5 + 0.3 * (1 - abs(rsi - 50) / 20)

        # Boost confidence if MACD aligns
        if macd_hist is not None:
            if regime in (MomentumRegime.STRONG_BULLISH, MomentumRegime.BULLISH) and macd_hist > 0:
                confidence = min(1.0, confidence + 0.15)
            elif regime in (MomentumRegime.STRONG_BEARISH, MomentumRegime.BEARISH) and macd_hist < 0:
                confidence = min(1.0, confidence + 0.15)
            elif regime == MomentumRegime.NEUTRAL and abs(macd_hist) < 0.1:
                confidence = min(1.0, confidence + 0.15)

        return regime, confidence

    def _build_primary_regime(
        self,
        trend: TrendRegime,
        volatility: VolatilityRegime,
        momentum: MomentumRegime,
    ) -> str:
        """
        Build a primary regime label from components.

        Format: "{volatility}_{trend}" or simplified labels.
        """
        # Handle unknown cases
        if trend == TrendRegime.UNKNOWN:
            return "unknown"

        # Build label based on dominant characteristics
        vol_prefix = ""
        if volatility == VolatilityRegime.HIGH_VOLATILITY:
            vol_prefix = "volatile_"
        elif volatility == VolatilityRegime.LOW_VOLATILITY:
            vol_prefix = "quiet_"

        trend_label = trend.value

        return f"{vol_prefix}{trend_label}"

    def _compute_confidence(
        self, trend_conf: float, vol_conf: float, mom_conf: float
    ) -> float:
        """
        Compute overall confidence from component confidences.

        Weighted average: trend (40%), volatility (30%), momentum (30%)
        """
        return 0.4 * trend_conf + 0.3 * vol_conf + 0.3 * mom_conf


def classify_regime(
    context: FeatureContext,
    thresholds: RegimeThresholds | None = None,
) -> RegimeClassification:
    """
    Convenience function to classify regime from a FeatureContext.

    Args:
        context: FeatureContext with computed features
        thresholds: Optional custom thresholds

    Returns:
        RegimeClassification
    """
    detector = RegimeDetector(thresholds)
    return detector.classify(context)


def update_feature_context_with_regime(
    context: FeatureContext,
    classification: RegimeClassification,
) -> FeatureContext:
    """
    Create a new FeatureContext with regime information.

    Since FeatureContext is frozen, this creates a new instance.

    Args:
        context: Original FeatureContext
        classification: RegimeClassification result

    Returns:
        New FeatureContext with regime fields populated
    """
    from dataclasses import replace

    return replace(
        context,
        regime=classification.primary_regime,
        regime_confidence=classification.confidence,
        regime_components={
            "trend": classification.trend.value,
            "volatility": classification.volatility.value,
            "momentum": classification.momentum.value,
        },
    )
