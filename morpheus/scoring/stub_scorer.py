"""
Stub Scorer - Rule-based deterministic signal scorer.

This is the initial implementation of a Scorer that uses
simple rule-based logic to assign confidence scores.
Designed to be replaced with ML models in future phases.

All scoring is DETERMINISTIC: same input -> same score.

Phase 5 Scope:
- Signal scoring only
- No position sizing
- No risk overlays
- No order execution
"""

from __future__ import annotations

from dataclasses import dataclass

from morpheus.features.feature_engine import FeatureContext
from morpheus.strategies.base import SignalCandidate, SignalDirection
from morpheus.scoring.base import Scorer, ScoredSignal


@dataclass(frozen=True)
class StubScorerConfig:
    """
    Configuration for the stub scorer.

    Defines weights and thresholds for rule-based scoring.
    """

    # Base confidence for any signal
    base_confidence: float = 0.3

    # Bonus for regime alignment
    regime_alignment_bonus: float = 0.15

    # Bonus for strong trend
    trend_strength_bonus: float = 0.1
    trend_strength_threshold: float = 0.5

    # Bonus for favorable RSI
    rsi_bonus: float = 0.1
    rsi_oversold_threshold: float = 35.0
    rsi_overbought_threshold: float = 65.0

    # Bonus for volume confirmation
    volume_bonus: float = 0.1
    volume_threshold: float = 1.2

    # Bonus for MACD alignment
    macd_bonus: float = 0.1

    # Penalty for extreme conditions
    extreme_rsi_penalty: float = 0.15
    rsi_extreme_low: float = 20.0
    rsi_extreme_high: float = 80.0

    # Penalty for high volatility
    high_volatility_penalty: float = 0.1
    volatility_threshold: float = 3.5


class StubScorer(Scorer):
    """
    Rule-based signal scorer.

    Uses a combination of feature checks and bonuses/penalties
    to compute a confidence score. Designed to be deterministic
    and replaceable with ML models.

    Scoring factors:
    - Base confidence (starting point)
    - Regime alignment (signal direction matches regime)
    - Trend strength (strong trend = higher confidence)
    - RSI position (favorable = bonus, extreme = penalty)
    - Volume confirmation (above average = bonus)
    - MACD alignment (histogram supports direction)
    - Volatility (high = penalty for mean reversion)
    """

    def __init__(self, config: StubScorerConfig | None = None):
        """
        Initialize the stub scorer.

        Args:
            config: Optional custom configuration
        """
        self.config = config or StubScorerConfig()

    @property
    def name(self) -> str:
        return "stub_scorer"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return "Rule-based deterministic scorer using feature heuristics"

    def score(
        self,
        signal: SignalCandidate,
        features: FeatureContext,
    ) -> ScoredSignal:
        """
        Score a signal using rule-based logic.

        Args:
            signal: SignalCandidate to score
            features: FeatureContext with computed features

        Returns:
            ScoredSignal with confidence and rationale
        """
        # No signal = zero confidence
        if signal.direction == SignalDirection.NONE:
            return self.create_scored_signal(
                signal=signal,
                confidence=0.0,
                rationale="No actionable signal (direction=NONE)",
                contributing_factors=(),
            )

        cfg = self.config
        feat = features.features
        contributing: list[str] = []
        rationale_parts: list[str] = []

        # Start with base confidence
        confidence = cfg.base_confidence
        rationale_parts.append(f"Base: {cfg.base_confidence:.2f}")

        # Check regime alignment
        regime = features.regime or ""
        is_long = signal.direction == SignalDirection.LONG
        is_short = signal.direction == SignalDirection.SHORT

        regime_aligned = False
        if is_long and "trending_up" in regime:
            regime_aligned = True
        elif is_short and "trending_down" in regime:
            regime_aligned = True
        elif "ranging" in regime and signal.strategy_name in ("vwap_reclaim",):
            regime_aligned = True

        if regime_aligned:
            confidence += cfg.regime_alignment_bonus
            contributing.append("regime_alignment")
            rationale_parts.append(f"Regime aligned: +{cfg.regime_alignment_bonus:.2f}")

        # Check trend strength
        trend_strength = feat.get("trend_strength")
        if trend_strength is not None and trend_strength > cfg.trend_strength_threshold:
            confidence += cfg.trend_strength_bonus
            contributing.append("trend_strength")
            rationale_parts.append(f"Strong trend ({trend_strength:.2f}): +{cfg.trend_strength_bonus:.2f}")

        # Check RSI
        rsi = feat.get("rsi_14")
        if rsi is not None:
            # Favorable RSI for direction
            if is_long and rsi < cfg.rsi_oversold_threshold:
                confidence += cfg.rsi_bonus
                contributing.append("rsi_favorable")
                rationale_parts.append(f"RSI oversold ({rsi:.1f}): +{cfg.rsi_bonus:.2f}")
            elif is_short and rsi > cfg.rsi_overbought_threshold:
                confidence += cfg.rsi_bonus
                contributing.append("rsi_favorable")
                rationale_parts.append(f"RSI overbought ({rsi:.1f}): +{cfg.rsi_bonus:.2f}")

            # Penalty for extreme RSI (against the signal)
            if is_long and rsi > cfg.rsi_extreme_high:
                confidence -= cfg.extreme_rsi_penalty
                contributing.append("rsi_extreme_penalty")
                rationale_parts.append(f"RSI extreme high ({rsi:.1f}): -{cfg.extreme_rsi_penalty:.2f}")
            elif is_short and rsi < cfg.rsi_extreme_low:
                confidence -= cfg.extreme_rsi_penalty
                contributing.append("rsi_extreme_penalty")
                rationale_parts.append(f"RSI extreme low ({rsi:.1f}): -{cfg.extreme_rsi_penalty:.2f}")

        # Check volume
        relative_vol = feat.get("relative_volume")
        if relative_vol is not None and relative_vol > cfg.volume_threshold:
            confidence += cfg.volume_bonus
            contributing.append("volume_confirmation")
            rationale_parts.append(f"Volume elevated ({relative_vol:.2f}x): +{cfg.volume_bonus:.2f}")

        # Check MACD alignment
        macd_hist = feat.get("macd_histogram")
        if macd_hist is not None:
            if is_long and macd_hist > 0:
                confidence += cfg.macd_bonus
                contributing.append("macd_alignment")
                rationale_parts.append(f"MACD positive: +{cfg.macd_bonus:.2f}")
            elif is_short and macd_hist < 0:
                confidence += cfg.macd_bonus
                contributing.append("macd_alignment")
                rationale_parts.append(f"MACD negative: +{cfg.macd_bonus:.2f}")

        # Volatility penalty for mean reversion strategies
        atr_pct = feat.get("atr_pct")
        if atr_pct is not None and atr_pct > cfg.volatility_threshold:
            if "mean_reversion" in signal.tags or "vwap" in signal.tags:
                confidence -= cfg.high_volatility_penalty
                contributing.append("volatility_penalty")
                rationale_parts.append(f"High volatility ({atr_pct:.2f}%): -{cfg.high_volatility_penalty:.2f}")

        # Momentum engine bonus/penalty (if available)
        momentum_score = feat.get("momentum_engine_score")
        momentum_state = feat.get("momentum_engine_state_str", "")
        if momentum_score is not None:
            # Bonus: high momentum aligned with signal direction
            if momentum_score > 70:
                nofi = feat.get("momentum_nofi", 0)
                if (is_long and nofi > 0) or (is_short and nofi < 0):
                    confidence += 0.10
                    contributing.append("momentum_aligned")
                    rationale_parts.append(f"Momentum aligned (score={momentum_score:.0f}): +0.10")
            # Penalty: decaying momentum for new longs
            if momentum_state == "DECAYING" and is_long:
                confidence -= 0.10
                contributing.append("momentum_decaying_penalty")
                rationale_parts.append(f"Momentum decaying: -0.10")
            # Penalty: reversing momentum against direction
            if momentum_state == "REVERSING":
                confidence -= 0.05
                contributing.append("momentum_reversing_penalty")
                rationale_parts.append(f"Momentum reversing: -0.05")

        # Build feature snapshot for audit (include spread for risk evaluation)
        feature_snapshot = {
            "trend_strength": trend_strength,
            "rsi_14": rsi,
            "relative_volume": relative_vol,
            "macd_histogram": macd_hist,
            "atr_pct": atr_pct,
            # Momentum intelligence
            "momentum_engine_score": momentum_score,
            "momentum_engine_state_str": momentum_state,
            "momentum_engine_confidence": feat.get("momentum_engine_confidence"),
            "momentum_nofi": feat.get("momentum_nofi"),
            "momentum_l2_pressure": feat.get("momentum_l2_pressure"),
        }

        # Include spread data from snapshot for room-to-profit analysis
        if features.snapshot:
            feature_snapshot["spread"] = features.snapshot.spread
            feature_snapshot["spread_pct"] = features.snapshot.spread_pct
            feature_snapshot["bid"] = features.snapshot.bid
            feature_snapshot["ask"] = features.snapshot.ask

        # Clamp confidence to [0, 1]
        final_confidence = max(0.0, min(1.0, confidence))

        return self.create_scored_signal(
            signal=signal,
            confidence=final_confidence,
            rationale="; ".join(rationale_parts),
            contributing_factors=tuple(contributing),
            feature_snapshot=feature_snapshot,
        )


def create_stub_scorer(config: StubScorerConfig | None = None) -> StubScorer:
    """Factory function to create a StubScorer."""
    return StubScorer(config)
