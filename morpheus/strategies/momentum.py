"""
Momentum Strategies - Signal generation for momentum-based setups.

Strategies:
1. FirstPullback - Enter on first pullback in a trending market
2. HighOfDayContinuation - Enter on breakout above session high

All strategies are DETERMINISTIC: same input -> same output.

Phase 4 Scope:
- Signal candidate generation only
- No trade decisions
- No AI scoring
- No risk logic
- No execution
"""

from __future__ import annotations

from morpheus.strategies.base import (
    Strategy,
    StrategyContext,
    SignalCandidate,
    SignalDirection,
)


class FirstPullbackStrategy(Strategy):
    """
    First Pullback Strategy.

    Looks for the first pullback/consolidation after a strong move
    in a trending market. Generates a signal in the direction of
    the trend when price pulls back to a support level (e.g., EMA).

    Entry conditions:
    - Market is trending (up or down)
    - Price has pulled back toward EMA
    - RSI is not extreme (room to run)
    - Momentum still positive in trend direction

    This strategy is designed for trending regimes only.
    """

    @property
    def name(self) -> str:
        return "first_pullback"

    @property
    def description(self) -> str:
        return "Enter on first pullback to EMA in trending market"

    @property
    def required_features(self) -> tuple[str, ...]:
        return (
            "ema_9",
            "ema_21",
            "price_vs_sma20",
            "rsi_14",
            "trend_strength",
            "trend_direction",
        )

    @property
    def compatible_regimes(self) -> tuple[str, ...]:
        return ("trending_up", "trending_down")

    def evaluate(self, context: StrategyContext) -> SignalCandidate:
        """Evaluate first pullback setup."""
        if not self.can_evaluate(context):
            return self.create_no_signal(context)

        features = context.features.features
        snapshot = context.snapshot
        regime = context.features.regime or ""

        # Extract feature values
        ema_9 = features.get("ema_9")
        ema_21 = features.get("ema_21")
        price_vs_sma = features.get("price_vs_sma20")
        rsi = features.get("rsi_14")
        trend_strength = features.get("trend_strength")
        trend_direction = features.get("trend_direction")

        # Safety checks
        if any(v is None for v in [ema_9, ema_21, price_vs_sma, rsi, trend_strength, trend_direction]):
            return self.create_no_signal(context)

        current_price = snapshot.last

        # Determine trend direction from regime
        is_uptrend = "trending_up" in regime
        is_downtrend = "trending_down" in regime

        # Check for LONG setup in uptrend
        if is_uptrend:
            # Price should be near EMA (pulled back)
            # EMA9 > EMA21 (trend intact)
            # RSI not overbought (< 70)
            # Trend strength reasonable (> 0.3)
            price_near_ema = abs(current_price - ema_21) / ema_21 < 0.02  # Within 2%
            ema_aligned = ema_9 > ema_21
            rsi_ok = rsi < 70
            trend_ok = trend_strength > 0.3 and trend_direction > 0

            if price_near_ema and ema_aligned and rsi_ok and trend_ok:
                return self.create_signal(
                    context=context,
                    direction=SignalDirection.LONG,
                    rationale=f"First pullback to EMA21 in uptrend. RSI={rsi:.1f}, trend_strength={trend_strength:.2f}",
                    triggering_features=("ema_21", "rsi_14", "trend_strength"),
                    tags=("momentum", "pullback", "trend_continuation"),
                    entry_reference=current_price,
                    invalidation_reference=ema_21 * 0.98,  # 2% below EMA21
                    target_reference=current_price * 1.03,  # 3% target
                )

        # Check for SHORT setup in downtrend
        if is_downtrend:
            # Price should be near EMA (bounced)
            # EMA9 < EMA21 (trend intact)
            # RSI not oversold (> 30)
            # Trend strength reasonable (> 0.3)
            price_near_ema = abs(current_price - ema_21) / ema_21 < 0.02
            ema_aligned = ema_9 < ema_21
            rsi_ok = rsi > 30
            trend_ok = trend_strength > 0.3 and trend_direction < 0

            if price_near_ema and ema_aligned and rsi_ok and trend_ok:
                return self.create_signal(
                    context=context,
                    direction=SignalDirection.SHORT,
                    rationale=f"First pullback to EMA21 in downtrend. RSI={rsi:.1f}, trend_strength={trend_strength:.2f}",
                    triggering_features=("ema_21", "rsi_14", "trend_strength"),
                    tags=("momentum", "pullback", "trend_continuation"),
                    entry_reference=current_price,
                    invalidation_reference=ema_21 * 1.02,  # 2% above EMA21
                    target_reference=current_price * 0.97,  # 3% target
                )

        return self.create_no_signal(context)


class HighOfDayContinuationStrategy(Strategy):
    """
    High of Day Continuation Strategy.

    Generates a signal when price breaks above the session high
    in an uptrend with strong momentum, suggesting continuation.

    Entry conditions:
    - Market is in uptrend
    - Price is at or near high of day
    - Strong relative volume
    - RSI showing strength (> 50)
    - MACD positive

    This strategy is designed for volatile trending regimes.
    """

    @property
    def name(self) -> str:
        return "hod_continuation"

    @property
    def description(self) -> str:
        return "Enter on high-of-day breakout in trending market"

    @property
    def required_features(self) -> tuple[str, ...]:
        return (
            "rsi_14",
            "macd_histogram",
            "relative_volume",
            "price_in_range",
            "trend_direction",
        )

    @property
    def compatible_regimes(self) -> tuple[str, ...]:
        return ("trending_up", "volatile_trending_up")

    def evaluate(self, context: StrategyContext) -> SignalCandidate:
        """Evaluate HOD continuation setup."""
        if not self.can_evaluate(context):
            return self.create_no_signal(context)

        features = context.features.features
        snapshot = context.snapshot
        regime = context.features.regime or ""

        # Extract feature values
        rsi = features.get("rsi_14")
        macd_hist = features.get("macd_histogram")
        relative_vol = features.get("relative_volume")
        price_in_range = features.get("price_in_range")
        trend_direction = features.get("trend_direction")

        # Safety checks
        if any(v is None for v in [rsi, macd_hist, relative_vol, price_in_range, trend_direction]):
            return self.create_no_signal(context)

        current_price = snapshot.last

        # Only for uptrends
        if "trending_up" not in regime:
            return self.create_no_signal(context)

        # Check conditions:
        # - Price near high of range (> 90 percentile)
        # - Relative volume elevated (> 1.2x average)
        # - RSI showing strength (> 50, but not extreme > 80)
        # - MACD histogram positive
        # - Trend direction positive

        at_highs = price_in_range > 90
        volume_ok = relative_vol > 1.2
        rsi_ok = 50 < rsi < 80
        macd_ok = macd_hist > 0
        trend_ok = trend_direction > 0

        if at_highs and volume_ok and rsi_ok and macd_ok and trend_ok:
            return self.create_signal(
                context=context,
                direction=SignalDirection.LONG,
                rationale=f"HOD breakout with volume. RVOL={relative_vol:.2f}, RSI={rsi:.1f}, MACD_hist={macd_hist:.3f}",
                triggering_features=("price_in_range", "relative_volume", "macd_histogram"),
                tags=("momentum", "breakout", "hod", "volume_confirmed"),
                entry_reference=current_price,
                invalidation_reference=current_price * 0.99,  # 1% below entry
                target_reference=current_price * 1.02,  # 2% target
            )

        return self.create_no_signal(context)


# Convenience function to get all momentum strategies
def get_momentum_strategies() -> list[Strategy]:
    """Return all momentum strategy instances."""
    return [
        FirstPullbackStrategy(),
        HighOfDayContinuationStrategy(),
    ]
