"""
Mean Reversion Strategies - Signal generation for mean reversion setups.

Strategies:
1. VWAPReclaim - Enter when price reclaims VWAP after deviation

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


class VWAPReclaimStrategy(Strategy):
    """
    VWAP Reclaim Strategy.

    Generates a signal when price reclaims VWAP after being
    extended below (for longs) or above (for shorts).

    Entry conditions for LONG:
    - Market is ranging or quiet
    - Price was below VWAP (oversold)
    - Price has reclaimed VWAP
    - Mean reversion score positive
    - RSI recovering from oversold

    Entry conditions for SHORT:
    - Market is ranging or quiet
    - Price was above VWAP (overbought)
    - Price has failed at VWAP
    - Mean reversion score negative
    - RSI retreating from overbought

    This strategy is designed for ranging/quiet regimes.
    """

    @property
    def name(self) -> str:
        return "vwap_reclaim"

    @property
    def description(self) -> str:
        return "Enter on VWAP reclaim after deviation in ranging market"

    @property
    def required_features(self) -> tuple[str, ...]:
        return (
            "vwap",
            "price_vs_vwap",
            "rsi_14",
            "mean_reversion_score",
            "bb_position",
            "atr_pct",
        )

    @property
    def compatible_regimes(self) -> tuple[str, ...]:
        return ("ranging", "quiet_ranging", "quiet_trending_up", "quiet_trending_down")

    def evaluate(self, context: StrategyContext) -> SignalCandidate:
        """Evaluate VWAP reclaim setup."""
        if not self.can_evaluate(context):
            return self.create_no_signal(context)

        features = context.features.features
        snapshot = context.snapshot
        regime = context.features.regime or ""

        # Extract feature values
        vwap = features.get("vwap")
        price_vs_vwap = features.get("price_vs_vwap")
        rsi = features.get("rsi_14")
        mean_rev_score = features.get("mean_reversion_score")
        bb_position = features.get("bb_position")
        atr_pct = features.get("atr_pct")

        # Safety checks
        if any(v is None for v in [vwap, price_vs_vwap, rsi, mean_rev_score, bb_position, atr_pct]):
            return self.create_no_signal(context)

        current_price = snapshot.last

        # Skip if volatility is too high (risky for mean reversion)
        if atr_pct > 3.0:
            return self.create_no_signal(context)

        # Check for LONG setup (reclaim from below)
        # - Price is slightly above VWAP (just reclaimed)
        # - Was recently below (mean_rev_score > 0 indicates oversold)
        # - RSI recovering (30-50 range ideal)
        # - BB position was low, now recovering

        price_just_above_vwap = 0 < price_vs_vwap < 0.5  # 0-0.5% above
        oversold_recovery = mean_rev_score > 0.5  # Was oversold
        rsi_recovering = 30 < rsi < 55
        bb_low = bb_position < 40  # Still in lower portion

        if price_just_above_vwap and oversold_recovery and rsi_recovering and bb_low:
            return self.create_signal(
                context=context,
                direction=SignalDirection.LONG,
                rationale=f"VWAP reclaim from below. MR_score={mean_rev_score:.2f}, RSI={rsi:.1f}, BB_pos={bb_position:.1f}",
                triggering_features=("vwap", "mean_reversion_score", "rsi_14", "bb_position"),
                tags=("mean_reversion", "vwap", "oversold_recovery"),
                entry_reference=current_price,
                invalidation_reference=vwap * 0.995,  # 0.5% below VWAP
                target_reference=vwap * 1.01,  # 1% above VWAP
            )

        # Check for SHORT setup (rejection from above)
        # - Price is slightly below VWAP (just lost it)
        # - Was recently above (mean_rev_score < 0 indicates overbought)
        # - RSI retreating (50-70 range, coming down)
        # - BB position was high, now retreating

        price_just_below_vwap = -0.5 < price_vs_vwap < 0  # 0-0.5% below
        overbought_rejection = mean_rev_score < -0.5  # Was overbought
        rsi_retreating = 45 < rsi < 70
        bb_high = bb_position > 60  # Still in upper portion

        if price_just_below_vwap and overbought_rejection and rsi_retreating and bb_high:
            return self.create_signal(
                context=context,
                direction=SignalDirection.SHORT,
                rationale=f"VWAP rejection from above. MR_score={mean_rev_score:.2f}, RSI={rsi:.1f}, BB_pos={bb_position:.1f}",
                triggering_features=("vwap", "mean_reversion_score", "rsi_14", "bb_position"),
                tags=("mean_reversion", "vwap", "overbought_rejection"),
                entry_reference=current_price,
                invalidation_reference=vwap * 1.005,  # 0.5% above VWAP
                target_reference=vwap * 0.99,  # 1% below VWAP
            )

        return self.create_no_signal(context)


# Convenience function to get all mean reversion strategies
def get_mean_reversion_strategies() -> list[Strategy]:
    """Return all mean reversion strategy instances."""
    return [
        VWAPReclaimStrategy(),
    ]
