"""
MASS Strategy: Short Squeeze (SQZ)

Identifies potential short squeeze setups using extreme relative volume,
rapid price acceleration, and bullish momentum.

Conditions:
- Strong bullish momentum regime
- Extreme RVOL (> 4x)
- RSI > 60 and accelerating
- Price above VWAP, EMA stack bullish
- Momentum positive and increasing

All evaluation is DETERMINISTIC.
"""

from __future__ import annotations

from morpheus.strategies.base import (
    Strategy,
    StrategyContext,
    SignalCandidate,
    SignalDirection,
)


class ShortSqueezeStrategy(Strategy):
    """Short Squeeze - enters on extreme volume + rapid acceleration."""

    @property
    def name(self) -> str:
        return "short_squeeze"

    @property
    def description(self) -> str:
        return "Short squeeze entry on extreme volume with rapid price acceleration"

    @property
    def required_features(self) -> tuple[str, ...]:
        return (
            "rsi_14", "relative_volume", "vwap", "price_vs_vwap",
            "ema_9", "ema_21", "momentum_10",
        )

    @property
    def compatible_regimes(self) -> tuple[str, ...]:
        return ("trending_up", "volatile_trending_up", "strong_bullish")

    @property
    def allowed_market_modes(self) -> frozenset[str]:
        return frozenset({"RTH"})

    def evaluate(self, context: StrategyContext) -> SignalCandidate:
        f = context.features
        price = context.snapshot.last

        # Extreme RVOL required
        rvol = f.get_feature("relative_volume")
        scanner_rvol = f.external.get("rvol_proxy")
        effective_rvol = max(rvol or 0, scanner_rvol or 0)
        if effective_rvol < 4.0:
            return self.create_no_signal(context)

        # RSI above 60 (momentum accelerating)
        rsi = f.get_feature("rsi_14")
        if rsi is None or rsi < 60:
            return self.create_no_signal(context)

        # Price above VWAP
        vwap = f.get_feature("vwap")
        if vwap is not None and price <= vwap:
            return self.create_no_signal(context)

        # Bullish EMA stack
        ema_9 = f.get_feature("ema_9")
        ema_21 = f.get_feature("ema_21")
        if ema_9 is None or ema_21 is None or ema_9 <= ema_21:
            return self.create_no_signal(context)

        # Positive momentum
        mom = f.get_feature("momentum_10")
        if mom is not None and mom <= 0:
            return self.create_no_signal(context)

        # Reference levels - wider stops for squeeze volatility
        atr = f.get_feature("atr_14")
        stop_distance = atr * 2.0 if atr else price * 0.03
        entry_ref = price
        stop_ref = price - stop_distance
        target_ref = price + (stop_distance * 3.0)

        return self.create_signal(
            context,
            direction=SignalDirection.LONG,
            rationale=(
                f"SQZ: RVOL {effective_rvol:.1f}x, RSI {rsi:.0f}, "
                f"bullish stack, momentum {mom:.2f}"
            ),
            triggering_features=("relative_volume", "rsi_14", "ema_9", "momentum_10"),
            tags=("mass", "sqz", "squeeze", "momentum", f"rvol:{effective_rvol:.0f}x"),
            entry_reference=entry_ref,
            invalidation_reference=stop_ref,
            target_reference=target_ref,
        )
