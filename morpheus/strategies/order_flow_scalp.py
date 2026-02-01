"""
MASS Strategy: Order Flow Scalping (SCALP)

Quick scalps on high-volume, tight-spread stocks with clear
micro-momentum. Smallest targets, tightest stops.

Conditions:
- High volatility regime
- Very tight spread (< 0.2%)
- RVOL > 2x
- Clear micro-trend (consecutive directional bars)
- Price at key intraday level

All evaluation is DETERMINISTIC.
"""

from __future__ import annotations

from morpheus.strategies.base import (
    Strategy,
    StrategyContext,
    SignalCandidate,
    SignalDirection,
)


class OrderFlowScalpStrategy(Strategy):
    """Order Flow Scalp - quick scalps on tight-spread, high-volume stocks."""

    @property
    def name(self) -> str:
        return "order_flow_scalp"

    @property
    def description(self) -> str:
        return "Order flow scalping on tight spread, high volume with micro-momentum"

    @property
    def required_features(self) -> tuple[str, ...]:
        return ("atr_pct", "relative_volume", "consecutive_bars", "rsi_14", "price_in_range")

    @property
    def compatible_regimes(self) -> tuple[str, ...]:
        return ()  # Works in any regime with sufficient liquidity

    @property
    def allowed_market_modes(self) -> frozenset[str]:
        return frozenset({"RTH"})

    def evaluate(self, context: StrategyContext) -> SignalCandidate:
        f = context.features
        snapshot = context.snapshot
        price = snapshot.last

        # Tight spread required
        spread_pct = snapshot.spread_pct
        if spread_pct > 0.2:
            return self.create_no_signal(context)

        # RVOL check
        rvol = f.get_feature("relative_volume")
        scanner_rvol = f.external.get("rvol_proxy")
        effective_rvol = max(rvol or 0, scanner_rvol or 0)
        if effective_rvol < 2.0:
            return self.create_no_signal(context)

        # ATR must show enough movement to scalp
        atr_pct = f.get_feature("atr_pct")
        if atr_pct is None or atr_pct < 1.5:
            return self.create_no_signal(context)

        # Consecutive directional bars (micro-trend)
        consec = f.get_feature("consecutive_bars")
        if consec is None or abs(consec) < 2:
            return self.create_no_signal(context)

        # Determine direction from micro-trend
        if consec > 0:
            direction = SignalDirection.LONG
            tag_dir = "long"
        else:
            direction = SignalDirection.SHORT
            tag_dir = "short"

        # Tight reference levels for scalp
        atr = f.get_feature("atr_14")
        stop_distance = atr * 0.5 if atr else price * 0.005
        entry_ref = price

        if direction == SignalDirection.LONG:
            stop_ref = price - stop_distance
            target_ref = price + (stop_distance * 1.5)
        else:
            stop_ref = price + stop_distance
            target_ref = price - (stop_distance * 1.5)

        return self.create_signal(
            context,
            direction=direction,
            rationale=(
                f"SCALP: spread {spread_pct:.2f}%, RVOL {effective_rvol:.1f}x, "
                f"{abs(consec):.0f} consec bars {tag_dir}, ATR {atr_pct:.1f}%"
            ),
            triggering_features=("consecutive_bars", "relative_volume", "atr_pct"),
            tags=("mass", "scalp", "order_flow", tag_dir),
            entry_reference=entry_ref,
            invalidation_reference=stop_ref,
            target_reference=target_ref,
        )
