"""
MASS Strategy: Gap Fade (FADE)

Fades overextended gap-ups that show signs of reversal.
Counter-trend strategy - higher risk, tighter stops.

Conditions:
- Gap up > 5% (from scanner)
- Price fading below VWAP
- RSI retreating from overbought
- MACD histogram turning negative
- Volume on the fade

Direction: SHORT

All evaluation is DETERMINISTIC.
"""

from __future__ import annotations

from morpheus.strategies.base import (
    Strategy,
    StrategyContext,
    SignalCandidate,
    SignalDirection,
)


class GapFadeStrategy(Strategy):
    """Gap Fade - shorts overextended gap-ups showing reversal signs."""

    @property
    def name(self) -> str:
        return "gap_fade"

    @property
    def description(self) -> str:
        return "Gap fade SHORT on overextended gap-ups with reversal confirmation"

    @property
    def required_features(self) -> tuple[str, ...]:
        return ("vwap", "price_vs_vwap", "rsi_14", "macd_histogram", "relative_volume")

    @property
    def compatible_regimes(self) -> tuple[str, ...]:
        return ("trending_down", "volatile_trending_down", "bearish", "ranging")

    @property
    def allowed_market_modes(self) -> frozenset[str]:
        return frozenset({"RTH"})

    def evaluate(self, context: StrategyContext) -> SignalCandidate:
        f = context.features
        price = context.snapshot.last

        # Gap must be significant
        gap_pct = f.external.get("gap_pct", 0)
        if not gap_pct or gap_pct < 5.0:
            return self.create_no_signal(context)

        # Price must be below VWAP (fading)
        vwap = f.get_feature("vwap")
        if vwap is not None and price >= vwap:
            return self.create_no_signal(context)

        # RSI retreating (was overbought, now pulling back)
        rsi = f.get_feature("rsi_14")
        if rsi is None or rsi > 65 or rsi < 35:
            return self.create_no_signal(context)

        # MACD histogram negative (momentum shifting down)
        macd_hist = f.get_feature("macd_histogram")
        if macd_hist is None or macd_hist >= 0:
            return self.create_no_signal(context)

        # Volume on the fade
        rvol = f.get_feature("relative_volume")
        scanner_rvol = f.external.get("rvol_proxy")
        effective_rvol = max(rvol or 0, scanner_rvol or 0)
        if effective_rvol < 1.5:
            return self.create_no_signal(context)

        # Reference levels - tight stops for counter-trend
        atr = f.get_feature("atr_14")
        stop_distance = atr * 1.0 if atr else price * 0.015
        entry_ref = price
        stop_ref = price + stop_distance  # Stop above for SHORT
        target_ref = price - (stop_distance * 2.0)  # Target below for SHORT

        return self.create_signal(
            context,
            direction=SignalDirection.SHORT,
            rationale=(
                f"FADE: gap {gap_pct:.1f}% fading, below VWAP, "
                f"RSI {rsi:.0f}, MACD hist {macd_hist:.3f}"
            ),
            triggering_features=("vwap", "rsi_14", "macd_histogram"),
            tags=("mass", "fade", "counter_trend", "short", f"gap:{gap_pct:.0f}pct"),
            entry_reference=entry_ref,
            invalidation_reference=stop_ref,
            target_reference=target_ref,
        )
