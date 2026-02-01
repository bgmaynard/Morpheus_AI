"""
MASS Strategy: Day-2 Continuation (D2)

Enters on day-2 of a multi-day runner when price holds above
prior day's range and shows continuation pattern.

Conditions:
- Trending up regime
- Gap holding (gap_pct > 0 from scanner)
- Price above VWAP
- Moderate RSI (45-65, not exhausted)
- Volume confirming (RVOL > 1.5)

All evaluation is DETERMINISTIC.
"""

from __future__ import annotations

from morpheus.strategies.base import (
    Strategy,
    StrategyContext,
    SignalCandidate,
    SignalDirection,
)


class Day2ContinuationStrategy(Strategy):
    """Day-2 Continuation - enters on multi-day runners holding gains."""

    @property
    def name(self) -> str:
        return "day2_continuation"

    @property
    def description(self) -> str:
        return "Day-2 continuation on prior-day runners holding above key levels"

    @property
    def required_features(self) -> tuple[str, ...]:
        return ("vwap", "price_vs_vwap", "rsi_14", "ema_9", "ema_21", "relative_volume")

    @property
    def compatible_regimes(self) -> tuple[str, ...]:
        return ("trending_up",)

    @property
    def allowed_market_modes(self) -> frozenset[str]:
        return frozenset({"RTH"})

    def evaluate(self, context: StrategyContext) -> SignalCandidate:
        f = context.features
        price = context.snapshot.last

        # Gap must be holding (positive gap from scanner)
        gap_pct = f.external.get("gap_pct", 0)
        if not gap_pct or gap_pct <= 0:
            return self.create_no_signal(context)

        # Price above VWAP
        vwap = f.get_feature("vwap")
        if vwap is not None and price <= vwap:
            return self.create_no_signal(context)

        # RSI not exhausted
        rsi = f.get_feature("rsi_14")
        if rsi is not None and (rsi < 45 or rsi > 65):
            return self.create_no_signal(context)

        # EMA stack should be bullish
        ema_9 = f.get_feature("ema_9")
        ema_21 = f.get_feature("ema_21")
        if ema_9 is not None and ema_21 is not None and ema_9 <= ema_21:
            return self.create_no_signal(context)

        # Volume confirmation
        rvol = f.get_feature("relative_volume")
        scanner_rvol = f.external.get("rvol_proxy")
        effective_rvol = max(rvol or 0, scanner_rvol or 0)
        if effective_rvol < 1.5:
            return self.create_no_signal(context)

        # Reference levels - tighter stops for D2
        atr = f.get_feature("atr_14")
        stop_distance = atr * 1.0 if atr else price * 0.015
        entry_ref = price
        stop_ref = max(price - stop_distance, vwap) if vwap else price - stop_distance
        target_ref = price + (stop_distance * 2.5)

        return self.create_signal(
            context,
            direction=SignalDirection.LONG,
            rationale=(
                f"D2: gap holding {gap_pct:.1f}%, above VWAP, "
                f"RSI {rsi:.0f}, RVOL {effective_rvol:.1f}x"
            ),
            triggering_features=("vwap", "rsi_14", "ema_9", "ema_21"),
            tags=("mass", "d2", "continuation", "day2", f"gap:{gap_pct:.0f}pct"),
            entry_reference=entry_ref,
            invalidation_reference=stop_ref,
            target_reference=target_ref,
        )
