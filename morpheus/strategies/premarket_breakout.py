"""
MASS Strategy: Premarket Breakout (PMB)

Triggers when a gapped-up symbol in premarket breaks above a key level
(overnight high, resistance) with volume confirmation.

Conditions:
- PREMARKET mode
- Gap > 3% (from scanner context)
- Price above VWAP
- RSI 50-75 (not overextended)
- RVOL > 2x
- Structure grade A or B

All evaluation is DETERMINISTIC.
"""

from __future__ import annotations

from morpheus.strategies.base import (
    Strategy,
    StrategyContext,
    SignalCandidate,
    SignalDirection,
)


class PremarketBreakoutStrategy(Strategy):
    """Premarket Breakout - enters on gapped-up symbols breaking key levels."""

    @property
    def name(self) -> str:
        return "premarket_breakout"

    @property
    def description(self) -> str:
        return "Premarket breakout on gapped-up symbols with volume confirmation"

    @property
    def required_features(self) -> tuple[str, ...]:
        return ("vwap", "price_vs_vwap", "rsi_14", "relative_volume", "atr_pct")

    @property
    def compatible_regimes(self) -> tuple[str, ...]:
        return ("trending_up", "volatile_trending_up", "high_volatility")

    @property
    def allowed_market_modes(self) -> frozenset[str]:
        return frozenset({"PREMARKET"})

    def evaluate(self, context: StrategyContext) -> SignalCandidate:
        f = context.features
        price = context.snapshot.last

        # Check gap from scanner
        gap_pct = f.external.get("gap_pct", 0)
        if not gap_pct or gap_pct < 3.0:
            return self.create_no_signal(context)

        # Price must be above VWAP
        vwap = f.get_feature("vwap")
        if vwap is not None and price <= vwap:
            return self.create_no_signal(context)

        # RSI check - relaxed for gap stocks (indicators lag the gap)
        rsi = f.get_feature("rsi_14")
        if rsi is not None and rsi > 80:
            return self.create_no_signal(context)  # Only filter extreme overbought

        # Relative volume confirmation
        rvol = f.get_feature("relative_volume")
        scanner_rvol = f.external.get("rvol_proxy")
        effective_rvol = max(rvol or 0, scanner_rvol or 0)
        if effective_rvol < 1.5:
            return self.create_no_signal(context)

        # Structure grade check
        if f.structure_grade not in ("A", "B"):
            return self.create_no_signal(context)

        # Compute reference levels
        atr = f.get_feature("atr_14")
        stop_distance = atr * 1.5 if atr else price * 0.02
        entry_ref = price
        stop_ref = price - stop_distance
        target_ref = price + (stop_distance * 2.5)

        return self.create_signal(
            context,
            direction=SignalDirection.LONG,
            rationale=(
                f"PMB: gap {gap_pct:.1f}%, above VWAP, "
                f"RSI {rsi:.0f}, RVOL {effective_rvol:.1f}x, "
                f"grade {f.structure_grade}"
            ),
            triggering_features=("vwap", "rsi_14", "relative_volume"),
            tags=("mass", "pmb", "premarket", "breakout", f"gap:{gap_pct:.0f}pct"),
            entry_reference=entry_ref,
            invalidation_reference=stop_ref,
            target_reference=target_ref,
        )
