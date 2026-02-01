"""
MASS Strategy: Consolidation/Coil Breakout (COIL)

Identifies tight consolidation patterns where Bollinger Bands
narrow and enters on expansion breakout.

Conditions:
- Low volatility regime (BB squeeze)
- BB bandwidth below threshold
- Price near BB middle or slightly above
- Volume expanding on breakout
- Trend direction positive

All evaluation is DETERMINISTIC.
"""

from __future__ import annotations

from morpheus.strategies.base import (
    Strategy,
    StrategyContext,
    SignalCandidate,
    SignalDirection,
)


class CoilBreakoutStrategy(Strategy):
    """Coil Breakout - enters on BB squeeze expansion breakouts."""

    @property
    def name(self) -> str:
        return "coil_breakout"

    @property
    def description(self) -> str:
        return "Consolidation breakout when Bollinger Bands squeeze and expand"

    @property
    def required_features(self) -> tuple[str, ...]:
        return (
            "bb_upper", "bb_lower", "bb_middle", "bb_width",
            "bb_position", "atr_pct", "relative_volume", "trend_direction",
        )

    @property
    def compatible_regimes(self) -> tuple[str, ...]:
        return ("ranging", "low_volatility", "quiet")

    @property
    def allowed_market_modes(self) -> frozenset[str]:
        return frozenset({"RTH"})

    def evaluate(self, context: StrategyContext) -> SignalCandidate:
        f = context.features
        price = context.snapshot.last

        # BB width must be tight (squeeze)
        bb_width = f.get_feature("bb_width")
        if bb_width is None or bb_width >= 3.0:
            return self.create_no_signal(context)

        # Price position in bands - should be upper half (breaking out)
        bb_pos = f.get_feature("bb_position")
        if bb_pos is None or bb_pos < 0.5:
            return self.create_no_signal(context)

        # ATR should be low (confirming squeeze)
        atr_pct = f.get_feature("atr_pct")
        if atr_pct is not None and atr_pct > 3.0:
            return self.create_no_signal(context)

        # Trend direction should be positive
        trend_dir = f.get_feature("trend_direction")
        if trend_dir is not None and trend_dir <= 0:
            return self.create_no_signal(context)

        # Volume expanding (relative volume picking up)
        rvol = f.get_feature("relative_volume")
        scanner_rvol = f.external.get("rvol_proxy")
        effective_rvol = max(rvol or 0, scanner_rvol or 0)
        if effective_rvol < 1.2:
            return self.create_no_signal(context)

        # Structure grade check
        if f.structure_grade == "C":
            return self.create_no_signal(context)

        # Reference levels
        bb_lower = f.get_feature("bb_lower")
        bb_upper = f.get_feature("bb_upper")
        bb_middle = f.get_feature("bb_middle")

        stop_ref = bb_middle if bb_middle else price * 0.98
        target_ref = bb_upper * 1.01 if bb_upper else price * 1.03

        return self.create_signal(
            context,
            direction=SignalDirection.LONG,
            rationale=(
                f"COIL: BB squeeze (width={bb_width:.1f}), "
                f"position {bb_pos:.2f}, trend +, RVOL {effective_rvol:.1f}x"
            ),
            triggering_features=("bb_width", "bb_position", "trend_direction"),
            tags=("mass", "coil", "squeeze", "breakout", f"bbw:{bb_width:.1f}"),
            entry_reference=price,
            invalidation_reference=stop_ref,
            target_reference=target_ref,
        )
