"""
MASS Structure Analyzer - Classifies symbol structure quality.

Evaluates market structure to produce A/B/C grades based on:
- VWAP position
- EMA stacking
- Trend slope alignment
- Support/Resistance proximity
- Liquidity (spread)
- Volume (relative volume)

All analysis is DETERMINISTIC: same FeatureContext -> same StructureGrade.
"""

from __future__ import annotations

import logging
from typing import Any

from morpheus.data.market_snapshot import MarketSnapshot
from morpheus.features.feature_engine import FeatureContext
from morpheus.structure.config import StructureConfig
from morpheus.structure.types import StructureGrade

logger = logging.getLogger(__name__)


class StructureAnalyzer:
    """
    Classifies symbol structure quality from FeatureContext.

    Deterministic: same FeatureContext -> same StructureGrade.
    Does NOT access external state.
    """

    def __init__(self, config: StructureConfig | None = None):
        self.config = config or StructureConfig()

    def classify(
        self, features: FeatureContext, snapshot: MarketSnapshot
    ) -> StructureGrade:
        """
        Classify a symbol's structure into A/B/C grade.

        Scoring rubric (100 points total):
        - VWAP position: 0-20 points
        - EMA stacking: 0-20 points
        - Trend slope: 0-15 points
        - S/R proximity: 0-15 points
        - Liquidity: 0-15 points
        - Volume: 0-15 points

        A >= 75, B >= 50, C < 50
        """
        price = snapshot.last

        # Component analysis
        vwap_pos, vwap_score = self._score_vwap(features, price)
        ema_stack_label, ema_score = self._score_ema_stack(features)
        trend_slope, trend_score = self._score_trend(features)
        sr_levels, sr_score = self._score_sr_proximity(features, price)
        spread_pct, liq_score = self._score_liquidity(snapshot)
        rvol, vol_score = self._score_volume(features)

        total_score = vwap_score + ema_score + trend_score + sr_score + liq_score + vol_score

        # Determine grade
        if total_score >= self.config.grade_a_threshold:
            grade = "A"
        elif total_score >= self.config.grade_b_threshold:
            grade = "B"
        else:
            grade = "C"

        # Build rationale
        parts = []
        if vwap_score >= 15:
            parts.append(f"VWAP {vwap_pos}")
        if ema_score >= 15:
            parts.append(f"EMA {ema_stack_label}")
        if trend_score >= 10:
            parts.append(f"trend slope {trend_slope:+.2f}")
        if sr_score >= 10:
            parts.append("near S/R")
        if liq_score >= 10:
            parts.append(f"spread {spread_pct:.2f}%")
        if vol_score >= 10:
            parts.append(f"RVOL {rvol:.1f}x")
        rationale = f"Grade {grade} ({total_score:.0f}/100): {', '.join(parts) if parts else 'weak structure'}"

        # Tags
        tags = [f"structure:{grade.lower()}", f"vwap:{vwap_pos}"]
        if ema_stack_label:
            tags.append(f"ema:{ema_stack_label}")
        if rvol >= 2.0:
            tags.append("volume:high")
        elif rvol >= 1.5:
            tags.append("volume:moderate")

        return StructureGrade(
            grade=grade,
            score=total_score,
            vwap_position=vwap_pos,
            vwap_score=vwap_score,
            ema_stack=ema_stack_label,
            ema_score=ema_score,
            trend_slope=trend_slope,
            trend_score=trend_score,
            sr_levels=sr_levels,
            sr_score=sr_score,
            liquidity_score=liq_score,
            spread_pct=spread_pct,
            volume_score=vol_score,
            relative_volume=rvol,
            rationale=rationale,
            tags=tuple(tags),
        )

    def _score_vwap(
        self, features: FeatureContext, price: float
    ) -> tuple[str, float]:
        """Score VWAP position (0-20 points)."""
        vwap_val = features.get_feature("vwap")
        if vwap_val is None or vwap_val <= 0:
            return "unknown", 5.0

        pct_from_vwap = ((price - vwap_val) / vwap_val) * 100

        if pct_from_vwap > 1.0:
            return "above", 20.0
        elif pct_from_vwap > 0.0:
            return "at", 10.0
        elif pct_from_vwap > -1.0:
            return "at", 8.0
        else:
            return "below", 0.0

    def _score_ema_stack(self, features: FeatureContext) -> tuple[str, float]:
        """Score EMA stacking (0-20 points)."""
        ema_9 = features.get_feature("ema_9")
        ema_21 = features.get_feature("ema_21")

        if ema_9 is None or ema_21 is None:
            return "unknown", 5.0

        if ema_9 > ema_21:
            # Bullish stack
            separation_pct = ((ema_9 - ema_21) / ema_21) * 100
            if separation_pct > 0.5:
                return "bullish", 20.0
            else:
                return "bullish", 15.0
        elif ema_9 < ema_21:
            # Bearish stack
            separation_pct = ((ema_21 - ema_9) / ema_21) * 100
            if separation_pct > 0.5:
                return "bearish", 5.0
            else:
                return "bearish", 8.0
        else:
            return "mixed", 10.0

    def _score_trend(self, features: FeatureContext) -> tuple[float, float]:
        """Score trend slope alignment (0-15 points)."""
        trend_dir = features.get_feature("trend_direction")
        trend_str = features.get_feature("trend_strength")

        if trend_dir is None or trend_str is None:
            return 0.0, 5.0

        # trend_direction is typically -1 to 1
        slope = trend_dir

        # Strong positive trend is best for longs
        abs_strength = abs(trend_str) if trend_str else 0.0

        if slope > 0.3 and abs_strength > 0.5:
            return slope, 15.0
        elif slope > 0.1 and abs_strength > 0.3:
            return slope, 10.0
        elif abs(slope) < 0.1:
            return slope, 7.0  # Flat/consolidating - not terrible
        else:
            return slope, 3.0

    def _score_sr_proximity(
        self, features: FeatureContext, price: float
    ) -> tuple[tuple[float, ...], float]:
        """Score support/resistance proximity (0-15 points)."""
        # Use Bollinger bands as S/R proxy
        bb_upper = features.get_feature("bb_upper")
        bb_lower = features.get_feature("bb_lower")
        bb_middle = features.get_feature("bb_middle")

        if bb_upper is None or bb_lower is None or bb_middle is None:
            return (), 5.0

        levels = (bb_lower, bb_middle, bb_upper)
        proximity_pct = self.config.sr_proximity_pct

        # Check proximity to any key level
        for level in levels:
            if level <= 0:
                continue
            dist_pct = abs(price - level) / level * 100
            if dist_pct <= proximity_pct:
                # Near a key level - good for breakout/bounce setups
                return levels, 15.0

        # Price position relative to bands
        bb_pos = features.get_feature("bb_position")
        if bb_pos is not None:
            if 0.3 <= bb_pos <= 0.7:
                # Mid-range, less interesting
                return levels, 7.0
            else:
                # Near extremes, more interesting
                return levels, 12.0

        return levels, 5.0

    def _score_liquidity(self, snapshot: MarketSnapshot) -> tuple[float, float]:
        """Score liquidity based on spread (0-15 points)."""
        spread_pct = snapshot.spread_pct

        if spread_pct <= self.config.tight_spread_pct:
            return spread_pct, 15.0
        elif spread_pct <= 0.5:
            return spread_pct, 12.0
        elif spread_pct <= 1.0:
            return spread_pct, 8.0
        elif spread_pct <= self.config.max_spread_pct:
            return spread_pct, 4.0
        else:
            return spread_pct, 0.0

    def _score_volume(self, features: FeatureContext) -> tuple[float, float]:
        """Score relative volume (0-15 points)."""
        rvol = features.get_feature("relative_volume")

        # Also check scanner RVOL if available
        scanner_rvol = features.external.get("rvol_proxy")
        if scanner_rvol is not None and (rvol is None or scanner_rvol > rvol):
            rvol = scanner_rvol

        if rvol is None:
            return 0.0, 5.0

        if rvol >= self.config.min_rvol_for_a:
            return rvol, 15.0
        elif rvol >= self.config.min_rvol_for_b:
            return rvol, 10.0
        elif rvol >= 1.0:
            return rvol, 7.0
        else:
            return rvol, 2.0
