"""Configuration for MASS Structure Analyzer."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StructureConfig:
    """Configuration for structure analysis scoring thresholds."""

    # Grade thresholds (score 0-100)
    grade_a_threshold: float = 75.0
    grade_b_threshold: float = 50.0

    # Liquidity thresholds
    max_spread_pct: float = 1.5  # Max spread to consider tradable
    tight_spread_pct: float = 0.3  # Spread below this is excellent

    # Volume thresholds
    min_rvol_for_a: float = 2.0  # RVOL for A-grade volume score
    min_rvol_for_b: float = 1.5  # RVOL for B-grade volume score

    # S/R analysis
    sr_lookback_bars: int = 50  # Bars to look back for S/R levels
    sr_proximity_pct: float = 1.0  # Within 1% of level = "near"
