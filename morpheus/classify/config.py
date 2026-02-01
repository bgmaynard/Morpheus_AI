"""Configuration for MASS Strategy Classifier."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ClassifierConfig:
    """Strategy classifier configuration."""

    # Minimum structure grade to allow any strategy assignment
    min_grade_for_entry: str = "B"  # "A" or "B"

    # Maximum strategies to assign per symbol
    max_strategies_per_symbol: int = 2

    # Minimum fit score to assign a strategy
    min_fit_score: float = 0.3

    # Strategy weights (higher = more likely to be assigned when conditions match)
    strategy_weights: dict[str, float] = field(
        default_factory=lambda: {
            "premarket_breakout": 1.0,
            "vwap_reclaim": 1.0,
            "catalyst_momentum": 1.0,
            "day2_continuation": 1.0,
            "coil_breakout": 1.0,
            "short_squeeze": 0.8,
            "gap_fade": 0.7,
            "order_flow_scalp": 0.5,
        }
    )
