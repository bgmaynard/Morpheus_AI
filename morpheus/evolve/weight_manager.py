"""
MASS Strategy Weight Manager - Adapts strategy weights based on performance.

Weights influence:
- Strategy classifier routing priority
- Position sizing (higher weight = potentially larger size)

Weights are updated periodically (end-of-day or after N trades).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from morpheus.core.events import Event, EventType, create_event
from morpheus.evolve.tracker import PerformanceTracker

logger = logging.getLogger(__name__)

# Weight clamp range
MIN_WEIGHT = 0.3
MAX_WEIGHT = 2.0

# Minimum trades before adjusting weight
MIN_TRADES_FOR_ADJUSTMENT = 5


class StrategyWeightManager:
    """
    Manages dynamic strategy weights based on performance feedback.

    Weight adjustment logic:
    - Win rate > 60%: boost by (win_rate - 0.5) * 2
    - Win rate < 40%: reduce by (0.5 - win_rate) * 2
    - Profit factor > 2: boost 20%
    - On 3+ loss streak: reduce 30%
    - Clamp to [0.3, 2.0] range
    """

    def __init__(
        self,
        tracker: PerformanceTracker,
        base_weights: dict[str, float] | None = None,
    ):
        self._tracker = tracker
        self._base_weights = base_weights or {}
        self._current_weights: dict[str, float] = dict(self._base_weights)
        self._last_update: datetime | None = None

    def get_weights(self) -> dict[str, float]:
        """Get current strategy weights."""
        return dict(self._current_weights)

    def update_weights(self) -> dict[str, float]:
        """
        Recalculate all strategy weights from tracker data.

        Returns new weights dict.
        """
        all_perf = self._tracker.get_all_performance()
        new_weights: dict[str, float] = {}

        for strategy_name, perf in all_perf.items():
            base = self._base_weights.get(strategy_name, 1.0)

            if perf.total_trades < MIN_TRADES_FOR_ADJUSTMENT:
                new_weights[strategy_name] = base
                continue

            multiplier = 1.0

            # Win rate adjustment
            if perf.win_rate > 0.60:
                multiplier += (perf.win_rate - 0.5) * 2
            elif perf.win_rate < 0.40:
                multiplier -= (0.5 - perf.win_rate) * 2

            # Profit factor bonus
            if perf.profit_factor > 2.0:
                multiplier *= 1.2

            # Loss streak penalty
            if perf.streak <= -3:
                multiplier *= 0.7

            # Apply to base weight and clamp
            adjusted = base * multiplier
            adjusted = max(MIN_WEIGHT, min(MAX_WEIGHT, adjusted))
            new_weights[strategy_name] = round(adjusted, 4)

        self._current_weights = new_weights
        self._last_update = datetime.now(timezone.utc)

        logger.info(f"[WEIGHT_MGR] Updated weights: {new_weights}")
        return new_weights

    def get_weight_update_event(self) -> Event:
        """Create a STRATEGY_WEIGHT_ADJUSTED event with current weights."""
        return create_event(
            EventType.STRATEGY_WEIGHT_ADJUSTED,
            payload={
                "weights": self._current_weights,
                "updated_at": self._last_update.isoformat() if self._last_update else None,
                "total_outcomes": self._tracker.total_outcomes,
            },
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize for API response."""
        return {
            "current_weights": self._current_weights,
            "base_weights": self._base_weights,
            "last_update": self._last_update.isoformat() if self._last_update else None,
            "total_outcomes": self._tracker.total_outcomes,
        }
