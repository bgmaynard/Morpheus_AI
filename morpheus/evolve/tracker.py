"""
MASS Performance Tracker - Records trade outcomes and computes strategy metrics.

Listens for TRADE_CLOSED events and updates per-strategy performance.
Persists outcomes to JSONL for analysis.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any

from morpheus.evolve.types import TradeOutcome, StrategyPerformance

logger = logging.getLogger(__name__)

# Max recent trades to keep for decay detection
MAX_RECENT_WINDOW = 50


class PerformanceTracker:
    """
    Tracks strategy performance and computes metrics.

    Thread-safe via lock. Persists outcomes to JSONL.
    """

    def __init__(self, log_dir: Path | None = None):
        self._log_dir = log_dir or Path("./logs/performance")
        self._log_dir.mkdir(parents=True, exist_ok=True)

        self._performance: dict[str, StrategyPerformance] = {}
        self._lock = Lock()
        self._total_outcomes = 0

    def record_outcome(self, outcome: TradeOutcome) -> None:
        """
        Record a completed trade outcome.

        Updates in-memory performance and persists to JSONL.
        """
        with self._lock:
            strategy = outcome.strategy_name
            if strategy not in self._performance:
                self._performance[strategy] = StrategyPerformance(
                    strategy_name=strategy
                )

            perf = self._performance[strategy]
            perf.total_trades += 1
            perf.total_pnl_dollars += outcome.pnl_dollars
            perf.last_updated = datetime.now(timezone.utc)

            if outcome.is_win:
                perf.wins += 1
                perf.total_win_dollars += outcome.pnl_dollars
                perf.streak = max(perf.streak, 0) + 1
                perf.recent_outcomes.append(True)
            else:
                perf.losses += 1
                perf.total_loss_dollars += outcome.pnl_dollars
                perf.streak = min(perf.streak, 0) - 1
                perf.recent_outcomes.append(False)

            # Trim recent window
            if len(perf.recent_outcomes) > MAX_RECENT_WINDOW:
                perf.recent_outcomes = perf.recent_outcomes[-MAX_RECENT_WINDOW:]

            self._total_outcomes += 1

        # Persist
        self._persist_outcome(outcome)

        logger.info(
            f"[TRACKER] Recorded {strategy}: "
            f"{'WIN' if outcome.is_win else 'LOSS'} "
            f"${outcome.pnl_dollars:+.2f} "
            f"(total: {perf.total_trades}, WR: {perf.win_rate:.0%})"
        )

    def get_performance(self, strategy_name: str) -> StrategyPerformance | None:
        """Get performance for a specific strategy."""
        with self._lock:
            return self._performance.get(strategy_name)

    def get_all_performance(self) -> dict[str, StrategyPerformance]:
        """Get performance for all strategies."""
        with self._lock:
            return dict(self._performance)

    def get_all_performance_dicts(self) -> dict[str, dict[str, Any]]:
        """Get all performance as serializable dicts."""
        with self._lock:
            return {
                name: perf.to_dict()
                for name, perf in self._performance.items()
            }

    @property
    def total_outcomes(self) -> int:
        return self._total_outcomes

    def _persist_outcome(self, outcome: TradeOutcome) -> None:
        """Append outcome to daily JSONL file."""
        try:
            date_str = outcome.timestamp.strftime("%Y-%m-%d")
            file_path = self._log_dir / f"outcomes_{date_str}.jsonl"
            with open(file_path, "a") as f:
                f.write(json.dumps(outcome.to_dict()) + "\n")
        except Exception as e:
            logger.warning(f"[TRACKER] Failed to persist outcome: {e}")
