"""MASS Evolution types - TradeOutcome and StrategyPerformance."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from morpheus.core.events import Event, EventType, create_event


@dataclass(frozen=True)
class TradeOutcome:
    """
    Completed trade record for performance feedback.

    Recorded when a trade closes (TRADE_CLOSED event).
    """

    trade_id: str = ""
    symbol: str = ""
    strategy_name: str = ""
    direction: str = ""  # "long" or "short"
    entry_price: float = 0.0
    exit_price: float = 0.0
    shares: int = 0
    pnl_pct: float = 0.0
    pnl_dollars: float = 0.0
    hold_time_seconds: float = 0.0
    regime_at_entry: str = ""
    structure_grade_at_entry: str = ""
    mass_regime_mode: str = ""
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def is_win(self) -> bool:
        return self.pnl_dollars > 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "trade_id": self.trade_id,
            "symbol": self.symbol,
            "strategy_name": self.strategy_name,
            "direction": self.direction,
            "entry_price": self.entry_price,
            "exit_price": self.exit_price,
            "shares": self.shares,
            "pnl_pct": self.pnl_pct,
            "pnl_dollars": self.pnl_dollars,
            "hold_time_seconds": self.hold_time_seconds,
            "regime_at_entry": self.regime_at_entry,
            "structure_grade_at_entry": self.structure_grade_at_entry,
            "mass_regime_mode": self.mass_regime_mode,
            "timestamp": self.timestamp.isoformat(),
        }

    def to_event(self) -> Event:
        return create_event(
            EventType.FEEDBACK_UPDATE,
            payload=self.to_dict(),
            symbol=self.symbol,
        )


@dataclass
class StrategyPerformance:
    """
    Aggregate performance metrics for a single strategy.

    Mutable - updated as trades close. Use frozen snapshots for events.
    """

    strategy_name: str = ""
    total_trades: int = 0
    wins: int = 0
    losses: int = 0
    total_pnl_dollars: float = 0.0
    total_win_dollars: float = 0.0
    total_loss_dollars: float = 0.0
    current_weight: float = 1.0
    streak: int = 0  # positive = win streak, negative = loss streak
    last_updated: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # Recent trades window (for decay detection)
    recent_outcomes: list[bool] = field(default_factory=list)  # True=win, False=loss

    @property
    def win_rate(self) -> float:
        if self.total_trades == 0:
            return 0.0
        return self.wins / self.total_trades

    @property
    def avg_win_pct(self) -> float:
        if self.wins == 0:
            return 0.0
        return self.total_win_dollars / self.wins

    @property
    def avg_loss_pct(self) -> float:
        if self.losses == 0:
            return 0.0
        return abs(self.total_loss_dollars) / self.losses

    @property
    def expectancy(self) -> float:
        """Expected value per trade."""
        if self.total_trades == 0:
            return 0.0
        return self.total_pnl_dollars / self.total_trades

    @property
    def profit_factor(self) -> float:
        """Gross profit / gross loss."""
        if self.total_loss_dollars == 0:
            return float("inf") if self.total_win_dollars > 0 else 0.0
        return abs(self.total_win_dollars / self.total_loss_dollars)

    def to_dict(self) -> dict[str, Any]:
        return {
            "strategy_name": self.strategy_name,
            "total_trades": self.total_trades,
            "wins": self.wins,
            "losses": self.losses,
            "win_rate": round(self.win_rate, 4),
            "expectancy": round(self.expectancy, 2),
            "profit_factor": round(self.profit_factor, 2),
            "total_pnl_dollars": round(self.total_pnl_dollars, 2),
            "current_weight": round(self.current_weight, 4),
            "streak": self.streak,
            "last_updated": self.last_updated.isoformat(),
        }
