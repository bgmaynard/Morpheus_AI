"""
Repositories - Clean interfaces for persisting signals, trades, and decisions.

Each repository provides:
- Type-safe insert/update methods
- Common query patterns for reporting
- Proper serialization of complex types
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone, date
from typing import Any

from morpheus.persistence.database import Database

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    """Get current UTC timestamp as ISO string."""
    return datetime.now(timezone.utc).isoformat()


def _generate_id() -> str:
    """Generate a unique ID."""
    return str(uuid.uuid4())[:8]


# =============================================================================
# Data Transfer Objects
# =============================================================================


@dataclass
class SignalRecord:
    """Signal data for persistence."""
    signal_id: str
    symbol: str
    strategy: str
    direction: str
    detected_at: str

    entry_price: float | None = None
    stop_price: float | None = None
    target_price: float | None = None

    raw_confidence: float | None = None
    ai_score: float | None = None
    final_score: float | None = None

    reasons: list[str] | None = None
    market_regime: str | None = None
    session: str | None = None


@dataclass
class DecisionRecord:
    """Decision data for persistence."""
    signal_id: str
    decision_type: str  # 'meta_gate', 'risk_gate', 'human_confirm'
    decision: str  # 'approved', 'rejected', 'timeout'
    decided_at: str

    reason: str | None = None
    details: dict[str, Any] | None = None


@dataclass
class TradeRecord:
    """Trade data for persistence."""
    trade_id: str
    symbol: str
    direction: str
    trade_type: str  # 'shadow', 'probe', 'full'
    status: str  # 'open', 'closed', 'cancelled'

    signal_id: str | None = None

    entry_price: float | None = None
    entry_time: str | None = None
    exit_price: float | None = None
    exit_time: str | None = None
    shares: int | None = None

    pnl: float | None = None
    pnl_percent: float | None = None
    hold_time_seconds: int | None = None

    expected_entry: float | None = None
    actual_spread: float | None = None
    slippage: float | None = None

    exit_reason: str | None = None
    notes: str | None = None


@dataclass
class WatchlistRecord:
    """Watchlist state data for persistence."""
    symbol: str
    state: str  # 'new', 'active', 'stale', 'removed'
    state_changed_at: str

    discovery_reason: str | None = None
    discovery_score: float | None = None
    previous_state: str | None = None

    rvol: float | None = None
    price_change_pct: float | None = None
    spread_avg: float | None = None

    notes: str | None = None


# =============================================================================
# Repositories
# =============================================================================


class SignalRepository:
    """Repository for signal persistence."""

    def __init__(self, db: Database):
        self.db = db

    def save(self, signal: SignalRecord) -> int:
        """Save a signal and return its row ID."""
        reasons_json = json.dumps(signal.reasons) if signal.reasons else None

        row_id = self.db.insert(
            """
            INSERT INTO signals (
                signal_id, symbol, strategy, direction, detected_at,
                entry_price, stop_price, target_price,
                raw_confidence, ai_score, final_score,
                reasons, market_regime, session
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                signal.signal_id, signal.symbol, signal.strategy, signal.direction,
                signal.detected_at, signal.entry_price, signal.stop_price,
                signal.target_price, signal.raw_confidence, signal.ai_score,
                signal.final_score, reasons_json, signal.market_regime, signal.session
            )
        )

        logger.debug(f"Saved signal {signal.signal_id} for {signal.symbol}")
        return row_id

    def update_score(self, signal_id: str, ai_score: float, final_score: float) -> None:
        """Update scoring for a signal."""
        self.db.execute(
            "UPDATE signals SET ai_score = ?, final_score = ? WHERE signal_id = ?",
            (ai_score, final_score, signal_id)
        )

    def get_by_id(self, signal_id: str) -> dict | None:
        """Get a signal by ID."""
        rows = self.db.execute(
            "SELECT * FROM signals WHERE signal_id = ?", (signal_id,)
        )
        if rows:
            row = dict(rows[0])
            if row.get('reasons'):
                row['reasons'] = json.loads(row['reasons'])
            return row
        return None

    def get_today_signals(self, symbol: str | None = None) -> list[dict]:
        """Get all signals from today."""
        today = date.today().isoformat()
        query = "SELECT * FROM signals WHERE DATE(detected_at) = ?"
        params: tuple = (today,)

        if symbol:
            query += " AND symbol = ?"
            params = (today, symbol)

        query += " ORDER BY detected_at DESC"

        rows = self.db.execute(query, params)
        return [dict(row) for row in rows]

    def get_by_date_range(
        self, start_date: str, end_date: str, symbol: str | None = None
    ) -> list[dict]:
        """Get signals in a date range."""
        query = "SELECT * FROM signals WHERE DATE(detected_at) BETWEEN ? AND ?"
        params: list = [start_date, end_date]

        if symbol:
            query += " AND symbol = ?"
            params.append(symbol)

        query += " ORDER BY detected_at DESC"

        rows = self.db.execute(query, tuple(params))
        return [dict(row) for row in rows]

    def count_by_strategy(self, start_date: str | None = None) -> dict[str, int]:
        """Count signals grouped by strategy."""
        query = "SELECT strategy, COUNT(*) as count FROM signals"
        params: tuple = ()

        if start_date:
            query += " WHERE DATE(detected_at) >= ?"
            params = (start_date,)

        query += " GROUP BY strategy"

        rows = self.db.execute(query, params)
        return {row['strategy']: row['count'] for row in rows}


class DecisionRepository:
    """Repository for decision persistence."""

    def __init__(self, db: Database):
        self.db = db

    def save(self, decision: DecisionRecord) -> int:
        """Save a decision and return its row ID."""
        details_json = json.dumps(decision.details) if decision.details else None

        row_id = self.db.insert(
            """
            INSERT INTO decisions (
                signal_id, decision_type, decision, reason, details, decided_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                decision.signal_id, decision.decision_type, decision.decision,
                decision.reason, details_json, decision.decided_at
            )
        )

        logger.debug(
            f"Saved {decision.decision_type} decision for signal {decision.signal_id}: "
            f"{decision.decision}"
        )
        return row_id

    def get_for_signal(self, signal_id: str) -> list[dict]:
        """Get all decisions for a signal."""
        rows = self.db.execute(
            "SELECT * FROM decisions WHERE signal_id = ? ORDER BY decided_at",
            (signal_id,)
        )
        return [dict(row) for row in rows]

    def get_rejections_today(self) -> list[dict]:
        """Get all rejections from today with reasons."""
        today = date.today().isoformat()
        rows = self.db.execute(
            """
            SELECT d.*, s.symbol, s.strategy
            FROM decisions d
            JOIN signals s ON d.signal_id = s.signal_id
            WHERE DATE(d.decided_at) = ? AND d.decision = 'rejected'
            ORDER BY d.decided_at DESC
            """,
            (today,)
        )
        return [dict(row) for row in rows]

    def count_rejection_reasons(self, start_date: str | None = None) -> dict[str, int]:
        """Count rejections grouped by reason."""
        query = "SELECT reason, COUNT(*) as count FROM decisions WHERE decision = 'rejected'"
        params: tuple = ()

        if start_date:
            query += " AND DATE(decided_at) >= ?"
            params = (start_date,)

        query += " GROUP BY reason ORDER BY count DESC"

        rows = self.db.execute(query, params)
        return {row['reason'] or 'unknown': row['count'] for row in rows}


class TradeRepository:
    """Repository for trade persistence."""

    def __init__(self, db: Database):
        self.db = db

    def save(self, trade: TradeRecord) -> int:
        """Save a trade and return its row ID."""
        row_id = self.db.insert(
            """
            INSERT INTO trades (
                trade_id, signal_id, symbol, direction, trade_type, status,
                entry_price, entry_time, exit_price, exit_time, shares,
                pnl, pnl_percent, hold_time_seconds,
                expected_entry, actual_spread, slippage,
                exit_reason, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                trade.trade_id, trade.signal_id, trade.symbol, trade.direction,
                trade.trade_type, trade.status, trade.entry_price, trade.entry_time,
                trade.exit_price, trade.exit_time, trade.shares, trade.pnl,
                trade.pnl_percent, trade.hold_time_seconds, trade.expected_entry,
                trade.actual_spread, trade.slippage, trade.exit_reason, trade.notes
            )
        )

        logger.debug(f"Saved trade {trade.trade_id} for {trade.symbol}")
        return row_id

    def update_exit(
        self,
        trade_id: str,
        exit_price: float,
        exit_time: str,
        pnl: float,
        pnl_percent: float,
        hold_time_seconds: int,
        exit_reason: str,
        slippage: float | None = None,
    ) -> None:
        """Update a trade with exit information."""
        self.db.execute(
            """
            UPDATE trades SET
                exit_price = ?, exit_time = ?, pnl = ?, pnl_percent = ?,
                hold_time_seconds = ?, exit_reason = ?, slippage = ?,
                status = 'closed'
            WHERE trade_id = ?
            """,
            (exit_price, exit_time, pnl, pnl_percent, hold_time_seconds,
             exit_reason, slippage, trade_id)
        )

    def get_open_trades(self) -> list[dict]:
        """Get all open trades."""
        rows = self.db.execute(
            "SELECT * FROM trades WHERE status = 'open' ORDER BY entry_time"
        )
        return [dict(row) for row in rows]

    def get_today_trades(self, trade_type: str | None = None) -> list[dict]:
        """Get all trades from today."""
        today = date.today().isoformat()
        query = "SELECT * FROM trades WHERE DATE(entry_time) = ?"
        params: list = [today]

        if trade_type:
            query += " AND trade_type = ?"
            params.append(trade_type)

        query += " ORDER BY entry_time DESC"

        rows = self.db.execute(query, tuple(params))
        return [dict(row) for row in rows]

    def get_pnl_summary(self, start_date: str, end_date: str) -> dict:
        """Get P&L summary for a date range."""
        rows = self.db.execute(
            """
            SELECT
                COUNT(*) as total_trades,
                SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as winning_trades,
                SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) as losing_trades,
                SUM(pnl) as total_pnl,
                AVG(pnl) as avg_pnl,
                MAX(pnl) as best_trade,
                MIN(pnl) as worst_trade,
                AVG(hold_time_seconds) as avg_hold_time
            FROM trades
            WHERE DATE(entry_time) BETWEEN ? AND ?
            AND status = 'closed'
            """,
            (start_date, end_date)
        )

        if rows:
            row = dict(rows[0])
            # Calculate win rate
            total = row.get('total_trades') or 0
            wins = row.get('winning_trades') or 0
            row['win_rate'] = (wins / total * 100) if total > 0 else 0
            return row

        return {
            'total_trades': 0, 'winning_trades': 0, 'losing_trades': 0,
            'total_pnl': 0, 'avg_pnl': 0, 'best_trade': 0, 'worst_trade': 0,
            'avg_hold_time': 0, 'win_rate': 0
        }


class WatchlistRepository:
    """Repository for watchlist state persistence."""

    def __init__(self, db: Database):
        self.db = db

    def save_state_change(self, record: WatchlistRecord) -> int:
        """Save a watchlist state change."""
        row_id = self.db.insert(
            """
            INSERT INTO watchlist (
                symbol, state, state_changed_at, discovery_reason, discovery_score,
                previous_state, rvol, price_change_pct, spread_avg, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.symbol, record.state, record.state_changed_at,
                record.discovery_reason, record.discovery_score, record.previous_state,
                record.rvol, record.price_change_pct, record.spread_avg, record.notes
            )
        )

        logger.debug(f"Saved watchlist state: {record.symbol} -> {record.state}")
        return row_id

    def get_current_state(self, symbol: str) -> str | None:
        """Get current state of a symbol."""
        rows = self.db.execute(
            """
            SELECT state FROM watchlist
            WHERE symbol = ?
            ORDER BY state_changed_at DESC
            LIMIT 1
            """,
            (symbol,)
        )
        return rows[0]['state'] if rows else None

    def get_active_symbols(self) -> list[str]:
        """Get all symbols currently in 'active' state."""
        rows = self.db.execute(
            """
            SELECT DISTINCT symbol FROM watchlist w1
            WHERE state = 'active'
            AND state_changed_at = (
                SELECT MAX(state_changed_at) FROM watchlist w2
                WHERE w2.symbol = w1.symbol
            )
            """
        )
        return [row['symbol'] for row in rows]

    def get_state_history(self, symbol: str, limit: int = 10) -> list[dict]:
        """Get state history for a symbol."""
        rows = self.db.execute(
            """
            SELECT * FROM watchlist
            WHERE symbol = ?
            ORDER BY state_changed_at DESC
            LIMIT ?
            """,
            (symbol, limit)
        )
        return [dict(row) for row in rows]


class RegimeRepository:
    """Repository for regime observation persistence."""

    def __init__(self, db: Database):
        self.db = db

    def save(
        self,
        regime_type: str,
        regime_value: str,
        confidence: float | None = None,
        symbol: str | None = None,
        details: dict | None = None,
    ) -> int:
        """Save a regime observation."""
        details_json = json.dumps(details) if details else None

        row_id = self.db.insert(
            """
            INSERT INTO regimes (
                symbol, regime_type, regime_value, confidence, details, observed_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (symbol, regime_type, regime_value, confidence, details_json, _now_iso())
        )

        logger.debug(f"Saved regime: {regime_type}={regime_value} for {symbol or 'market'}")
        return row_id

    def get_latest(self, regime_type: str, symbol: str | None = None) -> dict | None:
        """Get the most recent regime of a type."""
        query = "SELECT * FROM regimes WHERE regime_type = ?"
        params: list = [regime_type]

        if symbol:
            query += " AND symbol = ?"
            params.append(symbol)
        else:
            query += " AND symbol IS NULL"

        query += " ORDER BY observed_at DESC LIMIT 1"

        rows = self.db.execute(query, tuple(params))
        return dict(rows[0]) if rows else None

    def get_today_regimes(self) -> list[dict]:
        """Get all regime observations from today."""
        today = date.today().isoformat()
        rows = self.db.execute(
            "SELECT * FROM regimes WHERE DATE(observed_at) = ? ORDER BY observed_at",
            (today,)
        )
        return [dict(row) for row in rows]


class DailySummaryRepository:
    """Repository for daily summary persistence."""

    def __init__(self, db: Database):
        self.db = db

    def save_or_update(
        self,
        trade_date: str,
        signals_detected: int = 0,
        signals_approved: int = 0,
        signals_rejected: int = 0,
        trades_executed: int = 0,
        trades_shadow: int = 0,
        trades_won: int = 0,
        trades_lost: int = 0,
        total_pnl: float = 0,
        max_drawdown: float = 0,
        rejection_reasons: dict | None = None,
        market_regime_summary: str | None = None,
        notes: str | None = None,
    ) -> None:
        """Save or update daily summary."""
        rejection_json = json.dumps(rejection_reasons) if rejection_reasons else None

        self.db.execute(
            """
            INSERT INTO daily_summaries (
                trade_date, signals_detected, signals_approved, signals_rejected,
                trades_executed, trades_shadow, trades_won, trades_lost,
                total_pnl, max_drawdown, rejection_reasons, market_regime_summary, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(trade_date) DO UPDATE SET
                signals_detected = excluded.signals_detected,
                signals_approved = excluded.signals_approved,
                signals_rejected = excluded.signals_rejected,
                trades_executed = excluded.trades_executed,
                trades_shadow = excluded.trades_shadow,
                trades_won = excluded.trades_won,
                trades_lost = excluded.trades_lost,
                total_pnl = excluded.total_pnl,
                max_drawdown = excluded.max_drawdown,
                rejection_reasons = excluded.rejection_reasons,
                market_regime_summary = excluded.market_regime_summary,
                notes = excluded.notes
            """,
            (
                trade_date, signals_detected, signals_approved, signals_rejected,
                trades_executed, trades_shadow, trades_won, trades_lost,
                total_pnl, max_drawdown, rejection_json, market_regime_summary, notes
            )
        )

    def get(self, trade_date: str) -> dict | None:
        """Get summary for a specific date."""
        rows = self.db.execute(
            "SELECT * FROM daily_summaries WHERE trade_date = ?",
            (trade_date,)
        )
        return dict(rows[0]) if rows else None

    def get_range(self, start_date: str, end_date: str) -> list[dict]:
        """Get summaries for a date range."""
        rows = self.db.execute(
            """
            SELECT * FROM daily_summaries
            WHERE trade_date BETWEEN ? AND ?
            ORDER BY trade_date
            """,
            (start_date, end_date)
        )
        return [dict(row) for row in rows]
