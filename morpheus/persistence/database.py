"""
Database - SQLite connection and schema management.

Schema designed for:
- Append-only writes (crash-safe)
- Fast reads for EOD reporting
- Full audit trail of all decisions
"""

from __future__ import annotations

import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

logger = logging.getLogger(__name__)

# Schema version for migrations
SCHEMA_VERSION = 1

SCHEMA = """
-- Schema version tracking
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);

-- Signals: All detected trading signals
CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),

    -- Signal identification
    signal_id TEXT UNIQUE NOT NULL,
    symbol TEXT NOT NULL,
    strategy TEXT NOT NULL,
    direction TEXT NOT NULL,  -- 'long' or 'short'

    -- Signal context
    entry_price REAL,
    stop_price REAL,
    target_price REAL,

    -- Confidence and scoring
    raw_confidence REAL,
    ai_score REAL,
    final_score REAL,

    -- Metadata
    reasons TEXT,  -- JSON array of reasons
    market_regime TEXT,
    session TEXT,  -- 'pre_market', 'opening', 'regular', 'after_hours'

    -- Timestamps
    detected_at TEXT NOT NULL,

    -- Indexes for common queries
    UNIQUE(signal_id)
);
CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol);
CREATE INDEX IF NOT EXISTS idx_signals_detected_at ON signals(detected_at);
CREATE INDEX IF NOT EXISTS idx_signals_strategy ON signals(strategy);

-- Decisions: Gate and risk decisions on signals
CREATE TABLE IF NOT EXISTS decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),

    -- Link to signal
    signal_id TEXT NOT NULL,

    -- Decision details
    decision_type TEXT NOT NULL,  -- 'meta_gate', 'risk_gate', 'human_confirm'
    decision TEXT NOT NULL,  -- 'approved', 'rejected', 'timeout'

    -- Reasoning (critical for learning)
    reason TEXT,
    details TEXT,  -- JSON with full context

    -- Timestamps
    decided_at TEXT NOT NULL,

    FOREIGN KEY (signal_id) REFERENCES signals(signal_id)
);
CREATE INDEX IF NOT EXISTS idx_decisions_signal_id ON decisions(signal_id);
CREATE INDEX IF NOT EXISTS idx_decisions_type ON decisions(decision_type);

-- Trades: Executed and shadow trades
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),

    -- Trade identification
    trade_id TEXT UNIQUE NOT NULL,
    signal_id TEXT,  -- May be NULL for manual trades

    -- Trade details
    symbol TEXT NOT NULL,
    direction TEXT NOT NULL,
    trade_type TEXT NOT NULL,  -- 'shadow', 'probe', 'full'

    -- Execution
    entry_price REAL,
    entry_time TEXT,
    exit_price REAL,
    exit_time TEXT,
    shares INTEGER,

    -- Calculated fields
    pnl REAL,
    pnl_percent REAL,
    hold_time_seconds INTEGER,

    -- Slippage and spread analysis
    expected_entry REAL,
    actual_spread REAL,
    slippage REAL,

    -- Status
    status TEXT NOT NULL,  -- 'open', 'closed', 'cancelled'
    exit_reason TEXT,

    -- Metadata
    notes TEXT,

    FOREIGN KEY (signal_id) REFERENCES signals(signal_id)
);
CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol);
CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);
CREATE INDEX IF NOT EXISTS idx_trades_entry_time ON trades(entry_time);

-- Regimes: Market regime observations
CREATE TABLE IF NOT EXISTS regimes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),

    -- Regime details
    symbol TEXT,  -- NULL for market-wide regime
    regime_type TEXT NOT NULL,
    regime_value TEXT NOT NULL,
    confidence REAL,

    -- Context
    details TEXT,  -- JSON with indicators
    observed_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_regimes_symbol ON regimes(symbol);
CREATE INDEX IF NOT EXISTS idx_regimes_observed_at ON regimes(observed_at);

-- Watchlist: Symbol lifecycle tracking
CREATE TABLE IF NOT EXISTS watchlist (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),

    symbol TEXT NOT NULL,
    state TEXT NOT NULL,  -- 'new', 'active', 'stale', 'removed'

    -- Discovery context
    discovery_reason TEXT,
    discovery_score REAL,

    -- State transitions
    state_changed_at TEXT NOT NULL,
    previous_state TEXT,

    -- Current metrics
    rvol REAL,
    price_change_pct REAL,
    spread_avg REAL,

    notes TEXT
);
CREATE INDEX IF NOT EXISTS idx_watchlist_symbol ON watchlist(symbol);
CREATE INDEX IF NOT EXISTS idx_watchlist_state ON watchlist(state);

-- Daily summaries: EOD aggregated stats
CREATE TABLE IF NOT EXISTS daily_summaries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),

    trade_date TEXT NOT NULL UNIQUE,

    -- Signal stats
    signals_detected INTEGER DEFAULT 0,
    signals_approved INTEGER DEFAULT 0,
    signals_rejected INTEGER DEFAULT 0,

    -- Trade stats
    trades_executed INTEGER DEFAULT 0,
    trades_shadow INTEGER DEFAULT 0,
    trades_won INTEGER DEFAULT 0,
    trades_lost INTEGER DEFAULT 0,

    -- P&L
    total_pnl REAL DEFAULT 0,
    max_drawdown REAL DEFAULT 0,

    -- Rejection analysis
    rejection_reasons TEXT,  -- JSON: {reason: count}

    -- Market context
    market_regime_summary TEXT,

    notes TEXT
);
CREATE INDEX IF NOT EXISTS idx_daily_summaries_date ON daily_summaries(trade_date);
"""


class Database:
    """SQLite database manager with connection pooling."""

    def __init__(self, db_path: str | Path | None = None):
        """
        Initialize database.

        Args:
            db_path: Path to SQLite file. Defaults to ./data/morpheus.db
        """
        if db_path is None:
            db_path = Path("./data/morpheus.db")

        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self._initialized = False
        self._init_schema()

    def _init_schema(self) -> None:
        """Initialize database schema."""
        with self.connection() as conn:
            # Check if we need to initialize
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'"
            )
            if cursor.fetchone() is None:
                # Fresh database - create all tables
                conn.executescript(SCHEMA)
                conn.execute(
                    "INSERT INTO schema_version (version, applied_at) VALUES (?, ?)",
                    (SCHEMA_VERSION, datetime.now(timezone.utc).isoformat())
                )
                logger.info(f"Database initialized at {self.db_path} (schema v{SCHEMA_VERSION})")
            else:
                # Check version for migrations
                cursor = conn.execute("SELECT MAX(version) FROM schema_version")
                current_version = cursor.fetchone()[0] or 0
                if current_version < SCHEMA_VERSION:
                    self._migrate(conn, current_version, SCHEMA_VERSION)

        self._initialized = True

    def _migrate(self, conn: sqlite3.Connection, from_version: int, to_version: int) -> None:
        """Run schema migrations."""
        logger.info(f"Migrating database from v{from_version} to v{to_version}")
        # Add migration logic here as schema evolves
        # For now, just update version
        conn.execute(
            "INSERT INTO schema_version (version, applied_at) VALUES (?, ?)",
            (to_version, datetime.now(timezone.utc).isoformat())
        )

    @contextmanager
    def connection(self) -> Generator[sqlite3.Connection, None, None]:
        """Get a database connection with proper cleanup."""
        conn = sqlite3.connect(
            self.db_path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            timeout=30.0,
        )
        conn.row_factory = sqlite3.Row  # Enable dict-like access
        conn.execute("PRAGMA journal_mode=WAL")  # Better concurrency
        conn.execute("PRAGMA foreign_keys=ON")

        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def execute(self, query: str, params: tuple = ()) -> list[sqlite3.Row]:
        """Execute a query and return results."""
        with self.connection() as conn:
            cursor = conn.execute(query, params)
            return cursor.fetchall()

    def execute_many(self, query: str, params_list: list[tuple]) -> int:
        """Execute a query with multiple parameter sets."""
        with self.connection() as conn:
            cursor = conn.executemany(query, params_list)
            return cursor.rowcount

    def insert(self, query: str, params: tuple = ()) -> int:
        """Insert a row and return the last row ID."""
        with self.connection() as conn:
            cursor = conn.execute(query, params)
            return cursor.lastrowid
