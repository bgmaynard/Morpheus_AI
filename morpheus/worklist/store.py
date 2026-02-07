"""
Worklist Store - Persistent storage for tradeable symbols.

The worklist is the ONLY source Morpheus trades from.
Persisted to JSONL files at data/worklist/YYYY-MM-DD.worklist

Lifecycle:
    - Symbols enter via scanner or news
    - Scored and ranked
    - Selected for trading
    - Expired at session end
"""

from __future__ import annotations

import json
import logging
import os
import shutil
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, date
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Awaitable, Optional
from threading import Lock

logger = logging.getLogger(__name__)

# Worklist storage location
WORKLIST_DIR = Path("data/worklist")
ARCHIVE_DIR = Path("data/worklist/archive")


class WorklistStatus(Enum):
    """Symbol status in worklist lifecycle."""
    CANDIDATE = "candidate"      # Passed scrutiny, awaiting selection
    ACTIVE = "active"            # Selected for active monitoring/trading
    TRADED = "traded"            # Already traded this session
    EXPIRED = "expired"          # Removed due to staleness/decay
    REJECTED = "rejected"        # Failed scrutiny or risk checks


class MomentumState(Enum):
    """
    Momentum state classification - mirrors professional scanner reasoning.

    This helps operators quickly understand WHY a symbol is on the worklist.
    """
    RUNNING_UP = "running_up"      # Active momentum, multiple alerts
    SQUEEZE = "squeeze"            # Short squeeze pattern (high short %, low float, high RVOL)
    HALTED = "halted"              # Currently in trading halt
    GAP_OPEN = "gap_open"          # Gap up/down at open, watching for continuation
    BREAKOUT = "breakout"          # Breaking key level (HOD, resistance)
    CATALYST = "catalyst"          # News-driven move
    UNKNOWN = "unknown"            # Default state


class WorklistSource(Enum):
    """How the symbol entered the worklist."""
    SCANNER = "scanner"          # From MAX_AI scanner
    NEWS = "news"                # From news/alert feed
    MANUAL = "manual"            # Manually added (debugging only)


@dataclass
class WorklistEntry:
    """
    A symbol entry in the worklist.

    This is the canonical representation of a tradeable symbol.
    All trading decisions must reference this structure.

    Enhanced to mirror professional scanner cognitive model:
    - Momentum state classification
    - Multiple RVOL timeframes
    - Halt tracking
    - Trigger context
    """

    # Identity
    symbol: str
    session_date: str  # YYYY-MM-DD format

    # Timestamps
    first_seen_ts: str  # ISO-8601
    last_update_ts: str  # ISO-8601

    # Source tracking
    source: str  # WorklistSource value

    # Scanner data
    scanner_score: float = 0.0  # MAX_AI ai_score (0-100)
    gap_pct: float = 0.0
    rvol: float = 0.0           # Daily RVOL
    rvol_5m: float = 0.0        # 5-minute RVOL (recent velocity)
    volume: int = 0
    price: float = 0.0

    # Extended scanner data (for operator visibility)
    float_shares: Optional[float] = None  # Float size in millions
    avg_volume: Optional[int] = None      # Average daily volume
    short_pct: Optional[float] = None     # Short interest %
    market_cap: Optional[float] = None    # Market cap in millions
    spread_pct: Optional[float] = None    # Bid-ask spread %

    # Halt tracking
    halt_status: Optional[str] = None     # HALTED, RESUMED, None
    halt_ts: Optional[str] = None         # When halt occurred/resumed

    # News data (scanner news indicator is authoritative)
    news_present: bool = False            # Simple flag for UI (from scanner_news_indicator)
    scanner_news_indicator: bool = False  # Authoritative: scanner says there's news
    news_score: Optional[float] = None
    news_headline: Optional[str] = None
    news_category: Optional[str] = None   # For negative category detection
    news_ts: Optional[str] = None
    news_hits: int = 0                    # Reinforcement counter from news feed

    # Momentum state classification (professional scanner alignment)
    momentum_state: str = "unknown"       # MomentumState value
    trigger_context: list[str] = field(default_factory=list)  # GAP, RUNNING_UP, HALT, NEWS, SQUEEZE, LOW_FLOAT
    trigger_reason: str = ""              # Human-readable reason symbol is interesting
    alert_count: int = 0                  # Number of times this symbol triggered alerts

    # Computed scores
    combined_priority_score: float = 0.0

    # Context
    regime: str = ""  # Current MASS regime
    strategy_eligible: list[str] = field(default_factory=list)

    # Status
    status: str = "candidate"  # WorklistStatus value

    # Trading info
    traded_at: Optional[str] = None
    trade_result: Optional[str] = None

    # Rejection info
    rejection_reason: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict for serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "WorklistEntry":
        """Create from dict (deserialization)."""
        # Handle legacy data without all fields
        return cls(
            symbol=data.get("symbol", ""),
            session_date=data.get("session_date", ""),
            first_seen_ts=data.get("first_seen_ts", ""),
            last_update_ts=data.get("last_update_ts", ""),
            source=data.get("source", "scanner"),
            scanner_score=data.get("scanner_score", 0.0),
            gap_pct=data.get("gap_pct", 0.0),
            rvol=data.get("rvol", 0.0),
            rvol_5m=data.get("rvol_5m", 0.0),
            volume=data.get("volume", 0),
            price=data.get("price", 0.0),
            # Extended scanner data
            float_shares=data.get("float_shares"),
            avg_volume=data.get("avg_volume"),
            short_pct=data.get("short_pct"),
            market_cap=data.get("market_cap"),
            spread_pct=data.get("spread_pct"),
            # Halt tracking
            halt_status=data.get("halt_status"),
            halt_ts=data.get("halt_ts"),
            # News data
            news_present=data.get("news_present", False),
            scanner_news_indicator=data.get("scanner_news_indicator", False),
            news_score=data.get("news_score"),
            news_headline=data.get("news_headline"),
            news_category=data.get("news_category"),
            news_ts=data.get("news_ts"),
            news_hits=data.get("news_hits", 0),
            # Momentum state
            momentum_state=data.get("momentum_state", "unknown"),
            trigger_context=data.get("trigger_context", []),
            trigger_reason=data.get("trigger_reason", ""),
            alert_count=data.get("alert_count", 0),
            # Scores and context
            combined_priority_score=data.get("combined_priority_score", 0.0),
            regime=data.get("regime", ""),
            strategy_eligible=data.get("strategy_eligible", []),
            status=data.get("status", "candidate"),
            traded_at=data.get("traded_at"),
            trade_result=data.get("trade_result"),
            rejection_reason=data.get("rejection_reason"),
        )

    def to_jsonl(self) -> str:
        """Convert to JSONL line."""
        return json.dumps(self.to_dict())


class WorklistStore:
    """
    Persistent worklist storage manager.

    Responsibilities:
        - Store/retrieve worklist entries
        - Persist to JSONL files
        - Handle session resets
        - Provide query capabilities

    Thread-safe for concurrent access.
    """

    def __init__(
        self,
        emit_event: Optional[Callable[[Any], Awaitable[None]]] = None,
    ):
        """
        Initialize worklist store.

        Args:
            emit_event: Optional callback for emitting events
        """
        self._emit_event = emit_event
        self._entries: dict[str, WorklistEntry] = {}  # symbol -> entry
        self._lock = Lock()
        self._session_date = self._get_session_date()
        self._worklist_path = WORKLIST_DIR / f"{self._session_date}.worklist"

        # Counters for observability
        self._symbols_added_today = 0
        self._symbols_expired = 0
        self._symbols_traded = 0
        self._scanner_rows_received = 0
        self._news_events_received = 0

        # Ensure directories exist
        WORKLIST_DIR.mkdir(parents=True, exist_ok=True)
        ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

        # Load existing worklist for today if it exists
        self._load_worklist()

        logger.info(f"[WORKLIST] Store initialized for session {self._session_date}")
        logger.info(f"[WORKLIST] Loaded {len(self._entries)} existing entries")

    def _get_session_date(self) -> str:
        """Get current session date in YYYY-MM-DD format."""
        # Use Eastern time for session boundaries
        try:
            import pytz
            eastern = pytz.timezone('US/Eastern')
            now_et = datetime.now(eastern)
            return now_et.strftime("%Y-%m-%d")
        except ImportError:
            return datetime.now().strftime("%Y-%m-%d")

    def _load_worklist(self) -> None:
        """Load worklist from disk if it exists for today."""
        if not self._worklist_path.exists():
            return

        try:
            with open(self._worklist_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        entry = WorklistEntry.from_dict(data)
                        # Only load entries from today's session
                        if entry.session_date == self._session_date:
                            self._entries[entry.symbol] = entry
                    except json.JSONDecodeError:
                        logger.warning(f"[WORKLIST] Skipping invalid line: {line[:50]}")
        except Exception as e:
            logger.error(f"[WORKLIST] Error loading worklist: {e}")

    def _save_entry(self, entry: WorklistEntry) -> None:
        """Append entry to worklist file."""
        try:
            with open(self._worklist_path, "a") as f:
                f.write(entry.to_jsonl() + "\n")
        except Exception as e:
            logger.error(f"[WORKLIST] Error saving entry {entry.symbol}: {e}")

    def _rewrite_worklist(self) -> None:
        """Rewrite entire worklist file (for updates)."""
        try:
            with open(self._worklist_path, "w") as f:
                for entry in self._entries.values():
                    f.write(entry.to_jsonl() + "\n")
        except Exception as e:
            logger.error(f"[WORKLIST] Error rewriting worklist: {e}")

    # =========================================================================
    # PUBLIC API
    # =========================================================================

    def add(self, entry: WorklistEntry) -> bool:
        """
        Add or update a symbol in the worklist.

        Args:
            entry: WorklistEntry to add

        Returns:
            True if new entry, False if updated existing
        """
        with self._lock:
            is_new = entry.symbol not in self._entries
            self._entries[entry.symbol] = entry

            if is_new:
                self._symbols_added_today += 1
                self._save_entry(entry)
                logger.info(
                    f"[WORKLIST] Added {entry.symbol}: "
                    f"score={entry.combined_priority_score:.1f}, "
                    f"source={entry.source}"
                )
            else:
                # For updates, rewrite file periodically (or use WAL)
                self._rewrite_worklist()
                logger.debug(f"[WORKLIST] Updated {entry.symbol}")

            return is_new

    def get(self, symbol: str) -> Optional[WorklistEntry]:
        """Get entry by symbol."""
        with self._lock:
            return self._entries.get(symbol)

    def exists(self, symbol: str) -> bool:
        """Check if symbol exists in worklist."""
        with self._lock:
            return symbol in self._entries

    def is_tradeable(self, symbol: str) -> bool:
        """
        Check if symbol is eligible for trading.

        Must exist AND have valid status (CANDIDATE or ACTIVE).
        """
        with self._lock:
            entry = self._entries.get(symbol)
            if not entry:
                return False
            if entry.session_date != self._session_date:
                return False
            return entry.status in (
                WorklistStatus.CANDIDATE.value,
                WorklistStatus.ACTIVE.value,
            )

    def get_all(self) -> list[WorklistEntry]:
        """Get all entries."""
        with self._lock:
            return list(self._entries.values())

    def get_candidates(self, min_score: float = 0.0) -> list[WorklistEntry]:
        """
        Get tradeable candidates sorted by priority score.

        Args:
            min_score: Minimum combined_priority_score

        Returns:
            List of entries sorted by score (highest first)
        """
        with self._lock:
            candidates = [
                e for e in self._entries.values()
                if e.status in (WorklistStatus.CANDIDATE.value, WorklistStatus.ACTIVE.value)
                and e.combined_priority_score >= min_score
                and e.session_date == self._session_date
            ]
            return sorted(
                candidates,
                key=lambda x: x.combined_priority_score,
                reverse=True,
            )

    def get_top_n(self, n: int = 10, min_score: float = 0.0) -> list[WorklistEntry]:
        """Get top N candidates by score."""
        return self.get_candidates(min_score)[:n]

    def update_status(
        self,
        symbol: str,
        status: WorklistStatus,
        reason: Optional[str] = None,
    ) -> bool:
        """
        Update symbol status.

        Args:
            symbol: Symbol to update
            status: New status
            reason: Optional reason (for rejections/expirations)

        Returns:
            True if updated, False if symbol not found
        """
        with self._lock:
            entry = self._entries.get(symbol)
            if not entry:
                return False

            old_status = entry.status
            entry.status = status.value
            entry.last_update_ts = datetime.now(timezone.utc).isoformat()

            if status == WorklistStatus.REJECTED:
                entry.rejection_reason = reason
            elif status == WorklistStatus.EXPIRED:
                self._symbols_expired += 1
            elif status == WorklistStatus.TRADED:
                self._symbols_traded += 1
                entry.traded_at = datetime.now(timezone.utc).isoformat()

            self._rewrite_worklist()
            logger.info(f"[WORKLIST] {symbol}: {old_status} -> {status.value}")
            return True

    def mark_traded(self, symbol: str, result: Optional[str] = None) -> bool:
        """Mark symbol as traded."""
        with self._lock:
            entry = self._entries.get(symbol)
            if not entry:
                return False

            entry.status = WorklistStatus.TRADED.value
            entry.traded_at = datetime.now(timezone.utc).isoformat()
            entry.trade_result = result
            self._symbols_traded += 1

            self._rewrite_worklist()
            logger.info(f"[WORKLIST] {symbol} marked as TRADED")
            return True

    def increment_scanner_count(self) -> None:
        """Increment scanner rows received counter."""
        self._scanner_rows_received += 1

    def increment_news_count(self) -> None:
        """Increment news events received counter."""
        self._news_events_received += 1

    # =========================================================================
    # SESSION MANAGEMENT
    # =========================================================================

    def reset_session(self, archive: bool = True) -> dict[str, Any]:
        """
        Reset worklist for new session.

        Archives yesterday's worklist and creates empty one for today.

        Args:
            archive: Whether to archive old worklist

        Returns:
            Summary of reset operation
        """
        with self._lock:
            old_session = self._session_date
            old_count = len(self._entries)
            old_path = self._worklist_path

            # Archive old worklist
            if archive and old_path.exists():
                archive_path = ARCHIVE_DIR / f"{old_session}.worklist"
                try:
                    shutil.move(str(old_path), str(archive_path))
                    logger.info(f"[WORKLIST] Archived {old_path} -> {archive_path}")
                except Exception as e:
                    logger.error(f"[WORKLIST] Archive failed: {e}")

            # Reset state
            self._entries.clear()
            self._session_date = self._get_session_date()
            self._worklist_path = WORKLIST_DIR / f"{self._session_date}.worklist"

            # Reset counters
            old_stats = {
                "symbols_added": self._symbols_added_today,
                "symbols_expired": self._symbols_expired,
                "symbols_traded": self._symbols_traded,
                "scanner_rows": self._scanner_rows_received,
                "news_events": self._news_events_received,
            }

            self._symbols_added_today = 0
            self._symbols_expired = 0
            self._symbols_traded = 0
            self._scanner_rows_received = 0
            self._news_events_received = 0

            summary = {
                "old_session": old_session,
                "new_session": self._session_date,
                "archived_count": old_count,
                "old_stats": old_stats,
            }

            logger.info(
                f"[WORKLIST] Session reset: {old_session} -> {self._session_date}, "
                f"archived {old_count} symbols"
            )

            return summary

    def check_session_date(self) -> bool:
        """
        Check if current session date matches stored date.

        Returns:
            True if session is current, False if stale
        """
        current = self._get_session_date()
        return self._session_date == current

    # =========================================================================
    # OBSERVABILITY
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        """Get worklist status for observability."""
        with self._lock:
            status_counts = {}
            for entry in self._entries.values():
                status_counts[entry.status] = status_counts.get(entry.status, 0) + 1

            return {
                "session_date": self._session_date,
                "worklist_size": len(self._entries),
                "symbols_added_today": self._symbols_added_today,
                "symbols_expired": self._symbols_expired,
                "symbols_traded": self._symbols_traded,
                "scanner_rows_received": self._scanner_rows_received,
                "news_events_received": self._news_events_received,
                "status_breakdown": status_counts,
                "worklist_path": str(self._worklist_path),
                "is_session_current": self.check_session_date(),
            }

    def get_summary(self) -> str:
        """Get human-readable summary."""
        status = self.get_status()
        return (
            f"Worklist {status['session_date']}: "
            f"{status['worklist_size']} symbols, "
            f"{status['symbols_traded']} traded, "
            f"{status['scanner_rows_received']} scanner rows"
        )


# Global instance
_global_store: Optional[WorklistStore] = None


def get_worklist_store(
    emit_event: Optional[Callable[[Any], Awaitable[None]]] = None,
) -> WorklistStore:
    """Get or create global worklist store."""
    global _global_store
    if _global_store is None:
        _global_store = WorklistStore(emit_event=emit_event)
    return _global_store


def reset_worklist_store() -> None:
    """Reset global worklist store (for testing)."""
    global _global_store
    _global_store = None
