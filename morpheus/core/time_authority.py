"""
Market Time Authority - Single Source of Truth
===============================================
If time is wrong, everything is wrong.
Morpheus trades market phases, not clocks.

This module is the ONLY place where:
- Current time is resolved
- Timezone is handled
- Broker timestamps are ingested
- Drift is tracked

NO module outside this file may call datetime.now() or time.time() for market time.
AI models never own time. They only reason over timestamps provided by this system.
Time is infrastructure, not intelligence.

Primary source: Broker timestamps (Schwab quote_time/trade_time ms)
Fallback:       System clock in US/Eastern
"""

import logging
import threading
import time as _time
from datetime import datetime, time, timedelta, timezone
from typing import Optional, Dict, Any
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

# Canonical Eastern timezone - defined once, used everywhere
ET = ZoneInfo("America/New_York")
UTC = timezone.utc


class TimeAuthority:
    """
    Single source of truth for all market time in the trading bot.

    Thread-safe singleton. All public methods return timezone-aware ET datetimes.

    Usage:
        from morpheus.core.time_authority import now, now_iso, source
        current = now()          # tz-aware ET datetime
        ts = now_iso()           # ISO 8601 string
        src = source()           # "broker_schwab" | "system_clock"
    """

    _instance = None
    _init_lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._init_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True

        self._et_tz = ET
        self._utc = UTC

        # Broker timestamp state (protected by lock)
        self._lock = threading.Lock()
        self._last_broker_ts: Optional[datetime] = None
        self._last_broker_source: str = ""
        self._last_broker_update: float = 0.0  # time.monotonic()
        self._broker_update_count: int = 0

        # Drift tracking
        self._drift_ms: float = 0.0
        self._max_drift_ms: float = 0.0
        self._drift_warning_threshold_ms: float = 500.0
        self._drift_halt_threshold_ms: float = 1000.0

        # Staleness threshold (seconds) - fall back to system clock if broker data older
        self._stale_threshold_s: float = 30.0

        # Boot time
        self._boot_time = _time.monotonic()
        self._boot_datetime = datetime.now(self._et_tz)

        logger.info("TimeAuthority initialized (source: system_clock)")

    # ─── Core Time Methods ───────────────────────────────────────────────

    def now(self) -> datetime:
        """
        Returns current authoritative datetime (timezone-aware, US/Eastern).

        Uses broker-corrected time if available and fresh (<30s old).
        Falls back to system clock otherwise.
        """
        system_et = datetime.now(self._et_tz)

        with self._lock:
            if self._last_broker_ts is None:
                return system_et

            elapsed = _time.monotonic() - self._last_broker_update

            if elapsed > self._stale_threshold_s:
                # Broker data is stale - use system clock
                return system_et

            # Broker-corrected time: last known broker time + elapsed since update
            corrected = self._last_broker_ts + timedelta(seconds=elapsed)

            # Calculate drift
            drift = abs((corrected - system_et).total_seconds() * 1000)
            self._drift_ms = drift
            if drift > self._max_drift_ms:
                self._max_drift_ms = drift

            if drift > self._drift_halt_threshold_ms:
                logger.error(
                    f"TIME_DRIFT_CRITICAL: {drift:.0f}ms - "
                    f"source={self._last_broker_source} - blocking entries"
                )
            elif drift > self._drift_warning_threshold_ms:
                logger.warning(f"TIME_DRIFT_WARNING: {drift:.0f}ms - source={self._last_broker_source}")

            return corrected

    def now_utc(self) -> datetime:
        """Returns current authoritative datetime in UTC."""
        return self.now().astimezone(self._utc)

    def now_iso(self) -> str:
        """Returns ISO-8601 string with ET offset."""
        return self.now().isoformat()

    def now_utc_iso(self) -> str:
        """Returns ISO-8601 string in UTC (for event timestamps)."""
        return self.now_utc().isoformat()

    def now_time(self) -> time:
        """Returns current ET time-of-day (for session boundary checks)."""
        return self.now().time()

    def now_hhmm(self) -> int:
        """Returns HHMM integer (e.g., 930 for 9:30 AM) for fast comparisons."""
        t = self.now()
        return t.hour * 100 + t.minute

    def now_date(self) -> datetime:
        """Returns current ET date as date object."""
        return self.now().date()

    # ─── Source & Drift ──────────────────────────────────────────────────

    def source(self) -> str:
        """Returns current time source: 'broker_schwab' or 'system_clock'."""
        with self._lock:
            if self._last_broker_ts is None:
                return "system_clock"

            elapsed = _time.monotonic() - self._last_broker_update
            if elapsed > self._stale_threshold_s:
                return "system_clock"

            return f"broker_{self._last_broker_source}"

    def drift_ms(self) -> float:
        """Returns current drift in milliseconds between broker and system clock."""
        return self._drift_ms

    def is_drift_safe(self) -> bool:
        """True if drift < halt threshold (1000ms). Safe to trade."""
        # If no broker data, assume safe (system clock is all we have)
        if self.source() == "system_clock":
            return True
        return self._drift_ms < self._drift_halt_threshold_ms

    # ─── Broker Timestamp Ingestion ──────────────────────────────────────

    def update_from_broker(self, ts: datetime, source: str) -> None:
        """
        Called whenever a broker timestamp is received.
        ts must be timezone-aware (ET or UTC - will be converted to ET).
        """
        if ts is None:
            return

        # Convert to ET if needed
        if ts.tzinfo is None:
            # Assume ET for naive datetimes
            ts = ts.replace(tzinfo=self._et_tz)
        else:
            ts = ts.astimezone(self._et_tz)

        with self._lock:
            self._last_broker_ts = ts
            self._last_broker_source = source
            self._last_broker_update = _time.monotonic()
            self._broker_update_count += 1

    def update_from_schwab_epoch_ms(self, epoch_ms: int | float) -> None:
        """
        Called by Schwab streaming handler on every quote tick.
        Converts UTC millisecond epoch (from QUOTE_TIME_MS / TRADE_TIME_MS) to ET.
        """
        if not epoch_ms or epoch_ms <= 0:
            return

        try:
            utc_dt = datetime.fromtimestamp(epoch_ms / 1000.0, tz=self._utc)
            et_dt = utc_dt.astimezone(self._et_tz)
            self.update_from_broker(et_dt, "schwab")
        except (ValueError, OSError, OverflowError):
            pass  # Invalid epoch - ignore silently

    def update_from_schwab_quote(self, quote_time: datetime | None = None,
                                  trade_time: datetime | None = None) -> None:
        """
        Called with parsed QuoteUpdate timestamps.
        Prefers trade_time (most recent), falls back to quote_time.
        """
        ts = trade_time or quote_time
        if ts is not None:
            self.update_from_broker(ts, "schwab")

    def update_from_iso(self, iso_str: str, source: str = "schwab") -> None:
        """
        Called with an ISO 8601 timestamp string.
        """
        if not iso_str:
            return

        try:
            dt = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=self._et_tz)
            self.update_from_broker(dt, source)
        except (ValueError, TypeError):
            pass

    # ─── Event Enrichment ────────────────────────────────────────────────

    def enrich_event(self, event: dict) -> dict:
        """
        Add standard time fields to any event dict.
        Used by event loggers, trade records, gating decisions.
        """
        event["timestamp_et"] = self.now_iso()
        event["time_source"] = self.source()
        return event

    # ─── Status / API ────────────────────────────────────────────────────

    def get_status(self) -> Dict[str, Any]:
        """Full status for /api/time/status endpoint."""
        now_et = self.now()
        system_et = datetime.now(self._et_tz)
        src = self.source()

        with self._lock:
            broker_age_s = (
                _time.monotonic() - self._last_broker_update
                if self._last_broker_ts is not None
                else None
            )
            broker_count = self._broker_update_count
            broker_src = self._last_broker_source

        return {
            "now_et": now_et.isoformat(),
            "now_display": now_et.strftime("%I:%M:%S %p ET"),
            "source": src,
            "drift_ms": round(self._drift_ms, 1),
            "max_drift_ms": round(self._max_drift_ms, 1),
            "drift_safe": self.is_drift_safe(),
            "drift_warning_threshold_ms": self._drift_warning_threshold_ms,
            "drift_halt_threshold_ms": self._drift_halt_threshold_ms,
            "system_clock_et": system_et.isoformat(),
            "broker_source": broker_src or None,
            "broker_update_count": broker_count,
            "broker_last_update_ago_s": round(broker_age_s, 2) if broker_age_s is not None else None,
            "broker_stale_threshold_s": self._stale_threshold_s,
            "uptime_s": round(_time.monotonic() - self._boot_time, 1),
        }


# ─── Singleton Accessor ─────────────────────────────────────────────────

_authority: Optional[TimeAuthority] = None
_accessor_lock = threading.Lock()


def get_time_authority() -> TimeAuthority:
    """Get the TimeAuthority singleton."""
    global _authority
    if _authority is None:
        with _accessor_lock:
            if _authority is None:
                _authority = TimeAuthority()
    return _authority


# ─── Module-Level Convenience Functions ──────────────────────────────────
# These are the primary interface for all modules in the codebase.
# Usage: from morpheus.core.time_authority import now, now_iso, source

def now() -> datetime:
    """Returns current authoritative ET datetime (timezone-aware)."""
    return get_time_authority().now()


def now_utc() -> datetime:
    """Returns current authoritative UTC datetime."""
    return get_time_authority().now_utc()


def now_iso() -> str:
    """Returns current time as ISO-8601 string with ET offset."""
    return get_time_authority().now_iso()


def now_utc_iso() -> str:
    """Returns current time as ISO-8601 string in UTC."""
    return get_time_authority().now_utc_iso()


def now_time() -> time:
    """Returns current ET time-of-day."""
    return get_time_authority().now_time()


def now_hhmm() -> int:
    """Returns HHMM integer (e.g., 930)."""
    return get_time_authority().now_hhmm()


def now_date():
    """Returns current ET date."""
    return get_time_authority().now_date()


def source() -> str:
    """Returns current time source."""
    return get_time_authority().source()


def drift_ms() -> float:
    """Returns current drift in ms."""
    return get_time_authority().drift_ms()


def is_drift_safe() -> bool:
    """True if safe to trade (drift < 1s)."""
    return get_time_authority().is_drift_safe()


def enrich_event(event: dict) -> dict:
    """Add timestamp_et and time_source to event dict."""
    return get_time_authority().enrich_event(event)
