"""
Market Data Scheduler - Cadence-controlled snapshot polling.

Manages the timing of market data retrieval:
- Configurable polling intervals
- Market hours awareness
- Graceful error handling with event emission
- No retries that block the scheduler

Phase 2 Scope:
- Snapshot cadence control
- Event emission per snapshot
- READ-ONLY (no trade state mutation)
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, time as dt_time
from enum import Enum
from typing import Callable, Any

from morpheus.core.events import Event, EventType, create_event
from morpheus.core.event_sink import EventSink
from morpheus.data.market_snapshot import MarketSnapshot

logger = logging.getLogger(__name__)


class SchedulerState(str, Enum):
    """Scheduler operational states."""

    STOPPED = "STOPPED"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    ERROR = "ERROR"


class MarketHours:
    """
    Market hours checker for US equity markets.

    Default: 9:30 AM - 4:00 PM Eastern
    """

    def __init__(
        self,
        market_open: dt_time = dt_time(9, 30),
        market_close: dt_time = dt_time(16, 0),
        timezone_offset_hours: int = -5,  # EST (adjust for EDT as needed)
    ):
        self.market_open = market_open
        self.market_close = market_close
        self.tz_offset = timezone_offset_hours

    def is_market_open(self, utc_time: datetime | None = None) -> bool:
        """
        Check if market is currently open.

        Args:
            utc_time: Time to check (defaults to now)

        Returns:
            True if market is open
        """
        if utc_time is None:
            utc_time = datetime.now(timezone.utc)

        # Convert to market timezone (simple offset, doesn't handle DST)
        from datetime import timedelta

        market_time = utc_time + timedelta(hours=self.tz_offset)

        # Check day of week (0=Monday, 6=Sunday)
        if market_time.weekday() >= 5:  # Weekend
            return False

        # Check time
        current_time = market_time.time()
        return self.market_open <= current_time < self.market_close


@dataclass
class SchedulerConfig:
    """Configuration for the market data scheduler."""

    # Polling intervals (seconds)
    quote_interval: float = 5.0  # How often to poll quotes
    candle_interval: float = 60.0  # How often to fetch fresh candles

    # Symbol configuration
    symbols: list[str] = field(default_factory=list)

    # Behavior
    respect_market_hours: bool = True
    emit_events: bool = True
    include_candles: bool = True

    # Error handling
    max_consecutive_errors: int = 5
    error_backoff_seconds: float = 30.0


class SnapshotScheduler:
    """
    Scheduler for market data snapshot retrieval.

    Manages polling cadence and event emission for market snapshots.
    Each snapshot for each symbol emits its own event.

    Usage:
        scheduler = SnapshotScheduler(
            config=SchedulerConfig(symbols=["SPY", "QQQ"]),
            snapshot_provider=market_client.build_snapshot,
            event_sink=event_sink,
        )
        scheduler.start()
        # ... later ...
        scheduler.stop()
    """

    def __init__(
        self,
        config: SchedulerConfig,
        snapshot_provider: Callable[[str, bool], MarketSnapshot],
        event_sink: EventSink | None = None,
        market_hours: MarketHours | None = None,
    ):
        """
        Initialize the scheduler.

        Args:
            config: Scheduler configuration
            snapshot_provider: Function that takes (symbol, include_candles)
                             and returns a MarketSnapshot
            event_sink: Optional EventSink for event emission
            market_hours: Optional MarketHours checker
        """
        self.config = config
        self.snapshot_provider = snapshot_provider
        self.event_sink = event_sink
        self.market_hours = market_hours or MarketHours()

        self._state = SchedulerState.STOPPED
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._consecutive_errors = 0
        self._last_candle_fetch: datetime | None = None
        self._lock = threading.Lock()

        # Stats
        self._snapshots_emitted = 0
        self._errors_emitted = 0

    @property
    def state(self) -> SchedulerState:
        """Current scheduler state."""
        return self._state

    @property
    def is_running(self) -> bool:
        """Check if scheduler is running."""
        return self._state == SchedulerState.RUNNING

    def _emit_event(self, event: Event) -> None:
        """Emit an event to the sink if configured."""
        if self.event_sink and self.config.emit_events:
            self.event_sink.emit(event)

    def _emit_snapshot_event(self, snapshot: MarketSnapshot) -> None:
        """Emit a MARKET_SNAPSHOT event."""
        event = snapshot.to_event()
        self._emit_event(event)
        self._snapshots_emitted += 1
        logger.debug(f"Emitted snapshot for {snapshot.symbol}")

    def _emit_error_event(
        self,
        symbol: str | None,
        error: str,
        error_type: str = "SNAPSHOT_ERROR",
    ) -> None:
        """Emit an error event."""
        event = create_event(
            EventType.SYSTEM_STOP,  # Using SYSTEM_STOP for errors (could add MARKET_ERROR)
            payload={
                "error_type": error_type,
                "error_message": error,
                "symbol": symbol,
                "scheduler_state": self._state.value,
            },
            symbol=symbol,
        )
        self._emit_event(event)
        self._errors_emitted += 1
        logger.warning(f"Emitted error event: {error}")

    def _should_fetch_candles(self) -> bool:
        """Check if it's time to fetch fresh candles."""
        if not self.config.include_candles:
            return False

        now = datetime.now(timezone.utc)
        if self._last_candle_fetch is None:
            return True

        elapsed = (now - self._last_candle_fetch).total_seconds()
        return elapsed >= self.config.candle_interval

    def _fetch_and_emit(self) -> None:
        """Fetch snapshots for all symbols and emit events."""
        include_candles = self._should_fetch_candles()
        if include_candles:
            self._last_candle_fetch = datetime.now(timezone.utc)

        for symbol in self.config.symbols:
            try:
                snapshot = self.snapshot_provider(symbol, include_candles)
                self._emit_snapshot_event(snapshot)
                self._consecutive_errors = 0

            except Exception as e:
                logger.error(f"Error fetching snapshot for {symbol}: {e}")
                self._emit_error_event(symbol, str(e))
                self._consecutive_errors += 1

                if self._consecutive_errors >= self.config.max_consecutive_errors:
                    logger.error(
                        f"Max consecutive errors ({self.config.max_consecutive_errors}) reached. "
                        "Pausing scheduler."
                    )
                    self._state = SchedulerState.ERROR
                    return

    def _run_loop(self) -> None:
        """Main scheduler loop."""
        logger.info(
            f"Scheduler started. Symbols: {self.config.symbols}, "
            f"Interval: {self.config.quote_interval}s"
        )

        self._state = SchedulerState.RUNNING

        while not self._stop_event.is_set():
            try:
                # Check market hours
                if self.config.respect_market_hours:
                    if not self.market_hours.is_market_open():
                        logger.debug("Market closed, skipping poll")
                        self._stop_event.wait(self.config.quote_interval)
                        continue

                # Fetch and emit snapshots
                self._fetch_and_emit()

                # Handle error state
                if self._state == SchedulerState.ERROR:
                    logger.info(
                        f"In error state, backing off for "
                        f"{self.config.error_backoff_seconds}s"
                    )
                    self._stop_event.wait(self.config.error_backoff_seconds)
                    self._state = SchedulerState.RUNNING
                    self._consecutive_errors = 0
                    continue

                # Wait for next interval
                self._stop_event.wait(self.config.quote_interval)

            except Exception as e:
                logger.error(f"Unexpected error in scheduler loop: {e}")
                self._emit_error_event(None, str(e), "SCHEDULER_ERROR")
                self._stop_event.wait(self.config.error_backoff_seconds)

        self._state = SchedulerState.STOPPED
        logger.info("Scheduler stopped")

    def start(self) -> None:
        """Start the scheduler in a background thread."""
        if self._thread is not None and self._thread.is_alive():
            logger.warning("Scheduler already running")
            return

        if not self.config.symbols:
            logger.warning("No symbols configured, scheduler will not poll")

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        """
        Stop the scheduler.

        Args:
            timeout: Max seconds to wait for thread to stop
        """
        logger.info("Stopping scheduler...")
        self._stop_event.set()

        if self._thread is not None:
            self._thread.join(timeout=timeout)
            if self._thread.is_alive():
                logger.warning("Scheduler thread did not stop cleanly")

        self._state = SchedulerState.STOPPED

    def pause(self) -> None:
        """Pause the scheduler (keeps thread alive but skips polling)."""
        with self._lock:
            if self._state == SchedulerState.RUNNING:
                self._state = SchedulerState.PAUSED
                logger.info("Scheduler paused")

    def resume(self) -> None:
        """Resume a paused scheduler."""
        with self._lock:
            if self._state == SchedulerState.PAUSED:
                self._state = SchedulerState.RUNNING
                logger.info("Scheduler resumed")

    def get_stats(self) -> dict[str, Any]:
        """Get scheduler statistics."""
        return {
            "state": self._state.value,
            "snapshots_emitted": self._snapshots_emitted,
            "errors_emitted": self._errors_emitted,
            "consecutive_errors": self._consecutive_errors,
            "symbols": self.config.symbols,
            "quote_interval": self.config.quote_interval,
            "candle_interval": self.config.candle_interval,
        }

    def add_symbol(self, symbol: str) -> None:
        """Add a symbol to the watchlist."""
        with self._lock:
            if symbol.upper() not in [s.upper() for s in self.config.symbols]:
                self.config.symbols.append(symbol.upper())
                logger.info(f"Added symbol: {symbol.upper()}")

    def remove_symbol(self, symbol: str) -> None:
        """Remove a symbol from the watchlist."""
        with self._lock:
            symbol_upper = symbol.upper()
            self.config.symbols = [
                s for s in self.config.symbols if s.upper() != symbol_upper
            ]
            logger.info(f"Removed symbol: {symbol_upper}")
