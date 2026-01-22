"""
Watchlist - Dynamic symbol tracking with lifecycle states.

The watchlist is STATEFUL, tracking symbols through their lifecycle:
- NEW: Just discovered by scanner, being evaluated
- ACTIVE: Being monitored for signals
- STALE: Lost momentum, about to be removed
- REMOVED: No longer tracked

Presence on the watchlist does NOT imply tradability.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, Callable, Awaitable

logger = logging.getLogger(__name__)


class WatchlistState(str, Enum):
    """Watchlist entry states."""
    NEW = "new"  # Just discovered
    ACTIVE = "active"  # Being monitored
    STALE = "stale"  # Lost momentum
    REMOVED = "removed"  # No longer tracked


@dataclass
class WatchlistEntry:
    """A symbol on the watchlist."""

    symbol: str
    state: WatchlistState = WatchlistState.NEW

    # Discovery info
    discovered_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    discovery_reason: str = ""
    discovery_score: float = 0.0

    # State tracking
    state_changed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    activated_at: datetime | None = None
    stale_at: datetime | None = None

    # Current metrics (updated periodically)
    last_price: float = 0.0
    last_volume: int = 0
    last_rvol: float = 0.0
    last_change_pct: float = 0.0
    last_spread_pct: float = 0.0
    last_updated: datetime | None = None

    # Tracking
    signals_generated: int = 0
    times_stale: int = 0  # How many times it went stale

    def transition_to(self, new_state: WatchlistState) -> None:
        """Transition to a new state."""
        old_state = self.state
        self.state = new_state
        self.state_changed_at = datetime.now(timezone.utc)

        if new_state == WatchlistState.ACTIVE and old_state == WatchlistState.NEW:
            self.activated_at = self.state_changed_at
        elif new_state == WatchlistState.STALE:
            self.stale_at = self.state_changed_at
            self.times_stale += 1

        logger.info(f"[WATCHLIST] {self.symbol}: {old_state.value} -> {new_state.value}")

    def update_metrics(
        self,
        price: float,
        volume: int,
        rvol: float,
        change_pct: float,
        spread_pct: float,
    ) -> None:
        """Update current metrics."""
        self.last_price = price
        self.last_volume = volume
        self.last_rvol = rvol
        self.last_change_pct = change_pct
        self.last_spread_pct = spread_pct
        self.last_updated = datetime.now(timezone.utc)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "symbol": self.symbol,
            "state": self.state.value,
            "discovered_at": self.discovered_at.isoformat(),
            "discovery_reason": self.discovery_reason,
            "discovery_score": self.discovery_score,
            "state_changed_at": self.state_changed_at.isoformat(),
            "activated_at": self.activated_at.isoformat() if self.activated_at else None,
            "last_price": self.last_price,
            "last_volume": self.last_volume,
            "last_rvol": self.last_rvol,
            "last_change_pct": self.last_change_pct,
            "last_spread_pct": self.last_spread_pct,
            "last_updated": self.last_updated.isoformat() if self.last_updated else None,
            "signals_generated": self.signals_generated,
            "times_stale": self.times_stale,
        }


@dataclass
class WatchlistConfig:
    """Configuration for watchlist behavior."""

    # State transition timing
    new_to_active_delay_seconds: float = 30.0  # Confirm momentum before activating
    stale_timeout_seconds: float = 300.0  # 5 min without activity = stale
    stale_to_removed_seconds: float = 600.0  # 10 min stale = removed

    # Staleness criteria
    min_rvol_for_active: float = 1.5  # Below this = might go stale
    min_change_pct_for_active: float = 2.0  # Below this = might go stale

    # Capacity
    max_watchlist_size: int = 30  # Maximum symbols to track

    # Monitoring
    update_interval_seconds: float = 10.0


class Watchlist:
    """
    Dynamic watchlist with lifecycle management.

    Symbols flow through states:
    NEW -> ACTIVE -> (STALE) -> REMOVED

    Active symbols are monitored for signals.
    Stale symbols may recover or be removed.
    """

    def __init__(
        self,
        config: WatchlistConfig | None = None,
        on_state_change: Callable[[WatchlistEntry, WatchlistState, WatchlistState], Awaitable[None]] | None = None,
        on_activated: Callable[[WatchlistEntry], Awaitable[None]] | None = None,
        on_removed: Callable[[WatchlistEntry], Awaitable[None]] | None = None,
    ):
        """
        Initialize watchlist.

        Args:
            config: Watchlist configuration
            on_state_change: Callback for all state changes
            on_activated: Callback when symbol becomes ACTIVE
            on_removed: Callback when symbol is REMOVED
        """
        self.config = config or WatchlistConfig()
        self._on_state_change = on_state_change
        self._on_activated = on_activated
        self._on_removed = on_removed

        self._entries: dict[str, WatchlistEntry] = {}
        self._running = False
        self._monitor_task: asyncio.Task | None = None

        logger.info("Watchlist initialized")
        logger.info(f"  Max size: {self.config.max_watchlist_size}")
        logger.info(f"  Stale timeout: {self.config.stale_timeout_seconds}s")

    async def start(self) -> None:
        """Start watchlist monitoring."""
        if self._running:
            return

        self._running = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("Watchlist monitoring started")

    async def stop(self) -> None:
        """Stop watchlist monitoring."""
        self._running = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        logger.info("Watchlist monitoring stopped")

    async def add(
        self,
        symbol: str,
        reason: str = "",
        score: float = 0.0,
        initial_price: float = 0.0,
        initial_rvol: float = 0.0,
        initial_change_pct: float = 0.0,
    ) -> WatchlistEntry | None:
        """
        Add a symbol to the watchlist.

        Args:
            symbol: Symbol to add
            reason: Why this symbol was added
            score: Scanner score if available
            initial_*: Initial metrics

        Returns:
            The watchlist entry, or None if at capacity
        """
        symbol = symbol.upper()

        # Already tracked?
        if symbol in self._entries:
            entry = self._entries[symbol]
            # If it was stale, revive it
            if entry.state == WatchlistState.STALE:
                await self._transition(entry, WatchlistState.NEW)
            return entry

        # At capacity?
        active_count = len(self.get_active())
        if active_count >= self.config.max_watchlist_size:
            logger.warning(f"Watchlist at capacity ({active_count}), not adding {symbol}")
            return None

        # Create new entry
        entry = WatchlistEntry(
            symbol=symbol,
            state=WatchlistState.NEW,
            discovery_reason=reason,
            discovery_score=score,
            last_price=initial_price,
            last_rvol=initial_rvol,
            last_change_pct=initial_change_pct,
        )

        self._entries[symbol] = entry
        logger.info(f"[WATCHLIST] Added {symbol}: {reason} (score={score:.1f})")

        return entry

    async def remove(self, symbol: str) -> None:
        """Remove a symbol from the watchlist."""
        symbol = symbol.upper()

        if symbol not in self._entries:
            return

        entry = self._entries[symbol]
        await self._transition(entry, WatchlistState.REMOVED)
        del self._entries[symbol]

    async def update_metrics(
        self,
        symbol: str,
        price: float,
        volume: int,
        rvol: float,
        change_pct: float,
        spread_pct: float,
    ) -> None:
        """Update metrics for a symbol."""
        symbol = symbol.upper()

        if symbol not in self._entries:
            return

        entry = self._entries[symbol]
        entry.update_metrics(price, volume, rvol, change_pct, spread_pct)

        # Check for state transitions based on metrics
        await self._evaluate_state(entry)

    def record_signal(self, symbol: str) -> None:
        """Record that a signal was generated for this symbol."""
        symbol = symbol.upper()
        if symbol in self._entries:
            self._entries[symbol].signals_generated += 1

    # =========================================================================
    # State management
    # =========================================================================

    async def _transition(
        self, entry: WatchlistEntry, new_state: WatchlistState
    ) -> None:
        """Transition an entry to a new state."""
        old_state = entry.state
        if old_state == new_state:
            return

        entry.transition_to(new_state)

        # Fire callbacks
        if self._on_state_change:
            await self._on_state_change(entry, old_state, new_state)

        if new_state == WatchlistState.ACTIVE and self._on_activated:
            await self._on_activated(entry)

        if new_state == WatchlistState.REMOVED and self._on_removed:
            await self._on_removed(entry)

    async def _evaluate_state(self, entry: WatchlistEntry) -> None:
        """Evaluate if an entry should transition state based on metrics."""
        now = datetime.now(timezone.utc)

        if entry.state == WatchlistState.NEW:
            # Check if ready to activate
            time_in_new = (now - entry.state_changed_at).total_seconds()
            if time_in_new >= self.config.new_to_active_delay_seconds:
                # Confirm it still has momentum
                if (
                    entry.last_rvol >= self.config.min_rvol_for_active
                    and abs(entry.last_change_pct) >= self.config.min_change_pct_for_active
                ):
                    await self._transition(entry, WatchlistState.ACTIVE)
                else:
                    # Lost momentum during NEW phase
                    await self._transition(entry, WatchlistState.STALE)

        elif entry.state == WatchlistState.ACTIVE:
            # Check for staleness
            if (
                entry.last_rvol < self.config.min_rvol_for_active
                or abs(entry.last_change_pct) < self.config.min_change_pct_for_active
            ):
                await self._transition(entry, WatchlistState.STALE)

        elif entry.state == WatchlistState.STALE:
            # Check for recovery or removal
            if (
                entry.last_rvol >= self.config.min_rvol_for_active
                and abs(entry.last_change_pct) >= self.config.min_change_pct_for_active
            ):
                # Recovered!
                await self._transition(entry, WatchlistState.ACTIVE)
            else:
                # Check if should be removed
                time_stale = (now - entry.state_changed_at).total_seconds()
                if time_stale >= self.config.stale_to_removed_seconds:
                    await self._transition(entry, WatchlistState.REMOVED)
                    del self._entries[entry.symbol]

    async def _monitor_loop(self) -> None:
        """Background monitoring loop."""
        while self._running:
            try:
                now = datetime.now(timezone.utc)

                # Check all entries for time-based transitions
                for symbol in list(self._entries.keys()):
                    entry = self._entries.get(symbol)
                    if not entry:
                        continue

                    # Check for stale due to no updates
                    if entry.last_updated:
                        time_since_update = (now - entry.last_updated).total_seconds()
                        if time_since_update > self.config.stale_timeout_seconds:
                            if entry.state == WatchlistState.ACTIVE:
                                await self._transition(entry, WatchlistState.STALE)

                    # Evaluate state (handles NEW->ACTIVE and STALE->REMOVED)
                    await self._evaluate_state(entry)

                await asyncio.sleep(self.config.update_interval_seconds)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Watchlist monitor error: {e}")
                await asyncio.sleep(5.0)

    # =========================================================================
    # Query methods
    # =========================================================================

    def get(self, symbol: str) -> WatchlistEntry | None:
        """Get an entry by symbol."""
        return self._entries.get(symbol.upper())

    def get_all(self) -> list[WatchlistEntry]:
        """Get all entries."""
        return list(self._entries.values())

    def get_active(self) -> list[WatchlistEntry]:
        """Get all ACTIVE entries."""
        return [e for e in self._entries.values() if e.state == WatchlistState.ACTIVE]

    def get_new(self) -> list[WatchlistEntry]:
        """Get all NEW entries."""
        return [e for e in self._entries.values() if e.state == WatchlistState.NEW]

    def get_stale(self) -> list[WatchlistEntry]:
        """Get all STALE entries."""
        return [e for e in self._entries.values() if e.state == WatchlistState.STALE]

    def get_symbols(self, state: WatchlistState | None = None) -> list[str]:
        """Get symbols, optionally filtered by state."""
        if state:
            return [e.symbol for e in self._entries.values() if e.state == state]
        return list(self._entries.keys())

    def get_stats(self) -> dict[str, Any]:
        """Get watchlist statistics."""
        entries = list(self._entries.values())
        return {
            "total": len(entries),
            "new": len([e for e in entries if e.state == WatchlistState.NEW]),
            "active": len([e for e in entries if e.state == WatchlistState.ACTIVE]),
            "stale": len([e for e in entries if e.state == WatchlistState.STALE]),
            "max_size": self.config.max_watchlist_size,
            "symbols": {e.symbol: e.state.value for e in entries},
        }

    def reset(self) -> None:
        """Reset the watchlist (e.g., at market open)."""
        self._entries.clear()
        logger.info("Watchlist reset")
