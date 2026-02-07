"""
MAX_AI Scanner Integration for Morpheus Orchestrator.

This module is the SINGLE integration point between MAX_AI_SCANNER and Morpheus.

MAX_AI_SCANNER provides:
- Symbol discovery (ONLY source of symbols)
- Market events (halts, resumes, momentum signals)
- Context data (read-only augmentation)

Morpheus provides:
- Feature computation
- Regime detection
- Strategy execution
- Risk management

NO OVERLAP ALLOWED.
"""

import asyncio
import logging
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Optional, Callable, Awaitable, Any, Set, TYPE_CHECKING

from morpheus.scanner.max_ai.client import MaxAIScannerClient, HaltInfo
from morpheus.core.events import Event, EventType, create_event

if TYPE_CHECKING:
    from morpheus.features.feature_engine import FeatureContext

logger = logging.getLogger(__name__)


# Event mapping: Scanner Event -> Morpheus EventType
SCANNER_EVENT_MAP = {
    "HALT": EventType.MARKET_HALT,
    "RESUME": EventType.MARKET_RESUME,
    "GAP_ALERT": EventType.SCANNER_GAP_SIGNAL,
    "MOMO_SURGE": EventType.SCANNER_MOMENTUM_SIGNAL,
    "HOD_BREAK": EventType.SCANNER_HOD_SIGNAL,
}


@dataclass
class ScannerIntegrationConfig:
    """Configuration for scanner integration."""

    # Polling intervals (seconds)
    symbol_poll_interval: float = 30.0  # How often to fetch symbols
    halt_poll_interval: float = 10.0    # How often to check halts
    event_poll_interval: float = 5.0    # How often to poll for events

    # Symbol management
    max_symbols: int = 25               # Maximum symbols to track
    min_score: float = 0.0              # Minimum AI score to include
    profiles: list = field(default_factory=lambda: ["FAST_MOVERS", "GAPPERS"])

    # Behavior flags
    auto_subscribe: bool = True         # Auto-subscribe new symbols to streaming
    emit_discovery_events: bool = True  # Emit events when symbols discovered

    # Safety
    require_scanner_healthy: bool = True  # Require scanner up to operate


class ScannerIntegration:
    """
    Integration layer between MAX_AI_SCANNER and Morpheus Orchestrator.

    Responsibilities:
    1. Poll scanner for symbol discovery -> sync to pipeline/streamer
    2. Poll scanner for alerts/events -> map to Morpheus events
    3. Provide context augmentation for symbols being evaluated
    4. Enforce that scanner is the ONLY discovery source

    This class does NOT:
    - Execute trades
    - Compute features
    - Make strategy decisions
    """

    def __init__(
        self,
        config: Optional[ScannerIntegrationConfig] = None,
        on_event: Optional[Callable[[Event], Awaitable[None]]] = None,
        on_symbol_discovered: Optional[Callable[[str], Awaitable[None]]] = None,
        on_symbol_removed: Optional[Callable[[str], Awaitable[None]]] = None,
        on_heartbeat: Optional[Callable[[int], Awaitable[None]]] = None,
    ):
        """
        Initialize scanner integration.

        Args:
            config: Integration configuration
            on_event: Callback for emitting Morpheus events
            on_symbol_discovered: Callback when new symbol should be subscribed
            on_symbol_removed: Callback when symbol should be unsubscribed
            on_heartbeat: Callback on every successful poll (passes active symbol count)
        """
        self.config = config or ScannerIntegrationConfig()
        self._on_event = on_event
        self._on_symbol_discovered = on_symbol_discovered
        self._on_symbol_removed = on_symbol_removed
        self._on_heartbeat = on_heartbeat

        # Client
        self._client = MaxAIScannerClient()

        # State
        self._active_symbols: Set[str] = set()
        self._known_halts: Set[str] = set()
        self._last_alerts: dict[str, datetime] = {}  # symbol -> last alert time
        self._is_running = False
        self._scanner_healthy = False

        # Tasks
        self._symbol_task: Optional[asyncio.Task] = None
        self._halt_task: Optional[asyncio.Task] = None
        self._event_task: Optional[asyncio.Task] = None
        self._health_task: Optional[asyncio.Task] = None

        # Health monitoring
        self._consecutive_failures: int = 0
        self._max_consecutive_failures: int = 5
        self._health_check_interval: float = 30.0  # Check health every 30s

    @property
    def active_symbols(self) -> Set[str]:
        """Get currently tracked symbols (read-only)."""
        return self._active_symbols.copy()

    @property
    def is_healthy(self) -> bool:
        """Check if scanner integration is healthy."""
        return self._scanner_healthy and self._is_running

    async def start(self) -> None:
        """Start the scanner integration loops."""
        if self._is_running:
            logger.warning("Scanner integration already running")
            return

        logger.info("Starting MAX_AI Scanner integration...")

        # Health check first
        self._scanner_healthy = await self._client.health_check()
        if not self._scanner_healthy:
            if self.config.require_scanner_healthy:
                logger.error("MAX_AI_SCANNER not healthy - integration will not start")
                return
            logger.warning("MAX_AI_SCANNER not healthy - continuing anyway")

        self._is_running = True

        # Start polling loops
        self._symbol_task = asyncio.create_task(self._symbol_poll_loop())
        self._halt_task = asyncio.create_task(self._halt_poll_loop())
        self._health_task = asyncio.create_task(self._health_monitor_loop())

        logger.info("Scanner integration started with health monitoring")

    async def stop(self) -> None:
        """Stop the scanner integration."""
        logger.info("Stopping scanner integration...")
        self._is_running = False

        # Cancel tasks
        for task in [self._symbol_task, self._halt_task, self._event_task, self._health_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        await self._client.close()
        logger.info("Scanner integration stopped")

    async def _symbol_poll_loop(self) -> None:
        """Poll scanner for symbols and sync to pipeline."""
        while self._is_running:
            try:
                await self._sync_symbols()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Symbol poll error: {e}")

            await asyncio.sleep(self.config.symbol_poll_interval)

    async def _halt_poll_loop(self) -> None:
        """Poll scanner for trading halts and emit events."""
        while self._is_running:
            try:
                await self._check_halts()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Halt poll error: {e}")

            await asyncio.sleep(self.config.halt_poll_interval)

    async def _health_monitor_loop(self) -> None:
        """Monitor scanner health and attempt reconnection if needed."""
        while self._is_running:
            try:
                # Check scanner health
                is_healthy = await self._client.health_check()

                if is_healthy:
                    if not self._scanner_healthy:
                        logger.info("[SCANNER] Connection restored - scanner is healthy")
                    self._scanner_healthy = True
                    self._consecutive_failures = 0
                else:
                    self._consecutive_failures += 1
                    logger.warning(
                        f"[SCANNER] Health check failed "
                        f"({self._consecutive_failures}/{self._max_consecutive_failures})"
                    )

                    if self._consecutive_failures >= self._max_consecutive_failures:
                        self._scanner_healthy = False
                        logger.error(
                            "[SCANNER] Max consecutive failures reached - "
                            "attempting to reconnect..."
                        )
                        # Close and recreate client
                        await self._client.close()
                        self._client = MaxAIScannerClient()
                        self._consecutive_failures = 0

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health monitor error: {e}")
                self._consecutive_failures += 1

            await asyncio.sleep(self._health_check_interval)

    async def _sync_symbols(self) -> None:
        """Fetch symbols from scanner and sync to pipeline."""
        # Fetch from all configured profiles
        all_symbols: dict[str, dict] = {}
        fetch_success = False

        for profile in self.config.profiles:
            try:
                movers = await self._client.fetch_movers(
                    profile=profile,
                    limit=self.config.max_symbols
                )
                if movers:  # Got data from scanner
                    fetch_success = True
                    for mover in movers:
                        symbol = mover.get("symbol")
                        if symbol and mover.get("ai_score", 0) >= self.config.min_score:
                            if symbol not in all_symbols:
                                all_symbols[symbol] = mover
            except Exception as e:
                logger.warning(f"[SCANNER] Failed to fetch {profile}: {e}")

        # Emit heartbeat on EVERY successful poll (not just discoveries)
        # This keeps the scanner timestamp fresh
        if fetch_success:
            self._scanner_healthy = True
            if self._on_heartbeat:
                try:
                    await self._on_heartbeat(len(all_symbols))
                except Exception as e:
                    logger.error(f"[SCANNER] Heartbeat callback failed: {e}")
        else:
            # No data from any profile - scanner may be down
            self._scanner_healthy = False
            logger.warning("[SCANNER] No data from any profile - scanner may be offline")

        new_symbols = set(all_symbols.keys())

        # Find additions and removals
        added = new_symbols - self._active_symbols
        removed = self._active_symbols - new_symbols

        # Process additions
        for symbol in added:
            logger.info(f"[SCANNER] Discovered symbol: {symbol}")
            self._active_symbols.add(symbol)

            if self._on_symbol_discovered:
                await self._on_symbol_discovered(symbol)

            if self.config.emit_discovery_events:
                await self._emit_event(create_event(
                    EventType.SCANNER_SYMBOL_DISCOVERED,
                    payload={
                        "symbol": symbol,
                        "source": "MAX_AI_SCANNER",
                        "score": all_symbols[symbol].get("ai_score", 0),
                        "change_pct": all_symbols[symbol].get("netPercentChangeInDouble", 0),
                        "rvol": all_symbols[symbol].get("rvol", 0),
                    },
                    symbol=symbol,
                ))

        # Process removals (symbols no longer in scanner)
        for symbol in removed:
            logger.info(f"[SCANNER] Symbol removed from scanner: {symbol}")
            self._active_symbols.discard(symbol)

            if self._on_symbol_removed:
                await self._on_symbol_removed(symbol)

        if added or removed:
            logger.info(
                f"[SCANNER] Sync complete: {len(added)} added, {len(removed)} removed, "
                f"{len(self._active_symbols)} active"
            )

    async def _check_halts(self) -> None:
        """Check for trading halts and emit events."""
        # Get active halts
        active_halts = await self._client.get_active_halts()
        current_halted = {h.symbol for h in active_halts}

        # New halts
        new_halts = current_halted - self._known_halts
        for symbol in new_halts:
            halt = next((h for h in active_halts if h.symbol == symbol), None)
            if halt:
                logger.warning(f"[SCANNER] HALT: {symbol} - {halt.halt_reason}")
                await self._emit_event(create_event(
                    EventType.MARKET_HALT,
                    payload={
                        "symbol": symbol,
                        "halt_price": halt.halt_price,
                        "halt_reason": halt.halt_reason,
                        "halt_time": halt.halt_time.isoformat() if halt.halt_time else None,
                    },
                    symbol=symbol,
                ))

        # Resumed halts
        resumed = self._known_halts - current_halted
        if resumed:
            # Fetch resumed halts for details
            resumed_halts = await self._client.get_resumed_halts(hours=1)
            for halt in resumed_halts:
                if halt.symbol in resumed:
                    logger.info(f"[SCANNER] RESUME: {halt.symbol} @ ${halt.resume_price}")
                    await self._emit_event(create_event(
                        EventType.MARKET_RESUME,
                        payload={
                            "symbol": halt.symbol,
                            "halt_price": halt.halt_price,
                            "resume_price": halt.resume_price,
                            "halt_reason": halt.halt_reason,
                            "resume_time": halt.resume_time.isoformat() if halt.resume_time else None,
                        },
                        symbol=halt.symbol,
                    ))

        self._known_halts = current_halted

    async def get_symbol_context(self, symbol: str) -> dict[str, Any]:
        """
        Get scanner context for a symbol (read-only augmentation).

        This data should be attached to pipeline context as external data.
        Strategies may reference but NOT mutate these values.

        Args:
            symbol: Stock symbol

        Returns:
            Dict with scanner context fields
        """
        raw = await self._client.get_symbol_context(symbol)

        # Extract and normalize fields for pipeline context
        return {
            "scanner_score": raw.get("ai_score", 0),
            "gap_pct": raw.get("gap_pct", 0),
            "halt_status": raw.get("halt_status"),
            "rvol_proxy": raw.get("rvol", 0),
            "velocity_1m": raw.get("velocity_1m", 0),
            "hod_distance_pct": raw.get("hod_distance_pct", 0),
            "tags": raw.get("tags", []),
            "float_shares": raw.get("float_shares"),
            "market_cap": raw.get("market_cap"),
            "profiles": raw.get("profiles", []),
            "_source": "MAX_AI_SCANNER",
            "_timestamp": datetime.now(timezone.utc).isoformat(),
        }

    async def process_scanner_alert(self, alert_type: str, symbol: str, data: dict) -> None:
        """
        Process an alert from scanner WebSocket (if using streaming).

        Maps scanner alerts to Morpheus events.

        Args:
            alert_type: Scanner alert type (HALT, RESUME, GAP_ALERT, etc.)
            symbol: Stock symbol
            data: Alert payload
        """
        morpheus_type = SCANNER_EVENT_MAP.get(alert_type)
        if not morpheus_type:
            logger.warning(f"Unknown scanner alert type: {alert_type}")
            return

        await self._emit_event(create_event(
            morpheus_type,
            payload={
                "symbol": symbol,
                "scanner_alert_type": alert_type,
                **data,
            },
            symbol=symbol,
        ))

    async def _emit_event(self, event: Event) -> None:
        """Emit a Morpheus event via callback."""
        if self._on_event:
            try:
                await self._on_event(event)
            except Exception as e:
                logger.error(f"Failed to emit event: {e}")

    def get_status(self) -> dict[str, Any]:
        """Get integration status for diagnostics."""
        return {
            "is_running": self._is_running,
            "scanner_healthy": self._scanner_healthy,
            "active_symbols": list(self._active_symbols),
            "known_halts": list(self._known_halts),
            "config": {
                "symbol_poll_interval": self.config.symbol_poll_interval,
                "profiles": self.config.profiles,
                "max_symbols": self.config.max_symbols,
            },
        }


def update_feature_context_with_external(
    context: "FeatureContext",
    external_data: dict[str, Any],
) -> "FeatureContext":
    """
    Create a new FeatureContext with external scanner data attached.

    This is the ONLY way to add scanner context to a FeatureContext.
    The external data is READ-ONLY - strategies may reference but NOT mutate.

    Args:
        context: Original FeatureContext (immutable)
        external_data: Scanner context from get_symbol_context()

    Returns:
        New FeatureContext with external field populated
    """
    # FeatureContext is frozen, so we use replace() from dataclasses
    return replace(context, external=external_data)
