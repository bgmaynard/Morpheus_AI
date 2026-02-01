"""
Scanner Feed Adapter - Service 2 of the Morpheus Data Spine.

Polls MAX_AI_SCANNER on intervals (NOT per-quote), publishing to NATS:
  scanner.context.{sym}  - Every 15s per active symbol
  scanner.alerts          - Every 10s (halts/resumes)
  scanner.discovery       - Every 30s (new symbols)
  bot.telemetry.scanner_feed - Every 2s

This is the KEY FIX: scanner context is published on a timer interval,
not per-quote tick. This eliminates the 25,496 HTTP calls in 36 minutes
(708/min) that the monolith was making.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Set

from dotenv import load_dotenv

load_dotenv()

from morpheus.spine.nats_client import SpineClient
from morpheus.spine.config_store import ConfigStore
from morpheus.spine.schemas import (
    Topics,
    ScannerContextMsg,
    ScannerAlertMsg,
    ScannerDiscoveryMsg,
    TelemetryMsg,
)

# Reuse existing scanner client
from morpheus.scanner.max_ai.client import MaxAIScannerClient, HaltInfo
from morpheus.core.events import EventType

logger = logging.getLogger(__name__)


class ScannerFeedService:
    """
    Scanner Feed Adapter for the NATS spine.

    Replaces the per-quote HTTP polling with interval-based publishing.
    Scanner context is fetched every N seconds and published to NATS.
    Consumers (AI Core) read from NATS cache, never calling scanner HTTP directly.
    """

    SERVICE_NAME = "scanner_feed"

    def __init__(self, config_store: ConfigStore | None = None):
        self._config_store = config_store or ConfigStore()
        config = self._config_store.get()

        self._spine = SpineClient(
            url=config.nats_url,
            name=self.SERVICE_NAME,
        )

        # Scanner client
        self._client = MaxAIScannerClient()

        # Intervals from config
        self._context_interval = config.scanner_context_refresh_seconds
        self._discovery_interval = config.scanner_discovery_interval_seconds
        self._halt_interval = config.scanner_halt_interval_seconds

        # State
        self._active_symbols: Set[str] = set()
        self._known_halts: Set[str] = set()
        self._scanner_healthy = False

        # Telemetry
        self._start_time = 0.0
        self._context_publishes = 0
        self._alert_publishes = 0
        self._discovery_publishes = 0
        self._errors = 0

        # Control
        self._running = False
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        """Start the scanner feed service."""
        logger.info(f"[{self.SERVICE_NAME}] Starting...")
        self._start_time = time.time()
        self._running = True

        # Connect to NATS
        await self._spine.connect()

        # Health check scanner
        self._scanner_healthy = await self._client.health_check()
        if not self._scanner_healthy:
            logger.warning(f"[{self.SERVICE_NAME}] Scanner not healthy, will retry")

        # Start polling loops
        self._tasks = [
            asyncio.create_task(self._discovery_loop()),
            asyncio.create_task(self._halt_loop()),
            asyncio.create_task(self._context_loop()),
            asyncio.create_task(self._telemetry_loop()),
        ]

        logger.info(
            f"[{self.SERVICE_NAME}] Started "
            f"(context={self._context_interval}s, "
            f"discovery={self._discovery_interval}s, "
            f"halts={self._halt_interval}s)"
        )

    async def stop(self) -> None:
        """Stop the scanner feed service."""
        logger.info(f"[{self.SERVICE_NAME}] Stopping...")
        self._running = False

        for task in self._tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        await self._client.close()
        await self._spine.close()
        logger.info(f"[{self.SERVICE_NAME}] Stopped")

    # -----------------------------------------------------------------------
    # Discovery loop (every 30s)
    # -----------------------------------------------------------------------

    async def _discovery_loop(self) -> None:
        """Poll scanner for new symbols and publish discoveries."""
        while self._running:
            try:
                await self._sync_symbols()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._errors += 1
                logger.error(f"[{self.SERVICE_NAME}] Discovery error: {e}")

            await asyncio.sleep(self._discovery_interval)

    async def _sync_symbols(self) -> None:
        """Fetch symbols from scanner profiles and publish new discoveries."""
        all_symbols: dict[str, dict] = {}

        for profile in ["FAST_MOVERS", "GAPPERS"]:
            try:
                movers = await self._client.fetch_movers(profile=profile, limit=25)
                for mover in movers:
                    sym = mover.get("symbol")
                    if sym:
                        all_symbols[sym] = mover
            except Exception as e:
                self._errors += 1
                logger.warning(
                    f"[{self.SERVICE_NAME}] Failed to fetch {profile}: {e}"
                )

        new_symbols = set(all_symbols.keys())
        added = new_symbols - self._active_symbols
        removed = self._active_symbols - new_symbols

        # Publish discoveries
        for sym in added:
            mover_data = all_symbols[sym]
            msg = ScannerDiscoveryMsg(
                sym=sym,
                score=mover_data.get("ai_score", 0),
                change_pct=mover_data.get("netPercentChangeInDouble", 0),
                rvol=mover_data.get("rvol", 0),
            )
            await self._spine.publish(Topics.SCANNER_DISCOVERY, msg.encode())
            self._discovery_publishes += 1
            logger.info(f"[{self.SERVICE_NAME}] Discovered: {sym}")

        self._active_symbols = new_symbols

        if added or removed:
            logger.info(
                f"[{self.SERVICE_NAME}] Sync: +{len(added)} -{len(removed)} "
                f"= {len(self._active_symbols)} active"
            )

    # -----------------------------------------------------------------------
    # Halt loop (every 10s)
    # -----------------------------------------------------------------------

    async def _halt_loop(self) -> None:
        """Poll scanner for trading halts and publish alerts."""
        while self._running:
            try:
                await self._check_halts()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._errors += 1
                logger.error(f"[{self.SERVICE_NAME}] Halt check error: {e}")

            await asyncio.sleep(self._halt_interval)

    async def _check_halts(self) -> None:
        """Check for trading halts and publish NATS alerts."""
        active_halts = await self._client.get_active_halts()
        current_halted = {h.symbol for h in active_halts}

        # New halts
        for sym in current_halted - self._known_halts:
            halt = next((h for h in active_halts if h.symbol == sym), None)
            if halt:
                msg = ScannerAlertMsg(
                    alert_type="HALT",
                    sym=sym,
                    data={
                        "halt_price": halt.halt_price,
                        "halt_reason": halt.halt_reason,
                        "halt_time": (
                            halt.halt_time.isoformat() if halt.halt_time else None
                        ),
                    },
                )
                await self._spine.publish(Topics.SCANNER_ALERTS, msg.encode())
                self._alert_publishes += 1
                logger.warning(f"[{self.SERVICE_NAME}] HALT: {sym}")

        # Resumed
        for sym in self._known_halts - current_halted:
            msg = ScannerAlertMsg(
                alert_type="RESUME",
                sym=sym,
                data={},
            )
            await self._spine.publish(Topics.SCANNER_ALERTS, msg.encode())
            self._alert_publishes += 1
            logger.info(f"[{self.SERVICE_NAME}] RESUME: {sym}")

        self._known_halts = current_halted

    # -----------------------------------------------------------------------
    # Context loop (every 15s per symbol) — THE KEY FIX
    # -----------------------------------------------------------------------

    async def _context_loop(self) -> None:
        """
        Publish scanner context for all active symbols on a timer.

        This replaces the per-quote HTTP call pattern:
        - Old: 708 calls/min (every quote tick for every symbol)
        - New: ~4 calls/min per symbol (once every 15s)
        """
        while self._running:
            try:
                await self._publish_all_contexts()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._errors += 1
                logger.error(f"[{self.SERVICE_NAME}] Context loop error: {e}")

            await asyncio.sleep(self._context_interval)

    async def _publish_all_contexts(self) -> None:
        """Fetch and publish scanner context for all active symbols."""
        if not self._active_symbols:
            return

        for sym in list(self._active_symbols):
            try:
                raw = await self._client.get_symbol_context(sym)

                msg = ScannerContextMsg(
                    sym=sym,
                    scanner_score=raw.get("ai_score", 0),
                    gap_pct=raw.get("gap_pct", 0),
                    halt_status=raw.get("halt_status"),
                    rvol_proxy=raw.get("rvol", 0),
                    velocity_1m=raw.get("velocity_1m", 0),
                    hod_distance_pct=raw.get("hod_distance_pct", 0),
                    tags=raw.get("tags", []),
                    float_shares=raw.get("float_shares"),
                    market_cap=raw.get("market_cap"),
                    profiles=raw.get("profiles", []),
                )

                await self._spine.publish(msg.topic(), msg.encode())
                self._context_publishes += 1

            except Exception as e:
                self._errors += 1
                logger.warning(
                    f"[{self.SERVICE_NAME}] Context fetch failed for {sym}: {e}"
                )

    # -----------------------------------------------------------------------
    # Telemetry loop (every 2s)
    # -----------------------------------------------------------------------

    async def _telemetry_loop(self) -> None:
        """Publish telemetry every 2 seconds."""
        while self._running:
            try:
                msg = TelemetryMsg(
                    service=self.SERVICE_NAME,
                    status="running" if self._spine.is_connected else "degraded",
                    uptime_s=time.time() - self._start_time,
                    msgs_out=(
                        self._context_publishes
                        + self._alert_publishes
                        + self._discovery_publishes
                    ),
                    errors=self._errors,
                    extra={
                        "context_publishes": self._context_publishes,
                        "alert_publishes": self._alert_publishes,
                        "discovery_publishes": self._discovery_publishes,
                        "active_symbols": len(self._active_symbols),
                        "known_halts": len(self._known_halts),
                        "scanner_healthy": self._scanner_healthy,
                        "context_interval_s": self._context_interval,
                    },
                )
                await self._spine.publish(msg.topic(), msg.encode())
            except Exception as e:
                logger.error(f"[{self.SERVICE_NAME}] Telemetry error: {e}")

            await asyncio.sleep(2.0)


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

async def main() -> None:
    """Run Scanner Feed as a standalone service."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    service = ScannerFeedService()
    try:
        await service.start()
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        await service.stop()


if __name__ == "__main__":
    asyncio.run(main())
