"""
AI Core - Service 3 of the Morpheus Data Spine.

The brain of Morpheus. Subscribes to market data and scanner context from NATS,
runs the full MASS pipeline, and publishes decisions back to NATS.

Subscribes to:
  md.ohlc.1m.{sym}       - 1-minute bars (triggers pipeline)
  md.quotes.{sym}         - Real-time quotes (snapshot updates)
  scanner.context.{sym}   - Scanner context (cached locally, not HTTP)
  scanner.discovery       - New symbols to track

Publishes:
  ai.signals.{sym}        - Entry/exit signals (after full pipeline)
  ai.structure.{sym}      - MASS structure grades
  ai.regime               - Regime changes
  ai.orders               - Order lifecycle events
  bot.telemetry.ai_core   - Every 2s

Key difference from monolith:
- Scanner context comes from NATS cache, NOT per-quote HTTP calls
- Pipeline triggered by 1-min bars from NATS, not direct callbacks
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv

load_dotenv()

from morpheus.spine.nats_client import SpineClient
from morpheus.spine.config_store import ConfigStore
from morpheus.spine.schemas import (
    Topics,
    OHLCMsg,
    QuoteMsg,
    ScannerContextMsg,
    ScannerDiscoveryMsg,
    SignalMsg,
    StructureMsg,
    RegimeMsg,
    TelemetryMsg,
    decode,
)

# Pipeline components (reuse existing)
from morpheus.orchestrator.pipeline import SignalPipeline, PipelineConfig
from morpheus.features.indicators import OHLCV
from morpheus.core.events import Event, EventType, create_event
from morpheus.core.mass_config import MASSConfig
from morpheus.data.market_snapshot import MarketSnapshot

logger = logging.getLogger(__name__)


class AICoreService:
    """
    AI Core service for the NATS spine.

    Wraps the existing SignalPipeline, feeding it data from NATS
    instead of direct callbacks from the monolith server.

    Scanner context is cached locally from NATS subscriptions,
    eliminating the per-quote HTTP polling overhead.
    """

    SERVICE_NAME = "ai_core"

    def __init__(self, config_store: ConfigStore | None = None):
        self._config_store = config_store or ConfigStore()
        config = self._config_store.get()

        self._spine = SpineClient(
            url=config.nats_url,
            name=self.SERVICE_NAME,
        )

        # Pipeline configuration
        pipeline_config = PipelineConfig(
            min_bars_warmup=config.pipeline_min_bars_warmup,
            max_bars_history=config.pipeline_max_bars_history,
            permissive_mode=config.pipeline_permissive_mode,
        )

        # Initialize the pipeline with NATS-based event emission
        self._pipeline = SignalPipeline(
            config=pipeline_config,
            emit_event=self._on_pipeline_event,
            external_context_provider=self._get_cached_scanner_context,
            mass_config=MASSConfig(),
        )

        # Local cache of scanner context per symbol (from NATS, not HTTP!)
        self._scanner_cache: dict[str, dict[str, Any]] = {}
        self._scanner_cache_ts: dict[str, float] = {}

        # Latest quote per symbol (for snapshot building)
        self._latest_quotes: dict[str, QuoteMsg] = {}

        # Telemetry
        self._start_time = 0.0
        self._bars_processed = 0
        self._signals_emitted = 0
        self._events_published = 0
        self._errors = 0

        # Control
        self._running = False
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        """Start the AI Core service."""
        logger.info(f"[{self.SERVICE_NAME}] Starting...")
        self._start_time = time.time()
        self._running = True

        # Connect to NATS
        await self._spine.connect()

        # Subscribe to data feeds
        await self._spine.subscribe_decode(
            "md.ohlc.1m.*", self._on_ohlc_bar
        )
        await self._spine.subscribe_decode(
            "md.quotes.*", self._on_quote
        )
        await self._spine.subscribe_decode(
            "scanner.context.*", self._on_scanner_context
        )
        await self._spine.subscribe_decode(
            Topics.SCANNER_DISCOVERY, self._on_discovery
        )

        # Start pipeline
        await self._pipeline.start()

        # Start telemetry
        self._tasks.append(asyncio.create_task(self._telemetry_loop()))

        logger.info(f"[{self.SERVICE_NAME}] Started, subscribed to NATS feeds")

    async def stop(self) -> None:
        """Stop the AI Core service."""
        logger.info(f"[{self.SERVICE_NAME}] Stopping...")
        self._running = False

        for task in self._tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        await self._pipeline.stop()
        await self._spine.close()
        logger.info(f"[{self.SERVICE_NAME}] Stopped")

    # -----------------------------------------------------------------------
    # NATS message handlers
    # -----------------------------------------------------------------------

    async def _on_ohlc_bar(self, subject: str, data: dict) -> None:
        """
        Handle 1-minute OHLCV bar from NATS → trigger pipeline.

        This is the primary pipeline trigger, replacing the direct
        CandleAggregator → pipeline.on_candle() callback chain.
        """
        try:
            msg = OHLCMsg(**data)
            symbol = msg.sym.upper()

            # Add symbol if not tracked
            if symbol not in self._pipeline.get_active_symbols():
                self._pipeline.add_symbol(symbol)

            # Convert to OHLCV and feed to pipeline
            candle = OHLCV(
                open=msg.o,
                high=msg.h,
                low=msg.l,
                close=msg.c,
                volume=msg.v,
            )
            timestamp = datetime.fromtimestamp(msg.ts, tz=timezone.utc)

            await self._pipeline.on_candle(symbol, candle, timestamp)
            self._bars_processed += 1

        except Exception as e:
            self._errors += 1
            logger.error(f"[{self.SERVICE_NAME}] OHLC bar error: {e}")

    async def _on_quote(self, subject: str, data: dict) -> None:
        """
        Handle real-time quote from NATS → update latest snapshot.

        Quotes are NOT used to trigger the full pipeline (that's bars).
        They update the latest snapshot for snapshot-based evaluations.
        """
        try:
            msg = QuoteMsg(**data)
            self._latest_quotes[msg.sym.upper()] = msg
        except Exception as e:
            self._errors += 1
            logger.error(f"[{self.SERVICE_NAME}] Quote error: {e}")

    async def _on_scanner_context(self, subject: str, data: dict) -> None:
        """
        Handle scanner context from NATS → cache locally.

        This is the KEY DIFFERENCE from the monolith:
        - Old: Pipeline calls get_symbol_context() via HTTP on every quote tick
        - New: Scanner context arrives via NATS every 15s, cached here
        - Pipeline reads from local cache (zero HTTP overhead)
        """
        try:
            msg = ScannerContextMsg(**data)
            self._scanner_cache[msg.sym.upper()] = msg.to_pipeline_dict()
            self._scanner_cache_ts[msg.sym.upper()] = msg.ts
        except Exception as e:
            self._errors += 1
            logger.error(f"[{self.SERVICE_NAME}] Scanner context error: {e}")

    async def _on_discovery(self, subject: str, data: dict) -> None:
        """Handle scanner discovery → add symbol to pipeline tracking."""
        sym = data.get("sym", "")
        if sym:
            sym = sym.upper()
            self._pipeline.add_symbol(sym)
            logger.info(f"[{self.SERVICE_NAME}] Tracking new symbol: {sym}")

    # -----------------------------------------------------------------------
    # Pipeline integration
    # -----------------------------------------------------------------------

    async def _get_cached_scanner_context(self, symbol: str) -> dict[str, Any]:
        """
        Return cached scanner context for a symbol.

        This replaces the HTTP call to scanner. The context was published
        by scanner_feed every 15s and cached here from NATS subscription.
        Zero HTTP overhead. Instant response.
        """
        return self._scanner_cache.get(symbol.upper(), {})

    async def _on_pipeline_event(self, event: Event) -> None:
        """
        Handle events emitted by the pipeline → publish to NATS.

        Maps pipeline Event objects to NATS topic/message pairs.
        """
        try:
            self._events_published += 1

            # Route events to appropriate NATS topics
            etype = event.event_type
            symbol = event.symbol or ""
            payload = event.payload

            if etype == EventType.SIGNAL_CANDIDATE:
                msg = SignalMsg(
                    sym=symbol,
                    direction=payload.get("direction", ""),
                    strategy=payload.get("strategy_name", ""),
                    confidence=payload.get("confidence", 0),
                    rationale=payload.get("rationale", ""),
                    entry_ref=payload.get("entry_reference"),
                    invalidation_ref=payload.get("invalidation_reference"),
                    target_ref=payload.get("target_reference"),
                    signal_mode=payload.get("signal_mode", "ACTIVE"),
                    market_mode=payload.get("market_mode", ""),
                    structure_grade=payload.get("structure_grade", ""),
                )
                await self._spine.publish(
                    Topics.signal(symbol), msg.encode()
                )
                self._signals_emitted += 1

            elif etype == EventType.STRUCTURE_CLASSIFIED:
                msg = StructureMsg(
                    sym=symbol,
                    grade=payload.get("grade", ""),
                    score=payload.get("score", 0),
                    components=payload.get("components", {}),
                    rationale=payload.get("rationale", ""),
                    tags=payload.get("tags", []),
                )
                await self._spine.publish(
                    Topics.structure(symbol), msg.encode()
                )

            elif etype == EventType.REGIME_DETECTED:
                msg = RegimeMsg(
                    trend=payload.get("trend", ""),
                    volatility=payload.get("volatility", ""),
                    momentum=payload.get("momentum", ""),
                    confidence=payload.get("confidence", 0),
                    mass_mode=payload.get("mass_mode", ""),
                    aggression=payload.get("aggression_multiplier", 1.0),
                    max_positions=payload.get("max_concurrent_positions", 3),
                )
                await self._spine.publish(Topics.REGIME, msg.encode())

            elif etype in (
                EventType.RISK_APPROVED,
                EventType.RISK_VETO,
                EventType.META_APPROVED,
                EventType.META_REJECTED,
                EventType.SIGNAL_SCORED,
                EventType.SIGNAL_OBSERVED,
            ):
                # Publish all decision events to the decisions lane
                await self._spine.publish_msg(
                    f"ai.decisions.{symbol}",
                    {
                        "event_type": etype.value,
                        "symbol": symbol,
                        "payload": payload,
                        "ts": time.time(),
                    },
                )

            elif etype in (
                EventType.ORDER_SUBMITTED,
                EventType.ORDER_CONFIRMED,
                EventType.ORDER_FILL_RECEIVED,
                EventType.ORDER_CANCELLED,
                EventType.ORDER_REJECTED,
            ):
                await self._spine.publish_msg(
                    Topics.ORDERS,
                    {
                        "event_type": etype.value,
                        "symbol": symbol,
                        "payload": payload,
                        "ts": time.time(),
                    },
                )

            # Also publish ALL events in the generic event format for UI Gateway
            await self._spine.publish_msg(
                f"ai.events.{symbol or 'system'}",
                event.to_jsonl_dict(),
            )

        except Exception as e:
            self._errors += 1
            logger.error(f"[{self.SERVICE_NAME}] Event publish error: {e}")

    # -----------------------------------------------------------------------
    # Telemetry
    # -----------------------------------------------------------------------

    async def _telemetry_loop(self) -> None:
        """Publish telemetry every 2 seconds."""
        while self._running:
            try:
                pipeline_status = self._pipeline.get_status()
                msg = TelemetryMsg(
                    service=self.SERVICE_NAME,
                    status="running" if self._spine.is_connected else "degraded",
                    uptime_s=time.time() - self._start_time,
                    msgs_in=self._bars_processed,
                    msgs_out=self._events_published,
                    errors=self._errors,
                    extra={
                        "bars_processed": self._bars_processed,
                        "signals_emitted": self._signals_emitted,
                        "events_published": self._events_published,
                        "active_symbols": len(pipeline_status.get("active_symbols", [])),
                        "scanner_cache_size": len(self._scanner_cache),
                        "market_mode": pipeline_status.get("market_mode", ""),
                        "mass_enabled": pipeline_status.get("mass_enabled", False),
                        "kill_switch": pipeline_status.get("kill_switch_active", False),
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
    """Run AI Core as a standalone service."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    service = AICoreService()
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
