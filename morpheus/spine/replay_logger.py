"""
Replay Logger - Service 5 of the Morpheus Data Spine.

Subscribes to ALL NATS topics and writes JSONL event logs for replay.
Buffered writes with periodic flush. Never blocks the message flow.

Publishes:
  bot.telemetry.replay_logger - Every 2s

Output:
  logs/spine/YYYY-MM-DD/events.jsonl     - All events
  logs/spine/YYYY-MM-DD/quotes.jsonl     - Quote ticks (separate for size)
  logs/spine/YYYY-MM-DD/bars.jsonl       - OHLCV bars
  logs/spine/YYYY-MM-DD/decisions.jsonl  - AI decisions + signals
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from morpheus.spine.nats_client import SpineClient
from morpheus.spine.config_store import ConfigStore
from morpheus.spine.schemas import Topics, TelemetryMsg, decode

logger = logging.getLogger(__name__)


class BufferedWriter:
    """Buffered JSONL file writer with periodic flush."""

    def __init__(self, path: Path, flush_interval: float = 1.0):
        self._path = path
        self._flush_interval = flush_interval
        self._buffer: deque = deque()
        self._file = None
        self._lines_written = 0

    def open(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self._path, "a", encoding="utf-8")

    def write(self, data: dict) -> None:
        """Add a record to the buffer (non-blocking)."""
        self._buffer.append(data)

    def flush(self) -> int:
        """Flush buffer to disk. Returns number of lines flushed."""
        if not self._file or not self._buffer:
            return 0

        count = 0
        while self._buffer:
            record = self._buffer.popleft()
            try:
                line = json.dumps(record, default=str)
                self._file.write(line + "\n")
                count += 1
            except Exception as e:
                logger.warning(f"[REPLAY] Failed to serialize record: {e}")

        self._file.flush()
        self._lines_written += count
        return count

    def close(self) -> None:
        self.flush()
        if self._file:
            self._file.close()
            self._file = None

    @property
    def lines_written(self) -> int:
        return self._lines_written


class ReplayLoggerService:
    """
    Replay Logger for the NATS spine.

    Captures all NATS traffic for post-session replay and analysis.
    Writes are buffered and non-blocking to avoid slowing message flow.
    """

    SERVICE_NAME = "replay_logger"

    def __init__(self, config_store: ConfigStore | None = None):
        self._config_store = config_store or ConfigStore()
        config = self._config_store.get()

        self._spine = SpineClient(
            url=config.nats_url,
            name=self.SERVICE_NAME,
        )

        self._log_dir = Path(config.replay_log_dir)
        self._flush_interval = config.replay_flush_interval_seconds

        # Date-based directory for today's logs
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        day_dir = self._log_dir / today

        # Separate writers for different data types
        self._events_writer = BufferedWriter(day_dir / "events.jsonl", self._flush_interval)
        self._quotes_writer = BufferedWriter(day_dir / "quotes.jsonl", self._flush_interval)
        self._bars_writer = BufferedWriter(day_dir / "bars.jsonl", self._flush_interval)
        self._decisions_writer = BufferedWriter(day_dir / "decisions.jsonl", self._flush_interval)

        # Telemetry
        self._start_time = 0.0
        self._msgs_received = 0
        self._errors = 0

        # Control
        self._running = False
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        """Start the replay logger service."""
        logger.info(f"[{self.SERVICE_NAME}] Starting...")
        self._start_time = time.time()
        self._running = True

        # Open writers
        self._events_writer.open()
        self._quotes_writer.open()
        self._bars_writer.open()
        self._decisions_writer.open()

        # Connect to NATS
        await self._spine.connect()

        # Subscribe to everything
        await self._spine.subscribe_decode("md.quotes.*", self._on_quote)
        await self._spine.subscribe_decode("md.ohlc.>", self._on_bar)
        await self._spine.subscribe_decode("ai.>", self._on_decision)
        await self._spine.subscribe_decode("scanner.>", self._on_event)
        await self._spine.subscribe_decode("bot.>", self._on_event)

        # Start flush loop and telemetry
        self._tasks.append(asyncio.create_task(self._flush_loop()))
        self._tasks.append(asyncio.create_task(self._telemetry_loop()))

        logger.info(
            f"[{self.SERVICE_NAME}] Started, logging to {self._log_dir}"
        )

    async def stop(self) -> None:
        """Stop the replay logger service."""
        logger.info(f"[{self.SERVICE_NAME}] Stopping...")
        self._running = False

        for task in self._tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Final flush and close
        self._events_writer.close()
        self._quotes_writer.close()
        self._bars_writer.close()
        self._decisions_writer.close()

        await self._spine.close()

        total = (
            self._events_writer.lines_written
            + self._quotes_writer.lines_written
            + self._bars_writer.lines_written
            + self._decisions_writer.lines_written
        )
        logger.info(f"[{self.SERVICE_NAME}] Stopped. {total} lines written.")

    # -----------------------------------------------------------------------
    # NATS handlers
    # -----------------------------------------------------------------------

    async def _on_quote(self, subject: str, data: dict) -> None:
        """Log quote tick."""
        self._msgs_received += 1
        data["_subject"] = subject
        data["_rx_ts"] = time.time()
        self._quotes_writer.write(data)

    async def _on_bar(self, subject: str, data: dict) -> None:
        """Log OHLCV bar."""
        self._msgs_received += 1
        data["_subject"] = subject
        data["_rx_ts"] = time.time()
        self._bars_writer.write(data)

    async def _on_decision(self, subject: str, data: dict) -> None:
        """Log AI decision (signal, structure, regime, order)."""
        self._msgs_received += 1
        data["_subject"] = subject
        data["_rx_ts"] = time.time()
        self._decisions_writer.write(data)

    async def _on_event(self, subject: str, data: dict) -> None:
        """Log scanner/bot events."""
        self._msgs_received += 1
        data["_subject"] = subject
        data["_rx_ts"] = time.time()
        self._events_writer.write(data)

    # -----------------------------------------------------------------------
    # Flush loop
    # -----------------------------------------------------------------------

    async def _flush_loop(self) -> None:
        """Periodically flush all writers to disk."""
        while self._running:
            try:
                flushed = 0
                flushed += self._events_writer.flush()
                flushed += self._quotes_writer.flush()
                flushed += self._bars_writer.flush()
                flushed += self._decisions_writer.flush()

                if flushed > 0:
                    logger.debug(f"[{self.SERVICE_NAME}] Flushed {flushed} lines")
            except Exception as e:
                self._errors += 1
                logger.error(f"[{self.SERVICE_NAME}] Flush error: {e}")

            await asyncio.sleep(self._flush_interval)

    # -----------------------------------------------------------------------
    # Telemetry
    # -----------------------------------------------------------------------

    async def _telemetry_loop(self) -> None:
        """Publish telemetry every 2 seconds."""
        while self._running:
            try:
                msg = TelemetryMsg(
                    service=self.SERVICE_NAME,
                    status="running" if self._spine.is_connected else "degraded",
                    uptime_s=time.time() - self._start_time,
                    msgs_in=self._msgs_received,
                    errors=self._errors,
                    extra={
                        "events_written": self._events_writer.lines_written,
                        "quotes_written": self._quotes_writer.lines_written,
                        "bars_written": self._bars_writer.lines_written,
                        "decisions_written": self._decisions_writer.lines_written,
                        "log_dir": str(self._log_dir),
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
    """Run Replay Logger as a standalone service."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    service = ReplayLoggerService()
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
