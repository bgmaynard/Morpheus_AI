"""
NATS Client Wrapper for Morpheus Data Spine.

Provides:
- Async NATS connection with auto-reconnect
- MsgPack encode/decode via schemas
- Subscription helpers with typed callbacks
- JetStream support for durable lanes
- Health check publishing
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Callable, Awaitable

import nats
from nats.aio.client import Client as NATSClient
from nats.aio.msg import Msg
from nats.js import JetStreamContext

from morpheus.spine.schemas import encode, decode

logger = logging.getLogger(__name__)


class SpineClient:
    """
    Async NATS client wrapper for the Morpheus data spine.

    Usage:
        client = SpineClient(url="nats://localhost:4222", name="schwab_feed")
        await client.connect()
        await client.publish("md.quotes.AAPL", quote_msg.encode())
        await client.subscribe("md.quotes.*", callback=on_quote)
        await client.close()
    """

    def __init__(
        self,
        url: str = "nats://localhost:4222",
        name: str = "morpheus",
        max_reconnect_attempts: int = -1,  # infinite
        reconnect_time_wait: float = 2.0,
    ):
        self._url = url
        self._name = name
        self._max_reconnect = max_reconnect_attempts
        self._reconnect_wait = reconnect_time_wait
        self._nc: NATSClient | None = None
        self._js: JetStreamContext | None = None
        self._subscriptions: list[nats.aio.subscription.Subscription] = []
        self._connected = False
        self._stats = {"msgs_in": 0, "msgs_out": 0, "errors": 0, "reconnects": 0}
        self._connect_time: float = 0.0

    @property
    def is_connected(self) -> bool:
        return self._connected and self._nc is not None and self._nc.is_connected

    @property
    def stats(self) -> dict[str, Any]:
        return {
            **self._stats,
            "connected": self.is_connected,
            "uptime_s": time.time() - self._connect_time if self._connect_time else 0,
        }

    @property
    def js(self) -> JetStreamContext | None:
        return self._js

    async def connect(self) -> None:
        """Connect to NATS server with auto-reconnect."""
        if self._nc and self._nc.is_connected:
            return

        async def on_disconnect():
            self._connected = False
            logger.warning(f"[NATS:{self._name}] Disconnected")

        async def on_reconnect():
            self._connected = True
            self._stats["reconnects"] += 1
            logger.info(f"[NATS:{self._name}] Reconnected")

        async def on_error(e: Exception):
            self._stats["errors"] += 1
            logger.error(f"[NATS:{self._name}] Error: {e}")

        async def on_closed():
            self._connected = False
            logger.info(f"[NATS:{self._name}] Connection closed")

        try:
            self._nc = await nats.connect(
                servers=[self._url],
                name=self._name,
                max_reconnect_attempts=self._max_reconnect,
                reconnect_time_wait=self._reconnect_wait,
                disconnected_cb=on_disconnect,
                reconnected_cb=on_reconnect,
                error_cb=on_error,
                closed_cb=on_closed,
            )
            self._connected = True
            self._connect_time = time.time()

            # Initialize JetStream
            self._js = self._nc.jetstream()

            logger.info(
                f"[NATS:{self._name}] Connected to {self._url} "
                f"(server={self._nc.connected_url})"
            )
        except Exception as e:
            self._stats["errors"] += 1
            logger.error(f"[NATS:{self._name}] Failed to connect: {e}")
            raise

    async def ensure_streams(self) -> None:
        """
        Create JetStream streams for durable lanes.

        Lane 2 (Aggregated): 1hr retention for md.ohlc.*, md.features.*, scanner.context.*
        Lane 3 (Decisions):  24hr retention for ai.*, bot.*
        """
        if not self._js:
            logger.warning("[NATS] JetStream not available, skipping stream setup")
            return

        from nats.js.api import StreamConfig, RetentionPolicy

        # Lane 2: Aggregated data (1hr retention)
        try:
            await self._js.add_stream(
                config=StreamConfig(
                    name="AGGREGATED",
                    subjects=[
                        "md.ohlc.>",
                        "md.features.>",
                        "scanner.context.>",
                    ],
                    retention=RetentionPolicy.LIMITS,
                    max_age=3600,  # 1 hour in seconds
                    max_bytes=500 * 1024 * 1024,  # 500MB
                    storage="memory",
                ),
            )
            logger.info("[NATS] Stream AGGREGATED created/updated (1hr, memory)")
        except Exception as e:
            # Stream may already exist - try update
            try:
                await self._js.update_stream(
                    config=StreamConfig(
                        name="AGGREGATED",
                        subjects=[
                            "md.ohlc.>",
                            "md.features.>",
                            "scanner.context.>",
                        ],
                        retention=RetentionPolicy.LIMITS,
                        max_age=3600,
                        max_bytes=500 * 1024 * 1024,
                        storage="memory",
                    ),
                )
            except Exception:
                logger.debug(f"[NATS] AGGREGATED stream already exists: {e}")

        # Lane 3: Decisions (24hr retention)
        try:
            await self._js.add_stream(
                config=StreamConfig(
                    name="DECISIONS",
                    subjects=[
                        "ai.>",
                        "bot.>",
                    ],
                    retention=RetentionPolicy.LIMITS,
                    max_age=86400,  # 24 hours
                    max_bytes=1024 * 1024 * 1024,  # 1GB
                    storage="file",
                ),
            )
            logger.info("[NATS] Stream DECISIONS created/updated (24hr, file)")
        except Exception as e:
            try:
                await self._js.update_stream(
                    config=StreamConfig(
                        name="DECISIONS",
                        subjects=[
                            "ai.>",
                            "bot.>",
                        ],
                        retention=RetentionPolicy.LIMITS,
                        max_age=86400,
                        max_bytes=1024 * 1024 * 1024,
                        storage="file",
                    ),
                )
            except Exception:
                logger.debug(f"[NATS] DECISIONS stream already exists: {e}")

    async def publish(self, subject: str, data: bytes) -> None:
        """Publish a message to a subject."""
        if not self._nc or not self._nc.is_connected:
            self._stats["errors"] += 1
            return

        try:
            await self._nc.publish(subject, data)
            self._stats["msgs_out"] += 1
        except Exception as e:
            self._stats["errors"] += 1
            logger.error(f"[NATS:{self._name}] Publish error on {subject}: {e}")

    async def publish_msg(self, subject: str, obj: dict) -> None:
        """Publish a dict as MsgPack-encoded message."""
        await self.publish(subject, encode(obj))

    async def subscribe(
        self,
        subject: str,
        callback: Callable[[Msg], Awaitable[None]],
        queue: str = "",
    ) -> None:
        """
        Subscribe to a subject with an async callback.

        Args:
            subject: NATS subject (supports wildcards: *, >)
            callback: Async function receiving nats.Msg
            queue: Optional queue group for load balancing
        """
        if not self._nc:
            raise RuntimeError("Not connected")

        async def _wrapped(msg: Msg) -> None:
            self._stats["msgs_in"] += 1
            try:
                await callback(msg)
            except Exception as e:
                self._stats["errors"] += 1
                logger.error(
                    f"[NATS:{self._name}] Callback error on {msg.subject}: {e}"
                )

        sub = await self._nc.subscribe(subject, cb=_wrapped, queue=queue)
        self._subscriptions.append(sub)
        logger.debug(f"[NATS:{self._name}] Subscribed to {subject}")

    async def subscribe_decode(
        self,
        subject: str,
        callback: Callable[[str, dict], Awaitable[None]],
        queue: str = "",
    ) -> None:
        """
        Subscribe with auto-decode of MsgPack payload.

        Callback receives (subject: str, data: dict).
        """
        async def _handler(msg: Msg) -> None:
            data = decode(msg.data)
            await callback(msg.subject, data)

        await self.subscribe(subject, _handler, queue=queue)

    async def request(
        self,
        subject: str,
        data: bytes,
        timeout: float = 5.0,
    ) -> bytes:
        """Send a request and wait for a reply."""
        if not self._nc:
            raise RuntimeError("Not connected")

        response = await self._nc.request(subject, data, timeout=timeout)
        return response.data

    async def close(self) -> None:
        """Close connection and clean up subscriptions."""
        for sub in self._subscriptions:
            try:
                await sub.unsubscribe()
            except Exception:
                pass
        self._subscriptions.clear()

        if self._nc:
            try:
                await self._nc.drain()
            except Exception:
                pass
            self._nc = None

        self._connected = False
        logger.info(f"[NATS:{self._name}] Closed")

    async def flush(self, timeout: float = 5.0) -> None:
        """Flush pending messages."""
        if self._nc and self._nc.is_connected:
            await self._nc.flush(timeout=int(timeout))
