"""
Schwab Streaming Client.

WebSocket-based streaming for real-time quotes and account activity.
Replaces polling with a single persistent connection.

Based on Schwab's streaming API:
- wss://streamer-api.schwab.com/ws
- Services: LEVELONE_EQUITIES, ACCT_ACTIVITY
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Callable, Awaitable
from dataclasses import dataclass

import websockets
from websockets.client import WebSocketClientProtocol

logger = logging.getLogger(__name__)


# Field indices for LEVELONE_EQUITIES
# https://developer.schwab.com/products/trader-api--individual/details/documentation/Streaming
class QuoteFields:
    """Field indices for LEVELONE_EQUITIES streaming."""
    SYMBOL = "0"
    BID_PRICE = "1"
    ASK_PRICE = "2"
    LAST_PRICE = "3"
    BID_SIZE = "4"
    ASK_SIZE = "5"
    ASK_ID = "6"
    BID_ID = "7"
    TOTAL_VOLUME = "8"
    LAST_SIZE = "9"
    HIGH_PRICE = "10"
    LOW_PRICE = "11"
    CLOSE_PRICE = "12"
    EXCHANGE_ID = "13"
    MARGINABLE = "14"
    DESCRIPTION = "15"
    LAST_ID = "16"
    OPEN_PRICE = "17"
    NET_CHANGE = "18"
    HIGH_52_WEEK = "19"
    LOW_52_WEEK = "20"
    PE_RATIO = "21"
    DIVIDEND_AMOUNT = "22"
    DIVIDEND_YIELD = "23"
    NAV = "24"
    EXCHANGE_NAME = "25"
    DIVIDEND_DATE = "26"
    IS_REGULAR_MARKET_QUOTE = "27"
    IS_REGULAR_MARKET_TRADE = "28"
    SECURITY_STATUS = "29"
    MARK = "30"
    QUOTE_TIME_MS = "31"
    TRADE_TIME_MS = "32"
    REGULAR_MARKET_LAST_PRICE = "33"

    # Common fields for quotes
    DEFAULT = "0,1,2,3,4,5,8,10,11,12,17,18,30,31,32"


@dataclass
class StreamerInfo:
    """Schwab streamer connection info."""
    socket_url: str
    customer_id: str
    correl_id: str
    channel: str
    function_id: str


@dataclass
class QuoteUpdate:
    """Parsed quote update from stream."""
    symbol: str
    bid_price: float | None = None
    ask_price: float | None = None
    last_price: float | None = None
    bid_size: int | None = None
    ask_size: int | None = None
    volume: int | None = None
    high_price: float | None = None
    low_price: float | None = None
    close_price: float | None = None
    open_price: float | None = None
    net_change: float | None = None
    mark: float | None = None
    quote_time: datetime | None = None
    trade_time: datetime | None = None


@dataclass
class AccountActivity:
    """Parsed account activity from stream."""
    account: str
    message_type: str
    message_data: dict[str, Any]


class SchwabStreamer:
    """
    Async WebSocket streaming client for Schwab.

    Provides real-time quotes and account activity updates.
    """

    STREAMER_URL = "wss://streamer-api.schwab.com/ws"

    def __init__(
        self,
        streamer_info: StreamerInfo,
        access_token: str,
        on_quote: Callable[[QuoteUpdate], Awaitable[None]] | None = None,
        on_account_activity: Callable[[AccountActivity], Awaitable[None]] | None = None,
        on_error: Callable[[Exception], Awaitable[None]] | None = None,
    ):
        """
        Initialize streamer.

        Args:
            streamer_info: Connection credentials from userPreference endpoint
            access_token: Current OAuth access token
            on_quote: Async callback for quote updates
            on_account_activity: Async callback for account activity
            on_error: Async callback for errors
        """
        self.info = streamer_info
        self.access_token = access_token
        self.on_quote = on_quote
        self.on_account_activity = on_account_activity
        self.on_error = on_error

        self._ws: WebSocketClientProtocol | None = None
        self._request_id = 0
        self._running = False
        self._subscribed_symbols: set[str] = set()
        self._reconnect_delay = 2.0
        self._max_reconnect_delay = 120.0

    def _next_request_id(self) -> int:
        """Get next request ID."""
        self._request_id += 1
        return self._request_id

    def _build_request(
        self,
        service: str,
        command: str,
        parameters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build a streaming request message."""
        return {
            "service": service,
            "command": command,
            "requestid": self._next_request_id(),
            "SchwabClientCustomerId": self.info.customer_id,
            "SchwabClientCorrelId": self.info.correl_id,
            "parameters": parameters or {},
        }

    async def _send(self, request: dict[str, Any]) -> None:
        """Send request to WebSocket."""
        if self._ws:
            msg = json.dumps({"requests": [request]})
            await self._ws.send(msg)
            logger.debug(f"[STREAM] Sent: {request['service']}/{request['command']}")

    async def _login(self) -> bool:
        """Send login request."""
        request = self._build_request(
            service="ADMIN",
            command="LOGIN",
            parameters={
                "Authorization": self.access_token,
                "SchwabClientChannel": self.info.channel,
                "SchwabClientFunctionId": self.info.function_id,
            },
        )
        await self._send(request)

        # Wait for login response
        try:
            response = await asyncio.wait_for(self._ws.recv(), timeout=10.0)
            data = json.loads(response)

            # Check login response
            if "response" in data:
                for resp in data["response"]:
                    if resp.get("service") == "ADMIN" and resp.get("command") == "LOGIN":
                        content = resp.get("content", {})
                        code = content.get("code")
                        if code == 0:
                            logger.info("[STREAM] Login successful")
                            return True
                        else:
                            logger.error(f"[STREAM] Login failed: {content.get('msg', 'unknown')}")
                            return False

            logger.warning(f"[STREAM] Unexpected login response: {data}")
            return False

        except asyncio.TimeoutError:
            logger.error("[STREAM] Login timeout")
            return False

    async def subscribe_quotes(self, symbols: list[str]) -> None:
        """
        Subscribe to level 1 quotes for symbols.

        Args:
            symbols: List of stock symbols
        """
        if not symbols:
            return

        # Track subscribed symbols
        self._subscribed_symbols.update(symbols)

        request = self._build_request(
            service="LEVELONE_EQUITIES",
            command="ADD",
            parameters={
                "keys": ",".join(symbols),
                "fields": QuoteFields.DEFAULT,
            },
        )
        await self._send(request)
        logger.info(f"[STREAM] Subscribed to quotes: {symbols}")

    async def unsubscribe_quotes(self, symbols: list[str]) -> None:
        """Unsubscribe from quotes."""
        if not symbols:
            return

        self._subscribed_symbols -= set(symbols)

        request = self._build_request(
            service="LEVELONE_EQUITIES",
            command="UNSUBS",
            parameters={
                "keys": ",".join(symbols),
            },
        )
        await self._send(request)
        logger.info(f"[STREAM] Unsubscribed from quotes: {symbols}")

    async def subscribe_account_activity(self) -> None:
        """Subscribe to account activity (orders, fills, positions)."""
        request = self._build_request(
            service="ACCT_ACTIVITY",
            command="SUBS",
            parameters={
                "keys": "Account Activity",
                "fields": "0,1,2,3",
            },
        )
        await self._send(request)
        logger.info("[STREAM] Subscribed to account activity")

    def _parse_quote(self, content: dict[str, Any]) -> QuoteUpdate | None:
        """Parse a quote update from stream data."""
        try:
            symbol = content.get("key", content.get("0", ""))
            if not symbol:
                return None

            def safe_float(key: str) -> float | None:
                val = content.get(key)
                if val is None:
                    return None
                try:
                    return float(val)
                except (ValueError, TypeError):
                    return None

            def safe_int(key: str) -> int | None:
                val = content.get(key)
                if val is None:
                    return None
                try:
                    return int(float(val))  # Handle string numbers
                except (ValueError, TypeError):
                    return None

            def safe_datetime(key: str) -> datetime | None:
                val = content.get(key)
                if val is None:
                    return None
                try:
                    # Handle both int and string timestamps
                    ts = float(val) / 1000
                    return datetime.fromtimestamp(ts, tz=timezone.utc)
                except (ValueError, TypeError):
                    return None

            return QuoteUpdate(
                symbol=symbol,
                bid_price=safe_float("1"),
                ask_price=safe_float("2"),
                last_price=safe_float("3"),
                bid_size=safe_int("4"),
                ask_size=safe_int("5"),
                volume=safe_int("8"),
                high_price=safe_float("10"),
                low_price=safe_float("11"),
                close_price=safe_float("12"),
                open_price=safe_float("17"),
                net_change=safe_float("18"),
                mark=safe_float("30"),
                quote_time=safe_datetime("31"),
                trade_time=safe_datetime("32"),
            )
        except Exception as e:
            logger.warning(f"[STREAM] Failed to parse quote: {e}")
            return None

    def _parse_account_activity(self, content: dict[str, Any]) -> AccountActivity | None:
        """Parse account activity from stream data."""
        try:
            return AccountActivity(
                account=content.get("1", ""),
                message_type=content.get("2", ""),
                message_data=content.get("3", {}),
            )
        except Exception as e:
            logger.warning(f"[STREAM] Failed to parse account activity: {e}")
            return None

    async def _handle_message(self, message: str) -> None:
        """Handle incoming WebSocket message."""
        try:
            data = json.loads(message)

            # Handle response messages (confirmations)
            if "response" in data:
                for resp in data["response"]:
                    service = resp.get("service", "")
                    command = resp.get("command", "")
                    content = resp.get("content", {})
                    logger.debug(f"[STREAM] Response: {service}/{command} - {content.get('msg', 'ok')}")

            # Handle data messages (actual updates)
            if "data" in data:
                for item in data["data"]:
                    service = item.get("service", "")

                    if service == "LEVELONE_EQUITIES":
                        for content in item.get("content", []):
                            quote = self._parse_quote(content)
                            if quote and self.on_quote:
                                await self.on_quote(quote)

                    elif service == "ACCT_ACTIVITY":
                        for content in item.get("content", []):
                            activity = self._parse_account_activity(content)
                            if activity and self.on_account_activity:
                                await self.on_account_activity(activity)

            # Handle notify messages (heartbeats, etc.)
            if "notify" in data:
                for notify in data["notify"]:
                    heartbeat = notify.get("heartbeat")
                    if heartbeat:
                        logger.debug(f"[STREAM] Heartbeat: {heartbeat}")

        except json.JSONDecodeError as e:
            logger.warning(f"[STREAM] Invalid JSON: {e}")
        except Exception as e:
            logger.error(f"[STREAM] Error handling message: {e}")
            if self.on_error:
                await self.on_error(e)

    async def _run_loop(self) -> None:
        """Main streaming loop with reconnection."""
        while self._running:
            try:
                logger.info(f"[STREAM] Connecting to {self.STREAMER_URL}...")

                async with websockets.connect(
                    self.STREAMER_URL,
                    ping_interval=30,
                    ping_timeout=10,
                ) as ws:
                    self._ws = ws
                    self._reconnect_delay = 2.0  # Reset delay on successful connect

                    # Login
                    if not await self._login():
                        logger.error("[STREAM] Login failed, retrying...")
                        await asyncio.sleep(self._reconnect_delay)
                        continue

                    # Resubscribe to symbols
                    if self._subscribed_symbols:
                        await self.subscribe_quotes(list(self._subscribed_symbols))

                    # Subscribe to account activity
                    await self.subscribe_account_activity()

                    # Message loop
                    async for message in ws:
                        if not self._running:
                            break
                        await self._handle_message(message)

            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"[STREAM] Connection closed: {e}")
            except Exception as e:
                logger.error(f"[STREAM] Error: {e}")
                if self.on_error:
                    await self.on_error(e)

            # Reconnect with backoff
            if self._running:
                logger.info(f"[STREAM] Reconnecting in {self._reconnect_delay}s...")
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(
                    self._reconnect_delay * 2,
                    self._max_reconnect_delay
                )

    async def start(self) -> None:
        """Start the streaming connection."""
        if self._running:
            return
        self._running = True
        asyncio.create_task(self._run_loop())
        logger.info("[STREAM] Streamer started")

    async def stop(self) -> None:
        """Stop the streaming connection."""
        self._running = False
        if self._ws:
            await self._ws.close()
            self._ws = None
        logger.info("[STREAM] Streamer stopped")

    def update_access_token(self, new_token: str) -> None:
        """Update access token (for when it's refreshed)."""
        self.access_token = new_token
        logger.debug("[STREAM] Access token updated")
