"""
Schwab Feed Adapter - Service 1 of the Morpheus Data Spine.

Owns the Schwab WebSocket connection exclusively.
Publishes:
  md.quotes.{sym}        - On every tick
  md.ohlc.1m.{sym}       - Completed 1-minute bars
  bot.positions           - When account activity arrives
  bot.telemetry.schwab_feed - Every 2s

Replaces the tight coupling between schwab_stream.py, candle_aggregator.py,
and the monolith server's on_quote callback.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

from morpheus.spine.nats_client import SpineClient
from morpheus.spine.config_store import ConfigStore
from morpheus.spine.schemas import (
    Topics,
    QuoteMsg,
    OHLCMsg,
    PositionMsg,
    TelemetryMsg,
)

# Reuse existing broker components
from morpheus.broker.schwab_auth import SchwabAuth
from morpheus.broker.schwab_stream import (
    SchwabStreamer,
    StreamerInfo,
    QuoteUpdate,
    AccountActivity,
)
from morpheus.data.candle_aggregator import CandleAggregator
from morpheus.features.indicators import OHLCV
from morpheus.core.market_mode import get_market_mode, is_rth

logger = logging.getLogger(__name__)


class SchwabFeedService:
    """
    Schwab Feed Adapter for the NATS spine.

    Lifecycle:
    1. Connect to NATS
    2. Authenticate with Schwab
    3. Open Schwab WebSocket
    4. Forward quotes → NATS, build bars → NATS
    5. Forward account activity → NATS positions
    """

    SERVICE_NAME = "schwab_feed"

    def __init__(self, config_store: ConfigStore | None = None):
        self._config_store = config_store or ConfigStore()
        self._spine = SpineClient(
            url=self._config_store.get().nats_url,
            name=self.SERVICE_NAME,
        )

        # Schwab components (initialized on start)
        self._auth: SchwabAuth | None = None
        self._streamer: SchwabStreamer | None = None

        # Candle aggregator: builds 1m bars from quote ticks
        self._aggregator = CandleAggregator(
            on_candle=self._on_candle_complete,
            interval_seconds=60,
        )

        # Symbol management
        self._subscribed_symbols: set[str] = set()

        # Telemetry
        self._start_time = 0.0
        self._quotes_forwarded = 0
        self._bars_published = 0
        self._position_updates = 0
        self._errors = 0

        # Control
        self._running = False
        self._telemetry_task: asyncio.Task | None = None
        self._discovery_sub = None

    async def start(self) -> None:
        """Start the Schwab feed service."""
        logger.info(f"[{self.SERVICE_NAME}] Starting...")
        self._start_time = time.time()
        self._running = True

        # 1. Connect to NATS
        await self._spine.connect()

        # 2. Subscribe to scanner discovery to know which symbols to stream
        await self._spine.subscribe_decode(
            Topics.SCANNER_DISCOVERY,
            self._on_discovery,
        )

        # 3. Authenticate with Schwab
        self._auth = SchwabAuth(
            client_id=os.getenv("SCHWAB_CLIENT_ID", ""),
            client_secret=os.getenv("SCHWAB_CLIENT_SECRET", ""),
            redirect_uri=os.getenv("SCHWAB_REDIRECT_URI", "HTTPS://127.0.0.1:6969"),
            token_path=os.getenv("SCHWAB_TOKEN_PATH", "./tokens/schwab_token.json"),
        )

        token = self._auth.get_valid_token()
        if not token:
            logger.error(f"[{self.SERVICE_NAME}] Failed to get Schwab token")
            return

        # 4. Get streamer info and create streamer
        streamer_info = await self._get_streamer_info(token)
        if not streamer_info:
            logger.error(f"[{self.SERVICE_NAME}] Failed to get streamer info")
            return

        self._streamer = SchwabStreamer(
            streamer_info=streamer_info,
            access_token=token["access_token"],
            on_quote=self._on_quote,
            on_account_activity=self._on_account_activity,
            on_error=self._on_stream_error,
        )

        # 5. Start streaming
        await self._streamer.start()

        # 6. Start telemetry loop
        self._telemetry_task = asyncio.create_task(self._telemetry_loop())

        logger.info(f"[{self.SERVICE_NAME}] Started")

    async def stop(self) -> None:
        """Stop the Schwab feed service."""
        logger.info(f"[{self.SERVICE_NAME}] Stopping...")
        self._running = False

        if self._telemetry_task:
            self._telemetry_task.cancel()
            try:
                await self._telemetry_task
            except asyncio.CancelledError:
                pass

        # Flush remaining candle bars
        await self._aggregator.flush()

        if self._streamer:
            await self._streamer.stop()

        await self._spine.close()
        logger.info(f"[{self.SERVICE_NAME}] Stopped")

    async def subscribe_symbol(self, symbol: str) -> None:
        """Subscribe to quotes for a symbol."""
        symbol = symbol.upper()
        if symbol in self._subscribed_symbols:
            return

        self._subscribed_symbols.add(symbol)
        if self._streamer:
            await self._streamer.subscribe_quotes([symbol])
            logger.info(f"[{self.SERVICE_NAME}] Subscribed to {symbol}")

    async def unsubscribe_symbol(self, symbol: str) -> None:
        """Unsubscribe from quotes for a symbol."""
        symbol = symbol.upper()
        if symbol not in self._subscribed_symbols:
            return

        self._subscribed_symbols.discard(symbol)
        if self._streamer:
            await self._streamer.unsubscribe_quotes([symbol])
            logger.info(f"[{self.SERVICE_NAME}] Unsubscribed from {symbol}")

    # -----------------------------------------------------------------------
    # Callbacks
    # -----------------------------------------------------------------------

    async def _on_quote(self, update: QuoteUpdate) -> None:
        """Handle incoming quote from Schwab streamer → publish to NATS."""
        try:
            msg = QuoteMsg(
                sym=update.symbol,
                bid=update.bid_price or 0.0,
                ask=update.ask_price or 0.0,
                last=update.last_price or 0.0,
                bid_sz=update.bid_size or 0,
                ask_sz=update.ask_size or 0,
                vol=update.volume or 0,
                high=update.high_price or 0.0,
                low=update.low_price or 0.0,
                open=update.open_price or 0.0,
                close=update.close_price or 0.0,
                net_chg=update.net_change or 0.0,
                mark=update.mark or 0.0,
            )

            # Publish quote to NATS
            await self._spine.publish(msg.topic(), msg.encode())
            self._quotes_forwarded += 1

            # Feed to candle aggregator for 1m bar building
            price = update.last_price or update.mark or 0.0
            if price > 0:
                await self._aggregator.on_quote(
                    symbol=update.symbol,
                    price=price,
                    volume=update.volume or 0,
                    timestamp=update.trade_time or datetime.now(timezone.utc),
                )

        except Exception as e:
            self._errors += 1
            logger.error(f"[{self.SERVICE_NAME}] Quote forward error: {e}")

    async def _on_candle_complete(
        self, symbol: str, ohlcv: OHLCV, timestamp: datetime
    ) -> None:
        """Completed 1-min bar → publish to NATS."""
        try:
            msg = OHLCMsg(
                sym=symbol,
                o=ohlcv.open,
                h=ohlcv.high,
                l=ohlcv.low,
                c=ohlcv.close,
                v=ohlcv.volume,
                ts=timestamp.timestamp(),
            )
            await self._spine.publish(msg.topic(), msg.encode())
            self._bars_published += 1

            logger.debug(
                f"[{self.SERVICE_NAME}] Bar: {symbol} "
                f"O={ohlcv.open:.2f} H={ohlcv.high:.2f} "
                f"L={ohlcv.low:.2f} C={ohlcv.close:.2f} V={ohlcv.volume}"
            )
        except Exception as e:
            self._errors += 1
            logger.error(f"[{self.SERVICE_NAME}] Bar publish error: {e}")

    async def _on_account_activity(self, activity: AccountActivity) -> None:
        """Account activity (orders/fills/positions) → publish to NATS."""
        try:
            # Publish raw activity as position update
            msg = PositionMsg(
                sym=activity.message_data.get("symbol", ""),
                qty=int(activity.message_data.get("quantity", 0)),
                avg_price=float(activity.message_data.get("avg_price", 0)),
                market_value=float(activity.message_data.get("market_value", 0)),
            )
            await self._spine.publish(Topics.POSITIONS, msg.encode())
            self._position_updates += 1

        except Exception as e:
            self._errors += 1
            logger.error(f"[{self.SERVICE_NAME}] Account activity error: {e}")

    async def _on_stream_error(self, error: Exception) -> None:
        """Handle streamer errors."""
        self._errors += 1
        logger.error(f"[{self.SERVICE_NAME}] Stream error: {error}")

        # Refresh token if auth error
        if "401" in str(error) or "unauthorized" in str(error).lower():
            if self._auth:
                try:
                    token = self._auth.refresh_token()
                    if token and self._streamer:
                        self._streamer.update_access_token(token["access_token"])
                        logger.info(f"[{self.SERVICE_NAME}] Token refreshed")
                except Exception as e:
                    logger.error(f"[{self.SERVICE_NAME}] Token refresh failed: {e}")

    async def _on_discovery(self, subject: str, data: dict) -> None:
        """Handle scanner discovery → subscribe to new symbol."""
        sym = data.get("sym", "")
        if sym:
            await self.subscribe_symbol(sym)

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    async def _get_streamer_info(self, token: dict) -> StreamerInfo | None:
        """Get Schwab streamer connection info."""
        try:
            import httpx

            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    "https://api.schwabapi.com/trader/v1/userPreference",
                    headers={"Authorization": f"Bearer {token['access_token']}"},
                )
                if resp.status_code != 200:
                    logger.error(
                        f"[{self.SERVICE_NAME}] userPreference failed: {resp.status_code}"
                    )
                    return None

                prefs = resp.json()
                streamer = prefs.get("streamerInfo", [{}])[0]

                return StreamerInfo(
                    socket_url=streamer.get("streamerSocketUrl", ""),
                    customer_id=streamer.get("schwabClientCustomerId", ""),
                    correl_id=streamer.get("schwabClientCorrelId", ""),
                    channel=streamer.get("schwabClientChannel", ""),
                    function_id=streamer.get("schwabClientFunctionId", ""),
                )
        except Exception as e:
            logger.error(f"[{self.SERVICE_NAME}] Failed to get streamer info: {e}")
            return None

    async def _telemetry_loop(self) -> None:
        """Publish telemetry every 2 seconds."""
        while self._running:
            try:
                msg = TelemetryMsg(
                    service=self.SERVICE_NAME,
                    status="running" if self._spine.is_connected else "degraded",
                    uptime_s=time.time() - self._start_time,
                    msgs_out=self._quotes_forwarded + self._bars_published,
                    errors=self._errors,
                    extra={
                        "quotes_forwarded": self._quotes_forwarded,
                        "bars_published": self._bars_published,
                        "position_updates": self._position_updates,
                        "subscribed_symbols": len(self._subscribed_symbols),
                        "aggregator_stats": self._aggregator.get_stats(),
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
    """Run Schwab Feed as a standalone service."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    service = SchwabFeedService()
    try:
        await service.start()

        # Run until interrupted
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        await service.stop()


if __name__ == "__main__":
    asyncio.run(main())
