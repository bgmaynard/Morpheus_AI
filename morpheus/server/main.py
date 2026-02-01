"""
Morpheus Server - FastAPI server for UI communication.

Provides:
- REST API for commands (/api/ui/command)
- State endpoint (/api/ui/state)
- WebSocket event stream (/ws/events)
- Health check (/health)

SAFETY DEFAULTS:
- PAPER mode ON
- LIVE_TRADING_ARMED = FALSE
- Kill switch = SAFE
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from morpheus.core.events import Event, EventType, create_event
from morpheus.execution.base import TradingMode, BlockReason
from morpheus.execution.guards import (
    ConfirmationGuard,
    ConfirmationRequest,
    create_confirmation_guard,
)

# Market data imports (optional - graceful if not configured)
try:
    from morpheus.broker.schwab_auth import SchwabAuth, TokenNotFoundError
    from morpheus.broker.schwab_market import SchwabMarketClient, SchwabMarketError
    from morpheus.broker.schwab_trading import SchwabTradingClient, SchwabTradingError
    from morpheus.broker.schwab_stream import SchwabStreamer, StreamerInfo, QuoteUpdate, AccountActivity
    SCHWAB_AVAILABLE = True
except ImportError:
    SCHWAB_AVAILABLE = False

# Orchestrator imports (optional - graceful if not fully configured)
try:
    from morpheus.orchestrator.pipeline import SignalPipeline, PipelineConfig
    from morpheus.features.indicators import OHLCV
    from morpheus.data.candle_aggregator import CandleAggregator
    ORCHESTRATOR_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Orchestrator not available: {e}")
    ORCHESTRATOR_AVAILABLE = False

# Persistence imports (optional - graceful if database not available)
try:
    from morpheus.persistence.database import Database
    from morpheus.persistence.event_listener import PersistenceListener
    PERSISTENCE_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Persistence layer not available: {e}")
    PERSISTENCE_AVAILABLE = False

# Scanner imports (optional)
try:
    from morpheus.scanner.scanner import Scanner, ScannerConfig, ScanResult
    from morpheus.scanner.watchlist import Watchlist, WatchlistConfig, WatchlistState
    SCANNER_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Scanner not available: {e}")
    SCANNER_AVAILABLE = False

# MAX_AI Scanner integration (discovery source)
try:
    from morpheus.scanner.max_ai import MaxAIScannerClient
    from morpheus.integrations.max_ai_scanner import ScannerIntegration, ScannerIntegrationConfig
    MAX_AI_SCANNER_AVAILABLE = True
except ImportError as e:
    logger.warning(f"MAX_AI Scanner client not available: {e}")
    MAX_AI_SCANNER_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("morpheus.server")


# ============================================================================
# Pydantic Models for API
# ============================================================================


class Command(BaseModel):
    """Command envelope from UI."""
    command_id: str
    command_type: str
    payload: dict[str, Any]
    timestamp: str


class CommandResult(BaseModel):
    """Command result to UI."""
    accepted: bool
    command_id: str
    message: str = ""


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    trading_mode: str
    live_armed: bool
    kill_switch_active: bool
    uptime_seconds: float
    event_count: int
    websocket_clients: int
    pipeline_enabled: bool = False


# ============================================================================
# Server State
# ============================================================================


class MorpheusState:
    """
    Server state - SAFETY DEFAULTS enforced.

    CRITICAL: All state starts in SAFE mode:
    - PAPER trading only
    - Not armed for live
    - Kill switch safe
    """

    def __init__(self):
        # SAFETY DEFAULTS
        self.trading_mode: TradingMode = TradingMode.PAPER
        self.live_armed: bool = False
        self.kill_switch_active: bool = False

        # Active signal tracking (per symbol)
        self.active_signals: dict[str, dict[str, Any]] = {}

        # Active symbol (from UI chain)
        self.active_symbol: str | None = None

        # Event tracking
        self.event_count: int = 0
        self.start_time: datetime = datetime.now(timezone.utc)

        # Pending orders (for paper mode simulation)
        self.pending_orders: dict[str, dict[str, Any]] = {}

        # Profile settings
        self.profile = {
            "gate": "standard",
            "risk": "standard",
            "guard": "standard",
        }

        # Subscribed symbols for live streaming
        self.subscribed_symbols: set[str] = set()

        # Pipeline event history for monitoring (circular buffer)
        self.pipeline_events: list[dict[str, Any]] = []
        self.max_pipeline_events: int = 200

        logger.info("MorpheusState initialized with SAFE defaults")
        logger.info(f"  Trading Mode: {self.trading_mode.value}")
        logger.info(f"  Live Armed: {self.live_armed}")
        logger.info(f"  Kill Switch: {'ACTIVE' if self.kill_switch_active else 'SAFE'}")

    def to_dict(self) -> dict[str, Any]:
        """Export state for UI initialization."""
        return {
            "trading": {
                "mode": self.trading_mode.value.upper(),
                "liveArmed": self.live_armed,
                "killSwitchActive": self.kill_switch_active,
            },
            "profile": self.profile,
            "orders": {},
            "positions": {},
            "executions": [],
        }


class WebSocketManager:
    """Manages WebSocket connections for event broadcasting."""

    def __init__(self):
        self.active_connections: list[WebSocket] = []
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        async with self._lock:
            self.active_connections.append(websocket)
        logger.info(f"WebSocket client connected. Total: {len(self.active_connections)}")

    async def disconnect(self, websocket: WebSocket):
        async with self._lock:
            if websocket in self.active_connections:
                self.active_connections.remove(websocket)
        logger.info(f"WebSocket client disconnected. Total: {len(self.active_connections)}")

    async def broadcast(self, event: Event):
        """Broadcast event to all connected clients."""
        if not self.active_connections:
            return

        message = json.dumps(event.to_jsonl_dict())
        disconnected = []

        async with self._lock:
            for connection in self.active_connections:
                try:
                    await connection.send_text(message)
                except Exception:
                    disconnected.append(connection)

        # Clean up disconnected clients
        for conn in disconnected:
            await self.disconnect(conn)

    @property
    def client_count(self) -> int:
        return len(self.active_connections)


class MorpheusServer:
    """
    Main Morpheus server class.

    Coordinates:
    - State management
    - WebSocket broadcasting
    - Command processing
    - Event emission
    - Market data (Schwab)
    """

    def __init__(self):
        self.state = MorpheusState()
        self.ws_manager = WebSocketManager()
        self.confirmation_guard = create_confirmation_guard()
        self._heartbeat_task: asyncio.Task | None = None
        self._market_data_task: asyncio.Task | None = None
        self._trading_data_task: asyncio.Task | None = None

        # Market data client (initialized lazily)
        self._market_client: Any = None
        self._trading_client: Any = None
        self._http_client: Any = None
        self._market_data_available = False
        self._trading_available = False

        # Streaming client
        self._streamer: SchwabStreamer | None = None
        self._streamer_available = False
        self._use_streaming = True  # Use streaming instead of polling when available
        self._auth: Any = None  # Keep reference for streamer token refresh

        # Signal pipeline (orchestrator)
        self._pipeline: SignalPipeline | None = None
        self._pipeline_available = False
        self._candle_aggregator: CandleAggregator | None = None

        # Persistence layer
        self._db: Database | None = None
        self._persistence: PersistenceListener | None = None
        self._persistence_available = False

        # Scanner and watchlist
        self._scanner: Scanner | None = None
        self._watchlist: Watchlist | None = None
        self._scanner_available = False

        self._init_market_data()
        self._init_pipeline()
        self._init_persistence()
        self._init_scanner()

    def _init_market_data(self):
        """Initialize Schwab market data client if credentials are available."""
        if not SCHWAB_AVAILABLE:
            logger.warning("Schwab modules not available - market data disabled")
            return

        # Check for credentials
        client_id = os.environ.get("SCHWAB_CLIENT_ID")
        client_secret = os.environ.get("SCHWAB_CLIENT_SECRET")
        redirect_uri = os.environ.get("SCHWAB_REDIRECT_URI", "https://127.0.0.1:6969")
        token_path = os.environ.get("SCHWAB_TOKEN_PATH", "./tokens/schwab_token.json")

        if not client_id or not client_secret:
            logger.warning("Schwab credentials not configured - market data disabled")
            logger.info("Set SCHWAB_CLIENT_ID and SCHWAB_CLIENT_SECRET to enable")
            return

        try:
            # Create HTTP client
            self._http_client = httpx.Client(timeout=30.0)

            # Initialize auth
            auth = SchwabAuth(
                client_id=client_id,
                client_secret=client_secret,
                redirect_uri=redirect_uri,
                token_path=Path(token_path),
            )

            # Check if we have a token (even if expired)
            try:
                token = auth.get_token()
            except TokenNotFoundError:
                logger.warning("Schwab token not found - market data disabled")
                logger.info(f"Complete OAuth flow and save token to: {token_path}")
                return

            # If token is expired, try to refresh it
            if token.is_expired:
                logger.info("Schwab access token expired, attempting refresh...")
                try:
                    auth.refresh_token(self._http_client)
                    logger.info("Schwab token refreshed successfully")
                except Exception as e:
                    logger.error(f"Failed to refresh Schwab token: {e}")
                    logger.info("Re-authenticate using /api/auth/schwab/url endpoint")
                    return

            # Create market client
            self._market_client = SchwabMarketClient(auth=auth, http_client=self._http_client)
            self._market_data_available = True
            self._auth = auth  # Keep reference for streamer
            logger.info("Schwab market data client initialized successfully")

            # Create trading client (uses same auth)
            try:
                self._trading_client = SchwabTradingClient(auth=auth, http_client=self._http_client)
                self._trading_available = True
                logger.info("Schwab trading client initialized successfully")
            except Exception as e:
                logger.warning(f"Failed to initialize trading client: {e}")
                self._trading_available = False

            # Initialize streaming client
            self._init_streamer()

        except Exception as e:
            logger.error(f"Failed to initialize market data client: {e}")
            self._market_data_available = False

    def _init_streamer(self):
        """Initialize Schwab streaming client for real-time data."""
        if not self._auth or not self._http_client:
            logger.warning("Cannot initialize streamer - auth not available")
            return

        try:
            # Get user preferences with streamer info
            token = self._auth.get_token()
            headers = {"Authorization": f"Bearer {token.access_token}", "Accept": "application/json"}
            resp = self._http_client.get(
                "https://api.schwabapi.com/trader/v1/userPreference",
                headers=headers,
            )

            if resp.status_code != 200:
                logger.error(f"Failed to get streamer info: {resp.status_code}")
                return

            data = resp.json()
            streamer_info_list = data.get("streamerInfo", [])
            if not streamer_info_list:
                logger.error("No streamer info in user preferences")
                return

            info = streamer_info_list[0]
            streamer_info = StreamerInfo(
                socket_url=info.get("streamerSocketUrl", ""),
                customer_id=info.get("schwabClientCustomerId", ""),
                correl_id=info.get("schwabClientCorrelId", ""),
                channel=info.get("schwabClientChannel", ""),
                function_id=info.get("schwabClientFunctionId", ""),
            )

            # Create streamer with callbacks
            self._streamer = SchwabStreamer(
                streamer_info=streamer_info,
                access_token=token.access_token,
                on_quote=self._handle_streaming_quote,
                on_account_activity=self._handle_streaming_account_activity,
                on_error=self._handle_streaming_error,
            )
            self._streamer_available = True
            logger.info("Schwab streaming client initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize streaming client: {e}")
            self._streamer_available = False

    def _init_pipeline(self):
        """Initialize the signal pipeline (orchestrator)."""
        if not ORCHESTRATOR_AVAILABLE:
            logger.warning("Orchestrator module not available - pipeline disabled")
            return

        try:
            # Create pipeline config (permissive for paper trading)
            config = PipelineConfig(
                permissive_mode=True,  # Allow more signals for testing
                min_signal_confidence=0.0,  # Emit all signals
                respect_kill_switch=True,
            )

            # Create pipeline with emit callback
            self._pipeline = SignalPipeline(
                config=config,
                emit_event=self.emit_event,  # Wire to server's event emission
            )
            self._pipeline_available = True
            logger.info("Signal pipeline initialized successfully (OBSERVATIONAL MODE)")
            logger.info(f"  - Permissive mode: {config.permissive_mode}")
            logger.info(f"  - Kill switch respect: {config.respect_kill_switch}")

            # Create candle aggregator to build 1-min candles from streaming quotes
            self._candle_aggregator = CandleAggregator(
                on_candle=self._handle_aggregated_candle,
                interval_seconds=60,  # 1-minute candles
            )
            logger.info("Candle aggregator initialized (1-min bars from streaming quotes)")

        except Exception as e:
            logger.error(f"Failed to initialize signal pipeline: {e}")
            self._pipeline_available = False

    def _init_persistence(self):
        """Initialize persistence layer for logging all events."""
        if not PERSISTENCE_AVAILABLE:
            logger.warning("Persistence module not available - event logging disabled")
            return

        try:
            # Initialize database (creates ./data/morpheus.db)
            self._db = Database()
            self._persistence = PersistenceListener(self._db)
            self._persistence_available = True
            logger.info("Persistence layer initialized (SQLite)")
            logger.info(f"  - Database: {self._db.db_path}")

        except Exception as e:
            logger.error(f"Failed to initialize persistence: {e}")
            self._persistence_available = False

    def _init_scanner(self):
        """Initialize the scanner for auto-discovery of candidates."""
        if not SCANNER_AVAILABLE:
            logger.warning("Scanner module not available")
            return

        # Scanner can run with MAX_AI_SCANNER even without Schwab market data
        if not self._market_data_available and not MAX_AI_SCANNER_AVAILABLE:
            logger.warning("Scanner requires market data or MAX_AI_SCANNER - disabled")
            return

        try:
            # Create scanner config (small-cap momentum focus)
            scanner_config = ScannerConfig(
                min_price=1.00,
                max_price=20.00,
                min_rvol=2.0,
                min_change_pct=5.0,
                max_spread_pct=2.0,
                scan_interval_seconds=60.0,
                max_results=15,
            )

            # Create watchlist config
            watchlist_config = WatchlistConfig(
                new_to_active_delay_seconds=30.0,
                stale_timeout_seconds=300.0,
                max_watchlist_size=20,
            )

            # Initialize MAX_AI Scanner client (SINGLE SOURCE OF TRUTH)
            # Per integration spec: Morpheus_AI consumes discovery from MAX_AI_SCANNER only
            self._max_ai_client: MaxAIScannerClient | None = None
            if MAX_AI_SCANNER_AVAILABLE:
                self._max_ai_client = MaxAIScannerClient(
                    base_url="http://127.0.0.1:8787",
                    timeout=5.0,
                )
                logger.info("MAX_AI Scanner client initialized")

            # Create fetch function - uses MAX_AI_SCANNER as ONLY source
            # Per integration spec: NO FALLBACK - if scanner down, return empty
            async def fetch_movers():
                # ONLY use MAX_AI_SCANNER for discovery
                # NO FALLBACK to Schwab movers - scanner is single source of truth
                if self._max_ai_client:
                    movers = await self._max_ai_client.fetch_movers(
                        profile="FAST_MOVERS",
                        limit=50,
                    )
                    if movers:
                        logger.debug(f"[MAX_AI] Got {len(movers)} movers from scanner service")
                        return movers
                    else:
                        logger.warning("[MAX_AI] Scanner returned empty - Morpheus entering IDLE")
                        return []

                # No MAX_AI client = no discovery
                logger.warning("[MAX_AI] Scanner client not available - no discovery possible")
                return []

            # Create scanner with callback
            self._scanner = Scanner(
                config=scanner_config,
                fetch_movers=fetch_movers,
                on_candidate=self._handle_scanner_candidate,
            )

            # Create watchlist
            self._watchlist = Watchlist(
                config=watchlist_config,
                on_activated=self._handle_watchlist_activated,
            )

            self._scanner_available = True
            logger.info("Scanner initialized (auto-discovery enabled)")
            if self._max_ai_client:
                logger.info("  - Discovery source: MAX_AI_SCANNER (http://127.0.0.1:8787)")
            else:
                logger.info("  - Discovery source: Schwab Movers API (fallback)")
            logger.info(f"  - Price range: ${scanner_config.min_price}-${scanner_config.max_price}")
            logger.info(f"  - Min RVOL: {scanner_config.min_rvol}x")
            logger.info(f"  - Scan interval: {scanner_config.scan_interval_seconds}s")

            # Initialize Scanner Integration (event mapping, halt tracking)
            self._scanner_integration = None
            if MAX_AI_SCANNER_AVAILABLE:
                integration_config = ScannerIntegrationConfig(
                    symbol_poll_interval=30.0,
                    halt_poll_interval=10.0,
                    profiles=["FAST_MOVERS", "GAPPERS", "TOP_GAINERS"],
                    max_symbols=25,
                    auto_subscribe=True,
                    emit_discovery_events=True,
                )
                self._scanner_integration = ScannerIntegration(
                    config=integration_config,
                    on_event=self._handle_scanner_event,
                    on_symbol_discovered=self._handle_scanner_symbol_discovered,
                    on_symbol_removed=self._handle_scanner_symbol_removed,
                )
                logger.info("Scanner Integration initialized (events + halts)")

                # Wire scanner context to pipeline for external augmentation
                # This allows FeatureContext to include scanner_score, gap_pct, etc.
                if self._pipeline_available and self._pipeline:
                    self._pipeline.set_external_context_provider(
                        self._scanner_integration.get_symbol_context
                    )
                    logger.info("Pipeline wired to Scanner Integration (context augmentation enabled)")

        except Exception as e:
            logger.error(f"Failed to initialize scanner: {e}")
            self._scanner_available = False

    async def _handle_scanner_candidate(self, candidate: ScanResult) -> None:
        """Handle a new candidate from the scanner."""
        try:
            symbol = candidate.symbol

            # Add to watchlist
            if self._watchlist:
                entry = await self._watchlist.add(
                    symbol=symbol,
                    reason=", ".join(candidate.reasons),
                    score=candidate.score,
                    initial_price=candidate.price,
                    initial_rvol=candidate.rvol,
                    initial_change_pct=candidate.change_pct,
                )

                if entry:
                    logger.info(
                        f"[SCANNER] Candidate added: {symbol} "
                        f"${candidate.price:.2f} {candidate.change_pct:+.1f}% "
                        f"RVOL={candidate.rvol:.1f}x Score={candidate.score:.0f}"
                    )

            # Emit event for UI
            await self.emit_event(create_event(
                EventType.REGIME_DETECTED,  # Reuse for now, or add SCANNER_CANDIDATE
                symbol=symbol,
                payload={
                    "type": "scanner_candidate",
                    "symbol": symbol,
                    "price": candidate.price,
                    "change_pct": candidate.change_pct,
                    "rvol": candidate.rvol,
                    "score": candidate.score,
                    "reasons": candidate.reasons,
                    "scan_type": candidate.scan_type.value,
                },
            ))

        except Exception as e:
            logger.error(f"Error handling scanner candidate: {e}")

    async def _handle_watchlist_activated(self, entry) -> None:
        """Handle a watchlist entry becoming ACTIVE - subscribe for data."""
        try:
            symbol = entry.symbol

            # Subscribe to streaming quotes
            self.state.subscribed_symbols.add(symbol)
            if self._streamer_available and self._use_streaming and self._streamer:
                await self._streamer.subscribe_quotes([symbol])

            # Add to signal pipeline
            if self._pipeline_available and self._pipeline:
                self._pipeline.add_symbol(symbol)
                # Warmup with historical data
                asyncio.create_task(self._warmup_pipeline_symbol(symbol))

            logger.info(f"[WATCHLIST] {symbol} activated - subscribed for live data")

        except Exception as e:
            logger.error(f"Error activating watchlist symbol: {e}")

    async def _handle_scanner_event(self, event: Event) -> None:
        """
        Handle events from Scanner Integration.

        These are mapped scanner events (MARKET_HALT, MARKET_RESUME, SCANNER_*).
        They are emitted to the Morpheus event stream for UI and strategies.
        """
        try:
            # Emit to WebSocket clients
            await self.emit_event(event)
            logger.info(f"[SCANNER_EVENT] {event.event_type.value}: {event.symbol}")
        except Exception as e:
            logger.error(f"Error handling scanner event: {e}")

    async def _handle_scanner_symbol_discovered(self, symbol: str) -> None:
        """
        Handle new symbol discovered by Scanner Integration.

        This is the ONLY path for symbols to enter the pipeline.
        Scanner is the eyes - Morpheus is the brain.
        """
        try:
            logger.info(f"[SCANNER] Symbol discovered: {symbol}")

            # Add to subscribed symbols
            self.state.subscribed_symbols.add(symbol)

            # Subscribe to streaming quotes
            if self._streamer_available and self._use_streaming and self._streamer:
                await self._streamer.subscribe_quotes([symbol])
                logger.info(f"[SCANNER] Subscribed to streaming: {symbol}")

            # Add to signal pipeline
            if self._pipeline_available and self._pipeline:
                self._pipeline.add_symbol(symbol)
                # Warmup with historical data
                asyncio.create_task(self._warmup_pipeline_symbol(symbol))
                logger.info(f"[SCANNER] Added to pipeline: {symbol}")

        except Exception as e:
            logger.error(f"Error handling discovered symbol {symbol}: {e}")

    async def _handle_scanner_symbol_removed(self, symbol: str) -> None:
        """Handle symbol removed from scanner universe."""
        try:
            logger.info(f"[SCANNER] Symbol removed from scanner: {symbol}")
            # Note: We don't immediately remove from pipeline/streaming
            # Let the symbol naturally expire from watchlist if not active
        except Exception as e:
            logger.error(f"Error handling removed symbol {symbol}: {e}")

    async def _handle_aggregated_candle(self, symbol: str, ohlcv: OHLCV, timestamp: datetime) -> None:
        """Handle completed 1-min candle from aggregator."""
        if not self._pipeline_available or not self._pipeline:
            return

        try:
            # Feed the aggregated candle to the pipeline
            await self._pipeline.on_candle(symbol, ohlcv, timestamp=timestamp)
            logger.info(
                f"[CANDLE] {symbol} {timestamp.strftime('%H:%M')} "
                f"O={ohlcv.open:.2f} H={ohlcv.high:.2f} L={ohlcv.low:.2f} C={ohlcv.close:.2f} V={ohlcv.volume}"
            )
        except Exception as e:
            logger.error(f"Error feeding aggregated candle to pipeline: {e}")

    async def _handle_streaming_quote(self, quote: QuoteUpdate) -> None:
        """Handle incoming quote from streaming."""
        try:
            # Get the best available price - DO NOT use mark (it's unreliable garbage)
            # Priority: last_price > bid/ask midpoint > bid_price
            price = 0.0
            if quote.last_price and quote.last_price > 0:
                price = quote.last_price
            elif quote.bid_price and quote.ask_price and quote.bid_price > 0 and quote.ask_price > 0:
                price = (quote.bid_price + quote.ask_price) / 2
            elif quote.bid_price and quote.bid_price > 0:
                price = quote.bid_price

            # Only emit if we have meaningful price data
            if price <= 0:
                logger.debug(f"[STREAM] Skipping quote for {quote.symbol} - no price data")
                return

            # Emit QUOTE_UPDATE event
            await self.emit_event(create_event(
                EventType.QUOTE_UPDATE,
                symbol=quote.symbol,
                payload={
                    "symbol": quote.symbol,
                    "bid": quote.bid_price,
                    "ask": quote.ask_price,
                    "last": price,
                    "volume": quote.volume,
                    "high": quote.high_price,
                    "low": quote.low_price,
                    "open": quote.open_price,
                    "close": quote.close_price,
                    "net_change": quote.net_change,
                    "timestamp": quote.trade_time.isoformat() if quote.trade_time else datetime.now(timezone.utc).isoformat(),
                    "source": "SCHWAB_STREAM",
                },
            ))
            logger.debug(f"[STREAM] QUOTE {quote.symbol} ${price:.2f}")

            # Feed quote to candle aggregator (builds 1-min candles)
            if self._candle_aggregator:
                await self._candle_aggregator.on_quote(
                    symbol=quote.symbol,
                    price=price,
                    volume=quote.volume or 0,
                    timestamp=quote.trade_time,
                )

            # Also feed quote directly to pipeline for real-time evaluation
            if self._pipeline_available and self._pipeline:
                await self._pipeline.on_quote(
                    symbol=quote.symbol,
                    last=price,
                    bid=quote.bid_price,
                    ask=quote.ask_price,
                    volume=quote.volume,
                    timestamp=quote.trade_time,
                )
        except Exception as e:
            logger.error(f"Error handling streaming quote: {e}")

    async def _handle_streaming_account_activity(self, activity: AccountActivity) -> None:
        """Handle incoming account activity from streaming."""
        try:
            logger.info(f"[STREAM] ACCOUNT_ACTIVITY: {activity.message_type}")

            # Emit appropriate events based on activity type
            # Account activity can include order fills, position changes, etc.
            if activity.message_type:
                await self.emit_event(create_event(
                    EventType.POSITION_UPDATE,
                    payload={
                        "account": activity.account,
                        "message_type": activity.message_type,
                        "data": activity.message_data,
                        "source": "SCHWAB_STREAM",
                    },
                ))
        except Exception as e:
            logger.error(f"Error handling streaming account activity: {e}")

    async def _handle_streaming_error(self, error: Exception) -> None:
        """Handle streaming errors."""
        logger.error(f"[STREAM] Error: {error}")

    def get_quote(self, symbol: str) -> dict[str, Any] | None:
        """Get current quote for a symbol."""
        if not self._market_data_available or not self._market_client:
            return None

        try:
            quote = self._market_client.get_quote(symbol.upper())
            return {
                "symbol": quote.symbol,
                "bid": quote.bid_price,
                "ask": quote.ask_price,
                "last": quote.last_price,
                "volume": quote.total_volume,
                "timestamp": quote.quote_time.isoformat(),
                "is_tradeable": quote.is_tradeable,
                "is_market_open": quote.is_market_open,
                "close": quote.close_price,
                "change": quote.net_change,
                "change_percent": quote.net_change_percent,
            }
        except Exception as e:
            logger.error(f"Failed to get quote for {symbol}: {e}")
            return None

    def get_candles(
        self,
        symbol: str,
        period_type: str = "day",
        period: int = 1,
        frequency: int = 1,
        frequency_type: str = "minute",
    ) -> list[dict[str, Any]]:
        """Get historical candles for a symbol.

        For intraday data (frequency_type=minute), uses explicit date range
        to get TODAY's data rather than previous day's data.
        """
        if not self._market_data_available or not self._market_client:
            return []

        try:
            # For intraday minute data, use explicit date range to get today's candles
            start_date = None
            end_date = None

            if frequency_type == "minute" and period_type == "day":
                from datetime import datetime, timezone, timedelta
                import pytz

                # Get current time in Eastern timezone (market timezone)
                eastern = pytz.timezone('US/Eastern')
                now_eastern = datetime.now(eastern)

                # Market opens at 9:30 AM ET, but we want pre-market too
                # Start from 4:00 AM ET (pre-market start)
                today_start = now_eastern.replace(hour=4, minute=0, second=0, microsecond=0)

                # If it's before 4 AM, look at previous day
                if now_eastern.hour < 4:
                    today_start = today_start - timedelta(days=1)

                start_date = today_start.astimezone(timezone.utc)
                end_date = datetime.now(timezone.utc)

                logger.info(f"[CANDLES] Fetching intraday for {symbol}: {start_date} to {end_date}")

            candles = self._market_client.get_candles(
                symbol=symbol.upper(),
                period_type=period_type,
                period=period,
                frequency_type=frequency_type,
                frequency=frequency,
                start_date=start_date,
                end_date=end_date,
            )
            return [
                {
                    "time": int(c.timestamp.timestamp()),
                    "open": c.open,
                    "high": c.high,
                    "low": c.low,
                    "close": c.close,
                    "volume": c.volume,
                }
                for c in candles
            ]
        except Exception as e:
            logger.error(f"Failed to get candles for {symbol}: {e}")
            return []

    def get_positions(self) -> list[dict[str, Any]]:
        """Get current positions from Schwab account."""
        if not self._trading_available or not self._trading_client:
            return []

        try:
            positions = self._trading_client.get_positions()
            return [
                {
                    "symbol": p.symbol,
                    "quantity": p.quantity,
                    "avg_price": p.avg_price,
                    "current_price": p.current_price,
                    "market_value": p.market_value,
                    "unrealized_pnl": p.unrealized_pnl,
                    "unrealized_pnl_pct": p.unrealized_pnl_pct,
                    "asset_type": p.asset_type,
                }
                for p in positions
            ]
        except Exception as e:
            logger.error(f"Failed to get positions: {e}")
            return []

    def get_orders(self, status: str | None = None) -> list[dict[str, Any]]:
        """Get orders from Schwab account."""
        if not self._trading_available or not self._trading_client:
            return []

        try:
            orders = self._trading_client.get_orders(status=status)
            return [
                {
                    "order_id": o.order_id,
                    "symbol": o.symbol,
                    "side": o.side.lower(),  # normalize to 'buy'/'sell'
                    "quantity": o.quantity,
                    "filled_quantity": o.filled_quantity,
                    "order_type": o.order_type,
                    "limit_price": o.limit_price,
                    "stop_price": o.stop_price,
                    "status": o.status,
                    "entered_time": o.entered_time.isoformat(),
                    "close_time": o.close_time.isoformat() if o.close_time else None,
                }
                for o in orders
            ]
        except Exception as e:
            logger.error(f"Failed to get orders: {e}")
            return []

    def get_transactions(self) -> list[dict[str, Any]]:
        """Get today's transactions/executions from Schwab account."""
        if not self._trading_available or not self._trading_client:
            return []

        try:
            transactions = self._trading_client.get_transactions()
            return [
                {
                    "transaction_id": t.transaction_id,
                    "order_id": t.order_id,
                    "symbol": t.symbol,
                    "side": t.side.lower(),
                    "quantity": t.quantity,
                    "price": t.price,
                    "timestamp": t.timestamp.isoformat(),
                    "transaction_type": t.transaction_type,
                    "description": t.description,
                }
                for t in transactions
            ]
        except Exception as e:
            logger.error(f"Failed to get transactions: {e}")
            return []

    async def _trading_data_loop(self):
        """Background loop to periodically fetch and broadcast trading data."""
        logger.info("Trading data loop starting...")

        while True:
            try:
                await asyncio.sleep(5.0)  # Fetch every 5 seconds

                if not self._trading_available:
                    continue

                # Fetch positions and emit events
                positions = self.get_positions()
                for pos in positions:
                    await self.emit_event(create_event(
                        EventType.POSITION_UPDATE,
                        symbol=pos["symbol"],
                        payload=pos,
                    ))

                # Log summary
                if positions:
                    logger.debug(f"[TRADING] Updated {len(positions)} positions")

            except asyncio.CancelledError:
                logger.info("Trading data loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in trading data loop: {e}")
                await asyncio.sleep(5.0)

    async def start(self):
        """Start background tasks."""
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._trading_data_task = asyncio.create_task(self._trading_data_loop())

        # Start streamer if available, otherwise fall back to polling
        if self._streamer_available and self._use_streaming and self._streamer:
            await self._streamer.start()
            logger.info("Schwab WebSocket streaming started (replacing polling)")
        else:
            self._market_data_task = asyncio.create_task(self._market_data_loop())
            logger.info("Market data polling loop started (streaming not available)")

        # Start signal pipeline (orchestrator)
        if self._pipeline_available and self._pipeline:
            await self._pipeline.start()
            logger.info("Signal pipeline started (OBSERVATIONAL MODE - no auto-execution)")

        # Start scanner and watchlist
        if self._scanner_available:
            if self._watchlist:
                await self._watchlist.start()
            if self._scanner:
                await self._scanner.start()
            # Start scanner integration (event mapping, halt tracking)
            if hasattr(self, '_scanner_integration') and self._scanner_integration:
                await self._scanner_integration.start()
                logger.info("Scanner Integration started (events + halts)")
            logger.info("Scanner started (auto-discovery enabled)")

        # Emit system start event
        await self.emit_event(create_event(
            EventType.SYSTEM_START,
            payload={
                "trading_mode": self.state.trading_mode.value.upper(),
                "live_armed": self.state.live_armed,
                "kill_switch_active": self.state.kill_switch_active,
                "streaming_enabled": self._streamer_available and self._use_streaming,
                "pipeline_enabled": self._pipeline_available,
            },
        ))

        logger.info("MorpheusServer started")
        logger.info("Trading data loop started")

    async def stop(self):
        """Stop background tasks."""
        # Stop scanner integration
        if hasattr(self, '_scanner_integration') and self._scanner_integration:
            await self._scanner_integration.stop()

        # Stop scanner and watchlist
        if self._scanner:
            await self._scanner.stop()
        if self._watchlist:
            await self._watchlist.stop()

        # Close MAX_AI Scanner client
        if hasattr(self, '_max_ai_client') and self._max_ai_client:
            await self._max_ai_client.close()
            logger.info("MAX_AI Scanner client closed")

        # Stop signal pipeline
        if self._pipeline_available and self._pipeline:
            await self._pipeline.stop()

        # Stop streamer if running
        if self._streamer:
            await self._streamer.stop()

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        if self._market_data_task:
            self._market_data_task.cancel()
            try:
                await self._market_data_task
            except asyncio.CancelledError:
                pass

        if self._trading_data_task:
            self._trading_data_task.cancel()
            try:
                await self._trading_data_task
            except asyncio.CancelledError:
                pass

        # Emit system stop event
        await self.emit_event(create_event(
            EventType.SYSTEM_STOP,
            payload={"reason": "shutdown"},
        ))

        logger.info("MorpheusServer stopped")

    async def _heartbeat_loop(self):
        """Send periodic heartbeats."""
        while True:
            try:
                await asyncio.sleep(5.0)  # 5 second heartbeat
                await self.emit_event(create_event(
                    EventType.HEARTBEAT,
                    payload={
                        "uptime_seconds": (datetime.now(timezone.utc) - self.state.start_time).total_seconds(),
                        "event_count": self.state.event_count,
                        "websocket_clients": self.ws_manager.client_count,
                    },
                ))
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")

    async def _market_data_loop(self):
        """
        Stream market data for subscribed symbols.

        Fetches quotes every 1 second for all subscribed symbols
        and emits QUOTE_UPDATE events via WebSocket.
        """
        logger.info("Market data loop starting...")

        while True:
            try:
                await asyncio.sleep(1.0)  # 1 second quote interval

                # Skip if no market data client or no subscriptions
                if not self._market_data_available or not self.state.subscribed_symbols:
                    continue

                # Fetch quotes for all subscribed symbols
                for symbol in list(self.state.subscribed_symbols):
                    try:
                        quote = self.get_quote(symbol)
                        if quote:
                            # Emit QUOTE_UPDATE event
                            await self.emit_event(create_event(
                                EventType.QUOTE_UPDATE,
                                symbol=symbol,
                                payload={
                                    "symbol": quote["symbol"],
                                    "bid": quote["bid"],
                                    "ask": quote["ask"],
                                    "last": quote["last"],
                                    "volume": quote["volume"],
                                    "timestamp": quote["timestamp"],
                                    "is_tradeable": quote["is_tradeable"],
                                    "is_market_open": quote["is_market_open"],
                                    "close": quote["close"],
                                    "change": quote["change"],
                                    "change_percent": quote["change_percent"],
                                    "source": "SCHWAB",
                                },
                            ))
                            logger.info(f"[DATA] QUOTE_UPDATE {symbol} ${quote['last']:.2f} ({quote['change_percent']:+.2f}%)")
                    except Exception as e:
                        logger.error(f"Error fetching quote for {symbol}: {e}")

            except asyncio.CancelledError:
                logger.info("Market data loop stopped")
                break
            except Exception as e:
                logger.error(f"Market data loop error: {e}")
                await asyncio.sleep(1.0)  # Back off on error

    async def emit_event(self, event: Event):
        """Emit an event to all WebSocket clients and persist to database."""
        self.state.event_count += 1
        await self.ws_manager.broadcast(event)
        logger.debug(f"Event emitted: {event.event_type.value}")

        # Persist event to database (non-blocking)
        if self._persistence_available and self._persistence:
            try:
                await self._persistence.on_event(event)
            except Exception as e:
                logger.error(f"Error persisting event: {e}")

        # Track pipeline-related events for monitoring
        pipeline_event_types = {
            EventType.FEATURES_COMPUTED,
            EventType.REGIME_DETECTED,
            EventType.SIGNAL_CANDIDATE,
            EventType.SIGNAL_SCORED,
            EventType.META_APPROVED,
            EventType.META_REJECTED,
            EventType.RISK_APPROVED,
            EventType.RISK_VETO,
        }
        if event.event_type in pipeline_event_types:
            self.state.pipeline_events.append(event.to_jsonl_dict())
            # Keep only recent events
            if len(self.state.pipeline_events) > self.state.max_pipeline_events:
                self.state.pipeline_events = self.state.pipeline_events[-self.state.max_pipeline_events:]

    async def process_command(self, command: Command) -> CommandResult:
        """Process a command from UI."""
        logger.info(f"Processing command: {command.command_type} [{command.command_id}]")

        try:
            handler = getattr(self, f"_handle_{command.command_type.lower()}", None)
            if handler:
                return await handler(command)
            else:
                logger.warning(f"Unknown command type: {command.command_type}")
                return CommandResult(
                    accepted=False,
                    command_id=command.command_id,
                    message=f"Unknown command type: {command.command_type}",
                )
        except Exception as e:
            logger.error(f"Command error: {e}")
            return CommandResult(
                accepted=False,
                command_id=command.command_id,
                message=str(e),
            )

    # ========================================================================
    # Command Handlers
    # ========================================================================

    async def _handle_confirm_entry(self, command: Command) -> CommandResult:
        """Handle CONFIRM_ENTRY command - human tape-synchronized confirmation."""
        payload = command.payload
        symbol = payload.get("symbol", "").upper()
        chain_id = payload.get("chain_id", 0)
        signal_timestamp = payload.get("signal_timestamp")
        entry_price = payload.get("entry_price", 0.0)

        logger.info(f"CONFIRM_ENTRY: {symbol} @ {entry_price}")

        # Get active signal for this symbol
        active_signal = self.state.active_signals.get(symbol)
        signal_ts = active_signal.get("timestamp") if active_signal else None
        signal_entry = active_signal.get("entry_price") if active_signal else None

        # Create confirmation request
        request = ConfirmationRequest(
            symbol=symbol,
            chain_id=chain_id,
            signal_timestamp=signal_timestamp or "",
            entry_price=entry_price,
            command_id=command.command_id,
        )

        # Validate with confirmation guard
        result = self.confirmation_guard.validate(
            request=request,
            signal_timestamp=signal_ts,
            signal_entry_price=signal_entry,
            current_price=entry_price,  # UI sends current price as entry_price
            is_live_mode=self.state.trading_mode == TradingMode.LIVE,
            is_live_armed=self.state.live_armed,
            is_kill_switch_active=self.state.kill_switch_active,
            active_symbol=self.state.active_symbol,
        )

        if result.accepted:
            # Emit HUMAN_CONFIRM_ACCEPTED
            await self.emit_event(create_event(
                EventType.HUMAN_CONFIRM_ACCEPTED,
                payload={
                    "command_id": command.command_id,
                    **result.to_event_payload(),
                },
                symbol=symbol,
                correlation_id=command.command_id,
            ))

            # In PAPER mode, simulate order submission
            if self.state.trading_mode == TradingMode.PAPER:
                await self._simulate_paper_order(symbol, entry_price, command.command_id)

            return CommandResult(
                accepted=True,
                command_id=command.command_id,
                message="Confirmation accepted",
            )
        else:
            # Emit HUMAN_CONFIRM_REJECTED
            await self.emit_event(create_event(
                EventType.HUMAN_CONFIRM_REJECTED,
                payload={
                    "command_id": command.command_id,
                    "reason": result.reason.value if result.reason else "unknown",
                    **result.to_event_payload(),
                },
                symbol=symbol,
                correlation_id=command.command_id,
            ))

            return CommandResult(
                accepted=False,
                command_id=command.command_id,
                message=result.details,
            )

    async def _simulate_paper_order(self, symbol: str, price: float, correlation_id: str):
        """Simulate paper order for testing."""
        client_order_id = f"paper_{uuid.uuid4().hex[:12]}"

        # ORDER_SUBMITTED
        await self.emit_event(create_event(
            EventType.ORDER_SUBMITTED,
            payload={
                "client_order_id": client_order_id,
                "symbol": symbol,
                "side": "buy",
                "quantity": 100,
                "order_type": "market",
                "limit_price": None,
            },
            symbol=symbol,
            correlation_id=correlation_id,
        ))

        # Simulate small delay then fill
        await asyncio.sleep(0.1)

        # ORDER_FILL_RECEIVED
        await self.emit_event(create_event(
            EventType.ORDER_FILL_RECEIVED,
            payload={
                "client_order_id": client_order_id,
                "symbol": symbol,
                "side": "buy",
                "filled_quantity": 100,
                "fill_price": price,
                "exec_id": f"exec_{uuid.uuid4().hex[:8]}",
            },
            symbol=symbol,
            correlation_id=correlation_id,
        ))

    async def _handle_set_symbol_chain(self, command: Command) -> CommandResult:
        """Handle SET_SYMBOL_CHAIN - update active symbol and auto-subscribe."""
        symbol = command.payload.get("symbol", "").upper()
        self.state.active_symbol = symbol if symbol else None

        # Auto-subscribe to symbol for live data streaming
        if symbol:
            self.state.subscribed_symbols.add(symbol)
            logger.info(f"Active symbol set to: {symbol} (subscribed for live data)")
        else:
            logger.info("Active symbol cleared")

        return CommandResult(accepted=True, command_id=command.command_id)

    async def _handle_subscribe_symbol(self, command: Command) -> CommandResult:
        """Handle SUBSCRIBE_SYMBOL - subscribe to live quote updates."""
        symbol = command.payload.get("symbol", "").upper()
        if not symbol:
            return CommandResult(
                accepted=False,
                command_id=command.command_id,
                message="Symbol required",
            )

        self.state.subscribed_symbols.add(symbol)

        # Subscribe via streaming if available
        if self._streamer_available and self._use_streaming and self._streamer:
            await self._streamer.subscribe_quotes([symbol])

        # Add to signal pipeline for strategy evaluation
        if self._pipeline_available and self._pipeline:
            self._pipeline.add_symbol(symbol)
            # Load historical candles for warmup (in background)
            asyncio.create_task(self._warmup_pipeline_symbol(symbol))

        logger.info(f"Subscribed to {symbol}. Active subscriptions: {self.state.subscribed_symbols}")
        return CommandResult(accepted=True, command_id=command.command_id)

    async def _warmup_pipeline_symbol(self, symbol: str) -> None:
        """Load historical candles to warmup the pipeline for a symbol."""
        if not self._pipeline_available or not self._pipeline:
            return
        if not self._market_data_available or not self._market_client:
            logger.warning(f"[WARMUP] Cannot warmup {symbol} - market data not available")
            return

        try:
            logger.info(f"[WARMUP] Loading historical candles for {symbol}...")

            # Fetch daily candles for feature calculation (need ~60 bars)
            candles = self.get_candles(
                symbol=symbol,
                period_type="month",
                period=3,  # 3 months of daily data
                frequency_type="daily",
                frequency=1,
            )

            if not candles:
                logger.warning(f"[WARMUP] No historical candles for {symbol}")
                return

            # Feed candles to pipeline
            from morpheus.features.indicators import OHLCV
            fed_count = 0
            for candle in candles:
                ohlcv = OHLCV(
                    open=candle["open"],
                    high=candle["high"],
                    low=candle["low"],
                    close=candle["close"],
                    volume=candle["volume"],
                )
                candle_ts = datetime.fromtimestamp(candle["time"], tz=timezone.utc)
                await self._pipeline.on_candle(symbol, ohlcv, timestamp=candle_ts)
                fed_count += 1

            logger.info(f"[WARMUP] Fed {fed_count} candles to pipeline for {symbol}")

            # Check warmup status
            status = self._pipeline.get_status()
            warmup = status.get("warmup_status", {}).get(symbol, {})
            logger.info(f"[WARMUP] {symbol} warmup complete: {warmup.get('complete', False)}")

        except Exception as e:
            logger.error(f"[WARMUP] Failed to warmup {symbol}: {e}")

    async def _handle_unsubscribe_symbol(self, command: Command) -> CommandResult:
        """Handle UNSUBSCRIBE_SYMBOL - unsubscribe from live quote updates."""
        symbol = command.payload.get("symbol", "").upper()
        if symbol in self.state.subscribed_symbols:
            self.state.subscribed_symbols.discard(symbol)

            # Unsubscribe via streaming if available
            if self._streamer_available and self._use_streaming and self._streamer:
                await self._streamer.unsubscribe_quotes([symbol])

            # Remove from signal pipeline
            if self._pipeline_available and self._pipeline:
                self._pipeline.remove_symbol(symbol)

            logger.info(f"Unsubscribed from {symbol}. Active subscriptions: {self.state.subscribed_symbols}")
        return CommandResult(accepted=True, command_id=command.command_id)

    async def _handle_clear_subscriptions(self, command: Command) -> CommandResult:
        """Handle CLEAR_SUBSCRIPTIONS - clear all symbol subscriptions."""
        count = len(self.state.subscribed_symbols)

        # Unsubscribe all via streaming
        if self._streamer_available and self._use_streaming and self._streamer and count > 0:
            await self._streamer.unsubscribe_quotes(list(self.state.subscribed_symbols))

        self.state.subscribed_symbols.clear()
        logger.info(f"Cleared {count} subscriptions. Active subscriptions: {self.state.subscribed_symbols}")
        return CommandResult(accepted=True, command_id=command.command_id)

    async def _handle_arm_live_trading(self, command: Command) -> CommandResult:
        """Handle ARM_LIVE_TRADING - arm live trading (requires confirmation)."""
        confirmation = command.payload.get("confirmation", "")

        if confirmation != "I_UNDERSTAND_LIVE_TRADING":
            return CommandResult(
                accepted=False,
                command_id=command.command_id,
                message="Invalid confirmation code",
            )

        if self.state.trading_mode != TradingMode.LIVE:
            return CommandResult(
                accepted=False,
                command_id=command.command_id,
                message="Cannot arm: Not in LIVE mode",
            )

        self.state.live_armed = True
        logger.warning("LIVE TRADING ARMED")

        await self.emit_event(create_event(
            EventType.HEARTBEAT,
            payload={
                "live_armed": True,
                "trading_mode": "LIVE",
            },
        ))

        return CommandResult(accepted=True, command_id=command.command_id)

    async def _handle_disarm_live_trading(self, command: Command) -> CommandResult:
        """Handle DISARM_LIVE_TRADING - disarm live trading."""
        self.state.live_armed = False
        logger.info("Live trading disarmed")

        await self.emit_event(create_event(
            EventType.HEARTBEAT,
            payload={"live_armed": False},
        ))

        return CommandResult(accepted=True, command_id=command.command_id)

    async def _handle_activate_kill_switch(self, command: Command) -> CommandResult:
        """Handle ACTIVATE_KILL_SWITCH - emergency stop."""
        self.state.kill_switch_active = True
        self.state.live_armed = False  # Also disarm
        logger.warning("KILL SWITCH ACTIVATED")

        # Sync with signal pipeline
        if self._pipeline_available and self._pipeline:
            self._pipeline.set_kill_switch(True)

        await self.emit_event(create_event(
            EventType.HEARTBEAT,
            payload={
                "kill_switch_active": True,
                "live_armed": False,
            },
        ))

        return CommandResult(accepted=True, command_id=command.command_id)

    async def _handle_deactivate_kill_switch(self, command: Command) -> CommandResult:
        """Handle DEACTIVATE_KILL_SWITCH - reset kill switch."""
        self.state.kill_switch_active = False
        logger.info("Kill switch deactivated")

        # Sync with signal pipeline
        if self._pipeline_available and self._pipeline:
            self._pipeline.set_kill_switch(False)

        await self.emit_event(create_event(
            EventType.HEARTBEAT,
            payload={"kill_switch_active": False},
        ))

        return CommandResult(accepted=True, command_id=command.command_id)

    async def _handle_cancel_all(self, command: Command) -> CommandResult:
        """Handle CANCEL_ALL - cancel all pending orders."""
        logger.info("Cancel all orders requested")
        # In paper mode, just acknowledge
        return CommandResult(accepted=True, command_id=command.command_id)

    async def _handle_set_profile(self, command: Command) -> CommandResult:
        """Handle SET_PROFILE - update risk profile."""
        self.state.profile.update(command.payload)
        logger.info(f"Profile updated: {self.state.profile}")

        await self.emit_event(create_event(
            EventType.HEARTBEAT,
            payload={"profile": self.state.profile},
        ))

        return CommandResult(accepted=True, command_id=command.command_id)

    # ========================================================================
    # Signal Injection (for testing)
    # ========================================================================

    async def inject_signal(self, symbol: str, direction: str, entry_price: float):
        """Inject a mock signal for testing."""
        timestamp = datetime.now(timezone.utc).isoformat()

        self.state.active_signals[symbol] = {
            "direction": direction,
            "entry_price": entry_price,
            "timestamp": timestamp,
            "strategy_name": "test_strategy",
        }

        await self.emit_event(create_event(
            EventType.SIGNAL_CANDIDATE,
            payload={
                "direction": direction,
                "entry_price": entry_price,
                "strategy_name": "test_strategy",
                "stop_price": entry_price * 0.98,
                "target_price": entry_price * 1.04,
            },
            symbol=symbol,
        ))

        logger.info(f"Signal injected: {symbol} {direction} @ {entry_price}")


# ============================================================================
# FastAPI Application
# ============================================================================


def create_app() -> FastAPI:
    """Create and configure FastAPI application."""

    server = MorpheusServer()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """Application lifespan manager."""
        await server.start()
        yield
        await server.stop()

    app = FastAPI(
        title="Morpheus Trading Engine",
        description="Event-driven trading system API",
        version="0.1.0",
        lifespan=lifespan,
    )

    # CORS for Electron app
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Store server reference
    app.state.server = server

    # ========================================================================
    # Routes
    # ========================================================================

    @app.get("/health", response_model=HealthResponse)
    async def health_check():
        """Health check endpoint."""
        uptime = (datetime.now(timezone.utc) - server.state.start_time).total_seconds()
        return HealthResponse(
            status="healthy",
            trading_mode=server.state.trading_mode.value.upper(),
            live_armed=server.state.live_armed,
            kill_switch_active=server.state.kill_switch_active,
            uptime_seconds=uptime,
            event_count=server.state.event_count,
            websocket_clients=server.ws_manager.client_count,
            pipeline_enabled=server._pipeline_available,
        )

    @app.get("/api/ui/state")
    async def get_state():
        """Get current state for UI initialization."""
        return server.state.to_dict()

    @app.post("/api/ui/command", response_model=CommandResult)
    async def handle_command(command: Command):
        """Handle UI command."""
        return await server.process_command(command)

    @app.websocket("/ws/events")
    async def websocket_endpoint(websocket: WebSocket):
        """WebSocket endpoint for event streaming."""
        await server.ws_manager.connect(websocket)
        try:
            while True:
                # Keep connection alive, handle any incoming messages
                data = await websocket.receive_text()
                # Currently we don't process incoming WS messages
                logger.debug(f"WS received: {data}")
        except WebSocketDisconnect:
            await server.ws_manager.disconnect(websocket)
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
            await server.ws_manager.disconnect(websocket)

    # ========================================================================
    # Market Data Routes
    # ========================================================================

    @app.get("/api/market/status")
    async def market_status():
        """Check if market data is available."""
        return {
            "available": server._market_data_available,
            "provider": "schwab" if server._market_data_available else None,
        }

    @app.get("/api/market/quote/{symbol}")
    async def get_quote(symbol: str):
        """Get current quote for a symbol."""
        if not server._market_data_available:
            raise HTTPException(
                status_code=503,
                detail="Market data not available. Configure Schwab credentials."
            )

        quote = server.get_quote(symbol)
        if quote is None:
            raise HTTPException(status_code=404, detail=f"Quote not available for {symbol}")

        return quote

    @app.get("/api/market/candles/{symbol}")
    async def get_candles(
        symbol: str,
        period_type: str = "day",
        period: int = 1,
        frequency: int = 1,
        frequency_type: str = "minute",
    ):
        """Get historical candles for a symbol."""
        if not server._market_data_available:
            raise HTTPException(
                status_code=503,
                detail="Market data not available. Configure Schwab credentials."
            )

        candles = server.get_candles(
            symbol=symbol,
            period_type=period_type,
            period=period,
            frequency=frequency,
            frequency_type=frequency_type,
        )

        return {"symbol": symbol.upper(), "candles": candles}

    @app.get("/api/market/quotes")
    async def get_quotes(symbols: str):
        """Get quotes for multiple symbols (comma-separated)."""
        if not server._market_data_available:
            raise HTTPException(
                status_code=503,
                detail="Market data not available. Configure Schwab credentials."
            )

        symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
        results = {}

        for sym in symbol_list:
            quote = server.get_quote(sym)
            if quote:
                results[sym] = quote

        return {"quotes": results}

    # ========================================================================
    # Trading Data Routes (Positions, Orders, Transactions)
    # ========================================================================

    @app.get("/api/trading/positions")
    async def get_positions():
        """Get current account positions."""
        if not server._trading_available:
            raise HTTPException(
                status_code=503,
                detail="Trading data not available. Configure Schwab credentials."
            )

        positions = server.get_positions()
        return {"positions": positions}

    @app.get("/api/trading/orders")
    async def get_orders(status: str | None = None):
        """Get orders (optionally filtered by status)."""
        if not server._trading_available:
            raise HTTPException(
                status_code=503,
                detail="Trading data not available. Configure Schwab credentials."
            )

        orders = server.get_orders(status=status)
        return {"orders": orders}

    @app.get("/api/trading/transactions")
    async def get_transactions():
        """Get today's transactions/executions."""
        if not server._trading_available:
            raise HTTPException(
                status_code=503,
                detail="Trading data not available. Configure Schwab credentials."
            )

        transactions = server.get_transactions()
        return {"transactions": transactions}

    # ========================================================================
    # OAuth Setup Routes
    # ========================================================================

    @app.get("/api/auth/schwab/url")
    async def get_schwab_auth_url():
        """Get the Schwab OAuth authorization URL."""
        if not SCHWAB_AVAILABLE:
            raise HTTPException(status_code=503, detail="Schwab modules not available")

        client_id = os.environ.get("SCHWAB_CLIENT_ID")
        redirect_uri = os.environ.get("SCHWAB_REDIRECT_URI", "https://127.0.0.1:6969")

        if not client_id:
            raise HTTPException(status_code=503, detail="SCHWAB_CLIENT_ID not configured")

        from urllib.parse import urlencode
        params = {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
        }
        auth_url = f"https://api.schwabapi.com/v1/oauth/authorize?{urlencode(params)}"

        return {
            "auth_url": auth_url,
            "redirect_uri": redirect_uri,
            "instructions": "1. Open auth_url in browser. 2. Log in to Schwab. 3. After redirect, copy the 'code' parameter from the URL. 4. POST it to /api/auth/schwab/callback",
        }

    @app.post("/api/auth/schwab/callback")
    async def schwab_auth_callback(code: str):
        """Exchange auth code for tokens using Basic auth (base64 encoded credentials)."""
        import base64

        if not SCHWAB_AVAILABLE:
            raise HTTPException(status_code=503, detail="Schwab modules not available")

        client_id = os.environ.get("SCHWAB_CLIENT_ID")
        client_secret = os.environ.get("SCHWAB_CLIENT_SECRET")
        redirect_uri = os.environ.get("SCHWAB_REDIRECT_URI", "https://127.0.0.1:6969")
        token_path = os.environ.get("SCHWAB_TOKEN_PATH", "./tokens/schwab_token.json")

        if not client_id or not client_secret:
            raise HTTPException(status_code=503, detail="Schwab credentials not configured")

        try:
            # Create Basic auth header with base64 encoded credentials
            credentials = f"{client_id}:{client_secret}"
            encoded_credentials = base64.b64encode(credentials.encode()).decode()

            # Exchange code for tokens using Basic auth header
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    "https://api.schwabapi.com/v1/oauth/token",
                    data={
                        "grant_type": "authorization_code",
                        "code": code,
                        "redirect_uri": redirect_uri,
                    },
                    headers={
                        "Authorization": f"Basic {encoded_credentials}",
                        "Content-Type": "application/x-www-form-urlencoded",
                    },
                )

                if response.status_code != 200:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Token exchange failed: {response.status_code} - {response.text}"
                    )

                token_data = response.json()

                # Save token to file
                token_path_obj = Path(token_path)
                token_path_obj.parent.mkdir(parents=True, exist_ok=True)
                with open(token_path_obj, "w") as f:
                    json.dump(token_data, f, indent=2)
                logger.info(f"Schwab token saved to {token_path}")

                # Reinitialize market data client
                server._init_market_data()

                return {
                    "success": True,
                    "message": "Tokens saved successfully. Market data is now available.",
                    "expires_in": token_data.get("expires_in"),
                }

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"OAuth callback failed: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    # ========================================================================
    # Pipeline Status Routes
    # ========================================================================

    @app.get("/api/pipeline/status")
    async def get_pipeline_status():
        """Get signal pipeline status and diagnostics."""
        if not server._pipeline_available or not server._pipeline:
            return {
                "enabled": False,
                "message": "Signal pipeline not available",
            }

        status = server._pipeline.get_status()
        return {
            "enabled": True,
            "status": status,
        }

    # =========================================================================
    # MASS (Morpheus Adaptive Strategy System) API Endpoints
    # =========================================================================

    @app.get("/api/mass/status")
    async def get_mass_status():
        """Get MASS system status - regime, structure, classifier, supervisor."""
        if not server._pipeline_available or not server._pipeline:
            return {"enabled": False, "message": "Pipeline not available"}

        pipeline = server._pipeline
        status = pipeline.get_status()
        result = {
            "enabled": status.get("mass_enabled", False),
            "regime": status.get("mass_regime"),
            "regime_mapping_enabled": status.get("mass_regime_mapping_enabled", False),
            "feedback_enabled": status.get("mass_feedback_enabled", False),
            "supervisor_enabled": status.get("mass_supervisor_enabled", False),
            "structure_analyzer_active": status.get("mass_structure_analyzer", False),
            "strategy_classifier_active": status.get("mass_strategy_classifier", False),
        }

        # Add supervisor status if available
        if hasattr(pipeline, "_supervisor"):
            result["supervisor"] = pipeline._supervisor.get_status()

        return result

    @app.get("/api/mass/performance")
    async def get_mass_performance():
        """Get per-strategy performance metrics."""
        if not server._pipeline_available or not server._pipeline:
            return {"error": "Pipeline not available"}

        pipeline = server._pipeline
        if hasattr(pipeline, "_performance_tracker"):
            return {
                "strategies": pipeline._performance_tracker.get_all_performance_dicts(),
                "total_outcomes": pipeline._performance_tracker.total_outcomes,
            }
        return {"strategies": {}, "total_outcomes": 0}

    @app.get("/api/mass/weights")
    async def get_mass_weights():
        """Get current strategy weights."""
        if not server._pipeline_available or not server._pipeline:
            return {"error": "Pipeline not available"}

        pipeline = server._pipeline
        if hasattr(pipeline, "_weight_manager"):
            return pipeline._weight_manager.to_dict()
        return {"current_weights": {}, "base_weights": {}}

    @app.post("/api/mass/supervisor/check")
    async def trigger_supervisor_check():
        """Trigger a supervisor health check."""
        if not server._pipeline_available or not server._pipeline:
            return {"error": "Pipeline not available"}

        pipeline = server._pipeline
        if hasattr(pipeline, "_supervisor"):
            alerts = pipeline._supervisor.check_health()
            # Emit alert events
            for alert in alerts:
                await server._broadcast_event(alert.to_event())
            return {
                "alerts": [a.to_event_payload() for a in alerts],
                "count": len(alerts),
            }
        return {"alerts": [], "count": 0}

    @app.get("/api/pipeline/events")
    async def get_pipeline_events(limit: int = 50, event_type: str | None = None):
        """Get recent pipeline events for monitoring."""
        events = server.state.pipeline_events.copy()

        # Filter by event type if specified
        if event_type:
            events = [e for e in events if e.get("event_type") == event_type]

        # Return most recent first, limited
        events = list(reversed(events))[:limit]

        return {
            "events": events,
            "total_count": len(server.state.pipeline_events),
        }

    @app.get("/api/pipeline/monitor")
    async def get_pipeline_monitor():
        """Get full pipeline monitoring data for the monitor panel."""
        if not server._pipeline_available or not server._pipeline:
            return {
                "enabled": False,
                "message": "Signal pipeline not available",
            }

        status = server._pipeline.get_status()

        # Get recent events grouped by type
        events = server.state.pipeline_events.copy()
        recent_signals = [e for e in events if e.get("event_type") == "SIGNAL_CANDIDATE"][-10:]
        recent_scores = [e for e in events if e.get("event_type") == "SIGNAL_SCORED"][-10:]
        recent_gate = [e for e in events if e.get("event_type") in ("META_APPROVED", "META_REJECTED")][-10:]
        recent_risk = [e for e in events if e.get("event_type") in ("RISK_APPROVED", "RISK_VETO")][-10:]
        recent_regime = [e for e in events if e.get("event_type") == "REGIME_DETECTED"][-10:]

        return {
            "enabled": True,
            "status": status,
            "recent_events": {
                "signals": list(reversed(recent_signals)),
                "scores": list(reversed(recent_scores)),
                "gate_decisions": list(reversed(recent_gate)),
                "risk_decisions": list(reversed(recent_risk)),
                "regime_detections": list(reversed(recent_regime)),
            },
            "event_counts": {
                "total": len(events),
                "signals": len([e for e in events if e.get("event_type") == "SIGNAL_CANDIDATE"]),
                "approved": len([e for e in events if e.get("event_type") == "META_APPROVED"]),
                "rejected": len([e for e in events if e.get("event_type") == "META_REJECTED"]),
                "risk_approved": len([e for e in events if e.get("event_type") == "RISK_APPROVED"]),
                "risk_vetoed": len([e for e in events if e.get("event_type") == "RISK_VETO"]),
            },
        }

    # ========================================================================
    # Persistence/Stats Routes
    # ========================================================================

    @app.get("/api/stats/today")
    async def get_today_stats():
        """Get today's trading statistics from persistence layer."""
        if not server._persistence_available or not server._persistence:
            return {
                "enabled": False,
                "error": "Persistence layer not available",
            }

        try:
            stats = server._persistence.get_today_stats()
            return {
                "enabled": True,
                **stats,
            }
        except Exception as e:
            logger.error(f"Error getting today's stats: {e}")
            return {
                "enabled": True,
                "error": str(e),
            }

    @app.get("/api/stats/rejections")
    async def get_rejection_reasons(days: int = 7):
        """Get rejection reason breakdown."""
        if not server._persistence_available or not server._persistence:
            return {"enabled": False}

        try:
            from datetime import date, timedelta
            start_date = (date.today() - timedelta(days=days)).isoformat()
            reasons = server._persistence.decisions.count_rejection_reasons(start_date)
            return {
                "enabled": True,
                "days": days,
                "reasons": reasons,
            }
        except Exception as e:
            logger.error(f"Error getting rejection reasons: {e}")
            return {"enabled": True, "error": str(e)}

    @app.get("/api/stats/trades")
    async def get_trade_stats(days: int = 7):
        """Get trade P&L summary."""
        if not server._persistence_available or not server._persistence:
            return {"enabled": False}

        try:
            from datetime import date, timedelta
            start_date = (date.today() - timedelta(days=days)).isoformat()
            end_date = date.today().isoformat()
            summary = server._persistence.trades.get_pnl_summary(start_date, end_date)
            return {
                "enabled": True,
                "days": days,
                **summary,
            }
        except Exception as e:
            logger.error(f"Error getting trade stats: {e}")
            return {"enabled": True, "error": str(e)}

    # ========================================================================
    # Scanner Routes
    # ========================================================================

    @app.get("/api/scanner/status")
    async def get_scanner_status():
        """Get scanner status and statistics."""
        if not server._scanner_available:
            return {"enabled": False, "error": "Scanner not available"}

        scanner_stats = server._scanner.get_stats() if server._scanner else {}
        watchlist_stats = server._watchlist.get_stats() if server._watchlist else {}

        return {
            "enabled": True,
            "scanner": scanner_stats,
            "watchlist": watchlist_stats,
        }

    @app.get("/api/scanner/watchlist")
    async def get_watchlist():
        """Get current watchlist entries."""
        if not server._scanner_available or not server._watchlist:
            return {"enabled": False, "entries": []}

        entries = server._watchlist.get_all()
        return {
            "enabled": True,
            "entries": [e.to_dict() for e in entries],
            "active_count": len(server._watchlist.get_active()),
            "new_count": len(server._watchlist.get_new()),
            "stale_count": len(server._watchlist.get_stale()),
        }

    @app.post("/api/scanner/scan")
    async def force_scan():
        """Force an immediate scan (for testing)."""
        if not server._scanner_available or not server._scanner:
            return {"enabled": False, "error": "Scanner not available"}

        try:
            candidates = await server._scanner.force_scan()
            return {
                "enabled": True,
                "candidates_found": len(candidates),
                "candidates": [c.to_dict() for c in candidates],
            }
        except Exception as e:
            logger.error(f"Force scan error: {e}")
            return {"enabled": True, "error": str(e)}

    @app.post("/api/scanner/add/{symbol}")
    async def add_to_watchlist(symbol: str, reason: str = "manual"):
        """Manually add a symbol to the watchlist."""
        if not server._scanner_available or not server._watchlist:
            return {"enabled": False, "error": "Scanner not available"}

        try:
            entry = await server._watchlist.add(
                symbol=symbol.upper(),
                reason=reason,
            )
            if entry:
                return {"success": True, "symbol": symbol.upper(), "state": entry.state.value}
            else:
                return {"success": False, "error": "Watchlist at capacity"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    @app.delete("/api/scanner/remove/{symbol}")
    async def remove_from_watchlist(symbol: str):
        """Remove a symbol from the watchlist."""
        if not server._scanner_available or not server._watchlist:
            return {"enabled": False, "error": "Scanner not available"}

        try:
            await server._watchlist.remove(symbol.upper())
            return {"success": True, "symbol": symbol.upper()}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # ========================================================================
    # Testing Routes (only for paper mode)
    # ========================================================================

    @app.post("/api/test/inject_signal")
    async def inject_signal(symbol: str, direction: str = "long", entry_price: float = 100.0):
        """Inject a test signal (paper mode only)."""
        if server.state.trading_mode != TradingMode.PAPER:
            raise HTTPException(status_code=403, detail="Test endpoints only available in PAPER mode")

        await server.inject_signal(symbol.upper(), direction, entry_price)
        return {"status": "signal_injected", "symbol": symbol.upper()}

    return app


# ============================================================================
# Main Entry Point
# ============================================================================


def main():
    """Run the server."""
    import uvicorn

    logger.info("=" * 60)
    logger.info("MORPHEUS TRADING ENGINE")
    logger.info("=" * 60)
    logger.info("SAFETY DEFAULTS ENFORCED:")
    logger.info("  - PAPER mode: ON")
    logger.info("  - Live armed: FALSE")
    logger.info("  - Kill switch: SAFE")
    logger.info("=" * 60)

    uvicorn.run(
        "morpheus.server.main:create_app",
        factory=True,
        host="0.0.0.0",
        port=8010,
        log_level="info",
        reload=False,
    )


if __name__ == "__main__":
    main()
