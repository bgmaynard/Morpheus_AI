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
from datetime import datetime, time, timezone
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
from morpheus.core.bot_identity import (
    BOT_ID, BOT_NAME, BOT_VERSION,
    get_bot_identity, acquire_bot_lock, release_bot_lock,
    verify_bot_identity, get_running_bot_info,
)
from morpheus.core.runtime_config import (
    get_runtime_config, get_runtime_config_manager,
    reload_runtime_config, update_runtime_config,
)
from morpheus.execution.base import TradingMode, BlockReason
from morpheus.execution.guards import (
    ConfirmationGuard,
    ConfirmationRequest,
    create_confirmation_guard,
    QuoteFreshnessGuard,
    QuoteFreshnessConfig,
    create_quote_freshness_guard,
)
from morpheus.data.market_snapshot import create_snapshot

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

# Paper position manager (exit management + position tracking)
from morpheus.execution.paper_position_manager import PaperPositionManager, ExitPlan

# Microstructure gates (halt/borrow/SSR status + data staleness)
from morpheus.core.symbol_state import (
    SymbolStateManager,
    MicrostructureConfig,
    SecurityStatus,
    BorrowStatus,
)
from morpheus.risk.microstructure_gates import MicrostructureGates

# Validation mode (microstructure correctness testing)
from morpheus.core.validation_mode import (
    ValidationModeConfig,
    init_validation_mode,
    get_validation_config,
    get_validation_counters,
    get_validation_logger,
    is_validation_mode,
    reset_validation_counters,
    get_validation_summary,
)

# Worklist pipeline (symbol intake, scrutiny, scoring)
try:
    from morpheus.worklist.pipeline import (
        WorklistPipeline,
        WorklistPipelineConfig,
        get_worklist_pipeline,
        reset_worklist_pipeline,
    )
    from morpheus.worklist.scrutiny import ScrutinyConfig
    from morpheus.worklist.scoring import ScoringConfig
    WORKLIST_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Worklist pipeline not available: {e}")
    WORKLIST_AVAILABLE = False

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

        # Latest quotes cache (populated by polling/streaming loop)
        self.latest_quotes: dict[str, dict[str, Any]] = {}

        # Pipeline event history for monitoring (circular buffer)
        self.pipeline_events: list[dict[str, Any]] = []
        self.max_pipeline_events: int = 200

        # Scanner health tracking (for auto-trade permission)
        self.last_scanner_update: datetime | None = None
        self.scanner_health_ok: bool = False
        self.max_scanner_stale_seconds: float = 120.0  # 2 minutes

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
        # Quote freshness guard - CRITICAL SAFETY (blocks orders on stale/invalid quotes)
        self.quote_freshness_guard = create_quote_freshness_guard(
            QuoteFreshnessConfig(
                max_quote_age_ms=2000,  # 2 seconds max quote age
                max_spread_pct=3.0,     # 3% max spread for small caps
                max_price_deviation_pct=50.0,  # Bid/ask must be within 50% of last
            )
        )
        self._heartbeat_task: asyncio.Task | None = None
        self._market_data_task: asyncio.Task | None = None
        self._trading_data_task: asyncio.Task | None = None
        self._token_refresh_task: asyncio.Task | None = None
        self._daily_reset_task: asyncio.Task | None = None
        self._last_daily_reset: datetime | None = None

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

        # Paper position manager (exit management + position tracking)
        self._paper_position_manager: PaperPositionManager | None = None

        # Microstructure gates (halt/borrow/SSR status + data staleness)
        self._symbol_state_manager: SymbolStateManager | None = None
        self._microstructure_gates: MicrostructureGates | None = None

        # Worklist pipeline (symbol intake, scrutiny, scoring)
        self._worklist_pipeline: WorklistPipeline | None = None
        self._worklist_available = False

        self._init_market_data()
        self._init_pipeline()
        self._init_persistence()
        self._init_scanner()
        self._init_worklist()

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

            # Morpheus_AI is READ-ONLY for the shared token (IBKR bot owns refresh).
            # If token is expired at startup, log it but proceed — the reload daemon
            # will pick up the fresh token from disk once Morpheus writes it.
            if token.is_expired:
                logger.warning(
                    "[TOKEN_RELOAD] Shared token is expired at startup. "
                    "Waiting for Morpheus (IBKR bot) to refresh it. "
                    "Market data will be unavailable until a valid token appears on disk."
                )

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
                permissive_mode=False,  # Use real risk management
                min_signal_confidence=0.0,  # Emit all signals
                respect_kill_switch=True,
            )

            # MASS config with all Phase 2 features enabled
            from morpheus.core.mass_config import MASSConfig
            mass_config = MASSConfig(
                enabled=True,
                regime_mapping_enabled=True,
                feedback_enabled=True,
                supervisor_enabled=True,
            )

            # Create paper position manager for Exit Management v1
            self._paper_position_manager = PaperPositionManager(
                emit_event=self.emit_event,
                kill_switch_ref=lambda: self.state.kill_switch_active,
                get_runtime_config=get_runtime_config,  # Hot-reload support
            )
            logger.info("[EXIT_MGR] Exit Management v1 initialized")
            logger.info("[EXIT_MGR] All trades require valid exit plan (TIME_STOP, HARD_STOP, TRAIL_STOP)")

            # Initialize validation mode (microstructure correctness testing)
            validation_config = ValidationModeConfig(
                enabled=True,  # Enable validation mode
                max_concurrent_positions=1,
                max_notional_exposure=500.0,
                max_trades_per_phase={"PREMARKET": 3, "RTH": 5, "AFTER_HOURS": 0},
                allowed_strategies=[
                    "catalyst_momentum", "first_pullback", "vwap_reclaim",
                    "order_flow_scalp", "hod_continuation", "day2_continuation",
                    "coil_breakout", "short_squeeze",
                ],
                premarket_allowed_strategies=["premarket_breakout", "catalyst_momentum", "first_pullback"],
                allow_shorts=False,
                trading_end=time(16, 0),  # RTH window extends to 4:00 PM ET
            )
            init_validation_mode(validation_config)

            # Create microstructure gates (halt/borrow/SSR + data staleness)
            microstructure_config = MicrostructureConfig(
                stale_quote_seconds=get_runtime_config().max_quote_age_ms / 1000.0,  # Convert ms to seconds
                post_halt_cooldown_seconds=60.0,
                htb_short_size_haircut=0.5,
                htb_room_to_profit_relaxation=0.15,
            )
            self._symbol_state_manager = SymbolStateManager(
                config=microstructure_config,
                emit_event=self.emit_event,
            )
            self._microstructure_gates = MicrostructureGates(
                state_manager=self._symbol_state_manager,
                config=microstructure_config,
                emit_event=self.emit_event,
            )
            logger.info("[MICROSTRUCTURE] Symbol state manager + gates initialized")
            logger.info(f"  - Stale quote threshold: {microstructure_config.stale_quote_seconds}s")
            logger.info(f"  - Post-halt cooldown: {microstructure_config.post_halt_cooldown_seconds}s")
            logger.info(f"  - HTB short haircut: {microstructure_config.htb_short_size_haircut:.0%}")

            # Initialize Momentum Engine (optional — additive only)
            self._momentum_engine = None
            try:
                from morpheus.ai.momentum_engine import MomentumEngine

                async def _on_momentum_state_change(snapshot):
                    """Emit momentum state change as event."""
                    await self.emit_event(create_event(
                        EventType.MOMENTUM_STATE_CHANGE,
                        payload=snapshot.to_dict(),
                        symbol=snapshot.symbol,
                    ))

                self._momentum_engine = MomentumEngine(
                    on_state_change=lambda snap: asyncio.create_task(
                        _on_momentum_state_change(snap)
                    ),
                )
                logger.info("[MOMENTUM] Momentum engine initialized")
            except Exception as e:
                logger.warning(f"[MOMENTUM] Momentum engine not available: {e}")

            # Create pipeline with emit callback
            self._pipeline = SignalPipeline(
                config=config,
                emit_event=self.emit_event,  # Wire to server's event emission
                mass_config=mass_config,
                position_manager=self._paper_position_manager,
                momentum_engine=self._momentum_engine,
            )
            self._pipeline_available = True
            logger.info("Signal pipeline initialized successfully (FULL MASS + RISK)")
            logger.info(f"  - Permissive mode: {config.permissive_mode}")
            logger.info(f"  - MASS regime mapping: {mass_config.regime_mapping_enabled}")
            logger.info(f"  - MASS feedback: {mass_config.feedback_enabled}")
            logger.info(f"  - MASS supervisor: {mass_config.supervisor_enabled}")
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
                    profiles=["TOP_GAINERS", "GAPPERS"],
                    max_symbols=25,
                    auto_subscribe=True,
                    emit_discovery_events=True,
                )
                self._scanner_integration = ScannerIntegration(
                    config=integration_config,
                    on_event=self._handle_scanner_event,
                    on_symbol_discovered=self._handle_scanner_symbol_discovered,
                    on_symbol_removed=self._handle_scanner_symbol_removed,
                    on_heartbeat=self._handle_scanner_heartbeat,
                )
                logger.info("Scanner Integration initialized (events + halts + heartbeat)")

                # Wire scanner context to pipeline for external augmentation
                # This allows FeatureContext to include scanner_score, gap_pct, etc.
                # Enriched with Morpheus's own Schwab quotes as fallback
                if self._pipeline_available and self._pipeline:
                    self._pipeline.set_external_context_provider(
                        self._get_enriched_external_context
                    )
                    logger.info("Pipeline wired to Scanner Integration (context augmentation enabled)")

        except Exception as e:
            logger.error(f"Failed to initialize scanner: {e}")
            self._scanner_available = False

    def _init_worklist(self):
        """Initialize the worklist pipeline for symbol intake, scrutiny, and scoring."""
        if not WORKLIST_AVAILABLE:
            logger.warning("Worklist pipeline not available")
            return

        try:
            # Create scrutiny config (calibrated for small-cap momentum)
            scrutiny_config = ScrutinyConfig(
                min_price=0.50,
                max_price=20.00,
                min_volume=100_000,
                min_rvol=1.5,
                max_spread_pct=3.0,
                min_scanner_score=50.0,
                min_dollar_volume=100_000,
                max_data_age_seconds=300.0,
            )

            # Create scoring config (deterministic priority scoring)
            scoring_config = ScoringConfig(
                weight_scanner_score=0.30,
                weight_gap=0.25,
                weight_rvol=0.25,
                weight_volume=0.10,
                weight_news=0.10,
                news_present_bonus=1.10,
            )

            # Create pipeline config
            pipeline_config = WorklistPipelineConfig(
                scrutiny=scrutiny_config,
                scoring=scoring_config,
                min_score_for_trading=50.0,
                max_active_symbols=20,
                auto_reset_on_new_day=True,
                archive_old_sessions=True,
            )

            # Initialize worklist pipeline with event emission
            self._worklist_pipeline = get_worklist_pipeline(
                config=pipeline_config,
                emit_event=self.emit_event,
            )
            self._worklist_available = True

            logger.info("Worklist pipeline initialized successfully")
            logger.info(f"  - Min score for trading: {pipeline_config.min_score_for_trading}")
            logger.info(f"  - Max active symbols: {pipeline_config.max_active_symbols}")
            logger.info(f"  - Scrutiny: price ${scrutiny_config.min_price}-${scrutiny_config.max_price}")
            logger.info(f"  - Scrutiny: min RVOL {scrutiny_config.min_rvol}x, min score {scrutiny_config.min_scanner_score}")

        except Exception as e:
            logger.error(f"Failed to initialize worklist pipeline: {e}")
            self._worklist_available = False

    async def _get_enriched_external_context(self, symbol: str) -> dict:
        """
        Get external context enriched with Morpheus's own Schwab quotes.

        Falls back to Morpheus quote data when scanner returns zeroes.
        This fixes the gap where the scanner's Schwab cache is empty but
        Morpheus has live quotes from its own streaming/polling connection.
        """
        # Start with scanner data
        context = {}
        if self._scanner_available and self._scanner_integration:
            try:
                context = await self._scanner_integration.get_symbol_context(symbol)
            except Exception:
                context = {}

        # Enrich with Morpheus's own Schwab quote data if scanner data is empty
        if not context.get("scanner_score") and not context.get("gap_pct"):
            try:
                quote = self.state.latest_quotes.get(symbol, {})
                # If cached quote lacks change_percent, fetch fresh from Schwab
                if quote and not quote.get("change_percent"):
                    fresh = self.get_quote(symbol)
                    if fresh:
                        quote = fresh
                        self.state.latest_quotes[symbol] = fresh
                if quote:
                    change_pct = quote.get("change_percent", 0)
                    volume = quote.get("volume", 0)
                    last = quote.get("last", 0)
                    close_price = quote.get("close", 0)

                    # Compute gap_pct from change_percent (premarket gap from prev close)
                    gap_pct = abs(change_pct) if change_pct else 0

                    # Compute rvol_proxy from volume vs typical
                    # Use a rough estimate: 100K is "normal" premarket volume for small-caps
                    rvol_proxy = volume / 100_000 if volume else 0

                    # Compute a basic scanner_score from available data
                    # Score 0-100 based on change%, volume, spread
                    score = 0
                    if gap_pct >= 10:
                        score += 40
                    elif gap_pct >= 5:
                        score += 30
                    elif gap_pct >= 3:
                        score += 20
                    elif gap_pct >= 1:
                        score += 10

                    if volume >= 1_000_000:
                        score += 30
                    elif volume >= 500_000:
                        score += 20
                    elif volume >= 100_000:
                        score += 10

                    if rvol_proxy >= 5:
                        score += 30
                    elif rvol_proxy >= 2:
                        score += 20
                    elif rvol_proxy >= 1:
                        score += 10

                    context["scanner_score"] = max(context.get("scanner_score", 0), score)
                    context["gap_pct"] = max(context.get("gap_pct", 0), gap_pct)
                    context["rvol_proxy"] = max(context.get("rvol_proxy", 0), rvol_proxy)
                    context["change_pct"] = change_pct
                    context["_enriched_from"] = "SCHWAB_QUOTE_FALLBACK"

                    if score >= 50:
                        logger.info(
                            f"[SCANNER-ENRICH] {symbol}: score={score} gap={gap_pct:.1f}% "
                            f"rvol={rvol_proxy:.1f}x vol={volume:,}"
                        )
            except Exception as e:
                logger.debug(f"[SCANNER-ENRICH] Error enriching {symbol}: {e}")

        return context

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

        Flow: Scanner → Worklist (scrutiny + scoring) → Pipeline
        """
        try:
            # Update scanner health - scanner is alive!
            self.state.last_scanner_update = datetime.now(timezone.utc)
            self.state.scanner_health_ok = True

            logger.info(f"[SCANNER] Symbol discovered: {symbol}")

            # Get scanner context for worklist processing
            scanner_context = {}
            if self._scanner_available and self._scanner_integration:
                try:
                    scanner_context = await self._scanner_integration.get_symbol_context(symbol)
                except Exception as e:
                    logger.warning(f"[SCANNER] Could not get context for {symbol}: {e}")

            # Process through worklist pipeline (scrutiny + scoring)
            # This is the gatekeeper - only symbols that pass scrutiny enter the worklist
            worklist_entry = None
            if self._worklist_available and self._worklist_pipeline:
                # Get scanner score - normalize from 0-1 to 0-100 scale if needed
                raw_score = scanner_context.get("scanner_score") or scanner_context.get("ai_score") or 0.5
                # MAX_AI returns scores in 0-1 range, scrutiny expects 0-100
                scanner_score = raw_score * 100 if raw_score <= 1.0 else raw_score
                scanner_score = max(scanner_score, 50.0)  # Default minimum if no score

                gap_pct = scanner_context.get("gap_pct", 0.0) or 0.0
                rvol = scanner_context.get("rvol", 1.0) or scanner_context.get("rvol_proxy", 1.0) or 1.0
                volume = scanner_context.get("volume", 0) or scanner_context.get("totalVolume", 0) or 0
                price = scanner_context.get("price", 0.0) or scanner_context.get("last", 0.0) or scanner_context.get("lastPrice", 0.0) or 0.0

                # Extended scanner data for operator visibility (Float, Avg Volume, Short%)
                float_shares = scanner_context.get("float_shares") or scanner_context.get("float")
                avg_volume = scanner_context.get("averageVolume") or scanner_context.get("avg_volume")
                short_pct = scanner_context.get("short_pct") or scanner_context.get("short_percent") or scanner_context.get("shortInterest")
                market_cap = scanner_context.get("market_cap") or scanner_context.get("marketCap")

                # Enhanced data for professional scanner alignment
                rvol_5m = scanner_context.get("rvol_5m") or scanner_context.get("velocity_1m")  # 5-min RVOL
                halt_status = scanner_context.get("halt_status")
                halt_ts = scanner_context.get("halt_ts") or scanner_context.get("halt_time")
                news_present = bool(scanner_context.get("news_present") or scanner_context.get("has_news"))
                # Scanner news indicator is AUTHORITATIVE - if scanner says there's news, trust it
                scanner_news_indicator = bool(
                    scanner_context.get("scanner_news_indicator") or
                    scanner_context.get("has_news") or
                    scanner_context.get("news_indicator")
                )

                # If we don't have price from scanner, try to get from Schwab
                if price <= 0:
                    quote = self.state.latest_quotes.get(symbol)
                    if quote:
                        price = quote.get("last", 0.0)
                        volume = volume or quote.get("volume", 0)

                if price > 0:  # Only process if we have price data
                    # Parse scanner timestamp for staleness check
                    data_timestamp = None
                    ts_str = scanner_context.get("_timestamp")
                    if ts_str:
                        try:
                            data_timestamp = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                        except (ValueError, AttributeError):
                            pass

                    # Check for halt status
                    is_halted = halt_status == "HALTED"

                    worklist_entry = await self._worklist_pipeline.process_scanner_row(
                        symbol=symbol,
                        scanner_score=scanner_score,
                        gap_pct=gap_pct,
                        rvol=rvol,
                        volume=volume,
                        price=price,
                        is_halted=is_halted,
                        data_timestamp=data_timestamp,
                        # Extended data
                        float_shares=float_shares,
                        avg_volume=avg_volume,
                        short_pct=short_pct,
                        market_cap=market_cap,
                        # Enhanced for professional alignment
                        rvol_5m=rvol_5m,
                        halt_ts=halt_ts,
                        news_present=news_present,
                        # Scanner news indicator is AUTHORITATIVE
                        scanner_news_indicator=scanner_news_indicator,
                    )

                    if worklist_entry:
                        logger.info(
                            f"[WORKLIST] {symbol} added: score={worklist_entry.combined_priority_score:.1f}, "
                            f"gap={gap_pct:.1f}%, rvol={rvol:.1f}x, state={worklist_entry.momentum_state}"
                        )
                    else:
                        logger.info(f"[WORKLIST] {symbol} rejected by scrutiny")
                else:
                    logger.warning(f"[SCANNER] No price data for {symbol}, skipping worklist")

            # Only add to pipeline if passed worklist scrutiny (or if worklist not available)
            if worklist_entry or not self._worklist_available:
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

    async def _handle_scanner_heartbeat(self, active_count: int) -> None:
        """
        Handle scanner heartbeat - fires on EVERY successful poll.

        This keeps the scanner timestamp fresh even when no new symbols
        are discovered, preventing false "scanner stale" blocks.
        """
        # Update scanner health timestamp on every successful poll
        self.state.last_scanner_update = datetime.now(timezone.utc)
        self.state.scanner_health_ok = True

        # Log periodically (not every heartbeat to avoid log spam)
        # Only log if there are active symbols
        if active_count > 0:
            logger.debug(f"[SCANNER] Heartbeat: {active_count} active symbols")

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

            # Cache quote for enrichment fallback
            self.state.latest_quotes[quote.symbol] = {
                "symbol": quote.symbol,
                "bid": quote.bid_price,
                "ask": quote.ask_price,
                "last": price,
                "volume": quote.volume,
                "close": quote.close_price,
                "change_percent": (
                    ((price - quote.close_price) / quote.close_price * 100)
                    if quote.close_price and quote.close_price > 0 else 0
                ),
            }

            # Update symbol state for microstructure gates
            if self._symbol_state_manager:
                spread_pct = None
                if quote.bid_price and quote.ask_price and quote.bid_price > 0:
                    spread_pct = (quote.ask_price - quote.bid_price) / quote.bid_price
                self._symbol_state_manager.update_quote(
                    symbol=quote.symbol,
                    last_price=price,
                    bid=quote.bid_price,
                    ask=quote.ask_price,
                    spread_pct=spread_pct,
                )
                # Set prior close for SSR calculation
                if quote.close_price and quote.close_price > 0:
                    self._symbol_state_manager.set_prior_close(quote.symbol, quote.close_price)

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

    async def _token_reload_loop(self):
        """Background loop that reloads shared token from disk (Morpheus is the writer)."""
        logger.info("[TOKEN_RELOAD] Shared token reload daemon started (read-only, Morpheus owns refresh)")
        while True:
            try:
                await asyncio.sleep(60)  # Check every 60 seconds
                if self._auth:
                    try:
                        # Re-read token from shared file (Morpheus refreshes it)
                        old_token = self._auth._token
                        new_token = self._auth._load_token()
                        if new_token and old_token:
                            if new_token.access_token != old_token.access_token:
                                self._auth._token = new_token
                                logger.info(f"[TOKEN_RELOAD] Picked up refreshed token from shared file (expires in {new_token.seconds_until_expiry:.0f}s)")
                                # Update streamer with new access token
                                if self._streamer:
                                    self._streamer.update_access_token(new_token.access_token)
                                    logger.info("[TOKEN_RELOAD] Streamer token updated")
                        elif new_token and not old_token:
                            self._auth._token = new_token
                            logger.info("[TOKEN_RELOAD] Loaded initial token from shared file")
                    except Exception as e:
                        logger.error(f"[TOKEN_RELOAD] Reload failed: {e}")
            except asyncio.CancelledError:
                logger.info("[TOKEN_RELOAD] Shared token reload daemon stopped")
                break
            except Exception as e:
                logger.error(f"[TOKEN_RELOAD] Loop error: {e}")
                await asyncio.sleep(30)

    async def _daily_reset_loop(self):
        """Background loop that purges watchlist at 4:00 AM ET daily for fresh movers."""
        from morpheus.core.market_mode import ET as ET_TZ
        logger.info("[DAILY_RESET] Daily reset daemon started (purges watchlist at 4:00 AM ET)")
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute
                now_et = datetime.now(ET_TZ)

                # Check if it's 4:00 AM ET (morning reset time)
                if now_et.hour == 4 and now_et.minute == 0:
                    # Only reset once per day
                    today = now_et.date()
                    if self._last_daily_reset is None or self._last_daily_reset.date() != today:
                        logger.info("[DAILY_RESET] 4:00 AM ET - purging watchlist for fresh daily movers")
                        await self._purge_watchlist_for_new_day()
                        self._last_daily_reset = now_et
                        logger.info("[DAILY_RESET] Daily purge complete")
            except asyncio.CancelledError:
                logger.info("[DAILY_RESET] Daily reset daemon stopped")
                break
            except Exception as e:
                logger.error(f"[DAILY_RESET] Loop error: {e}")
                await asyncio.sleep(60)

    async def _purge_watchlist_for_new_day(self):
        """Clear watchlist, pipeline symbols, and streaming subscriptions for fresh start."""
        try:
            logger.info("[DAILY_RESET] Starting daily purge...")

            # Clear watchlist
            if self._watchlist:
                self._watchlist.reset()
                logger.info("[DAILY_RESET] Watchlist cleared")

            # Clear worklist pipeline (THIS WAS MISSING!)
            if self._worklist_available and self._worklist_pipeline:
                reset_worklist_pipeline()
                logger.info("[DAILY_RESET] Worklist pipeline reset")

            # Clear pipeline symbols
            if self._pipeline_available and self._pipeline:
                symbols_to_remove = list(self._pipeline._state.active_symbols)
                for sym in symbols_to_remove:
                    self._pipeline.remove_symbol(sym)
                logger.info(f"[DAILY_RESET] Removed {len(symbols_to_remove)} symbols from pipeline")

            # Clear subscribed symbols
            old_symbols = list(self.state.subscribed_symbols)
            self.state.subscribed_symbols.clear()
            logger.info(f"[DAILY_RESET] Cleared {len(old_symbols)} subscribed symbols")

            # Unsubscribe from streaming (if using streamer)
            if self._streamer_available and self._use_streaming and self._streamer:
                for sym in old_symbols:
                    try:
                        await self._streamer.unsubscribe_quotes([sym])
                    except Exception:
                        pass  # Ignore unsubscribe errors
                logger.info("[DAILY_RESET] Unsubscribed from streaming quotes")

            # Clear persistent watchlist file
            if os.path.exists(self.WATCHLIST_FILE):
                os.remove(self.WATCHLIST_FILE)
                logger.info("[DAILY_RESET] Persistent watchlist file removed")

            # Reset paper position manager daily stats
            if self._paper_position_manager:
                self._paper_position_manager.reset_daily()
                logger.info("[DAILY_RESET] Paper position manager daily stats reset")

            # Reset microstructure gates daily stats
            if self._symbol_state_manager:
                self._symbol_state_manager.reset_daily()
                logger.info("[DAILY_RESET] Symbol state manager daily stats reset")
            if self._microstructure_gates:
                self._microstructure_gates.reset_daily()
                logger.info("[DAILY_RESET] Microstructure gates daily stats reset")

            # Reset validation mode counters
            if is_validation_mode():
                reset_validation_counters()
                logger.info("[DAILY_RESET] Validation mode counters reset")

            # Emit event
            await self.emit_event(create_event(
                EventType.SYSTEM_STATUS,
                payload={"action": "daily_reset", "message": "Watchlist purged for new trading day"},
            ))

        except Exception as e:
            logger.error(f"[DAILY_RESET] Error during purge: {e}")

    async def start(self):
        """Start background tasks."""

        # ══════════════════════════════════════════════════════════════════
        # STARTUP PURGE: Clear stale data on first premarket startup of day
        # This ensures fresh session even if 4:00 AM purge was missed
        # ══════════════════════════════════════════════════════════════════
        from morpheus.core.market_mode import ET
        now_et = datetime.now(ET)
        today = now_et.date()

        # Check if we should purge on startup (premarket window: 4:00 AM - 9:30 AM ET)
        # Only purge if we haven't already done it today
        if 4 <= now_et.hour < 10:  # Premarket window
            if self._last_daily_reset is None or self._last_daily_reset.date() != today:
                logger.info(f"[STARTUP] Premarket startup at {now_et.strftime('%H:%M')} ET - purging stale data")
                await self._purge_watchlist_for_new_day()
                self._last_daily_reset = now_et
                logger.info("[STARTUP] Startup purge complete - fresh session ready")
            else:
                logger.info(f"[STARTUP] Already purged today at {self._last_daily_reset.strftime('%H:%M')} ET - skipping")

        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._trading_data_task = asyncio.create_task(self._trading_data_loop())

        # Start shared token reload daemon (read-only — Morpheus owns the refresh)
        if self._auth:
            self._token_refresh_task = asyncio.create_task(self._token_reload_loop())
            logger.info("[TOKEN_RELOAD] Shared token reload daemon started (Morpheus is the token owner)")

        # Start daily reset daemon (purges watchlist at 4:00 AM ET)
        self._daily_reset_task = asyncio.create_task(self._daily_reset_loop())
        logger.info("[DAILY_RESET] Daily reset daemon started (purges watchlist at 4:00 AM ET)")

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

        # Restore persistent watchlist (force-add symbols from previous session)
        await self._load_persistent_watchlist()

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

        if self._token_refresh_task:
            self._token_refresh_task.cancel()
            try:
                await self._token_refresh_task
            except asyncio.CancelledError:
                pass

        if self._daily_reset_task:
            self._daily_reset_task.cancel()
            try:
                await self._daily_reset_task
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
                                    "source": "SCHWAB_POLL",
                                },
                            ))

                            # Feed candle aggregator (same as streaming path)
                            price = quote["last"]
                            if self._candle_aggregator and price > 0:
                                await self._candle_aggregator.on_quote(
                                    symbol=symbol,
                                    price=price,
                                    volume=quote.get("volume", 0),
                                )

                            # Feed pipeline for real-time evaluation
                            if self._pipeline_available and self._pipeline and price > 0:
                                await self._pipeline.on_quote(
                                    symbol=symbol,
                                    last=price,
                                    bid=quote.get("bid"),
                                    ask=quote.get("ask"),
                                    volume=quote.get("volume"),
                                )

                            # Cache quote for enrichment fallback
                            self.state.latest_quotes[symbol] = quote

                            logger.debug(f"[DATA] QUOTE {symbol} ${quote['last']:.2f} ({quote['change_percent']:+.2f}%)")
                    except Exception as e:
                        logger.error(f"Error fetching quote for {symbol}: {e}")

            except asyncio.CancelledError:
                logger.info("Market data loop stopped")
                break
            except Exception as e:
                logger.error(f"Market data loop error: {e}")
                await asyncio.sleep(1.0)  # Back off on error

    async def emit_event(self, event: Event):
        """Emit an event to all WebSocket clients and persist to database + JSONL."""
        self.state.event_count += 1
        await self.ws_manager.broadcast(event)
        logger.debug(f"Event emitted: {event.event_type.value}")

        # Persist event to database (non-blocking)
        if self._persistence_available and self._persistence:
            try:
                await self._persistence.on_event(event)
            except Exception as e:
                logger.error(f"Error persisting event: {e}")

        # Persist event to JSONL via EventSink
        try:
            from morpheus.core.event_sink import get_event_sink
            get_event_sink().emit(event)
        except Exception as e:
            logger.error(f"Error writing event to JSONL sink: {e}")

        # Feed DecisionLogger for trade/signal/gating ledgers
        try:
            from morpheus.reporting.decision_logger import get_decision_logger
            get_decision_logger().on_event(event)
        except Exception as e:
            logger.error(f"Error in decision logger: {e}")

        # Feed PaperPositionManager for position tracking + exit management
        if self._paper_position_manager:
            try:
                await self._paper_position_manager.on_event(event)
            except Exception as e:
                logger.error(f"Error in paper position manager: {e}")

        # Track pipeline-related events for monitoring
        pipeline_event_types = {
            EventType.FEATURES_COMPUTED,
            EventType.REGIME_DETECTED,
            EventType.STRUCTURE_CLASSIFIED,
            EventType.STRATEGY_ASSIGNED,
            EventType.SIGNAL_CANDIDATE,
            EventType.SIGNAL_SCORED,
            EventType.SIGNAL_OBSERVED,
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

        # Auto-confirm RISK_APPROVED signals in PAPER mode (no human needed)
        if (
            event.event_type == EventType.RISK_APPROVED
            and self.state.trading_mode == TradingMode.PAPER
            and not self.state.kill_switch_active
        ):
            try:
                payload = event.payload or {}
                symbol = event.symbol or ""
                pos_size = payload.get("position_size", {})
                shares = pos_size.get("shares", 100)
                entry_price = float(pos_size.get("entry_price", 0))

                # Extract signal metadata for gate checks
                signal_data = (
                    payload.get("gate_result", {})
                    .get("scored_signal", {})
                    .get("signal", {})
                )
                direction = signal_data.get("direction", "long")
                strategy_name = signal_data.get("strategy_name", "unknown")

                # ══════════════════════════════════════════════════════════════
                # VALIDATION MODE: Hard safety constraints for correctness testing
                # ══════════════════════════════════════════════════════════════
                if is_validation_mode():
                    val_config = get_validation_config()
                    val_logger = get_validation_logger()
                    val_counters = get_validation_counters()

                    # Check 1: Strategy allowed? (phase-aware)
                    from morpheus.core.market_mode import get_market_phase, MarketPhase
                    current_phase = get_market_phase()
                    if current_phase == MarketPhase.PREMARKET:
                        phase_strategies = val_config.premarket_allowed_strategies
                    else:
                        phase_strategies = val_config.allowed_strategies
                    if strategy_name.lower() not in [s.lower() for s in phase_strategies]:
                        if val_logger:
                            val_logger.log_trade_blocked_strategy(symbol, strategy_name)
                        logger.warning(
                            f"[VALIDATION] BLOCKED: {symbol} strategy={strategy_name} "
                            f"not in {current_phase.value}_strategies={phase_strategies}"
                        )
                        return

                    # Check 2: Direction allowed?
                    if direction == "short" and not val_config.allow_shorts:
                        if val_logger:
                            val_logger.log_trade_blocked_strategy(symbol, f"short_{strategy_name}")
                        logger.warning(f"[VALIDATION] BLOCKED: {symbol} shorts disabled during validation")
                        return

                    # Check 3: Phase-scoped trade cap?
                    phase_name = current_phase.value
                    phase_cap = val_config.max_trades_per_phase.get(phase_name, 0)
                    phase_used = val_counters.trades_by_phase.get(phase_name, 0) if val_counters else 0
                    if val_counters and phase_used >= phase_cap:
                        val_counters.trades_blocked_phase_cap[phase_name] = (
                            val_counters.trades_blocked_phase_cap.get(phase_name, 0) + 1
                        )
                        logger.warning(
                            f"[VALIDATION] BLOCKED: {symbol} {phase_name} cap reached "
                            f"({phase_used}/{phase_cap})"
                        )
                        await self.emit_event(create_event(
                            EventType.EXECUTION_BLOCKED,
                            payload={
                                "symbol": symbol,
                                "block_reasons": ["VALIDATION_PHASE_CAP_REACHED"],
                                "phase": phase_name,
                                "cap": phase_cap,
                                "used": phase_used,
                            },
                            symbol=symbol,
                        ))
                        return

                    # Check 4: Position limit?
                    if self._paper_position_manager:
                        open_positions = len(self._paper_position_manager.get_positions_summary())
                        if open_positions >= val_config.max_concurrent_positions:
                            if val_logger:
                                val_logger.log_trade_blocked_position_limit(symbol, open_positions)
                            logger.warning(
                                f"[VALIDATION] BLOCKED: {symbol} max_concurrent_positions "
                                f"({val_config.max_concurrent_positions}) reached"
                            )
                            return

                    # Check 5: Notional exposure limit?
                    notional = shares * entry_price
                    if notional > val_config.max_notional_exposure:
                        original_shares = shares
                        shares = max(1, int(val_config.max_notional_exposure / entry_price))
                        logger.info(
                            f"[VALIDATION] {symbol} size reduced for notional limit: "
                            f"{original_shares} -> {shares} shares (${notional:.2f} -> ${shares * entry_price:.2f})"
                        )

                    # Check 6: Trading time window? (phase-aware)
                    from morpheus.core.market_mode import ET as ET_TZ
                    current_time = datetime.now(ET_TZ).time()
                    if current_phase == MarketPhase.PREMARKET:
                        window_start = val_config.premarket_start
                        window_end = val_config.premarket_end
                    else:
                        window_start = val_config.trading_start
                        window_end = val_config.trading_end
                    if not (window_start <= current_time <= window_end):
                        if val_logger:
                            val_logger.log_trade_blocked_time_window(symbol, str(current_time))
                        logger.warning(
                            f"[VALIDATION] BLOCKED: {symbol} outside {current_phase.value} window "
                            f"{window_start}-{window_end} (current={current_time})"
                        )
                        return

                # ══════════════════════════════════════════════════════════════
                # MICROSTRUCTURE GATES: Check halt/borrow/SSR/staleness FIRST
                # This prevents the "approve then block" pattern from EOD
                # ══════════════════════════════════════════════════════════════
                if self._microstructure_gates:
                    # Get current quote for spread check
                    quote = self.get_quote(symbol)
                    spread_pct = None
                    if quote:
                        bid = float(quote.get("bid", 0))
                        ask = float(quote.get("ask", 0))
                        if bid > 0 and ask > 0:
                            spread_pct = (ask - bid) / bid

                    # Run all microstructure gate checks
                    micro_result = await self._microstructure_gates.check_entry(
                        symbol=symbol,
                        direction=direction,
                        order_type="LIMIT",
                        time_in_force="DAY",
                        spread_pct=spread_pct,
                    )

                    if not micro_result.passed:
                        veto_reason = micro_result.veto_reason.value if micro_result.veto_reason else "MICROSTRUCTURE_BLOCK"
                        logger.warning(
                            f"[AUTO-TRADE] MICROSTRUCTURE BLOCKED: {symbol} - "
                            f"{veto_reason}: {micro_result.veto_details}"
                        )
                        await self.emit_event(create_event(
                            EventType.EXECUTION_BLOCKED,
                            payload={
                                "symbol": symbol,
                                "block_reasons": [veto_reason],
                                "details": micro_result.veto_details,
                                "gates_checked": micro_result.gates_checked,
                            },
                            symbol=symbol,
                        ))
                        return

                    # Apply any modifications from gates
                    if micro_result.final_size_multiplier < 1.0:
                        original_shares = shares
                        shares = max(1, int(shares * micro_result.final_size_multiplier))
                        logger.info(
                            f"[MICROSTRUCTURE] {symbol} size reduced: "
                            f"{original_shares} -> {shares} ({micro_result.final_size_multiplier:.0%})"
                        )

                # SCANNER HEALTH CHECK: No auto-trading if scanner is stale
                # This prevents trades when MAX_AI is down or stalled
                if self.state.last_scanner_update:
                    scanner_age = (datetime.now(timezone.utc) - self.state.last_scanner_update).total_seconds()
                    if scanner_age > self.state.max_scanner_stale_seconds:
                        self.state.scanner_health_ok = False
                        logger.warning(
                            f"[AUTO-TRADE] SCANNER STALE: {symbol} blocked - "
                            f"last update {scanner_age:.0f}s ago (max {self.state.max_scanner_stale_seconds}s)"
                        )
                        await self.emit_event(create_event(
                            EventType.EXECUTION_BLOCKED,
                            payload={
                                "symbol": symbol,
                                "block_reasons": ["SCANNER_STALE"],
                                "details": f"Scanner data is {scanner_age:.0f}s old, max allowed is {self.state.max_scanner_stale_seconds}s",
                            },
                            symbol=symbol,
                        ))
                        return
                else:
                    # No scanner data ever received - block trading
                    logger.warning(f"[AUTO-TRADE] SCANNER OFFLINE: {symbol} blocked - no scanner data received yet")
                    await self.emit_event(create_event(
                        EventType.EXECUTION_BLOCKED,
                        payload={
                            "symbol": symbol,
                            "block_reasons": ["SCANNER_OFFLINE"],
                            "details": "No scanner data received yet",
                        },
                        symbol=symbol,
                    ))
                    return

                # Block re-entry if already holding this symbol
                if self._paper_position_manager and self._paper_position_manager.has_position(symbol):
                    logger.info(f"[AUTO-TRADE] Blocked re-entry: already holding {symbol}")
                    return

                # WORKLIST CHECK: Trading MUST come from worklist only (Deliverable 5)
                # This is the SINGLE GATEKEEPER - no trades outside worklist
                if self._worklist_available and self._worklist_pipeline:
                    if not self._worklist_pipeline.is_tradeable(symbol):
                        logger.warning(f"[AUTO-TRADE] WORKLIST BLOCKED: {symbol} not in worklist or not tradeable")
                        await self.emit_event(create_event(
                            EventType.EXECUTION_BLOCKED,
                            payload={
                                "symbol": symbol,
                                "block_reasons": ["NOT_IN_WORKLIST"],
                                "details": "Symbol not in worklist or failed scrutiny/scoring",
                            },
                            symbol=symbol,
                        ))
                        return

                if symbol and entry_price > 0:
                    # CRITICAL SAFETY: Quote freshness check before order submission
                    # This prevents catastrophic losses from stale/cached quotes
                    quote = self.get_quote(symbol)
                    if quote:
                        quote_ts = datetime.fromisoformat(quote.get("timestamp", "").replace('Z', '+00:00')) if quote.get("timestamp") else datetime.now(timezone.utc)
                        snapshot = create_snapshot(
                            symbol=symbol,
                            bid=float(quote.get("bid", 0)),
                            ask=float(quote.get("ask", 0)),
                            last=float(quote.get("last", 0)),
                            volume=int(quote.get("volume", 0)),
                            timestamp=quote_ts,
                            is_market_open=quote.get("is_market_open", True),
                            is_tradeable=quote.get("is_tradeable", True),
                        )

                        # Run quote freshness check
                        freshness_check = self.quote_freshness_guard.check(
                            risk_result=None,  # We don't need full risk result for quote check
                            snapshot=snapshot,
                            config=None,  # Uses guard's internal config
                        )

                        if freshness_check.is_blocked:
                            block_reasons = [r.value for r in freshness_check.block_reasons]
                            logger.warning(
                                f"[AUTO-TRADE] QUOTE FRESHNESS BLOCKED: {symbol} - "
                                f"reasons={block_reasons}, details={freshness_check.reason_details}"
                            )
                            # Emit EXECUTION_BLOCKED event for logging/analysis
                            await self.emit_event(create_event(
                                EventType.EXECUTION_BLOCKED,
                                payload={
                                    "symbol": symbol,
                                    "block_reasons": block_reasons,
                                    "details": freshness_check.reason_details,
                                    "quote_age_ms": int((datetime.now(timezone.utc) - quote_ts).total_seconds() * 1000),
                                    "bid": snapshot.bid,
                                    "ask": snapshot.ask,
                                    "last": snapshot.last,
                                    "spread_pct": snapshot.spread_pct,
                                    "entry_price_attempted": entry_price,
                                },
                                symbol=symbol,
                            ))
                            return  # Do NOT submit order with bad quote data

                    # Extract signal metadata for exit plan validation
                    signal_data = (
                        payload.get("gate_result", {})
                        .get("scored_signal", {})
                        .get("signal", {})
                    )
                    strategy_name = signal_data.get("strategy_name", "unknown")
                    direction = signal_data.get("direction", "long")
                    invalidation_ref = signal_data.get("invalidation_reference")

                    # ══════════════════════════════════════════════════════════
                    # EXIT MANAGEMENT v1: VALIDATE EXIT PLAN BEFORE ORDER
                    # Core Rule: No trade without complete exit plan
                    # ══════════════════════════════════════════════════════════
                    if self._paper_position_manager:
                        exit_plan = self._paper_position_manager.validate_exit_plan(
                            symbol=symbol,
                            entry_price=entry_price,
                            direction=direction,
                            strategy_name=strategy_name,
                            invalidation_reference=invalidation_ref,
                        )

                        if exit_plan is None:
                            # REJECT: No valid exit plan
                            logger.warning(
                                f"[EXIT_MGR] TRADE REJECTED: {symbol} - no valid exit plan "
                                f"(entry=${entry_price:.4f} stop_ref={invalidation_ref} strategy={strategy_name})"
                            )
                            await self.emit_event(create_event(
                                EventType.TRADE_REJECTED_NO_EXIT_PLAN,
                                payload={
                                    "symbol": symbol,
                                    "entry_price": entry_price,
                                    "shares": shares,
                                    "direction": direction,
                                    "strategy_name": strategy_name,
                                    "invalidation_reference": invalidation_ref,
                                    "reason": "Exit plan validation failed - all three exit layers required",
                                },
                                symbol=symbol,
                            ))
                            return  # DO NOT TRADE

                        # Get microstructure flags for position
                        ssr_active = False
                        htb_active = False
                        disable_trailing = False
                        if self._symbol_state_manager:
                            sym_state = self._symbol_state_manager.get(symbol)
                            if sym_state:
                                ssr_active = sym_state.ssr_active
                                htb_active = sym_state.borrow_status == BorrowStatus.HTB
                        if self._microstructure_gates and 'micro_result' in dir():
                            disable_trailing = micro_result.disable_trailing

                        # Extract momentum intelligence from pipeline features
                        features_data = (
                            payload.get("gate_result", {})
                            .get("scored_signal", {})
                            .get("feature_snapshot", {})
                        )
                        entry_momentum_score = features_data.get("momentum_engine_score", 0.0)
                        entry_momentum_state = features_data.get("momentum_engine_state_str", "UNKNOWN")
                        entry_confidence_raw = features_data.get("momentum_engine_confidence", 0.0)

                        # Store validated exit plan for position manager
                        self._paper_position_manager.set_signal_metadata(symbol, {
                            "strategy_name": strategy_name,
                            "direction": direction,
                            "regime": signal_data.get("regime", ""),
                            "structure_grade": signal_data.get("structure_grade", ""),
                            # Exit plan parameters (validated)
                            "hard_stop_price": exit_plan.hard_stop_price,
                            "max_hold_seconds": exit_plan.max_hold_seconds,
                            "trail_activation_pct": exit_plan.trail_activation_pct,
                            "trail_distance_pct": exit_plan.trail_distance_pct,
                            # Partial profit-take parameters
                            "allow_partial_take": exit_plan.allow_partial_take,
                            "partial_take_pct": exit_plan.partial_take_pct,
                            "partial_trigger_pct": exit_plan.partial_trigger_pct,
                            # Microstructure flags
                            "ssr_active_at_entry": ssr_active,
                            "htb_at_entry": htb_active,
                            "disable_trailing": disable_trailing,
                            # Momentum intelligence
                            "entry_momentum_score": entry_momentum_score,
                            "entry_momentum_state": entry_momentum_state,
                            "entry_confidence": entry_confidence_raw,
                            "entry_reference": signal_data.get("entry_reference", entry_price),
                            "signal_timestamp": signal_data.get("timestamp", ""),
                        })

                        partial_str = f" partial={exit_plan.partial_take_pct:.0%}@{exit_plan.partial_trigger_pct:.1%}" if exit_plan.allow_partial_take else ""
                        logger.info(
                            f"[EXIT_MGR] EXIT PLAN OK: {symbol} "
                            f"HARD_STOP=${exit_plan.hard_stop_price:.4f} "
                            f"max_hold={exit_plan.max_hold_seconds:.0f}s "
                            f"trail={exit_plan.trail_activation_pct:.1%}/{exit_plan.trail_distance_pct:.1%}"
                            f"{partial_str}"
                        )

                        # Emit EXIT_PROFILE_APPLIED event (strategy-aware tuning audit)
                        await self.emit_event(create_event(
                            EventType.EXIT_PROFILE_APPLIED,
                            payload={
                                "symbol": symbol,
                                "strategy": strategy_name,
                                "max_hold": exit_plan.max_hold_seconds,
                                "trail_pct": exit_plan.trail_activation_pct,
                                "allow_partial_take": exit_plan.allow_partial_take,
                            },
                            symbol=symbol,
                        ))

                    logger.info(
                        f"[AUTO-TRADE] Paper auto-confirm: {symbol} "
                        f"{shares} shares @ ${entry_price:.2f} "
                        f"strategy={strategy_name}"
                    )

                    # Log trade execution for validation mode
                    if is_validation_mode():
                        val_logger = get_validation_logger()
                        if val_logger:
                            from morpheus.core.market_mode import get_market_phase
                            trade_phase = get_market_phase()
                            val_logger.log_trade_executed(symbol, strategy_name, shares, entry_price, phase=trade_phase.value)

                    asyncio.create_task(
                        self._simulate_paper_order_sized(
                            symbol, entry_price, shares,
                            correlation_id=f"auto_{uuid.uuid4().hex[:8]}",
                        )
                    )
            except Exception as e:
                logger.error(f"[AUTO-TRADE] Error auto-confirming {event.symbol}: {e}")

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

    async def _simulate_paper_order_sized(
        self, symbol: str, price: float, shares: int, correlation_id: str
    ):
        """Simulate paper order with actual position size from risk approval."""
        client_order_id = f"paper_{uuid.uuid4().hex[:12]}"

        # ORDER_SUBMITTED
        await self.emit_event(create_event(
            EventType.ORDER_SUBMITTED,
            payload={
                "client_order_id": client_order_id,
                "symbol": symbol,
                "side": "buy",
                "quantity": shares,
                "order_type": "market",
                "limit_price": None,
                "auto_confirmed": True,
            },
            symbol=symbol,
            correlation_id=correlation_id,
        ))

        await asyncio.sleep(0.1)

        # ORDER_FILL_RECEIVED
        await self.emit_event(create_event(
            EventType.ORDER_FILL_RECEIVED,
            payload={
                "client_order_id": client_order_id,
                "symbol": symbol,
                "side": "buy",
                "filled_quantity": shares,
                "fill_price": price,
                "exec_id": f"exec_{uuid.uuid4().hex[:8]}",
                "auto_confirmed": True,
            },
            symbol=symbol,
            correlation_id=correlation_id,
        ))

        logger.info(
            f"[AUTO-TRADE] Paper fill: {symbol} {shares} shares @ ${price:.2f} "
            f"(order={client_order_id})"
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

    # =========================================================================
    # PERSISTENT WATCHLIST
    # =========================================================================

    WATCHLIST_FILE = "data/persistent_watchlist.json"

    def _save_persistent_watchlist(self):
        """Save current pipeline symbols to disk for restart persistence."""
        try:
            symbols = []
            if self._pipeline_available and self._pipeline:
                symbols = list(self._pipeline._state.active_symbols)
            if not symbols:
                symbols = list(self.state.subscribed_symbols)
            data = {"symbols": sorted(symbols), "updated_at": datetime.now().isoformat()}
            os.makedirs(os.path.dirname(self.WATCHLIST_FILE), exist_ok=True)
            with open(self.WATCHLIST_FILE, "w") as f:
                json.dump(data, f, indent=2)
            logger.debug(f"[WATCHLIST] Saved {len(symbols)} symbols to {self.WATCHLIST_FILE}")
        except Exception as e:
            logger.error(f"[WATCHLIST] Error saving: {e}")

    async def _load_persistent_watchlist(self):
        """Reload symbols from persistent watchlist on startup."""
        try:
            if not os.path.exists(self.WATCHLIST_FILE):
                logger.info("[WATCHLIST] No persistent watchlist found")
                return
            with open(self.WATCHLIST_FILE) as f:
                data = json.load(f)
            symbols = data.get("symbols", [])
            if not symbols:
                return
            logger.info(f"[WATCHLIST] Restoring {len(symbols)} symbols from persistent watchlist")
            for sym in symbols:
                sym = sym.upper().strip()
                if not sym:
                    continue
                self.state.subscribed_symbols.add(sym)
                if self._streamer_available and self._use_streaming and self._streamer:
                    await self._streamer.subscribe_quotes([sym])
                if self._pipeline_available and self._pipeline:
                    self._pipeline.add_symbol(sym)
                    asyncio.create_task(self._warmup_pipeline_symbol(sym))
            logger.info(f"[WATCHLIST] Restored: {', '.join(symbols)}")
        except Exception as e:
            logger.error(f"[WATCHLIST] Error loading: {e}")

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
        # Acquire bot identity lock (Deliverable 4)
        if not acquire_bot_lock():
            logger.warning(f"[LIFESPAN] Could not acquire bot lock, continuing anyway")
        else:
            logger.info(f"[LIFESPAN] Bot identity: {BOT_ID} v{BOT_VERSION}")

        await server.start()
        yield
        await server.stop()

        # Release bot lock on shutdown
        release_bot_lock()

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

    @app.get("/api/bot/identity")
    async def bot_identity():
        """
        Bot Identity endpoint (Deliverable 4).

        Returns bot identity info to prevent accidental shutdown
        of wrong bot when multiple trading systems are running.
        """
        lock_info = get_running_bot_info()
        return {
            "bot_id": BOT_ID,
            "bot_name": BOT_NAME,
            "bot_version": BOT_VERSION,
            "lock_info": lock_info,
            "verified": verify_bot_identity(BOT_ID),
        }

    @app.get("/api/bot/verify/{expected_id}")
    async def verify_bot(expected_id: str):
        """
        Verify bot identity before shutdown/control operations.

        Use this endpoint to ensure you're controlling the right bot.
        """
        is_match = verify_bot_identity(expected_id)
        return {
            "expected": expected_id,
            "actual": BOT_ID,
            "verified": is_match,
            "message": f"Bot identity {'VERIFIED' if is_match else 'MISMATCH'}",
        }

    # =========================================================================
    # HOT RELOAD CONFIG (Deliverable 5)
    # =========================================================================

    @app.get("/api/config")
    async def get_config():
        """Get current runtime configuration."""
        config = get_runtime_config()
        return config.to_dict()

    @app.post("/api/config/reload")
    async def reload_config():
        """
        Hot reload configuration from disk.

        Edit data/runtime_config.json and call this endpoint
        to apply changes without restart.
        """
        success, message = reload_runtime_config()
        if success:
            logger.info(f"[CONFIG_RELOAD] {message}")
            return {"success": True, "message": message}
        else:
            logger.error(f"[CONFIG_RELOAD] {message}")
            raise HTTPException(status_code=400, detail=message)

    @app.post("/api/config/update")
    async def update_config(updates: dict):
        """
        Update specific config values and save to disk.

        Example:
            POST /api/config/update
            {"paper_equity": 1000, "risk_per_trade_pct": 0.01}
        """
        success, message = update_runtime_config(updates)
        if success:
            logger.info(f"[CONFIG_UPDATE] {message}")
            return {"success": True, "message": message, "config": get_runtime_config().to_dict()}
        else:
            logger.error(f"[CONFIG_UPDATE] {message}")
            raise HTTPException(status_code=400, detail=message)

    @app.get("/api/time/status")
    async def time_authority_status():
        """Market Time Authority - single source of truth for all time."""
        try:
            from morpheus.core.time_authority import get_time_authority
            return get_time_authority().get_status()
        except ImportError:
            return {"error": "TimeAuthority not available", "fallback": "system_clock"}

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

    # =========================================================================
    # Exit Management v1 API Endpoints
    # =========================================================================

    @app.get("/api/exit/status")
    async def get_exit_status():
        """
        Get Exit Management v1 status - positions, exits, config.

        Exit v1 ensures every trade has:
        1. TIME_STOP - mandatory failsafe
        2. HARD_STOP - risk definition
        3. TRAIL_STOP - profit protection
        """
        if not server._paper_position_manager:
            return {"enabled": False, "message": "Exit management not available"}

        pm = server._paper_position_manager
        config = pm._get_exit_config()

        return {
            "enabled": True,
            "version": "v1",
            "positions": pm.get_positions_summary(),
            "daily_stats": pm.get_daily_stats(),
            "config": {
                "max_hold_seconds": config.max_hold_seconds,
                "hard_stop_pct": config.hard_stop_pct,
                "trail_activation_pct": config.trail_activation_pct,
                "trail_distance_pct": config.trail_distance_pct,
                "strategy_max_hold": config.strategy_max_hold,
            },
        }

    @app.get("/api/exit/positions")
    async def get_exit_positions():
        """Get all open positions with exit plan details."""
        if not server._paper_position_manager:
            return {"positions": [], "count": 0}

        pm = server._paper_position_manager
        positions = pm.get_positions_summary()
        return {
            "positions": positions,
            "count": len(positions),
        }

    @app.get("/api/exit/closed")
    async def get_closed_trades():
        """Get all closed trades for today with exit reasons."""
        if not server._paper_position_manager:
            return {"trades": [], "count": 0}

        pm = server._paper_position_manager
        trades = pm.get_closed_trades()
        stats = pm.get_daily_stats()
        return {
            "trades": trades,
            "count": len(trades),
            "exit_counts": stats.get("exit_counts", {}),
            "realized_pnl": stats.get("realized_pnl", 0),
            "win_rate": stats.get("win_rate", 0),
        }

    @app.post("/api/exit/config")
    async def update_exit_config(
        max_hold_seconds: float | None = None,
        hard_stop_pct: float | None = None,
        trail_activation_pct: float | None = None,
        trail_distance_pct: float | None = None,
    ):
        """
        Update exit config (hot-reload).

        Changes take effect on next trade.
        """
        from morpheus.core.runtime_config import get_runtime_config, update_runtime_config

        updates = {}
        if max_hold_seconds is not None:
            updates["max_hold_seconds"] = max_hold_seconds
        if hard_stop_pct is not None:
            updates["hard_stop_pct"] = hard_stop_pct
        if trail_activation_pct is not None:
            updates["trail_activation_pct"] = trail_activation_pct
        if trail_distance_pct is not None:
            updates["trail_distance_pct"] = trail_distance_pct

        if updates:
            update_runtime_config(**updates)
            logger.info(f"[EXIT_MGR] Config updated: {updates}")

        config = get_runtime_config()
        return {
            "success": True,
            "config": {
                "max_hold_seconds": config.max_hold_seconds,
                "hard_stop_pct": config.hard_stop_pct,
                "trail_activation_pct": config.trail_activation_pct,
                "trail_distance_pct": config.trail_distance_pct,
            },
        }

    # ========================================================================
    # Momentum Intelligence API
    # ========================================================================

    @app.get("/api/momentum/status")
    async def get_momentum_status():
        """Get momentum engine status and all tracked symbols."""
        if not server._momentum_engine:
            return {"enabled": False, "message": "Momentum engine not available"}
        return {
            "enabled": True,
            **server._momentum_engine.status(),
        }

    @app.get("/api/momentum/{symbol}")
    async def get_momentum_symbol(symbol: str):
        """Get detailed momentum snapshot for a specific symbol."""
        if not server._momentum_engine:
            return {"error": "Momentum engine not available"}
        snap = server._momentum_engine.get_snapshot(symbol.upper())
        if snap is None:
            return {"symbol": symbol.upper(), "status": "no_data"}
        return snap.to_dict()

    # ========================================================================
    # Microstructure Gates API
    # ========================================================================

    @app.get("/api/microstructure/stats")
    async def get_microstructure_stats():
        """Get microstructure gate statistics."""
        if not server._microstructure_gates:
            return {"error": "Microstructure gates not available"}

        stats = server._microstructure_gates.get_stats()
        segments = server._microstructure_gates.get_eod_segments()
        return {
            "stats": stats,
            "segments": segments,
        }

    @app.get("/api/microstructure/symbols")
    async def get_microstructure_symbols():
        """Get symbols by microstructure status."""
        if not server._symbol_state_manager:
            return {"error": "Symbol state manager not available"}

        return {
            "ssr": server._symbol_state_manager.get_ssr_symbols(),
            "htb": server._symbol_state_manager.get_htb_symbols(),
            "halted": server._symbol_state_manager.get_halted_symbols(),
            "stale": server._symbol_state_manager.get_stale_symbols(),
        }

    @app.get("/api/microstructure/symbol/{symbol}")
    async def get_symbol_state(symbol: str):
        """Get microstructure state for a specific symbol."""
        if not server._symbol_state_manager:
            return {"error": "Symbol state manager not available"}

        state = server._symbol_state_manager.get(symbol.upper())
        if not state:
            return {"error": f"No state for {symbol}"}

        return state.to_dict()

    @app.post("/api/microstructure/borrow/{symbol}")
    async def set_borrow_status(symbol: str, status: str):
        """Manually set borrow status for a symbol (ETB, HTB, NTB)."""
        if not server._symbol_state_manager:
            return {"error": "Symbol state manager not available"}

        status = status.upper()
        if status not in ("ETB", "HTB", "NTB"):
            return {"error": f"Invalid status. Use: ETB, HTB, NTB"}

        from morpheus.core.symbol_state import BorrowStatus, BorrowSource
        borrow_status = BorrowStatus[status]
        server._symbol_state_manager.update_borrow_status(
            symbol.upper(), borrow_status, BorrowSource.MANUAL
        )
        logger.info(f"[MICROSTRUCTURE] Manual borrow status: {symbol} -> {status}")
        return {"success": True, "symbol": symbol.upper(), "borrow_status": status}

    @app.post("/api/microstructure/halt/{symbol}")
    async def set_halt_status(symbol: str, halted: bool, halt_code: str | None = None):
        """Manually set halt status for a symbol."""
        if not server._symbol_state_manager:
            return {"error": "Symbol state manager not available"}

        server._symbol_state_manager.set_halt(symbol.upper(), halted, halt_code)
        logger.info(f"[MICROSTRUCTURE] Manual halt status: {symbol} -> halted={halted}")
        return {"success": True, "symbol": symbol.upper(), "halted": halted, "halt_code": halt_code}

    # ═══════════════════════════════════════════════════════════════════════
    # Validation Mode Endpoints
    # ═══════════════════════════════════════════════════════════════════════

    @app.get("/api/validation/status")
    async def get_validation_status():
        """Get validation mode status and counters."""
        if not is_validation_mode():
            return {"validation_mode": False, "message": "Validation mode is disabled"}

        config = get_validation_config()
        summary = get_validation_summary()
        return {
            "validation_mode": True,
            "config": {
                "max_concurrent_positions": config.max_concurrent_positions,
                "max_notional_exposure": config.max_notional_exposure,
                "max_trades_per_phase": config.max_trades_per_phase,
                "allowed_strategies": config.allowed_strategies,
                "premarket_allowed_strategies": config.premarket_allowed_strategies,
                "allow_shorts": config.allow_shorts,
                "rth_window": f"{config.trading_start}-{config.trading_end} ET",
                "premarket_window": f"{config.premarket_start}-{config.premarket_end} ET",
            },
            "summary": summary,
        }

    @app.post("/api/validation/reset")
    async def reset_validation():
        """Reset validation counters."""
        if not is_validation_mode():
            return {"error": "Validation mode is disabled"}

        reset_validation_counters()
        logger.info("[VALIDATION] Counters manually reset via API")
        return {"success": True, "message": "Validation counters reset"}

    @app.post("/api/mass/regime/override")
    async def override_regime_mode(mode: str = "HOT"):
        """
        Override MASS regime mode to allow trading.

        Modes: HOT (5 pos), NORMAL (3 pos), CHOP (2 pos), DEAD (0 pos)
        """
        valid_modes = {"HOT", "NORMAL", "CHOP", "DEAD"}
        mode = mode.upper()
        if mode not in valid_modes:
            return {"error": f"Invalid mode. Valid: {valid_modes}"}

        if not server._pipeline_available or not server._pipeline:
            return {"error": "Pipeline not available"}

        pipeline = server._pipeline
        # Set on risk manager
        if hasattr(pipeline, "_risk_manager") and hasattr(pipeline._risk_manager, "set_regime_mode"):
            pipeline._risk_manager.set_regime_mode(mode)
            logger.info(f"[REGIME_OVERRIDE] Set regime mode to {mode}")
            return {"success": True, "mode": mode, "message": f"Regime overridden to {mode}"}

        return {"error": "Risk manager not available"}

    @app.post("/api/market/mode/override")
    async def override_market_mode(active_trading: bool = True):
        """
        Override market mode to enable/disable active trading.

        For paper trading during RTH when we want signals to be actionable.
        """
        from morpheus.core import market_mode

        # Patch the RTH mode at runtime
        market_mode.RTH = market_mode.MarketMode(
            name="RTH",
            active_trading=active_trading,
            observe_only=not active_trading
        )

        logger.info(f"[MARKET_MODE_OVERRIDE] RTH active_trading={active_trading}")
        return {
            "success": True,
            "active_trading": active_trading,
            "message": f"RTH mode now {'ACTIVE' if active_trading else 'OBSERVE-ONLY'}"
        }

    # =========================================================================
    # Worklist API Endpoints (Deliverable 7 - Observability)
    # =========================================================================

    @app.get("/api/worklist/status")
    async def get_worklist_status():
        """
        Get worklist pipeline status and statistics.

        Returns comprehensive observability data:
        - Worklist size and symbol counts
        - Scrutiny pass/reject rates
        - Scoring breakdown
        - Session info
        - Scanner health status
        """
        if not server._worklist_available or not server._worklist_pipeline:
            return {
                "enabled": False,
                "message": "Worklist pipeline not available",
            }

        status = server._worklist_pipeline.get_status()

        # Add scanner health info for operator visibility
        scanner_age_seconds = None
        if server.state.last_scanner_update:
            scanner_age_seconds = (datetime.now(timezone.utc) - server.state.last_scanner_update).total_seconds()

        return {
            "enabled": True,
            "scanner_health": {
                "ok": server.state.scanner_health_ok,
                "last_update": server.state.last_scanner_update.isoformat() if server.state.last_scanner_update else None,
                "age_seconds": scanner_age_seconds,
                "max_stale_seconds": server.state.max_scanner_stale_seconds,
                "auto_trade_permitted": scanner_age_seconds is not None and scanner_age_seconds < server.state.max_scanner_stale_seconds,
            },
            **status,
        }

    @app.get("/api/worklist/candidates")
    async def get_worklist_candidates(n: int = 10, min_score: float = 50.0):
        """
        Get top N trading candidates from worklist.

        Enhanced to mirror professional scanner cognitive model:
        - Momentum state classification (RUNNING_UP, SQUEEZE, HALTED, etc.)
        - Trigger reason explaining WHY this symbol is interesting
        - RVOL velocity (5m vs daily)
        - Float, short interest, halt status

        Args:
            n: Number of candidates to return (default 10)
            min_score: Minimum combined score (default 50.0)
        """
        if not server._worklist_available or not server._worklist_pipeline:
            return {"candidates": [], "error": "Worklist not available"}

        candidates = server._worklist_pipeline.get_top_candidates(n=n, min_score=min_score)

        # Build summary by momentum state for quick UI categorization
        state_summary = {}
        for c in candidates:
            state = c.momentum_state or "unknown"
            if state not in state_summary:
                state_summary[state] = []
            state_summary[state].append(c.symbol)

        return {
            "candidates": [c.to_dict() for c in candidates],
            "count": len(candidates),
            "min_score": min_score,
            # Group by momentum state for UI categorization
            "by_state": state_summary,
            # Quick stats
            "halted_count": len([c for c in candidates if c.halt_status == "HALTED"]),
            "squeeze_count": len([c for c in candidates if c.momentum_state == "squeeze"]),
            "running_up_count": len([c for c in candidates if c.momentum_state == "running_up"]),
        }

    @app.get("/api/worklist/symbol/{symbol}")
    async def get_worklist_symbol(symbol: str):
        """
        Get worklist entry for a specific symbol with score explanation.

        Returns full score breakdown for operator visibility,
        including all boost factors (RVOL velocity, low float, squeeze, alerts).
        """
        if not server._worklist_available or not server._worklist_pipeline:
            return {"error": "Worklist not available"}

        entry = server._worklist_pipeline._store.get(symbol.upper())
        if not entry:
            return {"found": False, "symbol": symbol.upper()}

        # Get score explanation with all enhanced parameters
        score_explanation = server._worklist_pipeline._scorer.explain_score(
            scanner_score=entry.scanner_score,
            gap_pct=entry.gap_pct,
            rvol=entry.rvol,
            volume=entry.volume,
            news_score=entry.news_score,
            # Enhanced parameters
            rvol_5m=entry.rvol_5m,
            float_shares=entry.float_shares,
            short_pct=entry.short_pct,
            halt_status=entry.halt_status,
            alert_count=entry.alert_count,
            # Scanner news and category
            scanner_news_indicator=entry.scanner_news_indicator,
            news_category=entry.news_category,
        )

        return {
            "found": True,
            "entry": entry.to_dict(),
            "is_tradeable": server._worklist_pipeline.is_tradeable(symbol.upper()),
            "score_explanation": score_explanation,
            # Quick summary for UI
            "momentum_state": entry.momentum_state,
            "trigger_context": entry.trigger_context,
            "trigger_reason": entry.trigger_reason,
            "news_hits": entry.news_hits,
        }

    @app.get("/api/worklist/tradeable/{symbol}")
    async def check_tradeable(symbol: str):
        """
        Check if a symbol is eligible for trading.

        CRITICAL: This is the authoritative check before any trade.
        """
        if not server._worklist_available or not server._worklist_pipeline:
            return {"tradeable": False, "reason": "Worklist not available"}

        is_tradeable = server._worklist_pipeline.is_tradeable(symbol.upper())
        return {
            "symbol": symbol.upper(),
            "tradeable": is_tradeable,
            "reason": "passed_scrutiny" if is_tradeable else "not_in_worklist_or_failed_checks",
        }

    @app.post("/api/worklist/reset")
    async def reset_worklist_session():
        """
        Reset worklist for new session.

        Archives current worklist and creates fresh one for today.
        """
        if not server._worklist_available or not server._worklist_pipeline:
            return {"error": "Worklist not available"}

        summary = await server._worklist_pipeline.reset_session()
        return {
            "success": True,
            **summary,
        }

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

    @app.get("/api/scanner/health")
    async def get_scanner_health():
        """
        MAX_AI Scanner Health Contract (Deliverable 6).

        Comprehensive health check for scanner integration.
        Use this to verify scanner is working before relying on it.
        """
        health = {
            "scanner_available": server._scanner_available,
            "scanner_healthy": False,
            "integration_running": False,
            "symbols_discovered": 0,
            "halts_tracked": 0,
            "last_error": None,
            "contract": {
                "source": "MAX_AI_SCANNER",
                "url": "http://127.0.0.1:8787",
                "role": "SYMBOL_DISCOVERY_ONLY",
                "overlap_allowed": False,
            },
        }

        if hasattr(server, '_scanner_integration') and server._scanner_integration:
            integration = server._scanner_integration
            health["integration_running"] = integration.is_healthy()
            status = integration.get_status()
            health["scanner_healthy"] = status.get("scanner_healthy", False)
            health["symbols_discovered"] = len(status.get("active_symbols", []))
            health["halts_tracked"] = len(status.get("known_halts", []))
            health["config"] = status.get("config", {})
            health["active_symbols"] = status.get("active_symbols", [])

        # Overall health assessment
        health["overall_status"] = (
            "HEALTHY" if health["scanner_healthy"] and health["integration_running"]
            else "DEGRADED" if health["scanner_available"]
            else "UNAVAILABLE"
        )

        return health

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

    @app.post("/api/scanner/force-add/{symbol}")
    async def force_add_symbol(symbol: str, reason: str = "manual_force"):
        """Force-add a symbol directly to pipeline, bypassing watchlist lifecycle gates."""
        symbol = symbol.upper().strip()
        try:
            # Subscribe to streaming quotes
            server.state.subscribed_symbols.add(symbol)
            if server._streamer_available and server._use_streaming and server._streamer:
                await server._streamer.subscribe_quotes([symbol])

            # Add directly to pipeline
            if server._pipeline_available and server._pipeline:
                server._pipeline.add_symbol(symbol)
                asyncio.create_task(server._warmup_pipeline_symbol(symbol))

            # Also add to worklist as ACTIVE with high score so trades can execute
            if server._worklist_available and server._worklist_pipeline:
                from morpheus.worklist.store import WorklistEntry
                now_iso = datetime.now(timezone.utc).isoformat()
                today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                entry = WorklistEntry(
                    symbol=symbol,
                    session_date=today_str,
                    first_seen_ts=now_iso,
                    last_update_ts=now_iso,
                    source="manual",
                    combined_priority_score=100.0,  # Max score for manual force-add
                    status="active",
                )
                server._worklist_pipeline._store.add(entry)
                logger.info(f"[FORCE-ADD] {symbol} added to worklist as ACTIVE (score=100)")

            logger.info(f"[FORCE-ADD] {symbol} added directly to pipeline ({reason})")
            server._save_persistent_watchlist()
            return {"success": True, "symbol": symbol, "reason": reason, "pipeline": True}
        except Exception as e:
            logger.error(f"Force-add error for {symbol}: {e}")
            return {"success": False, "symbol": symbol, "error": str(e)}

    @app.post("/api/scanner/force-add-batch")
    async def force_add_batch(symbols: list[str], reason: str = "manual_force"):
        """Force-add multiple symbols directly to pipeline."""
        results = []
        for sym in symbols:
            sym = sym.upper().strip()
            if not sym:
                continue
            try:
                server.state.subscribed_symbols.add(sym)
                if server._streamer_available and server._use_streaming and server._streamer:
                    await server._streamer.subscribe_quotes([sym])
                if server._pipeline_available and server._pipeline:
                    server._pipeline.add_symbol(sym)
                    asyncio.create_task(server._warmup_pipeline_symbol(sym))
                results.append({"symbol": sym, "success": True})
            except Exception as e:
                results.append({"symbol": sym, "success": False, "error": str(e)})

        added = [r["symbol"] for r in results if r["success"]]
        logger.info(f"[FORCE-ADD-BATCH] Added {len(added)} symbols to pipeline ({reason})")
        if added:
            server._save_persistent_watchlist()
        return {"added": len(added), "symbols": added, "results": results}

    # ========================================================================
    # EOD Reports & Decision Logging
    # ========================================================================

    @app.post("/api/reports/eod/generate")
    async def generate_eod_report(report_date: str | None = None):
        """
        Generate EOD report from today's JSONL ledgers.

        Aggregates signal_ledger, trade_ledger, and gating_blocks into
        an IBKR-comparable EOD summary. Saves to reports/{date}/.
        """
        from morpheus.reporting.decision_logger import get_decision_logger
        from datetime import date as date_type
        import json as json_mod

        try:
            if report_date:
                target_date = date_type.fromisoformat(report_date)
            else:
                target_date = date_type.today()

            date_str = target_date.isoformat()
            dl = get_decision_logger()
            day_dir = dl.get_reports_dir() / date_str

            # Read ledgers
            signals = _read_jsonl(day_dir / "signal_ledger.jsonl")
            trades = _read_jsonl(day_dir / "trade_ledger.jsonl")
            blocks = _read_jsonl(day_dir / "gating_blocks.jsonl")

            # Filter: only SIGNAL_CANDIDATE entries (not decision updates)
            signal_candidates = [s for s in signals if "approved" in s]
            approved_signals = [s for s in signal_candidates if s.get("approved")]
            rejected_from_signals = [s for s in signal_candidates if not s.get("approved") and not s.get("observed")]

            # Trade stats
            closed_trades = [t for t in trades if t.get("status") == "closed" or "pnl" in t]
            wins = [t for t in closed_trades if (t.get("pnl") or 0) > 0]
            losses = [t for t in closed_trades if (t.get("pnl") or 0) < 0]
            pnls = [t.get("pnl", 0) for t in closed_trades]
            total_pnl = sum(pnls)
            win_rate = len(wins) / len(closed_trades) if closed_trades else 0

            # Profit factor
            gross_profit = sum(p for p in pnls if p > 0)
            gross_loss = abs(sum(p for p in pnls if p < 0))
            profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (float("inf") if gross_profit > 0 else 0)

            # Gating breakdown
            gating_by_reason: dict[str, int] = {}
            gating_by_stage: dict[str, int] = {}
            for b in blocks:
                reason = b.get("reason", "unknown")
                stage = b.get("stage", "unknown")
                gating_by_reason[reason] = gating_by_reason.get(reason, 0) + 1
                gating_by_stage[stage] = gating_by_stage.get(stage, 0) + 1

            # Strategy breakdown
            strategy_stats: dict[str, dict] = {}
            for t in closed_trades:
                strat = t.get("entry_signal", "unknown")
                if strat not in strategy_stats:
                    strategy_stats[strat] = {"trades": 0, "wins": 0, "losses": 0, "pnl": 0}
                strategy_stats[strat]["trades"] += 1
                strategy_stats[strat]["pnl"] += t.get("pnl", 0)
                if (t.get("pnl") or 0) > 0:
                    strategy_stats[strat]["wins"] += 1
                else:
                    strategy_stats[strat]["losses"] += 1

            # Active symbols
            all_symbols = set()
            for s in signal_candidates:
                sym = s.get("symbol", "")
                if sym:
                    all_symbols.add(sym)

            # Build report
            report = {
                "date": date_str,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "summary": {
                    "signals_detected": len(signal_candidates),
                    "signals_approved": len(approved_signals),
                    "signals_rejected": len(rejected_from_signals) + len(blocks),
                    "trades_executed": len(closed_trades),
                    "wins": len(wins),
                    "losses": len(losses),
                    "win_rate": round(win_rate, 4),
                    "total_pnl": round(total_pnl, 2),
                    "profit_factor": round(profit_factor, 2) if profit_factor != float("inf") else "inf",
                },
                "strategies": strategy_stats,
                "gating_breakdown": {
                    "by_reason": gating_by_reason,
                    "by_stage": gating_by_stage,
                    "total_blocks": len(blocks),
                },
                "market_context": {
                    "active_symbols": sorted(all_symbols),
                    "total_symbols_tracked": len(all_symbols),
                },
                "trade_details": closed_trades,
                "non_trades": blocks[:100],  # Cap at 100 for readability
            }

            # Save JSON
            day_dir.mkdir(parents=True, exist_ok=True)
            json_path = day_dir / f"eod_{date_str}.json"
            with open(json_path, "w", encoding="utf-8") as f:
                json_mod.dump(report, f, indent=2, default=str)

            # Save Markdown
            md_path = day_dir / f"eod_{date_str}.md"
            md_content = _generate_eod_markdown(report)
            with open(md_path, "w", encoding="utf-8") as f:
                f.write(md_content)

            logger.info(f"[EOD] Report generated: {json_path}")
            return {
                "success": True,
                "date": date_str,
                "json_path": str(json_path),
                "md_path": str(md_path),
                "report": report,
            }

        except Exception as e:
            logger.error(f"EOD report generation failed: {e}")
            return {"success": False, "error": str(e)}

    @app.get("/api/reports/eod/{report_date}")
    async def get_eod_report(report_date: str):
        """Retrieve a saved EOD report by date."""
        import json as json_mod
        from morpheus.reporting.decision_logger import get_decision_logger

        dl = get_decision_logger()
        json_path = dl.get_reports_dir() / report_date / f"eod_{report_date}.json"

        if not json_path.exists():
            raise HTTPException(status_code=404, detail=f"No EOD report for {report_date}")

        with open(json_path, "r", encoding="utf-8") as f:
            report = json_mod.load(f)

        return report

    @app.get("/api/reports/ledger/{report_date}/{ledger_type}")
    async def get_ledger(report_date: str, ledger_type: str):
        """
        Retrieve a raw JSONL ledger by date and type.

        ledger_type: 'signals', 'trades', 'blocks'
        """
        from morpheus.reporting.decision_logger import get_decision_logger

        type_map = {
            "signals": "signal_ledger.jsonl",
            "trades": "trade_ledger.jsonl",
            "blocks": "gating_blocks.jsonl",
        }

        if ledger_type not in type_map:
            raise HTTPException(status_code=400, detail=f"Invalid ledger type. Use: {list(type_map.keys())}")

        dl = get_decision_logger()
        file_path = dl.get_reports_dir() / report_date / type_map[ledger_type]

        if not file_path.exists():
            return {"date": report_date, "ledger": ledger_type, "records": [], "count": 0}

        records = _read_jsonl(file_path)
        return {"date": report_date, "ledger": ledger_type, "records": records, "count": len(records)}

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

    @app.post("/api/watchlist/purge")
    async def purge_watchlist():
        """Manually trigger daily watchlist purge (for fresh start)."""
        try:
            await server._purge_watchlist_for_new_day()
            return {"success": True, "message": "Watchlist purged for fresh daily movers"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # ========================================================================
    # Reporting Parity Stubs (IBKR interface — not yet implemented)
    # ========================================================================

    @app.get("/api/momentum/snapshot/{symbol}")
    async def momentum_snapshot_stub(symbol: str):
        """IBKR-parity stub: detailed momentum snapshot with components."""
        return {"status": "not_implemented", "symbol": symbol}

    @app.get("/api/execution/status")
    async def execution_status_stub():
        """IBKR-parity stub: ExecutionManager status."""
        return {"status": "not_implemented", "note": "ExecutionManager not yet ported"}

    @app.get("/api/execution/records")
    async def execution_records_stub():
        """IBKR-parity stub: execution records with latency/slippage."""
        return {"status": "not_implemented"}

    @app.get("/api/execution/latency")
    async def execution_latency_stub():
        """IBKR-parity stub: execution latency percentiles."""
        return {"status": "not_implemented"}

    @app.get("/api/variance/status")
    async def variance_status_stub():
        """IBKR-parity stub: VarianceAnalyzer status."""
        return {"status": "not_implemented", "note": "VarianceAnalyzer not yet ported"}

    @app.get("/api/variance/top-failures")
    async def variance_top_failures_stub():
        """IBKR-parity stub: top variance failures."""
        return {"status": "not_implemented"}

    @app.get("/api/reports/eod/today")
    async def reports_eod_today_stub():
        """IBKR-parity stub: today's EOD report (structured 12-section format)."""
        return {"status": "not_implemented"}

    @app.get("/api/reports/weekly")
    async def reports_weekly_stub():
        """IBKR-parity stub: weekly report."""
        return {"status": "not_implemented", "note": "WeeklyReportGenerator not yet ported"}

    @app.get("/api/reports/weekly/{end_date}")
    async def reports_weekly_date_stub(end_date: str):
        """IBKR-parity stub: weekly report for specific end date."""
        return {"status": "not_implemented", "end_date": end_date}

    return app


# ============================================================================
# Helper functions for EOD report generation
# ============================================================================


def _read_jsonl(file_path: Path) -> list[dict]:
    """Read a JSONL file and return list of dicts."""
    import json as json_mod
    records = []
    if not file_path.exists():
        return records
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json_mod.loads(line))
                except Exception:
                    pass
    return records


def _generate_eod_markdown(report: dict) -> str:
    """Generate a human-readable markdown EOD report."""
    s = report.get("summary", {})
    date_str = report.get("date", "unknown")

    lines = [
        f"# Morpheus EOD Report - {date_str}",
        f"Generated: {report.get('generated_at', '')}",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Signals Detected | {s.get('signals_detected', 0)} |",
        f"| Signals Approved | {s.get('signals_approved', 0)} |",
        f"| Signals Rejected | {s.get('signals_rejected', 0)} |",
        f"| Trades Executed | {s.get('trades_executed', 0)} |",
        f"| Wins | {s.get('wins', 0)} |",
        f"| Losses | {s.get('losses', 0)} |",
        f"| Win Rate | {s.get('win_rate', 0):.1%} |",
        f"| Total P&L | ${s.get('total_pnl', 0):+.2f} |",
        f"| Profit Factor | {s.get('profit_factor', 0)} |",
        "",
    ]

    # Strategy breakdown
    strategies = report.get("strategies", {})
    if strategies:
        lines.extend([
            "## Strategy Breakdown",
            "",
            "| Strategy | Trades | Wins | Losses | P&L |",
            "|----------|--------|------|--------|-----|",
        ])
        for strat, stats in strategies.items():
            lines.append(
                f"| {strat} | {stats.get('trades', 0)} | {stats.get('wins', 0)} | "
                f"{stats.get('losses', 0)} | ${stats.get('pnl', 0):+.2f} |"
            )
        lines.append("")

    # Gating breakdown
    gating = report.get("gating_breakdown", {})
    by_reason = gating.get("by_reason", {})
    if by_reason:
        lines.extend([
            "## Gating Blocks",
            "",
            "| Reason | Count |",
            "|--------|-------|",
        ])
        for reason, count in sorted(by_reason.items(), key=lambda x: -x[1]):
            lines.append(f"| {reason} | {count} |")
        lines.append("")

    # Trade details
    trades = report.get("trade_details", [])
    if trades:
        lines.extend([
            "## Trade Details",
            "",
            "| Symbol | Dir | Strategy | P&L | % | Hold(s) | Exit Reason |",
            "|--------|-----|----------|-----|---|---------|-------------|",
        ])
        for t in trades[:30]:
            lines.append(
                f"| {t.get('symbol', '')} | {t.get('direction', '')} | "
                f"{t.get('entry_signal', '')} | ${t.get('pnl', 0):+.2f} | "
                f"{t.get('pnl_percent', 0):+.1f}% | {t.get('hold_time_seconds', 0):.0f} | "
                f"{t.get('exit_reason', '')} |"
            )
        lines.append("")

    # Market context
    ctx = report.get("market_context", {})
    lines.extend([
        "## Market Context",
        "",
        f"- **Active Symbols**: {ctx.get('total_symbols_tracked', 0)}",
        f"- **Symbols**: {', '.join(ctx.get('active_symbols', [])[:20])}",
        "",
    ])

    return "\n".join(lines)


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
        port=8020,
        log_level="info",
        reload=False,
    )


if __name__ == "__main__":
    main()
