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
    SCHWAB_AVAILABLE = True
except ImportError:
    SCHWAB_AVAILABLE = False

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

        # Market data client (initialized lazily)
        self._market_client: Any = None
        self._http_client: Any = None
        self._market_data_available = False
        self._init_market_data()

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
            logger.info("Schwab market data client initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize market data client: {e}")
            self._market_data_available = False

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
    ) -> list[dict[str, Any]]:
        """Get historical candles for a symbol."""
        if not self._market_data_available or not self._market_client:
            return []

        try:
            candles = self._market_client.get_candles(
                symbol=symbol.upper(),
                period_type=period_type,
                period=period,
                frequency_type="minute",
                frequency=frequency,
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

    async def start(self):
        """Start background tasks."""
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        # Emit system start event
        await self.emit_event(create_event(
            EventType.SYSTEM_START,
            payload={
                "trading_mode": self.state.trading_mode.value.upper(),
                "live_armed": self.state.live_armed,
                "kill_switch_active": self.state.kill_switch_active,
            },
        ))

        logger.info("MorpheusServer started")

    async def stop(self):
        """Stop background tasks."""
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
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

    async def emit_event(self, event: Event):
        """Emit an event to all WebSocket clients."""
        self.state.event_count += 1
        await self.ws_manager.broadcast(event)
        logger.debug(f"Event emitted: {event.event_type.value}")

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
        """Handle SET_SYMBOL_CHAIN - update active symbol."""
        symbol = command.payload.get("symbol", "").upper()
        self.state.active_symbol = symbol if symbol else None
        logger.info(f"Active symbol set to: {symbol or 'NONE'}")
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
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "scope": "readonly",
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
