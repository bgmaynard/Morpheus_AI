"""
UI Gateway - Service 4 of the Morpheus Data Spine.

Backward-compatible REST/WebSocket server that bridges NATS to Morpheus_UI.
The UI doesn't need any changes - same API contract as the monolith server.

Subscribes to:
  ai.events.*             - All pipeline events (for WebSocket broadcast)
  md.quotes.*             - Quote snapshots (throttled to 10Hz for UI)
  ai.signals.*            - Signals for Decision Support Panel
  ai.structure.*          - Structure grades
  ai.regime               - Regime updates
  bot.positions           - Position updates
  bot.telemetry.*         - Service health

Serves:
  GET  /health                      - Health check
  GET  /api/pipeline/status         - Pipeline diagnostics
  GET  /api/market/candles/{symbol} - OHLCV candle data (proxy to Schwab)
  GET  /api/market/quotes           - Real-time quotes (from cache)
  GET  /api/ui/state                - UI state snapshot
  POST /api/ui/command              - UI commands (kill switch, mode, etc.)
  POST /api/trading/confirm-entry   - Human confirms signal
  POST /api/trading/cancel-order    - Cancel order
  WS   /ws/events                   - Real-time event stream
  bot.telemetry.ui_gateway          - Every 2s
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

from dotenv import load_dotenv

load_dotenv()

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from morpheus.spine.nats_client import SpineClient
from morpheus.spine.config_store import ConfigStore
from morpheus.spine.schemas import (
    Topics,
    QuoteMsg,
    OHLCMsg,
    SignalMsg,
    StructureMsg,
    RegimeMsg,
    TelemetryMsg,
    PositionMsg,
    decode,
)
from morpheus.core.events import Event, EventType, create_event
from morpheus.core.market_mode import get_market_mode, get_time_et
from morpheus.execution.base import TradingMode

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pydantic models (same as monolith for backward compat)
# ---------------------------------------------------------------------------

class Command(BaseModel):
    command_id: str
    command_type: str
    payload: dict[str, Any]
    timestamp: str


class CommandResult(BaseModel):
    accepted: bool
    command_id: str
    message: str = ""


class HealthResponse(BaseModel):
    status: str
    trading_mode: str
    live_armed: bool
    kill_switch_active: bool
    uptime_seconds: float
    event_count: int
    websocket_clients: int
    pipeline_enabled: bool = True
    spine_mode: bool = True


# ---------------------------------------------------------------------------
# WebSocket Manager (same as monolith)
# ---------------------------------------------------------------------------

class WebSocketManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        async with self._lock:
            self.active_connections.append(websocket)
        logger.info(f"WS client connected. Total: {len(self.active_connections)}")

    async def disconnect(self, websocket: WebSocket):
        async with self._lock:
            if websocket in self.active_connections:
                self.active_connections.remove(websocket)
        logger.info(f"WS client disconnected. Total: {len(self.active_connections)}")

    async def broadcast_json(self, data: dict):
        if not self.active_connections:
            return
        message = json.dumps(data)
        disconnected = []
        async with self._lock:
            for conn in self.active_connections:
                try:
                    await conn.send_text(message)
                except Exception:
                    disconnected.append(conn)
        for conn in disconnected:
            await self.disconnect(conn)

    @property
    def client_count(self) -> int:
        return len(self.active_connections)


# ---------------------------------------------------------------------------
# UI Gateway Service
# ---------------------------------------------------------------------------

class UIGatewayService:
    """
    UI Gateway bridging NATS spine to Morpheus_UI.

    Maintains snapshot caches and throttles quote delivery to the UI.
    """

    SERVICE_NAME = "ui_gateway"

    def __init__(self, config_store: ConfigStore | None = None):
        self._config_store = config_store or ConfigStore()
        config = self._config_store.get()

        self._spine = SpineClient(
            url=config.nats_url,
            name=self.SERVICE_NAME,
        )

        self._ws_manager = WebSocketManager()

        # Snapshot caches
        self._latest_quotes: dict[str, dict] = {}
        self._latest_regime: dict[str, Any] = {}
        self._latest_structures: dict[str, dict] = {}
        self._service_telemetry: dict[str, dict] = {}
        self._active_signals: dict[str, dict] = {}
        self._positions: dict[str, dict] = {}

        # Event history (circular buffer for replay on connect)
        self._event_history: deque = deque(maxlen=200)

        # Throttling: track last quote send time per symbol
        self._quote_throttle_interval = 1.0 / config.ui_quote_throttle_hz
        self._last_quote_send: dict[str, float] = {}

        # State
        self._trading_mode = TradingMode.PAPER
        self._live_armed = False
        self._kill_switch_active = False
        self._start_time = 0.0
        self._event_count = 0
        self._errors = 0

        # Schwab market client for candle data proxying
        self._market_client: Any = None
        self._http_client: httpx.AsyncClient | None = None

        # Control
        self._running = False
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        """Start the UI Gateway service."""
        logger.info(f"[{self.SERVICE_NAME}] Starting...")
        self._start_time = time.time()
        self._running = True

        # Connect to NATS
        await self._spine.connect()

        # Subscribe to all relevant topics
        await self._spine.subscribe_decode("ai.events.>", self._on_ai_event)
        await self._spine.subscribe_decode("md.quotes.*", self._on_quote)
        await self._spine.subscribe_decode("ai.signals.*", self._on_signal)
        await self._spine.subscribe_decode("ai.structure.*", self._on_structure)
        await self._spine.subscribe_decode(Topics.REGIME, self._on_regime)
        await self._spine.subscribe_decode(Topics.POSITIONS, self._on_position)
        await self._spine.subscribe_decode("bot.telemetry.*", self._on_telemetry)
        await self._spine.subscribe_decode(Topics.SCANNER_DISCOVERY, self._on_discovery)

        # Init HTTP client for Schwab API proxying
        self._http_client = httpx.AsyncClient(timeout=30.0)

        # Start telemetry
        self._tasks.append(asyncio.create_task(self._telemetry_loop()))

        logger.info(f"[{self.SERVICE_NAME}] Started, subscribed to NATS")

    async def stop(self) -> None:
        """Stop the UI Gateway service."""
        logger.info(f"[{self.SERVICE_NAME}] Stopping...")
        self._running = False

        for task in self._tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        if self._http_client:
            await self._http_client.aclose()

        await self._spine.close()
        logger.info(f"[{self.SERVICE_NAME}] Stopped")

    # -----------------------------------------------------------------------
    # NATS handlers
    # -----------------------------------------------------------------------

    async def _on_ai_event(self, subject: str, data: dict) -> None:
        """Forward pipeline events to WebSocket clients."""
        self._event_count += 1
        self._event_history.append(data)
        await self._ws_manager.broadcast_json(data)

    async def _on_quote(self, subject: str, data: dict) -> None:
        """Cache quote and forward to UI (throttled)."""
        sym = data.get("sym", "")
        if not sym:
            return

        self._latest_quotes[sym] = data

        # Throttle: only forward to UI at configured Hz
        now = time.time()
        last_sent = self._last_quote_send.get(sym, 0)
        if now - last_sent < self._quote_throttle_interval:
            return

        self._last_quote_send[sym] = now

        # Broadcast as QUOTE_UPDATE event
        event_data = {
            "event_type": "QUOTE_UPDATE",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "symbol": sym,
            "payload": data,
        }
        await self._ws_manager.broadcast_json(event_data)

    async def _on_signal(self, subject: str, data: dict) -> None:
        """Cache signal and forward to UI."""
        sym = data.get("sym", "")
        self._active_signals[sym] = data

    async def _on_structure(self, subject: str, data: dict) -> None:
        """Cache structure grade."""
        sym = data.get("sym", "")
        self._latest_structures[sym] = data

    async def _on_regime(self, subject: str, data: dict) -> None:
        """Cache regime update."""
        self._latest_regime = data

    async def _on_position(self, subject: str, data: dict) -> None:
        """Cache position update."""
        sym = data.get("sym", "")
        if sym:
            self._positions[sym] = data

    async def _on_telemetry(self, subject: str, data: dict) -> None:
        """Cache service telemetry."""
        svc = data.get("service", "")
        if svc:
            self._service_telemetry[svc] = data

    async def _on_discovery(self, subject: str, data: dict) -> None:
        """Track new symbol discovery."""
        pass  # Handled by AI Core

    # -----------------------------------------------------------------------
    # FastAPI app factory
    # -----------------------------------------------------------------------

    def create_app(self) -> FastAPI:
        """Create the FastAPI app with all endpoints."""
        app = FastAPI(title="Morpheus UI Gateway (Spine)", version="2.0.0")

        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        gateway = self  # Capture for closures

        @app.on_event("startup")
        async def startup():
            await gateway.start()

        @app.on_event("shutdown")
        async def shutdown():
            await gateway.stop()

        # ---- Health ----

        @app.get("/health", response_model=HealthResponse)
        async def health():
            return HealthResponse(
                status="ok" if gateway._spine.is_connected else "degraded",
                trading_mode=gateway._trading_mode.value,
                live_armed=gateway._live_armed,
                kill_switch_active=gateway._kill_switch_active,
                uptime_seconds=time.time() - gateway._start_time,
                event_count=gateway._event_count,
                websocket_clients=gateway._ws_manager.client_count,
                pipeline_enabled=True,
                spine_mode=True,
            )

        # ---- Pipeline Status ----

        @app.get("/api/pipeline/status")
        async def pipeline_status():
            market_mode = get_market_mode()
            time_et = get_time_et()
            ai_telem = gateway._service_telemetry.get("ai_core", {})
            extra = ai_telem.get("extra", {})

            return {
                "market_mode": market_mode.name,
                "active_trading": market_mode.active_trading,
                "observe_only": market_mode.observe_only,
                "time_et": str(time_et),
                "active_symbols": list(gateway._latest_quotes.keys()),
                "kill_switch_active": gateway._kill_switch_active,
                "mass_enabled": extra.get("mass_enabled", False),
                "regime": gateway._latest_regime,
                "spine_mode": True,
                "services": {
                    svc: {
                        "status": t.get("status", "unknown"),
                        "uptime_s": t.get("uptime_s", 0),
                        "errors": t.get("errors", 0),
                    }
                    for svc, t in gateway._service_telemetry.items()
                },
            }

        # ---- Pipeline Monitor (polled every 2s by OrchestratorPanel) ----

        @app.get("/api/pipeline/monitor")
        async def pipeline_monitor():
            ai_telem = gateway._service_telemetry.get("ai_core", {})
            extra = ai_telem.get("extra", {})
            market_mode = get_market_mode()

            # Build recent events from history
            signals = []
            scores = []
            gate_decisions = []
            risk_decisions = []
            regime_detections = []
            for evt in gateway._event_history:
                et = evt.get("event_type", "")
                if et in ("SIGNAL_CANDIDATE", "SIGNAL_OBSERVED"):
                    signals.append(evt)
                elif et == "SIGNAL_SCORED":
                    scores.append(evt)
                elif et in ("META_APPROVED", "META_REJECTED"):
                    gate_decisions.append(evt)
                elif et in ("RISK_APPROVED", "RISK_VETO"):
                    risk_decisions.append(evt)
                elif et == "REGIME_DETECTED":
                    regime_detections.append(evt)

            return {
                "enabled": True,
                "status": {
                    "active_symbols": list(gateway._latest_quotes.keys()),
                    "kill_switch_active": gateway._kill_switch_active,
                    "permissive_mode": True,
                    "registered_strategies": [],
                    "warmup_status": {},
                    "market_mode": market_mode.name,
                    "mass_enabled": extra.get("mass_enabled", False),
                },
                "recent_events": {
                    "signals": signals[-20:],
                    "scores": scores[-20:],
                    "gate_decisions": gate_decisions[-20:],
                    "risk_decisions": risk_decisions[-20:],
                    "regime_detections": regime_detections[-10:],
                },
                "event_counts": {
                    "total": gateway._event_count,
                    "signals": len(signals),
                    "approved": len([g for g in gate_decisions if g.get("event_type") == "META_APPROVED"]),
                    "rejected": len([g for g in gate_decisions if g.get("event_type") == "META_REJECTED"]),
                    "risk_approved": len([r for r in risk_decisions if r.get("event_type") == "RISK_APPROVED"]),
                    "risk_vetoed": len([r for r in risk_decisions if r.get("event_type") == "RISK_VETO"]),
                },
            }

        # ---- Market Data ----

        @app.get("/api/market/status")
        async def market_status():
            return {
                "available": gateway._spine.is_connected,
                "provider": "schwab" if os.getenv("SCHWAB_CLIENT_ID") else None,
            }

        @app.get("/api/market/quotes")
        async def get_quotes():
            return {
                sym: {
                    "symbol": q.get("sym", sym),
                    "last": q.get("last", 0),
                    "bid": q.get("bid", 0),
                    "ask": q.get("ask", 0),
                    "volume": q.get("vol", 0),
                    "high": q.get("high", 0),
                    "low": q.get("low", 0),
                    "open": q.get("open", 0),
                    "close": q.get("close", 0),
                    "net_change": q.get("net_chg", 0),
                    "mark": q.get("mark", 0),
                }
                for sym, q in gateway._latest_quotes.items()
            }

        @app.get("/api/market/candles/{symbol}")
        async def get_candles(
            symbol: str,
            period_type: str = "day",
            period: int = 1,
            frequency: int = 1,
            frequency_type: str = "minute",
        ):
            """Proxy candle requests to Schwab API (same logic as monolith)."""
            try:
                from morpheus.broker.schwab_auth import SchwabAuth
                from morpheus.broker.schwab_market import SchwabMarketClient

                auth = SchwabAuth(
                    client_id=os.getenv("SCHWAB_CLIENT_ID", ""),
                    client_secret=os.getenv("SCHWAB_CLIENT_SECRET", ""),
                    redirect_uri=os.getenv("SCHWAB_REDIRECT_URI", ""),
                    token_path=os.getenv(
                        "SCHWAB_TOKEN_PATH", "./tokens/schwab_token.json"
                    ),
                )
                sync_http = httpx.Client(timeout=30.0)
                market = SchwabMarketClient(auth=auth, http_client=sync_http)

                candles = market.get_candles(
                    symbol=symbol.upper(),
                    period_type=period_type,
                    period=period,
                    frequency_type=frequency_type,
                    frequency=frequency,
                )
                sync_http.close()

                # Convert Candle objects to dict with unix timestamp (UI expects "time")
                candle_dicts = [
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
                return {"symbol": symbol.upper(), "candles": candle_dicts}

            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        # ---- UI State ----

        @app.get("/api/ui/state")
        async def get_state():
            return {
                "trading": {
                    "mode": gateway._trading_mode.value.upper(),
                    "liveArmed": gateway._live_armed,
                    "killSwitchActive": gateway._kill_switch_active,
                },
                "profile": {"gate": "standard", "risk": "standard", "guard": "standard"},
                "orders": {},
                "positions": gateway._positions,
                "executions": [],
            }

        # ---- Commands ----

        @app.post("/api/ui/command", response_model=CommandResult)
        async def handle_command(cmd: Command):
            ct = cmd.command_type

            if ct == "SET_KILL_SWITCH":
                active = cmd.payload.get("active", False)
                gateway._kill_switch_active = active
                # Publish to NATS so AI Core picks it up
                await gateway._spine.publish_msg(
                    "bot.commands.kill_switch",
                    {"active": active, "ts": time.time()},
                )
                return CommandResult(
                    accepted=True,
                    command_id=cmd.command_id,
                    message=f"Kill switch {'activated' if active else 'deactivated'}",
                )

            elif ct == "SET_TRADING_MODE":
                mode = cmd.payload.get("mode", "PAPER")
                gateway._trading_mode = TradingMode(mode.lower())
                return CommandResult(
                    accepted=True,
                    command_id=cmd.command_id,
                    message=f"Trading mode set to {mode}",
                )

            elif ct == "ARM_LIVE_TRADING":
                gateway._live_armed = cmd.payload.get("armed", False)
                return CommandResult(
                    accepted=True,
                    command_id=cmd.command_id,
                    message=f"Live trading {'armed' if gateway._live_armed else 'disarmed'}",
                )

            return CommandResult(
                accepted=False,
                command_id=cmd.command_id,
                message=f"Unknown command: {ct}",
            )

        # ---- Trading Data ----

        @app.get("/api/trading/orders")
        async def get_orders(status: str | None = None):
            """Return orders (from NATS cache or empty)."""
            # TODO: Wire to Schwab orders via NATS when schwab_feed publishes them
            return {"orders": []}

        @app.get("/api/trading/positions")
        async def get_positions():
            """Return positions from NATS cache."""
            positions_list = []
            for sym, pos in gateway._positions.items():
                positions_list.append({
                    "symbol": sym,
                    "quantity": pos.get("qty", 0),
                    "avg_price": pos.get("avg_price", 0),
                    "current_price": gateway._latest_quotes.get(sym, {}).get("last", 0),
                    "market_value": pos.get("market_value", 0),
                    "unrealized_pnl": pos.get("unrealized_pnl", 0),
                    "unrealized_pnl_pct": 0,
                    "asset_type": "EQUITY",
                })
            return {"positions": positions_list}

        @app.get("/api/trading/transactions")
        async def get_transactions():
            """Return today's transactions (from NATS cache or empty)."""
            # TODO: Wire to order fill events from NATS
            return {"transactions": []}

        # ---- Trading Actions ----

        @app.post("/api/trading/confirm-entry")
        async def confirm_entry(request: dict):
            """Forward confirmation to AI Core via NATS."""
            await gateway._spine.publish_msg(
                "bot.commands.confirm_entry",
                {**request, "ts": time.time()},
            )
            return {"accepted": True, "message": "Confirmation forwarded to AI Core"}

        @app.post("/api/trading/cancel-order")
        async def cancel_order(request: dict):
            await gateway._spine.publish_msg(
                "bot.commands.cancel_order",
                {**request, "ts": time.time()},
            )
            return {"accepted": True, "message": "Cancel forwarded to AI Core"}

        # ---- MASS Endpoints ----

        @app.get("/api/mass/status")
        async def mass_status():
            ai_extra = gateway._service_telemetry.get("ai_core", {}).get("extra", {})
            return {
                "mass_enabled": ai_extra.get("mass_enabled", False),
                "regime": gateway._latest_regime,
                "structures": gateway._latest_structures,
                "active_symbols": ai_extra.get("active_symbols", 0),
            }

        @app.get("/api/mass/performance")
        async def mass_performance():
            """Return per-strategy performance metrics."""
            # TODO: Wire to PerformanceTracker data via NATS when ai_core publishes it
            return {"strategies": {}, "updated_at": None}

        @app.get("/api/mass/weights")
        async def mass_weights():
            """Return current strategy weights."""
            # TODO: Wire to StrategyWeightManager data via NATS when ai_core publishes it
            return {"weights": {}, "updated_at": None}

        # ---- WebSocket ----

        @app.websocket("/ws/events")
        async def websocket_events(websocket: WebSocket):
            await gateway._ws_manager.connect(websocket)
            try:
                # Send recent event history on connect
                for event_data in gateway._event_history:
                    try:
                        await websocket.send_text(json.dumps(event_data))
                    except Exception:
                        break

                # Keep connection alive
                while True:
                    data = await websocket.receive_text()
                    # Client can send commands via WebSocket too
                    logger.debug(f"WS message: {data}")
            except WebSocketDisconnect:
                pass
            finally:
                await gateway._ws_manager.disconnect(websocket)

        # ---- Schwab OAuth (proxied from monolith) ----

        @app.get("/api/auth/schwab/url")
        async def get_auth_url():
            client_id = os.getenv("SCHWAB_CLIENT_ID", "")
            redirect_uri = os.getenv("SCHWAB_REDIRECT_URI", "HTTPS://127.0.0.1:6969")
            url = (
                f"https://api.schwabapi.com/v1/oauth/authorize"
                f"?response_type=code"
                f"&client_id={client_id}"
                f"&redirect_uri={redirect_uri}"
            )
            return {"url": url}

        return app

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
                    msgs_in=self._event_count,
                    errors=self._errors,
                    extra={
                        "ws_clients": self._ws_manager.client_count,
                        "cached_quotes": len(self._latest_quotes),
                        "cached_structures": len(self._latest_structures),
                        "event_history_size": len(self._event_history),
                    },
                )
                await self._spine.publish(msg.topic(), msg.encode())
            except Exception as e:
                logger.error(f"[{self.SERVICE_NAME}] Telemetry error: {e}")

            await asyncio.sleep(2.0)


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

def create_app() -> FastAPI:
    """Factory function for uvicorn."""
    gateway = UIGatewayService()
    return gateway.create_app()


async def main() -> None:
    """Run UI Gateway as a standalone service."""
    import uvicorn

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    config = uvicorn.Config(
        app="morpheus.spine.ui_gateway:create_app",
        factory=True,
        host="0.0.0.0",
        port=8011,
        log_level="info",
    )
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    asyncio.run(main())
