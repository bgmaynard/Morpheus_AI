"""
Paper Position Manager - Position tracking + exit management for paper trading.

Tracks paper positions opened via auto-confirm, monitors quote updates,
and generates exit orders when stop/target/trailing/time conditions are met.

Exit Priority:
1. Kill switch -> exit all immediately
2. Hard stop -> price hits invalidation_reference
3. Trailing stop -> price drops 1.5% from high watermark (after target hit)
4. Target hit -> activates trailing mode (does NOT exit)
5. Time stop -> position held too long
6. Regime exit -> market regime shifts to DEAD

Reference: IBKR_Algo_BOT_V2 trailing stop has 85% win rate in live trading.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Awaitable

from morpheus.core.events import Event, EventType, create_event
from morpheus.risk.base import AccountState

logger = logging.getLogger(__name__)


class ExitReason(str, Enum):
    """Why a position was exited."""

    HARD_STOP = "hard_stop"
    TRAILING_STOP = "trailing_stop"
    TIME_STOP = "time_stop"
    REGIME_EXIT = "regime_exit"
    KILL_SWITCH = "kill_switch"
    MANUAL = "manual"


@dataclass
class ExitConfig:
    """Configuration for exit management."""

    trailing_stop_pct: float = 0.015  # 1.5% from high watermark

    # Per-strategy max hold times (seconds)
    strategy_max_hold: dict[str, float] = field(default_factory=lambda: {
        "order_flow_scalp": 120,       # 2 min for scalps
        "premarket_breakout": 300,     # 5 min
        "catalyst_momentum": 300,
        "short_squeeze": 300,
        "coil_breakout": 300,
        "gap_fade": 300,
        "day2_continuation": 600,      # 10 min for continuation
        # Original strategies
        "first_pullback": 300,
        "hod_continuation": 300,
        "vwap_reclaim": 180,
    })
    default_max_hold_seconds: float = 300  # 5 min default

    # Regime-based exit: exit when regime enters these modes
    regime_exit_modes: set[str] = field(default_factory=lambda: {"DEAD"})

    # Feature toggles
    enable_trailing: bool = True
    enable_time_stop: bool = True
    enable_regime_exit: bool = True


@dataclass
class PaperPosition:
    """
    A tracked paper position.

    Mutable because we update high_watermark, trailing state, P&L on every tick.
    """

    position_id: str
    symbol: str
    shares: int
    direction: str  # "long" or "short"
    entry_price: float
    entry_time: datetime
    strategy_name: str

    # Exit setpoints (from strategy's SignalCandidate)
    stop_price: float       # invalidation_reference
    target_price: float     # target_reference

    # Trailing stop state
    high_watermark: float = 0.0   # Highest price since entry (longs) / lowest (shorts)
    trailing_active: bool = False
    trailing_pct: float = 0.015   # 1.5%

    # Time stop
    max_hold_seconds: float = 300.0

    # Metadata
    regime_at_entry: str = ""
    structure_grade: str = ""
    client_order_id: str = ""
    correlation_id: str = ""

    # State
    exit_pending: bool = False
    unrealized_pnl: float = 0.0
    unrealized_pnl_pct: float = 0.0
    last_price: float = 0.0

    def __post_init__(self):
        if self.high_watermark == 0.0:
            self.high_watermark = self.entry_price


class PaperPositionManager:
    """
    Manages paper positions: tracks entries, monitors exits, generates sell orders.

    Wired into MorpheusServer.emit_event() to receive all events.
    On QUOTE_UPDATE, checks all open positions for exit conditions.
    On ORDER_FILL_RECEIVED (buy), opens a position.
    On ORDER_FILL_RECEIVED (sell), closes a position and records P&L.
    """

    def __init__(
        self,
        emit_event: Callable[[Event], Awaitable[None]],
        config: ExitConfig | None = None,
        kill_switch_ref: Callable[[], bool] | None = None,
    ):
        self._emit = emit_event
        self._config = config or ExitConfig()
        self._kill_switch_ref = kill_switch_ref  # Callable that returns kill_switch state

        # Open positions: symbol -> PaperPosition
        self._positions: dict[str, PaperPosition] = {}

        # Closed positions (for P&L reporting)
        self._closed_positions: list[dict[str, Any]] = []

        # Daily P&L tracking
        self._daily_realized_pnl: float = 0.0
        self._daily_unrealized_pnl: float = 0.0
        self._trade_count: int = 0
        self._win_count: int = 0

        # Current regime mode (updated from REGIME_DETECTED events)
        self._current_regime_mode: str = "NORMAL"

        # Signal metadata cache: symbol -> {strategy_name, stop, target, ...}
        # Set by server before fill event arrives
        self._signal_metadata: dict[str, dict[str, Any]] = {}

        logger.info("[POSITION_MGR] Paper position manager initialized")

    # ─── Event Dispatch ───────────────────────────────────────────────

    async def on_event(self, event: Event) -> None:
        """Main event handler - dispatches by event type."""
        try:
            et = event.event_type

            if et == EventType.ORDER_FILL_RECEIVED:
                side = (event.payload or {}).get("side", "")
                if side == "buy":
                    await self._open_position(event)
                elif side == "sell":
                    await self._close_position(event)

            elif et == EventType.QUOTE_UPDATE:
                await self._check_exits(event)

            elif et == EventType.REGIME_DETECTED:
                payload = event.payload or {}
                # Use MASS regime mode if available, otherwise extract from primary
                regime = payload.get("primary_regime", "")
                self._current_regime_mode = self._map_regime_to_mass_mode(regime)

        except Exception as e:
            logger.error(f"[POSITION_MGR] Error handling {event.event_type}: {e}", exc_info=True)

    # ─── Position Opening ─────────────────────────────────────────────

    async def _open_position(self, event: Event) -> None:
        """Open a new paper position from a buy fill event."""
        payload = event.payload or {}
        symbol = event.symbol or payload.get("symbol", "")

        if not symbol:
            return

        # Skip if already holding (shouldn't happen with re-entry blocking)
        if symbol in self._positions:
            logger.warning(f"[POSITION_MGR] Already holding {symbol}, ignoring duplicate fill")
            return

        shares = payload.get("filled_quantity", payload.get("quantity", 0))
        fill_price = payload.get("fill_price", 0)
        client_order_id = payload.get("client_order_id", "")
        correlation_id = event.correlation_id or ""

        if shares <= 0 or fill_price <= 0:
            return

        # Get signal metadata (stop/target/strategy)
        meta = self._signal_metadata.pop(symbol, {})
        strategy_name = meta.get("strategy_name", "unknown")
        stop_price = meta.get("stop_price", fill_price * 0.97)  # 3% fallback
        target_price = meta.get("target_price", fill_price * 1.03)  # 3% fallback
        direction = meta.get("direction", "long")
        regime = meta.get("regime", self._current_regime_mode)
        structure_grade = meta.get("structure_grade", "")

        # Determine max hold time for this strategy
        max_hold = self._config.strategy_max_hold.get(
            strategy_name, self._config.default_max_hold_seconds
        )

        position = PaperPosition(
            position_id=str(uuid.uuid4()),
            symbol=symbol,
            shares=shares,
            direction=direction,
            entry_price=fill_price,
            entry_time=datetime.now(timezone.utc),
            strategy_name=strategy_name,
            stop_price=stop_price,
            target_price=target_price,
            high_watermark=fill_price,
            trailing_pct=self._config.trailing_stop_pct,
            max_hold_seconds=max_hold,
            regime_at_entry=regime,
            structure_grade=structure_grade,
            client_order_id=client_order_id,
            correlation_id=correlation_id,
            last_price=fill_price,
        )

        self._positions[symbol] = position

        logger.info(
            f"[POSITION_MGR] OPENED: {symbol} {direction.upper()} "
            f"{shares} shares @ ${fill_price:.4f} | "
            f"stop=${stop_price:.4f} target=${target_price:.4f} | "
            f"strategy={strategy_name} hold={max_hold}s"
        )

        # Emit TRADE_ACTIVE event
        await self._emit(create_event(
            EventType.TRADE_ACTIVE,
            payload={
                "position_id": position.position_id,
                "symbol": symbol,
                "direction": direction,
                "shares": shares,
                "entry_price": fill_price,
                "stop_price": stop_price,
                "target_price": target_price,
                "strategy_name": strategy_name,
                "max_hold_seconds": max_hold,
                "trailing_pct": self._config.trailing_stop_pct,
            },
            symbol=symbol,
            correlation_id=correlation_id,
        ))

    # ─── Exit Monitoring ──────────────────────────────────────────────

    async def _check_exits(self, event: Event) -> None:
        """Check all open positions against current quote for exit conditions."""
        payload = event.payload or {}
        symbol = event.symbol or payload.get("symbol", "")

        if not symbol or symbol not in self._positions:
            return

        position = self._positions[symbol]

        # Skip if already exiting
        if position.exit_pending:
            return

        last = payload.get("last")
        if not last or last <= 0:
            return

        price = float(last)
        position.last_price = price
        now = datetime.now(timezone.utc)

        # Update watermark and unrealized P&L
        if position.direction == "long":
            if price > position.high_watermark:
                position.high_watermark = price
            position.unrealized_pnl = (price - position.entry_price) * position.shares
        else:  # short
            if price < position.high_watermark or position.high_watermark == position.entry_price:
                if price < position.high_watermark:
                    position.high_watermark = price
            position.unrealized_pnl = (position.entry_price - price) * position.shares

        position.unrealized_pnl_pct = (
            (position.unrealized_pnl / (position.entry_price * position.shares)) * 100
            if position.entry_price > 0 else 0.0
        )

        # Update aggregate unrealized P&L
        self._daily_unrealized_pnl = sum(
            p.unrealized_pnl for p in self._positions.values()
        )

        # ── Exit checks in priority order ──

        # 1. Kill switch
        if self._kill_switch_ref and self._kill_switch_ref():
            await self._execute_exit(position, price, ExitReason.KILL_SWITCH)
            return

        # 2. Hard stop
        if position.direction == "long" and price <= position.stop_price:
            await self._execute_exit(position, price, ExitReason.HARD_STOP)
            return
        elif position.direction == "short" and price >= position.stop_price:
            await self._execute_exit(position, price, ExitReason.HARD_STOP)
            return

        # 3. Trailing stop (only if trailing mode activated)
        if position.trailing_active and self._config.enable_trailing:
            if position.direction == "long":
                trail_level = position.high_watermark * (1 - position.trailing_pct)
                if price <= trail_level:
                    await self._execute_exit(position, price, ExitReason.TRAILING_STOP)
                    return
            else:  # short
                trail_level = position.high_watermark * (1 + position.trailing_pct)
                if price >= trail_level:
                    await self._execute_exit(position, price, ExitReason.TRAILING_STOP)
                    return

        # 4. Target hit -> activate trailing mode (do NOT exit)
        if not position.trailing_active:
            if position.direction == "long" and price >= position.target_price:
                position.trailing_active = True
                position.high_watermark = price  # Reset HWM to current price
                logger.info(
                    f"[POSITION_MGR] TRAILING ACTIVATED: {symbol} "
                    f"hit target ${position.target_price:.4f} at ${price:.4f} "
                    f"(P&L: {position.unrealized_pnl_pct:+.2f}%)"
                )
            elif position.direction == "short" and price <= position.target_price:
                position.trailing_active = True
                position.high_watermark = price
                logger.info(
                    f"[POSITION_MGR] TRAILING ACTIVATED: {symbol} "
                    f"hit target ${position.target_price:.4f} at ${price:.4f} "
                    f"(P&L: {position.unrealized_pnl_pct:+.2f}%)"
                )

        # 5. Time stop
        if self._config.enable_time_stop:
            elapsed = (now - position.entry_time).total_seconds()
            if elapsed > position.max_hold_seconds:
                await self._execute_exit(position, price, ExitReason.TIME_STOP)
                return

        # 6. Regime exit
        if self._config.enable_regime_exit:
            if self._current_regime_mode in self._config.regime_exit_modes:
                await self._execute_exit(position, price, ExitReason.REGIME_EXIT)
                return

    # ─── Exit Execution ───────────────────────────────────────────────

    async def _execute_exit(
        self, position: PaperPosition, exit_price: float, reason: ExitReason
    ) -> None:
        """Execute an exit by emitting sell order events."""
        if position.exit_pending:
            return
        position.exit_pending = True

        symbol = position.symbol
        side = "sell" if position.direction == "long" else "buy"  # Cover for shorts
        client_order_id = f"paper_exit_{uuid.uuid4().hex[:12]}"
        correlation_id = f"exit_{position.position_id[:8]}"

        # Compute P&L
        if position.direction == "long":
            pnl = (exit_price - position.entry_price) * position.shares
        else:
            pnl = (position.entry_price - exit_price) * position.shares

        pnl_pct = (pnl / (position.entry_price * position.shares)) * 100 if position.entry_price > 0 else 0.0
        hold_seconds = (datetime.now(timezone.utc) - position.entry_time).total_seconds()

        logger.info(
            f"[POSITION_MGR] EXIT {reason.value.upper()}: {symbol} "
            f"{position.shares} shares @ ${exit_price:.4f} | "
            f"entry=${position.entry_price:.4f} P&L=${pnl:+.2f} ({pnl_pct:+.2f}%) | "
            f"held {hold_seconds:.0f}s strategy={position.strategy_name}"
        )

        # Emit TRADE_EXIT_PENDING
        await self._emit(create_event(
            EventType.TRADE_EXIT_PENDING,
            payload={
                "position_id": position.position_id,
                "symbol": symbol,
                "exit_reason": reason.value,
                "exit_price": exit_price,
                "entry_price": position.entry_price,
                "shares": position.shares,
                "direction": position.direction,
                "unrealized_pnl": pnl,
                "unrealized_pnl_pct": pnl_pct,
                "hold_seconds": hold_seconds,
                "strategy_name": position.strategy_name,
                "trailing_was_active": position.trailing_active,
                "high_watermark": position.high_watermark,
            },
            symbol=symbol,
            correlation_id=correlation_id,
        ))

        # Emit ORDER_SUBMITTED (sell)
        await self._emit(create_event(
            EventType.ORDER_SUBMITTED,
            payload={
                "client_order_id": client_order_id,
                "symbol": symbol,
                "side": side,
                "quantity": position.shares,
                "order_type": "market",
                "limit_price": None,
                "auto_confirmed": True,
                "exit_reason": reason.value,
                "position_id": position.position_id,
            },
            symbol=symbol,
            correlation_id=correlation_id,
        ))

        # Small delay to simulate fill
        await asyncio.sleep(0.05)

        # Emit ORDER_FILL_RECEIVED (sell)
        await self._emit(create_event(
            EventType.ORDER_FILL_RECEIVED,
            payload={
                "client_order_id": client_order_id,
                "symbol": symbol,
                "side": side,
                "filled_quantity": position.shares,
                "fill_price": exit_price,
                "exec_id": f"exec_exit_{uuid.uuid4().hex[:8]}",
                "auto_confirmed": True,
                "exit_reason": reason.value,
                "position_id": position.position_id,
            },
            symbol=symbol,
            correlation_id=correlation_id,
        ))

    # ─── Position Closing ─────────────────────────────────────────────

    async def _close_position(self, event: Event) -> None:
        """Close a position from a sell fill event and record P&L."""
        payload = event.payload or {}
        symbol = event.symbol or payload.get("symbol", "")

        if not symbol or symbol not in self._positions:
            return

        position = self._positions[symbol]
        exit_price = payload.get("fill_price", position.last_price)
        exit_reason = payload.get("exit_reason", "unknown")

        # Compute final P&L
        if position.direction == "long":
            realized_pnl = (exit_price - position.entry_price) * position.shares
        else:
            realized_pnl = (position.entry_price - exit_price) * position.shares

        pnl_pct = (
            (realized_pnl / (position.entry_price * position.shares)) * 100
            if position.entry_price > 0 else 0.0
        )
        hold_seconds = (datetime.now(timezone.utc) - position.entry_time).total_seconds()

        # Update daily totals
        self._daily_realized_pnl += realized_pnl
        self._trade_count += 1
        if realized_pnl > 0:
            self._win_count += 1

        # Record closed position
        closed_record = {
            "position_id": position.position_id,
            "symbol": symbol,
            "direction": position.direction,
            "shares": position.shares,
            "entry_price": position.entry_price,
            "exit_price": exit_price,
            "realized_pnl": realized_pnl,
            "pnl_pct": pnl_pct,
            "hold_seconds": hold_seconds,
            "exit_reason": exit_reason,
            "strategy_name": position.strategy_name,
            "trailing_was_active": position.trailing_active,
            "high_watermark": position.high_watermark,
            "regime_at_entry": position.regime_at_entry,
            "entry_time": position.entry_time.isoformat(),
            "exit_time": datetime.now(timezone.utc).isoformat(),
        }
        self._closed_positions.append(closed_record)

        # Remove from open positions
        del self._positions[symbol]

        logger.info(
            f"[POSITION_MGR] CLOSED: {symbol} P&L=${realized_pnl:+.2f} ({pnl_pct:+.2f}%) "
            f"reason={exit_reason} | Daily: ${self._daily_realized_pnl:+.2f} "
            f"trades={self._trade_count} wins={self._win_count}"
        )

        # Emit TRADE_CLOSED event
        await self._emit(create_event(
            EventType.TRADE_CLOSED,
            payload={
                **closed_record,
                "daily_realized_pnl": self._daily_realized_pnl,
                "daily_trade_count": self._trade_count,
                "daily_win_count": self._win_count,
                "daily_win_rate": (
                    self._win_count / self._trade_count * 100
                    if self._trade_count > 0 else 0.0
                ),
            },
            symbol=symbol,
            correlation_id=event.correlation_id,
        ))

    # ─── Account State ────────────────────────────────────────────────

    def get_account_state(
        self,
        base_equity: Decimal = Decimal("100000"),
        kill_switch_active: bool = False,
    ) -> AccountState:
        """Build a real AccountState from tracked positions."""
        open_count = len(self._positions)
        total_exposure = sum(
            p.shares * p.entry_price for p in self._positions.values()
        )
        total_unrealized = sum(
            p.unrealized_pnl for p in self._positions.values()
        )

        equity = float(base_equity) + self._daily_realized_pnl + total_unrealized
        cash = float(base_equity) + self._daily_realized_pnl - total_exposure

        return AccountState(
            total_equity=Decimal(str(round(equity, 2))),
            cash_available=Decimal(str(round(max(cash, 0), 2))),
            buying_power=Decimal(str(round(max(cash, 0), 2))),
            open_position_count=open_count,
            total_exposure=Decimal(str(round(total_exposure, 2))),
            exposure_pct=total_exposure / float(base_equity) * 100 if base_equity > 0 else 0.0,
            daily_pnl=Decimal(str(round(self._daily_realized_pnl + total_unrealized, 2))),
            daily_pnl_pct=(
                (self._daily_realized_pnl + total_unrealized) / float(base_equity) * 100
                if base_equity > 0 else 0.0
            ),
            peak_equity=base_equity,
            current_drawdown_pct=max(0, -((equity / float(base_equity)) - 1) * 100),
            kill_switch_active=kill_switch_active,
            manual_halt=False,
        )

    # ─── Query Methods ────────────────────────────────────────────────

    def has_position(self, symbol: str) -> bool:
        """Check if we currently hold a position in this symbol."""
        return symbol in self._positions

    def get_position(self, symbol: str) -> PaperPosition | None:
        """Get a specific open position."""
        return self._positions.get(symbol)

    def get_all_positions(self) -> dict[str, PaperPosition]:
        """Get all open positions."""
        return dict(self._positions)

    def get_positions_summary(self) -> list[dict[str, Any]]:
        """Get positions as serializable dicts for API responses."""
        result = []
        for pos in self._positions.values():
            result.append({
                "position_id": pos.position_id,
                "symbol": pos.symbol,
                "direction": pos.direction,
                "shares": pos.shares,
                "entry_price": pos.entry_price,
                "last_price": pos.last_price,
                "stop_price": pos.stop_price,
                "target_price": pos.target_price,
                "unrealized_pnl": round(pos.unrealized_pnl, 2),
                "unrealized_pnl_pct": round(pos.unrealized_pnl_pct, 2),
                "trailing_active": pos.trailing_active,
                "high_watermark": pos.high_watermark,
                "hold_seconds": (datetime.now(timezone.utc) - pos.entry_time).total_seconds(),
                "max_hold_seconds": pos.max_hold_seconds,
                "strategy_name": pos.strategy_name,
                "entry_time": pos.entry_time.isoformat(),
            })
        return result

    def get_closed_trades(self) -> list[dict[str, Any]]:
        """Get all closed trades for the day."""
        return list(self._closed_positions)

    def get_daily_stats(self) -> dict[str, Any]:
        """Get daily trading statistics."""
        return {
            "open_positions": len(self._positions),
            "closed_trades": self._trade_count,
            "wins": self._win_count,
            "losses": self._trade_count - self._win_count,
            "win_rate": (
                self._win_count / self._trade_count * 100
                if self._trade_count > 0 else 0.0
            ),
            "realized_pnl": round(self._daily_realized_pnl, 2),
            "unrealized_pnl": round(self._daily_unrealized_pnl, 2),
            "total_pnl": round(self._daily_realized_pnl + self._daily_unrealized_pnl, 2),
        }

    def set_signal_metadata(self, symbol: str, metadata: dict[str, Any]) -> None:
        """
        Cache signal metadata for a symbol before its fill event arrives.

        Called by the auto-confirm handler in main.py to pass strategy info
        (stop, target, strategy_name) to the position manager.
        """
        self._signal_metadata[symbol] = metadata

    def reset_daily(self) -> None:
        """Reset daily stats. Call at market open."""
        self._daily_realized_pnl = 0.0
        self._daily_unrealized_pnl = 0.0
        self._trade_count = 0
        self._win_count = 0
        self._closed_positions.clear()
        logger.info("[POSITION_MGR] Daily stats reset")

    # ─── Helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _map_regime_to_mass_mode(primary_regime: str) -> str:
        """Map primary regime string to MASS mode for exit decisions."""
        regime_lower = primary_regime.lower()
        if "dead" in regime_lower or ("low" in regime_lower and "rang" in regime_lower):
            return "DEAD"
        if "chop" in regime_lower or "ranging" in regime_lower:
            return "CHOP"
        if "trending_up" in regime_lower or "hot" in regime_lower:
            return "HOT"
        return "NORMAL"
