"""
Paper Position Manager - Exit Management v1 for paper trading.

EXIT MANAGEMENT v1 - PERMISSION TO TRADE LOGIC
==============================================

Core Rule (Non-Negotiable):
No trade may be entered unless its full exit plan is defined at entry time.
If an exit plan cannot be created → reject the trade with TRADE_REJECTED_NO_EXIT_PLAN.

Exit v1 consists of three layers, initialized at entry, evaluated continuously:

1. TIME-BASED FAILSAFE (MANDATORY)
   - Every trade has max_hold_seconds (default 180-300s)
   - Timer starts at fill
   - When time expires → exit at market
   - Exit reason: TIME_STOP
   - CANNOT be disabled

2. HARD STOP (Risk Definition)
   - Every trade defines hard_stop_price before order submission
   - Calculated from structure (invalidation_reference) or fallback %
   - Stored in trade state
   - Active immediately, cannot be widened
   - Exit reason: HARD_STOP

3. TRAILING STOP (Conditional Profit Protection)
   - Inactive at entry
   - Activates after price moves favorably by trail_activation_pct
   - Trail only moves in direction of profit
   - Exit reason: TRAIL_STOP

Exit Priority (evaluated in order):
1. Kill switch → exit all immediately
2. Hard stop → price hits stop_price
3. Trailing stop → price drops trail_distance_pct from high watermark
4. Time stop → position held too long

Reference: IBKR_Algo_BOT_V2 trailing stop has 85% win rate in live trading.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Awaitable

from morpheus.core.events import Event, EventType, create_event
from morpheus.risk.base import AccountState
from morpheus.core.validation_mode import (
    is_validation_mode,
    get_validation_logger,
)

logger = logging.getLogger(__name__)


class ExitReason(str, Enum):
    """Why a position was exited - matches spec exactly."""

    TIME_STOP = "TIME_STOP"      # Mandatory failsafe
    HARD_STOP = "HARD_STOP"      # Risk definition
    TRAIL_STOP = "TRAIL_STOP"    # Profit protection
    KILL_SWITCH = "KILL_SWITCH"  # Emergency exit
    MANUAL = "MANUAL"            # Human intervention

    # Microstructure exit reasons
    SSR_MOMENTUM_DECAY = "SSR_MOMENTUM_DECAY"        # SSR velocity/ROC decay
    SSR_BID_COLLAPSE = "SSR_BID_COLLAPSE"            # SSR bid depth collapse
    SSR_UPTICK_EXHAUSTION = "SSR_UPTICK_EXHAUSTION"  # Consecutive uptick rejections
    SSR_SPREAD_BLOWOUT = "SSR_SPREAD_BLOWOUT"        # SSR spread widening
    SSR_JACKKNIFE = "SSR_JACKKNIFE"                  # SSR jackknife pattern
    SSR_TIME_FAILSAFE = "SSR_TIME_FAILSAFE"          # SSR no new high timeout
    HTB_PROFIT_PROTECT = "HTB_PROFIT_PROTECT"        # HTB fast profit taking
    HALT_EXIT = "HALT_EXIT"                          # Halted during position
    POST_HALT_FAILURE = "POST_HALT_FAILURE"          # Post-halt instability
    LIQUIDITY_EVAPORATION = "LIQUIDITY_EVAPORATION"  # General liquidity vanish


@dataclass
class ExitPlan:
    """
    Complete exit plan for a trade - ALL three layers required.

    This is the permission-to-trade gate. A trade without a valid
    ExitPlan is rejected with TRADE_REJECTED_NO_EXIT_PLAN.
    """

    # LAYER 1: Time-based failsafe (MANDATORY, cannot be disabled)
    max_hold_seconds: float          # Must be > 0

    # LAYER 2: Hard stop (risk definition)
    hard_stop_price: float           # Must be valid price > 0

    # LAYER 3: Trailing stop (conditional)
    trail_activation_pct: float      # e.g., 0.01 = +1% to activate
    trail_distance_pct: float        # e.g., 0.015 = 1.5% trail

    # Metadata
    strategy_name: str = ""
    direction: str = "long"
    computed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # Partial profit-taking (optional, strategy-dependent)
    allow_partial_take: bool = False
    partial_take_pct: float = 0.50       # fraction of shares to sell (0.50 = 50%)
    partial_trigger_pct: float = 0.005   # favorable move to trigger partial (+0.5%)

    def is_valid(self) -> tuple[bool, str]:
        """Validate exit plan has all required components."""
        if self.max_hold_seconds <= 0:
            return False, "max_hold_seconds must be > 0"
        if self.hard_stop_price <= 0:
            return False, "hard_stop_price must be > 0"
        if self.trail_distance_pct <= 0:
            return False, "trail_distance_pct must be > 0"
        if self.allow_partial_take:
            if not (0.0 < self.partial_take_pct < 1.0):
                return False, "partial_take_pct must be between 0 and 1"
            if self.partial_trigger_pct <= 0:
                return False, "partial_trigger_pct must be > 0"
        return True, ""


@dataclass
class ExitConfig:
    """
    Hot-reloadable exit configuration.

    Loaded from runtime_config.py - changes take effect on next trade.
    """

    # Layer 1: Time failsafe
    max_hold_seconds: float = 300.0  # 5 min default

    # Layer 2: Hard stop fallback
    hard_stop_pct: float = 0.03  # 3% if no structure-based stop

    # Layer 3: Trailing stop
    trail_activation_pct: float = 0.01  # +1% move to activate
    trail_distance_pct: float = 0.015   # 1.5% trail

    # Per-strategy max hold overrides
    strategy_max_hold: dict[str, float] = field(default_factory=lambda: {
        "order_flow_scalp": 120,       # 2 min for scalps
        "premarket_breakout": 1800,    # 30 min - let premarket runners run
        "catalyst_momentum": 600,      # 10 min (was 300) - more time to express
        "short_squeeze": 300,
        "coil_breakout": 300,
        "gap_fade": 300,
        "day2_continuation": 600,      # 10 min for continuation
        "first_pullback": 900,         # 15 min (was 300) - pullbacks need time
        "hod_continuation": 300,
        "vwap_reclaim": 180,
    })

    # Per-strategy trail activation overrides (pct move to arm trailing)
    strategy_trail_activation: dict[str, float] = field(default_factory=lambda: {
        "catalyst_momentum": 0.005,    # 0.5% (was global 1%)
        "first_pullback": 0.004,       # 0.4%
        "premarket_breakout": 0.008,   # 0.8%
        "order_flow_scalp": 0.003,     # 0.3%
        "day2_continuation": 0.006,    # 0.6%
        "coil_breakout": 0.005,        # 0.5%
        "short_squeeze": 0.007,        # 0.7%
        "gap_fade": 0.005,             # 0.5%
        "hod_continuation": 0.005,     # 0.5%
        "vwap_reclaim": 0.004,         # 0.4%
    })

    # Per-strategy partial profit-take config
    strategy_partial_take: dict[str, dict] = field(default_factory=lambda: {
        "catalyst_momentum": {"enabled": True, "take_pct": 0.50, "trigger_pct": 0.005},
        "first_pullback":    {"enabled": True, "take_pct": 0.50, "trigger_pct": 0.005},
        "short_squeeze":     {"enabled": True, "take_pct": 0.50, "trigger_pct": 0.007},
    })


@dataclass
class PaperPosition:
    """
    A tracked paper position with complete exit state.

    Mutable because we update high_watermark, trailing state, P&L on every tick.
    """

    position_id: str
    symbol: str
    shares: int
    direction: str  # "long" or "short"
    entry_price: float
    entry_time: datetime
    strategy_name: str

    # ── Exit Plan (all three layers) ────────────────────────────────────
    # Layer 1: Time failsafe
    max_hold_seconds: float          # Max time before forced exit
    max_exit_ts: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # Layer 2: Hard stop
    hard_stop_price: float = 0.0     # Price that triggers HARD_STOP

    # Layer 3: Trailing stop
    trail_activation_pct: float = 0.01   # Move required to activate trailing
    trail_distance_pct: float = 0.015    # Trail distance from HWM
    trailing_active: bool = False        # Becomes True after activation threshold
    high_watermark: float = 0.0          # Best price since entry

    # ── State ───────────────────────────────────────────────────────────
    exit_pending: bool = False
    exit_reason: str | None = None
    unrealized_pnl: float = 0.0
    unrealized_pnl_pct: float = 0.0
    last_price: float = 0.0

    # ── Metadata ────────────────────────────────────────────────────────
    regime_at_entry: str = ""
    structure_grade: str = ""
    client_order_id: str = ""
    correlation_id: str = ""

    # ── Partial Profit-Taking ──────────────────────────────────────────
    allow_partial_take: bool = False     # Strategy enables partial take?
    partial_taken: bool = False          # Has partial been executed?
    partial_take_pct: float = 0.50       # Fraction of shares to sell
    partial_trigger_pct: float = 0.005   # Favorable move to trigger
    original_shares: int = 0             # Shares at entry (before partial)
    partial_take_pnl: float = 0.0        # Realized P&L from partial sell

    # ── Microstructure State ─────────────────────────────────────────────
    ssr_active_at_entry: bool = False    # Was SSR active when we entered?
    htb_at_entry: bool = False           # Was HTB when we entered?
    disable_trailing: bool = False       # SSR/microstructure override
    last_new_high_ts: float = 0.0        # For SSR time-based failsafe
    consecutive_rejections: int = 0      # For SSR uptick exhaustion

    def __post_init__(self):
        from datetime import timedelta
        if self.high_watermark == 0.0:
            self.high_watermark = self.entry_price
        if self.original_shares == 0:
            self.original_shares = self.shares
        # Calculate max exit timestamp
        self.max_exit_ts = self.entry_time + timedelta(seconds=self.max_hold_seconds)


class PaperPositionManager:
    """
    Exit Management v1 - Paper Position Manager.

    Core responsibility: Ensure every trade has a complete exit plan.
    No trade executes without all three exit layers defined.

    Wired into MorpheusServer.emit_event() to receive all events.
    On QUOTE_UPDATE, evaluates exit conditions for all positions.
    On ORDER_FILL_RECEIVED (buy), opens position with exit plan.
    On ORDER_FILL_RECEIVED (sell), closes position and records P&L.
    """

    def __init__(
        self,
        emit_event: Callable[[Event], Awaitable[None]],
        config: ExitConfig | None = None,
        kill_switch_ref: Callable[[], bool] | None = None,
        get_runtime_config: Callable[[], Any] | None = None,
    ):
        self._emit = emit_event
        self._config = config or ExitConfig()
        self._kill_switch_ref = kill_switch_ref
        self._get_runtime_config = get_runtime_config  # Hot-reload support

        # Open positions: symbol -> PaperPosition
        self._positions: dict[str, PaperPosition] = {}

        # Closed positions (for P&L reporting)
        self._closed_positions: list[dict[str, Any]] = []

        # Daily P&L tracking
        self._daily_realized_pnl: float = 0.0
        self._daily_unrealized_pnl: float = 0.0
        self._trade_count: int = 0
        self._win_count: int = 0

        # Exit reason counts (for EOD report)
        self._exit_counts: dict[str, int] = {
            "TIME_STOP": 0,
            "HARD_STOP": 0,
            "TRAIL_STOP": 0,
            "KILL_SWITCH": 0,
            "MANUAL": 0,
            # Microstructure exits
            "SSR_MOMENTUM_DECAY": 0,
            "SSR_BID_COLLAPSE": 0,
            "SSR_UPTICK_EXHAUSTION": 0,
            "SSR_SPREAD_BLOWOUT": 0,
            "SSR_JACKKNIFE": 0,
            "SSR_TIME_FAILSAFE": 0,
            "HTB_PROFIT_PROTECT": 0,
            "HALT_EXIT": 0,
            "POST_HALT_FAILURE": 0,
            "LIQUIDITY_EVAPORATION": 0,
        }

        # Current regime mode (informational only, not used for exits in v1)
        self._current_regime_mode: str = "NORMAL"

        # Signal metadata cache: symbol -> {strategy_name, stop, target, ...}
        self._signal_metadata: dict[str, dict[str, Any]] = {}

        logger.info("[EXIT_MGR] Exit Management v1 initialized - all trades require exit plan")

    # ─── Exit Config (Hot Reload) ──────────────────────────────────────

    def _get_exit_config(self) -> ExitConfig:
        """Get current exit config, with hot-reload from runtime config."""
        if self._get_runtime_config:
            try:
                rc = self._get_runtime_config()
                return ExitConfig(
                    max_hold_seconds=rc.max_hold_seconds,
                    hard_stop_pct=rc.hard_stop_pct,
                    trail_activation_pct=rc.trail_activation_pct,
                    trail_distance_pct=rc.trail_distance_pct,
                    strategy_max_hold=rc.strategy_max_hold,
                    strategy_trail_activation=getattr(rc, 'strategy_trail_activation', {}),
                    strategy_partial_take=getattr(rc, 'strategy_partial_take', {}),
                )
            except Exception:
                pass
        return self._config

    # ─── Exit Plan Validation (CRITICAL GATE) ──────────────────────────

    def validate_exit_plan(
        self,
        symbol: str,
        entry_price: float,
        direction: str,
        strategy_name: str,
        invalidation_reference: float | None = None,
    ) -> ExitPlan | None:
        """
        Create and validate exit plan for a trade.

        Returns ExitPlan if valid, None if trade should be rejected.

        This is the permission-to-trade gate. All three exit layers
        must be computable for the trade to proceed.
        """
        config = self._get_exit_config()

        if entry_price <= 0:
            logger.warning(f"[EXIT_MGR] {symbol}: Invalid entry price {entry_price}")
            return None

        # Layer 1: Time failsafe (MANDATORY)
        max_hold = config.strategy_max_hold.get(
            strategy_name.lower().replace(" ", "_"),
            config.max_hold_seconds
        )
        if max_hold <= 0:
            max_hold = config.max_hold_seconds
        if max_hold <= 0:
            max_hold = 300.0  # Absolute fallback

        # Layer 2: Hard stop
        if invalidation_reference and invalidation_reference > 0:
            # Use strategy-computed stop
            hard_stop_price = invalidation_reference
        else:
            # Compute from hard_stop_pct
            if direction == "long":
                hard_stop_price = entry_price * (1 - config.hard_stop_pct)
            else:  # short
                hard_stop_price = entry_price * (1 + config.hard_stop_pct)

        # Validate hard stop makes sense
        if direction == "long" and hard_stop_price >= entry_price:
            logger.warning(
                f"[EXIT_MGR] {symbol}: Invalid long stop {hard_stop_price} >= entry {entry_price}"
            )
            return None
        if direction == "short" and hard_stop_price <= entry_price:
            logger.warning(
                f"[EXIT_MGR] {symbol}: Invalid short stop {hard_stop_price} <= entry {entry_price}"
            )
            return None

        # Layer 3: Trailing stop parameters (strategy-aware)
        strategy_key = strategy_name.lower().replace(" ", "_")
        trail_activation = config.strategy_trail_activation.get(
            strategy_key, config.trail_activation_pct
        )
        trail_distance = config.trail_distance_pct

        if trail_activation <= 0 or trail_distance <= 0:
            logger.warning(
                f"[EXIT_MGR] {symbol}: Invalid trail params "
                f"activation={trail_activation} distance={trail_distance}"
            )
            return None

        # Partial profit-take config (strategy-specific)
        partial_cfg = config.strategy_partial_take.get(strategy_key, {})
        allow_partial = partial_cfg.get("enabled", False)
        partial_take_pct = partial_cfg.get("take_pct", 0.50)
        partial_trigger_pct = partial_cfg.get("trigger_pct", 0.005)

        # Create exit plan
        plan = ExitPlan(
            max_hold_seconds=max_hold,
            hard_stop_price=hard_stop_price,
            trail_activation_pct=trail_activation,
            trail_distance_pct=trail_distance,
            strategy_name=strategy_name,
            direction=direction,
            allow_partial_take=allow_partial,
            partial_take_pct=partial_take_pct,
            partial_trigger_pct=partial_trigger_pct,
        )

        # Final validation
        is_valid, reason = plan.is_valid()
        if not is_valid:
            logger.warning(f"[EXIT_MGR] {symbol}: Exit plan invalid - {reason}")
            return None

        partial_str = f" partial={partial_take_pct:.0%}@{partial_trigger_pct:.1%}" if allow_partial else ""
        logger.info(
            f"[EXIT_MGR] {symbol}: Exit plan valid - "
            f"max_hold={max_hold:.0f}s stop=${hard_stop_price:.4f} "
            f"trail_act={trail_activation:.1%} trail_dist={trail_distance:.1%}{partial_str}"
        )

        return plan

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
        """
        Open a new paper position from a buy fill event.

        Exit plan has already been validated before order submission.
        This method trusts the metadata set by set_signal_metadata().
        """
        payload = event.payload or {}
        symbol = event.symbol or payload.get("symbol", "")

        if not symbol:
            return

        # Skip if already holding
        if symbol in self._positions:
            logger.warning(f"[EXIT_MGR] Already holding {symbol}, ignoring duplicate fill")
            return

        shares = payload.get("filled_quantity", payload.get("quantity", 0))
        fill_price = payload.get("fill_price", 0)
        client_order_id = payload.get("client_order_id", "")
        correlation_id = event.correlation_id or ""

        if shares <= 0 or fill_price <= 0:
            return

        # Get validated exit plan from metadata
        meta = self._signal_metadata.pop(symbol, {})
        strategy_name = meta.get("strategy_name", "unknown")
        direction = meta.get("direction", "long")
        regime = meta.get("regime", self._current_regime_mode)
        structure_grade = meta.get("structure_grade", "")

        # Exit plan parameters (already validated before order)
        hard_stop_price = meta.get("hard_stop_price", fill_price * 0.97)
        max_hold_seconds = meta.get("max_hold_seconds", 300.0)
        trail_activation_pct = meta.get("trail_activation_pct", 0.01)
        trail_distance_pct = meta.get("trail_distance_pct", 0.015)

        # Partial profit-take flags from exit plan
        allow_partial_take = meta.get("allow_partial_take", False)
        partial_take_pct = meta.get("partial_take_pct", 0.50)
        partial_trigger_pct = meta.get("partial_trigger_pct", 0.005)

        # Microstructure flags from gate checks
        ssr_active_at_entry = meta.get("ssr_active_at_entry", False)
        htb_at_entry = meta.get("htb_at_entry", False)
        disable_trailing = meta.get("disable_trailing", False)

        position = PaperPosition(
            position_id=str(uuid.uuid4()),
            symbol=symbol,
            shares=shares,
            direction=direction,
            entry_price=fill_price,
            entry_time=datetime.now(timezone.utc),
            strategy_name=strategy_name,
            max_hold_seconds=max_hold_seconds,
            hard_stop_price=hard_stop_price,
            trail_activation_pct=trail_activation_pct,
            trail_distance_pct=trail_distance_pct,
            trailing_active=False,
            high_watermark=fill_price,
            # Partial profit-take
            allow_partial_take=allow_partial_take,
            partial_take_pct=partial_take_pct,
            partial_trigger_pct=partial_trigger_pct,
            original_shares=shares,
            # Metadata
            regime_at_entry=regime,
            structure_grade=structure_grade,
            client_order_id=client_order_id,
            correlation_id=correlation_id,
            last_price=fill_price,
            # Microstructure flags
            ssr_active_at_entry=ssr_active_at_entry,
            htb_at_entry=htb_at_entry,
            disable_trailing=disable_trailing,
            last_new_high_ts=time.time() if not disable_trailing else 0.0,
        )

        self._positions[symbol] = position

        # Build microstructure flags string
        micro_flags = []
        if ssr_active_at_entry:
            micro_flags.append("SSR")
        if htb_at_entry:
            micro_flags.append("HTB")
        if disable_trailing:
            micro_flags.append("NO_TRAIL")
        micro_str = f" | flags=[{','.join(micro_flags)}]" if micro_flags else ""

        partial_str = f" | PARTIAL={partial_take_pct:.0%}@{partial_trigger_pct:.1%}" if allow_partial_take else ""
        logger.info(
            f"[EXIT_MGR] TRADE OPENED: {symbol} {direction.upper()} "
            f"{shares} shares @ ${fill_price:.4f} | "
            f"HARD_STOP=${hard_stop_price:.4f} | "
            f"max_hold={max_hold_seconds:.0f}s | "
            f"trail_act={trail_activation_pct:.1%} trail_dist={trail_distance_pct:.1%}"
            f"{partial_str}{micro_str} | "
            f"strategy={strategy_name}"
        )

        # Emit TRADE_ACTIVE event with full exit plan
        await self._emit(create_event(
            EventType.TRADE_ACTIVE,
            payload={
                "position_id": position.position_id,
                "symbol": symbol,
                "direction": direction,
                "shares": shares,
                "entry_price": fill_price,
                "strategy_name": strategy_name,
                # Exit plan details (all three layers)
                "exit_plan": {
                    "hard_stop_price": hard_stop_price,
                    "max_hold_seconds": max_hold_seconds,
                    "max_exit_ts": position.max_exit_ts.isoformat(),
                    "trail_activation_pct": trail_activation_pct,
                    "trail_distance_pct": trail_distance_pct,
                    "allow_partial_take": allow_partial_take,
                    "partial_take_pct": partial_take_pct,
                    "partial_trigger_pct": partial_trigger_pct,
                },
                # Microstructure status at entry
                "microstructure": {
                    "ssr_active": ssr_active_at_entry,
                    "htb": htb_at_entry,
                    "trailing_disabled": disable_trailing,
                },
            },
            symbol=symbol,
            correlation_id=correlation_id,
        ))

    # ─── Exit Monitoring ──────────────────────────────────────────────

    async def _check_exits(self, event: Event) -> None:
        """
        Evaluate exit conditions for open positions on each quote update.

        Exit Priority (checked in order):
        1. Kill switch → exit all immediately
        2. Hard stop → price hits hard_stop_price
        3. Trailing stop → price drops trail_distance_pct from HWM (after activation)
        4. Time stop → position held > max_hold_seconds (MANDATORY, cannot be disabled)
        """
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

        # ── Update watermark and P&L ──
        if position.direction == "long":
            if price > position.high_watermark:
                position.high_watermark = price
            position.unrealized_pnl = (price - position.entry_price) * position.shares
        else:  # short
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

        # ══════════════════════════════════════════════════════════════════
        # EXIT PRIORITY ORDER
        # ══════════════════════════════════════════════════════════════════

        # 1. KILL SWITCH (emergency exit)
        if self._kill_switch_ref and self._kill_switch_ref():
            await self._execute_exit(position, price, ExitReason.KILL_SWITCH)
            return

        # 2. HARD STOP (risk definition - cannot be widened)
        if position.direction == "long" and price <= position.hard_stop_price:
            await self._execute_exit(position, price, ExitReason.HARD_STOP)
            return
        elif position.direction == "short" and price >= position.hard_stop_price:
            await self._execute_exit(position, price, ExitReason.HARD_STOP)
            return

        # 2.5. PARTIAL PROFIT TAKE (size reduction, NOT a full exit)
        if (position.allow_partial_take
                and not position.partial_taken
                and not position.trailing_active
                and not position.disable_trailing
                and position.shares > 1):
            pct_move = (price - position.entry_price) / position.entry_price
            if position.direction == "short":
                pct_move = -pct_move
            if pct_move >= position.partial_trigger_pct:
                await self._execute_partial_take(position, price)
                # Do NOT return - continue to check trailing/time stops

        # 3. TRAILING STOP (profit protection - only after activation)
        if position.trailing_active:
            if position.direction == "long":
                trail_level = position.high_watermark * (1 - position.trail_distance_pct)
                if price <= trail_level:
                    await self._execute_exit(position, price, ExitReason.TRAIL_STOP)
                    return
            else:  # short
                trail_level = position.high_watermark * (1 + position.trail_distance_pct)
                if price >= trail_level:
                    await self._execute_exit(position, price, ExitReason.TRAIL_STOP)
                    return

        # Check for trailing activation (after favorable move)
        if not position.trailing_active:
            pct_move = (price - position.entry_price) / position.entry_price
            if position.direction == "short":
                pct_move = -pct_move  # Invert for shorts

            if pct_move >= position.trail_activation_pct:
                position.trailing_active = True
                # Reset HWM to current price when trailing activates
                position.high_watermark = price
                logger.info(
                    f"[EXIT_MGR] TRAILING ACTIVATED: {symbol} "
                    f"+{pct_move:.2%} move @ ${price:.4f} | "
                    f"HWM=${position.high_watermark:.4f} "
                    f"trail_dist={position.trail_distance_pct:.1%}"
                )

        # 4. TIME STOP (MANDATORY - cannot be disabled)
        elapsed = (now - position.entry_time).total_seconds()
        if elapsed >= position.max_hold_seconds:
            await self._execute_exit(position, price, ExitReason.TIME_STOP)
            return

    # ─── Partial Profit-Taking ─────────────────────────────────────────

    async def _execute_partial_take(self, position: PaperPosition, current_price: float) -> None:
        """
        Execute partial profit-taking: sell a fraction, arm trailing on remainder.

        This is NOT a full exit - the position stays open with reduced shares.
        """
        shares_to_sell = max(1, int(position.shares * position.partial_take_pct))
        # Always keep at least 1 share for trailing
        if shares_to_sell >= position.shares:
            shares_to_sell = position.shares - 1
        if shares_to_sell < 1:
            return  # Can't split - skip partial

        symbol = position.symbol

        # Compute partial P&L
        if position.direction == "long":
            partial_pnl = (current_price - position.entry_price) * shares_to_sell
        else:
            partial_pnl = (position.entry_price - current_price) * shares_to_sell

        # Update position state
        position.partial_taken = True
        position.partial_take_pnl = partial_pnl
        remaining = position.shares - shares_to_sell
        position.shares = remaining

        # Activate trailing on remainder
        position.trailing_active = True
        position.high_watermark = current_price

        # Update daily P&L for the realized partial
        self._daily_realized_pnl += partial_pnl

        side = "sell" if position.direction == "long" else "buy"
        client_order_id = f"partial_{uuid.uuid4().hex[:8]}"
        correlation_id = f"partial_{position.position_id[:8]}"

        logger.info(
            f"[EXIT_MGR] PARTIAL_TAKE: {symbol} sold {shares_to_sell}/{position.original_shares} shares "
            f"@ ${current_price:.4f} | P&L=${partial_pnl:+.2f} | "
            f"remaining={remaining} | trailing now ACTIVE | "
            f"strategy={position.strategy_name}"
        )

        # Emit ORDER_SUBMITTED (partial sell)
        await self._emit(create_event(
            EventType.ORDER_SUBMITTED,
            payload={
                "client_order_id": client_order_id,
                "symbol": symbol,
                "side": side,
                "quantity": shares_to_sell,
                "order_type": "market",
                "auto_confirmed": True,
                "exit_reason": "PARTIAL_TAKE",
                "position_id": position.position_id,
                "partial": True,
            },
            symbol=symbol,
            correlation_id=correlation_id,
        ))

        await asyncio.sleep(0.05)

        # Emit ORDER_FILL_RECEIVED (partial sell)
        await self._emit(create_event(
            EventType.ORDER_FILL_RECEIVED,
            payload={
                "client_order_id": client_order_id,
                "symbol": symbol,
                "side": side,
                "filled_quantity": shares_to_sell,
                "fill_price": current_price,
                "exec_id": f"exec_partial_{uuid.uuid4().hex[:8]}",
                "auto_confirmed": True,
                "exit_reason": "PARTIAL_TAKE",
                "position_id": position.position_id,
                "partial": True,
            },
            symbol=symbol,
            correlation_id=correlation_id,
        ))

    # ─── Exit Execution ───────────────────────────────────────────────

    async def _execute_exit(
        self, position: PaperPosition, exit_price: float, reason: ExitReason
    ) -> None:
        """
        Execute an exit by emitting sell order events.

        Emits: TRADE_EXITED, ORDER_SUBMITTED (sell), ORDER_FILL_RECEIVED (sell)
        """
        if position.exit_pending:
            return
        position.exit_pending = True
        position.exit_reason = reason.value

        symbol = position.symbol
        side = "sell" if position.direction == "long" else "buy"  # Cover for shorts
        client_order_id = f"exit_{reason.value}_{uuid.uuid4().hex[:8]}"
        correlation_id = f"exit_{position.position_id[:8]}"

        # Compute P&L (remaining shares only)
        if position.direction == "long":
            remaining_pnl = (exit_price - position.entry_price) * position.shares
        else:
            remaining_pnl = (position.entry_price - exit_price) * position.shares

        # Total trade P&L = partial sell P&L + remaining shares P&L
        total_pnl = remaining_pnl + position.partial_take_pnl
        total_shares = position.original_shares if position.partial_taken else position.shares
        pnl_pct = (total_pnl / (position.entry_price * total_shares)) * 100 if position.entry_price > 0 else 0.0
        hold_seconds = (datetime.now(timezone.utc) - position.entry_time).total_seconds()

        # Track exit reason counts
        if reason.value in self._exit_counts:
            self._exit_counts[reason.value] += 1

        partial_str = f" (partial P&L=${position.partial_take_pnl:+.2f})" if position.partial_taken else ""
        logger.info(
            f"[EXIT_MGR] {reason.value}: {symbol} "
            f"{position.shares} shares @ ${exit_price:.4f} | "
            f"entry=${position.entry_price:.4f} total_P&L=${total_pnl:+.2f} ({pnl_pct:+.2f}%){partial_str} | "
            f"held {hold_seconds:.0f}s/{position.max_hold_seconds:.0f}s max | "
            f"strategy={position.strategy_name}"
        )

        # Emit TRADE_EXITED event (Exit v1 required event)
        await self._emit(create_event(
            EventType.TRADE_EXITED,
            payload={
                "position_id": position.position_id,
                "symbol": symbol,
                "exit_reason": reason.value,
                "exit_price": exit_price,
                "entry_price": position.entry_price,
                "shares": position.shares,
                "original_shares": position.original_shares,
                "direction": position.direction,
                "pnl": total_pnl,
                "pnl_pct": pnl_pct,
                "hold_time": hold_seconds,
                "max_hold_seconds": position.max_hold_seconds,
                "strategy_name": position.strategy_name,
                "trailing_was_active": position.trailing_active,
                "high_watermark": position.high_watermark,
                "hard_stop_price": position.hard_stop_price,
                "partial_taken": position.partial_taken,
                "partial_take_pnl": position.partial_take_pnl,
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

        # PARTIAL SELL: already handled in _execute_partial_take, don't close
        if payload.get("partial", False):
            return

        position = self._positions[symbol]
        exit_price = payload.get("fill_price", position.last_price)
        exit_reason = payload.get("exit_reason", "unknown")

        # Compute final P&L (remaining shares + partial P&L)
        if position.direction == "long":
            remaining_pnl = (exit_price - position.entry_price) * position.shares
        else:
            remaining_pnl = (position.entry_price - exit_price) * position.shares

        # Total trade P&L includes partial sell P&L
        realized_pnl = remaining_pnl + position.partial_take_pnl

        total_shares = position.original_shares if position.partial_taken else position.shares
        pnl_pct = (
            (realized_pnl / (position.entry_price * total_shares)) * 100
            if position.entry_price > 0 else 0.0
        )
        hold_seconds = (datetime.now(timezone.utc) - position.entry_time).total_seconds()

        # Update daily totals (partial P&L already counted, only add remaining)
        self._daily_realized_pnl += remaining_pnl
        self._trade_count += 1
        if realized_pnl > 0:
            self._win_count += 1

        # Record closed position
        closed_record = {
            "position_id": position.position_id,
            "symbol": symbol,
            "direction": position.direction,
            "shares": position.original_shares,
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
            "partial_taken": position.partial_taken,
            "partial_take_pnl": position.partial_take_pnl,
        }
        self._closed_positions.append(closed_record)

        # Remove from open positions
        del self._positions[symbol]

        logger.info(
            f"[POSITION_MGR] CLOSED: {symbol} P&L=${realized_pnl:+.2f} ({pnl_pct:+.2f}%) "
            f"reason={exit_reason} | Daily: ${self._daily_realized_pnl:+.2f} "
            f"trades={self._trade_count} wins={self._win_count}"
        )

        # ═══════════════════════════════════════════════════════════════════
        # VALIDATION MODE: Log exit verification for microstructure analysis
        # ═══════════════════════════════════════════════════════════════════
        if is_validation_mode():
            val_logger = get_validation_logger()
            if val_logger:
                val_logger.log_exit_verification(
                    symbol=symbol,
                    exit_reason=exit_reason,
                    ssr_active_at_entry=position.ssr_active_at_entry,
                    htb_at_entry=position.htb_at_entry,
                    halt_related=exit_reason in {
                        "HALT_EXIT", "POST_HALT_FAILURE", ExitReason.HALT_EXIT.value,
                        ExitReason.POST_HALT_FAILURE.value,
                    },
                    hold_time_seconds=hold_seconds,
                    profit=realized_pnl,
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
            is_paper_mode=True,  # Explicitly mark as paper mode data
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
        now = datetime.now(timezone.utc)
        for pos in self._positions.values():
            hold_seconds = (now - pos.entry_time).total_seconds()
            time_remaining = max(0, pos.max_hold_seconds - hold_seconds)
            result.append({
                "position_id": pos.position_id,
                "symbol": pos.symbol,
                "direction": pos.direction,
                "shares": pos.shares,
                "entry_price": pos.entry_price,
                "last_price": pos.last_price,
                # Exit plan
                "hard_stop_price": pos.hard_stop_price,
                "max_hold_seconds": pos.max_hold_seconds,
                "trail_activation_pct": pos.trail_activation_pct,
                "trail_distance_pct": pos.trail_distance_pct,
                # State
                "unrealized_pnl": round(pos.unrealized_pnl, 2),
                "unrealized_pnl_pct": round(pos.unrealized_pnl_pct, 2),
                "trailing_active": pos.trailing_active,
                "high_watermark": pos.high_watermark,
                "hold_seconds": hold_seconds,
                "time_remaining_seconds": time_remaining,
                "strategy_name": pos.strategy_name,
                "entry_time": pos.entry_time.isoformat(),
                # Partial take state
                "partial_taken": pos.partial_taken,
                "original_shares": pos.original_shares,
                "partial_take_pnl": round(pos.partial_take_pnl, 2),
            })
        return result

    def get_closed_trades(self) -> list[dict[str, Any]]:
        """Get all closed trades for the day."""
        return list(self._closed_positions)

    def get_daily_stats(self) -> dict[str, Any]:
        """Get daily trading statistics including exit reason breakdown."""
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
            # Exit v1 required: exit reason breakdown
            "exit_counts": dict(self._exit_counts),
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
        # Reset exit counts
        for key in self._exit_counts:
            self._exit_counts[key] = 0
        logger.info("[EXIT_MGR] Daily stats reset")

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
