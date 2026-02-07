"""
Symbol State - Single source of truth for market microstructure status.

Tracks per-symbol status including:
- Data freshness (staleness)
- Halt status (HALTED/NORMAL/CLOSED)
- Borrowability (ETB/HTB/NTB)
- SSR (Short Sale Restriction) - computed from price vs prior close

These states are used by microstructure gates to:
- Prevent toxic entries
- Exit earlier when liquidity evaporates
- Capitalize on squeeze/volatility regimes
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Awaitable

logger = logging.getLogger(__name__)


class SecurityStatus(str, Enum):
    """Security trading status."""
    NORMAL = "NORMAL"
    HALTED = "HALTED"
    CLOSED = "CLOSED"
    UNKNOWN = "UNKNOWN"


class BorrowStatus(str, Enum):
    """Stock borrowability status for shorting."""
    ETB = "ETB"      # Easy To Borrow - normal liquidity
    HTB = "HTB"      # Hard To Borrow - squeeze-prone, scarce borrow
    NTB = "NTB"      # Not Borrowable - no shorting possible
    UNKNOWN = "UNKNOWN"


class BorrowSource(str, Enum):
    """How borrow status was determined."""
    API = "API"           # From broker API
    REJECTION = "REJECTION"  # Inferred from order rejection
    MANUAL = "MANUAL"     # Manual override


@dataclass
class SymbolState:
    """
    Per-symbol microstructure state.

    This is the single source of truth for:
    - Data freshness
    - Halt/trading status
    - Borrowability
    - SSR status

    All fields are updated in real-time and used by microstructure gates.
    """

    symbol: str

    # ── Data Freshness ──────────────────────────────────────────────────
    last_quote_ts: float | None = None  # Unix timestamp of last quote
    data_stale: bool = False            # Derived: now - last_quote_ts > threshold

    # ── Halt Status ─────────────────────────────────────────────────────
    security_status: SecurityStatus = SecurityStatus.UNKNOWN
    halted: bool = False
    halt_code: str | None = None        # e.g., "T1", "H10", etc.
    halted_at: float | None = None      # Unix timestamp when halt started
    resumed_at: float | None = None     # Unix timestamp when resumed
    post_halt_cooldown_until: float | None = None  # Cooldown expiry timestamp

    # ── Borrowability ───────────────────────────────────────────────────
    borrow_status: BorrowStatus = BorrowStatus.UNKNOWN
    borrow_source: BorrowSource = BorrowSource.API
    borrow_last_updated: float | None = None

    # ── SSR (Short Sale Restriction) ────────────────────────────────────
    prior_close: float | None = None    # Previous day's close
    ssr_active: bool = False            # Computed: last <= 0.90 * prior_close
    ssr_triggered_at: float | None = None
    ssr_liquidity_risk: bool = False    # SSR + long position open = exit trap risk

    # ── Timestamps ──────────────────────────────────────────────────────
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        """Serialize for events/logging."""
        return {
            "symbol": self.symbol,
            "last_quote_ts": self.last_quote_ts,
            "data_stale": self.data_stale,
            "security_status": self.security_status.value,
            "halted": self.halted,
            "halt_code": self.halt_code,
            "halted_at": self.halted_at,
            "resumed_at": self.resumed_at,
            "post_halt_cooldown_until": self.post_halt_cooldown_until,
            "borrow_status": self.borrow_status.value,
            "borrow_source": self.borrow_source.value,
            "borrow_last_updated": self.borrow_last_updated,
            "prior_close": self.prior_close,
            "ssr_active": self.ssr_active,
            "ssr_triggered_at": self.ssr_triggered_at,
            "ssr_liquidity_risk": self.ssr_liquidity_risk,
            "updated_at": self.updated_at,
        }


# Configuration defaults
DEFAULT_STALE_QUOTE_SECONDS = 120.0  # 2 minutes
DEFAULT_POST_HALT_COOLDOWN_SECONDS = 60.0
DEFAULT_POST_HALT_MAX_SPREAD_PCT = 0.02  # 2%
DEFAULT_POST_HALT_MIN_PRINTS = 5
DEFAULT_HTB_SHORT_SIZE_HAIRCUT = 0.5  # 50% size reduction
DEFAULT_SSR_SPREAD_THRESHOLD_PCT = 0.015  # 1.5%
DEFAULT_BORROW_STATUS_TTL_SECONDS = 6 * 3600  # 6 hours


@dataclass
class MicrostructureConfig:
    """Configuration for microstructure gates."""

    # Data staleness
    stale_quote_seconds: float = DEFAULT_STALE_QUOTE_SECONDS

    # Halt recovery
    post_halt_cooldown_seconds: float = DEFAULT_POST_HALT_COOLDOWN_SECONDS
    post_halt_max_spread_pct: float = DEFAULT_POST_HALT_MAX_SPREAD_PCT
    post_halt_min_prints: int = DEFAULT_POST_HALT_MIN_PRINTS

    # Borrowability
    htb_short_size_haircut: float = DEFAULT_HTB_SHORT_SIZE_HAIRCUT
    htb_room_to_profit_relaxation: float = 0.15  # +15% threshold relaxation
    borrow_status_ttl_seconds: float = DEFAULT_BORROW_STATUS_TTL_SECONDS

    # SSR
    ssr_spread_threshold_pct: float = DEFAULT_SSR_SPREAD_THRESHOLD_PCT
    ssr_max_hold_no_new_high_seconds: float = 45.0  # Time-based failsafe

    # SSR exit triggers (any one triggers exit intent)
    ssr_exit_velocity_decay_threshold: float = -0.3  # ROC decay
    ssr_exit_spread_widen_multiplier: float = 2.0    # Spread doubles
    ssr_exit_consecutive_rejections: int = 3         # Failed continuations


class SymbolStateManager:
    """
    Manages symbol states across the system.

    Responsibilities:
    - Track per-symbol microstructure state
    - Update states from quotes, halts, rejections
    - Compute derived fields (data_stale, ssr_active)
    - Emit status update events
    """

    def __init__(
        self,
        config: MicrostructureConfig | None = None,
        emit_event: Callable[[Any], Awaitable[None]] | None = None,
    ):
        self._config = config or MicrostructureConfig()
        self._emit = emit_event

        # Symbol -> SymbolState
        self._states: dict[str, SymbolState] = {}

        # Rate limiting for data_stale events (avoid flood)
        self._last_stale_event: dict[str, float] = {}
        self._stale_event_interval = 30.0  # seconds between stale events per symbol

        logger.info("[SYMBOL_STATE] Manager initialized")

    def get_or_create(self, symbol: str) -> SymbolState:
        """Get existing state or create new one."""
        if symbol not in self._states:
            self._states[symbol] = SymbolState(symbol=symbol)
        return self._states[symbol]

    def get(self, symbol: str) -> SymbolState | None:
        """Get state if exists."""
        return self._states.get(symbol)

    def update_quote(
        self,
        symbol: str,
        last_price: float,
        bid: float | None = None,
        ask: float | None = None,
        spread_pct: float | None = None,
    ) -> SymbolState:
        """
        Update state from a quote update.

        - Updates last_quote_ts
        - Computes data_stale
        - Computes ssr_active if prior_close is set
        """
        state = self.get_or_create(symbol)
        now = time.time()

        state.last_quote_ts = now
        state.data_stale = False  # Just got fresh data
        state.updated_at = now

        # Compute SSR status
        if state.prior_close and state.prior_close > 0 and last_price > 0:
            ssr_threshold = state.prior_close * 0.90
            was_ssr = state.ssr_active
            state.ssr_active = last_price <= ssr_threshold

            if state.ssr_active and not was_ssr:
                state.ssr_triggered_at = now
                logger.info(
                    f"[SYMBOL_STATE] SSR TRIGGERED: {symbol} "
                    f"last=${last_price:.4f} <= 90% of prior_close=${state.prior_close:.4f}"
                )

        return state

    def check_staleness(self, symbol: str) -> bool:
        """
        Check if symbol data is stale.

        Returns True if stale, updates state accordingly.
        """
        state = self.get(symbol)
        if not state:
            return True  # No state = definitely stale

        now = time.time()

        if state.last_quote_ts is None:
            state.data_stale = True
            return True

        age = now - state.last_quote_ts
        was_stale = state.data_stale
        state.data_stale = age > self._config.stale_quote_seconds

        # Log transition to stale
        if state.data_stale and not was_stale:
            logger.warning(
                f"[SYMBOL_STATE] DATA_STALE: {symbol} "
                f"last_quote={age:.1f}s ago (threshold={self._config.stale_quote_seconds}s)"
            )

        return state.data_stale

    def set_prior_close(self, symbol: str, prior_close: float) -> None:
        """Set prior day's close for SSR calculation."""
        state = self.get_or_create(symbol)
        state.prior_close = prior_close
        state.updated_at = time.time()

    def set_halt(
        self,
        symbol: str,
        halted: bool,
        halt_code: str | None = None,
    ) -> SymbolState:
        """
        Update halt status.

        When halted: sets halted=True, halted_at timestamp
        When resumed: sets halted=False, resumed_at, post_halt_cooldown_until
        """
        state = self.get_or_create(symbol)
        now = time.time()

        was_halted = state.halted
        state.halted = halted
        state.halt_code = halt_code
        state.updated_at = now

        if halted:
            state.security_status = SecurityStatus.HALTED
            if not was_halted:
                state.halted_at = now
                logger.warning(f"[SYMBOL_STATE] HALTED: {symbol} code={halt_code}")
        else:
            state.security_status = SecurityStatus.NORMAL
            if was_halted:
                state.resumed_at = now
                state.post_halt_cooldown_until = now + self._config.post_halt_cooldown_seconds
                logger.info(
                    f"[SYMBOL_STATE] RESUMED: {symbol} "
                    f"cooldown until {state.post_halt_cooldown_until:.0f}"
                )

        return state

    def update_borrow_status(
        self,
        symbol: str,
        status: BorrowStatus,
        source: BorrowSource = BorrowSource.API,
    ) -> SymbolState:
        """
        Update borrowability status.

        Can be set from API, order rejection parsing, or manual override.
        """
        state = self.get_or_create(symbol)
        now = time.time()

        old_status = state.borrow_status
        state.borrow_status = status
        state.borrow_source = source
        state.borrow_last_updated = now
        state.updated_at = now

        if old_status != status:
            logger.info(
                f"[SYMBOL_STATE] BORROW_STATUS: {symbol} "
                f"{old_status.value} -> {status.value} (source={source.value})"
            )

        return state

    def parse_rejection_for_borrow_status(
        self,
        symbol: str,
        rejection_reason: str,
    ) -> BorrowStatus | None:
        """
        Infer borrow status from order rejection message.

        Returns the inferred status, or None if not a borrow-related rejection.
        """
        reason_upper = rejection_reason.upper()

        # NTB patterns
        ntb_patterns = [
            "NOT_AVAILABLE_TO_BORROW",
            "NTB",
            "NO_SHARES",
            "NO SHARES AVAILABLE",
            "CANNOT LOCATE",
            "UNABLE TO BORROW",
        ]
        for pattern in ntb_patterns:
            if pattern in reason_upper:
                self.update_borrow_status(symbol, BorrowStatus.NTB, BorrowSource.REJECTION)
                return BorrowStatus.NTB

        # HTB patterns
        htb_patterns = [
            "HARD_TO_BORROW",
            "HTB",
            "LIMITED AVAILABILITY",
            "HIGH BORROW COST",
            "LOCATE REQUIRED",
        ]
        for pattern in htb_patterns:
            if pattern in reason_upper:
                self.update_borrow_status(symbol, BorrowStatus.HTB, BorrowSource.REJECTION)
                return BorrowStatus.HTB

        return None

    def set_ssr_liquidity_risk(self, symbol: str, has_long_position: bool) -> None:
        """
        Update SSR liquidity risk flag.

        SSR + long position = exit trap risk (liquidity can vanish).
        """
        state = self.get_or_create(symbol)
        state.ssr_liquidity_risk = state.ssr_active and has_long_position
        state.updated_at = time.time()

    def is_in_post_halt_cooldown(self, symbol: str) -> bool:
        """Check if symbol is in post-halt cooldown period."""
        state = self.get(symbol)
        if not state:
            return False

        if state.post_halt_cooldown_until is None:
            return False

        return time.time() < state.post_halt_cooldown_until

    def is_borrow_status_stale(self, symbol: str) -> bool:
        """Check if borrow status needs refresh."""
        state = self.get(symbol)
        if not state or state.borrow_last_updated is None:
            return True

        age = time.time() - state.borrow_last_updated
        return age > self._config.borrow_status_ttl_seconds

    def get_all_states(self) -> dict[str, SymbolState]:
        """Get all symbol states."""
        return dict(self._states)

    def get_stale_symbols(self) -> list[str]:
        """Get list of symbols with stale data."""
        return [s for s, state in self._states.items() if state.data_stale]

    def get_halted_symbols(self) -> list[str]:
        """Get list of halted symbols."""
        return [s for s, state in self._states.items() if state.halted]

    def get_ssr_symbols(self) -> list[str]:
        """Get list of symbols with SSR active."""
        return [s for s, state in self._states.items() if state.ssr_active]

    def get_htb_symbols(self) -> list[str]:
        """Get list of HTB symbols."""
        return [
            s for s, state in self._states.items()
            if state.borrow_status == BorrowStatus.HTB
        ]

    def reset_daily(self) -> None:
        """Reset states for new trading day."""
        for state in self._states.values():
            # Keep borrow status (persists across days until updated)
            # Reset halt state
            state.halted = False
            state.halt_code = None
            state.halted_at = None
            state.resumed_at = None
            state.post_halt_cooldown_until = None
            # Reset SSR (will be recomputed from new prior_close)
            state.ssr_active = False
            state.ssr_triggered_at = None
            state.ssr_liquidity_risk = False
            # Reset staleness
            state.last_quote_ts = None
            state.data_stale = True

        logger.info(f"[SYMBOL_STATE] Daily reset complete for {len(self._states)} symbols")
