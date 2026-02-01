"""
Execution Base Contract - Defines execution guard, order routing, and broker adapter interfaces.

Execution is the most constrained layer:
- Cannot override risk vetoes
- Cannot override meta-gate decisions
- Cannot bypass FSM lifecycle transitions
- Must be idempotent and deterministic

All execution is EVENT-DRIVEN: every decision emits an event.

Phase 7 Scope:
- Execution guard checks only
- Order routing only
- Broker adapter integration only
- No direct FSM manipulation
- No upstream decision overrides
"""

from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any

from morpheus.core.events import Event, EventType, create_event
from morpheus.data.market_snapshot import MarketSnapshot
from morpheus.risk.base import RiskResult, PositionSize


# Schema version for execution results
EXECUTION_SCHEMA_VERSION = "1.0"


class TradingMode(Enum):
    """Trading mode for safety control."""

    PAPER = "paper"
    LIVE = "live"


class ExecutionDecision(Enum):
    """Execution guard decision outcomes."""

    EXECUTE = "execute"
    BLOCK = "block"


class BlockReason(Enum):
    """Standard execution block reason codes."""

    SPREAD_TOO_WIDE = "spread_too_wide"
    SLIPPAGE_EXCEEDED = "slippage_exceeded"
    LOW_LIQUIDITY = "low_liquidity"
    PRICE_MOVED = "price_moved"
    MARKET_CLOSED = "market_closed"
    SYMBOL_HALTED = "symbol_halted"
    UPSTREAM_REJECTED = "upstream_rejected"
    LIVE_NOT_ARMED = "live_not_armed"
    DUPLICATE_ORDER = "duplicate_order"
    INVALID_ORDER = "invalid_order"
    BROKER_ERROR = "broker_error"
    TIMEOUT = "timeout"

    # Human Confirmation specific
    NO_ACTIVE_SIGNAL = "no_active_signal"
    SIGNAL_STALE = "signal_stale"
    PRICE_DRIFT_EXCEEDED = "price_drift_exceeded"
    SYMBOL_MISMATCH = "symbol_mismatch"
    KILL_SWITCH_ACTIVE = "kill_switch_active"

    # Time authority
    TIME_DRIFT_UNSAFE = "time_drift_unsafe"


class OrderType(Enum):
    """Order types supported."""

    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"


class OrderSide(Enum):
    """Order side."""

    BUY = "buy"
    SELL = "sell"


class OrderStatus(Enum):
    """Order status from broker."""

    PENDING = "pending"
    SUBMITTED = "submitted"
    WORKING = "working"
    FILLED = "filled"
    PARTIALLY_FILLED = "partially_filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"
    UNKNOWN = "unknown"


def generate_order_intent_id() -> str:
    """Generate a unique order intent ID."""
    return f"intent_{uuid.uuid4().hex[:16]}"


def generate_client_order_id(intent_id: str) -> str:
    """Generate a client order ID from intent ID (for idempotency)."""
    # Use intent ID as base to ensure same intent always gets same client ID
    return f"morph_{intent_id[-12:]}"


@dataclass(frozen=True)
class ExecutionCheck:
    """
    Result of execution guard evaluation.

    Contains:
    - Decision (execute/block)
    - Block reasons if blocked
    - Market conditions checked

    Immutable to ensure determinism.
    """

    schema_version: str = EXECUTION_SCHEMA_VERSION

    # Decision
    decision: ExecutionDecision = ExecutionDecision.BLOCK
    block_reasons: tuple[BlockReason, ...] = field(default_factory=tuple)
    reason_details: str = ""

    # Market conditions at check time
    spread_pct: float = 0.0
    spread_threshold: float = 0.0
    slippage_estimate_pct: float = 0.0
    slippage_threshold: float = 0.0
    volume_available: int = 0
    min_volume_required: int = 0

    # Guard metadata
    guard_name: str = ""
    guard_version: str = ""

    # Timestamp
    checked_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_event_payload(self) -> dict[str, Any]:
        """Convert to event payload dict."""
        return {
            "schema_version": self.schema_version,
            "decision": self.decision.value,
            "block_reasons": [r.value for r in self.block_reasons],
            "reason_details": self.reason_details,
            "spread_pct": self.spread_pct,
            "spread_threshold": self.spread_threshold,
            "slippage_estimate_pct": self.slippage_estimate_pct,
            "slippage_threshold": self.slippage_threshold,
            "volume_available": self.volume_available,
            "min_volume_required": self.min_volume_required,
            "guard_name": self.guard_name,
            "guard_version": self.guard_version,
            "checked_at": self.checked_at.isoformat(),
        }

    def to_event(self, symbol: str) -> Event:
        """Create EXECUTION_BLOCKED event if blocked."""
        if self.decision == ExecutionDecision.BLOCK:
            return create_event(
                EventType.EXECUTION_BLOCKED,
                payload=self.to_event_payload(),
                symbol=symbol,
                timestamp=self.checked_at,
            )
        # No event for EXECUTE - order submission events follow
        return create_event(
            EventType.EXECUTION_BLOCKED,  # Placeholder, shouldn't be used
            payload=self.to_event_payload(),
            symbol=symbol,
            timestamp=self.checked_at,
        )

    @property
    def is_executable(self) -> bool:
        """Check if execution is allowed."""
        return self.decision == ExecutionDecision.EXECUTE

    @property
    def is_blocked(self) -> bool:
        """Check if execution is blocked."""
        return self.decision == ExecutionDecision.BLOCK


@dataclass(frozen=True)
class OrderRequest:
    """
    Order request to be submitted to broker.

    Contains all information needed to submit an order.
    Immutable to ensure determinism.

    CRITICAL: order_intent_id ensures idempotency.
    Same intent ID = same order (no duplicates).
    """

    schema_version: str = EXECUTION_SCHEMA_VERSION

    # Idempotency keys (CRITICAL for safe execution)
    order_intent_id: str = ""  # Morpheus internal ID
    client_order_id: str = ""  # Broker-facing ID (derived from intent)

    # Order details
    symbol: str = ""
    side: OrderSide = OrderSide.BUY
    order_type: OrderType = OrderType.LIMIT
    quantity: int = 0
    limit_price: Decimal | None = None
    stop_price: Decimal | None = None

    # Time in force
    time_in_force: str = "DAY"

    # Source references (for audit trail)
    risk_result_id: str = ""
    signal_id: str = ""

    # Timestamp
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_event_payload(self) -> dict[str, Any]:
        """Convert to event payload dict."""
        return {
            "schema_version": self.schema_version,
            "order_intent_id": self.order_intent_id,
            "client_order_id": self.client_order_id,
            "symbol": self.symbol,
            "side": self.side.value,
            "order_type": self.order_type.value,
            "quantity": self.quantity,
            "limit_price": str(self.limit_price) if self.limit_price else None,
            "stop_price": str(self.stop_price) if self.stop_price else None,
            "time_in_force": self.time_in_force,
            "risk_result_id": self.risk_result_id,
            "signal_id": self.signal_id,
            "created_at": self.created_at.isoformat(),
        }

    def to_submitted_event(self, broker_order_id: str = "") -> Event:
        """Create ORDER_SUBMITTED event."""
        payload = self.to_event_payload()
        payload["broker_order_id"] = broker_order_id
        return create_event(
            EventType.ORDER_SUBMITTED,
            payload=payload,
            symbol=self.symbol,
            timestamp=datetime.now(timezone.utc),
        )


@dataclass(frozen=True)
class OrderResult:
    """
    Result of order submission to broker.

    Contains:
    - Status from broker
    - Order IDs for tracking
    - Fill information if filled

    Immutable for audit trail.
    """

    schema_version: str = EXECUTION_SCHEMA_VERSION

    # Idempotency keys
    order_intent_id: str = ""
    client_order_id: str = ""
    broker_order_id: str = ""

    # Status
    status: OrderStatus = OrderStatus.UNKNOWN
    status_message: str = ""

    # Fill information (if filled)
    filled_quantity: int = 0
    average_fill_price: Decimal | None = None
    fill_time: datetime | None = None

    # Order details (echoed back)
    symbol: str = ""
    side: OrderSide = OrderSide.BUY
    order_type: OrderType = OrderType.LIMIT
    requested_quantity: int = 0

    # Timestamps
    submitted_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_event_payload(self) -> dict[str, Any]:
        """Convert to event payload dict."""
        return {
            "schema_version": self.schema_version,
            "order_intent_id": self.order_intent_id,
            "client_order_id": self.client_order_id,
            "broker_order_id": self.broker_order_id,
            "status": self.status.value,
            "status_message": self.status_message,
            "filled_quantity": self.filled_quantity,
            "average_fill_price": str(self.average_fill_price) if self.average_fill_price else None,
            "fill_time": self.fill_time.isoformat() if self.fill_time else None,
            "symbol": self.symbol,
            "side": self.side.value,
            "order_type": self.order_type.value,
            "requested_quantity": self.requested_quantity,
            "submitted_at": self.submitted_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    def to_event(self) -> Event:
        """Create appropriate event based on status."""
        event_type_map = {
            OrderStatus.SUBMITTED: EventType.ORDER_SUBMITTED,
            OrderStatus.WORKING: EventType.ORDER_CONFIRMED,
            OrderStatus.FILLED: EventType.ORDER_FILL_RECEIVED,
            OrderStatus.PARTIALLY_FILLED: EventType.ORDER_FILL_RECEIVED,
            OrderStatus.REJECTED: EventType.ORDER_REJECTED,
            OrderStatus.CANCELLED: EventType.ORDER_CANCELLED,
            OrderStatus.UNKNOWN: EventType.ORDER_STATUS_UNKNOWN,
        }
        event_type = event_type_map.get(self.status, EventType.ORDER_STATUS_UNKNOWN)

        return create_event(
            event_type,
            payload=self.to_event_payload(),
            symbol=self.symbol,
            timestamp=self.updated_at,
        )

    @property
    def is_terminal(self) -> bool:
        """Check if order is in a terminal state."""
        return self.status in (
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
        )

    @property
    def is_success(self) -> bool:
        """Check if order was successfully filled."""
        return self.status == OrderStatus.FILLED


@dataclass(frozen=True)
class ExecutionConfig:
    """
    Configuration for execution layer.

    Includes trading mode safety controls.
    """

    # Trading mode (CRITICAL SAFETY)
    trading_mode: TradingMode = TradingMode.PAPER
    live_trading_armed: bool = False  # Must be True + LIVE mode for real orders

    # Guard thresholds
    max_spread_pct: float = 0.5  # 0.5% max spread
    max_slippage_pct: float = 0.5  # 0.5% max slippage
    min_volume: int = 1000  # Minimum volume for liquidity

    # Order defaults
    default_order_type: OrderType = OrderType.LIMIT
    default_time_in_force: str = "DAY"

    # Retry settings (for adapter)
    max_retries: int = 3
    retry_delay_seconds: float = 1.0
    timeout_seconds: float = 30.0

    def is_live_enabled(self) -> bool:
        """Check if live trading is properly armed."""
        return self.trading_mode == TradingMode.LIVE and self.live_trading_armed


class ExecutionGuard(ABC):
    """
    Abstract base class for execution guards.

    Execution guards:
    - Check market conditions before execution
    - Are PURE and DETERMINISTIC (no external calls)
    - Cannot override upstream decisions
    - Only depend on: MarketSnapshot, RiskResult, config thresholds

    Each guard must implement:
    - name: Unique identifier
    - version: Guard version string
    - check: Evaluate market conditions
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique name identifying this guard."""
        pass

    @property
    @abstractmethod
    def version(self) -> str:
        """Version string for this guard."""
        pass

    @property
    def description(self) -> str:
        """Human-readable description of the guard."""
        return ""

    @abstractmethod
    def check(
        self,
        risk_result: RiskResult,
        snapshot: MarketSnapshot,
        config: ExecutionConfig,
    ) -> ExecutionCheck:
        """
        Check if execution should proceed.

        Args:
            risk_result: Approved RiskResult from Phase 6
            snapshot: Current MarketSnapshot
            config: ExecutionConfig with thresholds

        Returns:
            ExecutionCheck with decision

        This method MUST:
        - Be pure and deterministic
        - Not make external calls
        - Not access network/time (except for timestamp)
        - Not modify any state
        """
        pass

    def create_executable(
        self,
        spread_pct: float,
        spread_threshold: float,
        slippage_pct: float,
        slippage_threshold: float,
        volume: int,
        min_volume: int,
        details: str = "",
    ) -> ExecutionCheck:
        """Helper to create an executable ExecutionCheck."""
        return ExecutionCheck(
            schema_version=EXECUTION_SCHEMA_VERSION,
            decision=ExecutionDecision.EXECUTE,
            block_reasons=(),
            reason_details=details,
            spread_pct=spread_pct,
            spread_threshold=spread_threshold,
            slippage_estimate_pct=slippage_pct,
            slippage_threshold=slippage_threshold,
            volume_available=volume,
            min_volume_required=min_volume,
            guard_name=self.name,
            guard_version=self.version,
            checked_at=datetime.now(timezone.utc),
        )

    def create_blocked(
        self,
        reasons: tuple[BlockReason, ...],
        spread_pct: float,
        spread_threshold: float,
        slippage_pct: float,
        slippage_threshold: float,
        volume: int,
        min_volume: int,
        details: str = "",
    ) -> ExecutionCheck:
        """Helper to create a blocked ExecutionCheck."""
        return ExecutionCheck(
            schema_version=EXECUTION_SCHEMA_VERSION,
            decision=ExecutionDecision.BLOCK,
            block_reasons=reasons,
            reason_details=details,
            spread_pct=spread_pct,
            spread_threshold=spread_threshold,
            slippage_estimate_pct=slippage_pct,
            slippage_threshold=slippage_threshold,
            volume_available=volume,
            min_volume_required=min_volume,
            guard_name=self.name,
            guard_version=self.version,
            checked_at=datetime.now(timezone.utc),
        )


class OrderRouter(ABC):
    """
    Abstract base class for order routers.

    Order routers:
    - Construct OrderRequest from RiskResult
    - Generate idempotent order IDs
    - Do NOT submit orders (that's the adapter's job)

    Each router must implement:
    - name: Unique identifier
    - version: Router version string
    - route: Construct order from risk result
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique name identifying this router."""
        pass

    @property
    @abstractmethod
    def version(self) -> str:
        """Version string for this router."""
        pass

    @abstractmethod
    def route(
        self,
        risk_result: RiskResult,
        snapshot: MarketSnapshot,
        config: ExecutionConfig,
    ) -> OrderRequest:
        """
        Construct an order request from risk result.

        Args:
            risk_result: Approved RiskResult
            snapshot: Current MarketSnapshot (for price reference)
            config: ExecutionConfig

        Returns:
            OrderRequest ready for submission

        This method MUST:
        - Generate stable order_intent_id (for idempotency)
        - Derive client_order_id from intent_id
        - Be deterministic
        """
        pass


class BrokerAdapter(ABC):
    """
    Abstract base class for broker adapters.

    Broker adapters:
    - Submit orders to external broker
    - Handle retries with same client_order_id (idempotent)
    - Emit explicit events for all outcomes
    - Respect paper/live mode

    Each adapter must implement:
    - name: Broker name
    - version: Adapter version string
    - submit_order: Submit order and return result
    - cancel_order: Cancel an existing order
    - get_order_status: Check order status
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Broker name."""
        pass

    @property
    @abstractmethod
    def version(self) -> str:
        """Adapter version string."""
        pass

    @abstractmethod
    def submit_order(
        self,
        order: OrderRequest,
        config: ExecutionConfig,
    ) -> OrderResult:
        """
        Submit an order to the broker.

        Args:
            order: OrderRequest to submit
            config: ExecutionConfig (includes trading mode)

        Returns:
            OrderResult with status

        CRITICAL:
        - If config.trading_mode is PAPER, simulate submission
        - If LIVE but not armed, return REJECTED with LIVE_NOT_ARMED
        - Use order.client_order_id for idempotency
        - Retries must reuse the same client_order_id
        """
        pass

    @abstractmethod
    def cancel_order(
        self,
        client_order_id: str,
        config: ExecutionConfig,
    ) -> OrderResult:
        """
        Cancel an existing order.

        Args:
            client_order_id: Client order ID to cancel
            config: ExecutionConfig

        Returns:
            OrderResult with cancel status
        """
        pass

    @abstractmethod
    def get_order_status(
        self,
        client_order_id: str,
        config: ExecutionConfig,
    ) -> OrderResult:
        """
        Get current status of an order.

        Args:
            client_order_id: Client order ID to check
            config: ExecutionConfig

        Returns:
            OrderResult with current status
        """
        pass


# FSM Transition Mapping (for documentation/reference)
# These mappings define how execution events should drive FSM transitions
# The FSM consumes these events - execution layer does NOT call FSM directly

FSM_TRANSITION_MAP = {
    # Execution outcome -> FSM event to consume
    EventType.ORDER_SUBMITTED: "TRADE_ENTRY_PENDING",
    EventType.ORDER_CONFIRMED: "TRADE_ENTRY_PENDING",  # Still pending until filled
    EventType.ORDER_FILL_RECEIVED: "TRADE_ENTRY_FILLED",
    EventType.ORDER_REJECTED: "TRADE_CANCELLED",
    EventType.ORDER_CANCELLED: "TRADE_CANCELLED",
    EventType.EXECUTION_BLOCKED: None,  # No trade opened, stays IDLE
    EventType.ORDER_STATUS_UNKNOWN: "TRADE_ERROR",  # Ambiguous state
}
