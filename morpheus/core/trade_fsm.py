"""
Trade Lifecycle Finite State Machine (FSM).

The TradeLifecycleFSM is the ONLY owner of trade state transitions.
Invalid transitions raise errors—this ensures deterministic behavior.

Trade States:
    INITIATED -> ENTRY_PENDING -> ENTRY_FILLED -> ACTIVE -> EXIT_PENDING -> CLOSED
                             |-> CANCELLED       |-> ERROR
                             |-> ERROR
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from morpheus.core.events import Event, EventType, create_event


class TradeState(str, Enum):
    """Valid states in the trade lifecycle."""

    INITIATED = "INITIATED"
    ENTRY_PENDING = "ENTRY_PENDING"
    ENTRY_FILLED = "ENTRY_FILLED"
    ACTIVE = "ACTIVE"
    EXIT_PENDING = "EXIT_PENDING"
    CLOSED = "CLOSED"
    CANCELLED = "CANCELLED"
    ERROR = "ERROR"


class InvalidTransitionError(Exception):
    """Raised when an invalid state transition is attempted."""

    def __init__(self, current_state: TradeState, attempted_state: TradeState, trade_id: str):
        self.current_state = current_state
        self.attempted_state = attempted_state
        self.trade_id = trade_id
        super().__init__(
            f"Invalid transition from {current_state.value} to {attempted_state.value} "
            f"for trade {trade_id}"
        )


# Valid state transitions (from_state -> set of valid to_states)
VALID_TRANSITIONS: dict[TradeState, set[TradeState]] = {
    TradeState.INITIATED: {TradeState.ENTRY_PENDING, TradeState.CANCELLED, TradeState.ERROR},
    TradeState.ENTRY_PENDING: {TradeState.ENTRY_FILLED, TradeState.CANCELLED, TradeState.ERROR},
    TradeState.ENTRY_FILLED: {TradeState.ACTIVE, TradeState.ERROR},
    TradeState.ACTIVE: {TradeState.EXIT_PENDING, TradeState.CLOSED, TradeState.ERROR},
    TradeState.EXIT_PENDING: {TradeState.CLOSED, TradeState.ERROR},
    TradeState.CLOSED: set(),  # Terminal state
    TradeState.CANCELLED: set(),  # Terminal state
    TradeState.ERROR: set(),  # Terminal state
}


@dataclass
class TradeRecord:
    """Immutable record of a trade's state and history."""

    trade_id: str
    symbol: str
    direction: str  # "LONG" or "SHORT"
    state: TradeState
    created_at: datetime
    updated_at: datetime

    # Entry details
    entry_price: float | None = None
    entry_quantity: int | None = None
    entry_time: datetime | None = None

    # Exit details
    exit_price: float | None = None
    exit_quantity: int | None = None
    exit_time: datetime | None = None
    exit_reason: str | None = None

    # Computed
    realized_pnl: float | None = None

    # Metadata
    strategy_name: str | None = None
    signal_id: str | None = None
    meta_score: float | None = None

    # State history for audit
    state_history: list[tuple[TradeState, datetime]] = field(default_factory=list)


class TradeLifecycleFSM:
    """
    Finite State Machine for managing trade lifecycle.

    This is the ONLY component that should manage trade state transitions.
    All transitions are validated and emit events.
    """

    def __init__(self, emit_events: bool = True):
        """
        Initialize the FSM.

        Args:
            emit_events: If True, emit events for each transition (set False for replay).
        """
        self._trades: dict[str, TradeRecord] = {}
        self._emit_events = emit_events
        self._pending_events: list[Event] = []

    def _validate_transition(
        self, trade_id: str, current_state: TradeState, new_state: TradeState
    ) -> None:
        """Validate that a transition is allowed. Raises InvalidTransitionError if not."""
        valid_next_states = VALID_TRANSITIONS.get(current_state, set())
        if new_state not in valid_next_states:
            raise InvalidTransitionError(current_state, new_state, trade_id)

    def _emit(self, event: Event) -> None:
        """Emit an event if event emission is enabled."""
        if self._emit_events:
            self._pending_events.append(event)

    def get_pending_events(self) -> list[Event]:
        """Get and clear pending events."""
        events = self._pending_events.copy()
        self._pending_events.clear()
        return events

    def initiate_trade(
        self,
        symbol: str,
        direction: str,
        strategy_name: str | None = None,
        signal_id: str | None = None,
        meta_score: float | None = None,
        trade_id: str | None = None,
        timestamp: datetime | None = None,
    ) -> TradeRecord:
        """
        Create a new trade in INITIATED state.

        Returns the created TradeRecord.
        """
        trade_id = trade_id or str(uuid.uuid4())
        now = timestamp or datetime.now(timezone.utc)

        trade = TradeRecord(
            trade_id=trade_id,
            symbol=symbol,
            direction=direction,
            state=TradeState.INITIATED,
            created_at=now,
            updated_at=now,
            strategy_name=strategy_name,
            signal_id=signal_id,
            meta_score=meta_score,
            state_history=[(TradeState.INITIATED, now)],
        )
        self._trades[trade_id] = trade

        self._emit(
            create_event(
                EventType.TRADE_INITIATED,
                payload={
                    "direction": direction,
                    "strategy_name": strategy_name,
                    "signal_id": signal_id,
                    "meta_score": meta_score,
                },
                trade_id=trade_id,
                symbol=symbol,
                timestamp=now,
            )
        )

        return trade

    def _transition(
        self,
        trade_id: str,
        new_state: TradeState,
        event_type: EventType,
        payload: dict[str, Any] | None = None,
        timestamp: datetime | None = None,
        **updates: Any,
    ) -> TradeRecord:
        """
        Internal method to perform a validated state transition.
        """
        trade = self._trades.get(trade_id)
        if trade is None:
            raise ValueError(f"Trade {trade_id} not found")

        self._validate_transition(trade_id, trade.state, new_state)

        now = timestamp or datetime.now(timezone.utc)

        # Create updated trade record
        new_trade = TradeRecord(
            trade_id=trade.trade_id,
            symbol=trade.symbol,
            direction=trade.direction,
            state=new_state,
            created_at=trade.created_at,
            updated_at=now,
            entry_price=updates.get("entry_price", trade.entry_price),
            entry_quantity=updates.get("entry_quantity", trade.entry_quantity),
            entry_time=updates.get("entry_time", trade.entry_time),
            exit_price=updates.get("exit_price", trade.exit_price),
            exit_quantity=updates.get("exit_quantity", trade.exit_quantity),
            exit_time=updates.get("exit_time", trade.exit_time),
            exit_reason=updates.get("exit_reason", trade.exit_reason),
            realized_pnl=updates.get("realized_pnl", trade.realized_pnl),
            strategy_name=trade.strategy_name,
            signal_id=trade.signal_id,
            meta_score=trade.meta_score,
            state_history=trade.state_history + [(new_state, now)],
        )
        self._trades[trade_id] = new_trade

        self._emit(
            create_event(
                event_type,
                payload=payload or {},
                trade_id=trade_id,
                symbol=trade.symbol,
                timestamp=now,
            )
        )

        return new_trade

    def submit_entry_order(
        self,
        trade_id: str,
        order_id: str | None = None,
        timestamp: datetime | None = None,
    ) -> TradeRecord:
        """Transition to ENTRY_PENDING when entry order is submitted."""
        return self._transition(
            trade_id,
            TradeState.ENTRY_PENDING,
            EventType.TRADE_ENTRY_PENDING,
            payload={"order_id": order_id},
            timestamp=timestamp,
        )

    def fill_entry(
        self,
        trade_id: str,
        entry_price: float,
        entry_quantity: int,
        timestamp: datetime | None = None,
    ) -> TradeRecord:
        """Transition to ENTRY_FILLED when entry order is filled."""
        now = timestamp or datetime.now(timezone.utc)
        return self._transition(
            trade_id,
            TradeState.ENTRY_FILLED,
            EventType.TRADE_ENTRY_FILLED,
            payload={"entry_price": entry_price, "entry_quantity": entry_quantity},
            timestamp=now,
            entry_price=entry_price,
            entry_quantity=entry_quantity,
            entry_time=now,
        )

    def activate_trade(
        self, trade_id: str, timestamp: datetime | None = None
    ) -> TradeRecord:
        """Transition to ACTIVE state."""
        return self._transition(
            trade_id,
            TradeState.ACTIVE,
            EventType.TRADE_ACTIVE,
            timestamp=timestamp,
        )

    def submit_exit_order(
        self,
        trade_id: str,
        exit_reason: str,
        order_id: str | None = None,
        timestamp: datetime | None = None,
    ) -> TradeRecord:
        """Transition to EXIT_PENDING when exit order is submitted."""
        return self._transition(
            trade_id,
            TradeState.EXIT_PENDING,
            EventType.TRADE_EXIT_PENDING,
            payload={"exit_reason": exit_reason, "order_id": order_id},
            timestamp=timestamp,
            exit_reason=exit_reason,
        )

    def close_trade(
        self,
        trade_id: str,
        exit_price: float,
        exit_quantity: int,
        exit_reason: str | None = None,
        timestamp: datetime | None = None,
    ) -> TradeRecord:
        """Transition to CLOSED state."""
        trade = self._trades.get(trade_id)
        if trade is None:
            raise ValueError(f"Trade {trade_id} not found")

        now = timestamp or datetime.now(timezone.utc)

        # Calculate P&L if we have entry data
        realized_pnl = None
        if trade.entry_price is not None and trade.entry_quantity is not None:
            if trade.direction == "LONG":
                realized_pnl = (exit_price - trade.entry_price) * exit_quantity
            else:  # SHORT
                realized_pnl = (trade.entry_price - exit_price) * exit_quantity

        return self._transition(
            trade_id,
            TradeState.CLOSED,
            EventType.TRADE_CLOSED,
            payload={
                "exit_price": exit_price,
                "exit_quantity": exit_quantity,
                "exit_reason": exit_reason or trade.exit_reason,
                "realized_pnl": realized_pnl,
                "entry_time": trade.entry_time.isoformat() if trade.entry_time else None,
                "exit_time": now.isoformat(),
            },
            timestamp=now,
            exit_price=exit_price,
            exit_quantity=exit_quantity,
            exit_time=now,
            exit_reason=exit_reason or trade.exit_reason,
            realized_pnl=realized_pnl,
        )

    def cancel_trade(
        self,
        trade_id: str,
        reason: str,
        timestamp: datetime | None = None,
    ) -> TradeRecord:
        """Transition to CANCELLED state."""
        return self._transition(
            trade_id,
            TradeState.CANCELLED,
            EventType.TRADE_CANCELLED,
            payload={"reason": reason},
            timestamp=timestamp,
        )

    def error_trade(
        self,
        trade_id: str,
        error_message: str,
        timestamp: datetime | None = None,
    ) -> TradeRecord:
        """Transition to ERROR state."""
        return self._transition(
            trade_id,
            TradeState.ERROR,
            EventType.TRADE_ERROR,
            payload={"error_message": error_message},
            timestamp=timestamp,
        )

    def get_trade(self, trade_id: str) -> TradeRecord | None:
        """Get a trade by ID."""
        return self._trades.get(trade_id)

    def get_all_trades(self) -> dict[str, TradeRecord]:
        """Get all trades."""
        return self._trades.copy()

    def get_active_trades(self) -> list[TradeRecord]:
        """Get all trades in non-terminal states."""
        terminal_states = {TradeState.CLOSED, TradeState.CANCELLED, TradeState.ERROR}
        return [t for t in self._trades.values() if t.state not in terminal_states]

    def get_trades_by_symbol(self, symbol: str) -> list[TradeRecord]:
        """Get all trades for a symbol."""
        return [t for t in self._trades.values() if t.symbol == symbol]
