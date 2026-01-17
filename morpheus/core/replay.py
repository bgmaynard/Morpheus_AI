"""
Event Replay - Rebuild system state from event logs.

Given a sequence of events, replay must rebuild:
- Last known positions
- Last FSM state for all trades
- Last known regime (if logged)
- Last risk locks / drawdown status

Determinism: Same event list => same reconstructed state.

IMPORTANT: Replay validates all transitions via FSM logic.
---------------------------------------------------------
Replay does NOT blindly set state from event payloads.
Instead, it re-applies transitions through the FSM, which validates
that each transition is legal. If the event stream contains an
impossible transition, replay will FAIL LOUDLY with a ReplayValidationError.

This prevents corrupted logs from silently rebuilding invalid state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Iterator

from morpheus.core.events import Event, EventType
from morpheus.core.event_sink import EventSink
from morpheus.core.trade_fsm import (
    TradeLifecycleFSM,
    TradeRecord,
    TradeState,
    InvalidTransitionError,
)


class ReplayValidationError(Exception):
    """
    Raised when replay encounters an invalid state transition in the event stream.

    This indicates either:
    - Corrupted event logs
    - A bug in the system that produced the events
    - Events from an incompatible version

    The error includes the event that caused the failure and the underlying
    InvalidTransitionError from the FSM.
    """

    def __init__(
        self,
        message: str,
        event: Event,
        cause: InvalidTransitionError | None = None,
    ):
        self.event = event
        self.cause = cause
        super().__init__(message)


@dataclass
class ReplayState:
    """
    Complete system state reconstructed from events.

    This represents a snapshot of system state at a point in time.
    """

    # Trade FSM state
    fsm: TradeLifecycleFSM = field(default_factory=lambda: TradeLifecycleFSM(emit_events=False))

    # Last known values
    last_regime: str | None = None
    last_regime_timestamp: datetime | None = None

    # Risk state
    daily_realized_pnl: float = 0.0
    daily_loss_locked: bool = False
    kill_switch_active: bool = False

    # Positions tracking (symbol -> quantity, positive=long, negative=short)
    positions: dict[str, int] = field(default_factory=dict)

    # Event counts for debugging
    events_processed: int = 0
    last_event_timestamp: datetime | None = None

    def get_trades(self) -> dict[str, TradeRecord]:
        """Get all trades from the FSM."""
        return self.fsm.get_all_trades()

    def get_active_trades(self) -> list[TradeRecord]:
        """Get trades in non-terminal states."""
        return self.fsm.get_active_trades()

    def get_position(self, symbol: str) -> int:
        """Get position for a symbol (0 if none)."""
        return self.positions.get(symbol, 0)


class EventReplayer:
    """
    Replays events to reconstruct system state.

    The replayer processes events in order and applies them to build
    a consistent state snapshot.
    """

    def __init__(self):
        self._handlers: dict[EventType, callable] = {
            # Trade lifecycle events
            EventType.TRADE_INITIATED: self._handle_trade_initiated,
            EventType.TRADE_ENTRY_PENDING: self._handle_trade_entry_pending,
            EventType.TRADE_ENTRY_FILLED: self._handle_trade_entry_filled,
            EventType.TRADE_ACTIVE: self._handle_trade_active,
            EventType.TRADE_EXIT_PENDING: self._handle_trade_exit_pending,
            EventType.TRADE_CLOSED: self._handle_trade_closed,
            EventType.TRADE_CANCELLED: self._handle_trade_cancelled,
            EventType.TRADE_ERROR: self._handle_trade_error,
            # Regime events
            EventType.REGIME_DETECTED: self._handle_regime_detected,
            # Risk events
            EventType.RISK_VETO: self._handle_risk_veto,
        }

    def replay(self, events: Iterator[Event]) -> ReplayState:
        """
        Replay a sequence of events and return the final state.

        Events must be in chronological order.
        """
        state = ReplayState()

        for event in events:
            self._process_event(state, event)

        return state

    def _process_event(self, state: ReplayState, event: Event) -> None:
        """
        Process a single event and update state.

        Raises ReplayValidationError if the event contains an invalid transition.
        This ensures replay fails loudly on corrupted data rather than
        silently building invalid state.
        """
        handler = self._handlers.get(event.event_type)
        if handler:
            try:
                handler(state, event)
            except InvalidTransitionError as e:
                raise ReplayValidationError(
                    f"Invalid state transition in event stream: {e}. "
                    f"Event {event.event_id} attempted transition "
                    f"{e.current_state.value} -> {e.attempted_state.value} "
                    f"for trade {e.trade_id}",
                    event=event,
                    cause=e,
                ) from e

        state.events_processed += 1
        state.last_event_timestamp = event.timestamp

    # Trade lifecycle handlers

    def _handle_trade_initiated(self, state: ReplayState, event: Event) -> None:
        """Handle TRADE_INITIATED event."""
        payload = event.payload
        state.fsm.initiate_trade(
            symbol=event.symbol or payload.get("symbol", "UNKNOWN"),
            direction=payload.get("direction", "LONG"),
            strategy_name=payload.get("strategy_name"),
            signal_id=payload.get("signal_id"),
            meta_score=payload.get("meta_score"),
            trade_id=event.trade_id,
            timestamp=event.timestamp,
        )

    def _handle_trade_entry_pending(self, state: ReplayState, event: Event) -> None:
        """Handle TRADE_ENTRY_PENDING event."""
        if event.trade_id:
            state.fsm.submit_entry_order(
                trade_id=event.trade_id,
                order_id=event.payload.get("order_id"),
                timestamp=event.timestamp,
            )

    def _handle_trade_entry_filled(self, state: ReplayState, event: Event) -> None:
        """Handle TRADE_ENTRY_FILLED event."""
        if event.trade_id:
            payload = event.payload
            state.fsm.fill_entry(
                trade_id=event.trade_id,
                entry_price=payload.get("entry_price", 0.0),
                entry_quantity=payload.get("entry_quantity", 0),
                timestamp=event.timestamp,
            )
            # Update positions
            trade = state.fsm.get_trade(event.trade_id)
            if trade:
                qty = payload.get("entry_quantity", 0)
                if trade.direction == "SHORT":
                    qty = -qty
                state.positions[trade.symbol] = state.positions.get(trade.symbol, 0) + qty

    def _handle_trade_active(self, state: ReplayState, event: Event) -> None:
        """Handle TRADE_ACTIVE event."""
        if event.trade_id:
            state.fsm.activate_trade(
                trade_id=event.trade_id,
                timestamp=event.timestamp,
            )

    def _handle_trade_exit_pending(self, state: ReplayState, event: Event) -> None:
        """Handle TRADE_EXIT_PENDING event."""
        if event.trade_id:
            payload = event.payload
            state.fsm.submit_exit_order(
                trade_id=event.trade_id,
                exit_reason=payload.get("exit_reason", "unknown"),
                order_id=payload.get("order_id"),
                timestamp=event.timestamp,
            )

    def _handle_trade_closed(self, state: ReplayState, event: Event) -> None:
        """Handle TRADE_CLOSED event."""
        if event.trade_id:
            payload = event.payload
            trade = state.fsm.get_trade(event.trade_id)

            state.fsm.close_trade(
                trade_id=event.trade_id,
                exit_price=payload.get("exit_price", 0.0),
                exit_quantity=payload.get("exit_quantity", 0),
                exit_reason=payload.get("exit_reason"),
                timestamp=event.timestamp,
            )

            # Update positions
            if trade:
                qty = payload.get("exit_quantity", 0)
                if trade.direction == "SHORT":
                    qty = -qty
                state.positions[trade.symbol] = state.positions.get(trade.symbol, 0) - qty

            # Update daily P&L
            pnl = payload.get("realized_pnl")
            if pnl is not None:
                state.daily_realized_pnl += pnl

    def _handle_trade_cancelled(self, state: ReplayState, event: Event) -> None:
        """Handle TRADE_CANCELLED event."""
        if event.trade_id:
            state.fsm.cancel_trade(
                trade_id=event.trade_id,
                reason=event.payload.get("reason", "unknown"),
                timestamp=event.timestamp,
            )

    def _handle_trade_error(self, state: ReplayState, event: Event) -> None:
        """Handle TRADE_ERROR event."""
        if event.trade_id:
            state.fsm.error_trade(
                trade_id=event.trade_id,
                error_message=event.payload.get("error_message", "unknown"),
                timestamp=event.timestamp,
            )

    # Regime handlers

    def _handle_regime_detected(self, state: ReplayState, event: Event) -> None:
        """Handle REGIME_DETECTED event."""
        state.last_regime = event.payload.get("regime")
        state.last_regime_timestamp = event.timestamp

    # Risk handlers

    def _handle_risk_veto(self, state: ReplayState, event: Event) -> None:
        """Handle RISK_VETO event."""
        reason = event.payload.get("reason", "")
        if "daily_loss" in reason.lower():
            state.daily_loss_locked = True
        if "kill_switch" in reason.lower():
            state.kill_switch_active = True


def replay_events(events: Iterator[Event]) -> ReplayState:
    """
    Convenience function to replay events.

    Args:
        events: Iterator of events in chronological order.

    Returns:
        ReplayState with the reconstructed system state.
    """
    replayer = EventReplayer()
    return replayer.replay(events)


def replay_from_sink(
    sink: EventSink,
    start_date: date | None = None,
    end_date: date | None = None,
) -> ReplayState:
    """
    Replay events from an EventSink.

    Args:
        sink: The EventSink to read from.
        start_date: Start date (inclusive). If None, reads all.
        end_date: End date (inclusive). If None, reads to most recent.

    Returns:
        ReplayState with the reconstructed system state.
    """
    if start_date and end_date:
        events = sink.read_events_range(start_date, end_date)
    elif start_date:
        events = sink.read_events_range(start_date, date.today())
    else:
        events = sink.read_all_events()

    return replay_events(events)


def replay_from_directory(
    log_dir: Path | str,
    start_date: date | None = None,
    end_date: date | None = None,
) -> ReplayState:
    """
    Replay events from a log directory.

    Args:
        log_dir: Path to the event log directory.
        start_date: Start date (inclusive). If None, reads all.
        end_date: End date (inclusive). If None, reads to most recent.

    Returns:
        ReplayState with the reconstructed system state.
    """
    sink = EventSink(log_dir)
    return replay_from_sink(sink, start_date, end_date)
