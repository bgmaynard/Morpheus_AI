"""
Tests for TradeLifecycleFSM.

Verifies:
- Valid state transitions work correctly
- Invalid state transitions raise errors
- Events are emitted for each transition
- Trade records are properly updated
"""

import pytest
from datetime import datetime, timezone

from morpheus.core.trade_fsm import (
    TradeLifecycleFSM,
    TradeState,
    InvalidTransitionError,
    VALID_TRANSITIONS,
)
from morpheus.core.events import EventType


class TestTradeLifecycleFSM:
    """Test suite for TradeLifecycleFSM."""

    def test_initiate_trade(self):
        """Test creating a new trade in INITIATED state."""
        fsm = TradeLifecycleFSM(emit_events=True)

        trade = fsm.initiate_trade(
            symbol="AAPL",
            direction="LONG",
            strategy_name="momentum",
            signal_id="sig-001",
            meta_score=0.75,
        )

        assert trade.symbol == "AAPL"
        assert trade.direction == "LONG"
        assert trade.state == TradeState.INITIATED
        assert trade.strategy_name == "momentum"
        assert trade.signal_id == "sig-001"
        assert trade.meta_score == 0.75
        assert len(trade.state_history) == 1
        assert trade.state_history[0][0] == TradeState.INITIATED

    def test_full_trade_lifecycle_long(self):
        """Test complete lifecycle: INITIATED -> ... -> CLOSED for a long trade."""
        fsm = TradeLifecycleFSM(emit_events=True)

        # Initiate
        trade = fsm.initiate_trade(symbol="MSFT", direction="LONG")
        assert trade.state == TradeState.INITIATED

        # Submit entry order
        trade = fsm.submit_entry_order(trade.trade_id, order_id="order-001")
        assert trade.state == TradeState.ENTRY_PENDING

        # Fill entry
        trade = fsm.fill_entry(trade.trade_id, entry_price=100.0, entry_quantity=10)
        assert trade.state == TradeState.ENTRY_FILLED
        assert trade.entry_price == 100.0
        assert trade.entry_quantity == 10

        # Activate
        trade = fsm.activate_trade(trade.trade_id)
        assert trade.state == TradeState.ACTIVE

        # Submit exit order
        trade = fsm.submit_exit_order(trade.trade_id, exit_reason="target_reached")
        assert trade.state == TradeState.EXIT_PENDING

        # Close trade
        trade = fsm.close_trade(
            trade.trade_id,
            exit_price=110.0,
            exit_quantity=10,
            exit_reason="target_reached",
        )
        assert trade.state == TradeState.CLOSED
        assert trade.exit_price == 110.0
        assert trade.realized_pnl == 100.0  # (110 - 100) * 10

    def test_full_trade_lifecycle_short(self):
        """Test complete lifecycle for a short trade with correct P&L."""
        fsm = TradeLifecycleFSM(emit_events=True)

        trade = fsm.initiate_trade(symbol="TSLA", direction="SHORT")
        trade = fsm.submit_entry_order(trade.trade_id)
        trade = fsm.fill_entry(trade.trade_id, entry_price=200.0, entry_quantity=5)
        trade = fsm.activate_trade(trade.trade_id)
        trade = fsm.close_trade(
            trade.trade_id,
            exit_price=180.0,
            exit_quantity=5,
            exit_reason="target_reached",
        )

        assert trade.state == TradeState.CLOSED
        assert trade.realized_pnl == 100.0  # (200 - 180) * 5 for short

    def test_cancel_from_initiated(self):
        """Test cancelling a trade from INITIATED state."""
        fsm = TradeLifecycleFSM(emit_events=True)

        trade = fsm.initiate_trade(symbol="GOOG", direction="LONG")
        trade = fsm.cancel_trade(trade.trade_id, reason="risk_veto")

        assert trade.state == TradeState.CANCELLED

    def test_cancel_from_entry_pending(self):
        """Test cancelling a trade from ENTRY_PENDING state."""
        fsm = TradeLifecycleFSM(emit_events=True)

        trade = fsm.initiate_trade(symbol="GOOG", direction="LONG")
        trade = fsm.submit_entry_order(trade.trade_id)
        trade = fsm.cancel_trade(trade.trade_id, reason="order_rejected")

        assert trade.state == TradeState.CANCELLED

    def test_error_from_any_non_terminal_state(self):
        """Test that ERROR transition is valid from any non-terminal state."""
        non_terminal_states = [
            TradeState.INITIATED,
            TradeState.ENTRY_PENDING,
            TradeState.ENTRY_FILLED,
            TradeState.ACTIVE,
            TradeState.EXIT_PENDING,
        ]

        for state in non_terminal_states:
            assert TradeState.ERROR in VALID_TRANSITIONS[state], (
                f"ERROR should be valid from {state}"
            )

    def test_invalid_transition_initiated_to_active(self):
        """Test that INITIATED -> ACTIVE raises InvalidTransitionError."""
        fsm = TradeLifecycleFSM(emit_events=True)

        trade = fsm.initiate_trade(symbol="NVDA", direction="LONG")

        with pytest.raises(InvalidTransitionError) as exc_info:
            fsm.activate_trade(trade.trade_id)

        assert exc_info.value.current_state == TradeState.INITIATED
        assert exc_info.value.attempted_state == TradeState.ACTIVE

    def test_invalid_transition_closed_to_anything(self):
        """Test that CLOSED is a terminal state (no transitions allowed)."""
        fsm = TradeLifecycleFSM(emit_events=True)

        # Get to CLOSED state
        trade = fsm.initiate_trade(symbol="AMD", direction="LONG")
        trade = fsm.submit_entry_order(trade.trade_id)
        trade = fsm.fill_entry(trade.trade_id, entry_price=100.0, entry_quantity=10)
        trade = fsm.activate_trade(trade.trade_id)
        trade = fsm.close_trade(trade.trade_id, exit_price=105.0, exit_quantity=10)

        # Try various transitions from CLOSED
        with pytest.raises(InvalidTransitionError):
            fsm.activate_trade(trade.trade_id)

        with pytest.raises(InvalidTransitionError):
            fsm.error_trade(trade.trade_id, "test error")

    def test_invalid_transition_cancelled_to_anything(self):
        """Test that CANCELLED is a terminal state."""
        fsm = TradeLifecycleFSM(emit_events=True)

        trade = fsm.initiate_trade(symbol="META", direction="LONG")
        trade = fsm.cancel_trade(trade.trade_id, reason="test")

        with pytest.raises(InvalidTransitionError):
            fsm.submit_entry_order(trade.trade_id)

    def test_events_emitted_for_transitions(self):
        """Test that events are emitted for each transition."""
        fsm = TradeLifecycleFSM(emit_events=True)

        trade = fsm.initiate_trade(symbol="AMZN", direction="LONG")
        events = fsm.get_pending_events()

        assert len(events) == 1
        assert events[0].event_type == EventType.TRADE_INITIATED
        assert events[0].trade_id == trade.trade_id
        assert events[0].symbol == "AMZN"

        # Continue with more transitions
        trade = fsm.submit_entry_order(trade.trade_id)
        events = fsm.get_pending_events()

        assert len(events) == 1
        assert events[0].event_type == EventType.TRADE_ENTRY_PENDING

    def test_events_not_emitted_when_disabled(self):
        """Test that events are not emitted when emit_events=False."""
        fsm = TradeLifecycleFSM(emit_events=False)

        trade = fsm.initiate_trade(symbol="NFLX", direction="LONG")
        events = fsm.get_pending_events()

        assert len(events) == 0

    def test_get_trade_not_found(self):
        """Test get_trade returns None for non-existent trade."""
        fsm = TradeLifecycleFSM(emit_events=False)

        result = fsm.get_trade("non-existent-id")
        assert result is None

    def test_get_active_trades(self):
        """Test get_active_trades returns only non-terminal trades."""
        fsm = TradeLifecycleFSM(emit_events=False)

        # Create trades in various states
        trade1 = fsm.initiate_trade(symbol="A", direction="LONG")

        trade2 = fsm.initiate_trade(symbol="B", direction="LONG")
        fsm.cancel_trade(trade2.trade_id, reason="test")

        trade3 = fsm.initiate_trade(symbol="C", direction="SHORT")
        fsm.submit_entry_order(trade3.trade_id)

        active = fsm.get_active_trades()

        assert len(active) == 2
        active_ids = {t.trade_id for t in active}
        assert trade1.trade_id in active_ids
        assert trade3.trade_id in active_ids
        assert trade2.trade_id not in active_ids

    def test_get_trades_by_symbol(self):
        """Test filtering trades by symbol."""
        fsm = TradeLifecycleFSM(emit_events=False)

        fsm.initiate_trade(symbol="AAPL", direction="LONG")
        fsm.initiate_trade(symbol="AAPL", direction="SHORT")
        fsm.initiate_trade(symbol="MSFT", direction="LONG")

        aapl_trades = fsm.get_trades_by_symbol("AAPL")
        assert len(aapl_trades) == 2

        msft_trades = fsm.get_trades_by_symbol("MSFT")
        assert len(msft_trades) == 1

    def test_state_history_tracking(self):
        """Test that state history is properly tracked."""
        fsm = TradeLifecycleFSM(emit_events=False)

        trade = fsm.initiate_trade(symbol="TEST", direction="LONG")
        trade = fsm.submit_entry_order(trade.trade_id)
        trade = fsm.fill_entry(trade.trade_id, entry_price=100.0, entry_quantity=10)

        assert len(trade.state_history) == 3
        states = [s[0] for s in trade.state_history]
        assert states == [
            TradeState.INITIATED,
            TradeState.ENTRY_PENDING,
            TradeState.ENTRY_FILLED,
        ]

    def test_trade_timestamps(self):
        """Test that timestamps are properly set."""
        fsm = TradeLifecycleFSM(emit_events=False)
        now = datetime.now(timezone.utc)

        trade = fsm.initiate_trade(symbol="TIME", direction="LONG", timestamp=now)

        assert trade.created_at == now
        assert trade.updated_at == now

    def test_transition_with_nonexistent_trade(self):
        """Test that transitioning a non-existent trade raises ValueError."""
        fsm = TradeLifecycleFSM(emit_events=False)

        with pytest.raises(ValueError, match="Trade .* not found"):
            fsm.submit_entry_order("fake-trade-id")


class TestValidTransitions:
    """Test the VALID_TRANSITIONS mapping."""

    def test_terminal_states_have_no_transitions(self):
        """Terminal states should have empty transition sets."""
        terminal_states = [TradeState.CLOSED, TradeState.CANCELLED, TradeState.ERROR]

        for state in terminal_states:
            assert VALID_TRANSITIONS[state] == set(), (
                f"{state} should have no valid transitions"
            )

    def test_all_states_have_transition_mapping(self):
        """All states should be present in VALID_TRANSITIONS."""
        for state in TradeState:
            assert state in VALID_TRANSITIONS, f"{state} missing from VALID_TRANSITIONS"

    def test_happy_path_transitions_valid(self):
        """Standard happy path should have valid transitions."""
        happy_path = [
            (TradeState.INITIATED, TradeState.ENTRY_PENDING),
            (TradeState.ENTRY_PENDING, TradeState.ENTRY_FILLED),
            (TradeState.ENTRY_FILLED, TradeState.ACTIVE),
            (TradeState.ACTIVE, TradeState.EXIT_PENDING),
            (TradeState.EXIT_PENDING, TradeState.CLOSED),
        ]

        for from_state, to_state in happy_path:
            assert to_state in VALID_TRANSITIONS[from_state], (
                f"Transition {from_state} -> {to_state} should be valid"
            )
