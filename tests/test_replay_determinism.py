"""
Tests for replay determinism.

Verifies:
- Same event list => same reconstructed state
- FSM disallows invalid transitions during replay
- Meta/risk decisions are explicit and logged
- Positions, P&L, and regime state are correctly reconstructed
"""

import pytest
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path

from morpheus.core.events import Event, EventType, create_event
from morpheus.core.event_sink import EventSink
from morpheus.core.trade_fsm import TradeLifecycleFSM, TradeState
from morpheus.core.replay import (
    replay_events,
    replay_from_sink,
    ReplayState,
    EventReplayer,
)


class TestReplayDeterminism:
    """Test that replay produces deterministic results."""

    def _create_trade_events(
        self,
        trade_id: str,
        symbol: str,
        direction: str = "LONG",
        entry_price: float = 100.0,
        exit_price: float = 110.0,
        quantity: int = 10,
        base_time: datetime | None = None,
    ) -> list[Event]:
        """Create a sequence of events for a complete trade lifecycle."""
        base = base_time or datetime.now(timezone.utc)

        return [
            create_event(
                EventType.TRADE_INITIATED,
                payload={"direction": direction, "strategy_name": "test"},
                trade_id=trade_id,
                symbol=symbol,
                timestamp=base,
            ),
            create_event(
                EventType.TRADE_ENTRY_PENDING,
                payload={"order_id": f"order-{trade_id}"},
                trade_id=trade_id,
                symbol=symbol,
                timestamp=base + timedelta(seconds=1),
            ),
            create_event(
                EventType.TRADE_ENTRY_FILLED,
                payload={"entry_price": entry_price, "entry_quantity": quantity},
                trade_id=trade_id,
                symbol=symbol,
                timestamp=base + timedelta(seconds=2),
            ),
            create_event(
                EventType.TRADE_ACTIVE,
                payload={},
                trade_id=trade_id,
                symbol=symbol,
                timestamp=base + timedelta(seconds=3),
            ),
            create_event(
                EventType.TRADE_EXIT_PENDING,
                payload={"exit_reason": "target_reached"},
                trade_id=trade_id,
                symbol=symbol,
                timestamp=base + timedelta(seconds=60),
            ),
            create_event(
                EventType.TRADE_CLOSED,
                payload={
                    "exit_price": exit_price,
                    "exit_quantity": quantity,
                    "exit_reason": "target_reached",
                    "realized_pnl": (exit_price - entry_price) * quantity
                    if direction == "LONG"
                    else (entry_price - exit_price) * quantity,
                },
                trade_id=trade_id,
                symbol=symbol,
                timestamp=base + timedelta(seconds=61),
            ),
        ]

    def test_same_events_same_state(self):
        """Same event list should produce identical final state."""
        events = self._create_trade_events(
            trade_id="trade-001",
            symbol="AAPL",
            entry_price=150.0,
            exit_price=155.0,
            quantity=100,
        )

        # Replay twice
        state1 = replay_events(iter(events))
        state2 = replay_events(iter(events))

        # Verify identical results
        trades1 = state1.get_trades()
        trades2 = state2.get_trades()

        assert len(trades1) == len(trades2)
        assert "trade-001" in trades1
        assert "trade-001" in trades2

        trade1 = trades1["trade-001"]
        trade2 = trades2["trade-001"]

        assert trade1.state == trade2.state == TradeState.CLOSED
        assert trade1.entry_price == trade2.entry_price == 150.0
        assert trade1.exit_price == trade2.exit_price == 155.0
        assert trade1.realized_pnl == trade2.realized_pnl == 500.0  # (155-150)*100

    def test_multiple_trades_determinism(self):
        """Multiple trades should all be reconstructed identically."""
        base_time = datetime.now(timezone.utc)

        events = []
        # Trade 1: Long AAPL
        events.extend(
            self._create_trade_events(
                trade_id="trade-001",
                symbol="AAPL",
                direction="LONG",
                entry_price=100.0,
                exit_price=110.0,
                quantity=10,
                base_time=base_time,
            )
        )
        # Trade 2: Short MSFT
        events.extend(
            self._create_trade_events(
                trade_id="trade-002",
                symbol="MSFT",
                direction="SHORT",
                entry_price=200.0,
                exit_price=190.0,
                quantity=5,
                base_time=base_time + timedelta(minutes=5),
            )
        )

        # Sort by timestamp to ensure chronological order
        events.sort(key=lambda e: e.timestamp)

        state1 = replay_events(iter(events))
        state2 = replay_events(iter(events))

        # Verify both trades
        for trade_id in ["trade-001", "trade-002"]:
            t1 = state1.fsm.get_trade(trade_id)
            t2 = state2.fsm.get_trade(trade_id)
            assert t1.state == t2.state
            assert t1.entry_price == t2.entry_price
            assert t1.exit_price == t2.exit_price
            assert t1.realized_pnl == t2.realized_pnl

    def test_pnl_reconstruction(self):
        """Daily P&L should be correctly reconstructed from closed trades."""
        events = []

        # Winning trade
        events.extend(
            self._create_trade_events(
                trade_id="win-001",
                symbol="AAPL",
                entry_price=100.0,
                exit_price=110.0,
                quantity=10,
            )
        )
        # Losing trade
        events.extend(
            self._create_trade_events(
                trade_id="loss-001",
                symbol="MSFT",
                entry_price=200.0,
                exit_price=195.0,
                quantity=10,
            )
        )

        events.sort(key=lambda e: e.timestamp)
        state = replay_events(iter(events))

        # Win: (110-100)*10 = 100
        # Loss: (195-200)*10 = -50
        # Total: 50
        assert state.daily_realized_pnl == 50.0

    def test_position_tracking(self):
        """Positions should be correctly tracked during replay."""
        events = [
            create_event(
                EventType.TRADE_INITIATED,
                payload={"direction": "LONG"},
                trade_id="t1",
                symbol="AAPL",
            ),
            create_event(
                EventType.TRADE_ENTRY_PENDING,
                payload={},
                trade_id="t1",
                symbol="AAPL",
            ),
            create_event(
                EventType.TRADE_ENTRY_FILLED,
                payload={"entry_price": 100.0, "entry_quantity": 50},
                trade_id="t1",
                symbol="AAPL",
            ),
            create_event(
                EventType.TRADE_ACTIVE,
                payload={},
                trade_id="t1",
                symbol="AAPL",
            ),
        ]

        state = replay_events(iter(events))

        # Position should be 50 long
        assert state.get_position("AAPL") == 50

    def test_position_tracking_short(self):
        """Short positions should be tracked as negative."""
        events = [
            create_event(
                EventType.TRADE_INITIATED,
                payload={"direction": "SHORT"},
                trade_id="t1",
                symbol="TSLA",
            ),
            create_event(
                EventType.TRADE_ENTRY_PENDING,
                payload={},
                trade_id="t1",
                symbol="TSLA",
            ),
            create_event(
                EventType.TRADE_ENTRY_FILLED,
                payload={"entry_price": 200.0, "entry_quantity": 25},
                trade_id="t1",
                symbol="TSLA",
            ),
        ]

        state = replay_events(iter(events))

        # Short position should be negative
        assert state.get_position("TSLA") == -25

    def test_position_cleared_on_close(self):
        """Position should return to zero when trade closes."""
        events = self._create_trade_events(
            trade_id="t1",
            symbol="AAPL",
            entry_price=100.0,
            exit_price=110.0,
            quantity=100,
        )

        state = replay_events(iter(events))

        # Position should be 0 after close
        assert state.get_position("AAPL") == 0

    def test_regime_tracking(self):
        """Regime state should be reconstructed."""
        events = [
            create_event(
                EventType.REGIME_DETECTED,
                payload={"regime": "trending_up", "confidence": 0.8},
            ),
            create_event(
                EventType.REGIME_DETECTED,
                payload={"regime": "choppy", "confidence": 0.6},
            ),
        ]

        state = replay_events(iter(events))

        # Should have the last regime
        assert state.last_regime == "choppy"

    def test_risk_veto_tracking(self):
        """Risk locks should be tracked."""
        events = [
            create_event(
                EventType.RISK_VETO,
                payload={"reason": "daily_loss_limit_exceeded", "loss": -600.0},
            ),
        ]

        state = replay_events(iter(events))

        assert state.daily_loss_locked is True

    def test_kill_switch_tracking(self):
        """Kill switch state should be tracked."""
        events = [
            create_event(
                EventType.RISK_VETO,
                payload={"reason": "kill_switch_activated", "trigger": "manual"},
            ),
        ]

        state = replay_events(iter(events))

        assert state.kill_switch_active is True

    def test_event_count_tracking(self):
        """Events processed count should be accurate."""
        events = self._create_trade_events(
            trade_id="t1",
            symbol="TEST",
        )

        state = replay_events(iter(events))

        assert state.events_processed == len(events)

    def test_last_timestamp_tracking(self):
        """Last event timestamp should be tracked."""
        base = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        events = [
            create_event(
                EventType.TRADE_INITIATED,
                payload={"direction": "LONG"},
                trade_id="t1",
                symbol="TEST",
                timestamp=base,
            ),
            create_event(
                EventType.TRADE_ENTRY_PENDING,
                payload={},
                trade_id="t1",
                symbol="TEST",
                timestamp=base + timedelta(minutes=5),
            ),
        ]

        state = replay_events(iter(events))

        assert state.last_event_timestamp == base + timedelta(minutes=5)


class TestReplayWithEventSink:
    """Test replay integration with EventSink."""

    def test_replay_from_sink(self):
        """Events written to sink should replay identically."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_dir = Path(tmpdir)
            sink = EventSink(log_dir)

            # Create and emit events
            events = []
            base_time = datetime.now(timezone.utc)

            event1 = create_event(
                EventType.TRADE_INITIATED,
                payload={"direction": "LONG", "strategy_name": "test"},
                trade_id="replay-test-001",
                symbol="SPY",
                timestamp=base_time,
            )
            event2 = create_event(
                EventType.TRADE_ENTRY_PENDING,
                payload={"order_id": "ord-001"},
                trade_id="replay-test-001",
                symbol="SPY",
                timestamp=base_time + timedelta(seconds=1),
            )
            event3 = create_event(
                EventType.TRADE_ENTRY_FILLED,
                payload={"entry_price": 450.0, "entry_quantity": 100},
                trade_id="replay-test-001",
                symbol="SPY",
                timestamp=base_time + timedelta(seconds=2),
            )

            sink.emit(event1)
            sink.emit(event2)
            sink.emit(event3)

            # Replay from sink
            state = replay_from_sink(sink)

            # Verify state
            trade = state.fsm.get_trade("replay-test-001")
            assert trade is not None
            assert trade.state == TradeState.ENTRY_FILLED
            assert trade.entry_price == 450.0
            assert trade.entry_quantity == 100

    def test_replay_produces_same_state_as_live(self):
        """Replayed state should match live FSM state."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_dir = Path(tmpdir)
            sink = EventSink(log_dir)

            # Create live FSM and perform operations
            live_fsm = TradeLifecycleFSM(emit_events=True)

            trade = live_fsm.initiate_trade(
                symbol="QQQ",
                direction="LONG",
                strategy_name="momentum",
            )
            # Emit events to sink
            for event in live_fsm.get_pending_events():
                sink.emit(event)

            trade = live_fsm.submit_entry_order(trade.trade_id, order_id="live-ord-001")
            for event in live_fsm.get_pending_events():
                sink.emit(event)

            trade = live_fsm.fill_entry(trade.trade_id, entry_price=380.0, entry_quantity=50)
            for event in live_fsm.get_pending_events():
                sink.emit(event)

            trade = live_fsm.activate_trade(trade.trade_id)
            for event in live_fsm.get_pending_events():
                sink.emit(event)

            # Now replay
            replayed_state = replay_from_sink(sink)

            # Compare live and replayed states
            live_trade = live_fsm.get_trade(trade.trade_id)
            replayed_trade = replayed_state.fsm.get_trade(trade.trade_id)

            assert live_trade.state == replayed_trade.state
            assert live_trade.entry_price == replayed_trade.entry_price
            assert live_trade.entry_quantity == replayed_trade.entry_quantity
            assert live_trade.symbol == replayed_trade.symbol
            assert live_trade.direction == replayed_trade.direction


class TestReplayEdgeCases:
    """Test edge cases and error handling in replay."""

    def test_empty_events(self):
        """Replaying empty events should produce empty state."""
        state = replay_events(iter([]))

        assert state.events_processed == 0
        assert len(state.get_trades()) == 0
        assert state.daily_realized_pnl == 0.0

    def test_partial_trade_lifecycle(self):
        """Trades that aren't closed should still be tracked."""
        events = [
            create_event(
                EventType.TRADE_INITIATED,
                payload={"direction": "LONG"},
                trade_id="partial-001",
                symbol="AMZN",
            ),
            create_event(
                EventType.TRADE_ENTRY_PENDING,
                payload={},
                trade_id="partial-001",
                symbol="AMZN",
            ),
        ]

        state = replay_events(iter(events))

        trade = state.fsm.get_trade("partial-001")
        assert trade is not None
        assert trade.state == TradeState.ENTRY_PENDING

        active_trades = state.get_active_trades()
        assert len(active_trades) == 1

    def test_cancelled_trade_replay(self):
        """Cancelled trades should be correctly replayed."""
        events = [
            create_event(
                EventType.TRADE_INITIATED,
                payload={"direction": "LONG"},
                trade_id="cancel-001",
                symbol="NFLX",
            ),
            create_event(
                EventType.TRADE_CANCELLED,
                payload={"reason": "risk_veto"},
                trade_id="cancel-001",
                symbol="NFLX",
            ),
        ]

        state = replay_events(iter(events))

        trade = state.fsm.get_trade("cancel-001")
        assert trade.state == TradeState.CANCELLED

        # Should not be in active trades
        assert len(state.get_active_trades()) == 0

    def test_unknown_event_types_ignored(self):
        """Unknown event types should not crash replay."""
        events = [
            create_event(
                EventType.TRADE_INITIATED,
                payload={"direction": "LONG"},
                trade_id="t1",
                symbol="TEST",
            ),
            create_event(
                EventType.HEARTBEAT,  # Unknown to replayer
                payload={"status": "ok"},
            ),
            create_event(
                EventType.TRADE_ENTRY_PENDING,
                payload={},
                trade_id="t1",
                symbol="TEST",
            ),
        ]

        state = replay_events(iter(events))

        # Should still process known events
        trade = state.fsm.get_trade("t1")
        assert trade.state == TradeState.ENTRY_PENDING
        assert state.events_processed == 3  # All events counted


class TestReplayValidation:
    """
    Tests for replay validation - ensuring replay fails loudly on invalid data.

    These tests verify the safety invariant that corrupted event logs
    cannot silently produce invalid state.
    """

    def test_replay_fails_on_invalid_transition(self):
        """
        Replay must fail loudly when event stream contains invalid transition.

        This is a CORE SAFETY INVARIANT - corrupted logs must not silently
        rebuild invalid state.
        """
        from morpheus.core.replay import ReplayValidationError

        # Create events with an INVALID transition:
        # INITIATED -> ACTIVE (skipping ENTRY_PENDING and ENTRY_FILLED)
        events = [
            create_event(
                EventType.TRADE_INITIATED,
                payload={"direction": "LONG"},
                trade_id="invalid-001",
                symbol="CORRUPT",
            ),
            create_event(
                EventType.TRADE_ACTIVE,  # INVALID: Can't go directly to ACTIVE
                payload={},
                trade_id="invalid-001",
                symbol="CORRUPT",
            ),
        ]

        with pytest.raises(ReplayValidationError) as exc_info:
            replay_events(iter(events))

        # Verify error contains useful information
        error = exc_info.value
        assert error.event is not None
        assert error.event.trade_id == "invalid-001"
        assert error.cause is not None
        assert "INITIATED" in str(error)
        assert "ACTIVE" in str(error)

    def test_replay_fails_on_closed_to_active(self):
        """Replay must fail when trying to transition from terminal state."""
        from morpheus.core.replay import ReplayValidationError

        # Create a complete trade, then try to reactivate it
        events = [
            create_event(
                EventType.TRADE_INITIATED,
                payload={"direction": "LONG"},
                trade_id="terminal-001",
                symbol="DONE",
            ),
            create_event(
                EventType.TRADE_CANCELLED,
                payload={"reason": "test"},
                trade_id="terminal-001",
                symbol="DONE",
            ),
            create_event(
                EventType.TRADE_ENTRY_PENDING,  # INVALID: CANCELLED is terminal
                payload={},
                trade_id="terminal-001",
                symbol="DONE",
            ),
        ]

        with pytest.raises(ReplayValidationError):
            replay_events(iter(events))

    def test_replay_validation_error_contains_event(self):
        """ReplayValidationError must include the offending event for debugging."""
        from morpheus.core.replay import ReplayValidationError

        bad_event = create_event(
            EventType.TRADE_CLOSED,  # Can't close a trade that was never opened
            payload={"exit_price": 100.0, "exit_quantity": 10},
            trade_id="ghost-trade",
            symbol="PHANTOM",
        )

        events = [bad_event]

        with pytest.raises((ReplayValidationError, ValueError)) as exc_info:
            replay_events(iter(events))

        # For this case, it's a ValueError because trade doesn't exist
        # But we should still get useful error info
        assert "ghost-trade" in str(exc_info.value)


class TestFromStateToStateInEvents:
    """Tests verifying from_state and to_state are correctly recorded in events."""

    def test_transition_events_contain_from_and_to_state(self):
        """All transition events must include from_state and to_state."""
        fsm = TradeLifecycleFSM(emit_events=True)

        # Initiate trade
        trade = fsm.initiate_trade(symbol="STATE", direction="LONG")
        events = fsm.get_pending_events()

        assert len(events) == 1
        init_event = events[0]
        assert init_event.payload.get("from_state") is None  # No prior state
        assert init_event.payload.get("to_state") == "INITIATED"

        # Transition to ENTRY_PENDING
        trade = fsm.submit_entry_order(trade.trade_id)
        events = fsm.get_pending_events()

        assert len(events) == 1
        pending_event = events[0]
        assert pending_event.payload.get("from_state") == "INITIATED"
        assert pending_event.payload.get("to_state") == "ENTRY_PENDING"

        # Fill entry
        trade = fsm.fill_entry(trade.trade_id, entry_price=100.0, entry_quantity=10)
        events = fsm.get_pending_events()

        assert len(events) == 1
        filled_event = events[0]
        assert filled_event.payload.get("from_state") == "ENTRY_PENDING"
        assert filled_event.payload.get("to_state") == "ENTRY_FILLED"

    def test_full_lifecycle_state_chain(self):
        """Verify complete state chain is recorded correctly."""
        fsm = TradeLifecycleFSM(emit_events=True)

        trade = fsm.initiate_trade(symbol="CHAIN", direction="LONG")
        trade = fsm.submit_entry_order(trade.trade_id)
        trade = fsm.fill_entry(trade.trade_id, entry_price=100.0, entry_quantity=10)
        trade = fsm.activate_trade(trade.trade_id)
        trade = fsm.submit_exit_order(trade.trade_id, exit_reason="target")
        trade = fsm.close_trade(trade.trade_id, exit_price=110.0, exit_quantity=10)

        events = fsm.get_pending_events()

        # Build the state chain from events
        state_chain = []
        for event in events:
            from_state = event.payload.get("from_state")
            to_state = event.payload.get("to_state")
            state_chain.append((from_state, to_state))

        expected_chain = [
            (None, "INITIATED"),
            ("INITIATED", "ENTRY_PENDING"),
            ("ENTRY_PENDING", "ENTRY_FILLED"),
            ("ENTRY_FILLED", "ACTIVE"),
            ("ACTIVE", "EXIT_PENDING"),
            ("EXIT_PENDING", "CLOSED"),
        ]

        assert state_chain == expected_chain
