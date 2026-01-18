"""
Tests for ConfirmationGuard - Human Confirmation Entry Validation.

Tests:
1. valid_confirm_executes - Fresh signal, no drift, armed → accepted
2. stale_signal_rejected - Signal age > TTL → rejected
3. drift_rejected - Price drift > max → rejected
4. not_armed_rejected - Live mode but not armed → rejected
5. context_mismatch_rejected - Symbol mismatch → rejected
6. kill_switch_blocks - Kill switch active → rejected
7. no_signal_rejected - No active signal → rejected
"""

import pytest
from datetime import datetime, timezone, timedelta

from morpheus.execution.guards import (
    ConfirmationGuard,
    ConfirmationGuardConfig,
    ConfirmationRequest,
    create_confirmation_guard,
)
from morpheus.execution.base import BlockReason


class TestConfirmationGuard:
    """Tests for ConfirmationGuard validation logic."""

    @pytest.fixture
    def guard(self) -> ConfirmationGuard:
        """Create a confirmation guard with default config."""
        return create_confirmation_guard()

    @pytest.fixture
    def strict_guard(self) -> ConfirmationGuard:
        """Create a confirmation guard with strict config."""
        config = ConfirmationGuardConfig(
            confirm_ttl_ms=2000,  # 2 seconds
            max_drift_pct=0.1,   # 0.1%
        )
        return create_confirmation_guard(config)

    def _create_request(
        self,
        symbol: str = "AAPL",
        chain_id: int = 1,
        entry_price: float = 100.0,
    ) -> ConfirmationRequest:
        """Helper to create a confirmation request."""
        return ConfirmationRequest(
            symbol=symbol,
            chain_id=chain_id,
            signal_timestamp=datetime.now(timezone.utc).isoformat(),
            entry_price=entry_price,
            command_id="test-cmd-123",
        )

    def _fresh_signal_timestamp(self) -> str:
        """Create a fresh signal timestamp (now)."""
        return datetime.now(timezone.utc).isoformat()

    def _stale_signal_timestamp(self, age_ms: int = 5000) -> str:
        """Create a stale signal timestamp."""
        stale_time = datetime.now(timezone.utc) - timedelta(milliseconds=age_ms)
        return stale_time.isoformat()

    def test_valid_confirm_executes(self, guard: ConfirmationGuard):
        """
        Test: Fresh signal, no drift, armed → accepted.

        Given: Active signal < TTL, price within drift, system ready
        When: Human confirms entry
        Then: Confirmation accepted
        """
        request = self._create_request(entry_price=100.0)
        signal_timestamp = self._fresh_signal_timestamp()

        result = guard.validate(
            request=request,
            signal_timestamp=signal_timestamp,
            signal_entry_price=100.0,
            current_price=100.0,  # No drift
            is_live_mode=False,   # Paper mode, no arm check
            is_live_armed=False,
            is_kill_switch_active=False,
            active_symbol="AAPL",
        )

        assert result.accepted is True
        assert result.reason is None
        assert "accepted" in result.details.lower()

    def test_valid_confirm_live_armed(self, guard: ConfirmationGuard):
        """
        Test: Live mode, armed, fresh signal → accepted.
        """
        request = self._create_request()
        signal_timestamp = self._fresh_signal_timestamp()

        result = guard.validate(
            request=request,
            signal_timestamp=signal_timestamp,
            signal_entry_price=100.0,
            current_price=100.0,
            is_live_mode=True,
            is_live_armed=True,  # Armed!
            is_kill_switch_active=False,
            active_symbol="AAPL",
        )

        assert result.accepted is True

    def test_stale_signal_rejected(self, guard: ConfirmationGuard):
        """
        Test: Signal age > TTL → rejected.

        Given: Signal is 1.5 seconds old (> 1.2s TTL for small-cap)
        When: Human confirms entry
        Then: Confirmation rejected with SIGNAL_STALE
        """
        request = self._create_request()
        signal_timestamp = self._stale_signal_timestamp(age_ms=1500)

        result = guard.validate(
            request=request,
            signal_timestamp=signal_timestamp,
            signal_entry_price=100.0,
            current_price=100.0,
            is_live_mode=False,
            is_live_armed=False,
            is_kill_switch_active=False,
            active_symbol="AAPL",
        )

        assert result.accepted is False
        assert result.reason == BlockReason.SIGNAL_STALE
        assert "stale" in result.details.lower()
        assert result.signal_age_ms > 1200

    def test_drift_rejected(self, guard: ConfirmationGuard):
        """
        Test: Price drift > max → rejected.

        Given: Signal entry price = 100, current price = 100.30 (0.30% drift)
        When: Human confirms entry
        Then: Confirmation rejected with PRICE_DRIFT_EXCEEDED (> 0.20% small-cap max)
        """
        request = self._create_request(entry_price=100.0)
        signal_timestamp = self._fresh_signal_timestamp()

        result = guard.validate(
            request=request,
            signal_timestamp=signal_timestamp,
            signal_entry_price=100.0,
            current_price=100.30,  # 0.30% drift > 0.20% max (small-cap)
            is_live_mode=False,
            is_live_armed=False,
            is_kill_switch_active=False,
            active_symbol="AAPL",
        )

        assert result.accepted is False
        assert result.reason == BlockReason.PRICE_DRIFT_EXCEEDED
        assert "drift" in result.details.lower()
        assert result.price_drift_pct > 0.20

    def test_small_drift_accepted(self, guard: ConfirmationGuard):
        """
        Test: Small price drift within threshold → accepted.

        Given: Signal entry price = 100, current price = 100.15 (0.15% drift)
        When: Human confirms entry
        Then: Confirmation accepted (drift < 0.20% small-cap max)
        """
        request = self._create_request(entry_price=100.0)
        signal_timestamp = self._fresh_signal_timestamp()

        result = guard.validate(
            request=request,
            signal_timestamp=signal_timestamp,
            signal_entry_price=100.0,
            current_price=100.15,  # 0.15% drift < 0.20% max (small-cap)
            is_live_mode=False,
            is_live_armed=False,
            is_kill_switch_active=False,
            active_symbol="AAPL",
        )

        assert result.accepted is True
        assert result.price_drift_pct < 0.20

    def test_not_armed_rejected(self, guard: ConfirmationGuard):
        """
        Test: Live mode but not armed → rejected.

        Given: System in LIVE mode but not armed
        When: Human confirms entry
        Then: Confirmation rejected with LIVE_NOT_ARMED
        """
        request = self._create_request()
        signal_timestamp = self._fresh_signal_timestamp()

        result = guard.validate(
            request=request,
            signal_timestamp=signal_timestamp,
            signal_entry_price=100.0,
            current_price=100.0,
            is_live_mode=True,    # Live mode
            is_live_armed=False,  # NOT armed!
            is_kill_switch_active=False,
            active_symbol="AAPL",
        )

        assert result.accepted is False
        assert result.reason == BlockReason.LIVE_NOT_ARMED
        assert "armed" in result.details.lower()

    def test_context_mismatch_rejected(self, guard: ConfirmationGuard):
        """
        Test: Symbol mismatch → rejected (symbol lock).

        Given: Request for AAPL but active symbol is MSFT
        When: Human confirms entry
        Then: Confirmation rejected with SYMBOL_MISMATCH
        """
        request = self._create_request(symbol="AAPL")
        signal_timestamp = self._fresh_signal_timestamp()

        result = guard.validate(
            request=request,
            signal_timestamp=signal_timestamp,
            signal_entry_price=100.0,
            current_price=100.0,
            is_live_mode=False,
            is_live_armed=False,
            is_kill_switch_active=False,
            active_symbol="MSFT",  # Mismatch!
        )

        assert result.accepted is False
        assert result.reason == BlockReason.SYMBOL_MISMATCH
        assert "mismatch" in result.details.lower()

    def test_symbol_lock_case_insensitive(self, guard: ConfirmationGuard):
        """
        Test: Symbol matching is case insensitive.

        Given: Request for "aapl" but active symbol is "AAPL"
        When: Human confirms entry
        Then: Confirmation accepted (case insensitive match)
        """
        request = self._create_request(symbol="aapl")  # lowercase
        signal_timestamp = self._fresh_signal_timestamp()

        result = guard.validate(
            request=request,
            signal_timestamp=signal_timestamp,
            signal_entry_price=100.0,
            current_price=100.0,
            is_live_mode=False,
            is_live_armed=False,
            is_kill_switch_active=False,
            active_symbol="AAPL",  # uppercase
        )

        assert result.accepted is True

    def test_kill_switch_blocks(self, guard: ConfirmationGuard):
        """
        Test: Kill switch active → rejected.

        Given: Kill switch is active
        When: Human confirms entry
        Then: Confirmation rejected with KILL_SWITCH_ACTIVE
        """
        request = self._create_request()
        signal_timestamp = self._fresh_signal_timestamp()

        result = guard.validate(
            request=request,
            signal_timestamp=signal_timestamp,
            signal_entry_price=100.0,
            current_price=100.0,
            is_live_mode=False,
            is_live_armed=False,
            is_kill_switch_active=True,  # Active!
            active_symbol="AAPL",
        )

        assert result.accepted is False
        assert result.reason == BlockReason.KILL_SWITCH_ACTIVE
        assert "kill" in result.details.lower()

    def test_no_signal_rejected(self, guard: ConfirmationGuard):
        """
        Test: No active signal → rejected.

        Given: No signal timestamp (signal is None)
        When: Human confirms entry
        Then: Confirmation rejected with NO_ACTIVE_SIGNAL
        """
        request = self._create_request()

        result = guard.validate(
            request=request,
            signal_timestamp=None,  # No signal!
            signal_entry_price=None,
            current_price=100.0,
            is_live_mode=False,
            is_live_armed=False,
            is_kill_switch_active=False,
            active_symbol="AAPL",
        )

        assert result.accepted is False
        assert result.reason == BlockReason.NO_ACTIVE_SIGNAL
        assert "no active signal" in result.details.lower()

    def test_strict_config_lower_ttl(self, strict_guard: ConfirmationGuard):
        """
        Test: Strict config with 2s TTL rejects 3s signal.
        """
        request = self._create_request()
        signal_timestamp = self._stale_signal_timestamp(age_ms=3000)

        result = strict_guard.validate(
            request=request,
            signal_timestamp=signal_timestamp,
            signal_entry_price=100.0,
            current_price=100.0,
            is_live_mode=False,
            is_live_armed=False,
            is_kill_switch_active=False,
            active_symbol="AAPL",
        )

        assert result.accepted is False
        assert result.reason == BlockReason.SIGNAL_STALE

    def test_strict_config_lower_drift(self, strict_guard: ConfirmationGuard):
        """
        Test: Strict config with 0.1% drift rejects 0.2% drift.
        """
        request = self._create_request(entry_price=100.0)
        signal_timestamp = self._fresh_signal_timestamp()

        result = strict_guard.validate(
            request=request,
            signal_timestamp=signal_timestamp,
            signal_entry_price=100.0,
            current_price=100.2,  # 0.2% drift > 0.1% max
            is_live_mode=False,
            is_live_armed=False,
            is_kill_switch_active=False,
            active_symbol="AAPL",
        )

        assert result.accepted is False
        assert result.reason == BlockReason.PRICE_DRIFT_EXCEEDED

    def test_no_active_symbol_accepts_any(self, guard: ConfirmationGuard):
        """
        Test: If no active symbol in context, accept any symbol.

        This handles the case where Morpheus context doesn't have a symbol set.
        """
        request = self._create_request(symbol="AAPL")
        signal_timestamp = self._fresh_signal_timestamp()

        result = guard.validate(
            request=request,
            signal_timestamp=signal_timestamp,
            signal_entry_price=100.0,
            current_price=100.0,
            is_live_mode=False,
            is_live_armed=False,
            is_kill_switch_active=False,
            active_symbol=None,  # No active symbol
        )

        assert result.accepted is True

    def test_result_to_event_payload(self, guard: ConfirmationGuard):
        """
        Test: ConfirmationResult can be converted to event payload.
        """
        request = self._create_request()
        signal_timestamp = self._fresh_signal_timestamp()

        result = guard.validate(
            request=request,
            signal_timestamp=signal_timestamp,
            signal_entry_price=100.0,
            current_price=100.0,
            is_live_mode=False,
            is_live_armed=False,
            is_kill_switch_active=False,
            active_symbol="AAPL",
        )

        payload = result.to_event_payload()

        assert "accepted" in payload
        assert payload["accepted"] is True
        assert "reason" in payload
        assert "details" in payload
        assert "signal_age_ms" in payload
        assert "price_drift_pct" in payload


class TestConfirmationGuardEdgeCases:
    """Edge case tests for ConfirmationGuard."""

    @pytest.fixture
    def guard(self) -> ConfirmationGuard:
        return create_confirmation_guard()

    def test_invalid_timestamp_format(self, guard: ConfirmationGuard):
        """
        Test: Invalid timestamp format is treated as stale.
        """
        request = ConfirmationRequest(
            symbol="AAPL",
            chain_id=1,
            signal_timestamp="not-a-timestamp",
            entry_price=100.0,
            command_id="test-cmd",
        )

        result = guard.validate(
            request=request,
            signal_timestamp="also-invalid",  # Invalid format
            signal_entry_price=100.0,
            current_price=100.0,
            is_live_mode=False,
            is_live_armed=False,
            is_kill_switch_active=False,
            active_symbol="AAPL",
        )

        assert result.accepted is False
        assert result.reason == BlockReason.SIGNAL_STALE

    def test_zero_entry_price_skips_drift(self, guard: ConfirmationGuard):
        """
        Test: Zero entry price skips drift check.
        """
        request = ConfirmationRequest(
            symbol="AAPL",
            chain_id=1,
            signal_timestamp=datetime.now(timezone.utc).isoformat(),
            entry_price=0.0,  # Zero
            command_id="test-cmd",
        )
        signal_timestamp = datetime.now(timezone.utc).isoformat()

        result = guard.validate(
            request=request,
            signal_timestamp=signal_timestamp,
            signal_entry_price=0.0,  # Zero
            current_price=100.0,
            is_live_mode=False,
            is_live_armed=False,
            is_kill_switch_active=False,
            active_symbol="AAPL",
        )

        assert result.accepted is True
        assert result.price_drift_pct == 0.0

    def test_priority_order_kill_switch_first(self, guard: ConfirmationGuard):
        """
        Test: Kill switch check has highest priority.

        Even if signal is stale and symbol mismatches,
        kill switch reason should be returned.
        """
        request = ConfirmationRequest(
            symbol="AAPL",
            chain_id=1,
            signal_timestamp=datetime.now(timezone.utc).isoformat(),
            entry_price=100.0,
            command_id="test-cmd",
        )

        result = guard.validate(
            request=request,
            signal_timestamp=None,  # No signal
            signal_entry_price=None,
            current_price=100.0,
            is_live_mode=False,
            is_live_armed=False,
            is_kill_switch_active=True,  # Kill switch first
            active_symbol="MSFT",  # Symbol mismatch
        )

        # Kill switch should be the reason, not symbol mismatch or no signal
        assert result.accepted is False
        assert result.reason == BlockReason.KILL_SWITCH_ACTIVE
