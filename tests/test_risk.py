"""
Phase 6 Tests - Risk Overlay + Position Governance.

Tests for:
- Position sizing (PositionSizer implementations)
- Risk overlay (RiskOverlay implementations)
- Kill switch logic
- Event emission
- Determinism
- Phase isolation
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from morpheus.core.events import EventType
from morpheus.risk import (
    RISK_SCHEMA_VERSION,
    VetoReason,
    RiskDecision,
    PositionSize,
    AccountState,
    RiskResult,
    PositionSizer,
    RiskOverlay,
    SizingMethod,
    PositionSizerConfig,
    StandardPositionSizer,
    ConservativePositionSizer,
    AggressivePositionSizer,
    create_standard_sizer,
    create_conservative_sizer,
    create_aggressive_sizer,
    RiskManagerConfig,
    StandardRiskManager,
    PermissiveRiskManager,
    StrictRiskManager,
    create_standard_risk_manager,
    create_permissive_risk_manager,
    create_strict_risk_manager,
    KillSwitchTrigger,
    KillSwitchState,
    KillSwitchConfig,
    KillSwitchResult,
    KillSwitch,
    ConservativeKillSwitch,
    AggressiveKillSwitch,
    create_kill_switch,
    create_conservative_kill_switch,
    create_aggressive_kill_switch,
)
from morpheus.scoring.base import (
    GateDecision,
    GateResult,
    ScoredSignal,
)
from morpheus.strategies.base import SignalCandidate, SignalDirection


# ============================================================================
# Test Fixtures
# ============================================================================


def create_account(
    total_equity: Decimal = Decimal("100000"),
    cash_available: Decimal | None = None,
    buying_power: Decimal | None = None,
    open_position_count: int = 0,
    total_exposure: Decimal = Decimal("0"),
    daily_pnl: Decimal = Decimal("0"),
    daily_pnl_pct: float = 0.0,
    peak_equity: Decimal | None = None,
    current_drawdown_pct: float = 0.0,
    kill_switch_active: bool = False,
    manual_halt: bool = False,
) -> AccountState:
    """Create an AccountState for testing."""
    return AccountState(
        total_equity=total_equity,
        cash_available=cash_available or total_equity,
        buying_power=buying_power or total_equity * Decimal("2"),  # 2x margin
        open_position_count=open_position_count,
        total_exposure=total_exposure,
        exposure_pct=float(total_exposure / total_equity) if total_equity > 0 else 0.0,
        daily_pnl=daily_pnl,
        daily_pnl_pct=daily_pnl_pct,
        peak_equity=peak_equity or total_equity,
        current_drawdown_pct=current_drawdown_pct,
        kill_switch_active=kill_switch_active,
        manual_halt=manual_halt,
    )


def create_signal(
    symbol: str = "AAPL",
    direction: SignalDirection = SignalDirection.LONG,
    strategy_name: str = "test_strategy",
) -> SignalCandidate:
    """Create a SignalCandidate for testing."""
    return SignalCandidate(
        symbol=symbol,
        direction=direction,
        strategy_name=strategy_name,
        triggering_features=("rsi_14", "trend_strength"),
        entry_reference=150.0,
        tags=("momentum",),
        timestamp=datetime.now(timezone.utc),
    )


def create_scored_signal(
    symbol: str = "AAPL",
    direction: SignalDirection = SignalDirection.LONG,
    confidence: float = 0.7,
) -> ScoredSignal:
    """Create a ScoredSignal for testing."""
    signal = create_signal(symbol, direction)
    return ScoredSignal(
        signal=signal,
        confidence=confidence,
        model_name="test_scorer",
        model_version="1.0.0",
        score_rationale="Test scoring",
        contributing_factors=("trend", "rsi"),
        feature_snapshot={"atr_pct": 1.5, "rsi_14": 45.0},
    )


def create_gate_result(
    symbol: str = "AAPL",
    decision: GateDecision = GateDecision.APPROVED,
    confidence: float = 0.7,
) -> GateResult:
    """Create a GateResult for testing."""
    scored_signal = create_scored_signal(symbol, confidence=confidence)
    return GateResult(
        scored_signal=scored_signal,
        decision=decision,
        reasons=(),
        reason_details="Approved" if decision == GateDecision.APPROVED else "Rejected",
        gate_name="test_gate",
        gate_version="1.0.0",
        min_confidence_required=0.5,
        confidence_met=True,
    )


# ============================================================================
# Position Size Tests
# ============================================================================


class TestPositionSize:
    """Tests for PositionSize dataclass."""

    def test_position_size_immutable(self):
        """PositionSize should be immutable."""
        ps = PositionSize(shares=100, entry_price=Decimal("150.00"))
        with pytest.raises(Exception):  # FrozenInstanceError
            ps.shares = 200

    def test_position_size_to_payload(self):
        """PositionSize should serialize to event payload."""
        ps = PositionSize(
            shares=100,
            notional_value=Decimal("15000.00"),
            entry_price=Decimal("150.00"),
            stop_price=Decimal("147.00"),
            risk_per_share=Decimal("3.00"),
            position_pct_of_account=0.15,
            risk_pct_of_account=0.003,
            sizer_name="test_sizer",
            sizer_version="1.0.0",
            sizing_rationale="Test sizing",
        )
        payload = ps.to_event_payload()

        assert payload["shares"] == 100
        assert payload["notional_value"] == "15000.00"
        assert payload["entry_price"] == "150.00"
        assert payload["stop_price"] == "147.00"
        assert payload["sizer_name"] == "test_sizer"


# ============================================================================
# Account State Tests
# ============================================================================


class TestAccountState:
    """Tests for AccountState dataclass."""

    def test_account_state_immutable(self):
        """AccountState should be immutable."""
        account = create_account()
        with pytest.raises(Exception):
            account.total_equity = Decimal("50000")

    def test_account_state_to_dict(self):
        """AccountState should serialize to dict."""
        account = create_account(
            total_equity=Decimal("100000"),
            daily_pnl_pct=-0.02,
            open_position_count=3,
        )
        data = account.to_dict()

        assert data["total_equity"] == "100000"
        assert data["daily_pnl_pct"] == -0.02
        assert data["open_position_count"] == 3


# ============================================================================
# Risk Result Tests
# ============================================================================


class TestRiskResult:
    """Tests for RiskResult dataclass."""

    def test_risk_result_immutable(self):
        """RiskResult should be immutable."""
        gate_result = create_gate_result()
        result = RiskResult(
            gate_result=gate_result,
            decision=RiskDecision.APPROVED,
        )
        with pytest.raises(Exception):
            result.decision = RiskDecision.VETOED

    def test_risk_result_approved_event(self):
        """Approved RiskResult should emit RISK_APPROVED event."""
        gate_result = create_gate_result()
        result = RiskResult(
            gate_result=gate_result,
            decision=RiskDecision.APPROVED,
            overlay_name="test_overlay",
        )

        assert result.is_approved
        assert not result.is_vetoed

        event = result.to_event()
        assert event.event_type == EventType.RISK_APPROVED

    def test_risk_result_vetoed_event(self):
        """Vetoed RiskResult should emit RISK_VETO event."""
        gate_result = create_gate_result()
        result = RiskResult(
            gate_result=gate_result,
            decision=RiskDecision.VETOED,
            veto_reasons=(VetoReason.MAX_DAILY_LOSS,),
            overlay_name="test_overlay",
        )

        assert result.is_vetoed
        assert not result.is_approved

        event = result.to_event()
        assert event.event_type == EventType.RISK_VETO


# ============================================================================
# Position Sizer Tests
# ============================================================================


class TestStandardPositionSizer:
    """Tests for StandardPositionSizer."""

    def test_fixed_percent_risk_sizing(self):
        """Fixed percent risk should size based on stop distance."""
        config = PositionSizerConfig(
            method=SizingMethod.FIXED_PERCENT_RISK,
            risk_per_trade_pct=0.01,  # 1% risk
            max_position_pct=0.50,  # Allow larger position for this test
        )
        sizer = StandardPositionSizer(config)
        account = create_account(total_equity=Decimal("100000"))
        gate_result = create_gate_result()

        # $100,000 * 1% = $1,000 risk
        # Stop at $147, entry at $150 = $3 risk per share
        # $1,000 / $3 = 333 shares -> rounded to 300
        size = sizer.compute_size(
            gate_result=gate_result,
            account=account,
            entry_price=Decimal("150.00"),
            stop_price=Decimal("147.00"),
        )

        assert size.shares == 300  # Rounded to 100s
        assert size.entry_price == Decimal("150.00")
        assert size.stop_price == Decimal("147.00")

    def test_fixed_fractional_sizing(self):
        """Fixed fractional should size as percentage of account."""
        config = PositionSizerConfig(
            method=SizingMethod.FIXED_FRACTIONAL,
            position_pct=0.05,  # 5% position
        )
        sizer = StandardPositionSizer(config)
        account = create_account(total_equity=Decimal("100000"))
        gate_result = create_gate_result()

        # $100,000 * 5% = $5,000
        # $5,000 / $150 = 33 shares
        size = sizer.compute_size(
            gate_result=gate_result,
            account=account,
            entry_price=Decimal("150.00"),
        )

        assert size.shares > 0
        assert size.sizer_name == "standard_position_sizer"

    def test_volatility_adjusted_sizing(self):
        """Volatility-adjusted should size based on ATR."""
        config = PositionSizerConfig(
            method=SizingMethod.VOLATILITY_ADJUSTED,
            atr_multiplier=2.0,
            vol_risk_pct=0.01,
        )
        sizer = StandardPositionSizer(config)
        account = create_account(total_equity=Decimal("100000"))
        gate_result = create_gate_result()  # Has atr_pct=1.5 in feature_snapshot

        size = sizer.compute_size(
            gate_result=gate_result,
            account=account,
            entry_price=Decimal("150.00"),
        )

        assert size.shares > 0
        assert "Volatility" in size.sizing_rationale

    def test_respects_max_position_limit(self):
        """Sizer should respect max position percentage."""
        config = PositionSizerConfig(
            method=SizingMethod.FIXED_FRACTIONAL,
            position_pct=0.50,  # 50% - too high
            max_position_pct=0.10,  # Limited to 10%
        )
        sizer = StandardPositionSizer(config)
        account = create_account(total_equity=Decimal("100000"))
        gate_result = create_gate_result()

        size = sizer.compute_size(
            gate_result=gate_result,
            account=account,
            entry_price=Decimal("100.00"),
        )

        # Should be capped at 10% = $10,000 / $100 = 100 shares
        assert size.notional_value <= Decimal("10000")

    def test_respects_buying_power(self):
        """Sizer should not exceed buying power."""
        config = PositionSizerConfig(
            method=SizingMethod.FIXED_FRACTIONAL,
            position_pct=0.50,
        )
        sizer = StandardPositionSizer(config)
        account = create_account(
            total_equity=Decimal("100000"),
            buying_power=Decimal("5000"),  # Only $5,000 available
        )
        gate_result = create_gate_result()

        size = sizer.compute_size(
            gate_result=gate_result,
            account=account,
            entry_price=Decimal("100.00"),
        )

        # Should be capped by buying power: $5,000 / $100 = 50 shares
        assert size.notional_value <= Decimal("5000")

    def test_default_stop_used_when_not_provided(self):
        """Sizer should use default stop percentage when stop not provided."""
        config = PositionSizerConfig(
            method=SizingMethod.FIXED_PERCENT_RISK,
            default_stop_pct=0.02,  # 2% default stop
        )
        sizer = StandardPositionSizer(config)
        account = create_account()
        gate_result = create_gate_result()

        size = sizer.compute_size(
            gate_result=gate_result,
            account=account,
            entry_price=Decimal("100.00"),
            stop_price=None,  # No stop provided
        )

        # Stop should be entry * (1 - 0.02) = $98
        assert size.stop_price == Decimal("98.00")


class TestConservativePositionSizer:
    """Tests for ConservativePositionSizer."""

    def test_conservative_has_tight_limits(self):
        """Conservative sizer should have tight limits."""
        sizer = ConservativePositionSizer()
        account = create_account(total_equity=Decimal("100000"))
        gate_result = create_gate_result()

        size = sizer.compute_size(
            gate_result=gate_result,
            account=account,
            entry_price=Decimal("100.00"),
            stop_price=Decimal("99.00"),
        )

        # Conservative uses 0.5% risk
        # $100,000 * 0.5% = $500 risk
        # $500 / $1 risk per share = 500 shares
        # But max position is 5% = $5,000 = 50 shares
        assert size.shares <= 500


class TestAggressivePositionSizer:
    """Tests for AggressivePositionSizer."""

    def test_aggressive_has_higher_limits(self):
        """Aggressive sizer should have higher limits than conservative."""
        aggressive = AggressivePositionSizer()
        conservative = ConservativePositionSizer()
        account = create_account(total_equity=Decimal("100000"))
        gate_result = create_gate_result()

        agg_size = aggressive.compute_size(
            gate_result=gate_result,
            account=account,
            entry_price=Decimal("100.00"),
            stop_price=Decimal("99.00"),
        )
        con_size = conservative.compute_size(
            gate_result=gate_result,
            account=account,
            entry_price=Decimal("100.00"),
            stop_price=Decimal("99.00"),
        )

        # Aggressive should size larger
        assert agg_size.shares >= con_size.shares


class TestPositionSizerFactories:
    """Tests for position sizer factory functions."""

    def test_create_standard_sizer(self):
        """create_standard_sizer should return StandardPositionSizer."""
        sizer = create_standard_sizer()
        assert isinstance(sizer, StandardPositionSizer)

    def test_create_conservative_sizer(self):
        """create_conservative_sizer should return ConservativePositionSizer."""
        sizer = create_conservative_sizer()
        assert isinstance(sizer, ConservativePositionSizer)

    def test_create_aggressive_sizer(self):
        """create_aggressive_sizer should return AggressivePositionSizer."""
        sizer = create_aggressive_sizer()
        assert isinstance(sizer, AggressivePositionSizer)


# ============================================================================
# Risk Manager Tests
# ============================================================================


class TestStandardRiskManager:
    """Tests for StandardRiskManager."""

    def test_approves_within_limits(self):
        """Risk manager should approve trades within all limits."""
        manager = StandardRiskManager()
        account = create_account(
            total_equity=Decimal("100000"),
            open_position_count=2,
            daily_pnl_pct=0.0,
            current_drawdown_pct=0.0,
        )
        gate_result = create_gate_result()
        position_size = PositionSize(
            shares=100,
            notional_value=Decimal("5000"),
            entry_price=Decimal("50.00"),
            position_pct_of_account=0.05,
        )

        result = manager.evaluate(gate_result, position_size, account)

        assert result.is_approved
        assert len(result.veto_reasons) == 0

    def test_vetoes_kill_switch_active(self):
        """Risk manager should veto when kill switch is active."""
        manager = StandardRiskManager()
        account = create_account(kill_switch_active=True)
        gate_result = create_gate_result()
        position_size = PositionSize(shares=100, entry_price=Decimal("50.00"))

        result = manager.evaluate(gate_result, position_size, account)

        assert result.is_vetoed
        assert VetoReason.KILL_SWITCH_ACTIVE in result.veto_reasons

    def test_vetoes_manual_halt(self):
        """Risk manager should veto when manual halt is active."""
        manager = StandardRiskManager()
        account = create_account(manual_halt=True)
        gate_result = create_gate_result()
        position_size = PositionSize(shares=100, entry_price=Decimal("50.00"))

        result = manager.evaluate(gate_result, position_size, account)

        assert result.is_vetoed
        assert VetoReason.MANUAL_HALT in result.veto_reasons

    def test_vetoes_max_positions(self):
        """Risk manager should veto when max positions reached."""
        config = RiskManagerConfig(max_open_positions=3)
        manager = StandardRiskManager(config)
        account = create_account(open_position_count=3)
        gate_result = create_gate_result()
        position_size = PositionSize(shares=100, entry_price=Decimal("50.00"))

        result = manager.evaluate(gate_result, position_size, account)

        assert result.is_vetoed
        assert VetoReason.MAX_OPEN_POSITIONS in result.veto_reasons

    def test_vetoes_daily_loss_exceeded(self):
        """Risk manager should veto when daily loss exceeded."""
        config = RiskManagerConfig(max_daily_loss_pct=0.03)
        manager = StandardRiskManager(config)
        account = create_account(daily_pnl_pct=-0.04)  # 4% loss
        gate_result = create_gate_result()
        position_size = PositionSize(shares=100, entry_price=Decimal("50.00"))

        result = manager.evaluate(gate_result, position_size, account)

        assert result.is_vetoed
        assert VetoReason.MAX_DAILY_LOSS in result.veto_reasons

    def test_vetoes_drawdown_exceeded(self):
        """Risk manager should veto when drawdown exceeded."""
        config = RiskManagerConfig(max_drawdown_pct=0.10)
        manager = StandardRiskManager(config)
        account = create_account(current_drawdown_pct=0.12)  # 12% drawdown
        gate_result = create_gate_result()
        position_size = PositionSize(shares=100, entry_price=Decimal("50.00"))

        result = manager.evaluate(gate_result, position_size, account)

        assert result.is_vetoed
        assert VetoReason.MAX_DRAWDOWN in result.veto_reasons

    def test_vetoes_position_too_large(self):
        """Risk manager should veto when position too large."""
        config = RiskManagerConfig(max_position_pct=0.05)
        manager = StandardRiskManager(config)
        account = create_account()
        gate_result = create_gate_result()
        position_size = PositionSize(
            shares=100,
            entry_price=Decimal("50.00"),
            notional_value=Decimal("10000"),  # 10%
            position_pct_of_account=0.10,  # Too large
        )

        result = manager.evaluate(gate_result, position_size, account)

        assert result.is_vetoed
        assert VetoReason.MAX_POSITION_SIZE in result.veto_reasons

    def test_vetoes_insufficient_buying_power(self):
        """Risk manager should veto when insufficient buying power."""
        manager = StandardRiskManager()
        account = create_account(
            total_equity=Decimal("100000"),
            buying_power=Decimal("5000"),  # Only $5k BP
        )
        gate_result = create_gate_result()
        position_size = PositionSize(
            shares=100,
            entry_price=Decimal("100.00"),
            notional_value=Decimal("10000"),  # Need $10k
        )

        result = manager.evaluate(gate_result, position_size, account)

        assert result.is_vetoed
        assert VetoReason.INSUFFICIENT_BUYING_POWER in result.veto_reasons

    def test_multiple_reasons_collected(self):
        """Risk manager should collect multiple veto reasons."""
        config = RiskManagerConfig(
            max_daily_loss_pct=0.03,
            max_drawdown_pct=0.10,
        )
        manager = StandardRiskManager(config)
        account = create_account(
            daily_pnl_pct=-0.05,  # Exceeds daily loss
            current_drawdown_pct=0.15,  # Exceeds drawdown
        )
        gate_result = create_gate_result()
        position_size = PositionSize(shares=100, entry_price=Decimal("50.00"))

        result = manager.evaluate(gate_result, position_size, account)

        assert result.is_vetoed
        assert VetoReason.MAX_DAILY_LOSS in result.veto_reasons
        assert VetoReason.MAX_DRAWDOWN in result.veto_reasons


class TestPermissiveRiskManager:
    """Tests for PermissiveRiskManager."""

    def test_approves_most_trades(self):
        """Permissive manager should approve most trades."""
        manager = PermissiveRiskManager()
        account = create_account(
            daily_pnl_pct=-0.10,  # 10% loss - would trigger standard
            current_drawdown_pct=0.20,  # 20% drawdown - would trigger standard
        )
        gate_result = create_gate_result()
        position_size = PositionSize(
            shares=100,
            entry_price=Decimal("50.00"),
            notional_value=Decimal("5000"),
        )

        result = manager.evaluate(gate_result, position_size, account)

        # Should approve despite losses/drawdown
        assert result.is_approved

    def test_still_checks_kill_switch(self):
        """Permissive manager should still check kill switch."""
        manager = PermissiveRiskManager()
        account = create_account(kill_switch_active=True)
        gate_result = create_gate_result()
        position_size = PositionSize(shares=100, entry_price=Decimal("50.00"))

        result = manager.evaluate(gate_result, position_size, account)

        assert result.is_vetoed
        assert VetoReason.KILL_SWITCH_ACTIVE in result.veto_reasons


class TestStrictRiskManager:
    """Tests for StrictRiskManager."""

    def test_has_tighter_limits(self):
        """Strict manager should have tighter limits."""
        strict = StrictRiskManager()
        standard = StandardRiskManager()
        # Account that passes standard but fails strict
        account = create_account(
            open_position_count=4,  # Passes standard (5), fails strict (3)
        )
        gate_result = create_gate_result()
        position_size = PositionSize(
            shares=100,
            entry_price=Decimal("50.00"),
            notional_value=Decimal("5000"),
            position_pct_of_account=0.05,
        )

        strict_result = strict.evaluate(gate_result, position_size, account)
        standard_result = standard.evaluate(gate_result, position_size, account)

        # Standard should approve, strict should veto
        assert standard_result.is_approved
        assert strict_result.is_vetoed


class TestRiskManagerFactories:
    """Tests for risk manager factory functions."""

    def test_create_standard_risk_manager(self):
        """create_standard_risk_manager should return StandardRiskManager."""
        manager = create_standard_risk_manager()
        assert isinstance(manager, StandardRiskManager)

    def test_create_permissive_risk_manager(self):
        """create_permissive_risk_manager should return PermissiveRiskManager."""
        manager = create_permissive_risk_manager()
        assert isinstance(manager, PermissiveRiskManager)

    def test_create_strict_risk_manager(self):
        """create_strict_risk_manager should return StrictRiskManager."""
        manager = create_strict_risk_manager()
        assert isinstance(manager, StrictRiskManager)


# ============================================================================
# Kill Switch Tests
# ============================================================================


class TestKillSwitch:
    """Tests for KillSwitch."""

    def test_armed_when_within_limits(self):
        """Kill switch should be armed when all limits within range."""
        ks = KillSwitch()
        account = create_account(
            daily_pnl_pct=-0.01,  # 1% loss - within 5% limit
            current_drawdown_pct=0.05,  # 5% DD - within 15% limit
        )

        result = ks.evaluate(account)

        assert result.state == KillSwitchState.ARMED
        assert not result.is_triggered

    def test_triggers_on_daily_loss(self):
        """Kill switch should trigger on daily loss threshold."""
        config = KillSwitchConfig(max_daily_loss_pct=0.05)
        ks = KillSwitch(config)
        account = create_account(daily_pnl_pct=-0.06)  # 6% loss

        result = ks.evaluate(account)

        assert result.is_triggered
        assert result.trigger_reason == KillSwitchTrigger.MAX_DAILY_LOSS

    def test_triggers_on_drawdown(self):
        """Kill switch should trigger on drawdown threshold."""
        config = KillSwitchConfig(max_drawdown_pct=0.15)
        ks = KillSwitch(config)
        account = create_account(current_drawdown_pct=0.20)  # 20% drawdown

        result = ks.evaluate(account)

        assert result.is_triggered
        assert result.trigger_reason == KillSwitchTrigger.MAX_DRAWDOWN

    def test_triggers_on_consecutive_losses(self):
        """Kill switch should trigger on consecutive losses."""
        config = KillSwitchConfig(max_consecutive_losses=5)
        ks = KillSwitch(config)
        account = create_account()

        result = ks.evaluate(account, consecutive_losses=6)

        assert result.is_triggered
        assert result.trigger_reason == KillSwitchTrigger.CONSECUTIVE_LOSSES

    def test_triggers_on_manual_activation(self):
        """Kill switch should trigger on manual activation."""
        ks = KillSwitch()
        account = create_account()

        result = ks.evaluate(account, manual_trigger=True)

        assert result.is_triggered
        assert result.trigger_reason == KillSwitchTrigger.MANUAL_ACTIVATION

    def test_recognizes_already_active(self):
        """Kill switch should recognize already active state."""
        ks = KillSwitch()
        account = create_account(kill_switch_active=True)

        result = ks.evaluate(account)

        assert result.is_triggered
        assert result.state == KillSwitchState.TRIGGERED

    def test_should_halt_convenience_method(self):
        """should_halt should return boolean trigger state."""
        ks = KillSwitch()
        safe_account = create_account(daily_pnl_pct=0.0)
        unsafe_account = create_account(daily_pnl_pct=-0.10)

        assert not ks.should_halt(safe_account)
        assert ks.should_halt(unsafe_account)

    def test_result_to_event(self):
        """KillSwitchResult should emit event."""
        ks = KillSwitch()
        account = create_account(daily_pnl_pct=-0.10)

        result = ks.evaluate(account)
        event = result.to_event()

        assert event.event_type == EventType.RISK_VETO
        assert "kill_switch" in event.payload


class TestConservativeKillSwitch:
    """Tests for ConservativeKillSwitch."""

    def test_has_tight_thresholds(self):
        """Conservative kill switch should have tight thresholds."""
        ks = ConservativeKillSwitch()
        # 3% daily loss should trigger conservative but not standard
        account = create_account(daily_pnl_pct=-0.04)

        result = ks.evaluate(account)

        assert result.is_triggered
        assert result.trigger_reason == KillSwitchTrigger.MAX_DAILY_LOSS


class TestAggressiveKillSwitch:
    """Tests for AggressiveKillSwitch."""

    def test_has_higher_thresholds(self):
        """Aggressive kill switch should have higher thresholds."""
        ks = AggressiveKillSwitch()
        # 8% daily loss should not trigger aggressive (10% threshold)
        account = create_account(daily_pnl_pct=-0.08)

        result = ks.evaluate(account)

        assert not result.is_triggered


class TestKillSwitchFactories:
    """Tests for kill switch factory functions."""

    def test_create_kill_switch(self):
        """create_kill_switch should return KillSwitch."""
        ks = create_kill_switch()
        assert isinstance(ks, KillSwitch)

    def test_create_conservative_kill_switch(self):
        """create_conservative_kill_switch should return ConservativeKillSwitch."""
        ks = create_conservative_kill_switch()
        assert isinstance(ks, ConservativeKillSwitch)

    def test_create_aggressive_kill_switch(self):
        """create_aggressive_kill_switch should return AggressiveKillSwitch."""
        ks = create_aggressive_kill_switch()
        assert isinstance(ks, AggressiveKillSwitch)


# ============================================================================
# Determinism Tests
# ============================================================================


class TestDeterminism:
    """Tests for deterministic behavior."""

    def test_position_sizer_deterministic(self):
        """Position sizer should produce same output for same input."""
        sizer = StandardPositionSizer()
        account = create_account()
        gate_result = create_gate_result()
        entry = Decimal("100.00")
        stop = Decimal("98.00")

        size1 = sizer.compute_size(gate_result, account, entry, stop)
        size2 = sizer.compute_size(gate_result, account, entry, stop)

        assert size1.shares == size2.shares
        assert size1.notional_value == size2.notional_value

    def test_risk_manager_deterministic(self):
        """Risk manager should produce same output for same input."""
        manager = StandardRiskManager()
        account = create_account()
        gate_result = create_gate_result()
        position_size = PositionSize(
            shares=100,
            entry_price=Decimal("50.00"),
            notional_value=Decimal("5000"),
            position_pct_of_account=0.05,
        )

        result1 = manager.evaluate(gate_result, position_size, account)
        result2 = manager.evaluate(gate_result, position_size, account)

        assert result1.decision == result2.decision
        assert result1.veto_reasons == result2.veto_reasons

    def test_kill_switch_deterministic(self):
        """Kill switch should produce same output for same input."""
        ks = KillSwitch()
        account = create_account(daily_pnl_pct=-0.06)

        result1 = ks.evaluate(account, consecutive_losses=3)
        result2 = ks.evaluate(account, consecutive_losses=3)

        assert result1.is_triggered == result2.is_triggered
        assert result1.trigger_reason == result2.trigger_reason


# ============================================================================
# Integration Tests
# ============================================================================


class TestRiskPipeline:
    """Tests for complete risk evaluation pipeline."""

    def test_full_approval_pipeline(self):
        """Test full pipeline from gate result to risk approval."""
        # Create components
        sizer = create_standard_sizer()
        manager = create_standard_risk_manager()
        ks = create_kill_switch()

        # Create test data
        account = create_account(
            total_equity=Decimal("100000"),
            open_position_count=1,
        )
        gate_result = create_gate_result()

        # Size the position
        position_size = sizer.compute_size(
            gate_result=gate_result,
            account=account,
            entry_price=Decimal("150.00"),
            stop_price=Decimal("147.00"),
        )

        # Check kill switch
        ks_result = ks.evaluate(account)
        assert not ks_result.is_triggered

        # Evaluate risk
        risk_result = manager.evaluate(gate_result, position_size, account)

        assert risk_result.is_approved
        assert risk_result.position_size is not None
        assert risk_result.position_size.shares > 0

    def test_full_veto_pipeline(self):
        """Test pipeline with risk veto."""
        sizer = create_standard_sizer()
        manager = create_standard_risk_manager()

        # Account exceeding limits
        account = create_account(
            total_equity=Decimal("100000"),
            daily_pnl_pct=-0.05,  # 5% daily loss
        )
        gate_result = create_gate_result()

        position_size = sizer.compute_size(
            gate_result=gate_result,
            account=account,
            entry_price=Decimal("150.00"),
        )

        risk_result = manager.evaluate(gate_result, position_size, account)

        assert risk_result.is_vetoed
        assert VetoReason.MAX_DAILY_LOSS in risk_result.veto_reasons


# ============================================================================
# Phase Isolation Tests
# ============================================================================


class TestPhaseIsolation:
    """Tests to ensure Phase 6 doesn't exceed its scope."""

    def test_no_order_imports_in_risk_base(self):
        """risk/base.py should not import order-related modules."""
        import morpheus.risk.base as base_module
        import inspect

        source = inspect.getsource(base_module)

        # Check for actual imports of forbidden modules
        assert "from morpheus.execution" not in source
        assert "import morpheus.execution" not in source
        assert "from morpheus.fsm" not in source
        assert "import morpheus.fsm" not in source

    def test_no_order_imports_in_position_sizer(self):
        """position_sizer.py should not import order-related modules."""
        import morpheus.risk.position_sizer as sizer_module
        import inspect

        source = inspect.getsource(sizer_module)

        assert "from morpheus.execution" not in source
        assert "import morpheus.execution" not in source
        assert "from morpheus.fsm" not in source
        assert "import morpheus.fsm" not in source

    def test_no_order_imports_in_risk_manager(self):
        """risk_manager.py should not import order-related modules."""
        import morpheus.risk.risk_manager as manager_module
        import inspect

        source = inspect.getsource(manager_module)

        assert "from morpheus.execution" not in source
        assert "import morpheus.execution" not in source
        assert "from morpheus.fsm" not in source
        assert "import morpheus.fsm" not in source

    def test_no_order_imports_in_kill_switch(self):
        """kill_switch.py should not import order-related modules."""
        import morpheus.risk.kill_switch as ks_module
        import inspect

        source = inspect.getsource(ks_module)

        assert "from morpheus.execution" not in source
        assert "import morpheus.execution" not in source
        assert "from morpheus.fsm" not in source
        assert "import morpheus.fsm" not in source


# ============================================================================
# Schema Version Tests
# ============================================================================


class TestSchemaVersion:
    """Tests for schema versioning."""

    def test_schema_version_in_position_size(self):
        """PositionSize should include schema version."""
        ps = PositionSize()
        assert ps.schema_version == RISK_SCHEMA_VERSION
        payload = ps.to_event_payload()
        assert payload["schema_version"] == RISK_SCHEMA_VERSION

    def test_schema_version_in_risk_result(self):
        """RiskResult should include schema version."""
        result = RiskResult()
        assert result.schema_version == RISK_SCHEMA_VERSION
        payload = result.to_event_payload()
        assert payload["schema_version"] == RISK_SCHEMA_VERSION

    def test_schema_version_in_kill_switch_result(self):
        """KillSwitchResult should include schema version."""
        result = KillSwitchResult()
        assert result.schema_version == RISK_SCHEMA_VERSION
        payload = result.to_event_payload()
        assert payload["schema_version"] == RISK_SCHEMA_VERSION
