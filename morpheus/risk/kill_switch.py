"""
Kill Switch - Emergency trading halt logic.

Evaluates conditions that warrant immediate trading halt:
- Maximum daily loss exceeded
- Maximum drawdown exceeded
- Rapid loss rate (multiple losses in short time)
- System errors or connectivity issues
- Manual activation

All evaluation is DETERMINISTIC: same input -> same decision.

Phase 6 Scope:
- Kill switch evaluation only
- No order cancellation
- No FSM state modification
- Veto only, no execution
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any

from morpheus.core.events import Event, EventType, create_event
from morpheus.risk.base import AccountState, RISK_SCHEMA_VERSION


class KillSwitchTrigger(Enum):
    """Reasons for kill switch activation."""

    MAX_DAILY_LOSS = "max_daily_loss"
    MAX_DRAWDOWN = "max_drawdown"
    RAPID_LOSS_RATE = "rapid_loss_rate"
    CONSECUTIVE_LOSSES = "consecutive_losses"
    SYSTEM_ERROR = "system_error"
    CONNECTIVITY_ISSUE = "connectivity_issue"
    MANUAL_ACTIVATION = "manual_activation"
    MARKET_CIRCUIT_BREAKER = "market_circuit_breaker"


class KillSwitchState(Enum):
    """Kill switch states."""

    ARMED = "armed"  # Ready to trigger
    TRIGGERED = "triggered"  # Trading halted
    MANUAL_OVERRIDE = "manual_override"  # Manually overridden
    DISABLED = "disabled"  # Disabled (testing only)


@dataclass(frozen=True)
class KillSwitchConfig:
    """
    Configuration for kill switch thresholds.

    Defines conditions that will trigger the kill switch.
    """

    # Daily loss threshold
    max_daily_loss_pct: float = 0.05  # 5% daily loss triggers

    # Drawdown threshold
    max_drawdown_pct: float = 0.15  # 15% drawdown triggers

    # Consecutive losses threshold
    max_consecutive_losses: int = 5

    # Rapid loss threshold (losses within time window)
    rapid_loss_count: int = 3
    rapid_loss_window_minutes: int = 30

    # Auto-reset after time (0 = no auto-reset)
    auto_reset_minutes: int = 0


@dataclass(frozen=True)
class KillSwitchResult:
    """
    Result of kill switch evaluation.

    Immutable snapshot of kill switch state.
    """

    schema_version: str = RISK_SCHEMA_VERSION

    # Current state
    state: KillSwitchState = KillSwitchState.ARMED

    # Activation details
    is_triggered: bool = False
    trigger_reason: KillSwitchTrigger | None = None
    trigger_details: str = ""

    # Thresholds that were checked
    daily_loss_pct: float = 0.0
    drawdown_pct: float = 0.0
    consecutive_losses: int = 0

    # Timestamps
    evaluated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    triggered_at: datetime | None = None

    def to_event_payload(self) -> dict[str, Any]:
        """Convert to event payload dict."""
        return {
            "schema_version": self.schema_version,
            "state": self.state.value,
            "is_triggered": self.is_triggered,
            "trigger_reason": self.trigger_reason.value if self.trigger_reason else None,
            "trigger_details": self.trigger_details,
            "daily_loss_pct": self.daily_loss_pct,
            "drawdown_pct": self.drawdown_pct,
            "consecutive_losses": self.consecutive_losses,
            "evaluated_at": self.evaluated_at.isoformat(),
            "triggered_at": self.triggered_at.isoformat() if self.triggered_at else None,
        }

    def to_event(self) -> Event:
        """Create a RISK_VETO event if triggered."""
        # Kill switch triggers emit RISK_VETO events
        return create_event(
            EventType.RISK_VETO,
            payload={
                "kill_switch": self.to_event_payload(),
                "reason": "kill_switch_triggered",
            },
            timestamp=self.evaluated_at,
        )


class KillSwitch:
    """
    Kill switch evaluator.

    Evaluates account state against kill switch thresholds.
    Does NOT maintain state - stateless evaluation.

    All evaluation is deterministic.
    """

    def __init__(self, config: KillSwitchConfig | None = None):
        """
        Initialize the kill switch.

        Args:
            config: Optional custom configuration
        """
        self.config = config or KillSwitchConfig()

    @property
    def name(self) -> str:
        return "kill_switch"

    @property
    def version(self) -> str:
        return "1.0.0"

    def evaluate(
        self,
        account: AccountState,
        consecutive_losses: int = 0,
        manual_trigger: bool = False,
    ) -> KillSwitchResult:
        """
        Evaluate whether kill switch should be triggered.

        Args:
            account: Current AccountState
            consecutive_losses: Number of consecutive losing trades
            manual_trigger: Manual activation flag

        Returns:
            KillSwitchResult with state and trigger details
        """
        cfg = self.config
        now = datetime.now(timezone.utc)

        # Check manual trigger first
        if manual_trigger:
            return KillSwitchResult(
                state=KillSwitchState.TRIGGERED,
                is_triggered=True,
                trigger_reason=KillSwitchTrigger.MANUAL_ACTIVATION,
                trigger_details="Manually activated",
                daily_loss_pct=account.daily_pnl_pct,
                drawdown_pct=account.current_drawdown_pct,
                consecutive_losses=consecutive_losses,
                evaluated_at=now,
                triggered_at=now,
            )

        # Check if already triggered (from account state)
        if account.kill_switch_active:
            return KillSwitchResult(
                state=KillSwitchState.TRIGGERED,
                is_triggered=True,
                trigger_reason=None,  # Unknown, already active
                trigger_details="Kill switch already active in account state",
                daily_loss_pct=account.daily_pnl_pct,
                drawdown_pct=account.current_drawdown_pct,
                consecutive_losses=consecutive_losses,
                evaluated_at=now,
                triggered_at=None,
            )

        # Check daily loss threshold
        # Note: daily_pnl_pct is negative for losses
        if account.daily_pnl_pct <= -cfg.max_daily_loss_pct:
            return KillSwitchResult(
                state=KillSwitchState.TRIGGERED,
                is_triggered=True,
                trigger_reason=KillSwitchTrigger.MAX_DAILY_LOSS,
                trigger_details=f"Daily loss {account.daily_pnl_pct:.2%} exceeds -{cfg.max_daily_loss_pct:.2%}",
                daily_loss_pct=account.daily_pnl_pct,
                drawdown_pct=account.current_drawdown_pct,
                consecutive_losses=consecutive_losses,
                evaluated_at=now,
                triggered_at=now,
            )

        # Check drawdown threshold
        if account.current_drawdown_pct >= cfg.max_drawdown_pct:
            return KillSwitchResult(
                state=KillSwitchState.TRIGGERED,
                is_triggered=True,
                trigger_reason=KillSwitchTrigger.MAX_DRAWDOWN,
                trigger_details=f"Drawdown {account.current_drawdown_pct:.2%} exceeds {cfg.max_drawdown_pct:.2%}",
                daily_loss_pct=account.daily_pnl_pct,
                drawdown_pct=account.current_drawdown_pct,
                consecutive_losses=consecutive_losses,
                evaluated_at=now,
                triggered_at=now,
            )

        # Check consecutive losses
        if consecutive_losses >= cfg.max_consecutive_losses:
            return KillSwitchResult(
                state=KillSwitchState.TRIGGERED,
                is_triggered=True,
                trigger_reason=KillSwitchTrigger.CONSECUTIVE_LOSSES,
                trigger_details=f"Consecutive losses {consecutive_losses} >= {cfg.max_consecutive_losses}",
                daily_loss_pct=account.daily_pnl_pct,
                drawdown_pct=account.current_drawdown_pct,
                consecutive_losses=consecutive_losses,
                evaluated_at=now,
                triggered_at=now,
            )

        # All checks passed - armed but not triggered
        return KillSwitchResult(
            state=KillSwitchState.ARMED,
            is_triggered=False,
            trigger_reason=None,
            trigger_details="All thresholds within limits",
            daily_loss_pct=account.daily_pnl_pct,
            drawdown_pct=account.current_drawdown_pct,
            consecutive_losses=consecutive_losses,
            evaluated_at=now,
            triggered_at=None,
        )

    def should_halt(
        self,
        account: AccountState,
        consecutive_losses: int = 0,
    ) -> bool:
        """
        Simple check if trading should be halted.

        Args:
            account: Current AccountState
            consecutive_losses: Number of consecutive losing trades

        Returns:
            True if trading should halt
        """
        result = self.evaluate(account, consecutive_losses)
        return result.is_triggered


class ConservativeKillSwitch(KillSwitch):
    """
    Conservative kill switch with tight thresholds.

    Triggers on:
    - 3% daily loss
    - 8% drawdown
    - 3 consecutive losses
    """

    def __init__(self):
        """Initialize with conservative thresholds."""
        super().__init__(
            KillSwitchConfig(
                max_daily_loss_pct=0.03,
                max_drawdown_pct=0.08,
                max_consecutive_losses=3,
            )
        )

    @property
    def name(self) -> str:
        return "conservative_kill_switch"


class AggressiveKillSwitch(KillSwitch):
    """
    More permissive kill switch for aggressive strategies.

    Triggers on:
    - 10% daily loss
    - 25% drawdown
    - 7 consecutive losses
    """

    def __init__(self):
        """Initialize with higher thresholds."""
        super().__init__(
            KillSwitchConfig(
                max_daily_loss_pct=0.10,
                max_drawdown_pct=0.25,
                max_consecutive_losses=7,
            )
        )

    @property
    def name(self) -> str:
        return "aggressive_kill_switch"


def create_kill_switch(config: KillSwitchConfig | None = None) -> KillSwitch:
    """Factory function to create a KillSwitch."""
    return KillSwitch(config)


def create_conservative_kill_switch() -> ConservativeKillSwitch:
    """Factory function to create a ConservativeKillSwitch."""
    return ConservativeKillSwitch()


def create_aggressive_kill_switch() -> AggressiveKillSwitch:
    """Factory function to create an AggressiveKillSwitch."""
    return AggressiveKillSwitch()
