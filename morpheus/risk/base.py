"""
Risk Base Contract - Defines position sizing and risk overlay interface.

Position sizers compute appropriate position sizes for approved signals.
Risk overlays evaluate account-level risk and can veto trades.

All risk evaluation is DETERMINISTIC: same input -> same decision.

Phase 6 Scope:
- Position sizing only
- Risk evaluation and veto only
- No order execution
- No FSM state modification
- No trade placement
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any

from morpheus.core.events import Event, EventType, create_event
from morpheus.scoring.base import GateResult


# Schema version for risk results
RISK_SCHEMA_VERSION = "1.0"


class VetoReason(Enum):
    """Standard risk veto reason codes."""

    MAX_POSITION_SIZE = "max_position_size"
    MAX_DAILY_LOSS = "max_daily_loss"
    MAX_DRAWDOWN = "max_drawdown"
    MAX_OPEN_POSITIONS = "max_open_positions"
    MAX_SECTOR_EXPOSURE = "max_sector_exposure"
    MAX_CORRELATION = "max_correlation"
    KILL_SWITCH_ACTIVE = "kill_switch_active"
    INSUFFICIENT_BUYING_POWER = "insufficient_buying_power"
    SYMBOL_BLOCKED = "symbol_blocked"
    MANUAL_HALT = "manual_halt"
    INSUFFICIENT_ROOM_TO_PROFIT = "insufficient_room_to_profit"  # Spread/slippage eats profit

    # MASS-specific veto reasons
    SYMBOL_COOLDOWN = "symbol_cooldown"
    REGIME_POSITION_LIMIT = "regime_position_limit"
    STRATEGY_RISK_EXCEEDED = "strategy_risk_exceeded"


class RiskDecision(Enum):
    """Risk evaluation decision outcomes."""

    APPROVED = "approved"
    VETOED = "vetoed"


@dataclass(frozen=True)
class PositionSize:
    """
    Computed position sizing for a trade.

    Contains:
    - Number of shares/contracts
    - Dollar amount
    - Risk metrics used in sizing
    - Sizing methodology

    Immutable to ensure determinism.
    Does NOT contain:
    - Order details (Phase 7+)
    - Execution parameters (Phase 7+)
    """

    schema_version: str = RISK_SCHEMA_VERSION

    # Core sizing
    shares: int = 0
    notional_value: Decimal = Decimal("0")

    # Risk-based sizing inputs
    entry_price: Decimal = Decimal("0")
    stop_price: Decimal | None = None
    risk_per_share: Decimal = Decimal("0")

    # Account-relative metrics
    position_pct_of_account: float = 0.0
    risk_pct_of_account: float = 0.0

    # Sizing methodology
    sizer_name: str = ""
    sizer_version: str = ""
    sizing_rationale: str = ""

    # Timestamp
    computed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_event_payload(self) -> dict[str, Any]:
        """Convert to event payload dict."""
        return {
            "schema_version": self.schema_version,
            "shares": self.shares,
            "notional_value": str(self.notional_value),
            "entry_price": str(self.entry_price),
            "stop_price": str(self.stop_price) if self.stop_price else None,
            "risk_per_share": str(self.risk_per_share),
            "position_pct_of_account": self.position_pct_of_account,
            "risk_pct_of_account": self.risk_pct_of_account,
            "sizer_name": self.sizer_name,
            "sizer_version": self.sizer_version,
            "sizing_rationale": self.sizing_rationale,
            "computed_at": self.computed_at.isoformat(),
        }


@dataclass(frozen=True)
class AccountState:
    """
    Current account state for risk calculations.

    Snapshot of account metrics at evaluation time.
    Immutable to ensure determinism.
    """

    # Account balance
    total_equity: Decimal = Decimal("0")
    cash_available: Decimal = Decimal("0")
    buying_power: Decimal = Decimal("0")

    # Current exposure
    open_position_count: int = 0
    total_exposure: Decimal = Decimal("0")
    exposure_pct: float = 0.0

    # Daily P&L
    daily_pnl: Decimal = Decimal("0")
    daily_pnl_pct: float = 0.0

    # Drawdown
    peak_equity: Decimal = Decimal("0")
    current_drawdown_pct: float = 0.0

    # Risk state
    kill_switch_active: bool = False
    manual_halt: bool = False

    # Timestamp
    as_of: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict for serialization."""
        return {
            "total_equity": str(self.total_equity),
            "cash_available": str(self.cash_available),
            "buying_power": str(self.buying_power),
            "open_position_count": self.open_position_count,
            "total_exposure": str(self.total_exposure),
            "exposure_pct": self.exposure_pct,
            "daily_pnl": str(self.daily_pnl),
            "daily_pnl_pct": self.daily_pnl_pct,
            "peak_equity": str(self.peak_equity),
            "current_drawdown_pct": self.current_drawdown_pct,
            "kill_switch_active": self.kill_switch_active,
            "manual_halt": self.manual_halt,
            "as_of": self.as_of.isoformat(),
        }


@dataclass(frozen=True)
class RiskResult:
    """
    Result of risk overlay evaluation.

    Contains:
    - Decision (approved/vetoed)
    - Veto reasons if rejected
    - Position size if approved
    - Risk metrics

    Immutable to ensure determinism.
    """

    schema_version: str = RISK_SCHEMA_VERSION

    # The gate result being evaluated
    gate_result: GateResult | None = None

    # Decision
    decision: RiskDecision = RiskDecision.VETOED

    # Reasons (for veto or notes)
    veto_reasons: tuple[VetoReason, ...] = field(default_factory=tuple)
    reason_details: str = ""

    # Position size (if approved)
    position_size: PositionSize | None = None

    # Risk overlay configuration used
    overlay_name: str = ""
    overlay_version: str = ""

    # Account state at evaluation
    account_snapshot: AccountState | None = None

    # Timestamp
    evaluated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_event_payload(self) -> dict[str, Any]:
        """Convert to event payload dict."""
        return {
            "schema_version": self.schema_version,
            "gate_result": self.gate_result.to_event_payload() if self.gate_result else None,
            "decision": self.decision.value,
            "veto_reasons": [r.value for r in self.veto_reasons],
            "reason_details": self.reason_details,
            "position_size": self.position_size.to_event_payload() if self.position_size else None,
            "overlay_name": self.overlay_name,
            "overlay_version": self.overlay_version,
            "account_snapshot": self.account_snapshot.to_dict() if self.account_snapshot else None,
            "evaluated_at": self.evaluated_at.isoformat(),
        }

    def to_event(self) -> Event:
        """Create appropriate risk event based on decision."""
        if self.decision == RiskDecision.APPROVED:
            event_type = EventType.RISK_APPROVED
        else:
            event_type = EventType.RISK_VETO

        symbol = ""
        if self.gate_result and self.gate_result.scored_signal:
            symbol = self.gate_result.scored_signal.symbol

        return create_event(
            event_type,
            payload=self.to_event_payload(),
            symbol=symbol,
            timestamp=self.evaluated_at,
        )

    @property
    def is_approved(self) -> bool:
        """Check if risk approved the trade."""
        return self.decision == RiskDecision.APPROVED

    @property
    def is_vetoed(self) -> bool:
        """Check if risk vetoed the trade."""
        return self.decision == RiskDecision.VETOED


class PositionSizer(ABC):
    """
    Abstract base class for position sizers.

    Position sizers:
    - Compute appropriate position sizes
    - Consider risk parameters (stop loss, account risk)
    - Are deterministic (same input -> same size)
    - Do NOT place orders
    - Do NOT modify state

    Each sizer must implement:
    - name: Unique identifier
    - version: Sizer version string
    - compute_size: Calculate position size
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique name identifying this sizer."""
        pass

    @property
    @abstractmethod
    def version(self) -> str:
        """Version string for this sizer."""
        pass

    @property
    def description(self) -> str:
        """Human-readable description of the sizer."""
        return ""

    @abstractmethod
    def compute_size(
        self,
        gate_result: GateResult,
        account: AccountState,
        entry_price: Decimal,
        stop_price: Decimal | None = None,
    ) -> PositionSize:
        """
        Compute position size for a trade.

        Args:
            gate_result: Approved GateResult
            account: Current AccountState
            entry_price: Expected entry price
            stop_price: Optional stop loss price

        Returns:
            PositionSize with computed sizing

        This method MUST:
        - Be deterministic
        - Not have side effects
        - Not access external state
        - Return a valid PositionSize
        """
        pass

    def create_position_size(
        self,
        shares: int,
        entry_price: Decimal,
        account: AccountState,
        stop_price: Decimal | None = None,
        rationale: str = "",
    ) -> PositionSize:
        """
        Helper to create a PositionSize with common fields populated.

        Args:
            shares: Number of shares
            entry_price: Entry price
            account: AccountState for relative metrics
            stop_price: Optional stop price
            rationale: Explanation for sizing

        Returns:
            PositionSize with all fields populated
        """
        notional = Decimal(shares) * entry_price
        risk_per_share = abs(entry_price - stop_price) if stop_price else Decimal("0")

        position_pct = float(notional / account.total_equity) if account.total_equity > 0 else 0.0
        risk_pct = (
            float(Decimal(shares) * risk_per_share / account.total_equity)
            if account.total_equity > 0 and risk_per_share > 0
            else 0.0
        )

        return PositionSize(
            schema_version=RISK_SCHEMA_VERSION,
            shares=shares,
            notional_value=notional,
            entry_price=entry_price,
            stop_price=stop_price,
            risk_per_share=risk_per_share,
            position_pct_of_account=position_pct,
            risk_pct_of_account=risk_pct,
            sizer_name=self.name,
            sizer_version=self.version,
            sizing_rationale=rationale,
            computed_at=datetime.now(timezone.utc),
        )


class RiskOverlay(ABC):
    """
    Abstract base class for risk overlays.

    Risk overlays:
    - Evaluate account-level risk constraints
    - Approve or veto trades based on risk rules
    - Are deterministic (same input -> same decision)
    - Do NOT place orders
    - Do NOT modify FSM state
    - May veto, never force

    Each overlay must implement:
    - name: Unique identifier
    - version: Overlay version string
    - evaluate: Decide on a sized position
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique name identifying this overlay."""
        pass

    @property
    @abstractmethod
    def version(self) -> str:
        """Version string for this overlay."""
        pass

    @property
    def description(self) -> str:
        """Human-readable description of the overlay."""
        return ""

    @abstractmethod
    def evaluate(
        self,
        gate_result: GateResult,
        position_size: PositionSize,
        account: AccountState,
    ) -> RiskResult:
        """
        Evaluate a sized position against risk constraints.

        Args:
            gate_result: Approved GateResult
            position_size: Computed PositionSize
            account: Current AccountState

        Returns:
            RiskResult with decision and reasons

        This method MUST:
        - Be deterministic
        - Not have side effects
        - Not access external state
        - Return a valid RiskResult
        """
        pass

    def create_approved(
        self,
        gate_result: GateResult,
        position_size: PositionSize,
        account: AccountState,
        details: str = "",
    ) -> RiskResult:
        """Helper to create an approved RiskResult."""
        return RiskResult(
            schema_version=RISK_SCHEMA_VERSION,
            gate_result=gate_result,
            decision=RiskDecision.APPROVED,
            veto_reasons=(),
            reason_details=details,
            position_size=position_size,
            overlay_name=self.name,
            overlay_version=self.version,
            account_snapshot=account,
            evaluated_at=datetime.now(timezone.utc),
        )

    def create_vetoed(
        self,
        gate_result: GateResult,
        position_size: PositionSize | None,
        account: AccountState,
        reasons: tuple[VetoReason, ...],
        details: str = "",
    ) -> RiskResult:
        """Helper to create a vetoed RiskResult."""
        return RiskResult(
            schema_version=RISK_SCHEMA_VERSION,
            gate_result=gate_result,
            decision=RiskDecision.VETOED,
            veto_reasons=reasons,
            reason_details=details,
            position_size=position_size,
            overlay_name=self.name,
            overlay_version=self.version,
            account_snapshot=account,
            evaluated_at=datetime.now(timezone.utc),
        )
