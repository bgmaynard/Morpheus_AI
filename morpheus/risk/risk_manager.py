"""
Risk Manager - Account-level risk overlay.

Evaluates trades against account-level risk constraints:
- Position count limits
- Exposure limits
- Daily loss limits
- Drawdown guards
- Kill switch status
- Room-to-profit (spread/slippage costs)

All evaluation is DETERMINISTIC: same input -> same decision.

Phase 6 Scope:
- Risk evaluation and veto only
- No order execution
- No FSM state modification
- May veto, never force
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal

from morpheus.risk.base import (
    AccountState,
    GateResult,
    PositionSize,
    RiskOverlay,
    RiskResult,
    RiskDecision,
    VetoReason,
    RISK_SCHEMA_VERSION,
)
from morpheus.risk.room_to_profit import RoomToProfitCalculator, RoomToProfitConfig

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RiskManagerConfig:
    """
    Configuration for the risk manager.

    Defines risk limits and thresholds.
    """

    # Position limits
    max_open_positions: int = 30  # Raised for paper trading with real positions
    max_position_pct: float = 0.10  # 10% max in single position

    # Exposure limits
    max_total_exposure_pct: float = 0.90  # 90% max total exposure (raised for paper testing)

    # Daily loss limits
    max_daily_loss_pct: float = 100.0  # 10000% - effectively disabled for paper testing

    # Drawdown limits
    max_drawdown_pct: float = 100.0  # 10000% - effectively disabled for paper testing

    # Buying power check
    min_buying_power_pct: float = 0.10  # Keep 10% buying power reserve


class StandardRiskManager(RiskOverlay):
    """
    Standard risk manager with configurable limits.

    Checks (in order):
    1. Kill switch / manual halt status
    2. Position count limit
    3. Daily loss limit
    4. Drawdown limit
    5. Total exposure limit
    6. Position size limit
    7. Buying power sufficiency

    All checks are deterministic.
    Vetoes on first failure (fail-fast).
    """

    def __init__(self, config: RiskManagerConfig | None = None):
        """
        Initialize the risk manager.

        Args:
            config: Optional custom configuration
        """
        self.config = config or RiskManagerConfig()

    @property
    def name(self) -> str:
        return "standard_risk_manager"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return "Standard risk overlay with configurable position and exposure limits"

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
        """
        cfg = self.config
        reasons: list[VetoReason] = []
        details_parts: list[str] = []

        # Check 1: Kill switch / manual halt
        if account.kill_switch_active:
            reasons.append(VetoReason.KILL_SWITCH_ACTIVE)
            details_parts.append("Kill switch is active")

        if account.manual_halt:
            reasons.append(VetoReason.MANUAL_HALT)
            details_parts.append("Manual trading halt in effect")

        # Fail fast on kill switch
        if reasons:
            return self.create_vetoed(
                gate_result=gate_result,
                position_size=position_size,
                account=account,
                reasons=tuple(reasons),
                details="; ".join(details_parts),
            )

        # Check 2: Position count limit
        if account.open_position_count >= cfg.max_open_positions:
            reasons.append(VetoReason.MAX_OPEN_POSITIONS)
            details_parts.append(
                f"Position count {account.open_position_count} >= {cfg.max_open_positions}"
            )

        # Check 3: Daily loss limit
        if account.daily_pnl_pct <= -cfg.max_daily_loss_pct:
            reasons.append(VetoReason.MAX_DAILY_LOSS)
            details_parts.append(
                f"Daily P&L {account.daily_pnl_pct:.2%} exceeds -{cfg.max_daily_loss_pct:.2%} limit"
            )

        # Check 4: Drawdown limit
        if account.current_drawdown_pct >= cfg.max_drawdown_pct:
            reasons.append(VetoReason.MAX_DRAWDOWN)
            details_parts.append(
                f"Drawdown {account.current_drawdown_pct:.2%} >= {cfg.max_drawdown_pct:.2%}"
            )

        # Check 5: Total exposure limit (including new position)
        new_exposure = account.total_exposure + position_size.notional_value
        new_exposure_pct = (
            float(new_exposure / account.total_equity)
            if account.total_equity > 0
            else 1.0
        )
        if new_exposure_pct > cfg.max_total_exposure_pct:
            reasons.append(VetoReason.MAX_SECTOR_EXPOSURE)  # Using for total exposure
            details_parts.append(
                f"New exposure {new_exposure_pct:.2%} would exceed {cfg.max_total_exposure_pct:.2%}"
            )

        # Check 6: Position size limit
        if position_size.position_pct_of_account > cfg.max_position_pct:
            reasons.append(VetoReason.MAX_POSITION_SIZE)
            details_parts.append(
                f"Position size {position_size.position_pct_of_account:.2%} > {cfg.max_position_pct:.2%}"
            )

        # Check 7: Buying power
        required_bp = position_size.notional_value
        available_bp = account.buying_power
        min_reserve = account.total_equity * Decimal(str(cfg.min_buying_power_pct))

        if required_bp > (available_bp - min_reserve):
            reasons.append(VetoReason.INSUFFICIENT_BUYING_POWER)
            details_parts.append(
                f"Required ${required_bp:.2f} exceeds available (${available_bp:.2f} - ${min_reserve:.2f} reserve)"
            )

        # Make decision
        if not reasons:
            return self.create_approved(
                gate_result=gate_result,
                position_size=position_size,
                account=account,
                details=f"All risk checks passed. {position_size.shares} shares @ ${position_size.entry_price:.2f}",
            )
        else:
            return self.create_vetoed(
                gate_result=gate_result,
                position_size=position_size,
                account=account,
                reasons=tuple(reasons),
                details="; ".join(details_parts),
            )


class PermissiveRiskManager(RiskOverlay):
    """
    Permissive risk manager for testing.

    Only checks:
    - Kill switch / manual halt
    - Buying power sufficiency

    Approves most trades (for paper trading / backtesting).
    """

    @property
    def name(self) -> str:
        return "permissive_risk_manager"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return "Permissive risk overlay for testing (only checks kill switch)"

    def evaluate(
        self,
        gate_result: GateResult,
        position_size: PositionSize,
        account: AccountState,
    ) -> RiskResult:
        """Approve most trades, only checking critical constraints."""
        reasons: list[VetoReason] = []
        details_parts: list[str] = []

        # Kill switch check
        if account.kill_switch_active:
            reasons.append(VetoReason.KILL_SWITCH_ACTIVE)
            details_parts.append("Kill switch active")

        if account.manual_halt:
            reasons.append(VetoReason.MANUAL_HALT)
            details_parts.append("Manual halt active")

        # Basic buying power check
        if position_size.notional_value > account.buying_power:
            reasons.append(VetoReason.INSUFFICIENT_BUYING_POWER)
            details_parts.append("Insufficient buying power")

        if reasons:
            return self.create_vetoed(
                gate_result=gate_result,
                position_size=position_size,
                account=account,
                reasons=tuple(reasons),
                details="; ".join(details_parts),
            )

        return self.create_approved(
            gate_result=gate_result,
            position_size=position_size,
            account=account,
            details=f"Permissive approval. {position_size.shares} shares",
        )


class StrictRiskManager(RiskOverlay):
    """
    Strict risk manager with tight limits.

    For production use with conservative risk limits:
    - Max 3 positions
    - Max 5% per position
    - Max 25% total exposure
    - Max 2% daily loss
    - Max 5% drawdown
    """

    def __init__(self):
        """Initialize with strict defaults."""
        self._config = RiskManagerConfig(
            max_open_positions=3,
            max_position_pct=0.05,
            max_total_exposure_pct=0.25,
            max_daily_loss_pct=0.02,
            max_drawdown_pct=0.05,
            min_buying_power_pct=0.20,
        )
        self._manager = StandardRiskManager(self._config)

    @property
    def name(self) -> str:
        return "strict_risk_manager"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return "Strict risk overlay with conservative limits"

    def evaluate(
        self,
        gate_result: GateResult,
        position_size: PositionSize,
        account: AccountState,
    ) -> RiskResult:
        """Delegate to internal manager with strict config."""
        return self._manager.evaluate(gate_result, position_size, account)


def create_standard_risk_manager(
    config: RiskManagerConfig | None = None,
) -> StandardRiskManager:
    """Factory function to create a StandardRiskManager."""
    return StandardRiskManager(config)


def create_permissive_risk_manager() -> PermissiveRiskManager:
    """Factory function to create a PermissiveRiskManager."""
    return PermissiveRiskManager()


def create_strict_risk_manager() -> StrictRiskManager:
    """Factory function to create a StrictRiskManager."""
    return StrictRiskManager()


class RoomToProfitRiskManager(RiskOverlay):
    """
    Risk manager with integrated room-to-profit checking.

    Wraps another risk manager and adds spread/slippage cost analysis.
    A trade is vetoed if execution costs would eat too much of the potential profit.

    This ensures we only take trades where there's realistic room to profit
    after accounting for:
    - Bid-ask spread
    - Expected slippage
    - Commissions (if any)

    The spread is extracted from the gate result's feature snapshot.
    """

    def __init__(
        self,
        base_manager: RiskOverlay | None = None,
        rtp_config: RoomToProfitConfig | None = None,
        default_spread_pct: float = 0.5,  # Assume 0.5% spread if not in features
    ):
        """
        Initialize with a base risk manager and room-to-profit config.

        Args:
            base_manager: Underlying risk manager (defaults to StandardRiskManager)
            rtp_config: Room-to-profit configuration
            default_spread_pct: Default spread % to use if not available in features
        """
        self._base = base_manager or StandardRiskManager()
        self._rtp = RoomToProfitCalculator(rtp_config)
        self._default_spread_pct = default_spread_pct

    @property
    def name(self) -> str:
        return "room_to_profit_risk_manager"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return "Risk manager with spread/slippage cost analysis"

    def evaluate(
        self,
        gate_result: GateResult,
        position_size: PositionSize,
        account: AccountState,
    ) -> RiskResult:
        """
        Evaluate position with room-to-profit analysis.

        First runs the base manager's checks, then if approved,
        performs room-to-profit analysis.
        """
        # Run base manager first
        base_result = self._base.evaluate(gate_result, position_size, account)

        # If base manager vetoed, return that result
        if base_result.is_vetoed:
            return base_result

        # Extract data for room-to-profit analysis
        entry_price = float(position_size.entry_price)
        stop_price = float(position_size.stop_price) if position_size.stop_price else None

        # Get target price from signal
        target_price = None
        direction = "long"

        if gate_result.scored_signal and gate_result.scored_signal.signal:
            signal = gate_result.scored_signal.signal
            target_price = signal.target_reference
            direction = signal.direction.value if signal.direction else "long"

        # If no stop or target, we can't do room-to-profit analysis
        if not stop_price or not target_price:
            logger.debug("No stop/target for room-to-profit, skipping check")
            return base_result

        # Get spread from feature snapshot
        spread_pct = self._default_spread_pct
        bid = entry_price * 0.999  # Estimate bid
        ask = entry_price * 1.001  # Estimate ask

        if gate_result.scored_signal:
            fs = gate_result.scored_signal.feature_snapshot
            if "spread_pct" in fs and fs["spread_pct"] is not None:
                spread_pct = float(fs["spread_pct"])
            if "bid" in fs and fs["bid"] is not None:
                bid = float(fs["bid"])
            if "ask" in fs and fs["ask"] is not None:
                ask = float(fs["ask"])
            elif spread_pct:
                # Reconstruct bid/ask from spread_pct
                half_spread = entry_price * (spread_pct / 100) / 2
                bid = entry_price - half_spread
                ask = entry_price + half_spread

        # Calculate room-to-profit
        rtp = self._rtp.calculate(
            symbol=gate_result.scored_signal.symbol if gate_result.scored_signal else "",
            entry_price=entry_price,
            stop_price=stop_price,
            target_price=target_price,
            bid=bid,
            ask=ask,
            direction=direction,
        )

        # Check if trade has room to profit
        if not rtp.has_room:
            logger.info(
                f"Room-to-profit veto: {rtp.symbol} - {rtp.rejection_reason} "
                f"(net R:R {rtp.net_rr_ratio:.2f}, breakeven {rtp.breakeven_pct:.2f}%)"
            )
            return self.create_vetoed(
                gate_result=gate_result,
                position_size=position_size,
                account=account,
                reasons=(VetoReason.INSUFFICIENT_ROOM_TO_PROFIT,),
                details=f"{rtp.rejection_reason}. Net R:R: {rtp.net_rr_ratio:.2f}, "
                        f"Breakeven move: {rtp.breakeven_pct:.2f}%, "
                        f"Spread: {rtp.execution_costs.spread_pct:.2f}%",
            )

        # Room-to-profit check passed
        logger.debug(
            f"Room-to-profit OK: {rtp.symbol} - net R:R {rtp.net_rr_ratio:.2f}, "
            f"score {rtp.room_score:.0f}"
        )

        # Return approved result with room-to-profit details in reason_details
        return RiskResult(
            schema_version=RISK_SCHEMA_VERSION,
            gate_result=base_result.gate_result,
            decision=RiskDecision.APPROVED,
            veto_reasons=(),
            reason_details=f"{base_result.reason_details} | Room-to-profit: "
                          f"net R:R {rtp.net_rr_ratio:.2f}, score {rtp.room_score:.0f}",
            position_size=base_result.position_size,
            overlay_name=self.name,
            overlay_version=self.version,
            account_snapshot=base_result.account_snapshot,
            evaluated_at=base_result.evaluated_at,
        )


def create_room_to_profit_manager(
    base_manager: RiskOverlay | None = None,
    rtp_config: RoomToProfitConfig | None = None,
) -> RoomToProfitRiskManager:
    """Factory function to create a RoomToProfitRiskManager."""
    return RoomToProfitRiskManager(base_manager, rtp_config)
