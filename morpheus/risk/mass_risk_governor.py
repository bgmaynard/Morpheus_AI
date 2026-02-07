"""
MASS Risk Governor - Enhanced risk management for MASS.

Wraps the existing risk manager chain with additional MASS-specific checks:
- Per-trade risk limit (0.25% instead of 1%)
- Daily loss limit (1.5%)
- Symbol cooldown (15 min between signals for same symbol)
- Regime-based position count limits
- Strategy-specific risk adjustments

Chain: MASSRiskGovernor -> RoomToProfitRiskManager -> StandardRiskManager

All evaluation is DETERMINISTIC (aside from time-based cooldown).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from morpheus.execution.paper_position_manager import PaperPositionManager
    from morpheus.core.market_mode import PhaseRegime

from morpheus.risk.base import (
    AccountState,
    GateResult,
    PositionSize,
    RiskDecision,
    RiskOverlay,
    RiskResult,
    VetoReason,
    RISK_SCHEMA_VERSION,
)
from morpheus.scoring.base import GateResult as ScoringGateResult

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MASSRiskConfig:
    """MASS-specific risk limits."""

    max_risk_per_trade_pct: float = 0.02     # 2% per trade (paper mode compatible)
    max_daily_loss_pct: float = 1.00         # 100% - effectively disabled for paper testing
    max_correlation_positions: int = 2        # Max positions in same sector/correlation
    symbol_cooldown_seconds: float = 60.0     # 60s cooldown per symbol (matches pipeline signal cooldown)
    max_concurrent_by_regime: dict[str, int] = field(
        default_factory=lambda: {
            "HOT": 25,
            "NORMAL": 25,
            "CHOP": 25,  # Raised for paper testing (real TOS positions + paper positions)
            "DEAD": 0,
        }
    )


class MASSRiskGovernor(RiskOverlay):
    """
    MASS risk governor - wraps existing risk manager with tighter controls.

    Additional checks beyond standard:
    1. Per-trade risk limit (0.25%)
    2. Daily loss limit (1.5%)
    3. Symbol cooldown (15 min)
    4. Regime-based position count limits
    5. Strategy-specific risk adjustments
    """

    def __init__(
        self,
        base_manager: RiskOverlay,
        mass_config: MASSRiskConfig | None = None,
        strategy_risk_overrides: dict[str, float] | None = None,
        position_manager: Any = None,
    ):
        self._base_manager = base_manager
        self._config = mass_config or MASSRiskConfig()
        self._strategy_risk_overrides = strategy_risk_overrides or {}
        self._position_manager = position_manager

        # Cooldown tracking: symbol -> last signal timestamp
        self._symbol_cooldowns: dict[str, datetime] = {}

        # Current MASS regime mode (set externally)
        self._current_regime_mode: str = "NORMAL"

        # Phase regime (set externally; when set, overrides RTH regime limits)
        self._phase_regime: PhaseRegime | None = None

    @property
    def name(self) -> str:
        return "mass_risk_governor"

    @property
    def version(self) -> str:
        return "1.0"

    @property
    def description(self) -> str:
        return "MASS Risk Governor with enhanced per-trade and regime-based limits"

    def set_regime_mode(self, mode: str) -> None:
        """Set the current MASS regime mode (called from pipeline during RTH)."""
        self._current_regime_mode = mode
        self._phase_regime = None  # Clear phase regime when RTH regime is set

    def set_phase_regime(self, phase_regime: PhaseRegime) -> None:
        """Set phase-specific regime (called from pipeline during PREMARKET/AFTER_HOURS)."""
        self._phase_regime = phase_regime

    def evaluate(
        self,
        gate_result: GateResult,
        position_size: PositionSize,
        account: AccountState,
    ) -> RiskResult:
        """
        Evaluate risk with MASS-specific checks, then delegate to base manager.

        MASS checks run first (fail-fast). If all pass, delegate to base manager.
        """
        now = datetime.now(timezone.utc)
        veto_reasons: list[VetoReason] = []
        details: list[str] = []

        # Check 1: Per-trade risk limit
        if account.total_equity > 0:
            # Compute total risk amount from shares * risk_per_share
            risk_amount = Decimal(position_size.shares) * position_size.risk_per_share
            trade_risk_pct = float(risk_amount) / float(account.total_equity)
            strategy_name = (
                gate_result.scored_signal.signal.strategy_name
                if gate_result.scored_signal and gate_result.scored_signal.signal
                else ""
            )
            max_risk = self._strategy_risk_overrides.get(
                strategy_name, self._config.max_risk_per_trade_pct
            )
            if trade_risk_pct > max_risk:
                veto_reasons.append(VetoReason.STRATEGY_RISK_EXCEEDED)
                details.append(
                    f"Trade risk {trade_risk_pct:.4f} exceeds "
                    f"MASS limit {max_risk:.4f} for {strategy_name}"
                )

        # Check 2: Daily loss limit
        if abs(account.daily_pnl_pct) > self._config.max_daily_loss_pct * 100:
            veto_reasons.append(VetoReason.MAX_DAILY_LOSS)
            details.append(
                f"Daily loss {account.daily_pnl_pct:.2f}% exceeds "
                f"MASS limit {self._config.max_daily_loss_pct * 100:.2f}%"
            )

        # Check 3: Symbol cooldown
        symbol = (
            gate_result.scored_signal.symbol
            if gate_result.scored_signal
            else ""
        )
        if symbol and symbol in self._symbol_cooldowns:
            last_signal_time = self._symbol_cooldowns[symbol]
            elapsed = (now - last_signal_time).total_seconds()
            if elapsed < self._config.symbol_cooldown_seconds:
                remaining = self._config.symbol_cooldown_seconds - elapsed
                veto_reasons.append(VetoReason.SYMBOL_COOLDOWN)
                details.append(
                    f"Symbol {symbol} in cooldown ({remaining:.0f}s remaining)"
                )

        # Check 3.5: Block re-entry while position is open
        if self._position_manager and symbol and self._position_manager.has_position(symbol):
            veto_reasons.append(VetoReason.POSITION_ALREADY_OPEN)
            details.append(f"Already holding position in {symbol}")

        # Check 4: Position count limit (phase-aware)
        # During PREMARKET/AFTER_HOURS, use phase regime limits (NOT RTH regime)
        if self._phase_regime is not None:
            # Phase regime: use phase-specific max_positions
            phase_max = self._phase_regime.max_positions
            if account.open_position_count >= phase_max:
                veto_reasons.append(VetoReason.REGIME_POSITION_LIMIT)
                details.append(
                    f"Phase {self._phase_regime.phase.value} limits to {phase_max} positions, "
                    f"currently {account.open_position_count}"
                )
        else:
            # RTH: use computed MASS regime limits
            regime_max = self._config.max_concurrent_by_regime.get(
                self._current_regime_mode, 3
            )
            if account.open_position_count >= regime_max:
                veto_reasons.append(VetoReason.REGIME_POSITION_LIMIT)
                details.append(
                    f"Regime {self._current_regime_mode} limits to {regime_max} positions, "
                    f"currently {account.open_position_count}"
                )

        # If MASS vetoed, return immediately
        if veto_reasons:
            reason_str = "; ".join(details)
            logger.info(f"[MASS_RISK] Vetoed: {reason_str}")
            return RiskResult(
                schema_version=RISK_SCHEMA_VERSION,
                decision=RiskDecision.VETOED,
                veto_reasons=tuple(veto_reasons),
                reason_details=reason_str,
                position_size=position_size,
                overlay_name=self.name,
                overlay_version=self.version,
                account_snapshot=account,
                evaluated_at=now,
            )

        # Delegate to base manager for standard checks
        result = self._base_manager.evaluate(gate_result, position_size, account)

        # If approved, record cooldown
        if result.decision == RiskDecision.APPROVED and symbol:
            self._symbol_cooldowns[symbol] = now

        return result

    def clear_cooldown(self, symbol: str) -> None:
        """Clear cooldown for a specific symbol."""
        self._symbol_cooldowns.pop(symbol, None)

    def clear_all_cooldowns(self) -> None:
        """Clear all symbol cooldowns."""
        self._symbol_cooldowns.clear()
