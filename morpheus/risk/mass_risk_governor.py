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
from typing import Any

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

    max_risk_per_trade_pct: float = 0.0025   # 0.25% per trade
    max_daily_loss_pct: float = 0.015        # 1.5% daily max
    max_correlation_positions: int = 2        # Max positions in same sector/correlation
    symbol_cooldown_seconds: float = 900.0    # 15-min cooldown per symbol
    max_concurrent_by_regime: dict[str, int] = field(
        default_factory=lambda: {
            "HOT": 5,
            "NORMAL": 3,
            "CHOP": 2,
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
    ):
        self._base_manager = base_manager
        self._config = mass_config or MASSRiskConfig()
        self._strategy_risk_overrides = strategy_risk_overrides or {}

        # Cooldown tracking: symbol -> last signal timestamp
        self._symbol_cooldowns: dict[str, datetime] = {}

        # Current MASS regime mode (set externally)
        self._current_regime_mode: str = "NORMAL"

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
        """Set the current MASS regime mode (called from pipeline)."""
        self._current_regime_mode = mode

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
            trade_risk_pct = float(position_size.risk_amount) / float(account.total_equity)
            strategy_name = gate_result.signal.strategy_name if hasattr(gate_result, "signal") else ""
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
        symbol = gate_result.signal.symbol if hasattr(gate_result, "signal") else ""
        if symbol and symbol in self._symbol_cooldowns:
            last_signal_time = self._symbol_cooldowns[symbol]
            elapsed = (now - last_signal_time).total_seconds()
            if elapsed < self._config.symbol_cooldown_seconds:
                remaining = self._config.symbol_cooldown_seconds - elapsed
                veto_reasons.append(VetoReason.SYMBOL_COOLDOWN)
                details.append(
                    f"Symbol {symbol} in cooldown ({remaining:.0f}s remaining)"
                )

        # Check 4: Regime-based position count limit
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
