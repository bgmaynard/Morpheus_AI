"""
Position Sizer - Computes appropriate position sizes for trades.

Implements various position sizing methodologies:
- Fixed percent risk (risk X% of account per trade)
- Fixed fractional (position as % of account)
- Volatility-adjusted (ATR-based sizing)

All sizing is DETERMINISTIC: same input -> same size.

Phase 6 Scope:
- Position sizing only
- No order execution
- No trade placement
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum

from morpheus.risk.base import (
    AccountState,
    GateResult,
    PositionSize,
    PositionSizer,
    RISK_SCHEMA_VERSION,
)


class SizingMethod(Enum):
    """Position sizing methods."""

    FIXED_PERCENT_RISK = "fixed_percent_risk"
    FIXED_FRACTIONAL = "fixed_fractional"
    VOLATILITY_ADJUSTED = "volatility_adjusted"


@dataclass(frozen=True)
class PositionSizerConfig:
    """
    Configuration for position sizer.

    Defines sizing parameters and limits.
    """

    # Sizing method
    method: SizingMethod = SizingMethod.FIXED_PERCENT_RISK

    # Fixed percent risk parameters
    risk_per_trade_pct: float = 0.01  # 1% of account
    default_stop_pct: float = 0.02  # 2% below entry if no stop

    # Fixed fractional parameters
    position_pct: float = 0.05  # 5% of account per position

    # Volatility-adjusted parameters
    atr_multiplier: float = 2.0  # Stop at 2x ATR
    vol_risk_pct: float = 0.01  # Risk 1% with vol-adjusted stop

    # Universal limits
    max_position_pct: float = 0.10  # Never more than 10% in one position
    max_shares: int = 10000  # Hard cap on shares
    min_shares: int = 1  # Minimum position size

    # Round lot preference
    prefer_round_lots: bool = True
    round_lot_size: int = 100


class StandardPositionSizer(PositionSizer):
    """
    Standard position sizer with configurable sizing methods.

    Supports:
    - Fixed percent risk: Size based on risk amount per trade
    - Fixed fractional: Size as percentage of account
    - Volatility-adjusted: Size based on ATR-adjusted stops

    All methods respect maximum position limits.
    """

    def __init__(self, config: PositionSizerConfig | None = None):
        """
        Initialize the position sizer.

        Args:
            config: Optional custom configuration
        """
        self.config = config or PositionSizerConfig()

    @property
    def name(self) -> str:
        return "standard_position_sizer"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return f"Standard position sizer using {self.config.method.value} method"

    def compute_size(
        self,
        gate_result: GateResult,
        account: AccountState,
        entry_price: Decimal,
        stop_price: Decimal | None = None,
    ) -> PositionSize:
        """
        Compute position size based on configured method.

        Args:
            gate_result: Approved GateResult
            account: Current AccountState
            entry_price: Expected entry price
            stop_price: Optional stop loss price

        Returns:
            PositionSize with computed sizing
        """
        cfg = self.config

        # Get effective stop price
        effective_stop = self._get_effective_stop(entry_price, stop_price)

        # Calculate risk per share
        risk_per_share = abs(entry_price - effective_stop)

        # Compute raw shares based on method
        if cfg.method == SizingMethod.FIXED_PERCENT_RISK:
            shares = self._compute_fixed_risk_shares(
                account, entry_price, risk_per_share
            )
            rationale = f"Fixed {cfg.risk_per_trade_pct:.1%} risk sizing"

        elif cfg.method == SizingMethod.FIXED_FRACTIONAL:
            shares = self._compute_fixed_fractional_shares(account, entry_price)
            rationale = f"Fixed {cfg.position_pct:.1%} position sizing"

        elif cfg.method == SizingMethod.VOLATILITY_ADJUSTED:
            # Get ATR from gate result's feature snapshot
            atr_pct = self._get_atr_pct(gate_result)
            shares = self._compute_volatility_shares(
                account, entry_price, atr_pct
            )
            rationale = f"Volatility-adjusted sizing (ATR: {atr_pct:.2f}%)"

        else:
            shares = 0
            rationale = "Unknown sizing method"

        # Apply limits
        shares = self._apply_limits(shares, account, entry_price)

        # Apply round lot preference
        if cfg.prefer_round_lots and shares >= cfg.round_lot_size:
            shares = (shares // cfg.round_lot_size) * cfg.round_lot_size

        # Create position size
        return self.create_position_size(
            shares=shares,
            entry_price=entry_price,
            account=account,
            stop_price=effective_stop,
            rationale=rationale,
        )

    def _get_effective_stop(
        self,
        entry_price: Decimal,
        stop_price: Decimal | None,
    ) -> Decimal:
        """Get effective stop price, using default if not provided."""
        if stop_price is not None:
            return stop_price

        # Use default stop percentage
        stop_distance = entry_price * Decimal(str(self.config.default_stop_pct))
        return entry_price - stop_distance

    def _compute_fixed_risk_shares(
        self,
        account: AccountState,
        entry_price: Decimal,
        risk_per_share: Decimal,
    ) -> int:
        """Compute shares using fixed percent risk method."""
        if risk_per_share <= 0:
            return 0

        # Risk amount in dollars
        risk_amount = account.total_equity * Decimal(str(self.config.risk_per_trade_pct))

        # Shares = risk amount / risk per share
        shares = int(risk_amount / risk_per_share)
        return max(0, shares)

    def _compute_fixed_fractional_shares(
        self,
        account: AccountState,
        entry_price: Decimal,
    ) -> int:
        """Compute shares using fixed fractional method."""
        if entry_price <= 0:
            return 0

        # Position value in dollars
        position_value = account.total_equity * Decimal(str(self.config.position_pct))

        # Shares = position value / entry price
        shares = int(position_value / entry_price)
        return max(0, shares)

    def _compute_volatility_shares(
        self,
        account: AccountState,
        entry_price: Decimal,
        atr_pct: float,
    ) -> int:
        """Compute shares using volatility-adjusted method."""
        if atr_pct <= 0 or entry_price <= 0:
            return 0

        # ATR-based stop distance
        atr_dollars = entry_price * Decimal(str(atr_pct / 100))
        stop_distance = atr_dollars * Decimal(str(self.config.atr_multiplier))

        if stop_distance <= 0:
            return 0

        # Risk amount in dollars
        risk_amount = account.total_equity * Decimal(str(self.config.vol_risk_pct))

        # Shares = risk amount / stop distance
        shares = int(risk_amount / stop_distance)
        return max(0, shares)

    def _get_atr_pct(self, gate_result: GateResult) -> float:
        """Extract ATR percentage from gate result's feature snapshot."""
        if not gate_result.scored_signal:
            return 2.0  # Default 2%

        atr_pct = gate_result.scored_signal.feature_snapshot.get("atr_pct")
        if atr_pct is None:
            return 2.0  # Default 2%

        return float(atr_pct)

    def _apply_limits(
        self,
        shares: int,
        account: AccountState,
        entry_price: Decimal,
    ) -> int:
        """Apply position limits to computed shares."""
        cfg = self.config

        # Ensure minimum
        if shares < cfg.min_shares:
            shares = cfg.min_shares

        # Apply hard cap
        shares = min(shares, cfg.max_shares)

        # Apply max position percentage
        if account.total_equity > 0:
            max_notional = account.total_equity * Decimal(str(cfg.max_position_pct))
            max_shares_by_pct = int(max_notional / entry_price) if entry_price > 0 else 0
            shares = min(shares, max_shares_by_pct)

        # Don't exceed buying power
        if account.buying_power > 0 and entry_price > 0:
            max_shares_by_bp = int(account.buying_power / entry_price)
            shares = min(shares, max_shares_by_bp)

        return max(0, shares)


class ConservativePositionSizer(PositionSizer):
    """
    Conservative position sizer with tight limits.

    Designed for:
    - New strategies being tested
    - High volatility periods
    - Accounts with strict risk budgets

    Uses 0.5% risk per trade and 5% max position.
    """

    def __init__(self):
        """Initialize with conservative defaults."""
        self._config = PositionSizerConfig(
            method=SizingMethod.FIXED_PERCENT_RISK,
            risk_per_trade_pct=0.005,  # 0.5%
            max_position_pct=0.05,  # 5%
            max_shares=5000,
        )
        self._sizer = StandardPositionSizer(self._config)

    @property
    def name(self) -> str:
        return "conservative_position_sizer"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return "Conservative sizer with 0.5% risk and 5% max position"

    def compute_size(
        self,
        gate_result: GateResult,
        account: AccountState,
        entry_price: Decimal,
        stop_price: Decimal | None = None,
    ) -> PositionSize:
        """Delegate to internal sizer."""
        return self._sizer.compute_size(gate_result, account, entry_price, stop_price)


class AggressivePositionSizer(PositionSizer):
    """
    Aggressive position sizer with higher limits.

    Designed for:
    - High-conviction signals
    - Favorable market conditions
    - Accounts with higher risk tolerance

    Uses 2% risk per trade and 15% max position.
    """

    def __init__(self):
        """Initialize with aggressive defaults."""
        self._config = PositionSizerConfig(
            method=SizingMethod.FIXED_PERCENT_RISK,
            risk_per_trade_pct=0.02,  # 2%
            max_position_pct=0.15,  # 15%
            max_shares=10000,
        )
        self._sizer = StandardPositionSizer(self._config)

    @property
    def name(self) -> str:
        return "aggressive_position_sizer"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return "Aggressive sizer with 2% risk and 15% max position"

    def compute_size(
        self,
        gate_result: GateResult,
        account: AccountState,
        entry_price: Decimal,
        stop_price: Decimal | None = None,
    ) -> PositionSize:
        """Delegate to internal sizer."""
        return self._sizer.compute_size(gate_result, account, entry_price, stop_price)


def create_standard_sizer(config: PositionSizerConfig | None = None) -> StandardPositionSizer:
    """Factory function to create a StandardPositionSizer."""
    return StandardPositionSizer(config)


def create_conservative_sizer() -> ConservativePositionSizer:
    """Factory function to create a ConservativePositionSizer."""
    return ConservativePositionSizer()


def create_aggressive_sizer() -> AggressivePositionSizer:
    """Factory function to create an AggressivePositionSizer."""
    return AggressivePositionSizer()
