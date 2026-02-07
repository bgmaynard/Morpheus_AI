"""
Validation Mode Configuration for Microstructure Gates.

PURPOSE: Prove behavioral correctness under live conditions.
NOT about performance - about correctness.

When VALIDATION_MODE is enabled:
- Allow real executions with strict limits
- Disable aggressive sizing
- Favor observability over profit
- Log everything, optimize nothing
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import time
from enum import Enum
from typing import Callable, Any

logger = logging.getLogger(__name__)


class ValidationStatus(str, Enum):
    """Overall validation status for the session."""
    PASSED = "PASSED"
    FAILED = "FAILED"
    PENDING = "PENDING"


@dataclass
class ValidationModeConfig:
    """
    Hard safety constraints for validation phase.

    These are NON-NEGOTIABLE during validation.
    """

    # Master switch
    enabled: bool = True

    # ─── Position Limits ───────────────────────────────────────────────
    max_concurrent_positions: int = 1  # Only 1 open position at a time
    max_notional_exposure: float = 500.0  # Small fixed $ exposure
    allow_pyramiding: bool = False  # No adding to positions

    # ─── Strategy Limits ───────────────────────────────────────────────
    # RTH allowed strategies
    allowed_strategies: list[str] = field(default_factory=lambda: [
        "catalyst_momentum",
        "first_pullback",
    ])
    # Premarket allowed strategies (phase-aware)
    premarket_allowed_strategies: list[str] = field(default_factory=lambda: [
        "premarket_breakout",
        "catalyst_momentum",
        "first_pullback",
    ])
    # Explicitly disabled
    disabled_strategies: list[str] = field(default_factory=lambda: [
        "gap_fade",  # No shorts during validation
    ])

    # ─── Time Limits ───────────────────────────────────────────────────
    # RTH trading window (ET)
    trading_start: time = field(default_factory=lambda: time(9, 30))
    trading_end: time = field(default_factory=lambda: time(11, 0))
    # Premarket trading window (ET)
    premarket_start: time = field(default_factory=lambda: time(7, 0))
    premarket_end: time = field(default_factory=lambda: time(9, 30))

    # Phase-scoped trade caps (validation budget per phase)
    max_trades_per_phase: dict[str, int] = field(default_factory=lambda: {
        "PREMARKET": 3,
        "RTH": 5,
        "AFTER_HOURS": 0,
    })

    # ─── Direction Limits ──────────────────────────────────────────────
    allow_shorts: bool = False  # Disable all shorts during validation


@dataclass
class ValidationCounters:
    """Track validation events throughout the day."""

    # Trade counts (phase-scoped)
    trades_executed: int = 0  # Total across all phases
    trades_by_phase: dict[str, int] = field(default_factory=lambda: {
        "PREMARKET": 0,
        "RTH": 0,
        "AFTER_HOURS": 0,
    })
    trades_blocked_phase_cap: dict[str, int] = field(default_factory=lambda: {
        "PREMARKET": 0,
        "RTH": 0,
        "AFTER_HOURS": 0,
    })
    trades_blocked_data_stale: int = 0
    trades_blocked_halt: int = 0
    trades_blocked_htb: int = 0
    trades_blocked_ssr: int = 0
    trades_blocked_time_window: int = 0
    trades_blocked_position_limit: int = 0
    trades_blocked_strategy_disabled: int = 0

    # Exit tracking
    ssr_exits: int = 0
    ssr_total_hold_time_seconds: float = 0.0
    htb_exits: int = 0
    htb_total_profit_at_exit: float = 0.0

    # Violations (any unexpected behavior)
    violations: int = 0
    violation_details: list[str] = field(default_factory=list)

    # Validation checks passed
    data_fresh_checks_passed: int = 0
    halt_block_checks_passed: int = 0
    htb_behavior_checks_passed: int = 0
    ssr_exit_checks_passed: int = 0

    def add_violation(self, detail: str) -> None:
        """Record a validation violation."""
        self.violations += 1
        self.violation_details.append(detail)
        logger.error(f"[VALIDATION] VIOLATION: {detail}")

    def get_status(self) -> ValidationStatus:
        """Determine overall validation status."""
        if self.trades_executed == 0:
            return ValidationStatus.PENDING
        if self.violations > 0:
            return ValidationStatus.FAILED
        return ValidationStatus.PASSED

    def get_ssr_avg_hold_time(self) -> float:
        """Average hold time for SSR exits."""
        if self.ssr_exits == 0:
            return 0.0
        return self.ssr_total_hold_time_seconds / self.ssr_exits

    def get_htb_avg_profit(self) -> float:
        """Average profit at exit for HTB trades."""
        if self.htb_exits == 0:
            return 0.0
        return self.htb_total_profit_at_exit / self.htb_exits

    def to_summary_dict(self) -> dict:
        """Generate validation summary for EOD report."""
        return {
            "validation_status": self.get_status().value,
            "trades_executed": self.trades_executed,
            "trades_by_phase": dict(self.trades_by_phase),
            "trades_blocked_phase_cap": dict(self.trades_blocked_phase_cap),
            "blocks": {
                "data_stale": self.trades_blocked_data_stale,
                "halt": self.trades_blocked_halt,
                "htb": self.trades_blocked_htb,
                "ssr": self.trades_blocked_ssr,
                "time_window": self.trades_blocked_time_window,
                "position_limit": self.trades_blocked_position_limit,
                "strategy_disabled": self.trades_blocked_strategy_disabled,
            },
            "ssr_exits": {
                "count": self.ssr_exits,
                "avg_hold_time_seconds": round(self.get_ssr_avg_hold_time(), 1),
            },
            "htb_exits": {
                "count": self.htb_exits,
                "avg_profit_at_exit": round(self.get_htb_avg_profit(), 4),
            },
            "checks_passed": {
                "data_fresh": self.data_fresh_checks_passed,
                "halt_block": self.halt_block_checks_passed,
                "htb_behavior": self.htb_behavior_checks_passed,
                "ssr_exit": self.ssr_exit_checks_passed,
            },
            "violations": {
                "count": self.violations,
                "details": self.violation_details[-10:],  # Last 10 violations
            },
        }

    def reset(self) -> None:
        """Reset counters for new day."""
        self.trades_executed = 0
        self.trades_by_phase = {"PREMARKET": 0, "RTH": 0, "AFTER_HOURS": 0}
        self.trades_blocked_phase_cap = {"PREMARKET": 0, "RTH": 0, "AFTER_HOURS": 0}
        self.trades_blocked_data_stale = 0
        self.trades_blocked_halt = 0
        self.trades_blocked_htb = 0
        self.trades_blocked_ssr = 0
        self.trades_blocked_time_window = 0
        self.trades_blocked_position_limit = 0
        self.trades_blocked_strategy_disabled = 0
        self.ssr_exits = 0
        self.ssr_total_hold_time_seconds = 0.0
        self.htb_exits = 0
        self.htb_total_profit_at_exit = 0.0
        self.violations = 0
        self.violation_details = []
        self.data_fresh_checks_passed = 0
        self.halt_block_checks_passed = 0
        self.htb_behavior_checks_passed = 0
        self.ssr_exit_checks_passed = 0


class ValidationLogger:
    """
    Assertive logging for validation checks.

    Logs VALIDATION_OK_* or VALIDATION_FAIL_* for each check.
    Does NOT raise exceptions - just logs and tracks.
    """

    def __init__(self, counters: ValidationCounters):
        self.counters = counters

    def log_data_fresh(self, symbol: str, data_stale: bool) -> bool:
        """Log data freshness validation."""
        if data_stale:
            logger.warning(
                f"[VALIDATION] BLOCKED_DATA_STALE symbol={symbol} | "
                f"Trade correctly blocked due to stale data"
            )
            self.counters.trades_blocked_data_stale += 1
            self.counters.data_fresh_checks_passed += 1
            return False
        else:
            logger.debug(f"[VALIDATION] VALIDATION_OK_DATA_FRESH symbol={symbol}")
            self.counters.data_fresh_checks_passed += 1
            return True

    def log_halt_block(self, symbol: str, halted: bool, halt_code: str | None = None) -> bool:
        """Log halt blocking validation."""
        if halted:
            logger.warning(
                f"[VALIDATION] VALIDATION_OK_HALT_BLOCK symbol={symbol} "
                f"halt_code={halt_code} | Entry correctly blocked"
            )
            self.counters.trades_blocked_halt += 1
            self.counters.halt_block_checks_passed += 1
            return False
        else:
            logger.debug(f"[VALIDATION] VALIDATION_OK_NOT_HALTED symbol={symbol}")
            self.counters.halt_block_checks_passed += 1
            return True

    def log_htb_behavior(
        self,
        symbol: str,
        direction: str,
        htb: bool,
        blocked: bool = False,
        size_reduced: bool = False,
    ) -> None:
        """Log HTB behavior validation."""
        if htb and direction == "short" and blocked:
            logger.warning(
                f"[VALIDATION] VALIDATION_OK_HTB_BEHAVIOR symbol={symbol} "
                f"direction=short | Short correctly blocked on HTB"
            )
            self.counters.trades_blocked_htb += 1
            self.counters.htb_behavior_checks_passed += 1
        elif htb and direction == "long" and size_reduced:
            logger.info(
                f"[VALIDATION] VALIDATION_OK_HTB_BEHAVIOR symbol={symbol} "
                f"direction=long | Size correctly reduced on HTB"
            )
            self.counters.htb_behavior_checks_passed += 1
        elif htb:
            logger.debug(
                f"[VALIDATION] HTB_PRESENT symbol={symbol} direction={direction}"
            )
            self.counters.htb_behavior_checks_passed += 1

    def log_ssr_exit(
        self,
        symbol: str,
        exit_reason: str,
        hold_time_seconds: float,
        ssr_active_at_entry: bool,
    ) -> None:
        """Log SSR exit validation."""
        if ssr_active_at_entry:
            # Verify SSR-specific exit reason was used
            ssr_exit_reasons = {
                "SSR_MOMENTUM_DECAY", "SSR_BID_COLLAPSE", "SSR_UPTICK_EXHAUSTION",
                "SSR_SPREAD_BLOWOUT", "SSR_JACKKNIFE", "SSR_TIME_FAILSAFE",
            }
            if exit_reason in ssr_exit_reasons:
                logger.info(
                    f"[VALIDATION] VALIDATION_OK_SSR_EXIT symbol={symbol} "
                    f"reason={exit_reason} hold_time={hold_time_seconds:.1f}s | "
                    f"SSR exit used correct microstructure logic"
                )
                self.counters.ssr_exits += 1
                self.counters.ssr_total_hold_time_seconds += hold_time_seconds
                self.counters.ssr_exit_checks_passed += 1
            else:
                # Violation: SSR trade used generic exit
                self.counters.add_violation(
                    f"SSR trade {symbol} exited with non-SSR reason: {exit_reason}"
                )
        else:
            logger.debug(
                f"[VALIDATION] NON_SSR_EXIT symbol={symbol} reason={exit_reason}"
            )

    def log_htb_exit(
        self,
        symbol: str,
        exit_reason: str,
        profit_at_exit: float,
        htb_at_entry: bool,
    ) -> None:
        """Log HTB exit validation."""
        if htb_at_entry:
            htb_exit_reasons = {"HTB_PROFIT_PROTECT", "TRAIL_STOP", "TIME_STOP"}
            if exit_reason in htb_exit_reasons or profit_at_exit > 0:
                logger.info(
                    f"[VALIDATION] VALIDATION_OK_HTB_EXIT symbol={symbol} "
                    f"reason={exit_reason} profit={profit_at_exit:.4f} | "
                    f"HTB exit shows profit-protect behavior"
                )
                self.counters.htb_exits += 1
                self.counters.htb_total_profit_at_exit += profit_at_exit
            else:
                # Not necessarily a violation - just tracking
                logger.warning(
                    f"[VALIDATION] HTB_EXIT_NO_PROFIT symbol={symbol} "
                    f"reason={exit_reason} profit={profit_at_exit:.4f}"
                )

    def log_exit_verification(
        self,
        symbol: str,
        exit_reason: str,
        ssr_active_at_entry: bool,
        htb_at_entry: bool,
        halt_related: bool,
        hold_time_seconds: float,
        profit: float,
    ) -> dict:
        """
        Log full exit verification record.

        Returns the verification dict for EOD logging.
        """
        verification = {
            "validation": True,
            "symbol": symbol,
            "exit_reason": exit_reason,
            "ssr_active_at_entry": ssr_active_at_entry,
            "htb_at_entry": htb_at_entry,
            "halt_related": halt_related,
            "hold_time_seconds": round(hold_time_seconds, 1),
            "profit": round(profit, 4),
        }

        logger.info(f"[VALIDATION] EXIT_VERIFICATION: {verification}")

        # Validate SSR exit
        if ssr_active_at_entry:
            self.log_ssr_exit(symbol, exit_reason, hold_time_seconds, ssr_active_at_entry)

        # Validate HTB exit
        if htb_at_entry:
            self.log_htb_exit(symbol, exit_reason, profit, htb_at_entry)

        return verification

    def log_trade_blocked_time_window(self, symbol: str, current_time: str) -> None:
        """Log trade blocked due to time window."""
        logger.warning(
            f"[VALIDATION] BLOCKED_TIME_WINDOW symbol={symbol} "
            f"current_time={current_time} | Outside validation trading hours"
        )
        self.counters.trades_blocked_time_window += 1

    def log_trade_blocked_position_limit(self, symbol: str, current_positions: int) -> None:
        """Log trade blocked due to position limit."""
        logger.warning(
            f"[VALIDATION] BLOCKED_POSITION_LIMIT symbol={symbol} "
            f"current_positions={current_positions} | Max concurrent positions reached"
        )
        self.counters.trades_blocked_position_limit += 1

    def log_trade_blocked_strategy(self, symbol: str, strategy: str) -> None:
        """Log trade blocked due to strategy not allowed."""
        logger.warning(
            f"[VALIDATION] BLOCKED_STRATEGY_DISABLED symbol={symbol} "
            f"strategy={strategy} | Strategy not allowed during validation"
        )
        self.counters.trades_blocked_strategy_disabled += 1

    def log_trade_executed(self, symbol: str, strategy: str, shares: int, price: float, phase: str = "RTH") -> None:
        """Log successful trade execution with phase tracking."""
        self.counters.trades_executed += 1
        self.counters.trades_by_phase[phase] = self.counters.trades_by_phase.get(phase, 0) + 1
        logger.info(
            f"[VALIDATION] TRADE_EXECUTED symbol={symbol} strategy={strategy} "
            f"shares={shares} price=${price:.4f} phase={phase} | "
            f"Trade #{self.counters.trades_by_phase[phase]} of {phase}"
        )


# ─── Global Validation State ──────────────────────────────────────────────

_validation_config: ValidationModeConfig | None = None
_validation_counters: ValidationCounters | None = None
_validation_logger: ValidationLogger | None = None


def init_validation_mode(config: ValidationModeConfig | None = None) -> None:
    """Initialize validation mode with config."""
    global _validation_config, _validation_counters, _validation_logger

    _validation_config = config or ValidationModeConfig()
    _validation_counters = ValidationCounters()
    _validation_logger = ValidationLogger(_validation_counters)

    if _validation_config.enabled:
        logger.info(
            f"[VALIDATION] MODE ENABLED | "
            f"max_positions={_validation_config.max_concurrent_positions} | "
            f"max_notional=${_validation_config.max_notional_exposure} | "
            f"phase_caps={_validation_config.max_trades_per_phase} | "
            f"premarket_window={_validation_config.premarket_start}-{_validation_config.premarket_end} ET | "
            f"rth_window={_validation_config.trading_start}-{_validation_config.trading_end} ET | "
            f"rth_strategies={_validation_config.allowed_strategies} | "
            f"premarket_strategies={_validation_config.premarket_allowed_strategies}"
        )
    else:
        logger.info("[VALIDATION] MODE DISABLED - normal operation")


def get_validation_config() -> ValidationModeConfig | None:
    """Get current validation config."""
    return _validation_config


def get_validation_counters() -> ValidationCounters | None:
    """Get current validation counters."""
    return _validation_counters


def get_validation_logger() -> ValidationLogger | None:
    """Get validation logger."""
    return _validation_logger


def is_validation_mode() -> bool:
    """Check if validation mode is enabled."""
    return _validation_config is not None and _validation_config.enabled


def reset_validation_counters() -> None:
    """Reset validation counters for new day."""
    if _validation_counters:
        _validation_counters.reset()
        logger.info("[VALIDATION] Counters reset for new session")


def get_validation_summary() -> dict:
    """Get validation summary for EOD report."""
    if _validation_counters:
        return _validation_counters.to_summary_dict()
    return {"validation_status": "DISABLED"}
