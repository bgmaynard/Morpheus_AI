"""
Worklist Scrutiny - Filter and validate symbols before worklist entry.

Scrutiny happens BEFORE symbols enter the worklist.
Rejected symbols emit WORKLIST_REJECTED events but are not stored.

Flow:
    MAX_AI scanner row → Scrutiny Filter → Pass/Reject → Worklist
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Awaitable, Optional

logger = logging.getLogger(__name__)


class RejectionReason(Enum):
    """Reasons a symbol can be rejected from worklist."""
    PRICE_TOO_LOW = "price_too_low"
    PRICE_TOO_HIGH = "price_too_high"
    VOLUME_TOO_LOW = "volume_too_low"
    RVOL_TOO_LOW = "rvol_too_low"
    SPREAD_TOO_WIDE = "spread_too_wide"
    LIQUIDITY_FAIL = "liquidity_fail"
    SCORE_TOO_LOW = "score_too_low"
    HALTED = "halted"
    STALE_DATA = "stale_data"
    ALREADY_TRADED = "already_traded"
    DUPLICATE = "duplicate"


@dataclass(frozen=True)
class ScrutinyConfig:
    """
    Configuration for scrutiny filters.

    Calibrated for small-cap momentum trading.
    """

    # Price filters
    min_price: float = 0.50  # Minimum price ($0.50)
    max_price: float = 20.00  # Maximum price ($20)

    # Volume filters
    min_volume: int = 100_000  # Minimum daily volume
    min_rvol: float = 1.5  # Minimum relative volume (1.5x)

    # Spread filter
    max_spread_pct: float = 3.0  # Maximum spread (3%)

    # Score filter
    min_scanner_score: float = 50.0  # Minimum MAX_AI score (0-100)

    # Liquidity sanity
    min_dollar_volume: float = 100_000  # Minimum $ volume

    # Data freshness
    max_data_age_seconds: float = 300.0  # 5 minutes max staleness


@dataclass
class ScrutinyResult:
    """Result of scrutiny check."""

    symbol: str
    passed: bool
    rejection_reason: Optional[RejectionReason] = None
    rejection_details: Optional[str] = None
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    # Input data (for logging)
    price: float = 0.0
    volume: int = 0
    rvol: float = 0.0
    scanner_score: float = 0.0

    def to_event_payload(self) -> dict[str, Any]:
        """Convert to event payload."""
        return {
            "symbol": self.symbol,
            "passed": self.passed,
            "rejection_reason": self.rejection_reason.value if self.rejection_reason else None,
            "rejection_details": self.rejection_details,
            "timestamp": self.timestamp,
            "price": self.price,
            "volume": self.volume,
            "rvol": self.rvol,
            "scanner_score": self.scanner_score,
        }


class WorklistScrutinizer:
    """
    Scrutiny engine for filtering symbols before worklist entry.

    Checks (in order):
        1. Price bounds
        2. Volume minimum
        3. RVOL minimum
        4. Scanner score minimum
        5. Dollar volume (liquidity)
        6. Spread (if available)
        7. Data freshness

    Fails fast on first rejection.
    """

    def __init__(
        self,
        config: Optional[ScrutinyConfig] = None,
        emit_event: Optional[Callable[[Any], Awaitable[None]]] = None,
    ):
        """
        Initialize scrutinizer.

        Args:
            config: Scrutiny configuration
            emit_event: Optional callback for emitting events
        """
        self._config = config or ScrutinyConfig()
        self._emit_event = emit_event

        # Counters
        self._total_checked = 0
        self._total_passed = 0
        self._rejections_by_reason: dict[str, int] = {}

        logger.info(
            f"[SCRUTINY] Initialized: "
            f"price ${self._config.min_price}-${self._config.max_price}, "
            f"vol >= {self._config.min_volume:,}, "
            f"rvol >= {self._config.min_rvol}x, "
            f"score >= {self._config.min_scanner_score}"
        )

    def check(
        self,
        symbol: str,
        price: float,
        volume: int,
        rvol: float,
        scanner_score: float,
        spread_pct: Optional[float] = None,
        is_halted: bool = False,
        data_timestamp: Optional[datetime] = None,
    ) -> ScrutinyResult:
        """
        Run scrutiny checks on a symbol.

        Args:
            symbol: Stock symbol
            price: Current price
            volume: Daily volume
            rvol: Relative volume multiplier
            scanner_score: MAX_AI scanner score (0-100)
            spread_pct: Optional spread percentage
            is_halted: Whether symbol is halted
            data_timestamp: Timestamp of the data

        Returns:
            ScrutinyResult with pass/fail status
        """
        self._total_checked += 1
        cfg = self._config

        # Create base result
        base_result = {
            "symbol": symbol,
            "price": price,
            "volume": volume,
            "rvol": rvol,
            "scanner_score": scanner_score,
        }

        # Check 1: Halted
        if is_halted:
            return self._reject(RejectionReason.HALTED, "Symbol is halted", **base_result)

        # Check 2: Price bounds
        if price < cfg.min_price:
            return self._reject(
                RejectionReason.PRICE_TOO_LOW,
                f"Price ${price:.2f} < ${cfg.min_price:.2f} min",
                **base_result,
            )

        if price > cfg.max_price:
            return self._reject(
                RejectionReason.PRICE_TOO_HIGH,
                f"Price ${price:.2f} > ${cfg.max_price:.2f} max",
                **base_result,
            )

        # Check 3: Volume minimum
        if volume < cfg.min_volume:
            return self._reject(
                RejectionReason.VOLUME_TOO_LOW,
                f"Volume {volume:,} < {cfg.min_volume:,} min",
                **base_result,
            )

        # Check 4: RVOL minimum
        if rvol < cfg.min_rvol:
            return self._reject(
                RejectionReason.RVOL_TOO_LOW,
                f"RVOL {rvol:.1f}x < {cfg.min_rvol:.1f}x min",
                **base_result,
            )

        # Check 5: Scanner score minimum
        if scanner_score < cfg.min_scanner_score:
            return self._reject(
                RejectionReason.SCORE_TOO_LOW,
                f"Score {scanner_score:.0f} < {cfg.min_scanner_score:.0f} min",
                **base_result,
            )

        # Check 6: Dollar volume (liquidity sanity)
        dollar_volume = price * volume
        if dollar_volume < cfg.min_dollar_volume:
            return self._reject(
                RejectionReason.LIQUIDITY_FAIL,
                f"Dollar volume ${dollar_volume:,.0f} < ${cfg.min_dollar_volume:,.0f}",
                **base_result,
            )

        # Check 7: Spread (if provided)
        if spread_pct is not None and spread_pct > cfg.max_spread_pct:
            return self._reject(
                RejectionReason.SPREAD_TOO_WIDE,
                f"Spread {spread_pct:.2f}% > {cfg.max_spread_pct:.2f}% max",
                **base_result,
            )

        # Check 8: Data freshness (if timestamp provided)
        if data_timestamp is not None:
            age_seconds = (datetime.now(timezone.utc) - data_timestamp).total_seconds()
            if age_seconds > cfg.max_data_age_seconds:
                return self._reject(
                    RejectionReason.STALE_DATA,
                    f"Data age {age_seconds:.0f}s > {cfg.max_data_age_seconds:.0f}s max",
                    **base_result,
                )

        # All checks passed
        self._total_passed += 1
        logger.debug(
            f"[SCRUTINY] {symbol} PASSED: "
            f"price=${price:.2f}, vol={volume:,}, rvol={rvol:.1f}x, score={scanner_score:.0f}"
        )

        return ScrutinyResult(
            symbol=symbol,
            passed=True,
            price=price,
            volume=volume,
            rvol=rvol,
            scanner_score=scanner_score,
        )

    def _reject(
        self,
        reason: RejectionReason,
        details: str,
        symbol: str,
        price: float,
        volume: int,
        rvol: float,
        scanner_score: float,
    ) -> ScrutinyResult:
        """Create rejection result and update counters."""
        self._rejections_by_reason[reason.value] = (
            self._rejections_by_reason.get(reason.value, 0) + 1
        )

        logger.debug(f"[SCRUTINY] {symbol} REJECTED: {reason.value} - {details}")

        return ScrutinyResult(
            symbol=symbol,
            passed=False,
            rejection_reason=reason,
            rejection_details=details,
            price=price,
            volume=volume,
            rvol=rvol,
            scanner_score=scanner_score,
        )

    def get_stats(self) -> dict[str, Any]:
        """Get scrutiny statistics."""
        pass_rate = (
            self._total_passed / self._total_checked * 100
            if self._total_checked > 0
            else 0.0
        )

        return {
            "total_checked": self._total_checked,
            "total_passed": self._total_passed,
            "total_rejected": self._total_checked - self._total_passed,
            "pass_rate_pct": pass_rate,
            "rejections_by_reason": dict(self._rejections_by_reason),
            "config": {
                "min_price": self._config.min_price,
                "max_price": self._config.max_price,
                "min_volume": self._config.min_volume,
                "min_rvol": self._config.min_rvol,
                "min_scanner_score": self._config.min_scanner_score,
            },
        }

    def reset_stats(self) -> None:
        """Reset statistics counters."""
        self._total_checked = 0
        self._total_passed = 0
        self._rejections_by_reason.clear()
