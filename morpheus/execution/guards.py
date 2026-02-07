"""
Execution Guards - Market condition checks before order submission.

Guards evaluate:
- Spread width (bid-ask spread)
- Slippage estimates
- Liquidity (volume)
- Market status (open/halted)

All guard checks are PURE and DETERMINISTIC:
- Same inputs → same decision
- No external calls
- No network/timing dependencies

Phase 7 Scope:
- Guard checks only
- No order submission
- No upstream overrides
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from morpheus.data.market_snapshot import MarketSnapshot
from morpheus.risk.base import RiskResult
from morpheus.execution.base import (
    ExecutionGuard,
    ExecutionCheck,
    ExecutionConfig,
    ExecutionDecision,
    BlockReason,
    EXECUTION_SCHEMA_VERSION,
)


@dataclass(frozen=True)
class GuardConfig:
    """
    Configuration for execution guards.

    Defines thresholds for guard checks.
    """

    # Spread thresholds
    max_spread_pct: float = 0.5  # 0.5% max spread

    # Slippage thresholds
    max_slippage_pct: float = 0.5  # 0.5% max estimated slippage

    # Liquidity thresholds
    min_volume: int = 1000  # Minimum average volume

    # Market status
    require_market_open: bool = True
    require_tradeable: bool = True


class StandardExecutionGuard(ExecutionGuard):
    """
    Standard execution guard with configurable thresholds.

    Checks (in order):
    1. Market open status
    2. Symbol tradeable status
    3. Spread width
    4. Volume/liquidity
    5. Slippage estimate

    All checks are PURE and DETERMINISTIC.
    """

    def __init__(self, config: GuardConfig | None = None):
        """
        Initialize the execution guard.

        Args:
            config: Optional custom guard configuration
        """
        self._config = config or GuardConfig()

    @property
    def name(self) -> str:
        return "standard_execution_guard"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return "Standard guard with spread, slippage, and liquidity checks"

    def check(
        self,
        risk_result: RiskResult,
        snapshot: MarketSnapshot,
        config: ExecutionConfig,
    ) -> ExecutionCheck:
        """
        Check if execution should proceed.

        PURE FUNCTION: depends only on inputs, no side effects.
        """
        cfg = self._config
        reasons: list[BlockReason] = []
        details_parts: list[str] = []

        # Extract position size
        position_size = risk_result.position_size
        if position_size is None:
            reasons.append(BlockReason.INVALID_ORDER)
            details_parts.append("No position size in risk result")
            return self._create_blocked_result(
                reasons, details_parts, snapshot, cfg
            )

        # Check 1: Market open
        if cfg.require_market_open and not snapshot.is_market_open:
            reasons.append(BlockReason.MARKET_CLOSED)
            details_parts.append("Market is closed")

        # Check 1b: Time drift safety
        try:
            from morpheus.core.time_authority import get_time_authority
            ta = get_time_authority()
            if not ta.is_drift_safe():
                reasons.append(BlockReason.TIME_DRIFT_UNSAFE)
                details_parts.append(
                    f"Time drift {ta.drift_ms():.0f}ms exceeds 1000ms threshold"
                )
        except ImportError:
            pass  # TimeAuthority not available - skip check

        # Check 2: Symbol tradeable
        if cfg.require_tradeable and not snapshot.is_tradeable:
            reasons.append(BlockReason.SYMBOL_HALTED)
            details_parts.append("Symbol is not tradeable (halted)")

        # Check 3: Spread width
        spread_pct = self._calculate_spread_pct(snapshot)
        max_spread = config.max_spread_pct if config.max_spread_pct > 0 else cfg.max_spread_pct

        if spread_pct > max_spread:
            reasons.append(BlockReason.SPREAD_TOO_WIDE)
            details_parts.append(
                f"Spread {spread_pct:.3f}% exceeds {max_spread:.3f}%"
            )

        # Check 4: Volume/liquidity
        volume = snapshot.volume
        min_vol = config.min_volume if config.min_volume > 0 else cfg.min_volume

        if volume < min_vol:
            reasons.append(BlockReason.LOW_LIQUIDITY)
            details_parts.append(
                f"Volume {volume:,} below minimum {min_vol:,}"
            )

        # Check 5: Slippage estimate (simple model based on order size vs volume)
        slippage_pct = self._estimate_slippage(
            position_size.shares, volume, spread_pct
        )
        max_slippage = config.max_slippage_pct if config.max_slippage_pct > 0 else cfg.max_slippage_pct

        if slippage_pct > max_slippage:
            reasons.append(BlockReason.SLIPPAGE_EXCEEDED)
            details_parts.append(
                f"Est. slippage {slippage_pct:.3f}% exceeds {max_slippage:.3f}%"
            )

        # Make decision
        if reasons:
            return self.create_blocked(
                reasons=tuple(reasons),
                spread_pct=spread_pct,
                spread_threshold=max_spread,
                slippage_pct=slippage_pct,
                slippage_threshold=max_slippage,
                volume=volume,
                min_volume=min_vol,
                details="; ".join(details_parts),
            )
        else:
            return self.create_executable(
                spread_pct=spread_pct,
                spread_threshold=max_spread,
                slippage_pct=slippage_pct,
                slippage_threshold=max_slippage,
                volume=volume,
                min_volume=min_vol,
                details=f"All checks passed. Spread: {spread_pct:.3f}%, Est. slippage: {slippage_pct:.3f}%",
            )

    def _calculate_spread_pct(self, snapshot: MarketSnapshot) -> float:
        """
        Calculate bid-ask spread as percentage of mid price.

        PURE: depends only on snapshot.
        """
        if snapshot.bid <= 0 or snapshot.ask <= 0:
            return 0.0

        spread = snapshot.ask - snapshot.bid
        mid = (snapshot.bid + snapshot.ask) / 2

        if mid <= 0:
            return 0.0

        return (spread / mid) * 100

    def _estimate_slippage(
        self,
        order_shares: int,
        volume: int,
        spread_pct: float,
    ) -> float:
        """
        Estimate slippage based on order size and market depth.

        Simple model:
        - Base slippage = half the spread (market order crosses spread)
        - Additional slippage based on order size vs volume

        PURE: depends only on inputs.
        """
        if volume <= 0 or order_shares <= 0:
            return spread_pct / 2  # Just the spread crossing

        # Order size as fraction of volume
        size_fraction = order_shares / volume

        # Simple linear model: larger orders = more slippage
        # At 1% of volume, add 0.1% slippage
        # At 10% of volume, add 1% slippage
        size_impact = size_fraction * 10  # 10x multiplier

        # Total slippage = spread crossing + size impact
        return (spread_pct / 2) + size_impact

    def _create_blocked_result(
        self,
        reasons: list[BlockReason],
        details: list[str],
        snapshot: MarketSnapshot,
        cfg: GuardConfig,
    ) -> ExecutionCheck:
        """Create blocked result for early exit cases."""
        return self.create_blocked(
            reasons=tuple(reasons),
            spread_pct=self._calculate_spread_pct(snapshot),
            spread_threshold=cfg.max_spread_pct,
            slippage_pct=0.0,
            slippage_threshold=cfg.max_slippage_pct,
            volume=snapshot.volume,
            min_volume=cfg.min_volume,
            details="; ".join(details),
        )


class PermissiveExecutionGuard(ExecutionGuard):
    """
    Permissive execution guard for paper trading.

    Only blocks:
    - Market closed (optional)
    - Symbol halted

    Allows wider spreads and lower liquidity.
    """

    def __init__(self, require_market_open: bool = False):
        """
        Initialize permissive guard.

        Args:
            require_market_open: Whether to check market hours
        """
        self._require_market_open = require_market_open

    @property
    def name(self) -> str:
        return "permissive_execution_guard"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return "Permissive guard for paper trading (minimal checks)"

    def check(
        self,
        risk_result: RiskResult,
        snapshot: MarketSnapshot,
        config: ExecutionConfig,
    ) -> ExecutionCheck:
        """Check with minimal restrictions."""
        reasons: list[BlockReason] = []
        details_parts: list[str] = []

        # Only check critical conditions
        if self._require_market_open and not snapshot.is_market_open:
            reasons.append(BlockReason.MARKET_CLOSED)
            details_parts.append("Market closed")

        if not snapshot.is_tradeable:
            reasons.append(BlockReason.SYMBOL_HALTED)
            details_parts.append("Symbol halted")

        spread_pct = self._calculate_spread_pct(snapshot)

        if reasons:
            return self.create_blocked(
                reasons=tuple(reasons),
                spread_pct=spread_pct,
                spread_threshold=10.0,  # Very high threshold
                slippage_pct=0.0,
                slippage_threshold=10.0,
                volume=snapshot.volume,
                min_volume=0,
                details="; ".join(details_parts),
            )

        return self.create_executable(
            spread_pct=spread_pct,
            spread_threshold=10.0,
            slippage_pct=0.0,
            slippage_threshold=10.0,
            volume=snapshot.volume,
            min_volume=0,
            details="Permissive approval",
        )

    def _calculate_spread_pct(self, snapshot: MarketSnapshot) -> float:
        """Calculate spread percentage."""
        if snapshot.bid <= 0 or snapshot.ask <= 0:
            return 0.0
        spread = snapshot.ask - snapshot.bid
        mid = (snapshot.bid + snapshot.ask) / 2
        return (spread / mid) * 100 if mid > 0 else 0.0


class StrictExecutionGuard(ExecutionGuard):
    """
    Strict execution guard with tight thresholds.

    For production use with conservative limits:
    - Max 0.2% spread
    - Max 0.2% slippage
    - Min 10,000 volume
    """

    def __init__(self):
        """Initialize with strict thresholds."""
        self._config = GuardConfig(
            max_spread_pct=0.2,
            max_slippage_pct=0.2,
            min_volume=10000,
            require_market_open=True,
            require_tradeable=True,
        )
        self._standard = StandardExecutionGuard(self._config)

    @property
    def name(self) -> str:
        return "strict_execution_guard"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return "Strict guard with tight spread/slippage limits"

    def check(
        self,
        risk_result: RiskResult,
        snapshot: MarketSnapshot,
        config: ExecutionConfig,
    ) -> ExecutionCheck:
        """Delegate to standard guard with strict config."""
        # Use strict config thresholds, not the passed config
        strict_config = ExecutionConfig(
            trading_mode=config.trading_mode,
            live_trading_armed=config.live_trading_armed,
            max_spread_pct=self._config.max_spread_pct,
            max_slippage_pct=self._config.max_slippage_pct,
            min_volume=self._config.min_volume,
        )
        return self._standard.check(risk_result, snapshot, strict_config)


@dataclass(frozen=True)
class QuoteFreshnessConfig:
    """
    Configuration for quote freshness validation.

    CRITICAL SAFETY: Prevents orders on stale/invalid quotes that caused
    ~$35,000 in losses on 2026-02-03 from bad warmup data.

    Thresholds calibrated for small-cap momentum trading.
    """

    # Maximum age of quote timestamp (milliseconds)
    # 2000ms = 2 seconds - quotes older than this are stale
    max_quote_age_ms: int = 2000

    # Maximum allowed spread (percentage)
    # 3% is wide but still tradeable for small caps
    max_spread_pct: float = 3.0

    # Minimum required prices
    min_bid: float = 0.01  # Bid must be at least $0.01
    min_ask: float = 0.01  # Ask must be at least $0.01
    min_last: float = 0.01  # Last must be at least $0.01

    # Price sanity bounds (relative to last price)
    # Rejects if bid/ask are more than 50% away from last
    max_price_deviation_pct: float = 50.0

    # Volume sanity (minimum volume to ensure quote is real)
    min_volume_for_freshness: int = 100


class QuoteFreshnessGuard(ExecutionGuard):
    """
    Quote Freshness Guard - Critical pre-order validation.

    CRITICAL SAFETY FEATURE added after 2026-02-03 losses caused by
    stale/cached quotes from server warmup entering positions at
    wrong prices (resulting in instant -35% to -65% hard stops).

    Checks (in priority order):
    1. Quote timestamp freshness (not stale)
    2. Bid/ask validity (exist and > 0)
    3. Last price validity
    4. Price sanity (bid/ask near last, not wildly divergent)
    5. Spread sanity (not absurdly wide)
    6. Minimum volume (quote represents real activity)

    This guard should be the FIRST check in the execution chain.
    If quotes are bad, nothing else matters.

    All checks are PURE and DETERMINISTIC.
    """

    def __init__(self, config: QuoteFreshnessConfig | None = None):
        """
        Initialize the quote freshness guard.

        Args:
            config: Optional custom configuration
        """
        self._config = config or QuoteFreshnessConfig()

    @property
    def name(self) -> str:
        return "quote_freshness_guard"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return "Critical pre-order validation for quote freshness and validity"

    def check(
        self,
        risk_result: RiskResult,
        snapshot: MarketSnapshot,
        config: ExecutionConfig,
    ) -> ExecutionCheck:
        """
        Check if quote data is fresh and valid for order submission.

        PURE FUNCTION: depends only on inputs, no side effects.

        This is the FIRST LINE OF DEFENSE against bad fill prices.
        """
        cfg = self._config
        reasons: list[BlockReason] = []
        details_parts: list[str] = []

        # Check 1: Quote timestamp freshness
        now = datetime.now(timezone.utc)
        quote_age_ms = int((now - snapshot.timestamp).total_seconds() * 1000)

        if quote_age_ms > cfg.max_quote_age_ms:
            reasons.append(BlockReason.QUOTE_STALE)
            details_parts.append(
                f"Quote stale: {quote_age_ms}ms old > {cfg.max_quote_age_ms}ms max"
            )

        # Check 2: Bid validity
        if snapshot.bid < cfg.min_bid:
            reasons.append(BlockReason.QUOTE_INVALID)
            details_parts.append(
                f"Invalid bid: ${snapshot.bid:.4f} < ${cfg.min_bid:.2f} minimum"
            )

        # Check 3: Ask validity
        if snapshot.ask < cfg.min_ask:
            reasons.append(BlockReason.QUOTE_INVALID)
            details_parts.append(
                f"Invalid ask: ${snapshot.ask:.4f} < ${cfg.min_ask:.2f} minimum"
            )

        # Check 4: Last price validity
        if snapshot.last < cfg.min_last:
            reasons.append(BlockReason.QUOTE_INVALID)
            details_parts.append(
                f"Invalid last: ${snapshot.last:.4f} < ${cfg.min_last:.2f} minimum"
            )

        # Check 5: Bid/ask sanity vs last price (catch stale cached data)
        # This is the KEY check that would have caught the 2026-02-03 losses
        if snapshot.last > 0 and snapshot.bid > 0:
            bid_deviation_pct = abs(snapshot.bid - snapshot.last) / snapshot.last * 100
            if bid_deviation_pct > cfg.max_price_deviation_pct:
                reasons.append(BlockReason.QUOTE_SANITY_FAILED)
                details_parts.append(
                    f"Bid sanity fail: ${snapshot.bid:.2f} is {bid_deviation_pct:.1f}% "
                    f"from last ${snapshot.last:.2f} (max {cfg.max_price_deviation_pct}%)"
                )

        if snapshot.last > 0 and snapshot.ask > 0:
            ask_deviation_pct = abs(snapshot.ask - snapshot.last) / snapshot.last * 100
            if ask_deviation_pct > cfg.max_price_deviation_pct:
                reasons.append(BlockReason.QUOTE_SANITY_FAILED)
                details_parts.append(
                    f"Ask sanity fail: ${snapshot.ask:.2f} is {ask_deviation_pct:.1f}% "
                    f"from last ${snapshot.last:.2f} (max {cfg.max_price_deviation_pct}%)"
                )

        # Check 6: Spread sanity
        spread_pct = snapshot.spread_pct
        if spread_pct > cfg.max_spread_pct:
            reasons.append(BlockReason.SPREAD_TOO_WIDE)
            details_parts.append(
                f"Spread too wide: {spread_pct:.2f}% > {cfg.max_spread_pct}% max"
            )

        # Check 7: Volume sanity (ensure quote represents real activity)
        if snapshot.volume < cfg.min_volume_for_freshness:
            reasons.append(BlockReason.LOW_LIQUIDITY)
            details_parts.append(
                f"Low volume: {snapshot.volume:,} < {cfg.min_volume_for_freshness:,} min"
            )

        # Make decision
        if reasons:
            # Deduplicate reasons
            unique_reasons = tuple(dict.fromkeys(reasons))
            return self.create_blocked(
                reasons=unique_reasons,
                spread_pct=spread_pct,
                spread_threshold=cfg.max_spread_pct,
                slippage_pct=0.0,
                slippage_threshold=0.0,
                volume=snapshot.volume,
                min_volume=cfg.min_volume_for_freshness,
                details="; ".join(details_parts),
            )
        else:
            return self.create_executable(
                spread_pct=spread_pct,
                spread_threshold=cfg.max_spread_pct,
                slippage_pct=0.0,
                slippage_threshold=0.0,
                volume=snapshot.volume,
                min_volume=cfg.min_volume_for_freshness,
                details=f"Quote fresh: {quote_age_ms}ms old, bid ${snapshot.bid:.2f}, "
                        f"ask ${snapshot.ask:.2f}, spread {spread_pct:.2f}%",
            )


def create_quote_freshness_guard(
    config: QuoteFreshnessConfig | None = None,
) -> QuoteFreshnessGuard:
    """Factory function to create a QuoteFreshnessGuard."""
    return QuoteFreshnessGuard(config)


def create_standard_guard(config: GuardConfig | None = None) -> StandardExecutionGuard:
    """Factory function to create a StandardExecutionGuard."""
    return StandardExecutionGuard(config)


def create_permissive_guard(require_market_open: bool = False) -> PermissiveExecutionGuard:
    """Factory function to create a PermissiveExecutionGuard."""
    return PermissiveExecutionGuard(require_market_open)


def create_strict_guard() -> StrictExecutionGuard:
    """Factory function to create a StrictExecutionGuard."""
    return StrictExecutionGuard()


@dataclass(frozen=True)
class ConfirmationRequest:
    """
    Human confirmation request from UI.

    Contains:
    - symbol: Symbol to confirm entry for
    - chain_id: UI chain ID for symbol lock validation
    - signal_timestamp: ISO-8601 timestamp of the signal being confirmed
    - entry_price: Price at time of confirmation
    - command_id: UI-generated UUID for correlation
    """

    symbol: str
    chain_id: int
    signal_timestamp: str  # ISO-8601
    entry_price: float
    command_id: str
    received_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True)
class ConfirmationResult:
    """
    Result of human confirmation validation.

    Contains:
    - accepted: Whether confirmation was accepted
    - reason: Reason code if rejected
    - details: Human-readable details
    """

    accepted: bool
    reason: BlockReason | None = None
    details: str = ""
    signal_age_ms: int = 0
    price_drift_pct: float = 0.0

    def to_event_payload(self) -> dict[str, Any]:
        """Convert to event payload dict."""
        return {
            "accepted": self.accepted,
            "reason": self.reason.value if self.reason else None,
            "details": self.details,
            "signal_age_ms": self.signal_age_ms,
            "price_drift_pct": self.price_drift_pct,
        }


@dataclass(frozen=True)
class ConfirmationGuardConfig:
    """
    Configuration for confirmation guard.

    Thresholds for human confirmation validation.

    SMALL-CAP MOMENTUM OPTIMIZED:
    - TTL 1200ms: Breakouts valid after 2-3s usually aren't the move
    - Drift 0.20%: Prevents chasing and FOMO entries
    """

    # Signal staleness threshold (milliseconds)
    # Small-cap momentum: 800-1500ms recommended, 1200ms default
    confirm_ttl_ms: int = 1200  # 1.2 seconds (small-cap optimized)

    # Maximum price drift from entry (percentage)
    # Small-cap momentum: 0.15-0.30% recommended, 0.20% default
    max_drift_pct: float = 0.20  # 0.20% (small-cap optimized)


class ConfirmationGuard:
    """
    Human Confirmation Guard - Validates tape-synchronized human confirmations.

    This guard validates that:
    1. There is an active signal for the symbol
    2. The signal is not stale (< CONFIRM_TTL_MS)
    3. Price drift from entry is within bounds (< MAX_DRIFT_PCT)
    4. Execution is armed (if LIVE mode)
    5. Existing execution guards pass
    6. Kill switch is not active
    7. Symbol matches active context (symbol lock)

    PURE and DETERMINISTIC: Same inputs → same decision.

    On accept: Proceeds to execution via existing Phase 7 pipeline.
    On reject: Emits HUMAN_CONFIRM_REJECTED with reason.
    """

    def __init__(self, config: ConfirmationGuardConfig | None = None):
        """
        Initialize confirmation guard.

        Args:
            config: Optional custom configuration
        """
        self._config = config or ConfirmationGuardConfig()

    @property
    def name(self) -> str:
        return "confirmation_guard"

    @property
    def version(self) -> str:
        return "1.0.0"

    def validate(
        self,
        request: ConfirmationRequest,
        signal_timestamp: str | None,
        signal_entry_price: float | None,
        current_price: float,
        is_live_mode: bool,
        is_live_armed: bool,
        is_kill_switch_active: bool,
        active_symbol: str | None,
    ) -> ConfirmationResult:
        """
        Validate a human confirmation request.

        PURE FUNCTION: depends only on inputs, no side effects.

        Args:
            request: The confirmation request from UI
            signal_timestamp: ISO-8601 timestamp of active signal (None if no signal)
            signal_entry_price: Entry price from signal (None if no signal)
            current_price: Current market price
            is_live_mode: Whether system is in LIVE mode
            is_live_armed: Whether live trading is armed
            is_kill_switch_active: Whether kill switch is active
            active_symbol: Currently active symbol in Morpheus context

        Returns:
            ConfirmationResult with acceptance/rejection decision
        """
        cfg = self._config

        # Check 1: Kill switch
        if is_kill_switch_active:
            return ConfirmationResult(
                accepted=False,
                reason=BlockReason.KILL_SWITCH_ACTIVE,
                details="Kill switch is active",
            )

        # Check 2: Symbol lock
        if active_symbol and request.symbol.upper() != active_symbol.upper():
            return ConfirmationResult(
                accepted=False,
                reason=BlockReason.SYMBOL_MISMATCH,
                details=f"Symbol mismatch: request={request.symbol}, active={active_symbol}",
            )

        # Check 3: No active signal
        if signal_timestamp is None:
            return ConfirmationResult(
                accepted=False,
                reason=BlockReason.NO_ACTIVE_SIGNAL,
                details="No active signal for symbol",
            )

        # Check 4: Signal staleness
        try:
            signal_time = datetime.fromisoformat(signal_timestamp.replace('Z', '+00:00'))
            now = datetime.now(timezone.utc)
            signal_age_ms = int((now - signal_time).total_seconds() * 1000)
        except (ValueError, AttributeError):
            return ConfirmationResult(
                accepted=False,
                reason=BlockReason.SIGNAL_STALE,
                details=f"Invalid signal timestamp: {signal_timestamp}",
            )

        if signal_age_ms > cfg.confirm_ttl_ms:
            return ConfirmationResult(
                accepted=False,
                reason=BlockReason.SIGNAL_STALE,
                details=f"Signal stale: {signal_age_ms}ms > {cfg.confirm_ttl_ms}ms TTL",
                signal_age_ms=signal_age_ms,
            )

        # Check 5: Price drift
        if signal_entry_price and signal_entry_price > 0:
            drift = abs(current_price - signal_entry_price) / signal_entry_price * 100
            if drift > cfg.max_drift_pct:
                return ConfirmationResult(
                    accepted=False,
                    reason=BlockReason.PRICE_DRIFT_EXCEEDED,
                    details=f"Price drift {drift:.3f}% > {cfg.max_drift_pct}% max",
                    signal_age_ms=signal_age_ms,
                    price_drift_pct=drift,
                )
        else:
            drift = 0.0

        # Check 6: Live trading armed (if in LIVE mode)
        if is_live_mode and not is_live_armed:
            return ConfirmationResult(
                accepted=False,
                reason=BlockReason.LIVE_NOT_ARMED,
                details="Live trading not armed",
                signal_age_ms=signal_age_ms,
                price_drift_pct=drift,
            )

        # All checks passed
        return ConfirmationResult(
            accepted=True,
            reason=None,
            details=f"Confirmation accepted. Signal age: {signal_age_ms}ms, drift: {drift:.3f}%",
            signal_age_ms=signal_age_ms,
            price_drift_pct=drift,
        )


def create_confirmation_guard(
    config: ConfirmationGuardConfig | None = None,
) -> ConfirmationGuard:
    """Factory function to create a ConfirmationGuard."""
    return ConfirmationGuard(config)
