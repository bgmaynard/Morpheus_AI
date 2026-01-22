"""
Scanner - Real-time market scanner for trading candidates.

Scans the market for stocks meeting criteria for momentum trading:
- Small-cap stocks with high relative volume
- Significant percent change (gappers, runners)
- Acceptable spread/liquidity
- Volatility expansion

This scanner produces CANDIDATES, not TRADES. Candidates are passed
to the watchlist for monitoring and potential signal generation.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, time
from enum import Enum
from typing import Any, Callable, Awaitable

logger = logging.getLogger(__name__)


class ScanType(str, Enum):
    """Types of scans to perform."""
    PREMARKET_GAPPER = "premarket_gapper"
    MOMENTUM_RUNNER = "momentum_runner"
    VOLUME_SURGE = "volume_surge"
    BREAKOUT = "breakout"


@dataclass
class ScannerConfig:
    """Configuration for the scanner."""

    # Price range (small-cap focus)
    min_price: float = 1.00
    max_price: float = 20.00

    # Volume requirements
    min_volume: int = 100_000  # Minimum absolute volume
    min_rvol: float = 2.0  # Minimum relative volume (vs 20-day avg)

    # Movement requirements
    min_change_pct: float = 3.0  # Minimum % change to qualify
    max_change_pct: float = 100.0  # Filter out extreme outliers

    # Spread/liquidity requirements
    max_spread_pct: float = 2.0  # Maximum bid-ask spread as % of price

    # Float requirements (if available)
    max_float: int | None = 50_000_000  # Small float focus

    # Scan frequency
    scan_interval_seconds: float = 30.0

    # Results
    max_results: int = 20  # Maximum candidates per scan


@dataclass
class ScanResult:
    """Result from a scan - a trading candidate."""

    symbol: str
    scan_type: ScanType
    scanned_at: datetime

    # Current metrics
    price: float
    change_pct: float
    volume: int
    rvol: float

    # Spread analysis
    bid: float
    ask: float
    spread: float
    spread_pct: float

    # Additional context
    float_shares: int | None = None
    avg_volume: int | None = None
    high_of_day: float | None = None
    low_of_day: float | None = None

    # Score for ranking
    score: float = 0.0

    # Reasons this candidate qualified
    reasons: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "symbol": self.symbol,
            "scan_type": self.scan_type.value,
            "scanned_at": self.scanned_at.isoformat(),
            "price": self.price,
            "change_pct": self.change_pct,
            "volume": self.volume,
            "rvol": self.rvol,
            "bid": self.bid,
            "ask": self.ask,
            "spread": self.spread,
            "spread_pct": self.spread_pct,
            "float_shares": self.float_shares,
            "avg_volume": self.avg_volume,
            "high_of_day": self.high_of_day,
            "low_of_day": self.low_of_day,
            "score": self.score,
            "reasons": self.reasons,
        }


class Scanner:
    """
    Real-time market scanner for trading candidates.

    Usage:
        scanner = Scanner(
            config=ScannerConfig(),
            fetch_movers=my_api_fetch_function,
            on_candidate=handle_new_candidate,
        )
        await scanner.start()

    The scanner runs continuously, periodically scanning for candidates
    and emitting them via the on_candidate callback.
    """

    def __init__(
        self,
        config: ScannerConfig | None = None,
        fetch_movers: Callable[[], Awaitable[list[dict]]] | None = None,
        fetch_quote: Callable[[str], Awaitable[dict | None]] | None = None,
        on_candidate: Callable[[ScanResult], Awaitable[None]] | None = None,
    ):
        """
        Initialize scanner.

        Args:
            config: Scanner configuration
            fetch_movers: Async function to fetch market movers/gappers
            fetch_quote: Async function to fetch quote for a symbol
            on_candidate: Async callback when candidate is found
        """
        self.config = config or ScannerConfig()
        self._fetch_movers = fetch_movers
        self._fetch_quote = fetch_quote
        self._on_candidate = on_candidate

        self._running = False
        self._scan_task: asyncio.Task | None = None

        # Track seen candidates to avoid duplicates
        self._seen_today: set[str] = set()
        self._last_scan: datetime | None = None

        # Stats
        self._scans_performed: int = 0
        self._candidates_found: int = 0

        logger.info("Scanner initialized")
        logger.info(f"  Price range: ${config.min_price:.2f} - ${config.max_price:.2f}")
        logger.info(f"  Min RVOL: {config.min_rvol}x")
        logger.info(f"  Min change: {config.min_change_pct}%")

    async def start(self) -> None:
        """Start the scanner loop."""
        if self._running:
            logger.warning("Scanner already running")
            return

        self._running = True
        self._scan_task = asyncio.create_task(self._scan_loop())
        logger.info("Scanner started")

    async def stop(self) -> None:
        """Stop the scanner loop."""
        self._running = False
        if self._scan_task:
            self._scan_task.cancel()
            try:
                await self._scan_task
            except asyncio.CancelledError:
                pass
        logger.info("Scanner stopped")

    async def _scan_loop(self) -> None:
        """Main scanning loop."""
        while self._running:
            try:
                # Only scan during market hours
                if self._is_scan_time():
                    await self._perform_scan()

                await asyncio.sleep(self.config.scan_interval_seconds)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Scanner error: {e}")
                await asyncio.sleep(5.0)  # Back off on error

    def _is_scan_time(self) -> bool:
        """Check if we should be scanning (market hours)."""
        now = datetime.now(timezone.utc)
        current_time = now.time()

        # Pre-market: 4:00 AM - 9:30 AM ET (9:00 - 14:30 UTC)
        # Regular: 9:30 AM - 4:00 PM ET (14:30 - 21:00 UTC)
        premarket_start = time(9, 0)  # 4 AM ET
        market_close = time(21, 0)  # 4 PM ET

        # Scan during pre-market and regular hours
        return premarket_start <= current_time <= market_close

    async def _perform_scan(self) -> None:
        """Perform a single scan cycle."""
        if not self._fetch_movers:
            logger.debug("No fetch_movers function configured")
            return

        self._scans_performed += 1
        self._last_scan = datetime.now(timezone.utc)

        try:
            # Fetch market movers
            movers = await self._fetch_movers()

            if not movers:
                logger.debug("No movers returned from API")
                return

            logger.debug(f"Scanning {len(movers)} movers")

            # Evaluate each mover
            candidates: list[ScanResult] = []

            for mover in movers:
                result = await self._evaluate_candidate(mover)
                if result:
                    candidates.append(result)

            # Sort by score and take top N
            candidates.sort(key=lambda x: x.score, reverse=True)
            top_candidates = candidates[: self.config.max_results]

            # Emit new candidates
            for candidate in top_candidates:
                if candidate.symbol not in self._seen_today:
                    self._seen_today.add(candidate.symbol)
                    self._candidates_found += 1

                    if self._on_candidate:
                        await self._on_candidate(candidate)

                    logger.info(
                        f"[SCANNER] New candidate: {candidate.symbol} "
                        f"{candidate.change_pct:+.1f}% RVOL={candidate.rvol:.1f}x "
                        f"Score={candidate.score:.1f}"
                    )

        except Exception as e:
            logger.error(f"Scan error: {e}")

    async def _evaluate_candidate(self, mover: dict) -> ScanResult | None:
        """
        Evaluate a mover and return ScanResult if it qualifies.

        Args:
            mover: Raw mover data from API

        Returns:
            ScanResult if qualified, None otherwise
        """
        symbol = mover.get("symbol", "").upper()
        if not symbol:
            return None

        # Extract basic data
        price = mover.get("lastPrice") or mover.get("last") or mover.get("price") or 0
        change_pct = mover.get("netPercentChangeInDouble") or mover.get("change_pct") or 0
        volume = mover.get("totalVolume") or mover.get("volume") or 0
        avg_volume = mover.get("averageVolume") or mover.get("avg_volume") or 1

        # Calculate RVOL
        rvol = volume / avg_volume if avg_volume > 0 else 0

        # Get quote for spread data
        bid = mover.get("bidPrice") or mover.get("bid") or price
        ask = mover.get("askPrice") or mover.get("ask") or price
        spread = ask - bid if ask > bid else 0
        spread_pct = (spread / price * 100) if price > 0 else 999

        # Check against criteria
        reasons: list[str] = []
        disqualified = False

        # Price range check
        if not (self.config.min_price <= price <= self.config.max_price):
            disqualified = True

        # Volume check
        if volume < self.config.min_volume:
            disqualified = True

        # RVOL check
        if rvol >= self.config.min_rvol:
            reasons.append(f"High RVOL ({rvol:.1f}x)")
        else:
            disqualified = True

        # Change percent check
        abs_change = abs(change_pct)
        if abs_change >= self.config.min_change_pct:
            if change_pct > 0:
                reasons.append(f"Strong gainer ({change_pct:+.1f}%)")
            else:
                reasons.append(f"Strong move ({change_pct:+.1f}%)")
        else:
            disqualified = True

        if abs_change > self.config.max_change_pct:
            disqualified = True  # Too extreme

        # Spread check
        if spread_pct <= self.config.max_spread_pct:
            if spread_pct < 0.5:
                reasons.append("Tight spread")
        else:
            disqualified = True
            reasons.append(f"Wide spread ({spread_pct:.1f}%)")

        if disqualified:
            return None

        # Determine scan type
        scan_type = self._determine_scan_type(change_pct, rvol, price, mover)

        # Calculate score
        score = self._calculate_score(change_pct, rvol, spread_pct, volume)

        return ScanResult(
            symbol=symbol,
            scan_type=scan_type,
            scanned_at=datetime.now(timezone.utc),
            price=price,
            change_pct=change_pct,
            volume=volume,
            rvol=rvol,
            bid=bid,
            ask=ask,
            spread=spread,
            spread_pct=spread_pct,
            float_shares=mover.get("float"),
            avg_volume=avg_volume,
            high_of_day=mover.get("highPrice") or mover.get("high"),
            low_of_day=mover.get("lowPrice") or mover.get("low"),
            score=score,
            reasons=reasons,
        )

    def _determine_scan_type(
        self, change_pct: float, rvol: float, price: float, mover: dict
    ) -> ScanType:
        """Determine the scan type based on characteristics."""
        now = datetime.now(timezone.utc)

        # Pre-market = before 14:30 UTC (9:30 AM ET)
        if now.time() < time(14, 30):
            if change_pct > 5:
                return ScanType.PREMARKET_GAPPER

        # Volume surge = very high RVOL
        if rvol > 5:
            return ScanType.VOLUME_SURGE

        # Breakout = near high of day with momentum
        hod = mover.get("highPrice") or mover.get("high")
        if hod and price > 0:
            if price >= hod * 0.98:  # Within 2% of HOD
                return ScanType.BREAKOUT

        # Default to momentum runner
        return ScanType.MOMENTUM_RUNNER

    def _calculate_score(
        self, change_pct: float, rvol: float, spread_pct: float, volume: int
    ) -> float:
        """
        Calculate a composite score for ranking candidates.

        Higher score = better candidate.
        """
        score = 0.0

        # Percent change component (0-40 points)
        abs_change = abs(change_pct)
        score += min(abs_change * 2, 40)

        # RVOL component (0-30 points)
        score += min(rvol * 5, 30)

        # Spread component (0-20 points) - tighter is better
        if spread_pct < 0.3:
            score += 20
        elif spread_pct < 0.5:
            score += 15
        elif spread_pct < 1.0:
            score += 10
        elif spread_pct < 2.0:
            score += 5

        # Volume component (0-10 points)
        if volume > 1_000_000:
            score += 10
        elif volume > 500_000:
            score += 7
        elif volume > 200_000:
            score += 4

        return score

    def reset_daily(self) -> None:
        """Reset daily tracking (call at market open)."""
        self._seen_today.clear()
        logger.info("Scanner daily tracking reset")

    def get_stats(self) -> dict[str, Any]:
        """Get scanner statistics."""
        return {
            "running": self._running,
            "scans_performed": self._scans_performed,
            "candidates_found": self._candidates_found,
            "seen_today": len(self._seen_today),
            "last_scan": self._last_scan.isoformat() if self._last_scan else None,
            "config": {
                "min_price": self.config.min_price,
                "max_price": self.config.max_price,
                "min_rvol": self.config.min_rvol,
                "min_change_pct": self.config.min_change_pct,
                "scan_interval": self.config.scan_interval_seconds,
            },
        }

    # =========================================================================
    # Manual scan methods
    # =========================================================================

    async def scan_symbol(self, symbol: str) -> ScanResult | None:
        """
        Manually evaluate a single symbol.

        Args:
            symbol: Symbol to evaluate

        Returns:
            ScanResult if qualified, None otherwise
        """
        if not self._fetch_quote:
            logger.warning("No fetch_quote function configured")
            return None

        quote = await self._fetch_quote(symbol)
        if not quote:
            return None

        return await self._evaluate_candidate(quote)

    async def force_scan(self) -> list[ScanResult]:
        """
        Force an immediate scan regardless of timing.

        Returns:
            List of candidates found
        """
        if not self._fetch_movers:
            return []

        movers = await self._fetch_movers()
        if not movers:
            return []

        candidates = []
        for mover in movers:
            result = await self._evaluate_candidate(mover)
            if result:
                candidates.append(result)

        candidates.sort(key=lambda x: x.score, reverse=True)
        return candidates[: self.config.max_results]
