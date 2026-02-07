"""
MAX_AI Scanner Client - Consumes discovery data from MAX_AI_SCANNER service.

This is the ONLY approved method for Morpheus_AI to access market discovery.
Morpheus_AI MUST NOT scrape Finviz, Yahoo, RSS, or any external source directly.

Per integration spec: docs/BOT_INTEGRATION_SPEC.md
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Any

import httpx

logger = logging.getLogger(__name__)

# MAX_AI_SCANNER service URL
MAX_AI_SCANNER_URL = "http://127.0.0.1:8787"


@dataclass
class ScannerRow:
    """A ranked row from MAX_AI_SCANNER."""
    rank: int
    symbol: str
    price: float
    change_pct: float
    volume: int
    ai_score: float
    tags: list[str] = field(default_factory=list)
    velocity_1m: Optional[float] = None
    rvol_proxy: Optional[float] = None
    rvol_5m: Optional[float] = None  # 5-minute RVOL for velocity detection
    hod_distance_pct: Optional[float] = None
    spread: Optional[float] = None
    float_shares: Optional[float] = None
    avg_volume: Optional[int] = None
    short_pct: Optional[float] = None  # Short interest percentage
    market_cap: Optional[float] = None
    gap_pct: Optional[float] = None
    prev_close: Optional[float] = None
    halt_status: Optional[str] = None
    halt_ts: Optional[str] = None  # Halt timestamp
    news_present: bool = False  # Has news/catalyst
    bid: Optional[float] = None
    ask: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None

    def to_mover_dict(self) -> dict[str, Any]:
        """
        Convert to Schwab-style mover dict format.

        This allows seamless integration with existing Scanner code
        which expects Schwab mover format.
        """
        # Calculate spread from bid/ask if available
        spread = self.spread
        if spread is None and self.bid and self.ask:
            spread = self.ask - self.bid

        # Estimate averageVolume from volume/rvol for scanner RVOL calculation
        rvol = self.rvol_proxy or 1.0
        avg_volume = int(self.volume / rvol) if rvol > 0 else self.volume

        return {
            "symbol": self.symbol,
            "lastPrice": self.price,
            "netPercentChangeInDouble": self.change_pct,
            "totalVolume": self.volume,
            "averageVolume": self.avg_volume or avg_volume,
            "bidPrice": self.bid or self.price * 0.999,
            "askPrice": self.ask or self.price * 1.001,
            "highPrice": self.high or self.price,
            "lowPrice": self.low or self.price * 0.95,
            # Additional fields from MAX_AI_SCANNER
            # Normalize score to 0-100 scale (MAX_AI returns 0-1)
            "ai_score": self.ai_score * 100 if self.ai_score <= 1.0 else self.ai_score,
            "scanner_score": self.ai_score * 100 if self.ai_score <= 1.0 else self.ai_score,
            "rvol": rvol,
            "rvol_proxy": rvol,  # Alias for consistency
            "rvol_5m": self.rvol_5m or self.velocity_1m,  # 5-min RVOL for velocity detection
            "velocity_1m": self.velocity_1m,
            "hod_distance_pct": self.hod_distance_pct,
            "tags": self.tags,
            "halt_status": self.halt_status,
            "halt_ts": self.halt_ts,
            "float_shares": self.float_shares,
            "avg_volume": self.avg_volume or avg_volume,
            "short_pct": self.short_pct,
            "market_cap": self.market_cap,
            "gap_pct": self.gap_pct,
            "news_present": self.news_present,
            # Source marker
            "_source": "MAX_AI_SCANNER",
        }


@dataclass
class HaltInfo:
    """Trading halt information."""
    symbol: str
    halt_time: Optional[datetime] = None
    halt_price: Optional[float] = None
    halt_reason: Optional[str] = None
    resume_time: Optional[datetime] = None
    resume_price: Optional[float] = None
    status: str = "HALTED"

    @property
    def is_active(self) -> bool:
        return self.status == "HALTED"

    @property
    def is_resumed(self) -> bool:
        return self.status == "RESUMED"


class MaxAIScannerClient:
    """
    Client for MAX_AI_SCANNER service.

    This replaces direct Schwab movers API for discovery.
    All discovery flows through MAX_AI_SCANNER.

    Usage:
        client = MaxAIScannerClient()

        # Get movers (compatible with existing Scanner interface)
        movers = await client.fetch_movers()

        # Get from specific profile
        movers = await client.fetch_movers(profile="GAPPERS")

        # Get trading halts
        halts = await client.get_active_halts()
    """

    def __init__(
        self,
        base_url: str = MAX_AI_SCANNER_URL,
        timeout: float = 5.0,
        fallback_to_schwab: bool = False,
    ):
        """
        Initialize MAX_AI Scanner client.

        Args:
            base_url: MAX_AI_SCANNER service URL
            timeout: Request timeout in seconds
            fallback_to_schwab: If True, fall back to Schwab on scanner failure
                               (NOT RECOMMENDED - violates integration spec)
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.fallback_to_schwab = fallback_to_schwab
        self._client: Optional[httpx.AsyncClient] = None
        self._is_healthy = False
        self._last_health_check: Optional[datetime] = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self.timeout,
                headers={"User-Agent": "Morpheus_AI/1.0"}
            )
        return self._client

    async def close(self):
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def health_check(self) -> bool:
        """Check if MAX_AI_SCANNER is healthy."""
        try:
            client = await self._get_client()
            resp = await client.get(f"{self.base_url}/health")
            self._is_healthy = resp.status_code == 200
            self._last_health_check = datetime.utcnow()
            return self._is_healthy
        except Exception as e:
            logger.warning(f"MAX_AI_SCANNER health check failed: {e}")
            self._is_healthy = False
            return False

    async def fetch_movers(
        self,
        profile: str = "FAST_MOVERS",
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """
        Fetch movers from MAX_AI_SCANNER.

        Returns data in Schwab-compatible mover dict format for
        seamless integration with existing Scanner code.

        Args:
            profile: Scanner profile (FAST_MOVERS, GAPPERS, HOD_BREAK, etc.)
            limit: Maximum results to return

        Returns:
            List of mover dicts in Schwab-compatible format
        """
        try:
            client = await self._get_client()

            resp = await client.get(
                f"{self.base_url}/scanner/rows",
                params={"profile": profile, "limit": limit}
            )
            resp.raise_for_status()

            data = resp.json()
            rows = []

            for row_data in data.get("rows", []):
                row = ScannerRow(
                    rank=row_data.get("rank", 0),
                    symbol=row_data.get("symbol", ""),
                    price=row_data.get("price", 0.0) or row_data.get("last_price", 0.0),
                    change_pct=row_data.get("change_pct", 0.0),
                    volume=row_data.get("volume", 0),
                    ai_score=row_data.get("ai_score", 0.0),
                    tags=row_data.get("tags", []),
                    velocity_1m=row_data.get("velocity_1m") or row_data.get("velocity"),
                    rvol_proxy=row_data.get("rvol_proxy") or row_data.get("rvol"),
                    rvol_5m=row_data.get("rvol_5m"),  # 5-min RVOL
                    hod_distance_pct=row_data.get("hod_distance_pct") or row_data.get("hod_proximity"),
                    spread=row_data.get("spread"),
                    float_shares=row_data.get("float_shares") or row_data.get("float"),
                    avg_volume=row_data.get("avg_volume") or row_data.get("averageVolume"),
                    short_pct=row_data.get("short_pct") or row_data.get("short_percent") or row_data.get("shortInterest") or row_data.get("short_interest"),
                    market_cap=row_data.get("market_cap") or row_data.get("marketCap"),
                    gap_pct=row_data.get("gap_pct"),
                    prev_close=row_data.get("prev_close"),
                    halt_status=row_data.get("halt_status"),
                    halt_ts=row_data.get("halt_ts") or row_data.get("halt_time"),
                    news_present=bool(row_data.get("news_present") or row_data.get("has_news")),
                    bid=row_data.get("bid"),
                    ask=row_data.get("ask"),
                    high=row_data.get("high"),
                    low=row_data.get("low"),
                )
                rows.append(row.to_mover_dict())

            logger.info(f"[MAX_AI] Fetched {len(rows)} movers from {profile} profile")
            return rows

        except httpx.HTTPStatusError as e:
            logger.error(f"MAX_AI_SCANNER HTTP error: {e.response.status_code}")
            return []
        except httpx.RequestError as e:
            logger.error(f"MAX_AI_SCANNER request error: {e}")
            return []
        except Exception as e:
            logger.error(f"MAX_AI_SCANNER unexpected error: {e}")
            return []

    async def fetch_all_profiles(self, limit: int = 25) -> list[dict[str, Any]]:
        """
        Fetch movers from all profiles and deduplicate.

        Returns:
            Combined list of unique movers from all profiles
        """
        profiles = ["FAST_MOVERS", "GAPPERS", "HOD_BREAK", "TOP_GAINERS"]

        all_movers = {}

        for profile in profiles:
            movers = await self.fetch_movers(profile=profile, limit=limit)
            for mover in movers:
                symbol = mover.get("symbol")
                if symbol and symbol not in all_movers:
                    all_movers[symbol] = mover

        return list(all_movers.values())

    async def get_symbol_context(self, symbol: str) -> dict[str, Any]:
        """
        Get full context for a symbol before making trade decisions.

        Args:
            symbol: Stock symbol

        Returns:
            Dict with profiles, scores, quote data
        """
        try:
            client = await self._get_client()
            resp = await client.get(f"{self.base_url}/scanner/symbol/{symbol.upper()}")
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Failed to get symbol context for {symbol}: {e}")
            return {}

    async def get_active_halts(self) -> list[HaltInfo]:
        """Get currently halted stocks."""
        try:
            client = await self._get_client()
            resp = await client.get(f"{self.base_url}/halts/active")
            resp.raise_for_status()

            halts = []
            for h in resp.json().get("halts", []):
                halts.append(HaltInfo(
                    symbol=h.get("symbol", ""),
                    halt_time=self._parse_datetime(h.get("halt_time")),
                    halt_price=h.get("halt_price"),
                    halt_reason=h.get("halt_reason"),
                    resume_time=self._parse_datetime(h.get("resume_time")),
                    resume_price=h.get("resume_price"),
                    status=h.get("status", "HALTED"),
                ))
            return halts

        except Exception as e:
            logger.error(f"Failed to get active halts: {e}")
            return []

    async def get_resumed_halts(self, hours: int = 2) -> list[HaltInfo]:
        """Get recently resumed halts."""
        try:
            client = await self._get_client()
            resp = await client.get(
                f"{self.base_url}/halts/resumed",
                params={"hours": hours}
            )
            resp.raise_for_status()

            halts = []
            for h in resp.json().get("halts", []):
                halts.append(HaltInfo(
                    symbol=h.get("symbol", ""),
                    halt_time=self._parse_datetime(h.get("halt_time")),
                    halt_price=h.get("halt_price"),
                    halt_reason=h.get("halt_reason"),
                    resume_time=self._parse_datetime(h.get("resume_time")),
                    resume_price=h.get("resume_price"),
                    status=h.get("status", "RESUMED"),
                ))
            return halts

        except Exception as e:
            logger.error(f"Failed to get resumed halts: {e}")
            return []

    @staticmethod
    def _parse_datetime(dt_str: Optional[str]) -> Optional[datetime]:
        """Parse datetime string."""
        if not dt_str:
            return None
        try:
            return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            return None
