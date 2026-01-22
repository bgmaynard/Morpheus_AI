"""
Schwab Scanner Integration - Fetch market movers from Schwab API.

Schwab provides movers for:
- $DJI, $COMPX, $SPX.X indices
- Top gainers/losers by percent change or volume
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


class SchwabScannerClient:
    """
    Fetches market movers and screener results from Schwab API.

    Integrates with the Scanner module to provide candidate discovery.
    """

    def __init__(self, market_client: Any):
        """
        Initialize with a SchwabMarketClient instance.

        Args:
            market_client: SchwabMarketClient for API calls
        """
        self._client = market_client

    async def fetch_movers(
        self,
        index: str = "$COMPX",  # NASDAQ
        direction: str = "up",  # 'up' for gainers, 'down' for losers
        change_type: str = "percent",  # 'percent' or 'value'
    ) -> list[dict]:
        """
        Fetch market movers from Schwab.

        Args:
            index: Index to get movers for ($DJI, $COMPX, $SPX.X)
            direction: 'up' for gainers, 'down' for losers
            change_type: 'percent' for % change, 'value' for $ change

        Returns:
            List of mover data dictionaries
        """
        try:
            # Use the market client's movers endpoint
            movers = await self._client.get_movers(
                index=index,
                direction=direction,
                change_type=change_type,
            )

            if not movers:
                return []

            # Normalize the data format
            normalized = []
            for mover in movers:
                normalized.append(self._normalize_mover(mover))

            logger.debug(f"Fetched {len(normalized)} movers from {index}")
            return normalized

        except Exception as e:
            logger.error(f"Error fetching movers: {e}")
            return []

    async def fetch_all_movers(self) -> list[dict]:
        """
        Fetch movers from all major indices.

        Returns:
            Combined list of movers from NASDAQ, NYSE, etc.
        """
        all_movers = []

        # Fetch NASDAQ gainers (primary focus for small-cap momentum)
        nasdaq_movers = await self.fetch_movers(
            index="$COMPX",
            direction="up",
            change_type="percent",
        )
        all_movers.extend(nasdaq_movers)

        # Could also fetch from other indices:
        # nyse_movers = await self.fetch_movers(index="$DJI", ...)
        # sp_movers = await self.fetch_movers(index="$SPX.X", ...)

        # Deduplicate by symbol
        seen = set()
        unique_movers = []
        for mover in all_movers:
            symbol = mover.get("symbol")
            if symbol and symbol not in seen:
                seen.add(symbol)
                unique_movers.append(mover)

        return unique_movers

    def _normalize_mover(self, mover: dict) -> dict:
        """
        Normalize mover data to a consistent format.

        Schwab mover format varies, so we normalize to:
        - symbol, lastPrice, netPercentChangeInDouble, totalVolume
        - bidPrice, askPrice, highPrice, lowPrice
        """
        # Handle different Schwab response formats
        return {
            "symbol": mover.get("symbol", ""),
            "lastPrice": mover.get("last") or mover.get("lastPrice") or mover.get("regularMarketLastPrice"),
            "netPercentChangeInDouble": mover.get("percent_change") or mover.get("netPercentChangeInDouble") or mover.get("regularMarketPercentChangeInDouble"),
            "totalVolume": mover.get("volume") or mover.get("totalVolume") or mover.get("regularMarketVolume"),
            "averageVolume": mover.get("averageVolume") or mover.get("averageVolume10Day") or 1,
            "bidPrice": mover.get("bidPrice") or mover.get("bid"),
            "askPrice": mover.get("askPrice") or mover.get("ask"),
            "highPrice": mover.get("highPrice") or mover.get("regularMarketDayHigh"),
            "lowPrice": mover.get("lowPrice") or mover.get("regularMarketDayLow"),
            "float": mover.get("float") or mover.get("floatShares"),
            # Pass through any additional data
            **{k: v for k, v in mover.items() if k not in (
                "symbol", "last", "lastPrice", "percent_change", "netPercentChangeInDouble",
                "volume", "totalVolume", "averageVolume", "bidPrice", "askPrice",
                "highPrice", "lowPrice", "float"
            )},
        }


async def create_schwab_movers_fetcher(market_client: Any):
    """
    Create a movers fetcher function for the Scanner.

    Usage:
        fetcher = await create_schwab_movers_fetcher(market_client)
        scanner = Scanner(fetch_movers=fetcher)
    """
    client = SchwabScannerClient(market_client)

    async def fetch_movers():
        return await client.fetch_all_movers()

    return fetch_movers


async def create_schwab_quote_fetcher(market_client: Any):
    """
    Create a quote fetcher function for the Scanner.

    Usage:
        fetcher = await create_schwab_quote_fetcher(market_client)
        scanner = Scanner(fetch_quote=fetcher)
    """
    async def fetch_quote(symbol: str) -> dict | None:
        try:
            quote = await market_client.get_quote(symbol)
            if quote:
                return {
                    "symbol": symbol,
                    "lastPrice": quote.get("lastPrice") or quote.get("mark"),
                    "netPercentChangeInDouble": quote.get("netPercentChangeInDouble"),
                    "totalVolume": quote.get("totalVolume"),
                    "averageVolume": quote.get("averageVolume"),
                    "bidPrice": quote.get("bidPrice"),
                    "askPrice": quote.get("askPrice"),
                    "highPrice": quote.get("highPrice"),
                    "lowPrice": quote.get("lowPrice"),
                }
            return None
        except Exception as e:
            logger.error(f"Error fetching quote for {symbol}: {e}")
            return None

    return fetch_quote
