"""
Schwab Market Data API Client.

Provides quote and candle data retrieval from Schwab API.
Candles are fetched from Schwab endpoints (source of truth),
NOT derived from quote sampling.

Phase 2 Scope:
- Read-only market data
- No trade decisions
- No state mutation
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Protocol

from morpheus.broker.schwab_auth import SchwabAuth
from morpheus.data.market_snapshot import (
    MarketSnapshot,
    Candle,
    create_snapshot,
)

logger = logging.getLogger(__name__)


class HttpClient(Protocol):
    """Protocol for HTTP client (allows dependency injection)."""

    def get(self, url: str, **kwargs: Any) -> Any:
        ...

    def post(self, url: str, **kwargs: Any) -> Any:
        ...


class SchwabMarketError(Exception):
    """Raised when market data retrieval fails."""

    pass


class QuoteUnavailableError(SchwabMarketError):
    """Raised when quote data is not available for a symbol."""

    pass


@dataclass
class QuoteData:
    """Raw quote data from Schwab API."""

    symbol: str
    bid_price: float
    ask_price: float
    last_price: float
    total_volume: int
    quote_time: datetime
    is_tradeable: bool
    is_market_open: bool

    @classmethod
    def from_schwab_response(cls, symbol: str, data: dict[str, Any]) -> QuoteData:
        """Parse Schwab quote response into QuoteData."""
        quote = data.get("quote", data)

        # Parse quote time (Schwab returns epoch milliseconds)
        quote_time_ms = quote.get("quoteTime", 0)
        if quote_time_ms:
            quote_time = datetime.fromtimestamp(
                quote_time_ms / 1000, tz=timezone.utc
            )
        else:
            quote_time = datetime.now(timezone.utc)

        return cls(
            symbol=symbol,
            bid_price=float(quote.get("bidPrice", 0.0)),
            ask_price=float(quote.get("askPrice", 0.0)),
            last_price=float(quote.get("lastPrice", 0.0)),
            total_volume=int(quote.get("totalVolume", 0)),
            quote_time=quote_time,
            is_tradeable=quote.get("securityStatus", "Normal") == "Normal",
            is_market_open=quote.get("isMarketOpen", True),
        )


class SchwabMarketClient:
    """
    Client for Schwab market data API.

    Provides methods to fetch quotes and candles.
    All data is READ-ONLY and returned as immutable objects.
    """

    BASE_URL = "https://api.schwabapi.com/marketdata/v1"

    def __init__(self, auth: SchwabAuth, http_client: HttpClient):
        """
        Initialize the market data client.

        Args:
            auth: SchwabAuth instance for token management
            http_client: HTTP client for API requests
        """
        self.auth = auth
        self.http = http_client

    def _get_headers(self) -> dict[str, str]:
        """Get headers for API requests with valid auth token."""
        return {
            **self.auth.get_auth_header(self.http),
            "Accept": "application/json",
        }

    def get_quote(self, symbol: str) -> QuoteData:
        """
        Get current quote for a single symbol.

        Args:
            symbol: Stock symbol (e.g., "AAPL")

        Returns:
            QuoteData with current bid/ask/last/volume

        Raises:
            QuoteUnavailableError: If quote cannot be retrieved
        """
        url = f"{self.BASE_URL}/quotes"
        params = {"symbols": symbol.upper()}

        try:
            response = self.http.get(
                url,
                headers=self._get_headers(),
                params=params,
            )

            if response.status_code != 200:
                raise SchwabMarketError(
                    f"Quote request failed: {response.status_code} - {response.text}"
                )

            data = response.json()

            if symbol.upper() not in data:
                raise QuoteUnavailableError(
                    f"No quote data for symbol: {symbol}"
                )

            return QuoteData.from_schwab_response(
                symbol.upper(), data[symbol.upper()]
            )

        except SchwabMarketError:
            raise
        except Exception as e:
            logger.error(f"Failed to get quote for {symbol}: {e}")
            raise SchwabMarketError(f"Quote request failed: {e}") from e

    def get_quotes(self, symbols: list[str]) -> dict[str, QuoteData]:
        """
        Get quotes for multiple symbols.

        Args:
            symbols: List of stock symbols

        Returns:
            Dict mapping symbol to QuoteData
        """
        if not symbols:
            return {}

        url = f"{self.BASE_URL}/quotes"
        symbols_upper = [s.upper() for s in symbols]
        params = {"symbols": ",".join(symbols_upper)}

        try:
            response = self.http.get(
                url,
                headers=self._get_headers(),
                params=params,
            )

            if response.status_code != 200:
                raise SchwabMarketError(
                    f"Quotes request failed: {response.status_code}"
                )

            data = response.json()
            results = {}

            for symbol in symbols_upper:
                if symbol in data:
                    results[symbol] = QuoteData.from_schwab_response(
                        symbol, data[symbol]
                    )
                else:
                    logger.warning(f"No quote data for {symbol}")

            return results

        except SchwabMarketError:
            raise
        except Exception as e:
            logger.error(f"Failed to get quotes: {e}")
            raise SchwabMarketError(f"Quotes request failed: {e}") from e

    def get_candles(
        self,
        symbol: str,
        period_type: str = "day",
        period: int = 1,
        frequency_type: str = "minute",
        frequency: int = 1,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[Candle]:
        """
        Get historical candles for a symbol.

        Candles come from Schwab endpoints (source of truth),
        NOT derived from quote sampling.

        Args:
            symbol: Stock symbol
            period_type: "day", "month", "year", "ytd"
            period: Number of periods
            frequency_type: "minute", "daily", "weekly", "monthly"
            frequency: Candle frequency (1, 5, 10, 15, 30 for minutes)
            start_date: Optional start datetime
            end_date: Optional end datetime

        Returns:
            List of Candle objects, oldest first
        """
        url = f"{self.BASE_URL}/pricehistory"

        params: dict[str, Any] = {
            "symbol": symbol.upper(),
            "periodType": period_type,
            "period": period,
            "frequencyType": frequency_type,
            "frequency": frequency,
        }

        if start_date:
            params["startDate"] = int(start_date.timestamp() * 1000)
        if end_date:
            params["endDate"] = int(end_date.timestamp() * 1000)

        try:
            response = self.http.get(
                url,
                headers=self._get_headers(),
                params=params,
            )

            if response.status_code != 200:
                raise SchwabMarketError(
                    f"Candles request failed: {response.status_code}"
                )

            data = response.json()
            candles_data = data.get("candles", [])

            candles = []
            for c in candles_data:
                candle_time = datetime.fromtimestamp(
                    c["datetime"] / 1000, tz=timezone.utc
                )
                candles.append(
                    Candle(
                        open=float(c["open"]),
                        high=float(c["high"]),
                        low=float(c["low"]),
                        close=float(c["close"]),
                        volume=int(c["volume"]),
                        timestamp=candle_time,
                    )
                )

            return candles

        except SchwabMarketError:
            raise
        except Exception as e:
            logger.error(f"Failed to get candles for {symbol}: {e}")
            raise SchwabMarketError(f"Candles request failed: {e}") from e

    def get_latest_candle(
        self, symbol: str, frequency: int = 1
    ) -> Candle | None:
        """
        Get the most recent candle for a symbol.

        Args:
            symbol: Stock symbol
            frequency: Candle frequency in minutes (1, 5, etc.)

        Returns:
            Most recent Candle or None if unavailable
        """
        candles = self.get_candles(
            symbol=symbol,
            period_type="day",
            period=1,
            frequency_type="minute",
            frequency=frequency,
        )
        return candles[-1] if candles else None

    def build_snapshot(
        self,
        symbol: str,
        include_candles: bool = True,
    ) -> MarketSnapshot:
        """
        Build a complete MarketSnapshot for a symbol.

        This is the primary method for Phase 2 market perception.
        One snapshot per symbol per call.

        Args:
            symbol: Stock symbol
            include_candles: Whether to fetch 1m and 5m candles

        Returns:
            Atomic MarketSnapshot with all available data
        """
        # Get quote data
        quote = self.get_quote(symbol)

        # Get candle data if requested
        candle_1m = None
        candle_5m = None

        if include_candles:
            try:
                candle_1m = self.get_latest_candle(symbol, frequency=1)
            except SchwabMarketError as e:
                logger.warning(f"Failed to get 1m candle for {symbol}: {e}")

            try:
                candle_5m = self.get_latest_candle(symbol, frequency=5)
            except SchwabMarketError as e:
                logger.warning(f"Failed to get 5m candle for {symbol}: {e}")

        # Build atomic snapshot
        return create_snapshot(
            symbol=symbol,
            bid=quote.bid_price,
            ask=quote.ask_price,
            last=quote.last_price,
            volume=quote.total_volume,
            timestamp=quote.quote_time,
            candle_1m=candle_1m,
            candle_5m=candle_5m,
            source="schwab",
            is_market_open=quote.is_market_open,
            is_tradeable=quote.is_tradeable,
        )

    def build_snapshots(
        self,
        symbols: list[str],
        include_candles: bool = True,
    ) -> dict[str, MarketSnapshot]:
        """
        Build snapshots for multiple symbols.

        Note: Events should be emitted per-symbol, not batched.

        Args:
            symbols: List of stock symbols
            include_candles: Whether to fetch candles

        Returns:
            Dict mapping symbol to MarketSnapshot
        """
        results = {}

        # Batch quote request
        quotes = self.get_quotes(symbols)

        # Build individual snapshots
        for symbol, quote in quotes.items():
            candle_1m = None
            candle_5m = None

            if include_candles:
                try:
                    candle_1m = self.get_latest_candle(symbol, frequency=1)
                except SchwabMarketError:
                    pass

                try:
                    candle_5m = self.get_latest_candle(symbol, frequency=5)
                except SchwabMarketError:
                    pass

            results[symbol] = create_snapshot(
                symbol=symbol,
                bid=quote.bid_price,
                ask=quote.ask_price,
                last=quote.last_price,
                volume=quote.total_volume,
                timestamp=quote.quote_time,
                candle_1m=candle_1m,
                candle_5m=candle_5m,
                source="schwab",
                is_market_open=quote.is_market_open,
                is_tradeable=quote.is_tradeable,
            )

        return results
