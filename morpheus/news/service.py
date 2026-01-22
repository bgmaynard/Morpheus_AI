"""
News Service - Multi-provider news aggregation and catalyst detection.

Supports multiple news providers:
- Benzinga (primary for trading news)
- Polygon.io (alternative)
- Alpha Vantage (free tier)
- SEC EDGAR (filings)

Provides:
- Real-time news monitoring
- Catalyst classification
- Sentiment analysis (basic)
- News-to-symbol correlation
"""

from __future__ import annotations

import asyncio
import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, Callable, Awaitable

import httpx

logger = logging.getLogger(__name__)


class CatalystType(str, Enum):
    """Types of news catalysts."""
    EARNINGS = "earnings"
    FDA = "fda"
    CONTRACT = "contract"
    PARTNERSHIP = "partnership"
    UPGRADE = "upgrade"
    DOWNGRADE = "downgrade"
    OFFERING = "offering"
    BUYBACK = "buyback"
    INSIDER = "insider"
    SEC_FILING = "sec_filing"
    LAWSUIT = "lawsuit"
    MERGER = "merger"
    PRODUCT = "product"
    GENERAL = "general"
    UNKNOWN = "unknown"


class Sentiment(str, Enum):
    """News sentiment."""
    BULLISH = "bullish"
    BEARISH = "bearish"
    NEUTRAL = "neutral"
    UNKNOWN = "unknown"


@dataclass
class NewsItem:
    """A single news item."""

    headline: str
    symbol: str | None
    published_at: datetime
    source: str

    # Content
    summary: str | None = None
    url: str | None = None

    # Classification
    catalyst_type: CatalystType = CatalystType.UNKNOWN
    sentiment: Sentiment = Sentiment.UNKNOWN
    importance: int = 1  # 1-5, higher = more important

    # Metadata
    provider: str = ""
    raw_data: dict = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "headline": self.headline,
            "symbol": self.symbol,
            "published_at": self.published_at.isoformat(),
            "source": self.source,
            "summary": self.summary,
            "url": self.url,
            "catalyst_type": self.catalyst_type.value,
            "sentiment": self.sentiment.value,
            "importance": self.importance,
            "provider": self.provider,
        }


@dataclass
class NewsCatalyst:
    """A detected catalyst for a symbol."""

    symbol: str
    catalyst_type: CatalystType
    sentiment: Sentiment
    headline: str
    published_at: datetime
    importance: int

    # Related news items
    news_items: list[NewsItem] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "catalyst_type": self.catalyst_type.value,
            "sentiment": self.sentiment.value,
            "headline": self.headline,
            "published_at": self.published_at.isoformat(),
            "importance": self.importance,
            "news_count": len(self.news_items),
        }


# =============================================================================
# News Providers (Abstract)
# =============================================================================


class NewsProvider(ABC):
    """Abstract base for news providers."""

    @abstractmethod
    async def fetch_news(
        self,
        symbols: list[str] | None = None,
        since: datetime | None = None,
    ) -> list[NewsItem]:
        """Fetch news items."""
        pass

    @abstractmethod
    async def health_check(self) -> bool:
        """Check if provider is available."""
        pass


class BenzingaProvider(NewsProvider):
    """Benzinga news provider."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.benzinga.com/api/v2"
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=30.0)
        return self._client

    async def fetch_news(
        self,
        symbols: list[str] | None = None,
        since: datetime | None = None,
    ) -> list[NewsItem]:
        """Fetch news from Benzinga."""
        try:
            client = await self._get_client()

            params = {
                "token": self.api_key,
                "pageSize": 50,
            }

            if symbols:
                params["tickers"] = ",".join(symbols)

            if since:
                params["dateFrom"] = since.strftime("%Y-%m-%d")

            response = await client.get(
                f"{self.base_url}/news",
                params=params,
            )
            response.raise_for_status()

            data = response.json()
            items = []

            for article in data:
                item = self._parse_article(article)
                if item:
                    items.append(item)

            return items

        except Exception as e:
            logger.error(f"Benzinga fetch error: {e}")
            return []

    def _parse_article(self, article: dict) -> NewsItem | None:
        """Parse a Benzinga article."""
        try:
            # Get primary ticker
            tickers = article.get("stocks", [])
            symbol = tickers[0].get("name") if tickers else None

            published = datetime.fromisoformat(
                article.get("created", "").replace("Z", "+00:00")
            )

            headline = article.get("title", "")

            return NewsItem(
                headline=headline,
                symbol=symbol,
                published_at=published,
                source="Benzinga",
                summary=article.get("teaser"),
                url=article.get("url"),
                catalyst_type=self._classify_catalyst(headline),
                sentiment=self._analyze_sentiment(headline),
                importance=self._calculate_importance(article),
                provider="benzinga",
                raw_data=article,
            )
        except Exception as e:
            logger.debug(f"Error parsing Benzinga article: {e}")
            return None

    def _classify_catalyst(self, headline: str) -> CatalystType:
        """Classify catalyst type from headline."""
        headline_lower = headline.lower()

        patterns = {
            CatalystType.EARNINGS: [r"earnings", r"eps", r"revenue", r"quarterly results"],
            CatalystType.FDA: [r"fda", r"approval", r"clinical trial", r"phase \d"],
            CatalystType.CONTRACT: [r"contract", r"awarded", r"wins deal"],
            CatalystType.PARTNERSHIP: [r"partnership", r"collaboration", r"joint venture"],
            CatalystType.UPGRADE: [r"upgrade", r"raises price target", r"buy rating"],
            CatalystType.DOWNGRADE: [r"downgrade", r"lowers price target", r"sell rating"],
            CatalystType.OFFERING: [r"offering", r"raises \$", r"secondary"],
            CatalystType.BUYBACK: [r"buyback", r"repurchase"],
            CatalystType.MERGER: [r"merger", r"acquisition", r"takeover", r"acquire"],
            CatalystType.PRODUCT: [r"launches", r"new product", r"release"],
            CatalystType.LAWSUIT: [r"lawsuit", r"sued", r"litigation", r"settlement"],
        }

        for catalyst_type, pattern_list in patterns.items():
            for pattern in pattern_list:
                if re.search(pattern, headline_lower):
                    return catalyst_type

        return CatalystType.GENERAL

    def _analyze_sentiment(self, headline: str) -> Sentiment:
        """Basic sentiment analysis from headline."""
        headline_lower = headline.lower()

        bullish_words = [
            "surge", "soar", "jump", "rally", "gain", "upgrade", "beat",
            "approval", "win", "strong", "record", "breakthrough", "bullish",
        ]
        bearish_words = [
            "plunge", "crash", "drop", "fall", "decline", "downgrade", "miss",
            "reject", "fail", "weak", "concern", "warning", "bearish",
        ]

        bullish_count = sum(1 for word in bullish_words if word in headline_lower)
        bearish_count = sum(1 for word in bearish_words if word in headline_lower)

        if bullish_count > bearish_count:
            return Sentiment.BULLISH
        elif bearish_count > bullish_count:
            return Sentiment.BEARISH
        else:
            return Sentiment.NEUTRAL

    def _calculate_importance(self, article: dict) -> int:
        """Calculate importance score 1-5."""
        score = 2  # Default

        # Check channels/categories
        channels = article.get("channels", [])
        important_channels = ["earnings", "fda", "mergers", "movers"]
        if any(ch.get("name", "").lower() in important_channels for ch in channels):
            score += 1

        # Check if major news outlet
        author = article.get("author", "").lower()
        if any(x in author for x in ["reuters", "bloomberg", "wsj"]):
            score += 1

        return min(5, score)

    async def health_check(self) -> bool:
        """Check if Benzinga API is accessible."""
        try:
            client = await self._get_client()
            response = await client.get(
                f"{self.base_url}/news",
                params={"token": self.api_key, "pageSize": 1},
            )
            return response.status_code == 200
        except Exception:
            return False


class PolygonNewsProvider(NewsProvider):
    """Polygon.io news provider."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.polygon.io/v2"
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=30.0)
        return self._client

    async def fetch_news(
        self,
        symbols: list[str] | None = None,
        since: datetime | None = None,
    ) -> list[NewsItem]:
        """Fetch news from Polygon."""
        try:
            client = await self._get_client()

            params = {
                "apiKey": self.api_key,
                "limit": 50,
                "order": "desc",
            }

            if symbols:
                params["ticker"] = symbols[0]  # Polygon only supports one at a time

            if since:
                params["published_utc.gte"] = since.isoformat()

            response = await client.get(
                f"{self.base_url}/reference/news",
                params=params,
            )
            response.raise_for_status()

            data = response.json()
            items = []

            for article in data.get("results", []):
                item = self._parse_article(article)
                if item:
                    items.append(item)

            return items

        except Exception as e:
            logger.error(f"Polygon news fetch error: {e}")
            return []

    def _parse_article(self, article: dict) -> NewsItem | None:
        """Parse a Polygon article."""
        try:
            tickers = article.get("tickers", [])
            symbol = tickers[0] if tickers else None

            published = datetime.fromisoformat(
                article.get("published_utc", "").replace("Z", "+00:00")
            )

            headline = article.get("title", "")

            # Reuse Benzinga's classification logic
            benzinga = BenzingaProvider("")
            catalyst = benzinga._classify_catalyst(headline)
            sentiment = benzinga._analyze_sentiment(headline)

            return NewsItem(
                headline=headline,
                symbol=symbol,
                published_at=published,
                source=article.get("publisher", {}).get("name", "Unknown"),
                summary=article.get("description"),
                url=article.get("article_url"),
                catalyst_type=catalyst,
                sentiment=sentiment,
                importance=2,
                provider="polygon",
                raw_data=article,
            )
        except Exception as e:
            logger.debug(f"Error parsing Polygon article: {e}")
            return None

    async def health_check(self) -> bool:
        """Check if Polygon API is accessible."""
        try:
            client = await self._get_client()
            response = await client.get(
                f"{self.base_url}/reference/news",
                params={"apiKey": self.api_key, "limit": 1},
            )
            return response.status_code == 200
        except Exception:
            return False


# =============================================================================
# News Service
# =============================================================================


class NewsService:
    """
    Central news service for catalyst awareness.

    Usage:
        service = NewsService()
        service.add_provider(BenzingaProvider(api_key))

        # Start monitoring
        await service.start()

        # Query news
        catalysts = service.get_catalysts("AAPL")
    """

    def __init__(
        self,
        on_catalyst: Callable[[NewsCatalyst], Awaitable[None]] | None = None,
        poll_interval_seconds: float = 60.0,
    ):
        """
        Initialize news service.

        Args:
            on_catalyst: Callback when new catalyst detected
            poll_interval_seconds: How often to poll for news
        """
        self._on_catalyst = on_catalyst
        self._poll_interval = poll_interval_seconds

        self._providers: list[NewsProvider] = []
        self._running = False
        self._poll_task: asyncio.Task | None = None

        # News cache
        self._news_cache: dict[str, list[NewsItem]] = {}  # symbol -> news
        self._catalysts: dict[str, NewsCatalyst] = {}  # symbol -> latest catalyst
        self._seen_headlines: set[str] = set()

        # Watchlist for targeted monitoring
        self._watchlist: set[str] = set()

        logger.info("News service initialized")

    def add_provider(self, provider: NewsProvider) -> None:
        """Add a news provider."""
        self._providers.append(provider)
        logger.info(f"Added news provider: {provider.__class__.__name__}")

    def set_watchlist(self, symbols: list[str]) -> None:
        """Set symbols to monitor for news."""
        self._watchlist = set(s.upper() for s in symbols)
        logger.info(f"News watchlist set: {len(self._watchlist)} symbols")

    def add_to_watchlist(self, symbol: str) -> None:
        """Add a symbol to the news watchlist."""
        self._watchlist.add(symbol.upper())

    def remove_from_watchlist(self, symbol: str) -> None:
        """Remove a symbol from the news watchlist."""
        self._watchlist.discard(symbol.upper())

    async def start(self) -> None:
        """Start the news polling loop."""
        if self._running:
            return

        self._running = True
        self._poll_task = asyncio.create_task(self._poll_loop())
        logger.info("News service started")

    async def stop(self) -> None:
        """Stop the news polling loop."""
        self._running = False
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
        logger.info("News service stopped")

    async def _poll_loop(self) -> None:
        """Background loop to poll for news."""
        while self._running:
            try:
                await self._fetch_all_news()
                await asyncio.sleep(self._poll_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"News poll error: {e}")
                await asyncio.sleep(10.0)

    async def _fetch_all_news(self) -> None:
        """Fetch news from all providers."""
        since = datetime.now(timezone.utc) - timedelta(hours=4)

        for provider in self._providers:
            try:
                # Fetch for watchlist if set, otherwise general
                symbols = list(self._watchlist) if self._watchlist else None
                items = await provider.fetch_news(symbols=symbols, since=since)

                for item in items:
                    await self._process_news_item(item)

            except Exception as e:
                logger.error(f"Error fetching from {provider.__class__.__name__}: {e}")

    async def _process_news_item(self, item: NewsItem) -> None:
        """Process a single news item."""
        # Dedup by headline
        headline_key = item.headline[:100].lower()
        if headline_key in self._seen_headlines:
            return
        self._seen_headlines.add(headline_key)

        # Add to cache
        if item.symbol:
            if item.symbol not in self._news_cache:
                self._news_cache[item.symbol] = []
            self._news_cache[item.symbol].append(item)

            # Keep cache size reasonable
            if len(self._news_cache[item.symbol]) > 50:
                self._news_cache[item.symbol] = self._news_cache[item.symbol][-50:]

        # Check for significant catalyst
        if item.importance >= 3 or item.catalyst_type != CatalystType.GENERAL:
            await self._emit_catalyst(item)

    async def _emit_catalyst(self, item: NewsItem) -> None:
        """Emit a catalyst detection."""
        if not item.symbol:
            return

        catalyst = NewsCatalyst(
            symbol=item.symbol,
            catalyst_type=item.catalyst_type,
            sentiment=item.sentiment,
            headline=item.headline,
            published_at=item.published_at,
            importance=item.importance,
            news_items=[item],
        )

        self._catalysts[item.symbol] = catalyst

        logger.info(
            f"[NEWS] Catalyst: {item.symbol} | {item.catalyst_type.value} | "
            f"{item.sentiment.value} | {item.headline[:60]}..."
        )

        if self._on_catalyst:
            await self._on_catalyst(catalyst)

    # =========================================================================
    # Query methods
    # =========================================================================

    def get_news(self, symbol: str, limit: int = 10) -> list[NewsItem]:
        """Get recent news for a symbol."""
        items = self._news_cache.get(symbol.upper(), [])
        return items[-limit:]

    def get_catalyst(self, symbol: str) -> NewsCatalyst | None:
        """Get latest catalyst for a symbol."""
        return self._catalysts.get(symbol.upper())

    def has_recent_catalyst(
        self,
        symbol: str,
        max_age_minutes: int = 60,
        catalyst_types: list[CatalystType] | None = None,
    ) -> bool:
        """Check if symbol has a recent catalyst."""
        catalyst = self._catalysts.get(symbol.upper())
        if not catalyst:
            return False

        age = datetime.now(timezone.utc) - catalyst.published_at
        if age > timedelta(minutes=max_age_minutes):
            return False

        if catalyst_types and catalyst.catalyst_type not in catalyst_types:
            return False

        return True

    def get_all_catalysts(self) -> list[NewsCatalyst]:
        """Get all current catalysts."""
        return list(self._catalysts.values())

    def get_stats(self) -> dict[str, Any]:
        """Get service statistics."""
        return {
            "running": self._running,
            "providers": len(self._providers),
            "watchlist_size": len(self._watchlist),
            "cached_symbols": len(self._news_cache),
            "active_catalysts": len(self._catalysts),
            "headlines_seen": len(self._seen_headlines),
        }

    async def fetch_now(self, symbol: str) -> list[NewsItem]:
        """Fetch news for a symbol immediately."""
        items = []
        since = datetime.now(timezone.utc) - timedelta(hours=24)

        for provider in self._providers:
            try:
                fetched = await provider.fetch_news(symbols=[symbol], since=since)
                items.extend(fetched)
            except Exception as e:
                logger.error(f"Error fetching {symbol} news: {e}")

        return items

    def clear_cache(self) -> None:
        """Clear the news cache."""
        self._news_cache.clear()
        self._catalysts.clear()
        self._seen_headlines.clear()
        logger.info("News cache cleared")
