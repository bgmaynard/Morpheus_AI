"""Databento API client with cost controls and disk caching.

Wraps the databento SDK to provide:
- Historical trades + MBP-10 data fetching
- Per-request and daily cost guards
- Disk caching of .dbn files under MORPHEUS_DATA_ROOT
- Cost estimation before downloads
"""

from __future__ import annotations

import hashlib
import logging
import os
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

import databento as db

logger = logging.getLogger(__name__)


def _default_cache_dir() -> str:
    base = os.getenv("MORPHEUS_DATA_ROOT", "D:/AI_BOT_DATA")
    return f"{base}/databento_cache"


@dataclass
class DatabentoConfig:
    """Configuration for the Databento client."""
    max_cost_per_request: float = 5.0
    max_daily_spend: float = 25.0
    default_dataset: str = "XNAS.ITCH"
    cache_dir: str = field(default_factory=_default_cache_dir)


class CostLimitExceeded(Exception):
    """Raised when a request would exceed cost limits."""


class DatabentoClient:
    """Databento Historical API wrapper with cost controls.

    Usage:
        client = DatabentoClient()
        cost = client.check_cost(["AAPL"], "trades", "2026-02-06", "2026-02-07")
        data = client.get_trades_cached(["AAPL"], "2026-02-06", "2026-02-07")
        df = data.to_df()
    """

    def __init__(self, config: DatabentoConfig | None = None):
        api_key = os.environ.get("DATABENTO_API_KEY")
        if not api_key:
            raise ValueError("DATABENTO_API_KEY environment variable not set")

        self._client = db.Historical(key=api_key)
        self._config = config or DatabentoConfig()
        # Use config cache_dir if set, otherwise fall back to env-based default
        cache_dir = self._config.cache_dir or _default_cache_dir()
        self._cache_dir = Path(cache_dir)
        self._cache_dir.mkdir(parents=True, exist_ok=True)

        # Daily spend tracking (resets on new day)
        self._daily_spend: float = 0.0
        self._spend_date: date = datetime.now(timezone.utc).date()

        logger.info(
            "DatabentoClient initialized | dataset=%s | cache=%s | daily_limit=$%.2f",
            self._config.default_dataset,
            self._cache_dir,
            self._config.max_daily_spend,
        )

    def _reset_daily_spend_if_needed(self) -> None:
        today = datetime.now(timezone.utc).date()
        if today != self._spend_date:
            logger.info("Daily spend reset: $%.2f (yesterday) -> $0.00", self._daily_spend)
            self._daily_spend = 0.0
            self._spend_date = today

    def check_cost(
        self,
        symbols: list[str],
        schema: str,
        start: str,
        end: str,
        dataset: str | None = None,
    ) -> float:
        """Estimate cost for a request without fetching data.

        Returns estimated cost in USD. Raises CostLimitExceeded if over limits.
        """
        ds = dataset or self._config.default_dataset
        cost = self._client.metadata.get_cost(
            dataset=ds,
            symbols=symbols,
            schema=schema,
            start=start,
            end=end,
        )

        if cost > self._config.max_cost_per_request:
            raise CostLimitExceeded(
                f"Request cost ${cost:.2f} exceeds per-request limit "
                f"${self._config.max_cost_per_request:.2f}"
            )

        self._reset_daily_spend_if_needed()
        if self._daily_spend + cost > self._config.max_daily_spend:
            raise CostLimitExceeded(
                f"Request cost ${cost:.2f} would push daily total to "
                f"${self._daily_spend + cost:.2f}, exceeding limit "
                f"${self._config.max_daily_spend:.2f}"
            )

        logger.info(
            "Cost estimate: $%.4f | symbols=%s schema=%s | daily_total=$%.2f",
            cost, symbols, schema, self._daily_spend,
        )
        return cost

    def _cache_path(
        self,
        dataset: str,
        schema: str,
        symbols: list[str],
        start: str,
        end: str,
    ) -> Path:
        """Compute deterministic cache file path for a request."""
        # Hash the symbols list for stable filename
        sym_key = "_".join(sorted(symbols))
        if len(sym_key) > 100:
            sym_key = hashlib.md5(sym_key.encode()).hexdigest()[:16]

        cache_subdir = self._cache_dir / dataset / schema
        cache_subdir.mkdir(parents=True, exist_ok=True)
        # Sanitize colons from timestamps — invalid in Windows filenames
        safe_start = start.replace(":", "")
        safe_end = end.replace(":", "")
        return cache_subdir / f"{sym_key}_{safe_start}_{safe_end}.dbn.zst"

    def _fetch_and_cache(
        self,
        symbols: list[str],
        schema: str,
        start: str,
        end: str,
        dataset: str,
    ) -> db.DBNStore:
        """Fetch data from Databento API and save to disk cache."""
        # Cost check first
        cost = self.check_cost(symbols, schema, start, end, dataset)

        logger.info(
            "Fetching from Databento: %s %s | %s to %s | est $%.4f",
            schema, symbols, start, end, cost,
        )

        data = self._client.timeseries.get_range(
            dataset=dataset,
            symbols=symbols,
            schema=schema,
            start=start,
            end=end,
        )

        # Track spend
        self._daily_spend += cost
        logger.info("Daily spend: $%.2f / $%.2f", self._daily_spend, self._config.max_daily_spend)

        # Cache to disk
        cache_file = self._cache_path(dataset, schema, symbols, start, end)
        data.to_file(str(cache_file))
        logger.info("Cached: %s (%.1f KB)", cache_file, cache_file.stat().st_size / 1024)

        return data

    def get_trades(
        self,
        symbols: list[str],
        start: str,
        end: str,
        dataset: str | None = None,
    ) -> db.DBNStore:
        """Fetch trades data (no caching). Always hits API."""
        ds = dataset or self._config.default_dataset
        cost = self.check_cost(symbols, "trades", start, end, ds)

        data = self._client.timeseries.get_range(
            dataset=ds,
            symbols=symbols,
            schema="trades",
            start=start,
            end=end,
        )
        self._daily_spend += cost
        return data

    def get_mbp10(
        self,
        symbols: list[str],
        start: str,
        end: str,
        dataset: str | None = None,
    ) -> db.DBNStore:
        """Fetch MBP-10 (10-level book) data (no caching). Always hits API."""
        ds = dataset or self._config.default_dataset
        cost = self.check_cost(symbols, "mbp-10", start, end, ds)

        data = self._client.timeseries.get_range(
            dataset=ds,
            symbols=symbols,
            schema="mbp-10",
            start=start,
            end=end,
        )
        self._daily_spend += cost
        return data

    def get_trades_cached(
        self,
        symbols: list[str],
        start: str,
        end: str,
        dataset: str | None = None,
    ) -> db.DBNStore:
        """Fetch trades data with disk caching. Returns cached if available."""
        ds = dataset or self._config.default_dataset
        cache_file = self._cache_path(ds, "trades", symbols, start, end)

        if cache_file.exists():
            logger.info("Cache hit: %s", cache_file)
            return db.DBNStore.from_file(str(cache_file))

        return self._fetch_and_cache(symbols, "trades", start, end, ds)

    def get_mbp10_cached(
        self,
        symbols: list[str],
        start: str,
        end: str,
        dataset: str | None = None,
    ) -> db.DBNStore:
        """Fetch MBP-10 data with disk caching. Returns cached if available."""
        ds = dataset or self._config.default_dataset
        cache_file = self._cache_path(ds, "mbp-10", symbols, start, end)

        if cache_file.exists():
            logger.info("Cache hit: %s", cache_file)
            return db.DBNStore.from_file(str(cache_file))

        return self._fetch_and_cache(symbols, "mbp-10", start, end, ds)

    @property
    def daily_spend(self) -> float:
        self._reset_daily_spend_if_needed()
        return self._daily_spend

    @property
    def daily_remaining(self) -> float:
        self._reset_daily_spend_if_needed()
        return max(0.0, self._config.max_daily_spend - self._daily_spend)

    def status(self) -> dict:
        """Return client status for API endpoint."""
        self._reset_daily_spend_if_needed()
        return {
            "dataset": self._config.default_dataset,
            "cache_dir": str(self._cache_dir),
            "daily_spend": round(self._daily_spend, 4),
            "daily_limit": self._config.max_daily_spend,
            "daily_remaining": round(self.daily_remaining, 4),
            "per_request_limit": self._config.max_cost_per_request,
        }
