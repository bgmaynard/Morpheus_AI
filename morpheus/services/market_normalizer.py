"""Normalize Databento records into internal Morpheus format.

Converts raw Databento trade and MBP-10 records into standardized
dataclasses with UTC datetimes, normalized side labels, and consistent
field naming. These normalized objects are consumed by the momentum engine
and replay engine.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

import databento as db

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────
# Internal data types
# ──────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class NormalizedTrade:
    """A single trade event in internal format."""
    symbol: str
    timestamp: datetime          # UTC, from ts_recv
    price: float
    size: int
    side: str                    # "buy" | "sell" | "unknown"
    source: str = "databento"


@dataclass(frozen=True)
class NormalizedBookLevel:
    """One price level in a book snapshot."""
    bid_price: float
    ask_price: float
    bid_size: int
    ask_size: int
    bid_orders: int
    ask_orders: int


@dataclass(frozen=True)
class NormalizedBookSnapshot:
    """Full L2 book snapshot (up to 10 levels) in internal format."""
    symbol: str
    timestamp: datetime          # UTC, from ts_recv
    levels: tuple[NormalizedBookLevel, ...]
    source: str = "databento"

    @property
    def best_bid(self) -> float:
        return self.levels[0].bid_price if self.levels else 0.0

    @property
    def best_ask(self) -> float:
        return self.levels[0].ask_price if self.levels else 0.0

    @property
    def spread(self) -> float:
        return self.best_ask - self.best_bid

    @property
    def mid(self) -> float:
        return (self.best_bid + self.best_ask) / 2.0 if self.levels else 0.0

    @property
    def total_bid_size(self) -> int:
        return sum(lvl.bid_size for lvl in self.levels)

    @property
    def total_ask_size(self) -> int:
        return sum(lvl.ask_size for lvl in self.levels)


# ──────────────────────────────────────────────────────────────────────
# Timestamp conversion
# ──────────────────────────────────────────────────────────────────────

def _ns_to_datetime(ts_ns: int) -> datetime:
    """Convert nanosecond epoch timestamp to UTC datetime.

    Databento uses nanosecond precision. Python datetime supports microsecond
    precision, so we truncate the last 3 digits.
    """
    ts_seconds = ts_ns / 1_000_000_000
    return datetime.fromtimestamp(ts_seconds, tz=timezone.utc)


def _map_side(side_value) -> str:
    """Map Databento side field to internal 'buy'/'sell'/'unknown'.

    Databento trade records use 'A' (ask/sell aggressor) and 'B' (bid/buy aggressor),
    or string values like 'Ask', 'Bid'. MBP records may use 'A'/'B' single chars.
    """
    if side_value is None:
        return "unknown"

    s = str(side_value).upper().strip()

    if s in ("A", "ASK", "S", "SELL"):
        return "sell"
    if s in ("B", "BID", "BUY"):
        return "buy"
    return "unknown"


# ──────────────────────────────────────────────────────────────────────
# Normalization functions
# ──────────────────────────────────────────────────────────────────────

def normalize_trades(dbn_store: db.DBNStore) -> list[NormalizedTrade]:
    """Convert Databento trades DBNStore to list of NormalizedTrade.

    Processes all trade records, converting timestamps and mapping side fields.
    Filters out records with zero price or size.
    """
    trades: list[NormalizedTrade] = []
    df = dbn_store.to_df()

    if df.empty:
        logger.warning("No trade records in DBNStore")
        return trades

    for idx, row in df.iterrows():
        price = float(row.get("price", 0))
        size = int(row.get("size", 0))
        if price <= 0 or size <= 0:
            continue

        # ts_recv is the index in Databento DataFrames (DatetimeIndex in UTC)
        if hasattr(idx, 'timestamp'):
            # pandas Timestamp
            ts = idx.to_pydatetime()
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
        else:
            ts = datetime.now(timezone.utc)

        # Symbol comes from the 'symbol' column
        symbol = str(row.get("symbol", "UNKNOWN"))

        trades.append(NormalizedTrade(
            symbol=symbol,
            timestamp=ts,
            price=price,
            size=size,
            side=_map_side(row.get("side")),
        ))

    logger.info("Normalized %d trades from %d raw records", len(trades), len(df))
    return trades


def normalize_mbp10(dbn_store: db.DBNStore) -> list[NormalizedBookSnapshot]:
    """Convert Databento MBP-10 DBNStore to list of NormalizedBookSnapshot.

    Each record contains 10 bid/ask levels. Filters out records where
    the best bid/ask are both zero (empty book).
    """
    snapshots: list[NormalizedBookSnapshot] = []
    df = dbn_store.to_df()

    if df.empty:
        logger.warning("No MBP-10 records in DBNStore")
        return snapshots

    for idx, row in df.iterrows():
        # Timestamp from index
        if hasattr(idx, 'timestamp'):
            ts = idx.to_pydatetime()
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
        else:
            ts = datetime.now(timezone.utc)

        symbol = str(row.get("symbol", "UNKNOWN"))

        # Extract 10 levels: bid_px_00..bid_px_09, ask_px_00..ask_px_09, etc.
        levels = []
        for i in range(10):
            suffix = f"{i:02d}"
            bid_px = float(row.get(f"bid_px_{suffix}", 0))
            ask_px = float(row.get(f"ask_px_{suffix}", 0))
            bid_sz = int(row.get(f"bid_sz_{suffix}", 0))
            ask_sz = int(row.get(f"ask_sz_{suffix}", 0))
            bid_ct = int(row.get(f"bid_ct_{suffix}", 0))
            ask_ct = int(row.get(f"ask_ct_{suffix}", 0))

            levels.append(NormalizedBookLevel(
                bid_price=bid_px,
                ask_price=ask_px,
                bid_size=bid_sz,
                ask_size=ask_sz,
                bid_orders=bid_ct,
                ask_orders=ask_ct,
            ))

        # Skip empty book snapshots
        if levels[0].bid_price <= 0 and levels[0].ask_price <= 0:
            continue

        snapshots.append(NormalizedBookSnapshot(
            symbol=symbol,
            timestamp=ts,
            levels=tuple(levels),
        ))

    logger.info("Normalized %d book snapshots from %d raw records", len(snapshots), len(df))
    return snapshots
