"""
MarketSnapshot - Atomic, timestamped view of market state.

This module provides the core market data representation for Morpheus.
Snapshots are READ-ONLY and designed for consumption by later phases
without mutation.

Phase 2 Scope:
- Snapshot creation from Schwab data
- Event emission (MARKET_SNAPSHOT)
- No trade decisions, no state mutation
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from morpheus.core.events import Event, EventType, create_event


# Schema version for forward compatibility
SNAPSHOT_SCHEMA_VERSION = "1.0"


@dataclass(frozen=True)
class Candle:
    """
    OHLCV candle data.

    Immutable representation of a price bar.
    """

    open: float
    high: float
    low: float
    close: float
    volume: int
    timestamp: datetime  # Bar open time, UTC


@dataclass(frozen=True)
class MarketSnapshot:
    """
    Atomic, timestamped snapshot of market state for a single symbol.

    Design Principles:
    - One snapshot per symbol per cadence
    - Immutable (frozen dataclass)
    - Schema versioned for replay compatibility
    - Source of truth for market perception

    This is Phase 2's core output: clear perception, no cognition.
    """

    # Schema version (required for replay/backtest compatibility)
    schema_version: str = SNAPSHOT_SCHEMA_VERSION

    # Identity
    symbol: str = ""
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # Quote data
    bid: float = 0.0
    ask: float = 0.0
    last: float = 0.0
    volume: int = 0

    # Derived quote metrics
    spread: float = 0.0  # ask - bid
    spread_pct: float = 0.0  # spread / mid * 100
    mid: float = 0.0  # (bid + ask) / 2

    # Candle data (populated based on cadence)
    candle_1m: Candle | None = None
    candle_5m: Candle | None = None

    # Metadata
    source: str = "schwab"
    is_market_open: bool = True
    is_tradeable: bool = True  # False if halted, etc.

    def to_event_payload(self) -> dict[str, Any]:
        """Convert snapshot to event payload dict."""
        payload = {
            "schema_version": self.schema_version,
            "symbol": self.symbol,
            "timestamp": self.timestamp.isoformat(),
            "bid": self.bid,
            "ask": self.ask,
            "last": self.last,
            "volume": self.volume,
            "spread": self.spread,
            "spread_pct": self.spread_pct,
            "mid": self.mid,
            "source": self.source,
            "is_market_open": self.is_market_open,
            "is_tradeable": self.is_tradeable,
        }

        if self.candle_1m:
            payload["candle_1m"] = {
                "open": self.candle_1m.open,
                "high": self.candle_1m.high,
                "low": self.candle_1m.low,
                "close": self.candle_1m.close,
                "volume": self.candle_1m.volume,
                "timestamp": self.candle_1m.timestamp.isoformat(),
            }

        if self.candle_5m:
            payload["candle_5m"] = {
                "open": self.candle_5m.open,
                "high": self.candle_5m.high,
                "low": self.candle_5m.low,
                "close": self.candle_5m.close,
                "volume": self.candle_5m.volume,
                "timestamp": self.candle_5m.timestamp.isoformat(),
            }

        return payload

    def to_event(self) -> Event:
        """Create a MARKET_SNAPSHOT event from this snapshot."""
        return create_event(
            EventType.MARKET_SNAPSHOT,
            payload=self.to_event_payload(),
            symbol=self.symbol,
            timestamp=self.timestamp,
        )


def create_snapshot(
    symbol: str,
    bid: float,
    ask: float,
    last: float,
    volume: int,
    timestamp: datetime | None = None,
    candle_1m: Candle | None = None,
    candle_5m: Candle | None = None,
    source: str = "schwab",
    is_market_open: bool = True,
    is_tradeable: bool = True,
) -> MarketSnapshot:
    """
    Factory function to create a MarketSnapshot with computed fields.

    Computes spread and mid from bid/ask automatically.
    """
    ts = timestamp or datetime.now(timezone.utc)
    mid = (bid + ask) / 2 if (bid > 0 and ask > 0) else 0.0
    spread = ask - bid if (bid > 0 and ask > 0) else 0.0
    spread_pct = (spread / mid * 100) if mid > 0 else 0.0

    return MarketSnapshot(
        schema_version=SNAPSHOT_SCHEMA_VERSION,
        symbol=symbol,
        timestamp=ts,
        bid=bid,
        ask=ask,
        last=last,
        volume=volume,
        spread=spread,
        spread_pct=spread_pct,
        mid=mid,
        candle_1m=candle_1m,
        candle_5m=candle_5m,
        source=source,
        is_market_open=is_market_open,
        is_tradeable=is_tradeable,
    )


def candle_from_dict(data: dict[str, Any]) -> Candle:
    """Create a Candle from a dictionary (e.g., from Schwab API response)."""
    return Candle(
        open=float(data.get("open", 0.0)),
        high=float(data.get("high", 0.0)),
        low=float(data.get("low", 0.0)),
        close=float(data.get("close", 0.0)),
        volume=int(data.get("volume", 0)),
        timestamp=datetime.fromisoformat(data["timestamp"])
        if isinstance(data.get("timestamp"), str)
        else data.get("timestamp", datetime.now(timezone.utc)),
    )


def snapshot_from_event_payload(payload: dict[str, Any]) -> MarketSnapshot:
    """Reconstruct a MarketSnapshot from an event payload (for replay)."""
    candle_1m = None
    candle_5m = None

    if "candle_1m" in payload and payload["candle_1m"]:
        candle_1m = candle_from_dict(payload["candle_1m"])

    if "candle_5m" in payload and payload["candle_5m"]:
        candle_5m = candle_from_dict(payload["candle_5m"])

    return MarketSnapshot(
        schema_version=payload.get("schema_version", "1.0"),
        symbol=payload.get("symbol", ""),
        timestamp=datetime.fromisoformat(payload["timestamp"])
        if isinstance(payload.get("timestamp"), str)
        else payload.get("timestamp", datetime.now(timezone.utc)),
        bid=float(payload.get("bid", 0.0)),
        ask=float(payload.get("ask", 0.0)),
        last=float(payload.get("last", 0.0)),
        volume=int(payload.get("volume", 0)),
        spread=float(payload.get("spread", 0.0)),
        spread_pct=float(payload.get("spread_pct", 0.0)),
        mid=float(payload.get("mid", 0.0)),
        candle_1m=candle_1m,
        candle_5m=candle_5m,
        source=payload.get("source", "schwab"),
        is_market_open=payload.get("is_market_open", True),
        is_tradeable=payload.get("is_tradeable", True),
    )
