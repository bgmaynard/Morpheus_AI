"""
Candle Aggregator - Build OHLCV candles from streaming quotes.

Aggregates real-time quote updates into minute bars that can be
fed to the signal pipeline for strategy evaluation.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Awaitable

from morpheus.features.indicators import OHLCV

logger = logging.getLogger(__name__)


@dataclass
class CandleBar:
    """Accumulating candle bar."""

    open: float = 0.0
    high: float = 0.0
    low: float = float('inf')
    close: float = 0.0
    volume: int = 0
    minute_key: str = ""  # "YYYY-MM-DD HH:MM" for deduplication
    tick_count: int = 0

    def update(self, price: float, volume: int = 0) -> None:
        """Update bar with new tick."""
        if self.tick_count == 0:
            # First tick of the bar
            self.open = price
            self.high = price
            self.low = price
        else:
            self.high = max(self.high, price)
            self.low = min(self.low, price)

        self.close = price
        self.volume += volume
        self.tick_count += 1

    def to_ohlcv(self) -> OHLCV:
        """Convert to OHLCV for pipeline."""
        return OHLCV(
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
        )

    def is_valid(self) -> bool:
        """Check if bar has valid data."""
        return self.tick_count > 0 and self.low != float('inf')


class CandleAggregator:
    """
    Aggregates streaming quotes into 1-minute OHLCV candles.

    Usage:
        aggregator = CandleAggregator(on_candle=my_callback)

        # Feed quotes as they arrive
        await aggregator.on_quote("AAPL", 150.25, volume=100)

        # When minute ends, on_candle callback is invoked with completed bar
    """

    def __init__(
        self,
        on_candle: Callable[[str, OHLCV, datetime], Awaitable[None]] | None = None,
        interval_seconds: int = 60,
    ):
        """
        Initialize aggregator.

        Args:
            on_candle: Async callback(symbol, ohlcv, timestamp) when candle completes
            interval_seconds: Candle interval in seconds (default 60 = 1 minute)
        """
        self._on_candle = on_candle
        self._interval = interval_seconds

        # Current accumulating bars per symbol
        self._bars: dict[str, CandleBar] = {}

        # Stats
        self._candles_emitted: int = 0
        self._quotes_processed: int = 0

    def _get_minute_key(self, ts: datetime) -> str:
        """Get minute key for timestamp (truncated to interval)."""
        # Truncate to interval boundary
        ts_utc = ts.astimezone(timezone.utc) if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
        seconds = int(ts_utc.timestamp())
        truncated = seconds - (seconds % self._interval)
        return str(truncated)

    def _get_minute_timestamp(self, minute_key: str) -> datetime:
        """Convert minute key back to datetime."""
        return datetime.fromtimestamp(int(minute_key), tz=timezone.utc)

    async def on_quote(
        self,
        symbol: str,
        price: float,
        volume: int = 0,
        timestamp: datetime | None = None,
    ) -> None:
        """
        Process a quote update.

        Args:
            symbol: Stock symbol
            price: Trade/quote price
            volume: Volume delta (if available)
            timestamp: Quote timestamp (defaults to now)
        """
        symbol = symbol.upper()
        ts = timestamp or datetime.now(timezone.utc)
        minute_key = self._get_minute_key(ts)

        self._quotes_processed += 1

        # Get or create bar for this symbol
        current_bar = self._bars.get(symbol)

        if current_bar is None or current_bar.minute_key != minute_key:
            # New minute - emit previous bar if valid
            if current_bar is not None and current_bar.is_valid():
                await self._emit_candle(symbol, current_bar)

            # Start new bar
            current_bar = CandleBar(minute_key=minute_key)
            self._bars[symbol] = current_bar

        # Update current bar
        current_bar.update(price, volume)

    async def _emit_candle(self, symbol: str, bar: CandleBar) -> None:
        """Emit a completed candle."""
        if self._on_candle is None:
            return

        ohlcv = bar.to_ohlcv()
        timestamp = self._get_minute_timestamp(bar.minute_key)

        try:
            await self._on_candle(symbol, ohlcv, timestamp)
            self._candles_emitted += 1

            logger.debug(
                f"[AGGREGATOR] {symbol} candle: "
                f"O={ohlcv.open:.2f} H={ohlcv.high:.2f} L={ohlcv.low:.2f} C={ohlcv.close:.2f} "
                f"V={ohlcv.volume} ticks={bar.tick_count}"
            )
        except Exception as e:
            logger.error(f"[AGGREGATOR] Error emitting candle for {symbol}: {e}")

    async def flush(self, symbol: str | None = None) -> None:
        """
        Force emit current bars (e.g., at market close).

        Args:
            symbol: Specific symbol to flush, or None for all
        """
        if symbol:
            bar = self._bars.get(symbol.upper())
            if bar and bar.is_valid():
                await self._emit_candle(symbol.upper(), bar)
                del self._bars[symbol.upper()]
        else:
            for sym in list(self._bars.keys()):
                bar = self._bars[sym]
                if bar.is_valid():
                    await self._emit_candle(sym, bar)
            self._bars.clear()

    def get_stats(self) -> dict:
        """Get aggregator statistics."""
        return {
            "quotes_processed": self._quotes_processed,
            "candles_emitted": self._candles_emitted,
            "active_bars": len(self._bars),
            "symbols": list(self._bars.keys()),
        }

    def get_current_bar(self, symbol: str) -> CandleBar | None:
        """Get current accumulating bar for a symbol."""
        return self._bars.get(symbol.upper())
