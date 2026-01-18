"""
Technical Indicators for Feature Engineering.

Pure functions that compute technical indicators from price/volume data.
All functions are DETERMINISTIC: same input → same output.

Phase 3 Scope:
- Indicator computation only
- No trade decisions
- No state mutation
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence
import math


@dataclass(frozen=True)
class OHLCV:
    """Single OHLCV bar for indicator calculations."""

    open: float
    high: float
    low: float
    close: float
    volume: int


def sma(values: Sequence[float], period: int) -> float | None:
    """
    Simple Moving Average.

    Args:
        values: Sequence of values (most recent last)
        period: Number of periods

    Returns:
        SMA value or None if insufficient data
    """
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


def ema(values: Sequence[float], period: int) -> float | None:
    """
    Exponential Moving Average.

    Uses standard EMA formula with multiplier = 2 / (period + 1).

    Args:
        values: Sequence of values (most recent last)
        period: Number of periods

    Returns:
        EMA value or None if insufficient data
    """
    if len(values) < period:
        return None

    # Initialize with SMA of first 'period' values
    multiplier = 2.0 / (period + 1)
    ema_value = sum(values[:period]) / period

    # Apply EMA formula for remaining values
    for value in values[period:]:
        ema_value = (value - ema_value) * multiplier + ema_value

    return ema_value


def rsi(closes: Sequence[float], period: int = 14) -> float | None:
    """
    Relative Strength Index.

    Args:
        closes: Sequence of closing prices (most recent last)
        period: RSI period (default 14)

    Returns:
        RSI value (0-100) or None if insufficient data
    """
    if len(closes) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(closes)):
        change = closes[i] - closes[i - 1]
        if change > 0:
            gains.append(change)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(abs(change))

    if len(gains) < period:
        return None

    # Initial averages
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    # Smoothed averages for remaining periods
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def true_range(bar: OHLCV, prev_close: float | None = None) -> float:
    """
    True Range for a single bar.

    TR = max(high - low, |high - prev_close|, |low - prev_close|)

    Args:
        bar: Current OHLCV bar
        prev_close: Previous bar's close (optional)

    Returns:
        True range value
    """
    hl = bar.high - bar.low

    if prev_close is None:
        return hl

    hc = abs(bar.high - prev_close)
    lc = abs(bar.low - prev_close)

    return max(hl, hc, lc)


def atr(bars: Sequence[OHLCV], period: int = 14) -> float | None:
    """
    Average True Range.

    Args:
        bars: Sequence of OHLCV bars (most recent last)
        period: ATR period (default 14)

    Returns:
        ATR value or None if insufficient data
    """
    if len(bars) < period + 1:
        return None

    # Calculate true ranges
    tr_values = [true_range(bars[0])]
    for i in range(1, len(bars)):
        tr_values.append(true_range(bars[i], bars[i - 1].close))

    # Calculate ATR using Wilder's smoothing
    atr_value = sum(tr_values[:period]) / period

    for i in range(period, len(tr_values)):
        atr_value = (atr_value * (period - 1) + tr_values[i]) / period

    return atr_value


def atr_percent(bars: Sequence[OHLCV], period: int = 14) -> float | None:
    """
    ATR as percentage of current price.

    Args:
        bars: Sequence of OHLCV bars (most recent last)
        period: ATR period (default 14)

    Returns:
        ATR percentage or None if insufficient data
    """
    atr_val = atr(bars, period)
    if atr_val is None or len(bars) == 0:
        return None

    current_price = bars[-1].close
    if current_price == 0:
        return None

    return (atr_val / current_price) * 100


def momentum(closes: Sequence[float], period: int = 10) -> float | None:
    """
    Price Momentum (current price - price N periods ago).

    Args:
        closes: Sequence of closing prices (most recent last)
        period: Lookback period

    Returns:
        Momentum value or None if insufficient data
    """
    if len(closes) < period + 1:
        return None

    return closes[-1] - closes[-(period + 1)]


def rate_of_change(closes: Sequence[float], period: int = 10) -> float | None:
    """
    Rate of Change (percentage change over N periods).

    Args:
        closes: Sequence of closing prices (most recent last)
        period: Lookback period

    Returns:
        ROC percentage or None if insufficient data
    """
    if len(closes) < period + 1:
        return None

    prev_price = closes[-(period + 1)]
    if prev_price == 0:
        return None

    return ((closes[-1] - prev_price) / prev_price) * 100


def vwap(bars: Sequence[OHLCV]) -> float | None:
    """
    Volume Weighted Average Price (intraday).

    Args:
        bars: Sequence of OHLCV bars for the session

    Returns:
        VWAP value or None if no volume
    """
    if not bars:
        return None

    cumulative_tp_vol = 0.0
    cumulative_vol = 0

    for bar in bars:
        typical_price = (bar.high + bar.low + bar.close) / 3
        cumulative_tp_vol += typical_price * bar.volume
        cumulative_vol += bar.volume

    if cumulative_vol == 0:
        return None

    return cumulative_tp_vol / cumulative_vol


def volume_sma(volumes: Sequence[int], period: int = 20) -> float | None:
    """
    Volume Simple Moving Average.

    Args:
        volumes: Sequence of volume values (most recent last)
        period: Number of periods

    Returns:
        Volume SMA or None if insufficient data
    """
    if len(volumes) < period:
        return None

    return sum(volumes[-period:]) / period


def relative_volume(current_volume: int, avg_volume: float) -> float | None:
    """
    Relative Volume (current vs average).

    Args:
        current_volume: Current bar's volume
        avg_volume: Average volume

    Returns:
        RVOL multiplier or None if avg is zero
    """
    if avg_volume == 0:
        return None

    return current_volume / avg_volume


def price_vs_sma(price: float, sma_value: float) -> float | None:
    """
    Price relative to SMA (percentage above/below).

    Args:
        price: Current price
        sma_value: SMA value

    Returns:
        Percentage above/below SMA
    """
    if sma_value == 0:
        return None

    return ((price - sma_value) / sma_value) * 100


def bollinger_bands(
    closes: Sequence[float], period: int = 20, std_dev: float = 2.0
) -> tuple[float, float, float] | None:
    """
    Bollinger Bands.

    Args:
        closes: Sequence of closing prices (most recent last)
        period: SMA period (default 20)
        std_dev: Standard deviation multiplier (default 2.0)

    Returns:
        Tuple of (upper, middle, lower) or None if insufficient data
    """
    if len(closes) < period:
        return None

    middle = sma(closes, period)
    if middle is None:
        return None

    # Calculate standard deviation
    recent = closes[-period:]
    variance = sum((x - middle) ** 2 for x in recent) / period
    std = math.sqrt(variance)

    upper = middle + (std_dev * std)
    lower = middle - (std_dev * std)

    return (upper, middle, lower)


def macd(
    closes: Sequence[float],
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
) -> tuple[float, float, float] | None:
    """
    MACD (Moving Average Convergence Divergence).

    Args:
        closes: Sequence of closing prices (most recent last)
        fast_period: Fast EMA period (default 12)
        slow_period: Slow EMA period (default 26)
        signal_period: Signal line period (default 9)

    Returns:
        Tuple of (macd_line, signal_line, histogram) or None if insufficient data
    """
    if len(closes) < slow_period + signal_period:
        return None

    fast_ema = ema(closes, fast_period)
    slow_ema = ema(closes, slow_period)

    if fast_ema is None or slow_ema is None:
        return None

    macd_line = fast_ema - slow_ema

    # Calculate MACD line history for signal line
    macd_history = []
    for i in range(slow_period, len(closes) + 1):
        fast = ema(closes[:i], fast_period)
        slow = ema(closes[:i], slow_period)
        if fast is not None and slow is not None:
            macd_history.append(fast - slow)

    if len(macd_history) < signal_period:
        return None

    signal_line = ema(macd_history, signal_period)
    if signal_line is None:
        return None

    histogram = macd_line - signal_line

    return (macd_line, signal_line, histogram)


def stochastic(
    bars: Sequence[OHLCV], k_period: int = 14, d_period: int = 3
) -> tuple[float, float] | None:
    """
    Stochastic Oscillator (%K and %D).

    Args:
        bars: Sequence of OHLCV bars (most recent last)
        k_period: %K lookback period (default 14)
        d_period: %D smoothing period (default 3)

    Returns:
        Tuple of (%K, %D) or None if insufficient data
    """
    if len(bars) < k_period + d_period - 1:
        return None

    # Calculate %K values
    k_values = []
    for i in range(k_period - 1, len(bars)):
        window = bars[i - k_period + 1 : i + 1]
        highest_high = max(b.high for b in window)
        lowest_low = min(b.low for b in window)

        if highest_high == lowest_low:
            k_values.append(50.0)  # Neutral when no range
        else:
            current_close = bars[i].close
            k = ((current_close - lowest_low) / (highest_high - lowest_low)) * 100
            k_values.append(k)

    if len(k_values) < d_period:
        return None

    # %D is SMA of %K
    percent_k = k_values[-1]
    percent_d = sum(k_values[-d_period:]) / d_period

    return (percent_k, percent_d)
