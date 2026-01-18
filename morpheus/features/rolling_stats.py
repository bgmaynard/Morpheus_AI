"""
Rolling Statistics for Feature Engineering.

Computes volatility, momentum, and mean reversion metrics
using rolling windows over price data.

All functions are DETERMINISTIC: same input → same output.

Phase 3 Scope:
- Statistical computations only
- No trade decisions
- No state mutation
"""

from __future__ import annotations

import math
from typing import Sequence

from morpheus.features.indicators import OHLCV


def realized_volatility(
    returns: Sequence[float], period: int = 20, annualize: bool = False
) -> float | None:
    """
    Realized Volatility (standard deviation of returns).

    Args:
        returns: Sequence of returns (most recent last)
        period: Lookback period
        annualize: Whether to annualize (assumes 252 trading days)

    Returns:
        Realized volatility or None if insufficient data
    """
    if len(returns) < period:
        return None

    recent = returns[-period:]
    mean = sum(recent) / period
    variance = sum((r - mean) ** 2 for r in recent) / period
    vol = math.sqrt(variance)

    if annualize:
        vol *= math.sqrt(252)

    return vol


def returns_from_closes(closes: Sequence[float]) -> list[float]:
    """
    Calculate simple returns from closing prices.

    Args:
        closes: Sequence of closing prices

    Returns:
        List of returns (one fewer than closes)
    """
    if len(closes) < 2:
        return []

    returns = []
    for i in range(1, len(closes)):
        if closes[i - 1] != 0:
            ret = (closes[i] - closes[i - 1]) / closes[i - 1]
            returns.append(ret)
        else:
            returns.append(0.0)

    return returns


def log_returns_from_closes(closes: Sequence[float]) -> list[float]:
    """
    Calculate log returns from closing prices.

    Args:
        closes: Sequence of closing prices

    Returns:
        List of log returns
    """
    if len(closes) < 2:
        return []

    returns = []
    for i in range(1, len(closes)):
        if closes[i - 1] > 0 and closes[i] > 0:
            ret = math.log(closes[i] / closes[i - 1])
            returns.append(ret)
        else:
            returns.append(0.0)

    return returns


def rolling_mean(values: Sequence[float], period: int) -> float | None:
    """
    Rolling mean of values.

    Args:
        values: Sequence of values
        period: Lookback period

    Returns:
        Rolling mean or None if insufficient data
    """
    if len(values) < period:
        return None

    return sum(values[-period:]) / period


def rolling_std(values: Sequence[float], period: int) -> float | None:
    """
    Rolling standard deviation.

    Args:
        values: Sequence of values
        period: Lookback period

    Returns:
        Rolling std or None if insufficient data
    """
    if len(values) < period:
        return None

    recent = values[-period:]
    mean = sum(recent) / period
    variance = sum((v - mean) ** 2 for v in recent) / period

    return math.sqrt(variance)


def z_score(value: float, mean: float, std: float) -> float | None:
    """
    Calculate z-score of a value.

    Args:
        value: Current value
        mean: Mean of the distribution
        std: Standard deviation

    Returns:
        Z-score or None if std is zero
    """
    if std == 0:
        return None

    return (value - mean) / std


def price_z_score(
    closes: Sequence[float], period: int = 20
) -> float | None:
    """
    Z-score of current price relative to rolling window.

    Args:
        closes: Sequence of closing prices
        period: Lookback period

    Returns:
        Price z-score or None if insufficient data
    """
    if len(closes) < period:
        return None

    mean = rolling_mean(closes, period)
    std = rolling_std(closes, period)

    if mean is None or std is None:
        return None

    return z_score(closes[-1], mean, std)


def mean_reversion_score(
    closes: Sequence[float], period: int = 20
) -> float | None:
    """
    Mean reversion score: negative z-score (high = oversold, low = overbought).

    Positive score suggests mean reversion buy opportunity.
    Negative score suggests mean reversion sell opportunity.

    Args:
        closes: Sequence of closing prices
        period: Lookback period

    Returns:
        Mean reversion score or None if insufficient data
    """
    z = price_z_score(closes, period)
    if z is None:
        return None

    return -z  # Invert so positive = oversold


def momentum_score(
    closes: Sequence[float], short_period: int = 5, long_period: int = 20
) -> float | None:
    """
    Momentum score: short-term vs long-term performance.

    Positive = short-term outperforming (bullish momentum)
    Negative = short-term underperforming (bearish momentum)

    Args:
        closes: Sequence of closing prices
        short_period: Short lookback
        long_period: Long lookback

    Returns:
        Momentum score or None if insufficient data
    """
    if len(closes) < long_period + 1:
        return None

    short_return = (closes[-1] - closes[-(short_period + 1)]) / closes[-(short_period + 1)]
    long_return = (closes[-1] - closes[-(long_period + 1)]) / closes[-(long_period + 1)]

    return short_return - long_return


def trend_strength(
    closes: Sequence[float], period: int = 20
) -> float | None:
    """
    Trend strength: R-squared of linear regression.

    High value (close to 1) = strong trend
    Low value (close to 0) = no clear trend / choppy

    Args:
        closes: Sequence of closing prices
        period: Lookback period

    Returns:
        R-squared (0-1) or None if insufficient data
    """
    if len(closes) < period:
        return None

    recent = closes[-period:]
    n = len(recent)

    # Linear regression
    x_mean = (n - 1) / 2
    y_mean = sum(recent) / n

    numerator = sum((i - x_mean) * (recent[i] - y_mean) for i in range(n))
    x_denominator = sum((i - x_mean) ** 2 for i in range(n))

    if x_denominator == 0:
        return None

    slope = numerator / x_denominator
    intercept = y_mean - slope * x_mean

    # Calculate R-squared
    ss_res = sum((recent[i] - (slope * i + intercept)) ** 2 for i in range(n))
    ss_tot = sum((recent[i] - y_mean) ** 2 for i in range(n))

    if ss_tot == 0:
        return None

    r_squared = 1 - (ss_res / ss_tot)

    return max(0, min(1, r_squared))  # Clamp to [0, 1]


def trend_direction(
    closes: Sequence[float], period: int = 20
) -> float | None:
    """
    Trend direction: slope of linear regression, normalized.

    Positive = uptrend
    Negative = downtrend

    Args:
        closes: Sequence of closing prices
        period: Lookback period

    Returns:
        Normalized slope or None if insufficient data
    """
    if len(closes) < period:
        return None

    recent = closes[-period:]
    n = len(recent)

    x_mean = (n - 1) / 2
    y_mean = sum(recent) / n

    numerator = sum((i - x_mean) * (recent[i] - y_mean) for i in range(n))
    x_denominator = sum((i - x_mean) ** 2 for i in range(n))

    if x_denominator == 0 or y_mean == 0:
        return None

    slope = numerator / x_denominator

    # Normalize by price level
    return (slope / y_mean) * 100


def volatility_percentile(
    current_vol: float, historical_vols: Sequence[float]
) -> float | None:
    """
    Percentile rank of current volatility vs historical.

    Args:
        current_vol: Current volatility value
        historical_vols: Historical volatility values

    Returns:
        Percentile (0-100) or None if no history
    """
    if not historical_vols:
        return None

    count_below = sum(1 for v in historical_vols if v < current_vol)
    return (count_below / len(historical_vols)) * 100


def high_low_range(bars: Sequence[OHLCV], period: int = 20) -> float | None:
    """
    High-Low range over period as percentage.

    Args:
        bars: Sequence of OHLCV bars
        period: Lookback period

    Returns:
        Range percentage or None if insufficient data
    """
    if len(bars) < period:
        return None

    recent = bars[-period:]
    highest = max(b.high for b in recent)
    lowest = min(b.low for b in recent)

    if lowest == 0:
        return None

    return ((highest - lowest) / lowest) * 100


def price_position_in_range(
    price: float, bars: Sequence[OHLCV], period: int = 20
) -> float | None:
    """
    Position of current price within recent range (0-100).

    0 = at low, 100 = at high, 50 = middle

    Args:
        price: Current price
        bars: Sequence of OHLCV bars
        period: Lookback period

    Returns:
        Position percentage or None if insufficient data
    """
    if len(bars) < period:
        return None

    recent = bars[-period:]
    highest = max(b.high for b in recent)
    lowest = min(b.low for b in recent)

    if highest == lowest:
        return 50.0  # No range

    return ((price - lowest) / (highest - lowest)) * 100


def consecutive_direction(closes: Sequence[float]) -> int:
    """
    Count consecutive bars in same direction.

    Positive = consecutive up bars
    Negative = consecutive down bars

    Args:
        closes: Sequence of closing prices

    Returns:
        Consecutive count (positive or negative)
    """
    if len(closes) < 2:
        return 0

    count = 0
    direction = None

    for i in range(len(closes) - 1, 0, -1):
        change = closes[i] - closes[i - 1]
        current_dir = 1 if change > 0 else -1 if change < 0 else 0

        if current_dir == 0:
            break

        if direction is None:
            direction = current_dir
            count = current_dir
        elif current_dir == direction:
            count += current_dir
        else:
            break

    return count
