"""
Tests for rolling statistics.

Phase 3 test coverage:
- Realized volatility
- Returns calculations
- Z-scores and mean reversion
- Trend strength and direction
- Price range calculations
"""

import pytest
import math

from morpheus.features.indicators import OHLCV
from morpheus.features.rolling_stats import (
    realized_volatility,
    returns_from_closes,
    log_returns_from_closes,
    rolling_mean,
    rolling_std,
    z_score,
    price_z_score,
    mean_reversion_score,
    momentum_score,
    trend_strength,
    trend_direction,
    volatility_percentile,
    high_low_range,
    price_position_in_range,
    consecutive_direction,
)


class TestReturns:
    """Tests for return calculations."""

    def test_returns_from_closes(self):
        """Simple returns should be (P1-P0)/P0."""
        closes = [100.0, 110.0, 105.0]
        returns = returns_from_closes(closes)

        assert len(returns) == 2
        assert abs(returns[0] - 0.10) < 0.0001  # +10%
        assert abs(returns[1] - (-0.0455)) < 0.001  # -4.55%

    def test_log_returns_from_closes(self):
        """Log returns should be ln(P1/P0)."""
        closes = [100.0, 110.0]
        returns = log_returns_from_closes(closes)

        assert len(returns) == 1
        expected = math.log(110 / 100)
        assert abs(returns[0] - expected) < 0.0001

    def test_returns_insufficient_data(self):
        """Returns should return empty list if < 2 prices."""
        closes = [100.0]
        returns = returns_from_closes(closes)

        assert returns == []

    def test_returns_handles_zero(self):
        """Returns should handle zero prices."""
        closes = [0.0, 100.0]
        returns = returns_from_closes(closes)

        assert len(returns) == 1
        assert returns[0] == 0.0  # Division by zero handled


class TestRealizedVolatility:
    """Tests for realized volatility."""

    def test_realized_volatility_basic(self):
        """Realized vol should be std dev of returns."""
        # Create returns with known volatility
        returns = [0.01, -0.01, 0.02, -0.02, 0.01] * 5  # 25 returns
        result = realized_volatility(returns, 20)

        assert result is not None
        assert result > 0

    def test_realized_volatility_annualize(self):
        """Annualized vol should scale by sqrt(252)."""
        returns = [0.01, -0.01, 0.02, -0.02, 0.01] * 5
        daily = realized_volatility(returns, 20, annualize=False)
        annual = realized_volatility(returns, 20, annualize=True)

        assert daily is not None and annual is not None
        assert abs(annual - daily * math.sqrt(252)) < 0.0001

    def test_realized_volatility_insufficient_data(self):
        """Realized vol should return None if insufficient data."""
        returns = [0.01, 0.02]
        result = realized_volatility(returns, 20)

        assert result is None


class TestRollingStats:
    """Tests for rolling mean and std."""

    def test_rolling_mean(self):
        """Rolling mean should be average of recent values."""
        values = [10.0, 20.0, 30.0, 40.0, 50.0]
        result = rolling_mean(values, 3)

        assert result == 40.0  # (30+40+50)/3

    def test_rolling_std(self):
        """Rolling std should be standard deviation."""
        values = [10.0, 20.0, 30.0]
        result = rolling_std(values, 3)

        expected = math.sqrt(((10-20)**2 + (20-20)**2 + (30-20)**2) / 3)
        assert result is not None
        assert abs(result - expected) < 0.0001


class TestZScore:
    """Tests for z-score calculations."""

    def test_z_score_basic(self):
        """Z-score should be (value - mean) / std."""
        result = z_score(value=110.0, mean=100.0, std=5.0)

        assert result == 2.0  # (110-100)/5

    def test_z_score_zero_std(self):
        """Z-score should return None if std is zero."""
        result = z_score(value=100.0, mean=100.0, std=0.0)

        assert result is None

    def test_price_z_score(self):
        """Price z-score should work on closing prices."""
        # Create prices with known distribution
        closes = [100.0] * 20 + [120.0]
        result = price_z_score(closes, 20)

        assert result is not None
        # Last price (120) is above all others (100)
        assert result > 0


class TestMeanReversion:
    """Tests for mean reversion score."""

    def test_mean_reversion_oversold(self):
        """Mean reversion should be positive when oversold."""
        # Price dropped significantly
        closes = [100.0] * 19 + [80.0]
        result = mean_reversion_score(closes, 20)

        assert result is not None
        assert result > 0  # Positive = oversold

    def test_mean_reversion_overbought(self):
        """Mean reversion should be negative when overbought."""
        # Price increased significantly
        closes = [100.0] * 19 + [120.0]
        result = mean_reversion_score(closes, 20)

        assert result is not None
        assert result < 0  # Negative = overbought


class TestMomentumScore:
    """Tests for momentum score."""

    def test_momentum_score_positive(self):
        """Momentum should be positive when short-term return > long-term return.

        This happens when price dipped in the middle but recovered strongly recently.
        The short_return (from -6) being higher than long_return (from -21) means
        the recent performance is stronger than historical.
        """
        # Index 0 (-21): 120 (long reference)
        # Index 15 (-6): 100 (short reference - a dip)
        # Index 20 (-1): 150 (current - strong recovery)
        # short_return = (150 - 100) / 100 = 50%
        # long_return = (150 - 120) / 120 = 25%
        # momentum = 50% - 25% = 25% (positive!)
        closes = [120.0] * 10 + [115.0, 110.0, 105.0, 102.0, 100.0, 100.0] + [110.0, 125.0, 140.0, 145.0, 150.0]
        result = momentum_score(closes, 5, 20)

        assert result is not None
        assert result > 0  # Short-term outperforming long-term

    def test_momentum_score_negative(self):
        """Momentum should be negative when long-term return > short-term return.

        This happens when most gains happened earlier, and recent moves are slower.
        """
        # Index 0 (-21): 100 (long reference)
        # Index 15 (-6): 145 (short reference - already gained a lot)
        # Index 20 (-1): 150 (current - small recent gain)
        # short_return = (150 - 145) / 145 = 3.4%
        # long_return = (150 - 100) / 100 = 50%
        # momentum = 3.4% - 50% = -46.6% (negative!)
        closes = [100.0] + [float(100 + i * 3) for i in range(1, 16)] + [146.0, 147.0, 148.0, 149.0, 150.0]
        result = momentum_score(closes, 5, 20)

        assert result is not None
        assert result < 0  # Long-term outperforming short-term


class TestTrendAnalysis:
    """Tests for trend strength and direction."""

    def test_trend_strength_strong(self):
        """Trend strength should be high in linear trend."""
        # Perfect linear uptrend
        closes = [float(100 + i) for i in range(30)]
        result = trend_strength(closes, 20)

        assert result is not None
        assert result > 0.9  # Nearly perfect R-squared

    def test_trend_strength_weak(self):
        """Trend strength should be low in choppy market."""
        # Choppy prices
        closes = [100.0, 105.0, 98.0, 103.0, 97.0, 102.0] * 5
        result = trend_strength(closes, 20)

        assert result is not None
        assert result < 0.5  # Low R-squared

    def test_trend_direction_up(self):
        """Trend direction should be positive in uptrend."""
        closes = [float(100 + i) for i in range(30)]
        result = trend_direction(closes, 20)

        assert result is not None
        assert result > 0

    def test_trend_direction_down(self):
        """Trend direction should be negative in downtrend."""
        closes = [float(130 - i) for i in range(30)]
        result = trend_direction(closes, 20)

        assert result is not None
        assert result < 0


class TestRangeAnalysis:
    """Tests for price range analysis."""

    def test_high_low_range(self):
        """High-low range should be percentage range."""
        bars = [
            OHLCV(open=100.0, high=110.0, low=90.0, close=105.0, volume=1000)
            for _ in range(25)
        ]

        result = high_low_range(bars, 20)
        assert result is not None

        # (110 - 90) / 90 * 100 = 22.22%
        assert abs(result - 22.22) < 0.1

    def test_price_position_in_range(self):
        """Price position should be 0-100."""
        bars = [
            OHLCV(open=100.0, high=110.0, low=90.0, close=100.0, volume=1000)
            for _ in range(25)
        ]

        # Price at bottom
        result_low = price_position_in_range(90.0, bars, 20)
        assert result_low is not None
        assert result_low == 0.0

        # Price at top
        result_high = price_position_in_range(110.0, bars, 20)
        assert result_high is not None
        assert result_high == 100.0

        # Price at middle
        result_mid = price_position_in_range(100.0, bars, 20)
        assert result_mid is not None
        assert result_mid == 50.0


class TestConsecutiveDirection:
    """Tests for consecutive direction counting."""

    def test_consecutive_up(self):
        """Consecutive up bars should be positive."""
        closes = [100.0, 101.0, 102.0, 103.0, 104.0]
        result = consecutive_direction(closes)

        assert result == 4  # 4 consecutive up moves

    def test_consecutive_down(self):
        """Consecutive down bars should be negative."""
        closes = [104.0, 103.0, 102.0, 101.0, 100.0]
        result = consecutive_direction(closes)

        assert result == -4  # 4 consecutive down moves

    def test_consecutive_mixed(self):
        """Mixed direction should count from end."""
        # Sequence: 100 -> 99 (down) -> 98 (down) -> 99 (up) -> 100 (up) -> 101 (up)
        # From end: 3 consecutive up moves (98->99, 99->100, 100->101)
        closes = [100.0, 99.0, 98.0, 99.0, 100.0, 101.0]
        result = consecutive_direction(closes)

        assert result == 3  # 3 consecutive up moves at end


class TestVolatilityPercentile:
    """Tests for volatility percentile."""

    def test_volatility_percentile_basic(self):
        """Percentile should rank current vol vs history."""
        historical = [0.01, 0.02, 0.03, 0.04, 0.05]
        current = 0.04

        result = volatility_percentile(current, historical)
        assert result is not None

        # 3 values below 0.04 out of 5
        assert result == 60.0

    def test_volatility_percentile_extreme(self):
        """Highest vol should be 100th percentile."""
        historical = [0.01, 0.02, 0.03]
        current = 0.05

        result = volatility_percentile(current, historical)
        assert result == 100.0
