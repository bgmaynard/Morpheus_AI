"""
Tests for technical indicators.

Phase 3 test coverage:
- SMA, EMA calculations
- RSI, ATR, MACD, Stochastic
- Volume indicators
- Bollinger Bands
- Edge cases and insufficient data
"""

import pytest
import math

from morpheus.features.indicators import (
    OHLCV,
    sma,
    ema,
    rsi,
    true_range,
    atr,
    atr_percent,
    momentum,
    rate_of_change,
    vwap,
    volume_sma,
    relative_volume,
    price_vs_sma,
    bollinger_bands,
    macd,
    stochastic,
)


class TestOHLCV:
    """Tests for OHLCV dataclass."""

    def test_ohlcv_creation(self):
        """OHLCV should store all fields."""
        bar = OHLCV(open=100.0, high=105.0, low=99.0, close=103.0, volume=1000)

        assert bar.open == 100.0
        assert bar.high == 105.0
        assert bar.low == 99.0
        assert bar.close == 103.0
        assert bar.volume == 1000

    def test_ohlcv_is_frozen(self):
        """OHLCV should be immutable."""
        bar = OHLCV(open=100.0, high=105.0, low=99.0, close=103.0, volume=1000)

        with pytest.raises(Exception):
            bar.close = 110.0


class TestSMA:
    """Tests for Simple Moving Average."""

    def test_sma_basic(self):
        """SMA should calculate correct average."""
        values = [10.0, 20.0, 30.0, 40.0, 50.0]
        result = sma(values, 5)

        assert result == 30.0

    def test_sma_partial_period(self):
        """SMA should use most recent values."""
        values = [10.0, 20.0, 30.0, 40.0, 50.0]
        result = sma(values, 3)

        assert result == 40.0  # (30 + 40 + 50) / 3

    def test_sma_insufficient_data(self):
        """SMA should return None if insufficient data."""
        values = [10.0, 20.0]
        result = sma(values, 5)

        assert result is None

    def test_sma_deterministic(self):
        """SMA should be deterministic."""
        values = [1.0, 2.0, 3.0, 4.0, 5.0]

        result1 = sma(values, 3)
        result2 = sma(values, 3)

        assert result1 == result2


class TestEMA:
    """Tests for Exponential Moving Average."""

    def test_ema_basic(self):
        """EMA should compute exponential average."""
        values = [10.0, 20.0, 30.0, 40.0, 50.0]
        result = ema(values, 3)

        assert result is not None
        # EMA gives more weight to recent values
        assert result > 30.0

    def test_ema_insufficient_data(self):
        """EMA should return None if insufficient data."""
        values = [10.0, 20.0]
        result = ema(values, 5)

        assert result is None

    def test_ema_single_value_equals_sma(self):
        """EMA with period equal to length should equal SMA initially."""
        values = [10.0, 20.0, 30.0]
        ema_result = ema(values, 3)
        sma_result = sma(values, 3)

        assert ema_result == sma_result


class TestRSI:
    """Tests for Relative Strength Index."""

    def test_rsi_range(self):
        """RSI should be between 0 and 100."""
        # Uptrend
        values = [float(i) for i in range(1, 20)]
        result = rsi(values, 14)

        assert result is not None
        assert 0 <= result <= 100

    def test_rsi_overbought(self):
        """RSI should be high in strong uptrend."""
        values = [float(i) for i in range(1, 30)]  # Strong uptrend
        result = rsi(values, 14)

        assert result is not None
        assert result > 70  # Overbought

    def test_rsi_oversold(self):
        """RSI should be low in strong downtrend."""
        values = [float(30 - i) for i in range(1, 30)]  # Strong downtrend
        result = rsi(values, 14)

        assert result is not None
        assert result < 30  # Oversold

    def test_rsi_insufficient_data(self):
        """RSI should return None if insufficient data."""
        values = [10.0, 11.0, 12.0]
        result = rsi(values, 14)

        assert result is None


class TestATR:
    """Tests for Average True Range."""

    def test_true_range_basic(self):
        """True range should be max of HL, HC, LC."""
        bar = OHLCV(open=100.0, high=105.0, low=95.0, close=102.0, volume=1000)

        tr = true_range(bar)
        assert tr == 10.0  # high - low

    def test_true_range_with_gap(self):
        """True range should handle gaps."""
        bar = OHLCV(open=110.0, high=115.0, low=108.0, close=112.0, volume=1000)
        prev_close = 100.0

        tr = true_range(bar, prev_close)
        # max(7, |115-100|, |108-100|) = max(7, 15, 8) = 15
        assert tr == 15.0

    def test_atr_basic(self):
        """ATR should compute average true range."""
        bars = [
            OHLCV(open=100.0, high=105.0, low=95.0, close=102.0, volume=1000)
            for _ in range(20)
        ]
        result = atr(bars, 14)

        assert result is not None
        assert result > 0

    def test_atr_insufficient_data(self):
        """ATR should return None if insufficient data."""
        bars = [OHLCV(open=100.0, high=105.0, low=95.0, close=102.0, volume=1000)]
        result = atr(bars, 14)

        assert result is None


class TestMomentum:
    """Tests for Momentum and ROC."""

    def test_momentum_positive(self):
        """Momentum should be positive in uptrend."""
        values = [100.0, 105.0, 110.0, 115.0, 120.0]
        result = momentum(values, 3)

        assert result is not None
        assert result > 0  # 120 - 105 = 15

    def test_momentum_negative(self):
        """Momentum should be negative in downtrend."""
        values = [120.0, 115.0, 110.0, 105.0, 100.0]
        result = momentum(values, 3)

        assert result is not None
        assert result < 0  # 100 - 115 = -15

    def test_rate_of_change(self):
        """ROC should calculate percentage change."""
        values = [100.0, 110.0, 120.0, 130.0]
        result = rate_of_change(values, 2)

        # (130 - 110) / 110 * 100 = 18.18%
        assert result is not None
        assert abs(result - 18.18) < 0.1


class TestVWAP:
    """Tests for Volume Weighted Average Price."""

    def test_vwap_basic(self):
        """VWAP should be volume-weighted average."""
        bars = [
            OHLCV(open=100.0, high=102.0, low=99.0, close=101.0, volume=1000),
            OHLCV(open=101.0, high=103.0, low=100.0, close=102.0, volume=2000),
        ]

        result = vwap(bars)
        assert result is not None

        # Calculate expected VWAP
        tp1 = (102 + 99 + 101) / 3  # 100.67
        tp2 = (103 + 100 + 102) / 3  # 101.67
        expected = (tp1 * 1000 + tp2 * 2000) / 3000

        assert abs(result - expected) < 0.01

    def test_vwap_no_volume(self):
        """VWAP should return None if no volume."""
        bars = [
            OHLCV(open=100.0, high=102.0, low=99.0, close=101.0, volume=0),
        ]

        result = vwap(bars)
        assert result is None


class TestBollingerBands:
    """Tests for Bollinger Bands."""

    def test_bollinger_bands_basic(self):
        """Bollinger Bands should compute upper, middle, lower."""
        values = [float(100 + i) for i in range(30)]
        result = bollinger_bands(values, 20, 2.0)

        assert result is not None
        upper, middle, lower = result

        assert upper > middle > lower
        # Middle should be SMA
        assert abs(middle - sma(values, 20)) < 0.01

    def test_bollinger_bands_width(self):
        """Higher std_dev multiplier should produce wider bands."""
        values = [float(100 + i % 10) for i in range(30)]

        narrow = bollinger_bands(values, 20, 1.0)
        wide = bollinger_bands(values, 20, 3.0)

        assert narrow is not None and wide is not None
        narrow_width = narrow[0] - narrow[2]
        wide_width = wide[0] - wide[2]

        assert wide_width > narrow_width


class TestMACD:
    """Tests for MACD."""

    def test_macd_basic(self):
        """MACD should return line, signal, histogram."""
        values = [float(100 + i) for i in range(50)]
        result = macd(values, 12, 26, 9)

        assert result is not None
        macd_line, signal_line, histogram = result

        assert abs(histogram - (macd_line - signal_line)) < 0.0001

    def test_macd_uptrend(self):
        """MACD should be positive in uptrend."""
        values = [float(100 + i * 2) for i in range(50)]
        result = macd(values, 12, 26, 9)

        assert result is not None
        assert result[0] > 0  # MACD line positive in uptrend


class TestStochastic:
    """Tests for Stochastic Oscillator."""

    def test_stochastic_range(self):
        """Stochastic %K and %D should be 0-100."""
        bars = [
            OHLCV(
                open=100.0 + i,
                high=105.0 + i,
                low=95.0 + i,
                close=102.0 + i,
                volume=1000,
            )
            for i in range(20)
        ]

        result = stochastic(bars, 14, 3)
        assert result is not None
        k, d = result

        assert 0 <= k <= 100
        assert 0 <= d <= 100

    def test_stochastic_overbought(self):
        """Stochastic should be high when price is near high."""
        # Price near top of range
        bars = []
        for i in range(20):
            bars.append(
                OHLCV(open=100.0, high=110.0, low=90.0, close=109.0, volume=1000)
            )

        result = stochastic(bars, 14, 3)
        assert result is not None
        k, d = result

        assert k > 80  # Near top of range
