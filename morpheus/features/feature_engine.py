"""
Feature Engine - Orchestrates feature computation from market snapshots.

Transforms raw MarketSnapshot data into meaningful features for
regime detection and strategy selection.

All computations are DETERMINISTIC: same input → same output.

Phase 3 Scope:
- Feature computation only
- No trade decisions
- No state mutation
- Consumes Phase 2 MarketSnapshot (read-only)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from morpheus.core.events import Event, EventType, create_event
from morpheus.data.market_snapshot import MarketSnapshot, Candle
from morpheus.features.indicators import (
    OHLCV,
    sma,
    ema,
    rsi,
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
from morpheus.features.rolling_stats import (
    realized_volatility,
    returns_from_closes,
    price_z_score,
    mean_reversion_score,
    momentum_score,
    trend_strength,
    trend_direction,
    high_low_range,
    price_position_in_range,
    consecutive_direction,
)


# Schema version for feature context
FEATURE_SCHEMA_VERSION = "1.0"

# Minimum bars required for full feature computation
MIN_BARS_REQUIRED = 50


@dataclass(frozen=True)
class FeatureContext:
    """
    Complete feature context for a symbol at a point in time.

    Contains:
    - Reference to the source snapshot
    - All computed features
    - Regime classification (set by RegimeDetector)
    - Allowed strategies (set by StrategyRouter)

    Immutable to ensure determinism.
    """

    schema_version: str = FEATURE_SCHEMA_VERSION
    symbol: str = ""
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # Source data reference
    snapshot: MarketSnapshot | None = None

    # Computed features (all floats, None if insufficient data)
    features: dict[str, float | None] = field(default_factory=dict)

    # Regime (set by RegimeDetector, None until classified)
    regime: str | None = None
    regime_confidence: float = 0.0
    regime_components: dict[str, str] = field(default_factory=dict)

    # Allowed strategies (set by StrategyRouter)
    allowed_strategies: tuple[str, ...] = field(default_factory=tuple)

    # Metadata
    bars_available: int = 0
    warmup_complete: bool = False

    def to_event_payload(self) -> dict[str, Any]:
        """Convert to event payload dict."""
        return {
            "schema_version": self.schema_version,
            "symbol": self.symbol,
            "timestamp": self.timestamp.isoformat(),
            "features": self.features,
            "regime": self.regime,
            "regime_confidence": self.regime_confidence,
            "regime_components": self.regime_components,
            "allowed_strategies": list(self.allowed_strategies),
            "bars_available": self.bars_available,
            "warmup_complete": self.warmup_complete,
        }

    def to_event(self) -> Event:
        """Create a FEATURES_COMPUTED event."""
        return create_event(
            EventType.FEATURES_COMPUTED,
            payload=self.to_event_payload(),
            symbol=self.symbol,
            timestamp=self.timestamp,
        )

    def get_feature(self, name: str) -> float | None:
        """Get a feature value by name."""
        return self.features.get(name)

    def has_feature(self, name: str) -> bool:
        """Check if a feature is available (not None)."""
        return self.features.get(name) is not None


def candle_to_ohlcv(candle: Candle) -> OHLCV:
    """Convert a Candle to OHLCV for indicator calculations."""
    return OHLCV(
        open=candle.open,
        high=candle.high,
        low=candle.low,
        close=candle.close,
        volume=candle.volume,
    )


class FeatureEngine:
    """
    Engine for computing features from market data.

    Maintains a rolling window of historical data per symbol
    and computes features on demand.

    Thread-safe for concurrent symbol processing.
    """

    def __init__(
        self,
        max_bars: int = 200,
        min_bars: int = MIN_BARS_REQUIRED,
    ):
        """
        Initialize the feature engine.

        Args:
            max_bars: Maximum bars to keep in history per symbol
            min_bars: Minimum bars required for full feature computation
        """
        self.max_bars = max_bars
        self.min_bars = min_bars

        # Historical data per symbol: symbol -> list of OHLCV
        self._history: dict[str, list[OHLCV]] = {}

    def add_bar(self, symbol: str, bar: OHLCV) -> None:
        """
        Add a bar to the history for a symbol.

        Args:
            symbol: Stock symbol
            bar: OHLCV bar data
        """
        if symbol not in self._history:
            self._history[symbol] = []

        self._history[symbol].append(bar)

        # Trim to max_bars
        if len(self._history[symbol]) > self.max_bars:
            self._history[symbol] = self._history[symbol][-self.max_bars :]

    def add_candle(self, symbol: str, candle: Candle) -> None:
        """
        Add a Candle to the history for a symbol.

        Args:
            symbol: Stock symbol
            candle: Candle data
        """
        self.add_bar(symbol, candle_to_ohlcv(candle))

    def get_bars(self, symbol: str) -> list[OHLCV]:
        """Get historical bars for a symbol."""
        return self._history.get(symbol, [])

    def get_closes(self, symbol: str) -> list[float]:
        """Get closing prices for a symbol."""
        return [bar.close for bar in self.get_bars(symbol)]

    def get_volumes(self, symbol: str) -> list[int]:
        """Get volumes for a symbol."""
        return [bar.volume for bar in self.get_bars(symbol)]

    def bars_available(self, symbol: str) -> int:
        """Get number of bars available for a symbol."""
        return len(self._history.get(symbol, []))

    def warmup_complete(self, symbol: str) -> bool:
        """Check if enough bars for full feature computation."""
        return self.bars_available(symbol) >= self.min_bars

    def compute_features(
        self, symbol: str, snapshot: MarketSnapshot | None = None
    ) -> FeatureContext:
        """
        Compute all features for a symbol.

        Args:
            symbol: Stock symbol
            snapshot: Optional current snapshot (for live price data)

        Returns:
            FeatureContext with all computed features
        """
        bars = self.get_bars(symbol)
        closes = self.get_closes(symbol)
        volumes = self.get_volumes(symbol)
        n_bars = len(bars)

        features: dict[str, float | None] = {}

        # Current price (from snapshot or last bar)
        current_price = snapshot.last if snapshot else (closes[-1] if closes else None)

        # ===== Price-Based Features =====
        features["sma_20"] = sma(closes, 20)
        features["sma_50"] = sma(closes, 50)
        features["ema_9"] = ema(closes, 9)
        features["ema_21"] = ema(closes, 21)

        # Price vs SMAs
        if features["sma_20"] and current_price:
            features["price_vs_sma20"] = price_vs_sma(current_price, features["sma_20"])
        else:
            features["price_vs_sma20"] = None

        if features["sma_50"] and current_price:
            features["price_vs_sma50"] = price_vs_sma(current_price, features["sma_50"])
        else:
            features["price_vs_sma50"] = None

        # VWAP (intraday)
        features["vwap"] = vwap(bars) if bars else None
        if features["vwap"] and current_price:
            features["price_vs_vwap"] = price_vs_sma(current_price, features["vwap"])
        else:
            features["price_vs_vwap"] = None

        # ===== Volatility Features =====
        features["atr_14"] = atr(bars, 14)
        features["atr_pct"] = atr_percent(bars, 14)

        returns = returns_from_closes(closes)
        features["realized_vol_20"] = realized_volatility(returns, 20)

        # Bollinger Bands
        bb = bollinger_bands(closes, 20, 2.0)
        if bb:
            features["bb_upper"] = bb[0]
            features["bb_middle"] = bb[1]
            features["bb_lower"] = bb[2]
            if current_price and bb[1] != bb[2]:
                # Position within bands (0 = lower, 100 = upper)
                features["bb_position"] = (
                    (current_price - bb[2]) / (bb[0] - bb[2]) * 100
                    if bb[0] != bb[2]
                    else 50.0
                )
            else:
                features["bb_position"] = None
        else:
            features["bb_upper"] = None
            features["bb_middle"] = None
            features["bb_lower"] = None
            features["bb_position"] = None

        # ===== Momentum Features =====
        features["rsi_14"] = rsi(closes, 14)
        features["momentum_10"] = momentum(closes, 10)
        features["roc_5"] = rate_of_change(closes, 5)
        features["roc_10"] = rate_of_change(closes, 10)

        # MACD
        macd_result = macd(closes, 12, 26, 9)
        if macd_result:
            features["macd_line"] = macd_result[0]
            features["macd_signal"] = macd_result[1]
            features["macd_histogram"] = macd_result[2]
        else:
            features["macd_line"] = None
            features["macd_signal"] = None
            features["macd_histogram"] = None

        # Stochastic
        stoch = stochastic(bars, 14, 3)
        if stoch:
            features["stoch_k"] = stoch[0]
            features["stoch_d"] = stoch[1]
        else:
            features["stoch_k"] = None
            features["stoch_d"] = None

        # ===== Volume Features =====
        features["volume_sma_20"] = volume_sma(volumes, 20)
        if features["volume_sma_20"] and volumes:
            features["relative_volume"] = relative_volume(
                volumes[-1], features["volume_sma_20"]
            )
        else:
            features["relative_volume"] = None

        # ===== Statistical Features =====
        features["price_z_score"] = price_z_score(closes, 20)
        features["mean_reversion_score"] = mean_reversion_score(closes, 20)
        features["momentum_score"] = momentum_score(closes, 5, 20)
        features["trend_strength"] = trend_strength(closes, 20)
        features["trend_direction"] = trend_direction(closes, 20)

        # Range features
        features["range_20"] = high_low_range(bars, 20)
        if current_price:
            features["price_in_range"] = price_position_in_range(
                current_price, bars, 20
            )
        else:
            features["price_in_range"] = None

        # Consecutive direction
        features["consecutive_bars"] = float(consecutive_direction(closes))

        # ===== Build FeatureContext =====
        return FeatureContext(
            schema_version=FEATURE_SCHEMA_VERSION,
            symbol=symbol,
            timestamp=snapshot.timestamp if snapshot else datetime.now(timezone.utc),
            snapshot=snapshot,
            features=features,
            regime=None,  # Set by RegimeDetector
            regime_confidence=0.0,
            regime_components={},
            allowed_strategies=(),  # Set by StrategyRouter
            bars_available=n_bars,
            warmup_complete=n_bars >= self.min_bars,
        )

    def reset_symbol(self, symbol: str) -> None:
        """Clear history for a symbol."""
        if symbol in self._history:
            del self._history[symbol]

    def reset_all(self) -> None:
        """Clear all symbol history."""
        self._history.clear()


def create_feature_context(
    symbol: str,
    snapshot: MarketSnapshot,
    features: dict[str, float | None],
    bars_available: int = 0,
    warmup_complete: bool = False,
) -> FeatureContext:
    """
    Factory function to create a FeatureContext.

    Args:
        symbol: Stock symbol
        snapshot: Source MarketSnapshot
        features: Computed feature dict
        bars_available: Number of bars used
        warmup_complete: Whether warmup period is done

    Returns:
        FeatureContext instance
    """
    return FeatureContext(
        schema_version=FEATURE_SCHEMA_VERSION,
        symbol=symbol,
        timestamp=snapshot.timestamp,
        snapshot=snapshot,
        features=features,
        regime=None,
        regime_confidence=0.0,
        regime_components={},
        allowed_strategies=(),
        bars_available=bars_available,
        warmup_complete=warmup_complete,
    )
