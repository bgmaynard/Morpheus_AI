"""Momentum Intelligence Engine.

Computes real-time momentum metrics from trade flow and order book data:
- NOFI (Net Order Flow Imbalance)
- L2 Pressure (bid/ask size imbalance across book levels)
- Velocity (price movement rate, normalized)
- Absorption (volume absorbed without price change)
- Spread Dynamics (spread trend direction)

Outputs a composite momentum_score (0-100), a momentum_state
(BUILDING/PEAKED/DECAYING/NEUTRAL/REVERSING), component drivers,
and confidence level.

Designed for both live feeds and historical replay.
"""

from __future__ import annotations

import logging
import math
import statistics
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum

from morpheus.services.market_normalizer import NormalizedBookSnapshot, NormalizedTrade

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────

# Rolling window sizes
TRADE_BUFFER_MAX = 500        # Max trades to keep per symbol
BOOK_BUFFER_MAX = 100         # Max book snapshots to keep per symbol
TRADE_WINDOW_SECONDS = 300    # 5-minute rolling window for trade metrics
BOOK_WINDOW_SECONDS = 60      # 1-minute rolling window for book metrics

# Recompute threshold: recalculate after this many new events
RECOMPUTE_TRADE_THRESHOLD = 10
RECOMPUTE_BOOK_THRESHOLD = 5

# State machine thresholds
BUILDING_ENTRY_SCORE = 60
PEAKED_ENTRY_SCORE = 80
DECAYING_ENTRY_SCORE = 70     # Score drops below this from PEAKED
NEUTRAL_ENTRY_SCORE = 40      # Score drops below this from DECAYING
STATE_PERSISTENCE_TICKS = 3   # Min ticks before state transition

# Composite score weights (sum to 1.0)
WEIGHT_NOFI = 0.30
WEIGHT_L2_PRESSURE = 0.25
WEIGHT_VELOCITY = 0.25
WEIGHT_ABSORPTION = 0.10
WEIGHT_SPREAD = 0.10


# ──────────────────────────────────────────────────────────────────────
# Data types
# ──────────────────────────────────────────────────────────────────────

class MomentumState(str, Enum):
    """Momentum regime classification."""
    BUILDING = "BUILDING"        # Increasing buying pressure, trend forming
    PEAKED = "PEAKED"            # Momentum at max, velocity decelerating
    DECAYING = "DECAYING"        # Declining momentum, profit-taking
    NEUTRAL = "NEUTRAL"          # No clear directional conviction
    REVERSING = "REVERSING"      # Direction change detected


@dataclass(frozen=True)
class MomentumSnapshot:
    """Point-in-time momentum state for a symbol."""
    symbol: str
    timestamp: datetime
    momentum_score: float        # 0-100 composite
    momentum_state: MomentumState
    confidence: float            # 0.0-1.0 (data quality / coverage)
    drivers: dict[str, float]    # Component name → normalized value

    # Raw component metrics
    nofi: float                  # Net Order Flow Imbalance [-1, +1]
    l2_pressure: float           # L2 bid/ask imbalance [-1, +1]
    velocity: float              # Price velocity (normalized)
    absorption: float            # Absorption ratio [0, 1]
    spread_dynamics: float       # Spread trend [-1 narrowing, +1 widening]

    @property
    def is_ignition(self) -> bool:
        """IBKR compatibility: BUILDING/PEAKED maps to IGNITION."""
        return self.momentum_state in (MomentumState.BUILDING, MomentumState.PEAKED)

    @property
    def is_tradeable(self) -> bool:
        """Tradeable when building/peaked with sufficient confidence."""
        return self.momentum_state in (MomentumState.BUILDING, MomentumState.PEAKED) and self.confidence >= 0.5

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "timestamp": self.timestamp.isoformat(),
            "momentum_score": round(self.momentum_score, 2),
            "momentum_state": self.momentum_state.value,
            "confidence": round(self.confidence, 3),
            "is_ignition": self.is_ignition,
            "is_tradeable": self.is_tradeable,
            "drivers": {k: round(v, 4) for k, v in self.drivers.items()},
            "nofi": round(self.nofi, 4),
            "l2_pressure": round(self.l2_pressure, 4),
            "velocity": round(self.velocity, 4),
            "absorption": round(self.absorption, 4),
            "spread_dynamics": round(self.spread_dynamics, 4),
        }


# ──────────────────────────────────────────────────────────────────────
# Per-symbol state tracker
# ──────────────────────────────────────────────────────────────────────

@dataclass
class _SymbolState:
    """Internal mutable state for one symbol."""
    trades: deque = field(default_factory=lambda: deque(maxlen=TRADE_BUFFER_MAX))
    books: deque = field(default_factory=lambda: deque(maxlen=BOOK_BUFFER_MAX))
    last_snapshot: MomentumSnapshot | None = None
    pending_trades: int = 0
    pending_books: int = 0

    # State machine
    current_state: MomentumState = MomentumState.NEUTRAL
    state_tick_count: int = 0   # Ticks in current candidate state
    candidate_state: MomentumState | None = None

    # Previous values for velocity/acceleration
    prev_nofi: float = 0.0
    prev_velocity: float = 0.0


# ──────────────────────────────────────────────────────────────────────
# Momentum Engine
# ──────────────────────────────────────────────────────────────────────

class MomentumEngine:
    """Core momentum intelligence engine.

    Feed it normalized trades and book snapshots. It maintains rolling
    buffers per symbol and recomputes momentum metrics when enough new
    data arrives. Query get_snapshot(symbol) for the latest state.
    """

    def __init__(self, on_state_change=None):
        """
        Args:
            on_state_change: Optional callback(MomentumSnapshot) called when
                            momentum state transitions (e.g. NEUTRAL → BUILDING).
        """
        self._symbols: dict[str, _SymbolState] = {}
        self._on_state_change = on_state_change

    def _get_state(self, symbol: str) -> _SymbolState:
        if symbol not in self._symbols:
            self._symbols[symbol] = _SymbolState()
        return self._symbols[symbol]

    def ingest_trade(self, trade: NormalizedTrade) -> MomentumSnapshot | None:
        """Ingest a single trade. Returns updated snapshot if recomputed."""
        state = self._get_state(trade.symbol)
        state.trades.append(trade)
        state.pending_trades += 1

        if state.pending_trades >= RECOMPUTE_TRADE_THRESHOLD:
            return self._recompute(trade.symbol, trade.timestamp)
        return None

    def ingest_book(self, book: NormalizedBookSnapshot) -> MomentumSnapshot | None:
        """Ingest a book snapshot. Returns updated snapshot if recomputed."""
        state = self._get_state(book.symbol)
        state.books.append(book)
        state.pending_books += 1

        if state.pending_books >= RECOMPUTE_BOOK_THRESHOLD:
            return self._recompute(book.symbol, book.timestamp)
        return None

    def get_snapshot(self, symbol: str) -> MomentumSnapshot | None:
        """Return the latest momentum snapshot for a symbol, or None."""
        state = self._symbols.get(symbol)
        if state is None:
            return None
        return state.last_snapshot

    def get_all_snapshots(self) -> dict[str, MomentumSnapshot]:
        """Return latest snapshots for all tracked symbols."""
        return {
            sym: st.last_snapshot
            for sym, st in self._symbols.items()
            if st.last_snapshot is not None
        }

    def tracked_symbols(self) -> list[str]:
        return list(self._symbols.keys())

    def reset(self, symbol: str | None = None) -> None:
        """Clear state for one symbol or all symbols."""
        if symbol:
            self._symbols.pop(symbol, None)
        else:
            self._symbols.clear()

    # ──────────────────────────────────────────────────────────────────
    # Core computation
    # ──────────────────────────────────────────────────────────────────

    def _recompute(self, symbol: str, now: datetime) -> MomentumSnapshot:
        """Recompute all momentum metrics for a symbol."""
        state = self._get_state(symbol)
        state.pending_trades = 0
        state.pending_books = 0

        # Trim to time windows
        trade_cutoff = now - timedelta(seconds=TRADE_WINDOW_SECONDS)
        book_cutoff = now - timedelta(seconds=BOOK_WINDOW_SECONDS)
        recent_trades = [t for t in state.trades if t.timestamp >= trade_cutoff]
        recent_books = [b for b in state.books if b.timestamp >= book_cutoff]

        # Compute components
        nofi = self._compute_nofi(recent_trades)
        l2_pressure = self._compute_l2_pressure(recent_books)
        velocity = self._compute_velocity(recent_trades)
        absorption = self._compute_absorption(recent_trades)
        spread_dynamics = self._compute_spread_dynamics(recent_books)

        # Normalize to [0, 100] for composite
        nofi_norm = (nofi + 1.0) / 2.0 * 100       # [-1,1] → [0,100]
        l2_norm = (l2_pressure + 1.0) / 2.0 * 100   # [-1,1] → [0,100]
        vel_norm = (velocity + 1.0) / 2.0 * 100      # [-1,1] → [0,100]
        abs_norm = absorption * 100                    # [0,1] → [0,100]
        spread_norm = (1.0 - spread_dynamics) / 2.0 * 100  # [-1,1] → [0,100], narrowing = high

        composite = (
            nofi_norm * WEIGHT_NOFI
            + l2_norm * WEIGHT_L2_PRESSURE
            + vel_norm * WEIGHT_VELOCITY
            + abs_norm * WEIGHT_ABSORPTION
            + spread_norm * WEIGHT_SPREAD
        )
        composite = max(0.0, min(100.0, composite))

        # Confidence based on data coverage
        confidence = self._compute_confidence(recent_trades, recent_books)

        # State machine
        new_state = self._update_state_machine(state, composite, nofi, velocity)

        drivers = {
            "nofi": nofi_norm,
            "l2_pressure": l2_norm,
            "velocity": vel_norm,
            "absorption": abs_norm,
            "spread_dynamics": spread_norm,
        }

        snapshot = MomentumSnapshot(
            symbol=symbol,
            timestamp=now,
            momentum_score=composite,
            momentum_state=new_state,
            confidence=confidence,
            drivers=drivers,
            nofi=nofi,
            l2_pressure=l2_pressure,
            velocity=velocity,
            absorption=absorption,
            spread_dynamics=spread_dynamics,
        )

        # Detect state change
        old_state = state.last_snapshot.momentum_state if state.last_snapshot else MomentumState.NEUTRAL
        state.last_snapshot = snapshot
        state.prev_nofi = nofi
        state.prev_velocity = velocity

        if new_state != old_state and self._on_state_change:
            logger.info(
                "Momentum state change: %s %s → %s (score=%.1f)",
                symbol, old_state.value, new_state.value, composite,
            )
            self._on_state_change(snapshot)

        return snapshot

    # ──────────────────────────────────────────────────────────────────
    # Individual metric computations
    # ──────────────────────────────────────────────────────────────────

    def _compute_nofi(self, trades: list[NormalizedTrade]) -> float:
        """Net Order Flow Imbalance: (buy_vol - sell_vol) / total_vol.

        Returns [-1, +1]. Positive = buying dominance.
        """
        if not trades:
            return 0.0

        buy_vol = sum(t.size for t in trades if t.side == "buy")
        sell_vol = sum(t.size for t in trades if t.side == "sell")
        total_vol = buy_vol + sell_vol

        if total_vol == 0:
            return 0.0

        return (buy_vol - sell_vol) / total_vol

    def _compute_l2_pressure(self, books: list[NormalizedBookSnapshot]) -> float:
        """L2 bid/ask size imbalance across top 5 levels.

        Returns [-1, +1]. Positive = more bid support (bullish).
        Uses the most recent book snapshot for instant read, with
        slight smoothing from recent snapshots.
        """
        if not books:
            return 0.0

        # Weight recent snapshots more (exponential decay)
        pressures = []
        for book in books[-10:]:  # Last 10 snapshots
            # Sum top 5 levels
            bid_total = sum(book.levels[i].bid_size for i in range(min(5, len(book.levels))))
            ask_total = sum(book.levels[i].ask_size for i in range(min(5, len(book.levels))))
            denom = bid_total + ask_total
            if denom > 0:
                pressures.append((bid_total - ask_total) / denom)

        if not pressures:
            return 0.0

        # Exponential weighted average (newer = heavier)
        weights = [1.2 ** i for i in range(len(pressures))]
        total_w = sum(weights)
        return sum(p * w for p, w in zip(pressures, weights)) / total_w

    def _compute_velocity(self, trades: list[NormalizedTrade]) -> float:
        """Price velocity: rate of price change over the window.

        Uses linear regression slope of trade prices, normalized by
        price level to make it comparable across symbols. Returns [-1, +1].
        """
        if len(trades) < 5:
            return 0.0

        prices = [t.price for t in trades]
        n = len(prices)

        # Simple linear regression slope
        x_mean = (n - 1) / 2.0
        y_mean = statistics.mean(prices)

        numerator = sum((i - x_mean) * (p - y_mean) for i, p in enumerate(prices))
        denominator = sum((i - x_mean) ** 2 for i in range(n))

        if denominator == 0 or y_mean == 0:
            return 0.0

        slope = numerator / denominator

        # Normalize: slope as percentage of mean price, then clamp to [-1, 1]
        normalized = (slope / y_mean) * 100  # Percentage per tick
        return max(-1.0, min(1.0, normalized))

    def _compute_absorption(self, trades: list[NormalizedTrade]) -> float:
        """Absorption: high volume with low price change = strong absorption.

        Measures how much volume was required to move price. High absorption
        means one side is absorbing flow (support or resistance). Returns [0, 1].
        """
        if len(trades) < 10:
            return 0.5  # Neutral when insufficient data

        prices = [t.price for t in trades]
        volumes = [t.size for t in trades]

        total_volume = sum(volumes)
        price_range = max(prices) - min(prices)
        avg_price = statistics.mean(prices)

        if total_volume == 0 or avg_price == 0:
            return 0.5

        # Price range as a fraction of average price
        range_pct = price_range / avg_price

        if range_pct == 0:
            return 1.0  # Massive absorption: lots of volume, zero price change

        # Volume-to-movement ratio, normalized
        # Higher ratio = more absorption (lots of volume for small moves)
        vol_per_pct_move = total_volume / (range_pct * 10000)

        # Sigmoid normalization to [0, 1]
        # Typical values: 1000-100000 shares per 1% move
        absorption = 1.0 / (1.0 + math.exp(-0.00005 * (vol_per_pct_move - 50000)))

        return absorption

    def _compute_spread_dynamics(self, books: list[NormalizedBookSnapshot]) -> float:
        """Spread trend: is spread narrowing or widening?

        Returns [-1, +1]. Negative = narrowing (improving), positive = widening (stress).
        """
        if len(books) < 3:
            return 0.0

        spreads = [b.spread for b in books if b.spread > 0]
        if len(spreads) < 3:
            return 0.0

        # Compare recent avg to older avg
        midpoint = len(spreads) // 2
        if midpoint == 0:
            return 0.0

        old_avg = statistics.mean(spreads[:midpoint])
        new_avg = statistics.mean(spreads[midpoint:])

        if old_avg == 0:
            return 0.0

        # Change as fraction of old average, clamped to [-1, 1]
        change = (new_avg - old_avg) / old_avg
        return max(-1.0, min(1.0, change))

    # ──────────────────────────────────────────────────────────────────
    # Confidence estimation
    # ──────────────────────────────────────────────────────────────────

    def _compute_confidence(
        self,
        trades: list[NormalizedTrade],
        books: list[NormalizedBookSnapshot],
    ) -> float:
        """Estimate confidence based on data coverage.

        Low trade count or missing book data reduces confidence.
        """
        trade_score = min(1.0, len(trades) / 50)   # Full confidence at 50+ trades
        book_score = min(1.0, len(books) / 10)      # Full confidence at 10+ snapshots
        side_known = sum(1 for t in trades if t.side != "unknown")
        side_score = side_known / len(trades) if trades else 0.0

        # Weighted: trades matter most, then book, then side quality
        confidence = trade_score * 0.4 + book_score * 0.3 + side_score * 0.3
        return round(min(1.0, confidence), 3)

    # ──────────────────────────────────────────────────────────────────
    # State machine
    # ──────────────────────────────────────────────────────────────────

    def _update_state_machine(
        self,
        state: _SymbolState,
        score: float,
        nofi: float,
        velocity: float,
    ) -> MomentumState:
        """Update momentum state machine. Requires persistence before transitioning."""
        current = state.current_state

        # Check for reversal: NOFI and velocity both flip sign
        nofi_flipped = (state.prev_nofi > 0.1 and nofi < -0.1) or \
                       (state.prev_nofi < -0.1 and nofi > 0.1)
        vel_flipped = (state.prev_velocity > 0.1 and velocity < -0.1) or \
                      (state.prev_velocity < -0.1 and velocity > 0.1)

        if nofi_flipped and vel_flipped:
            candidate = MomentumState.REVERSING
        elif current == MomentumState.NEUTRAL and score >= BUILDING_ENTRY_SCORE:
            candidate = MomentumState.BUILDING
        elif current == MomentumState.BUILDING and score >= PEAKED_ENTRY_SCORE and velocity < state.prev_velocity:
            candidate = MomentumState.PEAKED
        elif current == MomentumState.PEAKED and score < DECAYING_ENTRY_SCORE:
            candidate = MomentumState.DECAYING
        elif current == MomentumState.DECAYING and score < NEUTRAL_ENTRY_SCORE:
            candidate = MomentumState.NEUTRAL
        elif current == MomentumState.REVERSING and score < NEUTRAL_ENTRY_SCORE:
            candidate = MomentumState.NEUTRAL
        elif current == MomentumState.REVERSING and score >= BUILDING_ENTRY_SCORE:
            candidate = MomentumState.BUILDING
        else:
            # Stay in current state
            state.candidate_state = None
            state.state_tick_count = 0
            return current

        # Require persistence before transitioning
        if candidate == state.candidate_state:
            state.state_tick_count += 1
        else:
            state.candidate_state = candidate
            state.state_tick_count = 1

        if state.state_tick_count >= STATE_PERSISTENCE_TICKS:
            state.current_state = candidate
            state.candidate_state = None
            state.state_tick_count = 0
            return candidate

        return current

    # ──────────────────────────────────────────────────────────────────
    # Status / diagnostics
    # ──────────────────────────────────────────────────────────────────

    def status(self) -> dict:
        """Return engine status for API endpoint."""
        snapshots = self.get_all_snapshots()
        return {
            "tracked_symbols": len(self._symbols),
            "symbols_with_snapshots": len(snapshots),
            "symbols": {
                sym: snap.to_dict() for sym, snap in snapshots.items()
            },
        }
