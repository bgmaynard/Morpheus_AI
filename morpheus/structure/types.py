"""MASS Structure types - StructureGrade frozen dataclass."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from morpheus.core.events import Event, EventType, create_event


@dataclass(frozen=True)
class StructureGrade:
    """
    A/B/C quality classification for a symbol's trading structure.

    Immutable to ensure determinism.

    Grade interpretation:
    - A (>= 75): Strong setup - prime for trading
    - B (>= 50): Tradable setup - acceptable quality
    - C (< 50): Marginal/Avoid - poor structure
    """

    grade: str = "C"  # "A", "B", "C"
    score: float = 0.0  # 0-100 composite score

    # Component scores
    vwap_position: str = ""  # "above", "below", "at"
    vwap_score: float = 0.0

    ema_stack: str = ""  # "bullish", "bearish", "mixed"
    ema_score: float = 0.0

    trend_slope: float = 0.0  # Normalized -1 to 1
    trend_score: float = 0.0

    sr_levels: tuple[float, ...] = field(default_factory=tuple)
    sr_score: float = 0.0

    liquidity_score: float = 0.0
    spread_pct: float = 0.0

    volume_score: float = 0.0
    relative_volume: float = 0.0

    rationale: str = ""
    tags: tuple[str, ...] = field(default_factory=tuple)

    def to_event_payload(self) -> dict[str, Any]:
        """Convert to event payload dict."""
        return {
            "grade": self.grade,
            "score": self.score,
            "vwap_position": self.vwap_position,
            "vwap_score": self.vwap_score,
            "ema_stack": self.ema_stack,
            "ema_score": self.ema_score,
            "trend_slope": self.trend_slope,
            "trend_score": self.trend_score,
            "sr_levels": list(self.sr_levels),
            "sr_score": self.sr_score,
            "liquidity_score": self.liquidity_score,
            "spread_pct": self.spread_pct,
            "volume_score": self.volume_score,
            "relative_volume": self.relative_volume,
            "rationale": self.rationale,
            "tags": list(self.tags),
        }

    def to_event(self, symbol: str) -> Event:
        """Create a STRUCTURE_CLASSIFIED event."""
        return create_event(
            EventType.STRUCTURE_CLASSIFIED,
            payload=self.to_event_payload(),
            symbol=symbol,
        )
