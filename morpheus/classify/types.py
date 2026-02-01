"""MASS Strategy Classifier types."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from morpheus.core.events import Event, EventType, create_event


class StrategyCode(str, Enum):
    """MASS strategy codes."""

    PMB = "premarket_breakout"
    VWAP = "vwap_reclaim"
    CAT = "catalyst_momentum"
    D2 = "day2_continuation"
    COIL = "coil_breakout"
    SQZ = "short_squeeze"
    FADE = "gap_fade"
    SCALP = "order_flow_scalp"


@dataclass(frozen=True)
class ClassificationResult:
    """
    Result of strategy classification for a symbol.

    Contains the optimal strategy assignment(s) and supporting context.
    Immutable to ensure determinism.
    """

    symbol: str = ""
    primary_strategy: StrategyCode | None = None
    assigned_strategies: tuple[StrategyCode, ...] = field(default_factory=tuple)

    # Context
    structure_grade: str = ""  # "A"/"B"/"C"
    regime_mode: str = ""  # From RegimeClassification primary_regime
    market_mode: str = ""  # "PREMARKET"/"RTH"/"OFFHOURS"
    confidence: float = 0.0
    rationale: str = ""

    # Per-strategy fit scores
    strategy_scores: dict[str, float] = field(default_factory=dict)

    def to_event_payload(self) -> dict[str, Any]:
        """Convert to event payload dict."""
        return {
            "symbol": self.symbol,
            "primary_strategy": self.primary_strategy.value if self.primary_strategy else None,
            "assigned_strategies": [s.value for s in self.assigned_strategies],
            "structure_grade": self.structure_grade,
            "regime_mode": self.regime_mode,
            "market_mode": self.market_mode,
            "confidence": self.confidence,
            "rationale": self.rationale,
            "strategy_scores": self.strategy_scores,
        }

    def to_event(self) -> Event:
        """Create a STRATEGY_ASSIGNED event."""
        return create_event(
            EventType.STRATEGY_ASSIGNED,
            payload=self.to_event_payload(),
            symbol=self.symbol,
        )
