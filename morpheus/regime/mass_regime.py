"""
MASS Regime Mapper - Maps existing regime classification to MASS regime modes.

Translates the 3-component regime (trend/volatility/momentum) into
simplified MASS modes that control system aggression.

Modes:
- HOT: Strong trend + high vol + strong momentum -> aggressive
- NORMAL: Trending + normal vol -> standard
- CHOP: Ranging + any vol + neutral momentum -> defensive
- DEAD: Low vol + ranging + neutral -> no trading

All mapping is DETERMINISTIC: same RegimeClassification -> same MASSRegime.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from morpheus.core.events import Event, EventType, create_event
from morpheus.regime.regime_detector import (
    RegimeClassification,
    TrendRegime,
    VolatilityRegime,
    MomentumRegime,
)

logger = logging.getLogger(__name__)


class MASSRegimeMode(str, Enum):
    """MASS simplified regime modes."""

    HOT = "HOT"        # Strong conditions -> aggressive trading
    NORMAL = "NORMAL"  # Standard conditions -> normal trading
    CHOP = "CHOP"      # Range-bound -> defensive, scalps only
    DEAD = "DEAD"      # No edge -> halt trading


@dataclass(frozen=True)
class MASSRegime:
    """
    MASS regime classification result.

    Controls system-wide aggression, position limits, and strategy selection.
    """

    mode: MASSRegimeMode = MASSRegimeMode.NORMAL
    aggression_multiplier: float = 1.0  # 0.5 (DEAD) to 1.5 (HOT)
    max_concurrent_positions: int = 3
    allowed_strategy_types: tuple[str, ...] = field(default_factory=tuple)
    rationale: str = ""

    # Source classification
    source_trend: str = ""
    source_volatility: str = ""
    source_momentum: str = ""
    source_confidence: float = 0.0

    def to_event_payload(self) -> dict[str, Any]:
        """Convert to event payload dict."""
        return {
            "mode": self.mode.value,
            "aggression_multiplier": self.aggression_multiplier,
            "max_concurrent_positions": self.max_concurrent_positions,
            "allowed_strategy_types": list(self.allowed_strategy_types),
            "rationale": self.rationale,
            "source_trend": self.source_trend,
            "source_volatility": self.source_volatility,
            "source_momentum": self.source_momentum,
            "source_confidence": self.source_confidence,
        }


class MASSRegimeMapper:
    """
    Maps existing RegimeClassification to MASS regime modes.

    Deterministic: same classification -> same MASS regime.
    """

    def map(self, classification: RegimeClassification) -> MASSRegime:
        """
        Map a RegimeClassification to a MASSRegime.

        Mapping logic:
        - HOT: trending + high_vol + strong momentum
        - NORMAL: trending + normal_vol or moderate momentum
        - CHOP: ranging + any vol + neutral/weak momentum
        - DEAD: low_vol + ranging + neutral
        """
        trend = classification.trend
        vol = classification.volatility
        mom = classification.momentum

        # HOT: Strong directional move with volume and momentum
        if (
            trend in (TrendRegime.TRENDING_UP, TrendRegime.TRENDING_DOWN)
            and vol in (VolatilityRegime.HIGH_VOLATILITY, VolatilityRegime.NORMAL_VOLATILITY)
            and mom in (MomentumRegime.STRONG_BULLISH, MomentumRegime.STRONG_BEARISH)
        ):
            mode = MASSRegimeMode.HOT
            aggression = 1.5
            max_pos = 5
            strategies = (
                "premarket_breakout", "catalyst_momentum", "short_squeeze",
                "day2_continuation", "coil_breakout",
            )
            rationale = "HOT: strong trend + volume + momentum"

        # DEAD: Low volatility, no trend, no momentum
        elif (
            vol == VolatilityRegime.LOW_VOLATILITY
            and trend == TrendRegime.RANGING
            and mom == MomentumRegime.NEUTRAL
        ):
            mode = MASSRegimeMode.DEAD
            aggression = 0.5
            max_pos = 0
            strategies = ()
            rationale = "DEAD: no vol, no trend, no momentum"

        # CHOP: Ranging market, possibly volatile but no direction
        elif (
            trend == TrendRegime.RANGING
            or mom == MomentumRegime.NEUTRAL
        ):
            mode = MASSRegimeMode.CHOP
            aggression = 0.7
            max_pos = 2
            strategies = (
                "vwap_reclaim", "order_flow_scalp", "gap_fade",
            )
            rationale = "CHOP: ranging or neutral momentum"

        # NORMAL: Everything else (trending with moderate conditions)
        else:
            mode = MASSRegimeMode.NORMAL
            aggression = 1.0
            max_pos = 3
            strategies = (
                "catalyst_momentum", "day2_continuation",
                "coil_breakout", "vwap_reclaim", "gap_fade",
            )
            rationale = "NORMAL: standard trending conditions"

        result = MASSRegime(
            mode=mode,
            aggression_multiplier=aggression,
            max_concurrent_positions=max_pos,
            allowed_strategy_types=strategies,
            rationale=rationale,
            source_trend=trend.value,
            source_volatility=vol.value,
            source_momentum=mom.value,
            source_confidence=classification.confidence,
        )

        logger.debug(
            f"[MASS_REGIME] {mode.value} (aggression={aggression}, "
            f"max_pos={max_pos}): {rationale}"
        )

        return result
