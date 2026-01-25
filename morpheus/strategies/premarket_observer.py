"""
Premarket Observer Strategy - Structure Classification (OBSERVE-ONLY).

This strategy runs during PREMARKET (07:00-09:30 ET) and classifies
symbol behavior without generating actionable signals.

Purpose:
- Observe and log premarket structure
- Classify gap behavior, volume profile, price action
- Build context for RTH decision-making
- NO EXECUTION - all signals are OBSERVED mode

This is a SAFE strategy for Monday testing.
"""

from __future__ import annotations

from enum import Enum
from morpheus.strategies.base import (
    Strategy,
    StrategyContext,
    SignalCandidate,
    SignalDirection,
    SignalMode,
)
from morpheus.core.market_mode import get_market_mode, get_time_et


class PremarketStructure(Enum):
    """Classification of premarket price structure."""

    GAP_UP_HOLDING = "gap_up_holding"  # Gapped up and holding above gap
    GAP_UP_FADING = "gap_up_fading"  # Gapped up but fading back
    GAP_DOWN_HOLDING = "gap_down_holding"  # Gapped down and holding below
    GAP_DOWN_RECOVERING = "gap_down_recovering"  # Gapped down but recovering
    FLAT_CONSOLIDATING = "flat_consolidating"  # No significant gap, consolidating
    HIGH_VOLATILITY = "high_volatility"  # Erratic premarket action
    INSUFFICIENT_DATA = "insufficient_data"  # Not enough data to classify


class PremarketObserverStrategy(Strategy):
    """
    Premarket structure observer strategy.

    Classifies symbol behavior during premarket without generating
    actionable signals. All signals are OBSERVED mode.

    Classifications:
    - Gap behavior (up/down, holding/fading)
    - Volume profile (high/low relative to typical premarket)
    - Price structure (consolidating, trending, volatile)

    This strategy is PREMARKET-only and OBSERVE-only.
    """

    @property
    def name(self) -> str:
        return "PremarketStructureObserver"

    @property
    def description(self) -> str:
        return "Classifies premarket structure for context (observe-only)"

    @property
    def allowed_market_modes(self) -> frozenset[str]:
        """Only runs during PREMARKET."""
        return frozenset({"PREMARKET"})

    @property
    def required_features(self) -> tuple[str, ...]:
        """Minimal feature requirements."""
        return ("rsi_14", "atr_pct", "relative_volume")

    def evaluate(self, context: StrategyContext) -> SignalCandidate:
        """
        Classify premarket structure and emit OBSERVED signal.

        This method:
        1. Analyzes gap from prior close
        2. Classifies price action structure
        3. Assesses volume profile
        4. Emits OBSERVED signal with classification tags

        Returns:
            SignalCandidate with direction=NONE and structure tags
            (always OBSERVED mode - not actionable)
        """
        features = context.features
        snapshot = context.snapshot

        # Get current price and features
        current_price = snapshot.last
        rsi = features.get_feature("rsi_14")
        atr_pct = features.get_feature("atr_pct")
        rvol = features.get_feature("relative_volume")

        # Get external scanner context if available
        external = features.external or {}
        gap_pct = external.get("gap_pct", 0)
        scanner_score = external.get("scanner_score", 0)

        # Classify structure
        structure = self._classify_structure(
            gap_pct=gap_pct,
            rsi=rsi,
            atr_pct=atr_pct,
            rvol=rvol,
        )

        # Build classification tags
        tags = self._build_tags(structure, gap_pct, rvol, scanner_score)

        # Build rationale
        rationale = self._build_rationale(structure, gap_pct, rvol, rsi)

        # Always return OBSERVED signal (not actionable)
        # Direction is NONE - this is purely observational
        mode = context.market_mode or get_market_mode()

        return SignalCandidate(
            symbol=context.symbol,
            direction=SignalDirection.NONE,  # No trade direction - observation only
            strategy_name=self.name,
            timestamp=snapshot.timestamp,
            regime=features.regime or "",
            regime_confidence=features.regime_confidence,
            triggering_features=("gap_pct", "rsi_14", "relative_volume"),
            rationale=rationale,
            tags=tags,
            entry_reference=current_price,  # Current price for reference
            invalidation_reference=None,
            target_reference=None,
            # Always OBSERVED mode - this strategy never generates actionable signals
            market_mode=mode.name,
            signal_mode=SignalMode.OBSERVED,
            time_et=get_time_et(),
        )

    def _classify_structure(
        self,
        gap_pct: float,
        rsi: float | None,
        atr_pct: float | None,
        rvol: float | None,
    ) -> PremarketStructure:
        """Classify premarket structure based on available data."""

        # Check for insufficient data
        if rsi is None or atr_pct is None:
            return PremarketStructure.INSUFFICIENT_DATA

        # High volatility check (ATR% > 5% indicates erratic action)
        if atr_pct and atr_pct > 5.0:
            return PremarketStructure.HIGH_VOLATILITY

        # Gap classification
        if gap_pct > 3.0:  # Significant gap up
            if rsi and rsi > 50:
                return PremarketStructure.GAP_UP_HOLDING
            else:
                return PremarketStructure.GAP_UP_FADING
        elif gap_pct < -3.0:  # Significant gap down
            if rsi and rsi < 50:
                return PremarketStructure.GAP_DOWN_HOLDING
            else:
                return PremarketStructure.GAP_DOWN_RECOVERING
        else:
            # Flat/small gap
            return PremarketStructure.FLAT_CONSOLIDATING

    def _build_tags(
        self,
        structure: PremarketStructure,
        gap_pct: float,
        rvol: float | None,
        scanner_score: float,
    ) -> tuple[str, ...]:
        """Build classification tags for the signal."""
        tags = [
            "premarket_observer",
            f"structure:{structure.value}",
        ]

        # Gap size tag
        if abs(gap_pct) > 10:
            tags.append("gap:extreme")
        elif abs(gap_pct) > 5:
            tags.append("gap:large")
        elif abs(gap_pct) > 2:
            tags.append("gap:moderate")
        else:
            tags.append("gap:small")

        # Volume tag
        if rvol:
            if rvol > 3.0:
                tags.append("volume:high")
            elif rvol > 1.5:
                tags.append("volume:elevated")
            else:
                tags.append("volume:normal")

        # Scanner score tag
        if scanner_score > 80:
            tags.append("scanner:hot")
        elif scanner_score > 50:
            tags.append("scanner:warm")

        return tuple(tags)

    def _build_rationale(
        self,
        structure: PremarketStructure,
        gap_pct: float,
        rvol: float | None,
        rsi: float | None,
    ) -> str:
        """Build human-readable rationale."""
        parts = [f"Premarket structure: {structure.value}"]

        if gap_pct != 0:
            direction = "up" if gap_pct > 0 else "down"
            parts.append(f"Gap {direction} {abs(gap_pct):.1f}%")

        if rvol:
            parts.append(f"RVOL {rvol:.1f}x")

        if rsi:
            parts.append(f"RSI {rsi:.0f}")

        return " | ".join(parts)


def get_premarket_strategies() -> list[Strategy]:
    """Get all premarket observer strategies."""
    return [
        PremarketObserverStrategy(),
    ]
