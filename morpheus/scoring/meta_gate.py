"""
Meta Gate - Signal approval/rejection logic.

Evaluates ScoredSignal objects and makes approve/reject decisions
based on configurable rules and thresholds.

All gating is DETERMINISTIC: same input -> same decision.

Phase 5 Scope:
- Approval/rejection decisions only
- No position sizing
- No risk overlays
- No order execution
"""

from __future__ import annotations

from dataclasses import dataclass

from morpheus.features.feature_engine import FeatureContext
from morpheus.scoring.base import (
    MetaGate,
    ScoredSignal,
    GateResult,
    GateDecision,
    RejectionReason,
    SCORING_SCHEMA_VERSION,
)


@dataclass(frozen=True)
class StandardGateConfig:
    """
    Configuration for the standard meta-gate.

    Defines thresholds and rules for approval decisions.
    """

    # Minimum confidence to approve
    min_confidence: float = 0.5

    # Require warmup complete
    require_warmup: bool = True

    # Require market open
    require_market_open: bool = True

    # Maximum volatility for mean reversion signals
    max_volatility_for_mr: float = 3.5

    # Regime alignment required
    require_regime_alignment: bool = True

    # Minimum regime confidence
    min_regime_confidence: float = 0.4


class StandardMetaGate(MetaGate):
    """
    Standard meta-gate implementation.

    Applies a series of checks to decide approval:
    1. Confidence threshold
    2. Warmup status
    3. Market hours
    4. Regime alignment
    5. Volatility checks (for specific strategies)
    6. Feature quality

    All checks are deterministic.
    """

    def __init__(self, config: StandardGateConfig | None = None):
        """
        Initialize the meta-gate.

        Args:
            config: Optional custom configuration
        """
        self.config = config or StandardGateConfig()

    @property
    def name(self) -> str:
        return "standard_meta_gate"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return "Standard rule-based meta-gate with configurable thresholds"

    def evaluate(
        self,
        scored_signal: ScoredSignal,
        features: FeatureContext,
    ) -> GateResult:
        """
        Evaluate a scored signal for approval.

        Args:
            scored_signal: ScoredSignal to evaluate
            features: FeatureContext for additional checks

        Returns:
            GateResult with decision and reasons
        """
        cfg = self.config
        reasons: list[RejectionReason] = []
        details_parts: list[str] = []

        # Check 1: Confidence threshold
        confidence_met = scored_signal.confidence >= cfg.min_confidence
        if not confidence_met:
            reasons.append(RejectionReason.LOW_CONFIDENCE)
            details_parts.append(
                f"Confidence {scored_signal.confidence:.2f} < {cfg.min_confidence:.2f}"
            )

        # Check 2: Warmup status
        if cfg.require_warmup and not features.warmup_complete:
            reasons.append(RejectionReason.WARMUP_INCOMPLETE)
            details_parts.append(
                f"Warmup incomplete ({features.bars_available} bars)"
            )

        # Check 3: Market hours (from snapshot if available)
        if cfg.require_market_open and features.snapshot:
            if not features.snapshot.is_market_open:
                reasons.append(RejectionReason.MARKET_CLOSED)
                details_parts.append("Market is closed")

        # Check 4: Regime alignment
        if cfg.require_regime_alignment:
            regime_aligned = self._check_regime_alignment(scored_signal, features)
            if not regime_aligned:
                reasons.append(RejectionReason.REGIME_MISMATCH)
                details_parts.append(
                    f"Signal direction misaligned with regime '{features.regime}'"
                )

        # Check 5: Regime confidence
        if features.regime_confidence < cfg.min_regime_confidence:
            reasons.append(RejectionReason.MODEL_UNCERTAINTY)
            details_parts.append(
                f"Regime confidence {features.regime_confidence:.2f} < {cfg.min_regime_confidence:.2f}"
            )

        # Check 6: Volatility for mean reversion
        if scored_signal.signal:
            signal_tags = scored_signal.signal.tags
            if "mean_reversion" in signal_tags or "vwap" in signal_tags:
                atr_pct = features.features.get("atr_pct")
                if atr_pct is not None and atr_pct > cfg.max_volatility_for_mr:
                    reasons.append(RejectionReason.VOLATILITY_EXTREME)
                    details_parts.append(
                        f"Volatility {atr_pct:.2f}% too high for mean reversion"
                    )

        # Check 7: Feature quality
        feature_quality_ok = self._check_feature_quality(features)
        if not feature_quality_ok:
            reasons.append(RejectionReason.FEATURE_QUALITY)
            details_parts.append("Critical features missing or invalid")

        # Make decision
        if not reasons:
            return self.create_approved(
                scored_signal=scored_signal,
                min_confidence=cfg.min_confidence,
                details=f"All checks passed. Confidence: {scored_signal.confidence:.2f}",
            )
        else:
            return self.create_rejected(
                scored_signal=scored_signal,
                reasons=tuple(reasons),
                min_confidence=cfg.min_confidence,
                details="; ".join(details_parts),
            )

    def _check_regime_alignment(
        self,
        scored_signal: ScoredSignal,
        features: FeatureContext,
    ) -> bool:
        """
        Check if signal direction aligns with current regime.

        Returns True if:
        - LONG signal in trending_up regime
        - SHORT signal in trending_down regime
        - Any direction in ranging regime (for MR strategies)
        - No regime specified (pass through)
        """
        if not scored_signal.signal:
            return True

        regime = features.regime or ""
        if not regime:
            return True  # No regime info, allow

        from morpheus.strategies.base import SignalDirection

        direction = scored_signal.direction

        if direction == SignalDirection.LONG:
            if "trending_up" in regime or "ranging" in regime:
                return True
            return False

        elif direction == SignalDirection.SHORT:
            if "trending_down" in regime or "ranging" in regime:
                return True
            return False

        return True  # NONE direction always "aligned"

    def _check_feature_quality(self, features: FeatureContext) -> bool:
        """
        Check if critical features are present and valid.

        Returns False if essential features are missing.
        """
        critical_features = ["rsi_14", "atr_pct", "trend_strength"]

        for feature in critical_features:
            value = features.features.get(feature)
            if value is None:
                return False

        return True


class PermissiveMetaGate(MetaGate):
    """
    Permissive meta-gate that approves most signals.

    Useful for testing and paper trading where you want
    to see all signals without filtering.

    Only rejects:
    - Zero confidence signals
    - Signals with no direction (NONE)
    """

    @property
    def name(self) -> str:
        return "permissive_meta_gate"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return "Permissive gate that approves most signals (for testing)"

    def evaluate(
        self,
        scored_signal: ScoredSignal,
        features: FeatureContext,
    ) -> GateResult:
        """Approve almost everything."""
        from morpheus.strategies.base import SignalDirection

        # Reject zero confidence
        if scored_signal.confidence <= 0:
            return self.create_rejected(
                scored_signal=scored_signal,
                reasons=(RejectionReason.LOW_CONFIDENCE,),
                min_confidence=0.01,
                details="Zero confidence signal",
            )

        # Reject NONE direction
        if scored_signal.direction == SignalDirection.NONE:
            return self.create_rejected(
                scored_signal=scored_signal,
                reasons=(RejectionReason.LOW_CONFIDENCE,),
                min_confidence=0.01,
                details="No actionable direction",
            )

        return self.create_approved(
            scored_signal=scored_signal,
            min_confidence=0.01,
            details=f"Permissive approval. Confidence: {scored_signal.confidence:.2f}",
        )


class StrictMetaGate(MetaGate):
    """
    Strict meta-gate with high thresholds.

    For production use where you want high-confidence signals only.
    """

    def __init__(self, min_confidence: float = 0.7):
        """
        Initialize with minimum confidence threshold.

        Args:
            min_confidence: Minimum confidence to approve (default 0.7)
        """
        self._min_confidence = min_confidence

    @property
    def name(self) -> str:
        return "strict_meta_gate"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return f"Strict gate requiring {self._min_confidence:.0%} confidence"

    def evaluate(
        self,
        scored_signal: ScoredSignal,
        features: FeatureContext,
    ) -> GateResult:
        """Apply strict confidence threshold."""
        from morpheus.strategies.base import SignalDirection

        reasons: list[RejectionReason] = []
        details: list[str] = []

        # Check direction
        if scored_signal.direction == SignalDirection.NONE:
            reasons.append(RejectionReason.LOW_CONFIDENCE)
            details.append("No actionable direction")

        # Check confidence
        if scored_signal.confidence < self._min_confidence:
            reasons.append(RejectionReason.LOW_CONFIDENCE)
            details.append(
                f"Confidence {scored_signal.confidence:.2f} < {self._min_confidence:.2f}"
            )

        # Check warmup
        if not features.warmup_complete:
            reasons.append(RejectionReason.WARMUP_INCOMPLETE)
            details.append("Warmup incomplete")

        # Check regime confidence
        if features.regime_confidence < 0.6:
            reasons.append(RejectionReason.MODEL_UNCERTAINTY)
            details.append(f"Low regime confidence: {features.regime_confidence:.2f}")

        if reasons:
            return self.create_rejected(
                scored_signal=scored_signal,
                reasons=tuple(reasons),
                min_confidence=self._min_confidence,
                details="; ".join(details),
            )

        return self.create_approved(
            scored_signal=scored_signal,
            min_confidence=self._min_confidence,
            details=f"Strict approval. Confidence: {scored_signal.confidence:.2f}",
        )


def create_standard_gate(config: StandardGateConfig | None = None) -> StandardMetaGate:
    """Factory function to create a StandardMetaGate."""
    return StandardMetaGate(config)


def create_permissive_gate() -> PermissiveMetaGate:
    """Factory function to create a PermissiveMetaGate."""
    return PermissiveMetaGate()


def create_strict_gate(min_confidence: float = 0.7) -> StrictMetaGate:
    """Factory function to create a StrictMetaGate."""
    return StrictMetaGate(min_confidence)
