"""
Scoring Base Contract - Defines signal scoring and meta-gating interface.

Scorers evaluate SignalCandidate objects and produce confidence scores.
Meta-gates approve or reject scored signals based on configurable rules.

All scoring is DETERMINISTIC: same input -> same score.

Phase 5 Scope:
- Signal scoring only
- Meta-gate decisions only
- No position sizing
- No risk overlays
- No order execution
- No FSM transitions
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from morpheus.core.events import Event, EventType, create_event
from morpheus.features.feature_engine import FeatureContext
from morpheus.strategies.base import SignalCandidate, SignalDirection


# Schema version for scored signals
SCORING_SCHEMA_VERSION = "1.0"


class GateDecision(Enum):
    """Meta-gate decision outcomes."""

    APPROVED = "approved"
    REJECTED = "rejected"
    PENDING = "pending"  # Needs more information


class RejectionReason(Enum):
    """Standard rejection reason codes."""

    LOW_CONFIDENCE = "low_confidence"
    REGIME_MISMATCH = "regime_mismatch"
    FEATURE_QUALITY = "feature_quality"
    WARMUP_INCOMPLETE = "warmup_incomplete"
    MARKET_CLOSED = "market_closed"
    VOLATILITY_EXTREME = "volatility_extreme"
    CONFLICTING_SIGNALS = "conflicting_signals"
    RATE_LIMITED = "rate_limited"
    MODEL_UNCERTAINTY = "model_uncertainty"
    MANUAL_BLOCK = "manual_block"


@dataclass(frozen=True)
class ScoredSignal:
    """
    A signal candidate with AI-assigned confidence score.

    Contains:
    - Original SignalCandidate
    - Probability/confidence (0-1)
    - Model-generated rationale
    - Scoring metadata

    Immutable to ensure determinism.
    Does NOT contain:
    - Position sizing (Phase 6)
    - Risk parameters (Phase 6)
    - Execution details (Phase 7+)
    """

    schema_version: str = SCORING_SCHEMA_VERSION

    # Original signal
    signal: SignalCandidate | None = None

    # Scoring results
    confidence: float = 0.0  # 0-1 probability
    model_name: str = ""
    model_version: str = ""

    # Rationale from model
    score_rationale: str = ""
    contributing_factors: tuple[str, ...] = field(default_factory=tuple)

    # Timestamp
    scored_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # Feature values used for scoring (for audit/replay)
    feature_snapshot: dict[str, float | None] = field(default_factory=dict)

    def to_event_payload(self) -> dict[str, Any]:
        """Convert to event payload dict."""
        return {
            "schema_version": self.schema_version,
            "signal": self.signal.to_event_payload() if self.signal else None,
            "confidence": self.confidence,
            "model_name": self.model_name,
            "model_version": self.model_version,
            "score_rationale": self.score_rationale,
            "contributing_factors": list(self.contributing_factors),
            "scored_at": self.scored_at.isoformat(),
            "feature_snapshot": self.feature_snapshot,
        }

    def to_event(self) -> Event:
        """Create a SIGNAL_SCORED event."""
        return create_event(
            EventType.SIGNAL_SCORED,
            payload=self.to_event_payload(),
            symbol=self.signal.symbol if self.signal else "",
            timestamp=self.scored_at,
        )

    @property
    def symbol(self) -> str:
        """Get symbol from underlying signal."""
        return self.signal.symbol if self.signal else ""

    @property
    def direction(self) -> SignalDirection:
        """Get direction from underlying signal."""
        return self.signal.direction if self.signal else SignalDirection.NONE

    @property
    def strategy_name(self) -> str:
        """Get strategy name from underlying signal."""
        return self.signal.strategy_name if self.signal else ""


@dataclass(frozen=True)
class GateResult:
    """
    Result of meta-gate evaluation.

    Contains:
    - Decision (approved/rejected/pending)
    - Reason codes
    - Gate metadata
    """

    schema_version: str = SCORING_SCHEMA_VERSION

    # The scored signal being evaluated
    scored_signal: ScoredSignal | None = None

    # Decision
    decision: GateDecision = GateDecision.PENDING

    # Reasons (for rejection or notes)
    reasons: tuple[RejectionReason, ...] = field(default_factory=tuple)
    reason_details: str = ""

    # Gate configuration used
    gate_name: str = ""
    gate_version: str = ""

    # Thresholds that were applied
    min_confidence_required: float = 0.0
    confidence_met: bool = False

    # Timestamp
    evaluated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_event_payload(self) -> dict[str, Any]:
        """Convert to event payload dict."""
        return {
            "schema_version": self.schema_version,
            "scored_signal": self.scored_signal.to_event_payload() if self.scored_signal else None,
            "decision": self.decision.value,
            "reasons": [r.value for r in self.reasons],
            "reason_details": self.reason_details,
            "gate_name": self.gate_name,
            "gate_version": self.gate_version,
            "min_confidence_required": self.min_confidence_required,
            "confidence_met": self.confidence_met,
            "evaluated_at": self.evaluated_at.isoformat(),
        }

    def to_event(self) -> Event:
        """Create appropriate gate event based on decision."""
        if self.decision == GateDecision.APPROVED:
            event_type = EventType.META_APPROVED
        else:
            event_type = EventType.META_REJECTED

        return create_event(
            event_type,
            payload=self.to_event_payload(),
            symbol=self.scored_signal.symbol if self.scored_signal else "",
            timestamp=self.evaluated_at,
        )

    @property
    def is_approved(self) -> bool:
        """Check if signal was approved."""
        return self.decision == GateDecision.APPROVED

    @property
    def is_rejected(self) -> bool:
        """Check if signal was rejected."""
        return self.decision == GateDecision.REJECTED


class Scorer(ABC):
    """
    Abstract base class for signal scorers.

    Scorers:
    - Evaluate SignalCandidate objects
    - Produce confidence scores (0-1)
    - Are deterministic (same input -> same score)
    - Do NOT make trade decisions
    - Do NOT execute orders

    Each scorer must implement:
    - name: Unique identifier
    - version: Model version string
    - score: Evaluate signal and return ScoredSignal
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique name identifying this scorer."""
        pass

    @property
    @abstractmethod
    def version(self) -> str:
        """Version string for this scorer."""
        pass

    @property
    def description(self) -> str:
        """Human-readable description of the scorer."""
        return ""

    @abstractmethod
    def score(
        self,
        signal: SignalCandidate,
        features: FeatureContext,
    ) -> ScoredSignal:
        """
        Score a signal candidate.

        Args:
            signal: SignalCandidate to score
            features: FeatureContext with computed features

        Returns:
            ScoredSignal with confidence and rationale

        This method MUST:
        - Be deterministic
        - Not have side effects
        - Not access external state
        - Return a valid ScoredSignal
        """
        pass

    def create_scored_signal(
        self,
        signal: SignalCandidate,
        confidence: float,
        rationale: str,
        contributing_factors: tuple[str, ...] = (),
        feature_snapshot: dict[str, float | None] | None = None,
    ) -> ScoredSignal:
        """
        Helper to create a ScoredSignal with common fields populated.

        Args:
            signal: Original SignalCandidate
            confidence: Confidence score (0-1)
            rationale: Explanation for the score
            contributing_factors: Features/factors that influenced score
            feature_snapshot: Feature values used for scoring

        Returns:
            ScoredSignal with all fields populated
        """
        return ScoredSignal(
            schema_version=SCORING_SCHEMA_VERSION,
            signal=signal,
            confidence=max(0.0, min(1.0, confidence)),  # Clamp to [0, 1]
            model_name=self.name,
            model_version=self.version,
            score_rationale=rationale,
            contributing_factors=contributing_factors,
            scored_at=datetime.now(timezone.utc),
            feature_snapshot=feature_snapshot or {},
        )


class MetaGate(ABC):
    """
    Abstract base class for meta-gates.

    Meta-gates:
    - Evaluate ScoredSignal objects
    - Approve or reject based on configurable rules
    - Are deterministic (same input -> same decision)
    - Do NOT size positions
    - Do NOT execute orders

    Each meta-gate must implement:
    - name: Unique identifier
    - version: Gate version string
    - evaluate: Decide on a scored signal
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique name identifying this gate."""
        pass

    @property
    @abstractmethod
    def version(self) -> str:
        """Version string for this gate."""
        pass

    @property
    def description(self) -> str:
        """Human-readable description of the gate."""
        return ""

    @abstractmethod
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

        This method MUST:
        - Be deterministic
        - Not have side effects
        - Not access external state
        - Return a valid GateResult
        """
        pass

    def create_approved(
        self,
        scored_signal: ScoredSignal,
        min_confidence: float,
        details: str = "",
    ) -> GateResult:
        """Helper to create an approved GateResult."""
        return GateResult(
            schema_version=SCORING_SCHEMA_VERSION,
            scored_signal=scored_signal,
            decision=GateDecision.APPROVED,
            reasons=(),
            reason_details=details,
            gate_name=self.name,
            gate_version=self.version,
            min_confidence_required=min_confidence,
            confidence_met=True,
            evaluated_at=datetime.now(timezone.utc),
        )

    def create_rejected(
        self,
        scored_signal: ScoredSignal,
        reasons: tuple[RejectionReason, ...],
        min_confidence: float,
        details: str = "",
    ) -> GateResult:
        """Helper to create a rejected GateResult."""
        return GateResult(
            schema_version=SCORING_SCHEMA_VERSION,
            scored_signal=scored_signal,
            decision=GateDecision.REJECTED,
            reasons=reasons,
            reason_details=details,
            gate_name=self.name,
            gate_version=self.version,
            min_confidence_required=min_confidence,
            confidence_met=scored_signal.confidence >= min_confidence if scored_signal else False,
            evaluated_at=datetime.now(timezone.utc),
        )
