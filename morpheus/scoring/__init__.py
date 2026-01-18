"""Signal scoring and meta-gating (AI judgment layer)."""

from morpheus.scoring.base import (
    SCORING_SCHEMA_VERSION,
    GateDecision,
    RejectionReason,
    ScoredSignal,
    GateResult,
    Scorer,
    MetaGate,
)
from morpheus.scoring.stub_scorer import (
    StubScorerConfig,
    StubScorer,
    create_stub_scorer,
)
from morpheus.scoring.meta_gate import (
    StandardGateConfig,
    StandardMetaGate,
    PermissiveMetaGate,
    StrictMetaGate,
    create_standard_gate,
    create_permissive_gate,
    create_strict_gate,
)

__all__ = [
    # Base
    "SCORING_SCHEMA_VERSION",
    "GateDecision",
    "RejectionReason",
    "ScoredSignal",
    "GateResult",
    "Scorer",
    "MetaGate",
    # Stub Scorer
    "StubScorerConfig",
    "StubScorer",
    "create_stub_scorer",
    # Meta Gates
    "StandardGateConfig",
    "StandardMetaGate",
    "PermissiveMetaGate",
    "StrictMetaGate",
    "create_standard_gate",
    "create_permissive_gate",
    "create_strict_gate",
]
