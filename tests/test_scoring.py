"""
Tests for Signal Scoring and Meta-Gating.

Phase 5 test coverage:
- ScoredSignal creation and immutability
- Scorer base contract
- StubScorer rule-based scoring
- Meta-gate approval/rejection logic
- Event emission
- Isolation (no execution/risk imports)
- Determinism (same input -> same score)
"""

import pytest
from datetime import datetime, timezone

from morpheus.data.market_snapshot import create_snapshot
from morpheus.features.feature_engine import FeatureContext
from morpheus.strategies.base import SignalCandidate, SignalDirection
from morpheus.scoring.base import (
    SCORING_SCHEMA_VERSION,
    GateDecision,
    RejectionReason,
    ScoredSignal,
    GateResult,
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
from morpheus.core.events import EventType


class TestGateDecision:
    """Tests for GateDecision enum."""

    def test_decision_values(self):
        """GateDecision should have expected values."""
        assert GateDecision.APPROVED.value == "approved"
        assert GateDecision.REJECTED.value == "rejected"
        assert GateDecision.PENDING.value == "pending"


class TestRejectionReason:
    """Tests for RejectionReason enum."""

    def test_rejection_reasons(self):
        """RejectionReason should have expected values."""
        assert RejectionReason.LOW_CONFIDENCE.value == "low_confidence"
        assert RejectionReason.REGIME_MISMATCH.value == "regime_mismatch"
        assert RejectionReason.WARMUP_INCOMPLETE.value == "warmup_incomplete"


class TestScoredSignal:
    """Tests for ScoredSignal dataclass."""

    def _create_signal(self) -> SignalCandidate:
        """Create a test signal."""
        return SignalCandidate(
            symbol="SPY",
            direction=SignalDirection.LONG,
            strategy_name="test_strategy",
            regime="trending_up",
            rationale="Test signal",
        )

    def test_scored_signal_creation(self):
        """ScoredSignal should store all fields."""
        signal = self._create_signal()
        scored = ScoredSignal(
            signal=signal,
            confidence=0.75,
            model_name="test_model",
            model_version="1.0",
            score_rationale="High confidence based on features",
        )

        assert scored.signal == signal
        assert scored.confidence == 0.75
        assert scored.model_name == "test_model"

    def test_scored_signal_is_frozen(self):
        """ScoredSignal should be immutable."""
        scored = ScoredSignal(confidence=0.5)

        with pytest.raises(Exception):
            scored.confidence = 0.9

    def test_scored_signal_default_values(self):
        """ScoredSignal should have sensible defaults."""
        scored = ScoredSignal()

        assert scored.schema_version == SCORING_SCHEMA_VERSION
        assert scored.signal is None
        assert scored.confidence == 0.0
        assert scored.model_name == ""
        assert scored.contributing_factors == ()

    def test_scored_signal_properties(self):
        """ScoredSignal properties should delegate to underlying signal."""
        signal = self._create_signal()
        scored = ScoredSignal(signal=signal, confidence=0.8)

        assert scored.symbol == "SPY"
        assert scored.direction == SignalDirection.LONG
        assert scored.strategy_name == "test_strategy"

    def test_scored_signal_to_event(self):
        """to_event should create SIGNAL_SCORED event."""
        signal = self._create_signal()
        scored = ScoredSignal(signal=signal, confidence=0.8)

        event = scored.to_event()

        assert event.event_type == EventType.SIGNAL_SCORED
        assert event.symbol == "SPY"


class TestGateResult:
    """Tests for GateResult dataclass."""

    def _create_scored_signal(self) -> ScoredSignal:
        """Create a test scored signal."""
        signal = SignalCandidate(
            symbol="SPY",
            direction=SignalDirection.LONG,
            strategy_name="test",
        )
        return ScoredSignal(signal=signal, confidence=0.7)

    def test_gate_result_creation(self):
        """GateResult should store all fields."""
        scored = self._create_scored_signal()
        result = GateResult(
            scored_signal=scored,
            decision=GateDecision.APPROVED,
            gate_name="test_gate",
            min_confidence_required=0.5,
        )

        assert result.scored_signal == scored
        assert result.decision == GateDecision.APPROVED
        assert result.is_approved is True
        assert result.is_rejected is False

    def test_gate_result_rejected(self):
        """GateResult should track rejection reasons."""
        scored = self._create_scored_signal()
        result = GateResult(
            scored_signal=scored,
            decision=GateDecision.REJECTED,
            reasons=(RejectionReason.LOW_CONFIDENCE, RejectionReason.REGIME_MISMATCH),
            reason_details="Multiple issues",
        )

        assert result.is_rejected is True
        assert RejectionReason.LOW_CONFIDENCE in result.reasons

    def test_gate_result_to_event_approved(self):
        """to_event should create META_APPROVED for approved signals."""
        scored = self._create_scored_signal()
        result = GateResult(
            scored_signal=scored,
            decision=GateDecision.APPROVED,
        )

        event = result.to_event()

        assert event.event_type == EventType.META_APPROVED

    def test_gate_result_to_event_rejected(self):
        """to_event should create META_REJECTED for rejected signals."""
        scored = self._create_scored_signal()
        result = GateResult(
            scored_signal=scored,
            decision=GateDecision.REJECTED,
        )

        event = result.to_event()

        assert event.event_type == EventType.META_REJECTED


class TestStubScorer:
    """Tests for StubScorer."""

    def _create_signal(
        self,
        direction: SignalDirection = SignalDirection.LONG,
        strategy: str = "first_pullback",
        tags: tuple = (),
    ) -> SignalCandidate:
        """Create a test signal."""
        return SignalCandidate(
            symbol="SPY",
            direction=direction,
            strategy_name=strategy,
            regime="trending_up",
            tags=tags,
        )

    def _create_features(self, **features) -> FeatureContext:
        """Create a test feature context."""
        return FeatureContext(
            symbol="SPY",
            features=features,
            bars_available=100,
            warmup_complete=True,
            regime="trending_up",
            regime_confidence=0.8,
        )

    def test_scorer_name_and_version(self):
        """Scorer should have name and version."""
        scorer = StubScorer()

        assert scorer.name == "stub_scorer"
        assert scorer.version == "1.0.0"

    def test_score_none_signal(self):
        """Scorer should return zero confidence for NONE direction."""
        scorer = StubScorer()
        signal = self._create_signal(direction=SignalDirection.NONE)
        features = self._create_features()

        scored = scorer.score(signal, features)

        assert scored.confidence == 0.0
        assert "NONE" in scored.score_rationale

    def test_score_base_confidence(self):
        """Scorer should start with base confidence."""
        scorer = StubScorer()
        signal = self._create_signal()
        features = self._create_features()

        scored = scorer.score(signal, features)

        assert scored.confidence >= 0.3  # Base confidence

    def test_score_regime_alignment_bonus(self):
        """Scorer should add bonus for regime alignment."""
        scorer = StubScorer()
        signal = self._create_signal(direction=SignalDirection.LONG)
        features = self._create_features()  # regime="trending_up"

        scored = scorer.score(signal, features)

        assert "regime_alignment" in scored.contributing_factors

    def test_score_trend_strength_bonus(self):
        """Scorer should add bonus for strong trend."""
        scorer = StubScorer()
        signal = self._create_signal()
        features = self._create_features(trend_strength=0.7)

        scored = scorer.score(signal, features)

        assert "trend_strength" in scored.contributing_factors

    def test_score_volume_bonus(self):
        """Scorer should add bonus for elevated volume."""
        scorer = StubScorer()
        signal = self._create_signal()
        features = self._create_features(relative_volume=1.5)

        scored = scorer.score(signal, features)

        assert "volume_confirmation" in scored.contributing_factors

    def test_score_rsi_favorable(self):
        """Scorer should add bonus for favorable RSI."""
        scorer = StubScorer()
        signal = self._create_signal(direction=SignalDirection.LONG)
        features = self._create_features(rsi_14=30.0)  # Oversold

        scored = scorer.score(signal, features)

        assert "rsi_favorable" in scored.contributing_factors

    def test_score_volatility_penalty(self):
        """Scorer should penalize high volatility for MR strategies."""
        scorer = StubScorer()
        signal = self._create_signal(
            strategy="vwap_reclaim",
            tags=("mean_reversion", "vwap"),
        )
        features = self._create_features(atr_pct=4.0)  # High volatility

        scored = scorer.score(signal, features)

        assert "volatility_penalty" in scored.contributing_factors

    def test_score_determinism(self):
        """Same input should produce same score."""
        scorer = StubScorer()
        signal = self._create_signal()
        features = self._create_features(
            trend_strength=0.6,
            rsi_14=55.0,
            relative_volume=1.3,
            macd_histogram=0.2,
            atr_pct=2.0,
        )

        scored1 = scorer.score(signal, features)
        scored2 = scorer.score(signal, features)

        assert scored1.confidence == scored2.confidence
        assert scored1.score_rationale == scored2.score_rationale

    def test_score_confidence_clamped(self):
        """Confidence should be clamped to [0, 1]."""
        # Create config that would produce > 1.0
        config = StubScorerConfig(
            base_confidence=0.9,
            regime_alignment_bonus=0.3,
            trend_strength_bonus=0.3,
        )
        scorer = StubScorer(config)
        signal = self._create_signal()
        features = self._create_features(trend_strength=0.8)

        scored = scorer.score(signal, features)

        assert scored.confidence <= 1.0


class TestStandardMetaGate:
    """Tests for StandardMetaGate."""

    def _create_scored_signal(
        self,
        confidence: float = 0.6,
        direction: SignalDirection = SignalDirection.LONG,
        tags: tuple = (),
    ) -> ScoredSignal:
        """Create a test scored signal."""
        signal = SignalCandidate(
            symbol="SPY",
            direction=direction,
            strategy_name="test",
            tags=tags,
        )
        return ScoredSignal(signal=signal, confidence=confidence)

    def _create_features(
        self,
        regime: str = "trending_up",
        warmup: bool = True,
        market_open: bool = True,
        **features,
    ) -> FeatureContext:
        """Create a test feature context."""
        snapshot = create_snapshot(
            symbol="SPY",
            bid=450.0,
            ask=450.10,
            last=450.05,
            volume=5000000,
            is_market_open=market_open,
        )
        default_features = {
            "rsi_14": 55.0,
            "atr_pct": 2.0,
            "trend_strength": 0.6,
        }
        default_features.update(features)
        return FeatureContext(
            symbol="SPY",
            snapshot=snapshot,
            features=default_features,
            bars_available=100 if warmup else 10,
            warmup_complete=warmup,
            regime=regime,
            regime_confidence=0.8,
        )

    def test_gate_name_and_version(self):
        """Gate should have name and version."""
        gate = StandardMetaGate()

        assert gate.name == "standard_meta_gate"
        assert gate.version == "1.0.0"

    def test_approve_high_confidence(self):
        """Gate should approve high-confidence signals."""
        gate = StandardMetaGate()
        scored = self._create_scored_signal(confidence=0.7)
        features = self._create_features()

        result = gate.evaluate(scored, features)

        assert result.is_approved

    def test_reject_low_confidence(self):
        """Gate should reject low-confidence signals."""
        gate = StandardMetaGate()
        scored = self._create_scored_signal(confidence=0.3)
        features = self._create_features()

        result = gate.evaluate(scored, features)

        assert result.is_rejected
        assert RejectionReason.LOW_CONFIDENCE in result.reasons

    def test_reject_warmup_incomplete(self):
        """Gate should reject when warmup incomplete."""
        gate = StandardMetaGate()
        scored = self._create_scored_signal(confidence=0.7)
        features = self._create_features(warmup=False)

        result = gate.evaluate(scored, features)

        assert result.is_rejected
        assert RejectionReason.WARMUP_INCOMPLETE in result.reasons

    def test_reject_market_closed(self):
        """Gate should reject when market closed."""
        gate = StandardMetaGate()
        scored = self._create_scored_signal(confidence=0.7)
        features = self._create_features(market_open=False)

        result = gate.evaluate(scored, features)

        assert result.is_rejected
        assert RejectionReason.MARKET_CLOSED in result.reasons

    def test_reject_regime_mismatch(self):
        """Gate should reject regime-misaligned signals."""
        gate = StandardMetaGate()
        # LONG signal in downtrend
        scored = self._create_scored_signal(
            confidence=0.7,
            direction=SignalDirection.LONG,
        )
        features = self._create_features(regime="trending_down")

        result = gate.evaluate(scored, features)

        assert result.is_rejected
        assert RejectionReason.REGIME_MISMATCH in result.reasons

    def test_reject_high_volatility_mr(self):
        """Gate should reject MR signals in high volatility."""
        gate = StandardMetaGate()
        scored = self._create_scored_signal(
            confidence=0.7,
            tags=("mean_reversion",),
        )
        features = self._create_features(atr_pct=4.0)

        result = gate.evaluate(scored, features)

        assert result.is_rejected
        assert RejectionReason.VOLATILITY_EXTREME in result.reasons

    def test_evaluate_determinism(self):
        """Same input should produce same decision."""
        gate = StandardMetaGate()
        scored = self._create_scored_signal(confidence=0.65)
        features = self._create_features()

        result1 = gate.evaluate(scored, features)
        result2 = gate.evaluate(scored, features)

        assert result1.decision == result2.decision
        assert result1.reasons == result2.reasons


class TestPermissiveMetaGate:
    """Tests for PermissiveMetaGate."""

    def test_approve_low_confidence(self):
        """Permissive gate should approve low confidence."""
        gate = PermissiveMetaGate()
        signal = SignalCandidate(
            symbol="SPY",
            direction=SignalDirection.LONG,
        )
        scored = ScoredSignal(signal=signal, confidence=0.1)
        features = FeatureContext(symbol="SPY")

        result = gate.evaluate(scored, features)

        assert result.is_approved

    def test_reject_zero_confidence(self):
        """Permissive gate should reject zero confidence."""
        gate = PermissiveMetaGate()
        signal = SignalCandidate(
            symbol="SPY",
            direction=SignalDirection.LONG,
        )
        scored = ScoredSignal(signal=signal, confidence=0.0)
        features = FeatureContext(symbol="SPY")

        result = gate.evaluate(scored, features)

        assert result.is_rejected


class TestStrictMetaGate:
    """Tests for StrictMetaGate."""

    def test_reject_moderate_confidence(self):
        """Strict gate should reject moderate confidence."""
        gate = StrictMetaGate(min_confidence=0.7)
        signal = SignalCandidate(
            symbol="SPY",
            direction=SignalDirection.LONG,
        )
        scored = ScoredSignal(signal=signal, confidence=0.6)
        features = FeatureContext(
            symbol="SPY",
            warmup_complete=True,
            regime_confidence=0.8,
        )

        result = gate.evaluate(scored, features)

        assert result.is_rejected

    def test_approve_high_confidence(self):
        """Strict gate should approve high confidence."""
        gate = StrictMetaGate(min_confidence=0.7)
        signal = SignalCandidate(
            symbol="SPY",
            direction=SignalDirection.LONG,
        )
        scored = ScoredSignal(signal=signal, confidence=0.8)
        features = FeatureContext(
            symbol="SPY",
            warmup_complete=True,
            regime_confidence=0.8,
        )

        result = gate.evaluate(scored, features)

        assert result.is_approved


class TestScoringIsolation:
    """Tests verifying scoring doesn't import forbidden modules."""

    def test_base_has_no_execution_imports(self):
        """Base module should not import execution modules."""
        from morpheus.scoring import base

        source = open(base.__file__).read()

        assert "from morpheus.execution" not in source
        assert "from morpheus.risk" not in source

    def test_base_has_no_fsm_imports(self):
        """Base module should not import FSM."""
        from morpheus.scoring import base

        source = open(base.__file__).read()

        assert "trade_fsm" not in source
        assert "TradeLifecycle" not in source

    def test_stub_scorer_has_no_forbidden_imports(self):
        """Stub scorer should not import forbidden modules."""
        from morpheus.scoring import stub_scorer

        source = open(stub_scorer.__file__).read()

        assert "trade_fsm" not in source
        assert "from morpheus.execution" not in source
        assert "from morpheus.risk" not in source

    def test_meta_gate_has_no_forbidden_imports(self):
        """Meta gate should not import forbidden modules."""
        from morpheus.scoring import meta_gate

        source = open(meta_gate.__file__).read()

        assert "trade_fsm" not in source
        assert "from morpheus.execution" not in source
        assert "from morpheus.risk" not in source


class TestFactoryFunctions:
    """Tests for factory functions."""

    def test_create_stub_scorer(self):
        """create_stub_scorer should return StubScorer."""
        scorer = create_stub_scorer()
        assert isinstance(scorer, StubScorer)

    def test_create_stub_scorer_with_config(self):
        """create_stub_scorer should accept config."""
        config = StubScorerConfig(base_confidence=0.4)
        scorer = create_stub_scorer(config)
        assert scorer.config.base_confidence == 0.4

    def test_create_standard_gate(self):
        """create_standard_gate should return StandardMetaGate."""
        gate = create_standard_gate()
        assert isinstance(gate, StandardMetaGate)

    def test_create_permissive_gate(self):
        """create_permissive_gate should return PermissiveMetaGate."""
        gate = create_permissive_gate()
        assert isinstance(gate, PermissiveMetaGate)

    def test_create_strict_gate(self):
        """create_strict_gate should return StrictMetaGate."""
        gate = create_strict_gate(min_confidence=0.8)
        assert isinstance(gate, StrictMetaGate)
