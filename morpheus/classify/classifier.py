"""
MASS Strategy Classifier - Routes symbols to optimal strategies.

Selects the best strategy for each symbol based on:
- Structure grade (A/B/C)
- Market regime
- Feature context (indicators, scanner data)
- Market mode (PREMARKET/RTH)

All classification is DETERMINISTIC: same inputs -> same classification.
"""

from __future__ import annotations

import logging
from typing import Any

from morpheus.classify.config import ClassifierConfig
from morpheus.classify.types import ClassificationResult, StrategyCode
from morpheus.core.market_mode import MarketMode
from morpheus.features.feature_engine import FeatureContext
from morpheus.regime.regime_detector import RegimeClassification
from morpheus.structure.types import StructureGrade

logger = logging.getLogger(__name__)


class StrategyClassifier:
    """
    Routes symbols to optimal MASS strategies.

    Uses a scoring matrix to evaluate fit of each strategy against
    current conditions and assigns the top N strategies.

    Deterministic: same inputs -> same classification.
    """

    def __init__(self, config: ClassifierConfig | None = None):
        self.config = config or ClassifierConfig()

    def classify(
        self,
        features: FeatureContext,
        structure: StructureGrade,
        regime: RegimeClassification,
        market_mode: MarketMode,
    ) -> ClassificationResult:
        """
        Classify a symbol and assign optimal strategies.

        Args:
            features: Computed feature context (with external scanner data)
            structure: Structure grade from StructureAnalyzer
            regime: Regime classification
            market_mode: Current market mode

        Returns:
            ClassificationResult with assigned strategies
        """
        symbol = features.symbol

        # Check minimum grade
        grade_rank = {"A": 3, "B": 2, "C": 1, "": 0}
        min_rank = grade_rank.get(self.config.min_grade_for_entry, 2)
        current_rank = grade_rank.get(structure.grade, 0)

        if current_rank < min_rank:
            return ClassificationResult(
                symbol=symbol,
                primary_strategy=None,
                assigned_strategies=(),
                structure_grade=structure.grade,
                regime_mode=regime.primary_regime,
                market_mode=market_mode.name,
                confidence=0.0,
                rationale=f"Structure grade {structure.grade} below minimum {self.config.min_grade_for_entry}",
                strategy_scores={},
            )

        # Score each strategy
        scores: dict[str, float] = {}
        for strategy_code in StrategyCode:
            # Check if strategy is allowed in current market mode
            if not self._is_mode_compatible(strategy_code, market_mode):
                continue

            fit = self._score_strategy_fit(
                strategy_code, features, structure, regime
            )

            # Apply weight multiplier
            weight = self.config.strategy_weights.get(strategy_code.value, 1.0)
            weighted_score = fit * weight

            if weighted_score >= self.config.min_fit_score:
                scores[strategy_code.value] = weighted_score

        # Sort by score, take top N
        sorted_strategies = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        top_n = sorted_strategies[: self.config.max_strategies_per_symbol]

        if not top_n:
            return ClassificationResult(
                symbol=symbol,
                primary_strategy=None,
                assigned_strategies=(),
                structure_grade=structure.grade,
                regime_mode=regime.primary_regime,
                market_mode=market_mode.name,
                confidence=0.0,
                rationale="No strategy scored above minimum threshold",
                strategy_scores=scores,
            )

        assigned = tuple(StrategyCode(name) for name, _ in top_n)
        primary = assigned[0]
        confidence = top_n[0][1]

        rationale_parts = [f"{name}={score:.2f}" for name, score in top_n]
        rationale = (
            f"Assigned {', '.join(s.value for s in assigned)} "
            f"(scores: {', '.join(rationale_parts)})"
        )

        logger.info(
            f"[CLASSIFIER] {symbol} grade={structure.grade} -> {primary.value} "
            f"({confidence:.2f}) [{market_mode.name}]"
        )

        return ClassificationResult(
            symbol=symbol,
            primary_strategy=primary,
            assigned_strategies=assigned,
            structure_grade=structure.grade,
            regime_mode=regime.primary_regime,
            market_mode=market_mode.name,
            confidence=confidence,
            rationale=rationale,
            strategy_scores=scores,
        )

    def _is_mode_compatible(
        self, strategy: StrategyCode, market_mode: MarketMode
    ) -> bool:
        """Check if strategy is allowed in the current market mode."""
        premarket_strategies = {StrategyCode.PMB, StrategyCode.CAT}
        rth_strategies = {
            StrategyCode.VWAP, StrategyCode.CAT, StrategyCode.D2,
            StrategyCode.COIL, StrategyCode.SQZ, StrategyCode.FADE,
            StrategyCode.SCALP,
        }

        if market_mode.name == "PREMARKET":
            return strategy in premarket_strategies
        elif market_mode.name == "RTH":
            return strategy in rth_strategies
        else:
            return False

    def _score_strategy_fit(
        self,
        strategy: StrategyCode,
        features: FeatureContext,
        structure: StructureGrade,
        regime: RegimeClassification,
    ) -> float:
        """
        Score how well a strategy fits the current conditions.

        Returns 0.0 to 1.0 fit score.
        """
        if strategy == StrategyCode.PMB:
            return self._score_pmb(features, structure, regime)
        elif strategy == StrategyCode.VWAP:
            return self._score_vwap(features, structure, regime)
        elif strategy == StrategyCode.CAT:
            return self._score_cat(features, structure, regime)
        elif strategy == StrategyCode.D2:
            return self._score_d2(features, structure, regime)
        elif strategy == StrategyCode.COIL:
            return self._score_coil(features, structure, regime)
        elif strategy == StrategyCode.SQZ:
            return self._score_sqz(features, structure, regime)
        elif strategy == StrategyCode.FADE:
            return self._score_fade(features, structure, regime)
        elif strategy == StrategyCode.SCALP:
            return self._score_scalp(features, structure, regime)
        return 0.0

    def _score_pmb(
        self, f: FeatureContext, s: StructureGrade, r: RegimeClassification
    ) -> float:
        """Premarket Breakout: gap + holding + volume."""
        score = 0.0
        gap_pct = f.external.get("gap_pct", 0)

        if gap_pct and gap_pct > 3.0:
            score += 0.3
        elif gap_pct and gap_pct > 1.0:
            score += 0.15

        if s.vwap_position == "above":
            score += 0.25

        rvol = s.relative_volume
        if rvol >= 2.0:
            score += 0.25
        elif rvol >= 1.5:
            score += 0.15

        rsi = f.get_feature("rsi_14")
        if rsi is not None and 50 <= rsi <= 75:
            score += 0.2
        elif rsi is not None and 40 <= rsi < 50:
            score += 0.1

        return min(score, 1.0)

    def _score_vwap(
        self, f: FeatureContext, s: StructureGrade, r: RegimeClassification
    ) -> float:
        """VWAP Reclaim: price reclaiming VWAP in ranging market."""
        score = 0.0

        pvwap = f.get_feature("price_vs_vwap")
        if pvwap is not None and -0.5 <= pvwap <= 0.5:
            score += 0.3  # Near VWAP (potential reclaim)

        if "ranging" in r.primary_regime:
            score += 0.3
        elif "quiet" in r.primary_regime:
            score += 0.2

        rsi = f.get_feature("rsi_14")
        if rsi is not None and 40 <= rsi <= 60:
            score += 0.2

        if s.ema_stack == "mixed":
            score += 0.1

        if s.spread_pct <= 0.5:
            score += 0.1

        return min(score, 1.0)

    def _score_cat(
        self, f: FeatureContext, s: StructureGrade, r: RegimeClassification
    ) -> float:
        """Catalyst Momentum: strong catalyst + volume + trend."""
        score = 0.0

        scanner_score = f.external.get("scanner_score", 0)
        if scanner_score and scanner_score > 70:
            score += 0.3
        elif scanner_score and scanner_score > 50:
            score += 0.15

        rvol = s.relative_volume
        if rvol >= 3.0:
            score += 0.25
        elif rvol >= 2.0:
            score += 0.15

        if "trending_up" in r.primary_regime:
            score += 0.2

        if s.ema_stack == "bullish":
            score += 0.15

        if s.vwap_position == "above":
            score += 0.1

        return min(score, 1.0)

    def _score_d2(
        self, f: FeatureContext, s: StructureGrade, r: RegimeClassification
    ) -> float:
        """Day-2 Continuation: prior day runner holding gains."""
        score = 0.0

        gap_pct = f.external.get("gap_pct", 0)
        if gap_pct and gap_pct > 0:
            score += 0.25  # Gap holding

        if "trending_up" in r.primary_regime:
            score += 0.25

        if s.vwap_position == "above":
            score += 0.2

        rsi = f.get_feature("rsi_14")
        if rsi is not None and 45 <= rsi <= 65:
            score += 0.2  # Not exhausted

        rvol = s.relative_volume
        if rvol >= 1.5:
            score += 0.1

        return min(score, 1.0)

    def _score_coil(
        self, f: FeatureContext, s: StructureGrade, r: RegimeClassification
    ) -> float:
        """Coil Breakout: tight BB squeeze, low vol, ready to expand."""
        score = 0.0

        bb_width = f.get_feature("bb_width")
        if bb_width is not None and bb_width < 2.0:
            score += 0.35  # Tight bands
        elif bb_width is not None and bb_width < 3.0:
            score += 0.2

        atr_pct = f.get_feature("atr_pct")
        if atr_pct is not None and atr_pct < 2.0:
            score += 0.2

        if "low" in r.primary_regime or "quiet" in r.primary_regime:
            score += 0.15

        trend_dir = f.get_feature("trend_direction")
        if trend_dir is not None and trend_dir > 0:
            score += 0.15

        rvol = s.relative_volume
        if rvol >= 1.5:
            score += 0.15  # Volume picking up on squeeze resolution

        return min(score, 1.0)

    def _score_sqz(
        self, f: FeatureContext, s: StructureGrade, r: RegimeClassification
    ) -> float:
        """Short Squeeze: extreme volume + rapid acceleration."""
        score = 0.0

        rvol = s.relative_volume
        if rvol >= 4.0:
            score += 0.3
        elif rvol >= 3.0:
            score += 0.15

        rsi = f.get_feature("rsi_14")
        if rsi is not None and rsi > 60:
            score += 0.2

        mom = f.get_feature("momentum_10")
        if mom is not None and mom > 0:
            score += 0.15

        if s.ema_stack == "bullish" and s.vwap_position == "above":
            score += 0.2

        if "strong_bullish" in str(r.momentum).lower():
            score += 0.15

        return min(score, 1.0)

    def _score_fade(
        self, f: FeatureContext, s: StructureGrade, r: RegimeClassification
    ) -> float:
        """Gap Fade: overextended gap fading, counter-trend SHORT."""
        score = 0.0

        gap_pct = f.external.get("gap_pct", 0)
        if gap_pct and gap_pct > 5.0:
            score += 0.25
        elif gap_pct and gap_pct > 3.0:
            score += 0.1

        if s.vwap_position == "below":
            score += 0.25  # Fading below VWAP

        rsi = f.get_feature("rsi_14")
        if rsi is not None and rsi < 65 and rsi > 40:
            score += 0.15  # Retreating from overbought

        macd_hist = f.get_feature("macd_histogram")
        if macd_hist is not None and macd_hist < 0:
            score += 0.2

        rvol = s.relative_volume
        if rvol >= 1.5:
            score += 0.15  # Volume on the fade

        return min(score, 1.0)

    def _score_scalp(
        self, f: FeatureContext, s: StructureGrade, r: RegimeClassification
    ) -> float:
        """Order Flow Scalp: tight spread, high vol, micro-momentum."""
        score = 0.0

        if s.spread_pct <= 0.2:
            score += 0.3
        elif s.spread_pct <= 0.5:
            score += 0.15

        rvol = s.relative_volume
        if rvol >= 2.0:
            score += 0.2

        atr_pct = f.get_feature("atr_pct")
        if atr_pct is not None and atr_pct > 2.0:
            score += 0.15  # Enough movement to scalp

        consec = f.get_feature("consecutive_bars")
        if consec is not None and abs(consec) >= 2:
            score += 0.2  # Clear micro-trend

        if s.liquidity_score >= 12:
            score += 0.15

        return min(score, 1.0)

    def update_weights(self, new_weights: dict[str, float]) -> None:
        """
        Update strategy weights from the feedback loop.

        This is the interface for StrategyWeightManager to push
        updated weights based on performance data.
        """
        # Create new config with updated weights - note ClassifierConfig is frozen
        # so we need to work around that
        merged = dict(self.config.strategy_weights)
        merged.update(new_weights)
        object.__setattr__(self.config, "strategy_weights", merged)
        logger.info(f"[CLASSIFIER] Updated strategy weights: {merged}")
