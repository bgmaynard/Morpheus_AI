"""
Worklist Scoring - Deterministic priority scoring for worklist symbols.

Combines multiple factors into a single combined_priority_score (0-100).
Re-scores symbols whenever new data arrives.

NO ML - Simple, deterministic, auditable.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


def classify_momentum_state(
    gap_pct: float,
    rvol: float,
    rvol_5m: float = 0.0,
    short_pct: float = 0.0,
    float_shares: float = 0.0,
    halt_status: str = None,
    alert_count: int = 0,
    news_present: bool = False,
) -> tuple[str, str, list[str]]:
    """
    Classify momentum state to mirror professional scanner reasoning.

    Returns:
        Tuple of (momentum_state, trigger_reason, trigger_context)

    States:
        RUNNING_UP - Active momentum with multiple alerts
        SQUEEZE - Short squeeze pattern (high short, low float, high RVOL)
        HALTED - Currently in trading halt
        GAP_OPEN - Gap at open, watching for continuation
        BREAKOUT - Breaking key level
        CATALYST - News-driven move
        UNKNOWN - Default

    Trigger Context Tags:
        GAP, RUNNING_UP, HALT, NEWS, SQUEEZE, LOW_FLOAT, HIGH_RVOL, BREAKOUT
    """
    state = "unknown"
    reasons = []
    trigger_context = []

    # Build trigger context tags based on conditions
    if abs(gap_pct) >= 3:
        trigger_context.append("GAP")
    if float_shares and float_shares < 20:
        trigger_context.append("LOW_FLOAT")
    if rvol > 3.0:
        trigger_context.append("HIGH_RVOL")
    if news_present:
        trigger_context.append("NEWS")
    if halt_status == "HALTED":
        trigger_context.append("HALT")
    if short_pct and short_pct > 15:
        trigger_context.append("HIGH_SHORT")

    # Priority order matters - check most specific first

    # 1. HALTED - takes precedence
    if halt_status == "HALTED":
        state = "halted"
        reasons.append(f"Trading halted")
        if float_shares and float_shares < 10:
            reasons.append(f"Low float {float_shares:.1f}M")
        return state, " | ".join(reasons), trigger_context

    # 2. SQUEEZE pattern - high short + low float + high RVOL
    if (short_pct and short_pct > 15 and
        float_shares and float_shares < 10 and
        rvol > 3.0):
        state = "squeeze"
        trigger_context.append("SQUEEZE")
        reasons.append(f"Short squeeze: {short_pct:.1f}% SI")
        reasons.append(f"Float {float_shares:.1f}M")
        reasons.append(f"RVOL {rvol:.1f}x")
        return state, " | ".join(reasons), trigger_context

    # 3. RUNNING_UP - multiple alerts, accelerating RVOL
    if alert_count >= 2 and rvol_5m > rvol:
        state = "running_up"
        trigger_context.append("RUNNING_UP")
        reasons.append(f"{alert_count} alerts")
        reasons.append(f"RVOL accelerating: 5m={rvol_5m:.1f}x > daily={rvol:.1f}x")
        if gap_pct > 3:
            reasons.append(f"Gap +{gap_pct:.1f}%")
        return state, " | ".join(reasons), trigger_context

    # 4. CATALYST - news-driven
    if news_present and rvol > 2.0:
        state = "catalyst"
        reasons.append("News catalyst")
        reasons.append(f"RVOL {rvol:.1f}x")
        if gap_pct > 3:
            reasons.append(f"Gap +{gap_pct:.1f}%")
        return state, " | ".join(reasons), trigger_context

    # 5. GAP_OPEN - significant gap
    if abs(gap_pct) >= 5:
        state = "gap_open"
        direction = "up" if gap_pct > 0 else "down"
        reasons.append(f"Gap {direction} {abs(gap_pct):.1f}%")
        if rvol > 2:
            reasons.append(f"RVOL {rvol:.1f}x")
        if float_shares and float_shares < 20:
            reasons.append(f"Float {float_shares:.1f}M")
        return state, " | ".join(reasons), trigger_context

    # 6. BREAKOUT - high RVOL with momentum
    if rvol > 4.0 and alert_count >= 1:
        state = "breakout"
        trigger_context.append("BREAKOUT")
        reasons.append(f"High RVOL {rvol:.1f}x")
        if gap_pct > 0:
            reasons.append(f"+{gap_pct:.1f}%")
        return state, " | ".join(reasons), trigger_context

    # 7. Default - build generic reason
    if rvol > 1.5:
        reasons.append(f"RVOL {rvol:.1f}x")
    if gap_pct > 2:
        reasons.append(f"Gap +{gap_pct:.1f}%")
    if float_shares and float_shares < 20:
        reasons.append(f"Float {float_shares:.1f}M")

    return state, " | ".join(reasons) if reasons else "Monitoring", trigger_context


def is_negative_news_category(category: str) -> bool:
    """Check if news category is negative (should suppress score)."""
    if not category:
        return False
    category_upper = category.upper()
    for neg in NEGATIVE_NEWS_CATEGORIES:
        if neg in category_upper:
            return True
    return False


@dataclass(frozen=True)
class ScoringConfig:
    """
    Configuration for priority scoring.

    Weights must sum to 1.0 for normalized output.

    Enhanced to align with professional scanner reasoning:
    - RVOL velocity boost (5m > daily = momentum acceleration)
    - Low float + halt combination boost (squeeze setup)
    - Repeated alert boost (confirms momentum)
    """

    # Component weights (must sum to 1.0)
    weight_scanner_score: float = 0.30  # MAX_AI score
    weight_gap: float = 0.25            # Gap percentage
    weight_rvol: float = 0.25           # Relative volume
    weight_volume: float = 0.10         # Absolute volume
    weight_news: float = 0.10           # News score (if present)

    # Normalization parameters
    # Gap: 0-50% maps to 0-100 score
    gap_max_pct: float = 50.0

    # RVOL: 1x-100x maps to 0-100 score (log scale)
    rvol_min: float = 1.0
    rvol_max: float = 100.0

    # Volume: 100K-100M maps to 0-100 score (log scale)
    volume_min: int = 100_000
    volume_max: int = 100_000_000

    # News score already 0-100

    # Bonus multipliers (professional scanner alignment)
    news_present_bonus: float = 1.10      # 10% boost if news exists
    high_score_threshold: float = 80.0    # Score considered "high"

    # RVOL velocity boost: when 5m RVOL > daily RVOL = momentum accelerating
    rvol_velocity_boost: float = 1.15     # 15% boost when rvol_5m > rvol_daily

    # Low float + halt boost (squeeze setup)
    low_float_threshold: float = 10.0     # Float < 10M shares considered "low"
    halt_low_float_boost: float = 1.25    # 25% boost for low float + halt

    # Repeated alerts boost (confirms momentum)
    alert_boost_per_alert: float = 0.05   # 5% boost per additional alert
    max_alert_boost: float = 1.30         # Cap at 30% total alert boost

    # Short squeeze detection
    high_short_threshold: float = 15.0    # Short % > 15% considered high
    squeeze_boost: float = 1.20           # 20% boost for squeeze pattern

    # Scanner news indicator boost (authoritative - scanner says there's news)
    scanner_news_boost: float = 1.15      # 15% boost when scanner indicates news

    # Negative news category penalties (suppress risky situations)
    # These categories override positive signals
    negative_news_penalty: float = 0.50   # 50% penalty for negative categories
    negative_news_cap: float = 60.0       # Hard cap score at 60 for negative news


# Negative news categories that suppress scores (per instruction)
NEGATIVE_NEWS_CATEGORIES = {
    "OFFERING",
    "DILUTION",
    "PRICING OF OFFERING",
    "PRICING",
    "PRICED",
    "INVESTIGATION",
    "BANKRUPTCY",
    "DISTRESS",
    "DELISTING",
    "SEC",
    "FRAUD",
    "LAWSUIT",
}


class WorklistScorer:
    """
    Priority scoring engine for worklist symbols.

    Combines:
        - Scanner score (from MAX_AI)
        - Gap percentage
        - Relative volume
        - Absolute volume
        - News score (optional)

    All inputs normalized to 0-100, then weighted.
    Final score is 0-100.
    """

    def __init__(self, config: Optional[ScoringConfig] = None):
        """
        Initialize scorer.

        Args:
            config: Scoring configuration
        """
        self._config = config or ScoringConfig()

        # Validate weights sum to ~1.0
        total_weight = (
            self._config.weight_scanner_score +
            self._config.weight_gap +
            self._config.weight_rvol +
            self._config.weight_volume +
            self._config.weight_news
        )
        if not (0.99 <= total_weight <= 1.01):
            logger.warning(
                f"[SCORING] Weights sum to {total_weight:.2f}, expected 1.0"
            )

        logger.info(
            f"[SCORING] Initialized: "
            f"scanner={self._config.weight_scanner_score:.0%}, "
            f"gap={self._config.weight_gap:.0%}, "
            f"rvol={self._config.weight_rvol:.0%}, "
            f"volume={self._config.weight_volume:.0%}, "
            f"news={self._config.weight_news:.0%}"
        )

    def score(
        self,
        scanner_score: float,
        gap_pct: float,
        rvol: float,
        volume: int,
        news_score: Optional[float] = None,
        # Enhanced parameters for professional alignment
        rvol_5m: Optional[float] = None,
        float_shares: Optional[float] = None,
        short_pct: Optional[float] = None,
        halt_status: Optional[str] = None,
        alert_count: int = 0,
        # Scanner news indicator (authoritative)
        scanner_news_indicator: bool = False,
        news_category: Optional[str] = None,
    ) -> float:
        """
        Compute combined priority score.

        Args:
            scanner_score: MAX_AI scanner score (0-100)
            gap_pct: Gap percentage (e.g., 5.0 for 5%)
            rvol: Relative volume multiplier (e.g., 3.0 for 3x daily)
            volume: Absolute volume
            news_score: Optional news score (0-100)
            rvol_5m: Optional 5-minute RVOL (recent velocity)
            float_shares: Optional float in millions
            short_pct: Optional short interest percentage
            halt_status: Optional halt status ("HALTED", "RESUMED", None)
            alert_count: Number of times this symbol triggered alerts
            scanner_news_indicator: Authoritative scanner news flag (trusted)
            news_category: News category for negative detection

        Returns:
            Combined priority score (0-100)

        Note:
            Scanner news indicator is AUTHORITATIVE - if scanner says there's news,
            we trust it and apply boost. We do NOT re-infer or downgrade.

            Negative news categories (OFFERING, DILUTION, etc.) apply penalty
            regardless of other positive signals.
        """
        cfg = self._config

        # Normalize each component to 0-100
        norm_scanner = self._clamp(scanner_score, 0, 100)
        norm_gap = self._normalize_gap(abs(gap_pct))
        norm_rvol = self._normalize_rvol(rvol)
        norm_volume = self._normalize_volume(volume)

        # Handle news (use 50 as neutral if not present)
        has_news = news_score is not None
        norm_news = self._clamp(news_score, 0, 100) if has_news else 50.0

        # Weighted sum
        combined = (
            cfg.weight_scanner_score * norm_scanner +
            cfg.weight_gap * norm_gap +
            cfg.weight_rvol * norm_rvol +
            cfg.weight_volume * norm_volume +
            cfg.weight_news * norm_news
        )

        # Apply news presence bonus
        if has_news and news_score and news_score > 50:
            combined *= cfg.news_present_bonus

        # === PROFESSIONAL SCANNER ALIGNMENT BOOSTS ===

        # 1. RVOL velocity boost: 5m RVOL > daily RVOL = momentum accelerating
        if rvol_5m is not None and rvol > 0 and rvol_5m > rvol:
            combined *= cfg.rvol_velocity_boost
            logger.debug(f"[SCORING] RVOL velocity boost applied: 5m={rvol_5m:.1f} > daily={rvol:.1f}")

        # 2. Low float + halt boost (squeeze setup)
        is_low_float = float_shares is not None and float_shares < cfg.low_float_threshold
        is_halted = halt_status == "HALTED"
        if is_low_float and is_halted:
            combined *= cfg.halt_low_float_boost
            logger.debug(f"[SCORING] Low float + halt boost applied: float={float_shares:.1f}M")

        # 3. Short squeeze pattern boost
        is_high_short = short_pct is not None and short_pct > cfg.high_short_threshold
        if is_high_short and is_low_float and rvol > 3.0:
            combined *= cfg.squeeze_boost
            logger.debug(f"[SCORING] Squeeze boost applied: short={short_pct:.1f}%, float={float_shares:.1f}M")

        # 4. Repeated alerts boost (confirms momentum)
        if alert_count > 1:
            alert_boost = 1.0 + min(
                (alert_count - 1) * cfg.alert_boost_per_alert,
                cfg.max_alert_boost - 1.0
            )
            combined *= alert_boost
            logger.debug(f"[SCORING] Alert boost applied: {alert_count} alerts -> {alert_boost:.2f}x")

        # 5. Scanner news indicator boost (AUTHORITATIVE - scanner says there's news)
        # This is a trusted signal from professional scanners - do NOT re-infer or downgrade
        if scanner_news_indicator:
            combined *= cfg.scanner_news_boost
            logger.debug(f"[SCORING] Scanner news indicator boost applied: {cfg.scanner_news_boost:.2f}x")

        # === NEGATIVE NEWS OVERRIDE ===
        # Negative categories suppress score regardless of other positive signals
        # This is a risk control measure
        is_negative_news = is_negative_news_category(news_category)
        if is_negative_news:
            combined *= cfg.negative_news_penalty
            combined = min(combined, cfg.negative_news_cap)
            logger.info(
                f"[SCORING] NEGATIVE NEWS penalty applied: category={news_category}, "
                f"penalty={cfg.negative_news_penalty:.0%}, cap={cfg.negative_news_cap}"
            )

        # Clamp to 0-100
        final_score = self._clamp(combined, 0, 100)

        logger.debug(
            f"[SCORING] Components: "
            f"scanner={norm_scanner:.1f}, gap={norm_gap:.1f}, "
            f"rvol={norm_rvol:.1f}, vol={norm_volume:.1f}, "
            f"news={norm_news:.1f} -> combined={final_score:.1f}"
        )

        return final_score

    def _normalize_gap(self, gap_pct: float) -> float:
        """
        Normalize gap percentage to 0-100.

        0% gap -> 0 score
        50%+ gap -> 100 score (capped)
        Linear interpolation between.
        """
        if gap_pct <= 0:
            return 0.0
        if gap_pct >= self._config.gap_max_pct:
            return 100.0
        return (gap_pct / self._config.gap_max_pct) * 100

    def _normalize_rvol(self, rvol: float) -> float:
        """
        Normalize relative volume to 0-100 using log scale.

        1x -> 0 score
        100x -> 100 score
        Log scale for better distribution.
        """
        cfg = self._config

        if rvol <= cfg.rvol_min:
            return 0.0
        if rvol >= cfg.rvol_max:
            return 100.0

        # Log scale normalization
        log_min = math.log(cfg.rvol_min)
        log_max = math.log(cfg.rvol_max)
        log_val = math.log(max(rvol, 0.001))

        return ((log_val - log_min) / (log_max - log_min)) * 100

    def _normalize_volume(self, volume: int) -> float:
        """
        Normalize absolute volume to 0-100 using log scale.

        100K -> 0 score
        100M -> 100 score
        Log scale for better distribution.
        """
        cfg = self._config

        if volume <= cfg.volume_min:
            return 0.0
        if volume >= cfg.volume_max:
            return 100.0

        # Log scale normalization
        log_min = math.log(cfg.volume_min)
        log_max = math.log(cfg.volume_max)
        log_val = math.log(max(volume, 1))

        return ((log_val - log_min) / (log_max - log_min)) * 100

    @staticmethod
    def _clamp(value: float, min_val: float, max_val: float) -> float:
        """Clamp value to range."""
        return max(min_val, min(max_val, value))

    def explain_score(
        self,
        scanner_score: float,
        gap_pct: float,
        rvol: float,
        volume: int,
        news_score: Optional[float] = None,
        # Enhanced parameters
        rvol_5m: Optional[float] = None,
        float_shares: Optional[float] = None,
        short_pct: Optional[float] = None,
        halt_status: Optional[str] = None,
        alert_count: int = 0,
        # Scanner news indicator (authoritative)
        scanner_news_indicator: bool = False,
        news_category: Optional[str] = None,
    ) -> dict:
        """
        Explain score breakdown for debugging.

        Returns dict with each component's contribution and all boosts applied.
        """
        cfg = self._config

        norm_scanner = self._clamp(scanner_score, 0, 100)
        norm_gap = self._normalize_gap(abs(gap_pct))
        norm_rvol = self._normalize_rvol(rvol)
        norm_volume = self._normalize_volume(volume)
        has_news = news_score is not None
        norm_news = self._clamp(news_score, 0, 100) if has_news else 50.0

        # Detect which boosts would apply
        rvol_velocity_boost = (
            rvol_5m is not None and rvol > 0 and rvol_5m > rvol
        )
        is_low_float = float_shares is not None and float_shares < cfg.low_float_threshold
        is_halted = halt_status == "HALTED"
        is_high_short = short_pct is not None and short_pct > cfg.high_short_threshold
        squeeze_pattern = is_high_short and is_low_float and rvol > 3.0
        is_negative_news = is_negative_news_category(news_category)

        return {
            "inputs": {
                "scanner_score": scanner_score,
                "gap_pct": gap_pct,
                "rvol": rvol,
                "rvol_5m": rvol_5m,
                "volume": volume,
                "news_score": news_score,
                "float_shares": float_shares,
                "short_pct": short_pct,
                "halt_status": halt_status,
                "alert_count": alert_count,
                "scanner_news_indicator": scanner_news_indicator,
                "news_category": news_category,
            },
            "normalized": {
                "scanner": norm_scanner,
                "gap": norm_gap,
                "rvol": norm_rvol,
                "volume": norm_volume,
                "news": norm_news,
            },
            "contributions": {
                "scanner": cfg.weight_scanner_score * norm_scanner,
                "gap": cfg.weight_gap * norm_gap,
                "rvol": cfg.weight_rvol * norm_rvol,
                "volume": cfg.weight_volume * norm_volume,
                "news": cfg.weight_news * norm_news,
            },
            "boosts": {
                "news_present": has_news and news_score and news_score > 50,
                "scanner_news_indicator": scanner_news_indicator,
                "rvol_velocity": rvol_velocity_boost,
                "low_float_halt": is_low_float and is_halted,
                "squeeze_pattern": squeeze_pattern,
                "alert_count": alert_count,
            },
            "penalties": {
                "negative_news": is_negative_news,
                "negative_category": news_category if is_negative_news else None,
            },
            "boost_multipliers": {
                "news_bonus": cfg.news_present_bonus if has_news and news_score and news_score > 50 else 1.0,
                "scanner_news": cfg.scanner_news_boost if scanner_news_indicator else 1.0,
                "rvol_velocity": cfg.rvol_velocity_boost if rvol_velocity_boost else 1.0,
                "halt_low_float": cfg.halt_low_float_boost if (is_low_float and is_halted) else 1.0,
                "squeeze": cfg.squeeze_boost if squeeze_pattern else 1.0,
                "alerts": 1.0 + min((alert_count - 1) * cfg.alert_boost_per_alert, cfg.max_alert_boost - 1.0) if alert_count > 1 else 1.0,
                "negative_news_penalty": cfg.negative_news_penalty if is_negative_news else 1.0,
            },
            "final_score": self.score(
                scanner_score, gap_pct, rvol, volume, news_score,
                rvol_5m, float_shares, short_pct, halt_status, alert_count,
                scanner_news_indicator, news_category
            ),
        }
