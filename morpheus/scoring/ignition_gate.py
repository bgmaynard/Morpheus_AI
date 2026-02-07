"""
Ignition Gate - Hard momentum threshold filter (Pipeline Stage 5.5).

Enforces the Premarket Ignition Master Playbook thresholds as a fail-closed
gate in the signal pipeline. Any check failure rejects the signal.

If enabled=False, gate passes everything (additive design).
If momentum data is absent, gate REJECTS (fail-closed).

Thresholds derived from 6-symbol replay study (52 trades, +34 bps expectancy).
"""

from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, time
from typing import Any

logger = logging.getLogger(__name__)

# Eastern timezone - use datetime.now(ET_TZ) per MEMORY.md bug
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

ET_TZ = ZoneInfo("America/New_York")

# RTH open time
RTH_OPEN = time(9, 30)


@dataclass(frozen=True)
class IgnitionGateConfig:
    """Configuration for the ignition gate (all thresholds hot-reloadable)."""

    enabled: bool = True
    min_momentum_score: float = 60.0
    min_nofi: float = 0.2428
    min_l2_pressure: float = 0.5498
    max_spread_dynamics: float = 0.0243
    min_confidence: float = 0.557
    rth_open_cooldown_seconds: float = 120.0
    daily_loss_limit_pct: float = 0.02

    @classmethod
    def from_runtime_config(cls, rc: Any) -> IgnitionGateConfig:
        """Build from RuntimeConfig using getattr for backward compat."""
        return cls(
            enabled=getattr(rc, "ignition_gate_enabled", True),
            min_momentum_score=getattr(rc, "ignition_min_momentum_score", 60.0),
            min_nofi=getattr(rc, "ignition_min_nofi", 0.2428),
            min_l2_pressure=getattr(rc, "ignition_min_l2_pressure", 0.5498),
            max_spread_dynamics=getattr(rc, "ignition_max_spread_dynamics", 0.0243),
            min_confidence=getattr(rc, "ignition_min_confidence", 0.557),
            rth_open_cooldown_seconds=getattr(rc, "ignition_rth_open_cooldown_seconds", 120.0),
            daily_loss_limit_pct=getattr(rc, "ignition_daily_loss_limit_pct", 0.02),
        )


@dataclass(frozen=True)
class IgnitionResult:
    """Result of ignition gate evaluation."""

    passed: bool
    failures: tuple[str, ...]
    checks_run: int
    momentum_snapshot: dict = field(default_factory=dict)

    def to_event_payload(self) -> dict[str, Any]:
        """Convert to event payload for IGNITION_APPROVED / IGNITION_REJECTED."""
        return {
            "passed": self.passed,
            "failures": list(self.failures),
            "checks_run": self.checks_run,
            "momentum_snapshot": self.momentum_snapshot,
        }


class IgnitionGate:
    """
    Hard momentum threshold gate.

    9 checks, fail-closed: ANY failure rejects the signal.
    If disabled, passes everything. If momentum data absent, rejects.
    """

    def __init__(self, config: IgnitionGateConfig) -> None:
        self._config = config
        # Track recent momentum scores for declining-score check (per symbol)
        self._recent_scores: dict[str, deque[float]] = {}
        logger.info(
            f"[IGNITION_GATE] Initialized (enabled={config.enabled}, "
            f"min_score={config.min_momentum_score}, min_nofi={config.min_nofi})"
        )

    def update_config(self, config: IgnitionGateConfig) -> None:
        """Hot-reload configuration."""
        object.__setattr__(self, "_config", config)
        logger.info(f"[IGNITION_GATE] Config updated (enabled={config.enabled})")

    def evaluate(
        self,
        features: dict[str, Any],
        daily_pnl_pct: float = 0.0,
    ) -> IgnitionResult:
        """
        Evaluate a signal against all ignition thresholds.

        Args:
            features: Feature dict from FeatureContext.features
            daily_pnl_pct: Current daily P&L as fraction (negative = loss)

        Returns:
            IgnitionResult with pass/fail and failure reasons
        """
        cfg = self._config

        # If gate disabled, pass everything
        if not cfg.enabled:
            return IgnitionResult(passed=True, failures=(), checks_run=0)

        failures: list[str] = []
        checks_run = 0

        # Extract momentum fields from features
        score = features.get("momentum_engine_score")
        nofi = features.get("momentum_nofi")
        l2_pressure = features.get("momentum_l2_pressure")
        spread_dynamics = features.get("momentum_spread_dynamics")
        confidence = features.get("momentum_engine_confidence")
        velocity = features.get("momentum_velocity")

        # Build snapshot for event payload
        momentum_snapshot = {
            "momentum_score": score,
            "nofi": nofi,
            "l2_pressure": l2_pressure,
            "spread_dynamics": spread_dynamics,
            "confidence": confidence,
            "velocity": velocity,
        }

        # ── Check 0: Momentum data present (fail-closed) ────────────
        checks_run += 1
        if score is None:
            failures.append("NO_MOMENTUM_DATA: momentum_engine_score is None (fail-closed)")
            return IgnitionResult(
                passed=False,
                failures=tuple(failures),
                checks_run=checks_run,
                momentum_snapshot=momentum_snapshot,
            )

        # ── Check 1: Minimum momentum score ─────────────────────────
        checks_run += 1
        if score < cfg.min_momentum_score:
            failures.append(
                f"LOW_SCORE: {score:.2f} < {cfg.min_momentum_score}"
            )

        # ── Check 2: NOFI (order flow confirmation) ─────────────────
        checks_run += 1
        if nofi is not None and nofi < cfg.min_nofi:
            failures.append(
                f"LOW_NOFI: {nofi:.4f} < {cfg.min_nofi}"
            )

        # ── Check 3: L2 pressure (book support) ─────────────────────
        checks_run += 1
        if l2_pressure is not None and l2_pressure < cfg.min_l2_pressure:
            failures.append(
                f"LOW_L2_PRESSURE: {l2_pressure:.4f} < {cfg.min_l2_pressure}"
            )

        # ── Check 4: Spread dynamics (not widening) ──────────────────
        checks_run += 1
        if spread_dynamics is not None and spread_dynamics > cfg.max_spread_dynamics:
            failures.append(
                f"HIGH_SPREAD: {spread_dynamics:.4f} > {cfg.max_spread_dynamics}"
            )

        # ── Check 5: Data quality confidence ─────────────────────────
        checks_run += 1
        if confidence is not None and confidence < cfg.min_confidence:
            failures.append(
                f"LOW_CONFIDENCE: {confidence:.3f} < {cfg.min_confidence}"
            )

        # ── Check 6: No-trade zone (first 120s after RTH open) ──────
        checks_run += 1
        now_et = datetime.now(ET_TZ)
        rth_open_dt = now_et.replace(
            hour=RTH_OPEN.hour,
            minute=RTH_OPEN.minute,
            second=0,
            microsecond=0,
        )
        seconds_since_open = (now_et - rth_open_dt).total_seconds()
        if 0 <= seconds_since_open < cfg.rth_open_cooldown_seconds:
            failures.append(
                f"RTH_COOLDOWN: {seconds_since_open:.0f}s < {cfg.rth_open_cooldown_seconds}s after open"
            )

        # ── Check 7: Conflicting NOFI/velocity signs ────────────────
        checks_run += 1
        if nofi is not None and velocity is not None:
            nofi_magnitude = abs(nofi)
            velocity_magnitude = abs(velocity)
            if nofi_magnitude > 0.1 and velocity_magnitude > 0.1:
                nofi_sign = 1 if nofi > 0 else -1
                velocity_sign = 1 if velocity > 0 else -1
                if nofi_sign != velocity_sign:
                    failures.append(
                        f"CONFLICTING_SIGNALS: nofi={nofi:.3f} vs velocity={velocity:.3f}"
                    )

        # ── Check 8: Declining momentum (3 consecutive drops) ───────
        checks_run += 1
        symbol = features.get("symbol", "_unknown")
        if symbol not in self._recent_scores:
            self._recent_scores[symbol] = deque(maxlen=4)
        self._recent_scores[symbol].append(score)
        scores = list(self._recent_scores[symbol])
        if len(scores) >= 4:
            # Check last 3 deltas are all negative
            deltas = [scores[i] - scores[i - 1] for i in range(1, len(scores))]
            if all(d < 0 for d in deltas[-3:]):
                failures.append(
                    f"DECLINING_SCORE: {scores[-3:]!r} (3 consecutive drops)"
                )

        # ── Check 9: Daily loss limit ───────────────────────────────
        checks_run += 1
        if daily_pnl_pct < -cfg.daily_loss_limit_pct:
            failures.append(
                f"DAILY_LOSS_LIMIT: {daily_pnl_pct:.4f} < -{cfg.daily_loss_limit_pct}"
            )

        passed = len(failures) == 0

        if not passed:
            logger.info(
                f"[IGNITION_GATE] REJECTED ({len(failures)} failures): "
                f"{', '.join(failures)}"
            )
        else:
            logger.debug(
                f"[IGNITION_GATE] APPROVED: score={score:.1f} nofi={nofi} "
                f"l2={l2_pressure} spread={spread_dynamics}"
            )

        return IgnitionResult(
            passed=passed,
            failures=tuple(failures),
            checks_run=checks_run,
            momentum_snapshot=momentum_snapshot,
        )
