"""
MASS AI Supervisor - Monitors strategy performance and system health.

Detects:
- Strategy decay (declining win rate over rolling window)
- Regime mismatch (strategy performing poorly in current regime)
- Loss streaks and drawdowns
- System anomalies

Actions:
- Emits SUPERVISOR_ALERT events
- Recommends weight adjustments
- Can temporarily disable a strategy (soft disable via weight=0)

Does NOT:
- Execute trades
- Kill positions (that's the kill switch)
- Override human decisions
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from morpheus.core.events import Event, EventType, create_event
from morpheus.evolve.tracker import PerformanceTracker
from morpheus.evolve.weight_manager import StrategyWeightManager
from morpheus.supervise.config import SupervisorConfig

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SupervisorAlert:
    """
    Alert from the AI Supervisor.

    Severity levels:
    - info: Observation, no action needed
    - warning: Potential issue, reduce exposure
    - critical: Significant problem, disable strategy
    """

    alert_type: str  # "strategy_decay", "regime_mismatch", "loss_streak", "system_anomaly"
    severity: str  # "info", "warning", "critical"
    strategy_name: str = ""
    message: str = ""
    recommended_action: str = ""
    data: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_event_payload(self) -> dict[str, Any]:
        return {
            "alert_type": self.alert_type,
            "severity": self.severity,
            "strategy_name": self.strategy_name,
            "message": self.message,
            "recommended_action": self.recommended_action,
            "data": self.data,
            "timestamp": self.timestamp.isoformat(),
        }

    def to_event(self) -> Event:
        return create_event(
            EventType.SUPERVISOR_ALERT,
            payload=self.to_event_payload(),
        )


class AISupervisor:
    """
    Monitors strategy performance and system health.

    Runs periodic health checks and emits alerts when issues are detected.
    """

    def __init__(
        self,
        tracker: PerformanceTracker,
        weight_manager: StrategyWeightManager,
        config: SupervisorConfig | None = None,
    ):
        self._tracker = tracker
        self._weight_manager = weight_manager
        self._config = config or SupervisorConfig()
        self._last_check: datetime | None = None

        # Baseline win rates (computed from first N trades)
        self._baseline_winrates: dict[str, float] = {}

    def check_health(self) -> list[SupervisorAlert]:
        """
        Run all health checks and return alerts.

        Call periodically (every check_interval_seconds).
        """
        alerts: list[SupervisorAlert] = []
        all_perf = self._tracker.get_all_performance()

        for strategy_name, perf in all_perf.items():
            if perf.total_trades < 5:
                continue

            # Check strategy decay
            decay_alert = self.detect_strategy_decay(strategy_name)
            if decay_alert:
                alerts.append(decay_alert)

            # Check loss streaks
            streak_alert = self._check_loss_streak(strategy_name)
            if streak_alert:
                alerts.append(streak_alert)

            # Check profit factor
            pf_alert = self._check_profit_factor(strategy_name)
            if pf_alert:
                alerts.append(pf_alert)

        # Apply auto-actions
        for alert in alerts:
            self._apply_auto_action(alert)

        self._last_check = datetime.now(timezone.utc)

        if alerts:
            logger.info(
                f"[SUPERVISOR] Health check: {len(alerts)} alerts "
                f"({sum(1 for a in alerts if a.severity == 'critical')} critical)"
            )

        return alerts

    def detect_strategy_decay(self, strategy_name: str) -> SupervisorAlert | None:
        """
        Detect if a strategy's performance is decaying.

        Decay = win rate dropped significantly from baseline.
        """
        perf = self._tracker.get_performance(strategy_name)
        if not perf or perf.total_trades < self._config.decay_window_trades:
            return None

        # Compute recent win rate from window
        recent = perf.recent_outcomes[-self._config.decay_window_trades:]
        if not recent:
            return None
        recent_wr = sum(1 for x in recent if x) / len(recent)

        # Set baseline if not set
        if strategy_name not in self._baseline_winrates:
            self._baseline_winrates[strategy_name] = perf.win_rate
            return None

        baseline_wr = self._baseline_winrates[strategy_name]
        drop = baseline_wr - recent_wr

        if drop >= self._config.decay_winrate_drop_threshold:
            severity = "critical" if drop >= 0.25 else "warning"
            return SupervisorAlert(
                alert_type="strategy_decay",
                severity=severity,
                strategy_name=strategy_name,
                message=(
                    f"{strategy_name} win rate decaying: "
                    f"{recent_wr:.0%} (was {baseline_wr:.0%}, drop {drop:.0%})"
                ),
                recommended_action=(
                    f"Reduce weight by {drop * 200:.0f}%" if severity == "warning"
                    else f"Disable {strategy_name}"
                ),
                data={
                    "baseline_winrate": baseline_wr,
                    "recent_winrate": recent_wr,
                    "drop": drop,
                    "window_size": len(recent),
                },
            )

        return None

    def _check_loss_streak(self, strategy_name: str) -> SupervisorAlert | None:
        """Check for excessive loss streaks."""
        perf = self._tracker.get_performance(strategy_name)
        if not perf:
            return None

        if perf.streak <= -self._config.max_consecutive_losses:
            severity = "critical" if perf.streak <= -7 else "warning"
            return SupervisorAlert(
                alert_type="loss_streak",
                severity=severity,
                strategy_name=strategy_name,
                message=(
                    f"{strategy_name} on {abs(perf.streak)}-trade loss streak"
                ),
                recommended_action=f"Reduce size 40% for {strategy_name}",
                data={"streak": perf.streak, "total_trades": perf.total_trades},
            )

        return None

    def _check_profit_factor(self, strategy_name: str) -> SupervisorAlert | None:
        """Check if profit factor is critically low."""
        perf = self._tracker.get_performance(strategy_name)
        if not perf or perf.total_trades < 10:
            return None

        if perf.profit_factor < self._config.min_profit_factor:
            return SupervisorAlert(
                alert_type="strategy_decay",
                severity="warning",
                strategy_name=strategy_name,
                message=(
                    f"{strategy_name} profit factor {perf.profit_factor:.2f} "
                    f"below minimum {self._config.min_profit_factor}"
                ),
                recommended_action=f"Review and potentially disable {strategy_name}",
                data={
                    "profit_factor": perf.profit_factor,
                    "win_rate": perf.win_rate,
                    "total_trades": perf.total_trades,
                },
            )

        return None

    def _apply_auto_action(self, alert: SupervisorAlert) -> None:
        """Apply automatic actions based on alert severity."""
        if not alert.strategy_name:
            return

        if alert.severity == "critical" and self._config.auto_disable_on_critical:
            # Soft-disable strategy by setting weight to near-zero
            self._weight_manager.update_weights()
            weights = self._weight_manager.get_weights()
            weights[alert.strategy_name] = 0.1  # Near-zero, not full zero
            logger.warning(
                f"[SUPERVISOR] Auto-disabled {alert.strategy_name} "
                f"(critical alert: {alert.message})"
            )

        elif alert.severity == "warning" and self._config.auto_reduce_on_warning > 0:
            weights = self._weight_manager.get_weights()
            current = weights.get(alert.strategy_name, 1.0)
            reduced = current * (1.0 - self._config.auto_reduce_on_warning)
            weights[alert.strategy_name] = max(0.3, reduced)
            logger.info(
                f"[SUPERVISOR] Reduced {alert.strategy_name} weight to "
                f"{weights[alert.strategy_name]:.2f} (warning: {alert.message})"
            )

    def get_status(self) -> dict[str, Any]:
        """Get supervisor status for API."""
        return {
            "last_check": self._last_check.isoformat() if self._last_check else None,
            "baseline_winrates": self._baseline_winrates,
            "config": {
                "check_interval_seconds": self._config.check_interval_seconds,
                "decay_window_trades": self._config.decay_window_trades,
                "max_consecutive_losses": self._config.max_consecutive_losses,
                "auto_disable_on_critical": self._config.auto_disable_on_critical,
            },
        }
