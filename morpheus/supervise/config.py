"""Configuration for MASS AI Supervisor."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SupervisorConfig:
    """AI Supervisor configuration."""

    # How often to run health checks (seconds)
    check_interval_seconds: float = 300.0  # 5 minutes

    # Strategy decay detection
    decay_window_trades: int = 20  # Rolling window for decay analysis
    decay_winrate_drop_threshold: float = 0.15  # 15% drop from baseline
    max_consecutive_losses: int = 5
    min_profit_factor: float = 0.8

    # Auto-actions
    auto_disable_on_critical: bool = True  # Set weight to 0 on critical alerts
    auto_reduce_on_warning: float = 0.5  # Reduce weight by 50% on warnings
