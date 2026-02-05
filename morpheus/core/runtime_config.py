"""
Runtime Config - Hot-reloadable configuration (Deliverable 5).

Provides:
- JSON-based config file that can be modified while running
- Endpoint to trigger reload
- Callback system to notify components of config changes

Config changes take effect without server restart.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger(__name__)

# Config file location
CONFIG_DIR = Path("data")
CONFIG_FILE = CONFIG_DIR / "runtime_config.json"


@dataclass
class RuntimeConfig:
    """
    Hot-reloadable runtime configuration.

    These settings can be changed without restarting the server.
    """

    # Paper trading settings (Deliverable 2 parity)
    paper_equity: float = 500.0  # $500 for IBKR parity
    risk_per_trade_pct: float = 0.02  # 2% risk per trade

    # Quote freshness settings (Deliverable 1)
    max_quote_age_ms: int = 2000  # 2 seconds
    max_spread_pct: float = 3.0  # 3% max spread
    max_price_deviation_pct: float = 50.0  # Bid/ask within 50% of last

    # Risk limits (can be raised/lowered at runtime)
    max_daily_loss_pct: float = 1.0  # 100% - effectively disabled
    max_open_positions: int = 30
    max_total_exposure_pct: float = 0.90  # 90%

    # MASS regime settings
    regime_position_limits: dict[str, int] = field(default_factory=lambda: {
        "HOT": 25,
        "NORMAL": 25,
        "CHOP": 25,
        "DEAD": 0,
    })

    # Symbol cooldown (seconds)
    symbol_cooldown_seconds: float = 300.0  # 5 minutes

    # Auto-trading settings
    auto_confirm_enabled: bool = True  # Auto-confirm signals in paper mode

    # ── Exit Management v1 ─────────────────────────────────────────────
    # MANDATORY: Every trade must have all three exit layers defined

    # 1. Time-based failsafe (CANNOT BE DISABLED)
    max_hold_seconds: float = 300.0  # 5 min default, exit at market if exceeded

    # 2. Hard stop (risk definition)
    hard_stop_pct: float = 0.03  # 3% hard stop if no structure-based stop

    # 3. Trailing stop (profit protection)
    trail_activation_pct: float = 0.01  # +1% move to activate trailing
    trail_distance_pct: float = 0.015  # 1.5% trail from high watermark

    # Per-strategy max hold overrides (seconds)
    strategy_max_hold: dict[str, float] = field(default_factory=lambda: {
        "order_flow_scalp": 120,       # 2 min for scalps
        "premarket_breakout": 300,     # 5 min
        "catalyst_momentum": 300,
        "short_squeeze": 300,
        "coil_breakout": 300,
        "gap_fade": 300,
        "day2_continuation": 600,      # 10 min for continuation
        "first_pullback": 300,
        "hod_continuation": 300,
        "vwap_reclaim": 180,
    })

    # Last update timestamp
    last_reload: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict for serialization."""
        d = asdict(self)
        d["last_reload"] = datetime.now(timezone.utc).isoformat()
        return d


class RuntimeConfigManager:
    """
    Manager for hot-reloadable config.

    Usage:
        manager = RuntimeConfigManager()
        config = manager.get_config()

        # To reload from disk:
        manager.reload()

        # To register callbacks:
        manager.on_reload(my_callback)
    """

    def __init__(self, config_path: Path = CONFIG_FILE):
        self._config_path = config_path
        self._config = RuntimeConfig()
        self._callbacks: list[Callable[[RuntimeConfig], None]] = []
        self._loaded = False

        # Try to load existing config
        self._load_or_create()

    def _load_or_create(self) -> None:
        """Load config from disk or create default."""
        try:
            if self._config_path.exists():
                with open(self._config_path, "r") as f:
                    data = json.load(f)
                self._apply_dict(data)
                logger.info(f"[RUNTIME_CONFIG] Loaded from {self._config_path}")
            else:
                self._save()
                logger.info(f"[RUNTIME_CONFIG] Created default at {self._config_path}")
            self._loaded = True
        except Exception as e:
            logger.error(f"[RUNTIME_CONFIG] Error loading config: {e}")
            self._config = RuntimeConfig()

    def _apply_dict(self, data: dict[str, Any]) -> None:
        """Apply dict values to config, ignoring unknown keys."""
        for key, value in data.items():
            if hasattr(self._config, key):
                try:
                    setattr(self._config, key, value)
                except Exception:
                    pass

    def _save(self) -> None:
        """Save current config to disk."""
        try:
            CONFIG_DIR.mkdir(parents=True, exist_ok=True)
            with open(self._config_path, "w") as f:
                json.dump(self._config.to_dict(), f, indent=2)
        except Exception as e:
            logger.error(f"[RUNTIME_CONFIG] Error saving config: {e}")

    def get_config(self) -> RuntimeConfig:
        """Get current config."""
        return self._config

    def reload(self) -> tuple[bool, str]:
        """
        Reload config from disk.

        Returns:
            (success, message) tuple
        """
        try:
            if not self._config_path.exists():
                return False, f"Config file not found: {self._config_path}"

            with open(self._config_path, "r") as f:
                data = json.load(f)

            # Store old values for comparison
            old_values = self._config.to_dict()

            # Apply new values
            self._apply_dict(data)
            self._config.last_reload = datetime.now(timezone.utc).isoformat()

            # Detect what changed
            new_values = self._config.to_dict()
            changes = []
            for key in old_values:
                if key != "last_reload" and old_values[key] != new_values.get(key):
                    changes.append(f"{key}: {old_values[key]} -> {new_values.get(key)}")

            # Notify callbacks
            for callback in self._callbacks:
                try:
                    callback(self._config)
                except Exception as e:
                    logger.error(f"[RUNTIME_CONFIG] Callback error: {e}")

            if changes:
                logger.info(f"[RUNTIME_CONFIG] Reloaded with changes: {changes}")
                return True, f"Reloaded with {len(changes)} changes: {', '.join(changes)}"
            else:
                logger.info("[RUNTIME_CONFIG] Reloaded (no changes)")
                return True, "Reloaded (no changes detected)"

        except json.JSONDecodeError as e:
            return False, f"Invalid JSON: {e}"
        except Exception as e:
            return False, f"Reload error: {e}"

    def update(self, updates: dict[str, Any]) -> tuple[bool, str]:
        """
        Update config values and save to disk.

        Args:
            updates: Dict of key-value pairs to update

        Returns:
            (success, message) tuple
        """
        try:
            old_values = self._config.to_dict()

            for key, value in updates.items():
                if hasattr(self._config, key):
                    setattr(self._config, key, value)
                else:
                    logger.warning(f"[RUNTIME_CONFIG] Unknown key: {key}")

            self._config.last_reload = datetime.now(timezone.utc).isoformat()
            self._save()

            # Notify callbacks
            for callback in self._callbacks:
                try:
                    callback(self._config)
                except Exception as e:
                    logger.error(f"[RUNTIME_CONFIG] Callback error: {e}")

            new_values = self._config.to_dict()
            changes = [k for k in updates if k in old_values and old_values[k] != new_values.get(k)]

            logger.info(f"[RUNTIME_CONFIG] Updated {len(changes)} values")
            return True, f"Updated {len(changes)} values"

        except Exception as e:
            return False, f"Update error: {e}"

    def on_reload(self, callback: Callable[[RuntimeConfig], None]) -> None:
        """Register a callback to be called when config reloads."""
        self._callbacks.append(callback)


# Global instance
_global_config_manager: RuntimeConfigManager | None = None


def get_runtime_config_manager() -> RuntimeConfigManager:
    """Get or create the global config manager."""
    global _global_config_manager
    if _global_config_manager is None:
        _global_config_manager = RuntimeConfigManager()
    return _global_config_manager


def get_runtime_config() -> RuntimeConfig:
    """Get current runtime config."""
    return get_runtime_config_manager().get_config()


def reload_runtime_config() -> tuple[bool, str]:
    """Reload config from disk."""
    return get_runtime_config_manager().reload()


def update_runtime_config(updates: dict[str, Any]) -> tuple[bool, str]:
    """Update config values."""
    return get_runtime_config_manager().update(updates)
