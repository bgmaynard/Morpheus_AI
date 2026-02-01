"""
Config Store for the NATS Data Spine.

File-backed configuration with in-memory cache.
Services watch mtime and reload on change.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Default config path relative to this file
DEFAULT_CONFIG_PATH = Path(__file__).parent / "spine_config.json"


@dataclass
class SpineConfig:
    """Parsed spine configuration."""

    nats_url: str = "nats://localhost:4222"

    # Service enablement and CPU affinity
    services: dict[str, dict[str, Any]] = field(default_factory=lambda: {
        "schwab_feed": {"enabled": True, "cpu_affinity": [0, 1]},
        "scanner_feed": {"enabled": True, "cpu_affinity": [2, 3]},
        "ai_core": {"enabled": True, "cpu_affinity": [4, 5, 6, 7]},
        "ui_gateway": {"enabled": True, "cpu_affinity": [8, 9]},
        "replay_logger": {"enabled": True, "cpu_affinity": [10, 11]},
    })

    # Scanner polling intervals
    scanner_context_refresh_seconds: float = 15.0
    scanner_discovery_interval_seconds: float = 30.0
    scanner_halt_interval_seconds: float = 10.0

    # UI throttling
    ui_quote_throttle_hz: int = 10
    ui_max_active_symbols: int = 50

    # Position polling
    positions_poll_interval_market_open: float = 5.0
    positions_poll_interval_market_closed: float = 60.0

    # Replay logger
    replay_log_dir: str = "logs/spine"
    replay_flush_interval_seconds: float = 1.0

    # Pipeline settings (forwarded to AI Core)
    pipeline_min_bars_warmup: int = 50
    pipeline_max_bars_history: int = 200
    pipeline_permissive_mode: bool = True

    def is_service_enabled(self, service_name: str) -> bool:
        svc = self.services.get(service_name, {})
        return svc.get("enabled", True)

    def get_cpu_affinity(self, service_name: str) -> list[int]:
        svc = self.services.get(service_name, {})
        return svc.get("cpu_affinity", [])


class ConfigStore:
    """
    File-backed config store with mtime-based reload.

    Usage:
        store = ConfigStore()
        config = store.get()  # Returns SpineConfig
        # ... later, after file changes ...
        config = store.get()  # Auto-reloads if mtime changed
    """

    def __init__(self, config_path: str | Path | None = None):
        self._path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
        self._config: SpineConfig | None = None
        self._last_mtime: float = 0.0
        self._last_check: float = 0.0
        self._check_interval = 2.0  # Check mtime every 2s

    def get(self) -> SpineConfig:
        """Get current config, reloading if file changed."""
        now = time.time()

        # Throttle filesystem checks
        if now - self._last_check < self._check_interval and self._config:
            return self._config

        self._last_check = now

        # Check if file exists and has changed
        if self._path.exists():
            mtime = os.path.getmtime(self._path)
            if mtime != self._last_mtime or self._config is None:
                self._load()
                self._last_mtime = mtime
        elif self._config is None:
            # No file, use defaults
            self._config = SpineConfig()
            logger.info(f"[CONFIG] No config file at {self._path}, using defaults")

        return self._config

    def _load(self) -> None:
        """Load config from JSON file."""
        try:
            with open(self._path, "r") as f:
                raw = json.load(f)

            self._config = SpineConfig(
                nats_url=raw.get("nats_url", "nats://localhost:4222"),
                services=raw.get("services", {
                    "schwab_feed": {"enabled": True, "cpu_affinity": [0, 1]},
                    "scanner_feed": {"enabled": True, "cpu_affinity": [2, 3]},
                    "ai_core": {"enabled": True, "cpu_affinity": [4, 5, 6, 7]},
                    "ui_gateway": {"enabled": True, "cpu_affinity": [8, 9]},
                    "replay_logger": {"enabled": True, "cpu_affinity": [10, 11]},
                }),
                scanner_context_refresh_seconds=raw.get("scanner", {}).get(
                    "context_refresh_seconds", 15.0
                ),
                scanner_discovery_interval_seconds=raw.get("scanner", {}).get(
                    "discovery_interval_seconds", 30.0
                ),
                scanner_halt_interval_seconds=raw.get("scanner", {}).get(
                    "halt_interval_seconds", 10.0
                ),
                ui_quote_throttle_hz=raw.get("ui", {}).get("quote_throttle_hz", 10),
                ui_max_active_symbols=raw.get("ui", {}).get("max_active_symbols", 50),
                positions_poll_interval_market_open=raw.get("positions", {}).get(
                    "poll_interval_market_open", 5.0
                ),
                positions_poll_interval_market_closed=raw.get("positions", {}).get(
                    "poll_interval_market_closed", 60.0
                ),
                replay_log_dir=raw.get("replay", {}).get("log_dir", "logs/spine"),
                replay_flush_interval_seconds=raw.get("replay", {}).get(
                    "flush_interval_seconds", 1.0
                ),
                pipeline_min_bars_warmup=raw.get("pipeline", {}).get(
                    "min_bars_warmup", 50
                ),
                pipeline_max_bars_history=raw.get("pipeline", {}).get(
                    "max_bars_history", 200
                ),
                pipeline_permissive_mode=raw.get("pipeline", {}).get(
                    "permissive_mode", True
                ),
            )

            logger.info(f"[CONFIG] Loaded config from {self._path}")

        except json.JSONDecodeError as e:
            logger.error(f"[CONFIG] Invalid JSON in {self._path}: {e}")
            if self._config is None:
                self._config = SpineConfig()

        except Exception as e:
            logger.error(f"[CONFIG] Failed to load {self._path}: {e}")
            if self._config is None:
                self._config = SpineConfig()

    def save_defaults(self) -> None:
        """Write default config to file (for bootstrapping)."""
        default = {
            "nats_url": "nats://localhost:4222",
            "services": {
                "schwab_feed": {"enabled": True, "cpu_affinity": [0, 1]},
                "scanner_feed": {"enabled": True, "cpu_affinity": [2, 3]},
                "ai_core": {"enabled": True, "cpu_affinity": [4, 5, 6, 7]},
                "ui_gateway": {"enabled": True, "cpu_affinity": [8, 9]},
                "replay_logger": {"enabled": True, "cpu_affinity": [10, 11]},
            },
            "scanner": {
                "context_refresh_seconds": 15,
                "discovery_interval_seconds": 30,
                "halt_interval_seconds": 10,
            },
            "ui": {
                "quote_throttle_hz": 10,
                "max_active_symbols": 50,
            },
            "positions": {
                "poll_interval_market_open": 5,
                "poll_interval_market_closed": 60,
            },
            "replay": {
                "log_dir": "logs/spine",
                "flush_interval_seconds": 1.0,
            },
            "pipeline": {
                "min_bars_warmup": 50,
                "max_bars_history": 200,
                "permissive_mode": True,
            },
        }

        self._path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._path, "w") as f:
            json.dump(default, f, indent=2)

        logger.info(f"[CONFIG] Saved default config to {self._path}")
