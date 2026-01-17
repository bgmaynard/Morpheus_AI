"""
Centralized configuration for Morpheus.
Loads from environment variables with sensible defaults.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class SchwabConfig:
    """Schwab API configuration."""
    client_id: str = ""
    client_secret: str = ""
    redirect_uri: str = "https://localhost:8080/callback"
    token_path: Path = field(default_factory=lambda: Path("./tokens/schwab_token.json"))


@dataclass(frozen=True)
class RuntimeConfig:
    """Runtime cadence and environment settings."""
    env: str = "development"
    log_level: str = "INFO"
    event_log_dir: Path = field(default_factory=lambda: Path("./logs/events"))
    quote_poll_interval_sec: float = 5.0
    decision_bar_interval_sec: float = 60.0


@dataclass(frozen=True)
class RiskConfig:
    """Risk management limits."""
    max_daily_loss: float = 500.0
    max_position_size_pct: float = 0.05
    max_total_exposure_pct: float = 0.25
    max_single_trade_risk_pct: float = 0.01


@dataclass(frozen=True)
class MetaGateConfig:
    """Meta gate settings."""
    enabled: bool = True
    min_confidence: float = 0.6


@dataclass(frozen=True)
class StrategyConfig:
    """Strategy allowlists and parameters."""
    allowed_strategies: tuple[str, ...] = ("first_pullback", "hod_breakout")
    symbol_universe: tuple[str, ...] = ()
    watchlist_ttl_sec: int = 300


@dataclass
class MorpheusConfig:
    """Top-level configuration container."""
    schwab: SchwabConfig
    runtime: RuntimeConfig
    risk: RiskConfig
    meta_gate: MetaGateConfig
    strategy: StrategyConfig

    @classmethod
    def from_env(cls) -> "MorpheusConfig":
        """Load configuration from environment variables."""
        return cls(
            schwab=SchwabConfig(
                client_id=os.getenv("SCHWAB_CLIENT_ID", ""),
                client_secret=os.getenv("SCHWAB_CLIENT_SECRET", ""),
                redirect_uri=os.getenv("SCHWAB_REDIRECT_URI", "https://localhost:8080/callback"),
                token_path=Path(os.getenv("SCHWAB_TOKEN_PATH", "./tokens/schwab_token.json")),
            ),
            runtime=RuntimeConfig(
                env=os.getenv("MORPHEUS_ENV", "development"),
                log_level=os.getenv("MORPHEUS_LOG_LEVEL", "INFO"),
                event_log_dir=Path(os.getenv("MORPHEUS_EVENT_LOG_DIR", "./logs/events")),
            ),
            risk=RiskConfig(
                max_daily_loss=float(os.getenv("MORPHEUS_MAX_DAILY_LOSS", "500.0")),
                max_position_size_pct=float(os.getenv("MORPHEUS_MAX_POSITION_SIZE", "0.05")),
                max_total_exposure_pct=float(os.getenv("MORPHEUS_MAX_TOTAL_EXPOSURE", "0.25")),
            ),
            meta_gate=MetaGateConfig(
                enabled=os.getenv("MORPHEUS_META_GATE_ENABLED", "true").lower() == "true",
                min_confidence=float(os.getenv("MORPHEUS_META_MIN_CONFIDENCE", "0.6")),
            ),
            strategy=StrategyConfig(),
        )


# Global config instance (lazy-loaded)
_config: Optional[MorpheusConfig] = None


def get_config() -> MorpheusConfig:
    """Get or create the global configuration."""
    global _config
    if _config is None:
        _config = MorpheusConfig.from_env()
    return _config


def reset_config() -> None:
    """Reset configuration (useful for testing)."""
    global _config
    _config = None
