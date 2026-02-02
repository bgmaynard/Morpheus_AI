"""
MASS (Morpheus Adaptive Strategy System) Configuration.

Top-level configuration for all MASS components.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from morpheus.classify.config import ClassifierConfig
from morpheus.structure.config import StructureConfig


@dataclass(frozen=True)
class MASSConfig:
    """Top-level MASS configuration."""

    # Master switch - when False, pipeline falls back to original StrategyRouter
    enabled: bool = True

    # Structure analysis config
    structure: StructureConfig = field(default_factory=StructureConfig)

    # Strategy classifier config
    classifier: ClassifierConfig = field(default_factory=ClassifierConfig)

    # Per-strategy risk overrides (strategy_name -> risk_pct)
    # NOTE: Set to 2% for paper trading validation; tighten for live
    strategy_risk_overrides: dict[str, float] = field(
        default_factory=lambda: {
            "premarket_breakout": 0.02,     # 2% (paper mode)
            "catalyst_momentum": 0.02,      # 2% (paper mode)
            "day2_continuation": 0.02,      # 2% (paper mode)
            "coil_breakout": 0.02,          # 2% (paper mode)
            "short_squeeze": 0.02,          # 2% (paper mode)
            "gap_fade": 0.02,              # 2% (paper mode)
            "order_flow_scalp": 0.02,       # 2% (paper mode)
            "vwap_reclaim": 0.02,           # 2% (paper mode)
        }
    )

    # Phase 2 feature flags
    regime_mapping_enabled: bool = True
    feedback_enabled: bool = True
    supervisor_enabled: bool = True
