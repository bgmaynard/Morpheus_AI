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
    strategy_risk_overrides: dict[str, float] = field(
        default_factory=lambda: {
            "premarket_breakout": 0.0025,   # 0.25%
            "catalyst_momentum": 0.0025,    # 0.25%
            "day2_continuation": 0.002,     # 0.20%
            "coil_breakout": 0.002,         # 0.20%
            "short_squeeze": 0.0015,        # 0.15% (higher risk play)
            "gap_fade": 0.0015,             # 0.15% (counter-trend)
            "order_flow_scalp": 0.001,      # 0.10% (minimal risk)
            "vwap_reclaim": 0.002,          # 0.20%
        }
    )

    # Phase 2 feature flags (pre-defined for schema stability)
    regime_mapping_enabled: bool = False
    feedback_enabled: bool = False
    supervisor_enabled: bool = False
