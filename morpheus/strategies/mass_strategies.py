"""
MASS Strategy aggregator - returns all MASS strategy instances.

Follows the pattern of get_momentum_strategies() and get_mean_reversion_strategies().
"""

from __future__ import annotations

from morpheus.strategies.base import Strategy
from morpheus.strategies.premarket_breakout import PremarketBreakoutStrategy
from morpheus.strategies.catalyst_momentum import CatalystMomentumStrategy
from morpheus.strategies.day2_continuation import Day2ContinuationStrategy
from morpheus.strategies.coil_breakout import CoilBreakoutStrategy
from morpheus.strategies.short_squeeze import ShortSqueezeStrategy
from morpheus.strategies.gap_fade import GapFadeStrategy
from morpheus.strategies.order_flow_scalp import OrderFlowScalpStrategy


def get_mass_strategies() -> list[Strategy]:
    """Return all MASS strategy instances."""
    return [
        PremarketBreakoutStrategy(),
        CatalystMomentumStrategy(),
        Day2ContinuationStrategy(),
        CoilBreakoutStrategy(),
        ShortSqueezeStrategy(),
        GapFadeStrategy(),
        OrderFlowScalpStrategy(),
    ]
