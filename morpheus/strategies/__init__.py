"""Trading strategies (signal generation only, no execution)."""

from morpheus.strategies.base import (
    SIGNAL_SCHEMA_VERSION,
    SignalDirection,
    SignalCandidate,
    StrategyContext,
    Strategy,
    StrategyRunner,
)
from morpheus.strategies.momentum import (
    FirstPullbackStrategy,
    HighOfDayContinuationStrategy,
    get_momentum_strategies,
)
from morpheus.strategies.mean_reversion import (
    VWAPReclaimStrategy,
    get_mean_reversion_strategies,
)

__all__ = [
    # Base
    "SIGNAL_SCHEMA_VERSION",
    "SignalDirection",
    "SignalCandidate",
    "StrategyContext",
    "Strategy",
    "StrategyRunner",
    # Momentum
    "FirstPullbackStrategy",
    "HighOfDayContinuationStrategy",
    "get_momentum_strategies",
    # Mean Reversion
    "VWAPReclaimStrategy",
    "get_mean_reversion_strategies",
]


def get_all_strategies() -> list[Strategy]:
    """Return all available strategy instances."""
    return get_momentum_strategies() + get_mean_reversion_strategies()
