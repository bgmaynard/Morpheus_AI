"""
Persistence Layer - SQLite-based storage for signals, trades, and decisions.

Provides append-only, crash-safe logging of all system activity for
learning and analysis.
"""

from morpheus.persistence.database import Database
from morpheus.persistence.repository import (
    SignalRepository,
    TradeRepository,
    DecisionRepository,
)

__all__ = [
    "Database",
    "SignalRepository",
    "TradeRepository",
    "DecisionRepository",
]
