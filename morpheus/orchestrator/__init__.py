"""
Orchestrator package - Wires all components into a live decision pipeline.

The orchestrator is the central coordinator that:
- Subscribes to market data events
- Runs feature engineering on updates
- Detects market regime changes
- Invokes trading strategies
- Scores and gates signals
- Evaluates risk constraints
- Emits events for UI consumption

OBSERVATIONAL ONLY: Does not execute trades automatically.
Human confirmation is always required.
"""

from morpheus.orchestrator.pipeline import (
    SignalPipeline,
    PipelineConfig,
)

__all__ = [
    "SignalPipeline",
    "PipelineConfig",
]
