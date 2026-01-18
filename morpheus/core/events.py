"""
Canonical Event Schema for Morpheus.

All events must contain:
- event_id (uuid)
- event_type (string enum)
- timestamp (UTC ISO8601)
- payload (dict)

Do NOT write freeform logs for decisions—decisions must be events.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class EventType(str, Enum):
    """All event types in Morpheus."""

    # Market Data
    MARKET_SNAPSHOT = "MARKET_SNAPSHOT"

    # Features & Regime
    FEATURES_COMPUTED = "FEATURES_COMPUTED"
    REGIME_DETECTED = "REGIME_DETECTED"

    # Strategy Signals
    SIGNAL_CANDIDATE = "SIGNAL_CANDIDATE"
    SIGNAL_SCORED = "SIGNAL_SCORED"

    # Meta Gate
    META_APPROVED = "META_APPROVED"
    META_REJECTED = "META_REJECTED"

    # Risk
    RISK_VETO = "RISK_VETO"
    RISK_APPROVED = "RISK_APPROVED"

    # Human Confirmation
    HUMAN_CONFIRM_ACCEPTED = "HUMAN_CONFIRM_ACCEPTED"
    HUMAN_CONFIRM_REJECTED = "HUMAN_CONFIRM_REJECTED"

    # Execution
    EXECUTION_BLOCKED = "EXECUTION_BLOCKED"
    ORDER_SUBMITTED = "ORDER_SUBMITTED"
    ORDER_CONFIRMED = "ORDER_CONFIRMED"
    ORDER_REJECTED = "ORDER_REJECTED"
    ORDER_CANCELLED = "ORDER_CANCELLED"
    ORDER_FILL_RECEIVED = "ORDER_FILL_RECEIVED"
    ORDER_STATUS_UNKNOWN = "ORDER_STATUS_UNKNOWN"

    # Trade Lifecycle (FSM)
    TRADE_INITIATED = "TRADE_INITIATED"
    TRADE_ENTRY_PENDING = "TRADE_ENTRY_PENDING"
    TRADE_ENTRY_FILLED = "TRADE_ENTRY_FILLED"
    TRADE_ACTIVE = "TRADE_ACTIVE"
    TRADE_EXIT_PENDING = "TRADE_EXIT_PENDING"
    TRADE_CLOSED = "TRADE_CLOSED"
    TRADE_CANCELLED = "TRADE_CANCELLED"
    TRADE_ERROR = "TRADE_ERROR"

    # System
    SYSTEM_START = "SYSTEM_START"
    SYSTEM_STOP = "SYSTEM_STOP"
    HEARTBEAT = "HEARTBEAT"


class Event(BaseModel):
    """
    Immutable event record.

    All decision-making in Morpheus is recorded as events.
    Events are the source of truth for replay and audit.
    """

    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: EventType
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    payload: dict[str, Any] = Field(default_factory=dict)

    # Optional metadata
    trade_id: str | None = None
    symbol: str | None = None
    correlation_id: str | None = None

    model_config = {"frozen": True}

    def to_jsonl_dict(self) -> dict[str, Any]:
        """Convert to dict suitable for JSONL serialization."""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "timestamp": self.timestamp.isoformat(),
            "payload": self.payload,
            "trade_id": self.trade_id,
            "symbol": self.symbol,
            "correlation_id": self.correlation_id,
        }

    @classmethod
    def from_jsonl_dict(cls, data: dict[str, Any]) -> Event:
        """Reconstruct event from JSONL dict."""
        return cls(
            event_id=data["event_id"],
            event_type=EventType(data["event_type"]),
            timestamp=datetime.fromisoformat(data["timestamp"]),
            payload=data.get("payload", {}),
            trade_id=data.get("trade_id"),
            symbol=data.get("symbol"),
            correlation_id=data.get("correlation_id"),
        )


def create_event(
    event_type: EventType,
    payload: dict[str, Any] | None = None,
    *,
    trade_id: str | None = None,
    symbol: str | None = None,
    correlation_id: str | None = None,
    timestamp: datetime | None = None,
) -> Event:
    """Factory function to create events with consistent defaults."""
    return Event(
        event_type=event_type,
        payload=payload or {},
        trade_id=trade_id,
        symbol=symbol,
        correlation_id=correlation_id,
        timestamp=timestamp or datetime.now(timezone.utc),
    )
