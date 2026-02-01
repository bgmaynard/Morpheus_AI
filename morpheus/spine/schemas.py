"""
Message Schemas for the NATS Data Spine.

All messages are MsgPack-encoded dataclasses for compact binary transport.
Each schema includes encode/decode helpers for serialization.

Topic naming convention:
  md.quotes.{sym}        - Real-time quote ticks
  md.ohlc.1m.{sym}       - 1-minute OHLCV bars
  md.features.{sym}      - Computed feature vectors
  scanner.context.{sym}  - Scanner context per symbol
  scanner.alerts          - Trading halts and alerts
  scanner.discovery       - New symbol discoveries
  ai.signals.{sym}       - Entry/exit signals
  ai.structure.{sym}     - MASS structure grades
  ai.regime              - Regime changes
  ai.orders              - Order lifecycle events
  bot.health             - System health heartbeats
  bot.telemetry.{svc}    - Per-service telemetry
  bot.positions          - Position updates
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field, asdict
from typing import Any

try:
    import msgpack

    def encode(obj: dict) -> bytes:
        return msgpack.packb(obj, use_bin_type=True)

    def decode(data: bytes) -> dict:
        return msgpack.unpackb(data, raw=False)

except ImportError:
    # Fallback to JSON if msgpack not installed
    import json

    def encode(obj: dict) -> bytes:
        return json.dumps(obj).encode("utf-8")

    def decode(data: bytes) -> dict:
        return json.loads(data.decode("utf-8"))


# ---------------------------------------------------------------------------
# Topic helpers
# ---------------------------------------------------------------------------

class Topics:
    """NATS topic constants."""

    # Market data (Lane 1: firehose, plain pub/sub)
    QUOTE = "md.quotes.{sym}"
    OHLC_1M = "md.ohlc.1m.{sym}"
    FEATURES = "md.features.{sym}"

    # Scanner (Lane 2: aggregated, JetStream 1hr)
    SCANNER_CONTEXT = "scanner.context.{sym}"
    SCANNER_ALERTS = "scanner.alerts"
    SCANNER_DISCOVERY = "scanner.discovery"

    # AI decisions (Lane 3: decisions, JetStream 24hr)
    SIGNAL = "ai.signals.{sym}"
    STRUCTURE = "ai.structure.{sym}"
    REGIME = "ai.regime"
    ORDERS = "ai.orders"

    # Bot system
    HEALTH = "bot.health"
    TELEMETRY = "bot.telemetry.{svc}"
    POSITIONS = "bot.positions"

    @staticmethod
    def quote(sym: str) -> str:
        return f"md.quotes.{sym}"

    @staticmethod
    def ohlc_1m(sym: str) -> str:
        return f"md.ohlc.1m.{sym}"

    @staticmethod
    def features(sym: str) -> str:
        return f"md.features.{sym}"

    @staticmethod
    def scanner_context(sym: str) -> str:
        return f"scanner.context.{sym}"

    @staticmethod
    def signal(sym: str) -> str:
        return f"ai.signals.{sym}"

    @staticmethod
    def structure(sym: str) -> str:
        return f"ai.structure.{sym}"

    @staticmethod
    def telemetry(svc: str) -> str:
        return f"bot.telemetry.{svc}"


# ---------------------------------------------------------------------------
# Message dataclasses
# ---------------------------------------------------------------------------

@dataclass
class QuoteMsg:
    """Real-time quote tick (~65 bytes packed)."""
    sym: str
    bid: float
    ask: float
    last: float
    bid_sz: int = 0
    ask_sz: int = 0
    vol: int = 0
    high: float = 0.0
    low: float = 0.0
    open: float = 0.0
    close: float = 0.0
    net_chg: float = 0.0
    mark: float = 0.0
    ts: float = field(default_factory=time.time)  # unix epoch

    def encode(self) -> bytes:
        return encode(asdict(self))

    @classmethod
    def decode(cls, data: bytes) -> QuoteMsg:
        return cls(**decode(data))

    def topic(self) -> str:
        return Topics.quote(self.sym)


@dataclass
class OHLCMsg:
    """1-minute OHLCV bar."""
    sym: str
    o: float
    h: float
    l: float
    c: float
    v: int
    ts: float  # bar open time, unix epoch
    ticks: int = 0  # number of ticks in bar

    def encode(self) -> bytes:
        return encode(asdict(self))

    @classmethod
    def decode(cls, data: bytes) -> OHLCMsg:
        return cls(**decode(data))

    def topic(self) -> str:
        return Topics.ohlc_1m(self.sym)


@dataclass
class ScannerContextMsg:
    """Scanner context for a symbol (published every 15s per active symbol)."""
    sym: str
    scanner_score: float = 0.0
    gap_pct: float = 0.0
    halt_status: str | None = None
    rvol_proxy: float = 0.0
    velocity_1m: float = 0.0
    hod_distance_pct: float = 0.0
    tags: list[str] = field(default_factory=list)
    float_shares: float | None = None
    market_cap: float | None = None
    profiles: list[str] = field(default_factory=list)
    ts: float = field(default_factory=time.time)

    def encode(self) -> bytes:
        return encode(asdict(self))

    @classmethod
    def decode(cls, data: bytes) -> ScannerContextMsg:
        return cls(**decode(data))

    def topic(self) -> str:
        return Topics.scanner_context(self.sym)

    def to_pipeline_dict(self) -> dict[str, Any]:
        """Convert to dict format expected by pipeline's external context."""
        return {
            "scanner_score": self.scanner_score,
            "gap_pct": self.gap_pct,
            "halt_status": self.halt_status,
            "rvol_proxy": self.rvol_proxy,
            "velocity_1m": self.velocity_1m,
            "hod_distance_pct": self.hod_distance_pct,
            "tags": self.tags,
            "float_shares": self.float_shares,
            "market_cap": self.market_cap,
            "profiles": self.profiles,
            "_source": "NATS_SPINE",
            "_timestamp": self.ts,
        }


@dataclass
class ScannerAlertMsg:
    """Scanner alert (halt, resume, gap, momentum, HOD break)."""
    alert_type: str  # HALT, RESUME, GAP_ALERT, MOMO_SURGE, HOD_BREAK
    sym: str
    data: dict[str, Any] = field(default_factory=dict)
    ts: float = field(default_factory=time.time)

    def encode(self) -> bytes:
        return encode(asdict(self))

    @classmethod
    def decode(cls, data: bytes) -> ScannerAlertMsg:
        return cls(**decode(data))


@dataclass
class ScannerDiscoveryMsg:
    """New symbol discovered by scanner."""
    sym: str
    score: float = 0.0
    change_pct: float = 0.0
    rvol: float = 0.0
    source: str = "MAX_AI_SCANNER"
    ts: float = field(default_factory=time.time)

    def encode(self) -> bytes:
        return encode(asdict(self))

    @classmethod
    def decode(cls, data: bytes) -> ScannerDiscoveryMsg:
        return cls(**decode(data))


@dataclass
class SignalMsg:
    """Trading signal from AI Core."""
    sym: str
    direction: str  # LONG, SHORT, NONE
    strategy: str
    confidence: float = 0.0
    rationale: str = ""
    entry_ref: float | None = None
    invalidation_ref: float | None = None
    target_ref: float | None = None
    signal_mode: str = "ACTIVE"  # ACTIVE or OBSERVED
    market_mode: str = "RTH"
    structure_grade: str = ""
    gate_decision: str = ""  # APPROVED, REJECTED
    risk_decision: str = ""  # APPROVED, VETOED
    shares: int = 0
    payload: dict[str, Any] = field(default_factory=dict)
    ts: float = field(default_factory=time.time)

    def encode(self) -> bytes:
        return encode(asdict(self))

    @classmethod
    def decode(cls, data: bytes) -> SignalMsg:
        return cls(**decode(data))

    def topic(self) -> str:
        return Topics.signal(self.sym)


@dataclass
class StructureMsg:
    """MASS structure grade for a symbol."""
    sym: str
    grade: str  # A, B, C
    score: float  # 0-100
    components: dict[str, float] = field(default_factory=dict)
    rationale: str = ""
    tags: list[str] = field(default_factory=list)
    ts: float = field(default_factory=time.time)

    def encode(self) -> bytes:
        return encode(asdict(self))

    @classmethod
    def decode(cls, data: bytes) -> StructureMsg:
        return cls(**decode(data))

    def topic(self) -> str:
        return Topics.structure(self.sym)


@dataclass
class RegimeMsg:
    """Market regime classification."""
    trend: str  # uptrend, downtrend, ranging
    volatility: str  # low, normal, high
    momentum: str  # strong, neutral, weak
    confidence: float = 0.0
    mass_mode: str = ""  # HOT, NORMAL, CHOP, DEAD
    aggression: float = 1.0
    max_positions: int = 3
    ts: float = field(default_factory=time.time)

    def encode(self) -> bytes:
        return encode(asdict(self))

    @classmethod
    def decode(cls, data: bytes) -> RegimeMsg:
        return cls(**decode(data))


@dataclass
class OrderMsg:
    """Order lifecycle event."""
    event_type: str  # SUBMITTED, CONFIRMED, FILL_RECEIVED, CANCELLED, REJECTED
    sym: str
    order_id: str = ""
    direction: str = ""
    shares: int = 0
    price: float = 0.0
    fill_price: float = 0.0
    status: str = ""
    payload: dict[str, Any] = field(default_factory=dict)
    ts: float = field(default_factory=time.time)

    def encode(self) -> bytes:
        return encode(asdict(self))

    @classmethod
    def decode(cls, data: bytes) -> OrderMsg:
        return cls(**decode(data))


@dataclass
class PositionMsg:
    """Position update from broker."""
    sym: str
    qty: int = 0
    avg_price: float = 0.0
    market_value: float = 0.0
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    ts: float = field(default_factory=time.time)

    def encode(self) -> bytes:
        return encode(asdict(self))

    @classmethod
    def decode(cls, data: bytes) -> PositionMsg:
        return cls(**decode(data))


@dataclass
class TelemetryMsg:
    """Per-service telemetry heartbeat (every 2s)."""
    service: str
    status: str = "running"  # running, degraded, error
    uptime_s: float = 0.0
    msgs_in: int = 0
    msgs_out: int = 0
    errors: int = 0
    extra: dict[str, Any] = field(default_factory=dict)
    ts: float = field(default_factory=time.time)

    def encode(self) -> bytes:
        return encode(asdict(self))

    @classmethod
    def decode(cls, data: bytes) -> TelemetryMsg:
        return cls(**decode(data))

    def topic(self) -> str:
        return Topics.telemetry(self.service)


@dataclass
class HealthMsg:
    """System-wide health beacon."""
    services: dict[str, str] = field(default_factory=dict)  # service -> status
    ts: float = field(default_factory=time.time)

    def encode(self) -> bytes:
        return encode(asdict(self))

    @classmethod
    def decode(cls, data: bytes) -> HealthMsg:
        return cls(**decode(data))
