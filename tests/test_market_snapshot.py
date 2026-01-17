"""
Tests for MarketSnapshot and related data models.

Phase 2 test coverage:
- Snapshot creation with schema version
- Event emission from snapshots
- Candle data handling
- Serialization/deserialization for replay
"""

import pytest
from datetime import datetime, timezone, timedelta

from morpheus.data.market_snapshot import (
    MarketSnapshot,
    Candle,
    create_snapshot,
    candle_from_dict,
    snapshot_from_event_payload,
    SNAPSHOT_SCHEMA_VERSION,
)
from morpheus.core.events import EventType


class TestCandle:
    """Tests for Candle dataclass."""

    def test_candle_creation(self):
        """Candle should be created with all fields."""
        ts = datetime.now(timezone.utc)
        candle = Candle(
            open=100.0,
            high=105.0,
            low=99.0,
            close=103.0,
            volume=1000000,
            timestamp=ts,
        )

        assert candle.open == 100.0
        assert candle.high == 105.0
        assert candle.low == 99.0
        assert candle.close == 103.0
        assert candle.volume == 1000000
        assert candle.timestamp == ts

    def test_candle_immutable(self):
        """Candle should be immutable (frozen)."""
        candle = Candle(
            open=100.0,
            high=105.0,
            low=99.0,
            close=103.0,
            volume=1000000,
            timestamp=datetime.now(timezone.utc),
        )

        with pytest.raises(AttributeError):
            candle.close = 110.0  # type: ignore

    def test_candle_from_dict(self):
        """Candle should be creatable from dictionary."""
        ts = datetime.now(timezone.utc)
        data = {
            "open": 100.0,
            "high": 105.0,
            "low": 99.0,
            "close": 103.0,
            "volume": 1000000,
            "timestamp": ts.isoformat(),
        }

        candle = candle_from_dict(data)

        assert candle.open == 100.0
        assert candle.close == 103.0


class TestMarketSnapshot:
    """Tests for MarketSnapshot dataclass."""

    def test_snapshot_has_schema_version(self):
        """Every snapshot must have schema_version field."""
        snapshot = create_snapshot(
            symbol="SPY",
            bid=450.0,
            ask=450.10,
            last=450.05,
            volume=50000000,
        )

        assert snapshot.schema_version == SNAPSHOT_SCHEMA_VERSION
        assert snapshot.schema_version == "1.0"

    def test_snapshot_creation_with_computed_fields(self):
        """create_snapshot should compute spread and mid."""
        snapshot = create_snapshot(
            symbol="AAPL",
            bid=150.00,
            ask=150.10,
            last=150.05,
            volume=10000000,
        )

        assert snapshot.symbol == "AAPL"
        assert snapshot.bid == 150.00
        assert snapshot.ask == 150.10
        assert snapshot.spread == pytest.approx(0.10, rel=1e-6)
        assert snapshot.mid == pytest.approx(150.05, rel=1e-6)
        # spread_pct = spread / mid * 100 = 0.10 / 150.05 * 100
        assert snapshot.spread_pct == pytest.approx(0.0666, rel=1e-2)

    def test_snapshot_immutable(self):
        """Snapshot should be immutable (frozen)."""
        snapshot = create_snapshot(
            symbol="MSFT",
            bid=300.0,
            ask=300.10,
            last=300.05,
            volume=5000000,
        )

        with pytest.raises(AttributeError):
            snapshot.last = 301.0  # type: ignore

    def test_snapshot_with_candles(self):
        """Snapshot should include candle data when provided."""
        ts = datetime.now(timezone.utc)
        candle_1m = Candle(
            open=100.0,
            high=101.0,
            low=99.5,
            close=100.5,
            volume=100000,
            timestamp=ts - timedelta(minutes=1),
        )
        candle_5m = Candle(
            open=99.0,
            high=101.0,
            low=98.5,
            close=100.5,
            volume=500000,
            timestamp=ts - timedelta(minutes=5),
        )

        snapshot = create_snapshot(
            symbol="QQQ",
            bid=380.0,
            ask=380.10,
            last=380.05,
            volume=20000000,
            candle_1m=candle_1m,
            candle_5m=candle_5m,
        )

        assert snapshot.candle_1m is not None
        assert snapshot.candle_5m is not None
        assert snapshot.candle_1m.close == 100.5
        assert snapshot.candle_5m.volume == 500000

    def test_snapshot_timestamp_is_utc(self):
        """Snapshot timestamp should be UTC."""
        snapshot = create_snapshot(
            symbol="NVDA",
            bid=500.0,
            ask=500.20,
            last=500.10,
            volume=30000000,
        )

        assert snapshot.timestamp.tzinfo == timezone.utc

    def test_snapshot_with_explicit_timestamp(self):
        """Snapshot should use explicit timestamp when provided."""
        explicit_ts = datetime(2024, 6, 15, 10, 30, 0, tzinfo=timezone.utc)

        snapshot = create_snapshot(
            symbol="AMD",
            bid=150.0,
            ask=150.05,
            last=150.02,
            volume=10000000,
            timestamp=explicit_ts,
        )

        assert snapshot.timestamp == explicit_ts


class TestSnapshotEventEmission:
    """Tests for snapshot to event conversion."""

    def test_snapshot_to_event(self):
        """Snapshot should convert to MARKET_SNAPSHOT event."""
        snapshot = create_snapshot(
            symbol="TSLA",
            bid=250.0,
            ask=250.50,
            last=250.25,
            volume=80000000,
        )

        event = snapshot.to_event()

        assert event.event_type == EventType.MARKET_SNAPSHOT
        assert event.symbol == "TSLA"

    def test_event_payload_includes_schema_version(self):
        """Event payload must include schema_version."""
        snapshot = create_snapshot(
            symbol="META",
            bid=300.0,
            ask=300.10,
            last=300.05,
            volume=15000000,
        )

        event = snapshot.to_event()

        assert "schema_version" in event.payload
        assert event.payload["schema_version"] == "1.0"

    def test_event_payload_includes_all_fields(self):
        """Event payload should include all snapshot fields."""
        snapshot = create_snapshot(
            symbol="GOOG",
            bid=140.0,
            ask=140.10,
            last=140.05,
            volume=25000000,
        )

        payload = snapshot.to_event_payload()

        assert payload["symbol"] == "GOOG"
        assert payload["bid"] == 140.0
        assert payload["ask"] == 140.10
        assert payload["last"] == 140.05
        assert payload["volume"] == 25000000
        assert payload["spread"] == pytest.approx(0.10, rel=1e-6)
        assert payload["mid"] == pytest.approx(140.05, rel=1e-6)
        assert payload["source"] == "schwab"
        assert payload["is_market_open"] is True
        assert payload["is_tradeable"] is True

    def test_event_payload_includes_candles_when_present(self):
        """Event payload should include candle data when present."""
        ts = datetime.now(timezone.utc)
        candle = Candle(
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=50000,
            timestamp=ts,
        )

        snapshot = create_snapshot(
            symbol="AMZN",
            bid=180.0,
            ask=180.10,
            last=180.05,
            volume=30000000,
            candle_1m=candle,
        )

        payload = snapshot.to_event_payload()

        assert "candle_1m" in payload
        assert payload["candle_1m"]["open"] == 100.0
        assert payload["candle_1m"]["close"] == 100.5


class TestSnapshotSerialization:
    """Tests for snapshot serialization/deserialization."""

    def test_snapshot_roundtrip(self):
        """Snapshot should survive event payload roundtrip."""
        ts = datetime(2024, 6, 15, 14, 30, 0, tzinfo=timezone.utc)
        candle = Candle(
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=50000,
            timestamp=ts - timedelta(minutes=1),
        )

        original = create_snapshot(
            symbol="NFLX",
            bid=600.0,
            ask=600.50,
            last=600.25,
            volume=5000000,
            timestamp=ts,
            candle_1m=candle,
            is_market_open=True,
            is_tradeable=True,
        )

        # Convert to payload and back
        payload = original.to_event_payload()
        reconstructed = snapshot_from_event_payload(payload)

        assert reconstructed.schema_version == original.schema_version
        assert reconstructed.symbol == original.symbol
        assert reconstructed.bid == original.bid
        assert reconstructed.ask == original.ask
        assert reconstructed.last == original.last
        assert reconstructed.volume == original.volume
        assert reconstructed.spread == pytest.approx(original.spread, rel=1e-6)
        assert reconstructed.mid == pytest.approx(original.mid, rel=1e-6)
        assert reconstructed.is_market_open == original.is_market_open
        assert reconstructed.candle_1m is not None
        assert reconstructed.candle_1m.close == 100.5

    def test_snapshot_from_payload_handles_missing_candles(self):
        """Reconstruction should handle missing candle data."""
        payload = {
            "schema_version": "1.0",
            "symbol": "DIS",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "bid": 100.0,
            "ask": 100.10,
            "last": 100.05,
            "volume": 1000000,
            "spread": 0.10,
            "spread_pct": 0.1,
            "mid": 100.05,
            "source": "schwab",
            "is_market_open": True,
            "is_tradeable": True,
        }

        snapshot = snapshot_from_event_payload(payload)

        assert snapshot.candle_1m is None
        assert snapshot.candle_5m is None
