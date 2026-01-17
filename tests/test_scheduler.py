"""
Tests for SnapshotScheduler.

Phase 2 test coverage:
- Scheduler configuration
- Event emission per symbol
- Market hours awareness
- Error handling and backoff
"""

import pytest
import time
import tempfile
from datetime import datetime, timezone, time as dt_time, timedelta
from pathlib import Path
from unittest.mock import Mock, MagicMock

from morpheus.services.scheduler import (
    SnapshotScheduler,
    SchedulerConfig,
    SchedulerState,
    MarketHours,
)
from morpheus.data.market_snapshot import create_snapshot, MarketSnapshot
from morpheus.core.event_sink import EventSink
from morpheus.core.events import EventType


class TestMarketHours:
    """Tests for MarketHours checker."""

    def test_market_closed_on_weekend(self):
        """Market should be closed on weekends."""
        hours = MarketHours()

        # Saturday at noon EST (17:00 UTC)
        saturday = datetime(2024, 6, 15, 17, 0, 0, tzinfo=timezone.utc)
        # Verify it's actually a Saturday
        assert saturday.weekday() == 5

        assert hours.is_market_open(saturday) is False

    def test_market_closed_before_open(self):
        """Market should be closed before 9:30 AM."""
        hours = MarketHours()

        # Monday at 9:00 AM EST (14:00 UTC)
        monday_early = datetime(2024, 6, 17, 14, 0, 0, tzinfo=timezone.utc)
        # Verify it's Monday
        assert monday_early.weekday() == 0

        assert hours.is_market_open(monday_early) is False

    def test_market_open_during_hours(self):
        """Market should be open during trading hours."""
        hours = MarketHours()

        # Monday at 10:00 AM EST (15:00 UTC)
        monday_trading = datetime(2024, 6, 17, 15, 0, 0, tzinfo=timezone.utc)

        assert hours.is_market_open(monday_trading) is True

    def test_market_closed_after_close(self):
        """Market should be closed after 4:00 PM."""
        hours = MarketHours()

        # Monday at 4:30 PM EST (21:30 UTC)
        monday_late = datetime(2024, 6, 17, 21, 30, 0, tzinfo=timezone.utc)

        assert hours.is_market_open(monday_late) is False


class TestSchedulerConfig:
    """Tests for SchedulerConfig."""

    def test_default_config(self):
        """Default config should have sensible values."""
        config = SchedulerConfig()

        assert config.quote_interval == 5.0
        assert config.candle_interval == 60.0
        assert config.symbols == []
        assert config.respect_market_hours is True
        assert config.emit_events is True

    def test_custom_config(self):
        """Config should accept custom values."""
        config = SchedulerConfig(
            quote_interval=2.0,
            candle_interval=30.0,
            symbols=["SPY", "QQQ"],
            respect_market_hours=False,
        )

        assert config.quote_interval == 2.0
        assert config.symbols == ["SPY", "QQQ"]
        assert config.respect_market_hours is False


class TestSnapshotScheduler:
    """Tests for SnapshotScheduler."""

    def _create_mock_provider(
        self, symbol: str = "TEST", fail: bool = False
    ) -> Mock:
        """Create a mock snapshot provider."""

        def provider(sym: str, include_candles: bool) -> MarketSnapshot:
            if fail:
                raise Exception("Mock error")
            return create_snapshot(
                symbol=sym,
                bid=100.0,
                ask=100.10,
                last=100.05,
                volume=1000000,
            )

        return Mock(side_effect=provider)

    def test_scheduler_initial_state(self):
        """Scheduler should start in STOPPED state."""
        config = SchedulerConfig(symbols=["TEST"])
        provider = self._create_mock_provider()

        scheduler = SnapshotScheduler(
            config=config,
            snapshot_provider=provider,
        )

        assert scheduler.state == SchedulerState.STOPPED
        assert scheduler.is_running is False

    def test_scheduler_start_stop(self):
        """Scheduler should start and stop cleanly."""
        config = SchedulerConfig(
            symbols=["TEST"],
            quote_interval=0.1,
            respect_market_hours=False,
        )
        provider = self._create_mock_provider()

        scheduler = SnapshotScheduler(
            config=config,
            snapshot_provider=provider,
        )

        scheduler.start()
        time.sleep(0.05)  # Give thread time to start

        assert scheduler.state == SchedulerState.RUNNING

        scheduler.stop(timeout=1.0)

        assert scheduler.state == SchedulerState.STOPPED

    def test_scheduler_emits_events(self):
        """Scheduler should emit MARKET_SNAPSHOT events."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sink = EventSink(Path(tmpdir))

            config = SchedulerConfig(
                symbols=["TEST"],
                quote_interval=0.1,
                respect_market_hours=False,
                include_candles=False,
            )
            provider = self._create_mock_provider()

            scheduler = SnapshotScheduler(
                config=config,
                snapshot_provider=provider,
                event_sink=sink,
            )

            scheduler.start()
            time.sleep(0.25)  # Allow a couple of poll cycles
            scheduler.stop(timeout=1.0)

            # Read events from sink
            events = list(sink.read_all_events())

            # Should have emitted at least one event
            assert len(events) >= 1

            # All events should be MARKET_SNAPSHOT
            snapshot_events = [
                e for e in events if e.event_type == EventType.MARKET_SNAPSHOT
            ]
            assert len(snapshot_events) >= 1

            # Check event content
            event = snapshot_events[0]
            assert event.symbol == "TEST"
            assert "schema_version" in event.payload

    def test_scheduler_emits_per_symbol(self):
        """Scheduler should emit one event per symbol."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sink = EventSink(Path(tmpdir))

            config = SchedulerConfig(
                symbols=["SPY", "QQQ", "AAPL"],
                quote_interval=0.1,
                respect_market_hours=False,
                include_candles=False,
            )
            provider = self._create_mock_provider()

            scheduler = SnapshotScheduler(
                config=config,
                snapshot_provider=provider,
                event_sink=sink,
            )

            scheduler.start()
            time.sleep(0.15)  # One poll cycle
            scheduler.stop(timeout=1.0)

            events = list(sink.read_all_events())
            snapshot_events = [
                e for e in events if e.event_type == EventType.MARKET_SNAPSHOT
            ]

            # Should have events for all three symbols
            symbols_seen = {e.symbol for e in snapshot_events}
            assert "SPY" in symbols_seen
            assert "QQQ" in symbols_seen
            assert "AAPL" in symbols_seen

    def test_scheduler_handles_errors(self):
        """Scheduler should handle provider errors gracefully."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sink = EventSink(Path(tmpdir))

            config = SchedulerConfig(
                symbols=["FAIL"],
                quote_interval=0.1,
                respect_market_hours=False,
                max_consecutive_errors=3,
                error_backoff_seconds=0.1,
            )
            provider = self._create_mock_provider(fail=True)

            scheduler = SnapshotScheduler(
                config=config,
                snapshot_provider=provider,
                event_sink=sink,
            )

            scheduler.start()
            time.sleep(0.5)  # Allow errors to accumulate
            scheduler.stop(timeout=1.0)

            # Should have emitted error events
            stats = scheduler.get_stats()
            assert stats["errors_emitted"] > 0

    def test_scheduler_stats(self):
        """Scheduler should track statistics."""
        config = SchedulerConfig(
            symbols=["STATS"],
            quote_interval=0.1,
            respect_market_hours=False,
            include_candles=False,
        )
        provider = self._create_mock_provider()

        scheduler = SnapshotScheduler(
            config=config,
            snapshot_provider=provider,
        )

        scheduler.start()
        time.sleep(0.25)
        scheduler.stop(timeout=1.0)

        stats = scheduler.get_stats()

        assert stats["state"] == "STOPPED"
        assert stats["symbols"] == ["STATS"]
        assert stats["quote_interval"] == 0.1
        assert stats["snapshots_emitted"] >= 1

    def test_scheduler_add_remove_symbol(self):
        """Scheduler should allow dynamic symbol management."""
        config = SchedulerConfig(symbols=["SPY"])
        provider = self._create_mock_provider()

        scheduler = SnapshotScheduler(
            config=config,
            snapshot_provider=provider,
        )

        assert "SPY" in scheduler.config.symbols

        scheduler.add_symbol("QQQ")
        assert "QQQ" in scheduler.config.symbols

        scheduler.add_symbol("spy")  # Duplicate (case-insensitive)
        assert scheduler.config.symbols.count("SPY") == 1

        scheduler.remove_symbol("SPY")
        assert "SPY" not in scheduler.config.symbols

    def test_scheduler_pause_resume(self):
        """Scheduler should support pause/resume."""
        config = SchedulerConfig(
            symbols=["PAUSE"],
            quote_interval=0.1,
            respect_market_hours=False,
        )
        provider = self._create_mock_provider()

        scheduler = SnapshotScheduler(
            config=config,
            snapshot_provider=provider,
        )

        scheduler.start()
        time.sleep(0.05)

        scheduler.pause()
        assert scheduler.state == SchedulerState.PAUSED

        scheduler.resume()
        assert scheduler.state == SchedulerState.RUNNING

        scheduler.stop(timeout=1.0)


class TestSchedulerMarketHoursIntegration:
    """Tests for scheduler with market hours awareness."""

    def test_scheduler_skips_when_market_closed(self):
        """Scheduler should skip polling when market is closed."""
        # Create a market hours that always returns False
        mock_hours = Mock(spec=MarketHours)
        mock_hours.is_market_open.return_value = False

        config = SchedulerConfig(
            symbols=["SKIP"],
            quote_interval=0.1,
            respect_market_hours=True,
            include_candles=False,
        )

        call_count = 0

        def counting_provider(sym: str, include_candles: bool) -> MarketSnapshot:
            nonlocal call_count
            call_count += 1
            return create_snapshot(
                symbol=sym,
                bid=100.0,
                ask=100.10,
                last=100.05,
                volume=1000000,
            )

        scheduler = SnapshotScheduler(
            config=config,
            snapshot_provider=counting_provider,
            market_hours=mock_hours,
        )

        scheduler.start()
        time.sleep(0.3)
        scheduler.stop(timeout=1.0)

        # Provider should not have been called (market was "closed")
        assert call_count == 0
