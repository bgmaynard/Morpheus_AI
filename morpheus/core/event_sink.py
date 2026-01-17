"""
Event Sink - Append-only JSONL event logging.

Events are written to daily log files:
    logs/events/events_YYYY-MM-DD.jsonl

Thread-safe for concurrent writes.
"""

from __future__ import annotations

import json
import threading
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterator

from morpheus.core.events import Event


class EventSink:
    """
    Append-only event sink that writes events to daily JSONL files.

    Thread-safe: uses a lock for file operations.
    """

    def __init__(self, log_dir: Path | str = "./logs/events"):
        self._log_dir = Path(log_dir)
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._current_date: date | None = None
        self._current_file: Path | None = None

    def _get_log_file(self, event_date: date) -> Path:
        """Get the log file path for a given date."""
        return self._log_dir / f"events_{event_date.isoformat()}.jsonl"

    def _ensure_file_for_date(self, event_date: date) -> Path:
        """Ensure we have the correct file open for the given date."""
        if self._current_date != event_date:
            self._current_date = event_date
            self._current_file = self._get_log_file(event_date)
        return self._current_file

    def emit(self, event: Event) -> None:
        """
        Write an event to the log.

        Events are written as single-line JSON (JSONL format).
        Thread-safe.
        """
        event_date = event.timestamp.date()
        event_dict = event.to_jsonl_dict()
        line = json.dumps(event_dict, separators=(",", ":"), default=str)

        with self._lock:
            log_file = self._ensure_file_for_date(event_date)
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(line + "\n")

    def emit_batch(self, events: list[Event]) -> None:
        """Write multiple events atomically."""
        if not events:
            return

        # Group events by date
        events_by_date: dict[date, list[str]] = {}
        for event in events:
            event_date = event.timestamp.date()
            event_dict = event.to_jsonl_dict()
            line = json.dumps(event_dict, separators=(",", ":"), default=str)
            if event_date not in events_by_date:
                events_by_date[event_date] = []
            events_by_date[event_date].append(line)

        with self._lock:
            for event_date, lines in events_by_date.items():
                log_file = self._get_log_file(event_date)
                with open(log_file, "a", encoding="utf-8") as f:
                    for line in lines:
                        f.write(line + "\n")

    def read_events(self, event_date: date) -> Iterator[Event]:
        """
        Read all events from a specific date's log file.

        Yields events in order they were written.
        """
        log_file = self._get_log_file(event_date)
        if not log_file.exists():
            return

        with open(log_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    data = json.loads(line)
                    yield Event.from_jsonl_dict(data)

    def read_events_range(
        self, start_date: date, end_date: date
    ) -> Iterator[Event]:
        """
        Read events from a date range (inclusive).

        Yields events in chronological order by file, then by line order.
        """
        current = start_date
        while current <= end_date:
            yield from self.read_events(current)
            # Move to next day
            current = date.fromordinal(current.toordinal() + 1)

    def read_all_events(self) -> Iterator[Event]:
        """
        Read all events from all log files in the log directory.

        Files are processed in date order.
        """
        log_files = sorted(self._log_dir.glob("events_*.jsonl"))
        for log_file in log_files:
            with open(log_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        data = json.loads(line)
                        yield Event.from_jsonl_dict(data)

    def get_log_dir(self) -> Path:
        """Return the log directory path."""
        return self._log_dir

    def get_log_files(self) -> list[Path]:
        """Return list of all log files, sorted by date."""
        return sorted(self._log_dir.glob("events_*.jsonl"))


# Global sink instance (lazy-loaded)
_sink: EventSink | None = None


def get_event_sink() -> EventSink:
    """Get or create the global event sink."""
    global _sink
    if _sink is None:
        from morpheus.core.config import get_config

        config = get_config()
        _sink = EventSink(config.runtime.event_log_dir)
    return _sink


def reset_event_sink() -> None:
    """Reset the global event sink (useful for testing)."""
    global _sink
    _sink = None


def emit_event(event: Event) -> None:
    """Convenience function to emit an event to the global sink."""
    get_event_sink().emit(event)
