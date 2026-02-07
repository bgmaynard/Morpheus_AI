"""Replay Engine — replays historical Databento data through the momentum engine.

Supports two modes:
1. API mode: fetches data via DatabentoClient (with caching)
2. File mode: replays from local .dbn files on disk

Merges trade and MBP-10 streams by timestamp into a single chronological
sequence, feeding each event to the momentum engine. Emits momentum state
change events and logs replay progress.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import databento as db

from morpheus.ai.momentum_engine import MomentumEngine, MomentumSnapshot
from morpheus.services.databento_client import DatabentoClient
from morpheus.services.market_normalizer import (
    NormalizedBookSnapshot,
    NormalizedTrade,
    normalize_mbp10,
    normalize_trades,
)

logger = logging.getLogger(__name__)


def _default_replay_dir() -> str:
    base = os.getenv("MORPHEUS_DATA_ROOT", "D:/AI_BOT_DATA")
    return f"{base}/replays"


@dataclass
class ReplayResult:
    """Summary of a replay session."""
    symbols: list[str]
    start: str
    end: str
    total_events: int
    total_trades: int
    total_books: int
    state_changes: int
    final_snapshots: dict[str, dict]
    log_file: str | None


EventCallback = Callable[[NormalizedTrade | NormalizedBookSnapshot, MomentumSnapshot | None], None]


class ReplayEngine:
    """Replay historical market data through the momentum engine.

    Usage:
        client = DatabentoClient()
        engine = MomentumEngine()
        replay = ReplayEngine(client, engine)
        result = replay.replay_session(["AAPL"], "2026-02-06", "2026-02-07")
    """

    def __init__(
        self,
        databento_client: DatabentoClient | None = None,
        momentum_engine: MomentumEngine | None = None,
        replay_dir: str | None = None,
    ):
        self._client = databento_client
        self._engine = momentum_engine or MomentumEngine()
        self._replay_dir = Path(replay_dir or _default_replay_dir())
        self._replay_dir.mkdir(parents=True, exist_ok=True)

    def replay_session(
        self,
        symbols: list[str],
        start: str,
        end: str,
        callback: EventCallback | None = None,
        log_to_disk: bool = True,
        dataset: str | None = None,
    ) -> ReplayResult:
        """Replay a session from Databento API (with caching).

        Args:
            symbols: List of ticker symbols to replay
            start: Start date (inclusive), e.g. "2026-02-06"
            end: End date (exclusive), e.g. "2026-02-07"
            callback: Optional function called after each event
            log_to_disk: Whether to write replay log to disk
            dataset: Databento dataset override

        Returns:
            ReplayResult with summary and final momentum snapshots
        """
        if not self._client:
            raise RuntimeError("DatabentoClient required for API replay")

        logger.info("Replay session: %s | %s to %s", symbols, start, end)

        # Fetch data (cached if available)
        trades_store = self._client.get_trades_cached(symbols, start, end, dataset)
        mbp10_store = self._client.get_mbp10_cached(symbols, start, end, dataset)

        # Normalize
        trades = normalize_trades(trades_store)
        books = normalize_mbp10(mbp10_store)

        return self._run_replay(symbols, start, end, trades, books, callback, log_to_disk)

    def replay_from_files(
        self,
        trades_path: str,
        mbp10_path: str | None = None,
        callback: EventCallback | None = None,
        log_to_disk: bool = True,
    ) -> ReplayResult:
        """Replay from local .dbn files.

        Args:
            trades_path: Path to trades .dbn/.dbn.zst file
            mbp10_path: Optional path to MBP-10 .dbn/.dbn.zst file
            callback: Optional function called after each event
            log_to_disk: Whether to write replay log to disk

        Returns:
            ReplayResult with summary and final momentum snapshots
        """
        logger.info("Replay from files: trades=%s mbp10=%s", trades_path, mbp10_path)

        trades_store = db.DBNStore.from_file(trades_path)
        trades = normalize_trades(trades_store)

        books: list[NormalizedBookSnapshot] = []
        if mbp10_path:
            mbp10_store = db.DBNStore.from_file(mbp10_path)
            books = normalize_mbp10(mbp10_store)

        # Extract symbols from data
        symbols = sorted(set(t.symbol for t in trades))
        start = trades[0].timestamp.strftime("%Y-%m-%d") if trades else "unknown"
        end = trades[-1].timestamp.strftime("%Y-%m-%d") if trades else "unknown"

        return self._run_replay(symbols, start, end, trades, books, callback, log_to_disk)

    def _run_replay(
        self,
        symbols: list[str],
        start: str,
        end: str,
        trades: list[NormalizedTrade],
        books: list[NormalizedBookSnapshot],
        callback: EventCallback | None,
        log_to_disk: bool,
    ) -> ReplayResult:
        """Core replay loop: merge streams, feed engine, track state changes."""

        # Reset engine for clean replay
        for sym in symbols:
            self._engine.reset(sym)

        # Merge by timestamp
        merged = self._merge_streams(trades, books)

        logger.info(
            "Replay: %d events (%d trades + %d books) for %s",
            len(merged), len(trades), len(books), symbols,
        )

        # Track state changes via the engine's on_state_change callback.
        # The old approach (comparing get_snapshot after ingest) was broken
        # because _recompute() updates last_snapshot before we can compare.
        state_change_count = [0]  # mutable container for closure
        original_cb = self._engine._on_state_change

        def _count_state_changes(snapshot: MomentumSnapshot) -> None:
            state_change_count[0] += 1
            if original_cb:
                original_cb(snapshot)

        self._engine._on_state_change = _count_state_changes

        log_file = None
        log_handle = None

        if log_to_disk and merged:
            safe_start = start.replace(":", "")
            safe_end = end.replace(":", "")
            log_file = self._replay_dir / f"replay_{safe_start}_{safe_end}_{'_'.join(symbols[:3])}.jsonl"
            log_file.parent.mkdir(parents=True, exist_ok=True)
            log_handle = open(log_file, "w")

        try:
            for event in merged:
                snapshot = None

                if isinstance(event, NormalizedTrade):
                    snapshot = self._engine.ingest_trade(event)
                elif isinstance(event, NormalizedBookSnapshot):
                    snapshot = self._engine.ingest_book(event)

                if callback:
                    callback(event, snapshot)

                # Log snapshots (not every event — only when recomputed)
                if log_handle and snapshot:
                    log_handle.write(json.dumps(snapshot.to_dict()) + "\n")

        finally:
            self._engine._on_state_change = original_cb
            if log_handle:
                log_handle.close()

        # Collect final snapshots
        final_snapshots = {}
        for sym in symbols:
            snap = self._engine.get_snapshot(sym)
            if snap:
                final_snapshots[sym] = snap.to_dict()

        result = ReplayResult(
            symbols=symbols,
            start=start,
            end=end,
            total_events=len(merged),
            total_trades=len(trades),
            total_books=len(books),
            state_changes=state_change_count[0],
            final_snapshots=final_snapshots,
            log_file=str(log_file) if log_file else None,
        )

        logger.info(
            "Replay complete: %d events, %d state changes | %s",
            result.total_events, result.state_changes, symbols,
        )

        return result

    @staticmethod
    def _merge_streams(
        trades: list[NormalizedTrade],
        books: list[NormalizedBookSnapshot],
    ) -> list[NormalizedTrade | NormalizedBookSnapshot]:
        """Merge trade and book streams into single chronological sequence."""
        combined: list[NormalizedTrade | NormalizedBookSnapshot] = []
        combined.extend(trades)
        combined.extend(books)
        combined.sort(key=lambda e: e.timestamp)
        return combined
