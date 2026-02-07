"""Batch Premarket Replay — runs multiple symbol/date pairs through the momentum engine.

Usage:
    python scripts/batch_premarket_replay.py

Runs all configured symbol/date pairs, collects enriched JSONL, and prints summary.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import dotenv
dotenv.load_dotenv(PROJECT_ROOT / ".env")

from morpheus.ai.momentum_engine import MomentumEngine, MomentumSnapshot, MomentumState
from morpheus.services.databento_client import DatabentoClient, DatabentoConfig
from morpheus.services.market_normalizer import NormalizedTrade, NormalizedBookSnapshot
from morpheus.services.replay_engine import ReplayEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("batch_replay")

DATA_ROOT = os.getenv("MORPHEUS_DATA_ROOT", "D:/AI_BOT_DATA")

# ── Symbol/date pairs to replay ──────────────────────────────────────
# Window extended to 10:00 ET (15:00 UTC) to capture RTH open spike + first pullback.
# Premarket-only (07:00-09:30) showed all symbols stuck in BUILDING with no exits.
# The ignition cycle is: PREMARKET BUILD → RTH OPEN PEAK → FIRST PULLBACK DECAY.
REPLAY_TARGETS = [
    {"symbol": "QNST", "date": "2026-02-06", "catalyst": "Earnings +25.4%",
     "start": "2026-02-06T12:00:00", "end": "2026-02-06T15:00:00"},
    {"symbol": "JAGX", "date": "2026-01-16", "catalyst": "News +41.3%",
     "start": "2026-01-16T12:00:00", "end": "2026-01-16T15:00:00"},
    {"symbol": "BZAI", "date": "2026-01-27", "catalyst": "Nokia AI deal +26.4%",
     "start": "2026-01-27T12:00:00", "end": "2026-01-27T15:00:00"},
    {"symbol": "IBRX", "date": "2026-01-16", "catalyst": "Revenue beat +22.8%",
     "start": "2026-01-16T12:00:00", "end": "2026-01-16T15:00:00"},
    {"symbol": "BFRI", "date": "2026-01-02", "catalyst": "Biotech +28.5%",
     "start": "2026-01-02T12:00:00", "end": "2026-01-02T15:00:00"},
]


@dataclass
class ReplaySessionResult:
    symbol: str
    date: str
    catalyst: str
    total_events: int
    total_trades: int
    total_books: int
    state_changes: int
    enriched_snapshots: int
    enriched_path: str
    replay_log_path: str | None
    final_score: float
    final_state: str
    final_confidence: float
    success: bool
    error: str | None = None


def run_single_replay(
    symbol: str,
    date_str: str,
    catalyst: str,
    client: DatabentoClient,
    start_utc: str | None = None,
    end_utc: str | None = None,
    dataset: str = "XNAS.ITCH",
) -> ReplaySessionResult:
    """Run a single symbol/date replay and return results."""

    start_utc = start_utc or f"{date_str}T12:00:00"
    end_utc = end_utc or f"{date_str}T15:00:00"
    safe_start = start_utc.replace(":", "")
    safe_end = end_utc.replace(":", "")

    print(f"\n  [{symbol}] Replaying {date_str} premarket ({catalyst})...")

    # Fresh engine with state change tracking
    state_changes: list[MomentumSnapshot] = []

    def on_state_change(snapshot: MomentumSnapshot):
        state_changes.append(snapshot)

    engine = MomentumEngine(on_state_change=on_state_change)
    replay = ReplayEngine(
        databento_client=client,
        momentum_engine=engine,
        replay_dir=f"{DATA_ROOT}/replays",
    )

    # Enriched JSONL
    enriched_path = Path(f"{DATA_ROOT}/replays/enriched_{safe_start}_{safe_end}_{symbol}.jsonl")
    enriched_path.parent.mkdir(parents=True, exist_ok=True)

    latest_trade_price: dict[str, float] = {}
    latest_book_mid: dict[str, float] = {}
    enriched_count = [0]

    enriched_handle = open(enriched_path, "w")

    def replay_callback(event, snapshot):
        if isinstance(event, NormalizedTrade):
            latest_trade_price[event.symbol] = event.price
        elif isinstance(event, NormalizedBookSnapshot):
            if event.mid > 0:
                latest_book_mid[event.symbol] = event.mid

        if snapshot is not None:
            enriched = snapshot.to_dict()
            enriched["last_trade_price"] = latest_trade_price.get(snapshot.symbol, 0.0)
            enriched["book_mid"] = latest_book_mid.get(snapshot.symbol, 0.0)
            enriched_handle.write(json.dumps(enriched) + "\n")
            enriched_count[0] += 1

    try:
        result = replay.replay_session(
            symbols=[symbol],
            start=start_utc,
            end=end_utc,
            callback=replay_callback,
            log_to_disk=True,
            dataset=dataset,
        )
        enriched_handle.close()

        snap = engine.get_snapshot(symbol)

        session_result = ReplaySessionResult(
            symbol=symbol,
            date=date_str,
            catalyst=catalyst,
            total_events=result.total_events,
            total_trades=result.total_trades,
            total_books=result.total_books,
            state_changes=result.state_changes,
            enriched_snapshots=enriched_count[0],
            enriched_path=str(enriched_path),
            replay_log_path=result.log_file,
            final_score=snap.momentum_score if snap else 0.0,
            final_state=snap.momentum_state.value if snap else "UNKNOWN",
            final_confidence=snap.confidence if snap else 0.0,
            success=True,
        )

        final_state = snap.momentum_state.value if snap else "?"
        final_score = f"{snap.momentum_score:.1f}" if snap else "0"
        print(f"  [{symbol}] OK: {result.total_events} events, "
              f"{result.state_changes} state changes, "
              f"{enriched_count[0]} snapshots, "
              f"final={final_state} score={final_score}")

        # Print state change details
        for sc in state_changes:
            print(f"           {sc.timestamp.strftime('%H:%M:%S')} UTC  "
                  f"{sc.momentum_state.value}  score={sc.momentum_score:.1f}")

        return session_result

    except Exception as e:
        enriched_handle.close()
        logger.exception("Replay failed for %s", symbol)
        return ReplaySessionResult(
            symbol=symbol, date=date_str, catalyst=catalyst,
            total_events=0, total_trades=0, total_books=0,
            state_changes=0, enriched_snapshots=0,
            enriched_path=str(enriched_path),
            replay_log_path=None,
            final_score=0, final_state="ERROR", final_confidence=0,
            success=False, error=str(e),
        )


def main():
    print("=" * 60)
    print("  Batch Premarket Momentum Replay")
    print(f"  Targets: {len(REPLAY_TARGETS)} symbols")
    print("  Window: 07:00-10:00 ET (12:00-15:00 UTC)")
    print("=" * 60)

    try:
        client = DatabentoClient(DatabentoConfig(
            max_cost_per_request=5.0,
            max_daily_spend=25.0,
            default_dataset="XNAS.ITCH",
        ))
    except ValueError as e:
        print(f"\nERROR: {e}")
        sys.exit(1)

    results: list[ReplaySessionResult] = []

    for target in REPLAY_TARGETS:
        r = run_single_replay(
            symbol=target["symbol"],
            date_str=target["date"],
            catalyst=target["catalyst"],
            client=client,
            start_utc=target.get("start"),
            end_utc=target.get("end"),
        )
        results.append(r)

    # ── Summary ───────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print("  BATCH REPLAY SUMMARY")
    print(f"{'=' * 60}\n")

    print(f"  {'Symbol':<8} {'Date':<12} {'Events':>7} {'Trades':>7} "
          f"{'Books':>7} {'Changes':>8} {'Snaps':>7} {'Final State':<12} {'Score':>6}")
    print(f"  {'-'*8} {'-'*12} {'-'*7} {'-'*7} {'-'*7} {'-'*8} {'-'*7} {'-'*12} {'-'*6}")

    total_events = 0
    total_changes = 0
    total_snapshots = 0

    for r in results:
        status = "FAIL" if not r.success else ""
        print(f"  {r.symbol:<8} {r.date:<12} {r.total_events:>7} {r.total_trades:>7} "
              f"{r.total_books:>7} {r.state_changes:>8} {r.enriched_snapshots:>7} "
              f"{r.final_state:<12} {r.final_score:>6.1f} {status}")
        total_events += r.total_events
        total_changes += r.state_changes
        total_snapshots += r.enriched_snapshots

    success_count = sum(1 for r in results if r.success)
    print(f"\n  Totals: {total_events} events, {total_changes} state changes, "
          f"{total_snapshots} snapshots")
    print(f"  Success: {success_count}/{len(results)}")
    print(f"  Databento daily spend: ${client.daily_spend:.4f}")

    # Save results manifest
    manifest_path = Path(f"{DATA_ROOT}/replays/batch_manifest.json")
    manifest = {
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "targets": REPLAY_TARGETS,
        "results": [
            {
                "symbol": r.symbol, "date": r.date, "success": r.success,
                "total_events": r.total_events, "total_trades": r.total_trades,
                "total_books": r.total_books, "state_changes": r.state_changes,
                "enriched_snapshots": r.enriched_snapshots,
                "enriched_path": r.enriched_path,
                "final_score": round(r.final_score, 2),
                "final_state": r.final_state,
                "final_confidence": round(r.final_confidence, 3),
                "error": r.error,
            }
            for r in results
        ],
        "totals": {
            "events": total_events,
            "state_changes": total_changes,
            "snapshots": total_snapshots,
            "databento_spend": round(client.daily_spend, 4),
        },
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"\n  Manifest: {manifest_path}")

    # List enriched files
    print(f"\n  Enriched JSONL files:")
    for r in results:
        if r.success:
            print(f"    {r.enriched_path}")

    print()
    return results


if __name__ == "__main__":
    main()
