"""Validate Premarket Momentum — fetch BBAI data, replay through engine, run checks.

Usage:
    python scripts/validate_premarket_momentum.py --symbol BBAI --date 2026-02-06

Steps:
1. Instantiate DatabentoClient, MomentumEngine (with state_change callback), ReplayEngine
2. Replay premarket window (07:00-09:30 ET = 12:00-14:30 UTC)
3. Write enriched JSONL with last_trade_price + book_mid appended
4. Run 4 verification checks (A-D)
5. If all pass, run playbook extractor + report generator
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Ensure project root is importable and load .env
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
logger = logging.getLogger("validate_premarket")


DATA_ROOT = os.getenv("MORPHEUS_DATA_ROOT", "D:/AI_BOT_DATA")


def main():
    parser = argparse.ArgumentParser(description="Validate premarket momentum replay")
    parser.add_argument("--symbol", default="BBAI", help="Ticker symbol (default: BBAI)")
    parser.add_argument("--date", default="2026-02-06", help="Date YYYY-MM-DD (default: 2026-02-06)")
    parser.add_argument("--dataset", default="XNAS.ITCH", help="Databento dataset (default: XNAS.ITCH)")
    parser.add_argument("--skip-report", action="store_true", help="Skip playbook report generation")
    args = parser.parse_args()

    symbol = args.symbol.upper()
    date_str = args.date
    dataset = args.dataset

    # Premarket window: 07:00-09:30 ET = 12:00-14:30 UTC
    start_utc = f"{date_str}T12:00:00"
    end_utc = f"{date_str}T14:30:00"

    print(f"\n{'='*60}")
    print(f"  Premarket Momentum Validation")
    print(f"  Symbol: {symbol}  |  Date: {date_str}")
    print(f"  Window: 07:00-09:30 ET ({start_utc} to {end_utc} UTC)")
    print(f"  Dataset: {dataset}")
    print(f"{'='*60}\n")

    # ── Setup ─────────────────────────────────────────────────────────
    state_changes: list[MomentumSnapshot] = []

    def on_state_change(snapshot: MomentumSnapshot):
        state_changes.append(snapshot)
        logger.info(
            "STATE CHANGE: %s → %s (score=%.1f, confidence=%.3f)",
            snapshot.symbol, snapshot.momentum_state.value,
            snapshot.momentum_score, snapshot.confidence,
        )

    engine = MomentumEngine(on_state_change=on_state_change)

    try:
        client = DatabentoClient(DatabentoConfig(
            max_cost_per_request=5.0,
            max_daily_spend=25.0,
            default_dataset=dataset,
        ))
    except ValueError as e:
        print(f"\nERROR: {e}")
        print("Set DATABENTO_API_KEY environment variable and retry.")
        sys.exit(1)

    replay = ReplayEngine(
        databento_client=client,
        momentum_engine=engine,
        replay_dir=f"{DATA_ROOT}/replays",
    )

    # ── Enriched JSONL writer ─────────────────────────────────────────
    safe_start = start_utc.replace(":", "")
    safe_end = end_utc.replace(":", "")
    enriched_path = Path(f"{DATA_ROOT}/replays/enriched_{safe_start}_{safe_end}_{symbol}.jsonl")
    enriched_path.parent.mkdir(parents=True, exist_ok=True)
    enriched_handle = open(enriched_path, "w")

    # Track latest trade price and book mid for enrichment
    latest_trade_price: dict[str, float] = {}
    latest_book_mid: dict[str, float] = {}

    def replay_callback(event, snapshot):
        """Callback for each replay event — track prices, write enriched JSONL."""
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

    # ── Run replay ────────────────────────────────────────────────────
    print("Running replay (this may fetch from Databento API on first run)...\n")

    try:
        result = replay.replay_session(
            symbols=[symbol],
            start=start_utc,
            end=end_utc,
            callback=replay_callback,
            log_to_disk=True,
            dataset=dataset,
        )
    except Exception as e:
        enriched_handle.close()
        print(f"\nREPLAY FAILED: {e}")
        logger.exception("Replay failed")
        sys.exit(1)

    enriched_handle.close()

    print(f"\nReplay complete:")
    print(f"  Events: {result.total_events} ({result.total_trades} trades + {result.total_books} books)")
    print(f"  State changes: {result.state_changes}")
    print(f"  Replay log: {result.log_file}")
    print(f"  Enriched log: {enriched_path}")

    # ── Verification checks ───────────────────────────────────────────
    print(f"\n{'-'*60}")
    print("  VERIFICATION CHECKS")
    print(f"{'-'*60}\n")

    checks_passed = 0
    total_checks = 4

    # Check A: .dbn.zst files exist
    cache_dir = Path(f"{DATA_ROOT}/databento_cache/{dataset}")
    trades_cache = list(cache_dir.glob(f"trades/{symbol}*{date_str}*.dbn.zst"))
    mbp10_cache = list(cache_dir.glob(f"mbp-10/{symbol}*{date_str}*.dbn.zst"))
    check_a = len(trades_cache) > 0 or len(mbp10_cache) > 0
    status_a = "PASS" if check_a else "FAIL"
    print(f"  [{'PASS' if check_a else 'FAIL'}] Check A: .dbn.zst cache files exist")
    if trades_cache:
        print(f"         trades: {trades_cache[0]}")
    if mbp10_cache:
        print(f"         mbp-10: {mbp10_cache[0]}")
    if check_a:
        checks_passed += 1

    # Check B: on_state_change callback fired
    check_b = len(state_changes) > 0
    print(f"  [{'PASS' if check_b else 'FAIL'}] Check B: on_state_change fired ({len(state_changes)} transitions)")
    for sc in state_changes[:5]:
        print(f"         {sc.timestamp.strftime('%H:%M:%S')} UTC  {sc.momentum_state.value}  score={sc.momentum_score:.1f}")
    if len(state_changes) > 5:
        print(f"         ... and {len(state_changes) - 5} more")
    if check_b:
        checks_passed += 1

    # Check C: Enriched JSONL has records with momentum fields + price context
    enriched_count = 0
    enriched_with_price = 0
    if enriched_path.exists():
        with open(enriched_path) as f:
            for line in f:
                if line.strip():
                    enriched_count += 1
                    rec = json.loads(line)
                    if rec.get("last_trade_price", 0) > 0 and rec.get("momentum_score") is not None:
                        enriched_with_price += 1
    check_c = enriched_count > 0 and enriched_with_price > 0
    print(f"  [{'PASS' if check_c else 'FAIL'}] Check C: Enriched JSONL ({enriched_count} records, {enriched_with_price} with price context)")
    if check_c:
        checks_passed += 1

    # Check D: get_snapshot returns valid MomentumSnapshot
    snap = engine.get_snapshot(symbol)
    check_d = (
        snap is not None
        and 0 <= snap.momentum_score <= 100
        and 0 <= snap.confidence <= 1.0
        and isinstance(snap.momentum_state, MomentumState)
    )
    print(f"  [{'PASS' if check_d else 'FAIL'}] Check D: get_snapshot('{symbol}') returns valid snapshot")
    if snap:
        print(f"         score={snap.momentum_score:.1f}  state={snap.momentum_state.value}  confidence={snap.confidence:.3f}")
    if check_d:
        checks_passed += 1

    # ── Summary ───────────────────────────────────────────────────────
    all_passed = checks_passed == total_checks
    print(f"\n{'-'*60}")
    print(f"  RESULT: {checks_passed}/{total_checks} checks passed {'ALL PASS' if all_passed else 'SOME FAILED'}")
    print(f"{'-'*60}\n")

    if not all_passed:
        print("Fix failing checks before running playbook extraction.")
        sys.exit(1)

    # ── Playbook extraction + report ──────────────────────────────────
    if args.skip_report:
        print("Skipping playbook report (--skip-report flag).")
        sys.exit(0)

    print("Running playbook extraction...\n")

    from morpheus.analysis.ignition_playbook import extract_playbook
    from morpheus.analysis.playbook_report import generate_report

    try:
        playbook = extract_playbook(
            enriched_jsonl_path=str(enriched_path),
            symbol=symbol,
            date_str=date_str,
            window="07:00-09:30 ET",
        )
    except Exception as e:
        print(f"Playbook extraction failed: {e}")
        logger.exception("Playbook extraction failed")
        sys.exit(1)

    print(f"  Simulated trades: {len(playbook.simulated_trades)}")
    print(f"  Win/Loss/Incomplete: {playbook.win_count}W / {playbook.loss_count}L / {playbook.incomplete_count}I")
    if playbook.win_rate is not None:
        print(f"  Win rate: {playbook.win_rate:.0%}")
    print(f"  Thresholds: {len(playbook.thresholds)}")

    reports_dir = f"{DATA_ROOT}/reports"
    try:
        md_path, json_path = generate_report(playbook, reports_dir)
    except Exception as e:
        print(f"\nReport generation failed: {e}")
        logger.exception("Report generation failed")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  PLAYBOOK GENERATED SUCCESSFULLY")
    print(f"{'='*60}")
    print(f"  Markdown: {md_path}")
    print(f"  JSON:     {json_path}")
    print(f"  Enriched: {enriched_path}")
    print()

    # Print threshold summary
    if playbook.thresholds:
        print("  Recommended IGNITION Gate Thresholds:")
        for th in playbook.thresholds:
            print(f"    {th.parameter}: {th.value:.4f}  ({th.basis}, {th.confidence_level})")
    print()


if __name__ == "__main__":
    main()
