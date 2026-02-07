"""Playbook Report Generator — markdown + JSON from PlaybookResult.

Generates a human-readable markdown report with 9 sections plus a raw JSON
export for programmatic consumption.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

from morpheus.analysis.ignition_playbook import (
    MetricDistribution,
    PlaybookResult,
    STATE_TO_PREMARKET,
    PROFILE_METRICS,
)

logger = logging.getLogger(__name__)


def _fmt(val: float | None, decimals: int = 4) -> str:
    if val is None:
        return "—"
    return f"{val:.{decimals}f}"


def _profile_table(
    win_profiles: dict[str, MetricDistribution],
    loss_profiles: dict[str, MetricDistribution],
    label: str,
) -> str:
    """Build a markdown table comparing WIN vs LOSS metric distributions."""
    lines = [
        f"### {label}",
        "",
        "| Metric | WIN Mean | WIN Median | LOSS Mean | LOSS Median | Delta (Mean) |",
        "|--------|---------|-----------|----------|------------|-------------|",
    ]
    for metric in PROFILE_METRICS:
        w = win_profiles.get(metric)
        l = loss_profiles.get(metric)
        w_mean = w.mean if w and w.count > 0 else 0.0
        w_med = w.median if w and w.count > 0 else 0.0
        l_mean = l.mean if l and l.count > 0 else 0.0
        l_med = l.median if l and l.count > 0 else 0.0
        delta = w_mean - l_mean
        sign = "+" if delta >= 0 else ""
        lines.append(
            f"| {metric} | {_fmt(w_mean)} | {_fmt(w_med)} "
            f"| {_fmt(l_mean)} | {_fmt(l_med)} | {sign}{_fmt(delta)} |"
        )
    lines.append("")
    return "\n".join(lines)


def generate_report(
    result: PlaybookResult,
    output_dir: str,
) -> tuple[str, str]:
    """Generate markdown report and JSON export.

    Args:
        result: PlaybookResult from ignition_playbook.extract_playbook()
        output_dir: Directory to write report files

    Returns:
        Tuple of (markdown_path, json_path)
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    md_name = f"PREMARKET_IGNITION_PLAYBOOK_{result.date}.md"
    json_name = f"PREMARKET_IGNITION_PLAYBOOK_{result.date}.json"
    md_path = out / md_name
    json_path = out / json_name

    sections: list[str] = []

    # ── Section 1: Session Overview ──────────────────────────────────
    wr_str = f"{result.win_rate:.0%}" if result.win_rate is not None else "N/A"
    complete_trades = result.win_count + result.loss_count
    sections.append(f"""# Premarket IGNITION Playbook — {result.symbol} ({result.date})

## 1. Session Overview

| Field | Value |
|-------|-------|
| Symbol | {result.symbol} |
| Date | {result.date} |
| Window | {result.window} |
| Total Snapshots | {result.total_snapshots} |
| Simulated Trades | {len(result.simulated_trades)} ({complete_trades} complete, {result.incomplete_count} incomplete) |
| Win Rate | {wr_str} ({result.win_count}W / {result.loss_count}L) |
""")

    # ── Section 2: Momentum State Distribution ───────────────────────
    state_lines = [
        "## 2. Momentum State Distribution",
        "",
        "| Engine State | Premarket Term | Occurrences | Total Time (s) | Avg Duration (s) |",
        "|-------------|---------------|-------------|----------------|-----------------|",
    ]
    for ss in result.state_stats:
        state_lines.append(
            f"| {ss.state} | {ss.premarket_term} | {ss.count} "
            f"| {ss.total_seconds:.1f} | {ss.avg_seconds:.1f} |"
        )
    state_lines.append("")
    sections.append("\n".join(state_lines))

    # ── Section 3: State Transition Matrix ───────────────────────────
    all_states = sorted(
        set(list(result.transition_matrix.keys()) +
            [s for row in result.transition_matrix.values() for s in row.keys()])
    )
    matrix_lines = [
        "## 3. State Transition Matrix",
        "",
        "Rows = FROM, Columns = TO. Counts of observed transitions.",
        "",
        "| FROM \\ TO | " + " | ".join(all_states) + " |",
        "|--------" + "|------" * len(all_states) + "|",
    ]
    for from_state in all_states:
        row = result.transition_matrix.get(from_state, {})
        cells = [str(row.get(to_state, 0)) for to_state in all_states]
        matrix_lines.append(f"| {from_state} | " + " | ".join(cells) + " |")
    matrix_lines.append("")
    sections.append("\n".join(matrix_lines))

    # ── Section 4: WIN vs LOSS Entry Profiles ────────────────────────
    sections.append(
        _profile_table(
            result.win_entry_profiles,
            result.loss_entry_profiles,
            "4. WIN vs LOSS Entry Profiles",
        )
    )

    # ── Section 5: WIN vs LOSS Exit Profiles ─────────────────────────
    sections.append(
        _profile_table(
            result.win_exit_profiles,
            result.loss_exit_profiles,
            "5. WIN vs LOSS Exit Profiles",
        )
    )

    # ── Section 6: Simulated Trade Log ───────────────────────────────
    trade_lines = [
        "## 6. Simulated Trade Log",
        "",
        "| # | Entry Time | Exit Time | Entry $ | Exit $ | P&L % | Hold (s) | Entry State | Exit State | Result |",
        "|---|-----------|----------|---------|--------|-------|---------|------------|-----------|--------|",
    ]
    for i, t in enumerate(result.simulated_trades, 1):
        result_str = "WIN" if t.is_win is True else ("LOSS" if t.is_win is False else "INCOMPLETE")
        entry_t = t.entry_time.split("T")[1][:8] if "T" in t.entry_time else t.entry_time
        exit_t = (t.exit_time.split("T")[1][:8] if t.exit_time and "T" in t.exit_time else (t.exit_time or "—"))
        pnl_str = f"{t.pnl_pct:+.2%}" if t.pnl_pct is not None else "—"
        hold_str = f"{t.hold_seconds:.0f}" if t.hold_seconds is not None else "—"
        ep = STATE_TO_PREMARKET.get(t.entry_state, t.entry_state)
        xp = STATE_TO_PREMARKET.get(t.exit_state, t.exit_state) if t.exit_state else "—"
        trade_lines.append(
            f"| {i} | {entry_t} | {exit_t} | {t.entry_price:.3f} "
            f"| {_fmt(t.exit_price, 3)} | {pnl_str} | {hold_str} "
            f"| {ep} | {xp} | {result_str} |"
        )
    trade_lines.append("")
    sections.append("\n".join(trade_lines))

    # ── Section 7: Recommended IGNITION Gate Thresholds ──────────────
    threshold_lines = [
        "## 7. Recommended IGNITION Gate Thresholds",
        "",
    ]
    if result.thresholds:
        threshold_lines.extend([
            "| Parameter | Value | Basis | Confidence |",
            "|-----------|-------|-------|------------|",
        ])
        for th in result.thresholds:
            threshold_lines.append(
                f"| `{th.parameter}` | {_fmt(th.value)} | {th.basis} | {th.confidence_level} |"
            )
    else:
        threshold_lines.append("*No thresholds generated — insufficient trade data.*")
    threshold_lines.append("")
    sections.append("\n".join(threshold_lines))

    # ── Section 8: Action Plan ───────────────────────────────────────
    sections.append("""## 8. Action Plan

1. **Wire thresholds into scorer** — Add momentum gate in `stub_scorer.py` using the thresholds above. Block entries when `momentum_score < min_momentum_score` or `confidence < min_confidence`.
2. **Validate on additional symbols** — Run this playbook on 3-5 more premarket movers to check threshold stability across different stocks.
3. **Back-test with full week** — Replay 5 trading days (2026-02-03 through 2026-02-07) to see if thresholds hold across varying market conditions.
4. **Connect live Databento feed** — Wire `DatabentoClient` live stream to `MomentumEngine.ingest_trade/ingest_book` for real-time momentum scoring during premarket.
5. **Monitor IGNITION gate hit rate** — Track how often the gate blocks entries vs allows them. Target: block 30-50% of marginal setups while passing 90%+ of eventual winners.
""")

    # ── Section 9: Data Quality Notes ────────────────────────────────
    dq = result.data_quality
    sections.append(f"""## 9. Data Quality Notes

| Metric | Value |
|--------|-------|
| Total Snapshots | {dq.get('total_snapshots', 0)} |
| Duration (min) | {dq.get('duration_minutes', 0)} |
| Snapshots/min | {dq.get('snapshots_per_min', 0)} |
| Avg Confidence | {dq.get('avg_confidence', 0)} |
| Low-Confidence % | {dq.get('low_confidence_pct', 0)} |
| Has Price Context | {dq.get('has_price_context', 0)} / {dq.get('total_snapshots', 0)} |
| Has Book Mid | {dq.get('has_book_mid', 0)} / {dq.get('total_snapshots', 0)} |
| Warnings | {dq.get('warnings', 'None')} |

---

*Generated by Morpheus AI — Premarket Ignition Playbook Extractor*
*Timestamp: {datetime.utcnow().isoformat()}Z*
""")

    # ── Write files ──────────────────────────────────────────────────
    md_content = "\n".join(sections)
    md_path.write_text(md_content, encoding="utf-8")
    logger.info("Report written: %s", md_path)

    json_path.write_text(
        json.dumps(result.to_dict(), indent=2, default=str),
        encoding="utf-8",
    )
    logger.info("JSON export written: %s", json_path)

    return str(md_path), str(json_path)
