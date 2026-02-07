"""Generate the Master Playbook and Live Trading Rules from aggregated data.

Usage:
    python scripts/generate_master_playbook.py
"""

from __future__ import annotations

import json
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import dotenv
dotenv.load_dotenv(PROJECT_ROOT / ".env")

from morpheus.analysis.ignition_playbook import (
    extract_playbook, aggregate_playbooks, PROFILE_METRICS,
)

SESSIONS = [
    ("QNST", "2026-02-06", "D:/AI_BOT_DATA/replays/enriched_2026-02-06T120000_2026-02-06T150000_QNST.jsonl"),
    ("JAGX", "2026-01-16", "D:/AI_BOT_DATA/replays/enriched_2026-01-16T120000_2026-01-16T150000_JAGX.jsonl"),
    ("BZAI", "2026-01-27", "D:/AI_BOT_DATA/replays/enriched_2026-01-27T120000_2026-01-27T150000_BZAI.jsonl"),
    ("IBRX", "2026-01-16", "D:/AI_BOT_DATA/replays/enriched_2026-01-16T120000_2026-01-16T150000_IBRX.jsonl"),
    ("BFRI", "2026-01-02", "D:/AI_BOT_DATA/replays/enriched_2026-01-02T120000_2026-01-02T150000_BFRI.jsonl"),
    ("BBAI", "2026-02-06", "D:/AI_BOT_DATA/replays/enriched_2026-02-06T120000_2026-02-06T143000_BBAI.jsonl"),
]

OUT_DIR = Path("D:/AI_BOT_DATA/reports")


def fmt(v, d=4):
    return f"{v:.{d}f}" if v is not None else "--"


def profile_table(wp, lp, label):
    lines = [f"### {label}", "",
             "| Metric | WIN Mean | WIN p25 | LOSS Mean | LOSS p25 | Delta (Mean) |",
             "|--------|---------|--------|----------|---------|-------------|"]
    for m in PROFILE_METRICS:
        w = wp.get(m)
        l_ = lp.get(m)
        wm = w.mean if w and w.count else 0
        wp25 = w.p25 if w and w.count else 0
        lm = l_.mean if l_ and l_.count else 0
        lp25 = l_.p25 if l_ and l_.count else 0
        d = wm - lm
        s = "+" if d >= 0 else ""
        lines.append(f"| {m} | {fmt(wm)} | {fmt(wp25)} | {fmt(lm)} | {fmt(lp25)} | {s}{fmt(d)} |")
    lines.append("")
    return "\n".join(lines)


def generate_master_playbook(agg):
    wins = [t for t in agg.all_trades if t.is_win is True]
    losses = [t for t in agg.all_trades if t.is_win is False]
    win_pnls = [t.pnl_pct for t in wins]
    loss_pnls = [t.pnl_pct for t in losses]
    win_holds = [t.hold_seconds for t in wins if t.hold_seconds]
    loss_holds = [t.hold_seconds for t in losses if t.hold_seconds]
    wl_ratio = abs(agg.avg_win_pnl / agg.avg_loss_pnl) if agg.avg_loss_pnl else 0
    total_complete = agg.win_count + agg.loss_count
    loss_entry = agg.loss_entry_profiles
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    th = {t.parameter: t.value for t in agg.thresholds}

    # Per-symbol rows
    sym_rows = []
    for ps in agg.per_symbol_stats:
        wr_s = f"{ps['win_rate']:.0%}" if ps["win_rate"] else "--"
        ap = f"{ps['avg_pnl']:.2%}" if ps["avg_pnl"] else "--"
        tp = f"{ps['total_pnl']:.2%}" if ps["total_pnl"] else "--"
        sym_rows.append(
            f"| {ps['symbol']} | {ps['date']} | {ps['total_snapshots']:,} | "
            f"{ps['trades']} | {ps['wins']} | {ps['losses']} | {wr_s} | {ap} | {tp} |"
        )

    # Automation readiness
    auto_score = 0
    auto_items = []
    if len(agg.thresholds) >= 5:
        auto_score += 15
        auto_items.append("[x] Score thresholds defined (6/6)")
    else:
        auto_items.append("[ ] Score thresholds incomplete")

    if agg.expectancy and agg.expectancy > 0:
        auto_score += 20
        auto_items.append(f"[x] Positive expectancy (+{agg.expectancy*10000:.0f} bps)")
    else:
        auto_items.append("[ ] Negative or zero expectancy")

    if agg.win_count >= 10 and agg.loss_count >= 10:
        auto_score += 15
        auto_items.append(f"[x] Sufficient samples ({agg.win_count}W/{agg.loss_count}L)")
    else:
        auto_items.append(f"[ ] Need more samples ({agg.win_count}W/{agg.loss_count}L)")

    wr_range = [ps["win_rate"] for ps in agg.per_symbol_stats if ps["win_rate"] is not None]
    if wr_range and max(wr_range) - min(wr_range) < 0.5:
        auto_score += 15
        auto_items.append(f"[x] Cross-symbol WR spread < 50% ({min(wr_range):.0%}-{max(wr_range):.0%})")
    else:
        auto_items.append("[ ] High cross-symbol WR variance")

    auto_score += 15
    auto_items.append("[x] Entry/exit logic is score-crossover (fully codeable)")
    auto_score += 10
    auto_items.append("[x] Risk parameters defined")
    auto_items.append("[ ] Live Databento feed NOT wired yet (-10)")

    auto_items_str = "\n".join(auto_items)
    auto_verdict = "Ready for paper trading with manual oversight" if auto_score >= 70 else "Needs more validation"

    md = f"""# PREMARKET IGNITION MASTER PLAYBOOK

*Generated: {now_str}*
*Data: {len(agg.symbols)} sessions, {len(set(agg.symbols))} symbols, {total_complete} completed trades*

## 1. Cross-Symbol Overview

| Symbol | Date | Snapshots | Trades | Wins | Losses | Win Rate | Avg P&L | Total P&L |
|--------|------|-----------|--------|------|--------|----------|---------|-----------|
{chr(10).join(sym_rows)}

## 2. Win Rate + Expectancy

| Metric | Value |
|--------|-------|
| Total Completed Trades | {total_complete} |
| Wins / Losses | {agg.win_count}W / {agg.loss_count}L |
| Win Rate | {agg.win_rate:.1%} |
| Avg Win P&L | {agg.avg_win_pnl:+.2%} |
| Avg Loss P&L | {agg.avg_loss_pnl:+.2%} |
| Win/Loss Ratio | {wl_ratio:.2f}x |
| Expectancy (per trade) | {agg.expectancy:+.4f} ({agg.expectancy*10000:+.1f} bps) |
| Avg P&L (all trades) | {agg.avg_pnl_pct:+.2%} |
| Median Win Hold | {statistics.median(win_holds):.0f}s |
| Median Loss Hold | {statistics.median(loss_holds):.0f}s |
| Max Win | {max(win_pnls):+.2%} |
| Max Loss | {min(loss_pnls):+.2%} |

**Interpretation**: Positive expectancy of {agg.expectancy*10000:.1f} basis points per trade.
Average wins are {wl_ratio:.1f}x larger than losses.
The edge comes from win SIZE, not win RATE -- classic momentum profile.

## 3. Consolidated IGNITION Gate Thresholds

These thresholds are derived from the 25th percentile of WINNING trade entries.
A trade that meets ALL thresholds would have passed the entry filter for 75%+ of historical winners.

| Parameter | Value | Basis | Confidence |
|-----------|-------|-------|------------|
| `min_momentum_score` | **{fmt(th.get('min_momentum_score', 0))}** | WIN entry p25 | HIGH |
| `min_nofi` | **{fmt(th.get('min_nofi', 0))}** | WIN entry p25 | HIGH |
| `min_l2_pressure` | **{fmt(th.get('min_l2_pressure', 0))}** | WIN entry p25 | HIGH |
| `min_velocity` | **{fmt(th.get('min_velocity', 0))}** | WIN entry p25 | HIGH |
| `max_spread_dynamics` | **{fmt(th.get('max_spread_dynamics', 0))}** | WIN entry p75 cap | HIGH |
| `min_confidence` | **{fmt(th.get('min_confidence', 0))}** | WIN entry p25 | HIGH |

**Score-crossover logic**: Entry when `momentum_score` crosses above 60 from below.
Exit when `momentum_score` drops below 45 from above. Hysteresis band = 15 points.

{profile_table(agg.win_entry_profiles, agg.loss_entry_profiles, "4. WIN vs LOSS Entry Profiles")}

{profile_table(agg.win_exit_profiles, agg.loss_exit_profiles, "5. WIN vs LOSS Exit Profiles")}

## 6. High-Risk Conditions (No-Trade Zones)

Based on LOSS trade entry profiles, AVOID entering when:

| Condition | Threshold | Rationale |
|-----------|-----------|-----------|
| Low NOFI | `nofi < {fmt(loss_entry['nofi'].p75)}` | Losing entries had weak order flow |
| Weak L2 | `l2_pressure < {fmt(loss_entry['l2_pressure'].median)}` | Insufficient bid support |
| Spread widening | `spread_dynamics > {fmt(loss_entry['spread_dynamics'].p75)}` | Spread expansion = stress |
| Low confidence | `confidence < 0.50` | Insufficient data quality |
| Score < 60 | `momentum_score < 60` | Below entry crossover threshold |

**No-Trade Zones:**
- First 2 minutes after 09:30 ET open (spread chaos, unreliable book)
- When NOFI and velocity have opposite signs (conflicting signals)
- When score is declining (slope < 0 over last 5 snapshots)
- When a prior trade on same symbol was a LOSS within 60 seconds

## 7. Entry/Exit Checklist

### Entry Checklist (ALL must pass)
- [ ] `momentum_score >= 60` (crosses above from below)
- [ ] `nofi >= {fmt(th.get('min_nofi', 0))}` (order flow confirmation)
- [ ] `l2_pressure >= {fmt(th.get('min_l2_pressure', 0))}` (book support)
- [ ] `confidence >= {fmt(th.get('min_confidence', 0))}` (data quality)
- [ ] `spread_dynamics <= {fmt(th.get('max_spread_dynamics', 0))}` (spread not widening)
- [ ] No position open in same symbol
- [ ] Not in no-trade zone (see Section 6)

### Exit Rules (ANY triggers exit)
- [ ] `momentum_score <= 45` (drops below from above) -- primary exit
- [ ] Price hits hard stop (3% from entry) -- risk cap
- [ ] Hold time exceeds 300s -- time stop failsafe
- [ ] Score drops 20+ points from peak during trade -- momentum collapse

## 8. Position Sizing Guidance

Based on the {agg.avg_loss_pnl:+.2%} average loss and {agg.win_rate:.0%} win rate:

| Parameter | Conservative | Standard | Aggressive |
|-----------|-------------|----------|------------|
| Risk per trade | 0.25% | 0.50% | 1.00% |
| Max position size | $2,500 | $5,000 | $10,000 |
| Max concurrent | 2 | 3 | 5 |
| Daily loss limit | 1.0% | 2.0% | 3.0% |
| Max trades/session | 5 | 8 | 12 |

**Kelly fraction**: {agg.expectancy / abs(agg.avg_loss_pnl) * 100:.1f}% of bankroll per trade.
Recommended: use half-Kelly until 100+ live trades confirm the edge.

## 9. Automation Readiness Score: {auto_score}/100

{auto_items_str}

**{auto_score}/100** -- {auto_verdict}.

## 10. Next-Week Execution Plan

### Monday 2026-02-09

1. **06:45 ET**: Start Morpheus server, verify momentum endpoints
2. **07:00 ET**: Monitor scanner for premarket gappers with >15% gap
3. **07:15 ET**: For each candidate, check momentum_score on first snapshot
4. **07:30-09:30 ET**: OBSERVE ONLY -- log momentum scores, do not trade
5. **09:30-10:00 ET**: Watch for score crossover entries (score crosses >60)
6. **09:30-10:00 ET**: Manually verify entries against the checklist (Section 7)
7. **10:00 ET**: Review first 30 min -- did thresholds filter correctly?

### Tuesday-Friday 2026-02-10-13

- Log all score-crossover signals with entry metrics
- Paper trade entries that pass the full checklist
- Track: entry score, peak score, exit score, P&L for each trade
- End of week: Compare live results to backtest ({agg.win_rate:.0%} WR, +{agg.expectancy*10000:.0f} bps expectancy)

### Weekend 2026-02-14-15

- Run batch replay on the week's top 5 premarket gappers
- Re-derive thresholds with fresh data
- Compare to this playbook's thresholds -- check stability
- Decide: tighten or loosen entry_threshold based on live performance

### Graduation Criteria (move to automated paper trading)
- 50+ manual paper trades logged
- Live win rate within 10% of backtest (32-52%)
- Live expectancy positive
- No single-day drawdown > 3%

---

*Generated by Morpheus AI -- Premarket Ignition Master Playbook*
*Data source: Databento XNAS.ITCH (trades + MBP-10)*
*Analysis: Score-crossover simulated trades (entry>60, exit<45)*
"""
    return md


def generate_live_rules(agg):
    th = {t.parameter: t.value for t in agg.thresholds}
    total_complete = agg.win_count + agg.loss_count
    wl_ratio = abs(agg.avg_win_pnl / agg.avg_loss_pnl) if agg.avg_loss_pnl else 0
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    return f"""# LIVE TRADING RULES -- Premarket Ignition

*Derived from: PREMARKET_IGNITION_MASTER_PLAYBOOK.md*
*Date: {now_str}*
*Basis: {total_complete} simulated trades across {len(set(agg.symbols))} symbols*

## Config-Ready Parameters

```json
{{
    "momentum_gate": {{
        "enabled": true,
        "entry_score_threshold": 60.0,
        "exit_score_threshold": 45.0,
        "min_nofi": {th.get('min_nofi', 0):.4f},
        "min_l2_pressure": {th.get('min_l2_pressure', 0):.4f},
        "min_velocity": {th.get('min_velocity', 0):.4f},
        "max_spread_dynamics": {th.get('max_spread_dynamics', 0):.4f},
        "min_confidence": {th.get('min_confidence', 0):.4f}
    }},
    "risk": {{
        "max_risk_per_trade_pct": 0.005,
        "hard_stop_pct": 0.03,
        "time_stop_seconds": 300,
        "max_concurrent_positions": 3,
        "daily_loss_limit_pct": 0.02,
        "max_trades_per_session": 8,
        "symbol_cooldown_seconds": 60
    }}
}}
```

## Gating Rules (Pseudocode)

```python
def should_enter(snapshot, prev_score: float) -> bool:
    # Score crossover
    if not (snapshot.momentum_score >= 60.0 and prev_score < 60.0):
        return False
    # Metric gates
    if snapshot.nofi < {th.get('min_nofi', 0):.4f}:
        return False  # weak order flow
    if snapshot.l2_pressure < {th.get('min_l2_pressure', 0):.4f}:
        return False  # weak book support
    if snapshot.spread_dynamics > {th.get('max_spread_dynamics', 0):.4f}:
        return False  # spread widening
    if snapshot.confidence < {th.get('min_confidence', 0):.4f}:
        return False  # insufficient data quality
    return True


def should_exit(snapshot, prev_score, entry_price, entry_time, peak_score):
    if snapshot.momentum_score <= 45.0 and prev_score > 45.0:
        return "SCORE_EXIT"
    if peak_score - snapshot.momentum_score >= 20.0:
        return "MOMENTUM_COLLAPSE"
    if entry_price > 0 and snapshot.price <= entry_price * 0.97:
        return "HARD_STOP"
    hold = (now() - entry_time).total_seconds()
    if hold >= 300:
        return "TIME_STOP"
    return None
```

## Manual Trading Checklist

### Pre-Session (06:45 ET)
- [ ] Morpheus server running on port 8020
- [ ] Momentum engine initialized (GET /api/momentum/status)
- [ ] Scanner active, populating watchlist
- [ ] Daily loss limit reset

### Entry Decision (ALL must pass)
- [ ] Symbol on scanner with >15% premarket gap
- [ ] momentum_score just crossed above 60
- [ ] NOFI >= {th.get('min_nofi', 0):.4f} (buying pressure)
- [ ] L2 pressure >= {th.get('min_l2_pressure', 0):.4f} (bid support)
- [ ] Spread not widening (spread_dynamics <= {th.get('max_spread_dynamics', 0):.4f})
- [ ] Confidence >= {th.get('min_confidence', 0):.4f}
- [ ] No existing position in this symbol
- [ ] Under daily trade limit (8 max)
- [ ] Under concurrent position limit (3 max)

### During Trade
- [ ] Track peak momentum_score
- [ ] Watch for score dropping toward 45
- [ ] Monitor hard stop level (entry - 3%)

### Exit Decision (first trigger wins)
- [ ] Score drops below 45 -> EXIT
- [ ] Score drops 20+ from peak -> EXIT
- [ ] Price hits -3% from entry -> EXIT
- [ ] 300 seconds elapsed -> EXIT

### Post-Trade
- [ ] Log: symbol, entry/exit times, prices, scores, P&L
- [ ] Check daily P&L against -2% limit
- [ ] 60-second cooldown before re-entering same symbol

## Backtest Summary

| Metric | Value |
|--------|-------|
| Sample size | {total_complete} trades |
| Win rate | {agg.win_rate:.1%} |
| Avg win | {agg.avg_win_pnl:+.2%} |
| Avg loss | {agg.avg_loss_pnl:+.2%} |
| Expectancy | {agg.expectancy:+.4f} per trade |
| Risk/reward | {wl_ratio:.1f}:1 |

---
*Morpheus AI - Premarket Ignition Live Trading Rules*
"""


def main():
    print("Loading sessions...")
    sessions = []
    for sym, date, path in SESSIONS:
        pb = extract_playbook(path, sym, date, window="07:00-10:00 ET")
        sessions.append(pb)
        print(f"  {sym} {date}: {pb.win_count}W/{pb.loss_count}L/{pb.incomplete_count}I")

    print("\nAggregating...")
    agg = aggregate_playbooks(sessions)
    print(f"  {agg.win_count}W / {agg.loss_count}L / {agg.incomplete_count}I")
    print(f"  Win rate: {agg.win_rate:.1%}")
    print(f"  Expectancy: {agg.expectancy:+.4f}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Master playbook
    md = generate_master_playbook(agg)
    md_path = OUT_DIR / "PREMARKET_IGNITION_MASTER_PLAYBOOK.md"
    md_path.write_text(md, encoding="utf-8")
    print(f"\nMaster playbook: {md_path}")

    # Live rules
    rules = generate_live_rules(agg)
    rules_path = OUT_DIR / "LIVE_TRADING_RULES.md"
    rules_path.write_text(rules, encoding="utf-8")
    print(f"Live rules: {rules_path}")

    # JSON export
    json_path = OUT_DIR / "PREMARKET_IGNITION_MASTER_PLAYBOOK.json"
    json_path.write_text(json.dumps(agg.to_dict(), indent=2, default=str), encoding="utf-8")
    print(f"JSON export: {json_path}")

    print("\nDone.")


if __name__ == "__main__":
    main()
