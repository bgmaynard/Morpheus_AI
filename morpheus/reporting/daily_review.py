"""
Daily Review Generator - EOD validation summary for shadow/micro mode.

Reads from DecisionLogger ledgers and LiveValidationLogger JSONL to produce
a daily review markdown file comparing live performance to playbook baseline.

Output: D:/AI_BOT_DATA/reports/LIVE_VALIDATION_SUMMARY_{date}.md
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DATA_ROOT = Path(os.environ.get("MORPHEUS_DATA_ROOT", "D:/AI_BOT_DATA"))

# Playbook baseline (from 6-symbol replay study)
PLAYBOOK_BASELINE = {
    "win_rate": 0.423,         # 42.3%
    "expectancy_bps": 34,      # +34 bps
    "avg_win_pct": 0.0217,     # +2.17%
    "avg_loss_pct": -0.0100,   # -1.00%
    "win_loss_ratio": 2.2,
}


def _read_jsonl(path: Path) -> list[dict]:
    """Read a JSONL file and return list of dicts."""
    if not path.exists():
        return []
    records = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    except Exception as e:
        logger.error(f"[DAILY_REVIEW] Error reading {path}: {e}")
    return records


class DailyReviewGenerator:
    """Generates daily validation summary from ledger data."""

    def __init__(
        self,
        reports_dir: Path | None = None,
        data_root: Path | None = None,
    ) -> None:
        self._reports_dir = reports_dir or Path("reports")
        self._data_root = data_root or DATA_ROOT

    def generate(self, target_date: date | None = None) -> dict[str, Any]:
        """
        Generate daily review for a given date.

        Returns dict with all computed metrics (also writes markdown file).
        """
        target_date = target_date or date.today()
        date_str = target_date.isoformat()

        # Read data sources
        day_dir = self._reports_dir / date_str
        signals = _read_jsonl(day_dir / "signal_ledger.jsonl")
        trades = _read_jsonl(day_dir / "trade_ledger.jsonl")
        blocks = _read_jsonl(day_dir / "gating_blocks.jsonl")

        # Read live validation log for the month
        month_str = target_date.strftime("%Y-%m")
        validation_path = self._data_root / "logs" / f"LIVE_VALIDATION_{month_str}.jsonl"
        validation_records = _read_jsonl(validation_path)
        # Filter to target date
        validation_today = [
            r for r in validation_records
            if r.get("timestamp", "").startswith(date_str)
        ]

        # Compute sections
        result = {
            "date": date_str,
            "signals": self._signals_section(signals),
            "trades": self._trades_section(trades),
            "skipped": self._skipped_section(blocks, validation_today),
            "compliance": self._compliance_section(validation_today, signals),
            "delta": self._delta_section(trades),
            "adjustments": self._adjustments_section(validation_today, trades),
        }

        # Write markdown report
        output_path = self._data_root / "reports" / f"LIVE_VALIDATION_SUMMARY_{date_str}.md"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        self._write_markdown(result, output_path)
        result["output_path"] = str(output_path)

        logger.info(f"[DAILY_REVIEW] Generated for {date_str} -> {output_path}")
        return result

    def _signals_section(self, signals: list[dict]) -> dict:
        """Section 1: Signals Seen."""
        candidates = [s for s in signals if "approved" in s]
        by_strategy: dict[str, int] = {}
        for s in candidates:
            strat = s.get("strategy", "unknown")
            by_strategy[strat] = by_strategy.get(strat, 0) + 1

        return {
            "total": len(candidates),
            "approved": len([s for s in candidates if s.get("approved")]),
            "rejected": len([s for s in candidates if not s.get("approved") and not s.get("observed")]),
            "observed": len([s for s in candidates if s.get("observed")]),
            "by_strategy": by_strategy,
        }

    def _trades_section(self, trades: list[dict]) -> dict:
        """Section 2: Trades Taken."""
        closed = [t for t in trades if t.get("exit_reason")]
        wins = [t for t in closed if float(t.get("pnl", 0)) > 0]
        losses = [t for t in closed if float(t.get("pnl", 0)) <= 0]

        gross_pnl = sum(float(t.get("pnl", 0)) for t in closed)
        win_pnls = [float(t.get("pnl", 0)) for t in wins]
        loss_pnls = [float(t.get("pnl", 0)) for t in losses]

        avg_win = sum(win_pnls) / len(win_pnls) if win_pnls else 0
        avg_loss = sum(loss_pnls) / len(loss_pnls) if loss_pnls else 0
        win_rate = len(wins) / len(closed) if closed else 0

        # Exit reason breakdown
        exit_reasons: dict[str, int] = {}
        for t in closed:
            reason = t.get("exit_reason", "unknown")
            exit_reasons[reason] = exit_reasons.get(reason, 0) + 1

        return {
            "total": len(closed),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": win_rate,
            "gross_pnl": gross_pnl,
            "avg_win": avg_win,
            "avg_loss": avg_loss,
            "exit_reasons": exit_reasons,
        }

    def _skipped_section(self, blocks: list[dict], validation: list[dict]) -> dict:
        """Section 3: Trades Skipped."""
        # From gating blocks
        block_reasons: dict[str, int] = {}
        for b in blocks:
            for reason in b.get("block_reasons", []):
                block_reasons[reason] = block_reasons.get(reason, 0) + 1

        # From validation log (BLOCKED events)
        blocked_events = [v for v in validation if v.get("status") == "BLOCKED"]
        for b in blocked_events:
            for reason in b.get("block_reasons", []):
                key = f"VALIDATION_{reason}"
                block_reasons[key] = block_reasons.get(key, 0) + 1

        return {
            "total_blocks": len(blocks) + len(blocked_events),
            "reasons": block_reasons,
        }

    def _compliance_section(self, validation: list[dict], signals: list[dict]) -> dict:
        """Section 4: Rule Compliance %."""
        gate_passed = len([v for v in validation if v.get("status") == "GATE_PASSED"])
        gate_rejected = len([v for v in validation if v.get("status") == "GATE_REJECTED"])
        gate_total = gate_passed + gate_rejected
        gate_pass_rate = gate_passed / gate_total if gate_total > 0 else 1.0

        # Session cap compliance (no BLOCKED with SESSION_CAP reason)
        session_cap_blocks = len([
            v for v in validation
            if v.get("status") == "BLOCKED"
            and any("SESSION_CAP" in r for r in v.get("block_reasons", []))
        ])
        session_cap_compliant = session_cap_blocks == 0

        # No-trade zone compliance (no gate failures for RTH_COOLDOWN)
        cooldown_violations = len([
            v for v in validation
            if v.get("status") == "GATE_REJECTED"
            and any("RTH_COOLDOWN" in f for f in v.get("failures", []))
        ])

        return {
            "ignition_gate_pass_rate": gate_pass_rate,
            "ignition_gate_passed": gate_passed,
            "ignition_gate_rejected": gate_rejected,
            "session_cap_compliant": session_cap_compliant,
            "session_cap_blocks": session_cap_blocks,
            "rth_cooldown_violations": cooldown_violations,
            "overall_score": 1.0 if (session_cap_compliant and cooldown_violations == 0) else 0.9,
        }

    def _delta_section(self, trades: list[dict]) -> dict:
        """Section 5: Live vs Replay Delta."""
        closed = [t for t in trades if t.get("exit_reason")]
        if not closed:
            return {
                "live_win_rate": 0,
                "playbook_win_rate": PLAYBOOK_BASELINE["win_rate"],
                "delta_win_rate": -PLAYBOOK_BASELINE["win_rate"],
                "live_expectancy_bps": 0,
                "playbook_expectancy_bps": PLAYBOOK_BASELINE["expectancy_bps"],
                "delta_expectancy_bps": -PLAYBOOK_BASELINE["expectancy_bps"],
                "sample_size": 0,
                "sufficient_data": False,
            }

        wins = [t for t in closed if float(t.get("pnl", 0)) > 0]
        live_wr = len(wins) / len(closed)

        # Compute live expectancy in basis points
        pnls = [float(t.get("pnl_pct", 0)) for t in closed]
        live_expectancy_bps = (sum(pnls) / len(pnls)) * 10000 if pnls else 0

        return {
            "live_win_rate": live_wr,
            "playbook_win_rate": PLAYBOOK_BASELINE["win_rate"],
            "delta_win_rate": live_wr - PLAYBOOK_BASELINE["win_rate"],
            "live_expectancy_bps": live_expectancy_bps,
            "playbook_expectancy_bps": PLAYBOOK_BASELINE["expectancy_bps"],
            "delta_expectancy_bps": live_expectancy_bps - PLAYBOOK_BASELINE["expectancy_bps"],
            "sample_size": len(closed),
            "sufficient_data": len(closed) >= 5,
        }

    def _adjustments_section(self, validation: list[dict], trades: list[dict]) -> dict:
        """Section 6: Recommended Adjustments."""
        hints: list[str] = []

        # Check for high gate rejection rate
        gate_rejected = [v for v in validation if v.get("status") == "GATE_REJECTED"]
        gate_passed = [v for v in validation if v.get("status") == "GATE_PASSED"]
        if len(gate_rejected) > 3 * max(len(gate_passed), 1):
            # Analyze which checks fail most
            failure_counts: dict[str, int] = {}
            for v in gate_rejected:
                for f in v.get("failures", []):
                    tag = f.split(":")[0]
                    failure_counts[tag] = failure_counts.get(tag, 0) + 1
            top_failures = sorted(failure_counts.items(), key=lambda x: -x[1])[:3]
            hints.append(
                f"High rejection rate ({len(gate_rejected)}/{len(gate_rejected) + len(gate_passed)}). "
                f"Top failures: {top_failures}"
            )

        # Check for low win rate on closed trades
        closed = [t for t in trades if t.get("exit_reason")]
        if len(closed) >= 3:
            wins = len([t for t in closed if float(t.get("pnl", 0)) > 0])
            wr = wins / len(closed)
            if wr < 0.30:
                hints.append(f"Low win rate ({wr:.0%}). Consider tightening entry thresholds.")
            # Check for TIME_STOP dominance
            time_stops = len([t for t in closed if t.get("exit_reason") == "TIME_STOP"])
            if time_stops > len(closed) * 0.6:
                hints.append(
                    f"TIME_STOP dominance ({time_stops}/{len(closed)}). "
                    "Consider extending hold times or adjusting trail activation."
                )

        if not hints:
            hints.append("No adjustments recommended. Continue monitoring.")

        return {"hints": hints}

    def _write_markdown(self, result: dict, path: Path) -> None:
        """Write the review as a markdown file."""
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write(f"# Live Validation Daily Review - {result['date']}\n\n")
                f.write(f"Generated: {datetime.now(timezone.utc).isoformat()}\n\n")

                # Section 1: Signals
                sig = result["signals"]
                f.write("## 1. Signals Seen\n\n")
                f.write(f"| Metric | Value |\n|--------|-------|\n")
                f.write(f"| Total | {sig['total']} |\n")
                f.write(f"| Approved | {sig['approved']} |\n")
                f.write(f"| Rejected | {sig['rejected']} |\n")
                f.write(f"| Observed | {sig['observed']} |\n\n")
                if sig["by_strategy"]:
                    f.write("**By Strategy:**\n")
                    for strat, count in sorted(sig["by_strategy"].items()):
                        f.write(f"- {strat}: {count}\n")
                    f.write("\n")

                # Section 2: Trades
                tr = result["trades"]
                f.write("## 2. Trades Taken\n\n")
                f.write(f"| Metric | Value |\n|--------|-------|\n")
                f.write(f"| Total | {tr['total']} |\n")
                f.write(f"| Wins | {tr['wins']} |\n")
                f.write(f"| Losses | {tr['losses']} |\n")
                f.write(f"| Win Rate | {tr['win_rate']:.1%} |\n")
                f.write(f"| Gross PnL | ${tr['gross_pnl']:.2f} |\n")
                f.write(f"| Avg Win | ${tr['avg_win']:.2f} |\n")
                f.write(f"| Avg Loss | ${tr['avg_loss']:.2f} |\n\n")
                if tr["exit_reasons"]:
                    f.write("**Exit Reasons:**\n")
                    for reason, count in sorted(tr["exit_reasons"].items()):
                        f.write(f"- {reason}: {count}\n")
                    f.write("\n")

                # Section 3: Skipped
                sk = result["skipped"]
                f.write("## 3. Trades Skipped\n\n")
                f.write(f"Total blocks: {sk['total_blocks']}\n\n")
                if sk["reasons"]:
                    f.write("**Block Reasons:**\n")
                    for reason, count in sorted(sk["reasons"].items()):
                        f.write(f"- {reason}: {count}\n")
                    f.write("\n")

                # Section 4: Compliance
                comp = result["compliance"]
                f.write("## 4. Rule Compliance\n\n")
                f.write(f"| Check | Result |\n|-------|--------|\n")
                f.write(f"| Ignition Gate Pass Rate | {comp['ignition_gate_pass_rate']:.0%} ({comp['ignition_gate_passed']}/{comp['ignition_gate_passed'] + comp['ignition_gate_rejected']}) |\n")
                f.write(f"| Session Cap Compliant | {'YES' if comp['session_cap_compliant'] else 'NO'} |\n")
                f.write(f"| RTH Cooldown Violations | {comp['rth_cooldown_violations']} |\n")
                f.write(f"| Overall Score | {comp['overall_score']:.0%} |\n\n")

                # Section 5: Delta
                delta = result["delta"]
                f.write("## 5. Live vs Replay Delta\n\n")
                f.write(f"| Metric | Live | Playbook | Delta |\n|--------|------|----------|-------|\n")
                f.write(f"| Win Rate | {delta['live_win_rate']:.1%} | {delta['playbook_win_rate']:.1%} | {delta['delta_win_rate']:+.1%} |\n")
                f.write(f"| Expectancy (bps) | {delta['live_expectancy_bps']:.0f} | {delta['playbook_expectancy_bps']} | {delta['delta_expectancy_bps']:+.0f} |\n")
                f.write(f"| Sample Size | {delta['sample_size']} | 52 | |\n\n")
                if not delta["sufficient_data"]:
                    f.write("*Insufficient data (need >= 5 trades for meaningful comparison)*\n\n")

                # Section 6: Adjustments
                adj = result["adjustments"]
                f.write("## 6. Recommended Adjustments\n\n")
                for hint in adj["hints"]:
                    f.write(f"- {hint}\n")
                f.write("\n")

        except Exception as e:
            logger.error(f"[DAILY_REVIEW] Error writing markdown: {e}")
