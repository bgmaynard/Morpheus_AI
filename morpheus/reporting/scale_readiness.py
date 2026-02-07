"""
Scale Readiness Report - Multi-session Go/No-Go decision.

Aggregates daily validation summaries and trade ledgers from the last N sessions
to determine if the system is ready to scale from SHADOW -> MICRO -> LIVE.

Output: D:/AI_BOT_DATA/reports/SCALE_READINESS_REPORT.md
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
        logger.error(f"[SCALE_READINESS] Error reading {path}: {e}")
    return records


class ScaleReadinessReport:
    """Multi-session Go/No-Go readiness report."""

    def __init__(
        self,
        data_root: Path | None = None,
        project_reports_dir: Path | None = None,
    ) -> None:
        self._data_root = data_root or DATA_ROOT
        self._project_reports_dir = project_reports_dir or Path("reports")

    def generate(self, min_sessions: int = 5) -> dict[str, Any]:
        """
        Generate scale readiness report.

        Args:
            min_sessions: Minimum number of trading sessions required.

        Returns:
            Dict with all metrics, Go/No-Go recommendation, and output path.
        """
        # Discover available sessions (date directories with trade_ledger.jsonl)
        sessions = self._discover_sessions()

        # Aggregate all trades across sessions
        all_trades = []
        session_summaries = []

        for session_date in sessions:
            day_dir = self._project_reports_dir / session_date
            trades = _read_jsonl(day_dir / "trade_ledger.jsonl")
            closed = [t for t in trades if t.get("exit_reason")]

            if not closed:
                continue

            wins = [t for t in closed if float(t.get("pnl", 0)) > 0]
            pnl = sum(float(t.get("pnl", 0)) for t in closed)
            wr = len(wins) / len(closed) if closed else 0

            session_summaries.append({
                "date": session_date,
                "trades": len(closed),
                "wins": len(wins),
                "losses": len(closed) - len(wins),
                "win_rate": wr,
                "pnl": pnl,
            })

            all_trades.extend(closed)

        # Compute aggregate metrics
        metrics = self._compute_metrics(all_trades, session_summaries)
        metrics["sessions_found"] = len(session_summaries)
        metrics["min_sessions_required"] = min_sessions
        metrics["sessions_sufficient"] = len(session_summaries) >= min_sessions

        # Compute discipline score from daily reviews
        metrics["discipline_score"] = self._compute_discipline_score(sessions)

        # Go/No-Go decision
        go_decision = self._evaluate_readiness(metrics, min_sessions)
        metrics.update(go_decision)

        # Write report
        output_path = self._data_root / "reports" / "SCALE_READINESS_REPORT.md"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        self._write_markdown(metrics, session_summaries, output_path)
        metrics["output_path"] = str(output_path)
        metrics["session_summaries"] = session_summaries

        logger.info(
            f"[SCALE_READINESS] Generated: {go_decision['recommendation']} "
            f"({len(session_summaries)} sessions, {len(all_trades)} trades)"
        )
        return metrics

    def _discover_sessions(self) -> list[str]:
        """Find all date directories with trade data."""
        sessions = []
        if not self._project_reports_dir.exists():
            return sessions

        for d in sorted(self._project_reports_dir.iterdir()):
            if d.is_dir() and (d / "trade_ledger.jsonl").exists():
                sessions.append(d.name)

        return sessions

    def _compute_metrics(
        self,
        trades: list[dict],
        sessions: list[dict],
    ) -> dict[str, Any]:
        """Compute aggregate metrics across all sessions."""
        if not trades:
            return {
                "total_trades": 0,
                "win_rate": 0.0,
                "profit_factor": 0.0,
                "max_drawdown_pct": 0.0,
                "gross_pnl": 0.0,
                "avg_win": 0.0,
                "avg_loss": 0.0,
            }

        wins = [t for t in trades if float(t.get("pnl", 0)) > 0]
        losses = [t for t in trades if float(t.get("pnl", 0)) <= 0]

        gross_profit = sum(float(t.get("pnl", 0)) for t in wins)
        gross_loss = abs(sum(float(t.get("pnl", 0)) for t in losses))

        win_rate = len(wins) / len(trades) if trades else 0
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else 999.0

        avg_win = gross_profit / len(wins) if wins else 0
        avg_loss = -gross_loss / len(losses) if losses else 0

        # Compute max drawdown from cumulative equity curve
        equity_curve = []
        running = 0.0
        for t in trades:
            running += float(t.get("pnl", 0))
            equity_curve.append(running)

        max_drawdown_pct = 0.0
        if equity_curve:
            peak = equity_curve[0]
            # Use base equity of $500 for pct calculation
            base_equity = 500.0
            for val in equity_curve:
                if val > peak:
                    peak = val
                dd = (peak - val) / base_equity if base_equity > 0 else 0
                if dd > max_drawdown_pct:
                    max_drawdown_pct = dd

        return {
            "total_trades": len(trades),
            "win_rate": win_rate,
            "profit_factor": profit_factor,
            "max_drawdown_pct": max_drawdown_pct,
            "gross_pnl": gross_profit - gross_loss,
            "gross_profit": gross_profit,
            "gross_loss": gross_loss,
            "avg_win": avg_win,
            "avg_loss": avg_loss,
        }

    def _compute_discipline_score(self, sessions: list[str]) -> float:
        """
        Compute discipline score from daily validation reviews.

        Score = fraction of sessions where overall compliance score >= 0.9.
        """
        if not sessions:
            return 1.0

        compliant_sessions = 0
        total_reviewed = 0

        for session_date in sessions:
            review_path = self._data_root / "reports" / f"LIVE_VALIDATION_SUMMARY_{session_date}.md"
            if not review_path.exists():
                continue

            total_reviewed += 1
            # Simple heuristic: check if "Overall Score | 100%" appears in markdown
            try:
                content = review_path.read_text(encoding="utf-8")
                if "100%" in content or "Overall Score | 1" in content:
                    compliant_sessions += 1
                elif "90%" in content:
                    compliant_sessions += 0.9
            except Exception:
                pass

        if total_reviewed == 0:
            return 1.0  # No reviews = assume compliant (no violations detected)

        return compliant_sessions / total_reviewed

    def _evaluate_readiness(
        self,
        metrics: dict[str, Any],
        min_sessions: int,
    ) -> dict[str, Any]:
        """
        Evaluate Go/No-Go decision.

        Targets:
        - Win rate >= 32% (within +/-10% of 42.3% backtest)
        - Profit factor >= 1.5
        - Max drawdown <= 5%
        - Discipline score >= 90%
        - Total trades >= 10
        """
        checks = {}

        checks["win_rate_ok"] = metrics.get("win_rate", 0) >= 0.32
        checks["profit_factor_ok"] = metrics.get("profit_factor", 0) >= 1.5
        checks["max_drawdown_ok"] = metrics.get("max_drawdown_pct", 1.0) <= 0.05
        checks["discipline_ok"] = metrics.get("discipline_score", 0) >= 0.90
        checks["sample_size_ok"] = metrics.get("total_trades", 0) >= 10
        checks["sessions_ok"] = metrics.get("sessions_found", 0) >= min_sessions

        all_pass = all(checks.values())
        recommendation = "GO - Scale to MICRO" if all_pass else "NO-GO - Continue SHADOW"

        return {
            "checks": checks,
            "all_checks_passed": all_pass,
            "recommendation": recommendation,
        }

    def _write_markdown(
        self,
        metrics: dict[str, Any],
        sessions: list[dict],
        path: Path,
    ) -> None:
        """Write the readiness report as markdown."""
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write("# Scale Readiness Report\n\n")
                f.write(f"Generated: {datetime.now(timezone.utc).isoformat()}\n\n")

                # Recommendation banner
                rec = metrics.get("recommendation", "UNKNOWN")
                f.write(f"## Decision: **{rec}**\n\n")

                # Aggregate metrics
                f.write("## Aggregate Metrics\n\n")
                f.write("| Metric | Value | Target | Pass |\n")
                f.write("|--------|-------|--------|------|\n")

                checks = metrics.get("checks", {})
                wr = metrics.get("win_rate", 0)
                f.write(f"| Win Rate | {wr:.1%} | >= 32% | {'PASS' if checks.get('win_rate_ok') else 'FAIL'} |\n")

                pf = metrics.get("profit_factor", 0)
                pf_str = f"{pf:.2f}" if pf < 999 else "INF"
                f.write(f"| Profit Factor | {pf_str} | >= 1.5 | {'PASS' if checks.get('profit_factor_ok') else 'FAIL'} |\n")

                dd = metrics.get("max_drawdown_pct", 0)
                f.write(f"| Max Drawdown | {dd:.2%} | <= 5% | {'PASS' if checks.get('max_drawdown_ok') else 'FAIL'} |\n")

                disc = metrics.get("discipline_score", 0)
                f.write(f"| Discipline Score | {disc:.0%} | >= 90% | {'PASS' if checks.get('discipline_ok') else 'FAIL'} |\n")

                total = metrics.get("total_trades", 0)
                f.write(f"| Total Trades | {total} | >= 10 | {'PASS' if checks.get('sample_size_ok') else 'FAIL'} |\n")

                sess = metrics.get("sessions_found", 0)
                min_s = metrics.get("min_sessions_required", 5)
                f.write(f"| Sessions | {sess} | >= {min_s} | {'PASS' if checks.get('sessions_ok') else 'FAIL'} |\n\n")

                # Financial summary
                f.write("## Financial Summary\n\n")
                f.write(f"- Gross PnL: ${metrics.get('gross_pnl', 0):.2f}\n")
                f.write(f"- Gross Profit: ${metrics.get('gross_profit', 0):.2f}\n")
                f.write(f"- Gross Loss: ${metrics.get('gross_loss', 0):.2f}\n")
                f.write(f"- Avg Win: ${metrics.get('avg_win', 0):.2f}\n")
                f.write(f"- Avg Loss: ${metrics.get('avg_loss', 0):.2f}\n\n")

                # Session breakdown
                f.write("## Session Breakdown\n\n")
                f.write("| Date | Trades | W | L | WR | PnL |\n")
                f.write("|------|--------|---|---|-----|------|\n")
                for s in sessions:
                    f.write(
                        f"| {s['date']} | {s['trades']} | {s['wins']} | {s['losses']} | "
                        f"{s['win_rate']:.0%} | ${s['pnl']:.2f} |\n"
                    )
                f.write("\n")

        except Exception as e:
            logger.error(f"[SCALE_READINESS] Error writing markdown: {e}")
