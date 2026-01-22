"""
EOD Report Generator - End-of-day analysis and reporting.

Generates comprehensive daily reports for learning and improvement:
- Signal and trade statistics
- Rejection analysis (why signals were blocked)
- P&L breakdown
- Execution quality (slippage analysis)
- Ranked issues for improvement
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, date, timezone, timedelta
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class TradeAnalysis:
    """Analysis of a single trade."""
    trade_id: str
    symbol: str
    direction: str
    trade_type: str  # shadow, probe, full

    entry_price: float
    exit_price: float
    pnl: float
    pnl_percent: float
    hold_time_seconds: int

    # Execution quality
    expected_entry: float | None
    slippage: float | None
    actual_spread: float | None

    exit_reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "trade_id": self.trade_id,
            "symbol": self.symbol,
            "direction": self.direction,
            "trade_type": self.trade_type,
            "entry_price": self.entry_price,
            "exit_price": self.exit_price,
            "pnl": self.pnl,
            "pnl_percent": self.pnl_percent,
            "hold_time_seconds": self.hold_time_seconds,
            "expected_entry": self.expected_entry,
            "slippage": self.slippage,
            "actual_spread": self.actual_spread,
            "exit_reason": self.exit_reason,
        }


@dataclass
class RejectionAnalysis:
    """Analysis of rejection reasons."""
    reason: str
    count: int
    percentage: float
    examples: list[str]  # Example symbols/signals

    def to_dict(self) -> dict[str, Any]:
        return {
            "reason": self.reason,
            "count": self.count,
            "percentage": self.percentage,
            "examples": self.examples[:5],  # Top 5 examples
        }


@dataclass
class EODReport:
    """End-of-day report."""

    report_date: date
    generated_at: datetime

    # Summary stats
    signals_detected: int
    signals_approved: int
    signals_rejected: int
    approval_rate: float

    trades_executed: int
    trades_shadow: int
    trades_won: int
    trades_lost: int
    win_rate: float

    # P&L
    total_pnl: float
    avg_pnl_per_trade: float
    best_trade_pnl: float
    worst_trade_pnl: float
    max_drawdown: float

    # Execution quality
    avg_slippage: float
    avg_spread: float
    avg_hold_time_seconds: float

    # Analysis
    rejection_breakdown: list[RejectionAnalysis]
    trade_details: list[TradeAnalysis]
    top_issues: list[str]

    # Market context
    market_regime_summary: str
    active_symbols: list[str]

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "report_date": self.report_date.isoformat(),
            "generated_at": self.generated_at.isoformat(),
            "summary": {
                "signals_detected": self.signals_detected,
                "signals_approved": self.signals_approved,
                "signals_rejected": self.signals_rejected,
                "approval_rate": self.approval_rate,
                "trades_executed": self.trades_executed,
                "trades_shadow": self.trades_shadow,
                "trades_won": self.trades_won,
                "trades_lost": self.trades_lost,
                "win_rate": self.win_rate,
            },
            "pnl": {
                "total": self.total_pnl,
                "avg_per_trade": self.avg_pnl_per_trade,
                "best": self.best_trade_pnl,
                "worst": self.worst_trade_pnl,
                "max_drawdown": self.max_drawdown,
            },
            "execution_quality": {
                "avg_slippage": self.avg_slippage,
                "avg_spread": self.avg_spread,
                "avg_hold_time_seconds": self.avg_hold_time_seconds,
            },
            "rejection_analysis": [r.to_dict() for r in self.rejection_breakdown],
            "trades": [t.to_dict() for t in self.trade_details],
            "top_issues": self.top_issues,
            "market_context": {
                "regime_summary": self.market_regime_summary,
                "active_symbols": self.active_symbols,
            },
        }

    def to_markdown(self) -> str:
        """Generate markdown report."""
        lines = [
            f"# Morpheus EOD Report - {self.report_date.isoformat()}",
            f"Generated: {self.generated_at.strftime('%Y-%m-%d %H:%M:%S')} UTC",
            "",
            "## Summary",
            "",
            "| Metric | Value |",
            "|--------|-------|",
            f"| Signals Detected | {self.signals_detected} |",
            f"| Signals Approved | {self.signals_approved} ({self.approval_rate:.1f}%) |",
            f"| Signals Rejected | {self.signals_rejected} |",
            f"| Trades Executed | {self.trades_executed} |",
            f"| Shadow Trades | {self.trades_shadow} |",
            f"| Win Rate | {self.win_rate:.1f}% ({self.trades_won}W / {self.trades_lost}L) |",
            "",
            "## P&L",
            "",
            f"- **Total P&L**: ${self.total_pnl:+.2f}",
            f"- **Avg P&L/Trade**: ${self.avg_pnl_per_trade:+.2f}",
            f"- **Best Trade**: ${self.best_trade_pnl:+.2f}",
            f"- **Worst Trade**: ${self.worst_trade_pnl:+.2f}",
            f"- **Max Drawdown**: ${self.max_drawdown:.2f}",
            "",
            "## Execution Quality",
            "",
            f"- **Avg Slippage**: ${self.avg_slippage:.3f}",
            f"- **Avg Spread**: ${self.avg_spread:.3f}",
            f"- **Avg Hold Time**: {self.avg_hold_time_seconds:.0f}s",
            "",
        ]

        if self.rejection_breakdown:
            lines.extend([
                "## Rejection Analysis",
                "",
                "| Reason | Count | % |",
                "|--------|-------|---|",
            ])
            for r in self.rejection_breakdown[:10]:
                lines.append(f"| {r.reason} | {r.count} | {r.percentage:.1f}% |")
            lines.append("")

        if self.top_issues:
            lines.extend([
                "## Top Issues to Address",
                "",
            ])
            for i, issue in enumerate(self.top_issues[:5], 1):
                lines.append(f"{i}. {issue}")
            lines.append("")

        if self.trade_details:
            lines.extend([
                "## Trade Details",
                "",
                "| Symbol | Dir | P&L | % | Hold | Exit Reason |",
                "|--------|-----|-----|---|------|-------------|",
            ])
            for t in self.trade_details[:20]:
                hold_min = t.hold_time_seconds // 60
                lines.append(
                    f"| {t.symbol} | {t.direction} | ${t.pnl:+.2f} | "
                    f"{t.pnl_percent:+.1f}% | {hold_min}m | {t.exit_reason} |"
                )
            lines.append("")

        return "\n".join(lines)


class EODReportGenerator:
    """
    Generates end-of-day reports from persistence data.

    Usage:
        generator = EODReportGenerator(db)
        report = generator.generate_report()
        generator.save_report(report, "./reports/")
    """

    def __init__(self, db):
        """
        Initialize with database connection.

        Args:
            db: Database instance from persistence layer
        """
        self.db = db

        # Import repositories
        from morpheus.persistence.repository import (
            SignalRepository,
            DecisionRepository,
            TradeRepository,
            RegimeRepository,
            DailySummaryRepository,
        )

        self.signals = SignalRepository(db)
        self.decisions = DecisionRepository(db)
        self.trades = TradeRepository(db)
        self.regimes = RegimeRepository(db)
        self.daily_summaries = DailySummaryRepository(db)

    def generate_report(self, report_date: date | None = None) -> EODReport:
        """
        Generate EOD report for a given date.

        Args:
            report_date: Date to generate report for (default: today)

        Returns:
            EODReport instance
        """
        if report_date is None:
            report_date = date.today()

        date_str = report_date.isoformat()

        # Fetch data
        signals = self.signals.get_by_date_range(date_str, date_str)
        rejections = self.decisions.get_rejections_today() if report_date == date.today() else []
        trades_data = self.trades.get_today_trades() if report_date == date.today() else []
        regimes = self.regimes.get_today_regimes() if report_date == date.today() else []

        # Calculate signal stats
        signals_detected = len(signals)
        signals_rejected = len(rejections)
        signals_approved = signals_detected - signals_rejected
        approval_rate = (signals_approved / signals_detected * 100) if signals_detected > 0 else 0

        # Calculate trade stats
        trades_closed = [t for t in trades_data if t.get('status') == 'closed']
        trades_executed = len([t for t in trades_data if t.get('trade_type') == 'full'])
        trades_shadow = len([t for t in trades_data if t.get('trade_type') == 'shadow'])

        trades_won = len([t for t in trades_closed if (t.get('pnl') or 0) > 0])
        trades_lost = len([t for t in trades_closed if (t.get('pnl') or 0) < 0])
        win_rate = (trades_won / len(trades_closed) * 100) if trades_closed else 0

        # Calculate P&L
        pnls = [t.get('pnl', 0) for t in trades_closed]
        total_pnl = sum(pnls)
        avg_pnl = total_pnl / len(pnls) if pnls else 0
        best_pnl = max(pnls) if pnls else 0
        worst_pnl = min(pnls) if pnls else 0
        max_drawdown = self._calculate_max_drawdown(pnls)

        # Calculate execution quality
        slippages = [t.get('slippage', 0) for t in trades_closed if t.get('slippage')]
        spreads = [t.get('actual_spread', 0) for t in trades_closed if t.get('actual_spread')]
        hold_times = [t.get('hold_time_seconds', 0) for t in trades_closed if t.get('hold_time_seconds')]

        avg_slippage = sum(slippages) / len(slippages) if slippages else 0
        avg_spread = sum(spreads) / len(spreads) if spreads else 0
        avg_hold_time = sum(hold_times) / len(hold_times) if hold_times else 0

        # Analyze rejections
        rejection_breakdown = self._analyze_rejections(rejections)

        # Build trade details
        trade_details = [
            TradeAnalysis(
                trade_id=t.get('trade_id', ''),
                symbol=t.get('symbol', ''),
                direction=t.get('direction', ''),
                trade_type=t.get('trade_type', ''),
                entry_price=t.get('entry_price', 0),
                exit_price=t.get('exit_price', 0),
                pnl=t.get('pnl', 0),
                pnl_percent=t.get('pnl_percent', 0),
                hold_time_seconds=t.get('hold_time_seconds', 0),
                expected_entry=t.get('expected_entry'),
                slippage=t.get('slippage'),
                actual_spread=t.get('actual_spread'),
                exit_reason=t.get('exit_reason', ''),
            )
            for t in trades_closed
        ]

        # Identify top issues
        top_issues = self._identify_top_issues(rejection_breakdown, trade_details, avg_slippage)

        # Market context
        regime_values = [r.get('regime_value', '') for r in regimes]
        regime_summary = self._summarize_regimes(regime_values)
        active_symbols = list(set(s.get('symbol', '') for s in signals))

        return EODReport(
            report_date=report_date,
            generated_at=datetime.now(timezone.utc),
            signals_detected=signals_detected,
            signals_approved=signals_approved,
            signals_rejected=signals_rejected,
            approval_rate=approval_rate,
            trades_executed=trades_executed,
            trades_shadow=trades_shadow,
            trades_won=trades_won,
            trades_lost=trades_lost,
            win_rate=win_rate,
            total_pnl=total_pnl,
            avg_pnl_per_trade=avg_pnl,
            best_trade_pnl=best_pnl,
            worst_trade_pnl=worst_pnl,
            max_drawdown=max_drawdown,
            avg_slippage=avg_slippage,
            avg_spread=avg_spread,
            avg_hold_time_seconds=avg_hold_time,
            rejection_breakdown=rejection_breakdown,
            trade_details=trade_details,
            top_issues=top_issues,
            market_regime_summary=regime_summary,
            active_symbols=active_symbols,
        )

    def _calculate_max_drawdown(self, pnls: list[float]) -> float:
        """Calculate maximum drawdown from P&L sequence."""
        if not pnls:
            return 0

        cumulative = 0
        peak = 0
        max_dd = 0

        for pnl in pnls:
            cumulative += pnl
            if cumulative > peak:
                peak = cumulative
            dd = peak - cumulative
            if dd > max_dd:
                max_dd = dd

        return max_dd

    def _analyze_rejections(self, rejections: list[dict]) -> list[RejectionAnalysis]:
        """Analyze rejection reasons."""
        if not rejections:
            return []

        # Group by reason
        reason_counts: dict[str, list[str]] = {}
        for r in rejections:
            reason = r.get('reason') or 'unknown'
            symbol = r.get('symbol', '')
            if reason not in reason_counts:
                reason_counts[reason] = []
            reason_counts[reason].append(symbol)

        total = len(rejections)

        # Build analysis
        breakdown = []
        for reason, symbols in sorted(reason_counts.items(), key=lambda x: -len(x[1])):
            breakdown.append(RejectionAnalysis(
                reason=reason,
                count=len(symbols),
                percentage=(len(symbols) / total * 100) if total > 0 else 0,
                examples=symbols,
            ))

        return breakdown

    def _summarize_regimes(self, regime_values: list[str]) -> str:
        """Summarize market regimes observed."""
        if not regime_values:
            return "No regime data"

        from collections import Counter
        counts = Counter(regime_values)
        most_common = counts.most_common(3)

        parts = [f"{regime}({count})" for regime, count in most_common]
        return ", ".join(parts)

    def _identify_top_issues(
        self,
        rejections: list[RejectionAnalysis],
        trades: list[TradeAnalysis],
        avg_slippage: float,
    ) -> list[str]:
        """Identify top issues to address for improvement."""
        issues = []

        # High rejection rate for specific reason
        for r in rejections[:3]:
            if r.percentage > 30:
                issues.append(f"High rejection rate for '{r.reason}' ({r.percentage:.0f}%)")

        # Slippage issues
        if avg_slippage > 0.05:
            issues.append(f"High average slippage (${avg_slippage:.3f})")

        # Losing trades analysis
        losers = [t for t in trades if t.pnl < 0]
        if losers:
            quick_losses = [t for t in losers if t.hold_time_seconds < 60]
            if len(quick_losses) > len(losers) * 0.5:
                issues.append("Many losses within first minute - entry timing issue")

        # Win rate issues
        if trades:
            win_rate = len([t for t in trades if t.pnl > 0]) / len(trades) * 100
            if win_rate < 40:
                issues.append(f"Low win rate ({win_rate:.0f}%) - review signal quality")

        return issues[:5]

    def save_report(
        self,
        report: EODReport,
        output_dir: str | Path,
        formats: list[str] | None = None,
    ) -> list[Path]:
        """
        Save report to files.

        Args:
            report: EODReport to save
            output_dir: Directory to save to
            formats: List of formats ('json', 'md'). Default: both

        Returns:
            List of saved file paths
        """
        if formats is None:
            formats = ['json', 'md']

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        saved = []
        date_str = report.report_date.isoformat()

        if 'json' in formats:
            json_path = output_dir / f"eod_report_{date_str}.json"
            with open(json_path, 'w') as f:
                json.dump(report.to_dict(), f, indent=2)
            saved.append(json_path)
            logger.info(f"Saved JSON report: {json_path}")

        if 'md' in formats:
            md_path = output_dir / f"eod_report_{date_str}.md"
            with open(md_path, 'w') as f:
                f.write(report.to_markdown())
            saved.append(md_path)
            logger.info(f"Saved Markdown report: {md_path}")

        # Also update daily summary in database
        self.daily_summaries.save_or_update(
            trade_date=date_str,
            signals_detected=report.signals_detected,
            signals_approved=report.signals_approved,
            signals_rejected=report.signals_rejected,
            trades_executed=report.trades_executed,
            trades_shadow=report.trades_shadow,
            trades_won=report.trades_won,
            trades_lost=report.trades_lost,
            total_pnl=report.total_pnl,
            max_drawdown=report.max_drawdown,
            rejection_reasons={r.reason: r.count for r in report.rejection_breakdown},
            market_regime_summary=report.market_regime_summary,
        )

        return saved
