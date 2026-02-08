"""
EOD Report Template v1 - 12-section standard for cross-bot parity.

Each section is a function that takes a report data dict and returns markdown.
Returns empty string if the data needed for that section is absent.

This template defines the canonical section order and rendering so that
Morpheus_AI, IBKR_Algo_BOT, and any future bot produce directly comparable
end-of-day reports.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

# ── Canonical section order ─────────────────────────────────────────────

SECTION_ORDER: list[str] = [
    "header",
    "executive_summary",
    "performance_metrics",
    "momentum_intelligence",
    "strategy_analysis",
    "execution_quality",
    "risk_management",
    "variance_intelligence",
    "system_health",
    "dev_ops",
    "action_plan",
    "cross_bot_comparison",
]

VERSION = "v1"


# ── Section renderers ───────────────────────────────────────────────────

def render_header(data: dict) -> str:
    date_str = data.get("date", "unknown")
    bot_id = data.get("bot_id", "Morpheus_AI")
    mode = data.get("runtime_mode", "PAPER")
    generated = data.get("generated_at", "")
    return "\n".join([
        f"# {bot_id} EOD Report - {date_str}",
        f"**Mode:** {mode} | **Template:** {VERSION} | **Generated:** {generated}",
        "",
        "---",
    ])


def render_executive_summary(data: dict) -> str:
    s = data.get("summary", {})
    trades = s.get("trades_executed", 0)
    wins = s.get("wins", 0)
    losses = s.get("losses", 0)
    wr = s.get("win_rate", 0)
    pnl = s.get("total_pnl", 0)
    pf = s.get("profit_factor", 0)
    signals = s.get("signals_detected", 0)
    rejected = s.get("signals_rejected", 0)

    # Format win_rate: if it's a float <1 treat as ratio, else percent
    wr_display = f"{wr:.1%}" if isinstance(wr, float) and wr < 1 else f"{wr:.1f}%"
    pf_display = str(pf) if pf == "inf" else f"{pf:.2f}"

    return "\n".join([
        "## 1. Executive Summary",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Total Trades | {trades} |",
        f"| Win / Loss | {wins}W / {losses}L |",
        f"| Win Rate | {wr_display} |",
        f"| Net P&L | ${pnl:+.2f} |",
        f"| Profit Factor | {pf_display} |",
        f"| Signals Detected | {signals} |",
        f"| Signals Rejected | {rejected} |",
        "",
    ])


def render_performance_metrics(data: dict) -> str:
    s = data.get("summary", {})
    trades = data.get("trade_details", [])
    pnl = s.get("total_pnl", 0)

    if not trades:
        return "\n".join([
            "## 2. Performance Metrics",
            "",
            "_No closed trades today._",
            "",
        ])

    pnls = [t.get("pnl", 0) for t in trades]
    winners = [p for p in pnls if p > 0]
    losers = [p for p in pnls if p < 0]

    avg_win = sum(winners) / len(winners) if winners else 0
    avg_loss = sum(losers) / len(losers) if losers else 0
    best = max(pnls) if pnls else 0
    worst = min(pnls) if pnls else 0
    expectancy = sum(pnls) / len(pnls) if pnls else 0

    return "\n".join([
        "## 2. Performance Metrics",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Total P&L | ${pnl:+.2f} |",
        f"| Avg Winner | ${avg_win:+.2f} |",
        f"| Avg Loser | ${avg_loss:+.2f} |",
        f"| Best Trade | ${best:+.2f} |",
        f"| Worst Trade | ${worst:+.2f} |",
        f"| Expectancy/Trade | ${expectancy:+.2f} |",
        "",
    ])


def render_momentum_intelligence(data: dict) -> str:
    mi = data.get("momentum_intelligence")
    if not mi:
        return "\n".join([
            "## 3. Momentum Intelligence",
            "",
            "_Momentum data not available for this session._",
            "",
        ])

    lines = [
        "## 3. Momentum Intelligence",
        "",
    ]

    # Ignition gate stats
    ig = mi.get("ignition_gate", {})
    if ig:
        lines.extend([
            "### Ignition Gate",
            "",
            "| Metric | Value |",
            "|--------|-------|",
            f"| Signals Evaluated | {ig.get('evaluated', 0)} |",
            f"| Approved | {ig.get('approved', 0)} |",
            f"| Rejected | {ig.get('rejected', 0)} |",
            "",
        ])

        rejections = ig.get("rejection_reasons", {})
        if rejections:
            lines.extend([
                "| Rejection Reason | Count |",
                "|------------------|-------|",
            ])
            for reason, count in sorted(rejections.items(), key=lambda x: -x[1]):
                lines.append(f"| {reason} | {count} |")
            lines.append("")

    # Per-trade momentum at entry
    entries = mi.get("entry_snapshots", [])
    if entries:
        lines.extend([
            "### Entry Momentum Snapshots",
            "",
            "| Symbol | Score | State | NOFI | L2 Press | Confidence |",
            "|--------|-------|-------|------|----------|------------|",
        ])
        for e in entries[:20]:
            lines.append(
                f"| {e.get('symbol', '')} | {e.get('score', 0):.1f} | "
                f"{e.get('state', '')} | {e.get('nofi', 0):.3f} | "
                f"{e.get('l2_pressure', 0):.3f} | {e.get('confidence', 0):.3f} |"
            )
        lines.append("")

    return "\n".join(lines)


def render_strategy_analysis(data: dict) -> str:
    strategies = data.get("strategies", {})
    if not strategies:
        return "\n".join([
            "## 4. Strategy Analysis",
            "",
            "_No strategy data available._",
            "",
        ])

    lines = [
        "## 4. Strategy Analysis",
        "",
        "| Strategy | Trades | Wins | Losses | P&L | Win Rate |",
        "|----------|--------|------|--------|-----|----------|",
    ]

    for strat, stats in sorted(strategies.items(), key=lambda x: -x[1].get("pnl", 0)):
        t = stats.get("trades", 0)
        w = stats.get("wins", 0)
        l = stats.get("losses", 0)
        p = stats.get("pnl", 0)
        wr = (w / t * 100) if t > 0 else 0
        lines.append(f"| {strat} | {t} | {w} | {l} | ${p:+.2f} | {wr:.0f}% |")

    lines.append("")
    return "\n".join(lines)


def render_execution_quality(data: dict) -> str:
    trades = data.get("trade_details", [])
    if not trades:
        return "\n".join([
            "## 5. Execution Quality",
            "",
            "_No trades to analyze._",
            "",
        ])

    hold_times = [t.get("hold_time_seconds", 0) for t in trades if t.get("hold_time_seconds")]
    avg_hold = sum(hold_times) / len(hold_times) if hold_times else 0

    # Exit reason breakdown
    exit_counts: dict[str, int] = {}
    exit_pnls: dict[str, list[float]] = {}
    for t in trades:
        reason = t.get("exit_reason", "unknown")
        exit_counts[reason] = exit_counts.get(reason, 0) + 1
        exit_pnls.setdefault(reason, []).append(t.get("pnl", 0))

    lines = [
        "## 5. Execution Quality",
        "",
        f"**Avg Hold Time:** {avg_hold:.0f}s",
        "",
        "### Exit Reason Breakdown",
        "",
        "| Exit Type | Count | Avg P&L |",
        "|-----------|-------|---------|",
    ]

    for reason, count in sorted(exit_counts.items(), key=lambda x: -x[1]):
        avg_pnl = sum(exit_pnls[reason]) / len(exit_pnls[reason])
        lines.append(f"| {reason} | {count} | ${avg_pnl:+.2f} |")

    lines.append("")

    # Trade detail table
    lines.extend([
        "### Trade Details",
        "",
        "| Symbol | Dir | Strategy | P&L | % | Hold(s) | Exit Reason |",
        "|--------|-----|----------|-----|---|---------|-------------|",
    ])
    for t in trades[:30]:
        lines.append(
            f"| {t.get('symbol', '')} | {t.get('direction', '')} | "
            f"{t.get('entry_signal', '')} | ${t.get('pnl', 0):+.2f} | "
            f"{t.get('pnl_percent', 0):+.1f}% | {t.get('hold_time_seconds', 0):.0f} | "
            f"{t.get('exit_reason', '')} |"
        )
    lines.append("")

    return "\n".join(lines)


def render_risk_management(data: dict) -> str:
    gating = data.get("gating_breakdown", {})
    s = data.get("summary", {})

    lines = [
        "## 6. Risk Management",
        "",
    ]

    # Gating blocks
    by_reason = gating.get("by_reason", {})
    total_blocks = gating.get("total_blocks", 0)

    lines.extend([
        f"**Total Gating Blocks:** {total_blocks}",
        "",
    ])

    if by_reason:
        lines.extend([
            "| Block Reason | Count |",
            "|--------------|-------|",
        ])
        for reason, count in sorted(by_reason.items(), key=lambda x: -x[1]):
            lines.append(f"| {reason} | {count} |")
        lines.append("")

    by_stage = gating.get("by_stage", {})
    if by_stage:
        lines.extend([
            "| Pipeline Stage | Blocks |",
            "|----------------|--------|",
        ])
        for stage, count in sorted(by_stage.items(), key=lambda x: -x[1]):
            lines.append(f"| {stage} | {count} |")
        lines.append("")

    return "\n".join(lines)


def render_variance_intelligence(data: dict) -> str:
    vi = data.get("variance_intelligence")
    if not vi:
        return "\n".join([
            "## 7. Variance Intelligence",
            "",
            "_Variance analysis not available for this session._",
            "",
        ])

    lines = [
        "## 7. Variance Intelligence",
        "",
    ]

    # Live vs replay delta
    delta = vi.get("live_vs_replay", {})
    if delta:
        lines.extend([
            "### Live vs Replay Baseline",
            "",
            "| Metric | Live | Replay Baseline | Delta |",
            "|--------|------|-----------------|-------|",
        ])
        for metric, vals in delta.items():
            lines.append(
                f"| {metric} | {vals.get('live', 'N/A')} | "
                f"{vals.get('baseline', 'N/A')} | {vals.get('delta', 'N/A')} |"
            )
        lines.append("")

    return "\n".join(lines)


def render_system_health(data: dict) -> str:
    sh = data.get("system_health")
    if not sh:
        return "\n".join([
            "## 8. System Health",
            "",
            "_System health telemetry not available._",
            "",
        ])

    lines = [
        "## 8. System Health",
        "",
        "| Metric | Value |",
        "|--------|-------|",
    ]

    for key, val in sh.items():
        lines.append(f"| {key} | {val} |")
    lines.append("")

    return "\n".join(lines)


def render_dev_ops(data: dict) -> str:
    do = data.get("dev_ops")
    if not do:
        return "\n".join([
            "## 9. DevOps",
            "",
            "_No DevOps notes for this session._",
            "",
        ])

    lines = [
        "## 9. DevOps",
        "",
    ]

    changes = do.get("changes_today", [])
    if changes:
        for i, change in enumerate(changes, 1):
            lines.append(f"{i}. {change}")
        lines.append("")

    return "\n".join(lines)


def render_action_plan(data: dict) -> str:
    ap = data.get("action_plan")
    non_trades = data.get("non_trades", [])

    lines = [
        "## 10. Action Plan",
        "",
    ]

    if ap:
        items = ap if isinstance(ap, list) else ap.get("items", [])
        for i, item in enumerate(items, 1):
            lines.append(f"{i}. {item}")
        lines.append("")
    else:
        lines.extend([
            "_No action items generated._",
            "",
        ])

    return "\n".join(lines)


def render_cross_bot_comparison(data: dict) -> str:
    cbc = data.get("cross_bot_comparison")
    if not cbc:
        return "\n".join([
            "## 11. Cross-Bot Comparison",
            "",
            "_Cross-bot data not available. Requires multi-bot report aggregation._",
            "",
        ])

    lines = [
        "## 11. Cross-Bot Comparison",
        "",
        "| Bot | Trades | Win Rate | P&L | Profit Factor |",
        "|-----|--------|----------|-----|---------------|",
    ]

    for bot, stats in cbc.items():
        lines.append(
            f"| {bot} | {stats.get('trades', 0)} | "
            f"{stats.get('win_rate', 0):.1f}% | "
            f"${stats.get('pnl', 0):+.2f} | "
            f"{stats.get('profit_factor', 0):.2f} |"
        )
    lines.append("")

    return "\n".join(lines)


def render_market_context(data: dict) -> str:
    """Bonus section: market context (symbols tracked, regime)."""
    ctx = data.get("market_context", {})
    if not ctx:
        return ""

    symbols = ctx.get("active_symbols", [])
    total = ctx.get("total_symbols_tracked", len(symbols))

    return "\n".join([
        "## 12. Market Context",
        "",
        f"- **Symbols Tracked:** {total}",
        f"- **Symbols:** {', '.join(symbols[:25])}{'...' if len(symbols) > 25 else ''}",
        "",
    ])


# ── Renderer registry ───────────────────────────────────────────────────

SECTION_RENDERERS: dict[str, Any] = {
    "header": render_header,
    "executive_summary": render_executive_summary,
    "performance_metrics": render_performance_metrics,
    "momentum_intelligence": render_momentum_intelligence,
    "strategy_analysis": render_strategy_analysis,
    "execution_quality": render_execution_quality,
    "risk_management": render_risk_management,
    "variance_intelligence": render_variance_intelligence,
    "system_health": render_system_health,
    "dev_ops": render_dev_ops,
    "action_plan": render_action_plan,
    "cross_bot_comparison": render_cross_bot_comparison,
    "market_context": render_market_context,
}


# ── Main render entry point ─────────────────────────────────────────────

def render_report(data: dict, sections: list[str] | None = None) -> str:
    """
    Render a full EOD report from a data dict.

    Args:
        data: Report data dict (from generate_eod_report endpoint)
        sections: Override section order. Defaults to SECTION_ORDER + market_context.

    Returns:
        Complete markdown string.
    """
    if sections is None:
        sections = SECTION_ORDER + ["market_context"]

    parts: list[str] = []
    for section_name in sections:
        renderer = SECTION_RENDERERS.get(section_name)
        if renderer:
            content = renderer(data)
            if content.strip():
                parts.append(content)

    return "\n".join(parts)
