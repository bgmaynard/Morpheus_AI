# EOD Report Template v1 - Cross-Bot Standard

Version: v1
Status: Active
Applies to: Morpheus_AI, IBKR_Algo_BOT_V2

## Purpose

This template defines the canonical 12-section structure for End-of-Day reports.
All bots must emit these sections (even if some are empty/placeholder) so that
daily reports are directly comparable across systems.

## Section Order

| # | Section | Key | Data Source |
|---|---------|-----|-------------|
| - | Header | `header` | date, bot_id, runtime_mode, generated_at |
| 1 | Executive Summary | `executive_summary` | summary.* |
| 2 | Performance Metrics | `performance_metrics` | trade_details[], summary.total_pnl |
| 3 | Momentum Intelligence | `momentum_intelligence` | momentum_intelligence.ignition_gate, .entry_snapshots |
| 4 | Strategy Analysis | `strategy_analysis` | strategies.{name}.{trades,wins,losses,pnl} |
| 5 | Execution Quality | `execution_quality` | trade_details[].{hold_time_seconds,exit_reason,pnl} |
| 6 | Risk Management | `risk_management` | gating_breakdown.{by_reason,by_stage,total_blocks} |
| 7 | Variance Intelligence | `variance_intelligence` | variance_intelligence.live_vs_replay |
| 8 | System Health | `system_health` | system_health.{key: value} |
| 9 | DevOps | `dev_ops` | dev_ops.changes_today[] |
| 10 | Action Plan | `action_plan` | action_plan[] or action_plan.items[] |
| 11 | Cross-Bot Comparison | `cross_bot_comparison` | cross_bot_comparison.{bot}.{trades,win_rate,pnl,profit_factor} |
| 12 | Market Context | `market_context` | market_context.{active_symbols,total_symbols_tracked} |

## Report Data Dict Schema

The `data` dict passed to `render_report()` must contain these top-level keys:

```
{
  "date": "2026-02-07",
  "bot_id": "Morpheus_AI",
  "runtime_mode": "SHADOW",
  "generated_at": "2026-02-07T21:00:00Z",

  "summary": {
    "signals_detected": int,
    "signals_approved": int,
    "signals_rejected": int,
    "trades_executed": int,
    "wins": int,
    "losses": int,
    "win_rate": float,        # 0.0 - 1.0
    "total_pnl": float,
    "profit_factor": float | "inf"
  },

  "strategies": {
    "<strategy_name>": {
      "trades": int,
      "wins": int,
      "losses": int,
      "pnl": float
    }
  },

  "trade_details": [
    {
      "symbol": str,
      "direction": str,
      "entry_signal": str,
      "pnl": float,
      "pnl_percent": float,
      "hold_time_seconds": float,
      "exit_reason": str
    }
  ],

  "gating_breakdown": {
    "by_reason": { "<reason>": int },
    "by_stage": { "<stage>": int },
    "total_blocks": int
  },

  "market_context": {
    "active_symbols": [str],
    "total_symbols_tracked": int
  },

  // Optional enrichment keys (empty = placeholder section)
  "momentum_intelligence": { ... } | null,
  "variance_intelligence": { ... } | null,
  "system_health": { ... } | null,
  "dev_ops": { ... } | null,
  "action_plan": [...] | null,
  "cross_bot_comparison": { ... } | null,
  "non_trades": [...]
}
```

## How to Add a New Bot

1. Import `render_report` from `morpheus.reporting.templates.eod_v1`
2. Build the data dict with the schema above
3. Call `render_report(data)` to get markdown
4. Save alongside JSON output

## Config

Set `eod_template_version` in `runtime_config.json`:
- `"legacy"` - Original hardcoded markdown (pre-v1)
- `"v1"` - This 12-section standard template
