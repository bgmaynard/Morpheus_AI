"""
Pipeline Decision Logger - IBKR-comparable JSONL trade/signal/gating ledgers.

Writes real-time decision data to daily JSONL files:
    reports/{date}/trade_ledger.jsonl    - Completed trades with full context
    reports/{date}/signal_ledger.jsonl   - All signals (approved + rejected)
    reports/{date}/gating_blocks.jsonl   - Rejected signals with reasons

Thread-safe for concurrent writes.
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from morpheus.core.events import Event, EventType

logger = logging.getLogger(__name__)


class DecisionLogger:
    """
    Real-time JSONL logger for pipeline decisions.

    Captures enriched trade and signal data in IBKR-comparable format.
    Listens to events from emit_event() and writes to daily report directories.
    """

    def __init__(self, reports_dir: Path | str = "./reports"):
        self._reports_dir = Path(reports_dir)
        self._lock = threading.Lock()
        self._initialized = False

    def _ensure_dir(self, report_date: date) -> Path:
        """Ensure the daily report directory exists."""
        day_dir = self._reports_dir / report_date.isoformat()
        day_dir.mkdir(parents=True, exist_ok=True)
        return day_dir

    def _append_jsonl(self, file_path: Path, record: dict[str, Any]) -> None:
        """Thread-safe append a JSON record to a JSONL file."""
        line = json.dumps(record, separators=(",", ":"), default=str)
        with self._lock:
            with open(file_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")

    def on_event(self, event: Event) -> None:
        """
        Process a pipeline event and write to appropriate ledger.

        Called from emit_event() for every event.
        Only acts on decision-relevant event types.
        """
        try:
            et = event.event_type

            if et == EventType.SIGNAL_CANDIDATE:
                self._log_signal(event, approved=True, observed=False)
            elif et == EventType.SIGNAL_OBSERVED:
                self._log_signal(event, approved=False, observed=True)
            elif et == EventType.META_REJECTED:
                self._log_gating_block(event, stage="META_GATE")
            elif et == EventType.RISK_VETO:
                self._log_gating_block(event, stage="RISK")
            elif et == EventType.TRADE_CLOSED:
                self._log_trade(event)
            elif et == EventType.META_APPROVED:
                self._log_signal_decision(event, decision="META_APPROVED")
            elif et == EventType.RISK_APPROVED:
                self._log_signal_decision(event, decision="RISK_APPROVED")

        except Exception as e:
            logger.error(f"DecisionLogger error on {event.event_type.value}: {e}")

    # -------------------------------------------------------------------------
    # Signal Ledger
    # -------------------------------------------------------------------------

    def _log_signal(self, event: Event, approved: bool, observed: bool) -> None:
        """Log a signal candidate to signal_ledger.jsonl."""
        today = event.timestamp.date()
        day_dir = self._ensure_dir(today)

        payload = event.payload or {}
        record = {
            "event_id": event.event_id,
            "timestamp": event.timestamp.isoformat(),
            "symbol": event.symbol or payload.get("symbol", ""),
            "signal_type": payload.get("signal_type", ""),
            "strategy": payload.get("strategy", payload.get("strategy_name", "")),
            "direction": payload.get("direction", ""),
            "entry_price": payload.get("entry_price", 0),
            "confidence": payload.get("confidence", payload.get("score", 0)),
            "approved": approved,
            "observed": observed,
            "correlation_id": event.correlation_id,
            "trade_id": event.trade_id,
            # Market conditions at signal time
            "market_conditions": self._extract_market_conditions(payload),
            # MASS context
            "structure_grade": payload.get("structure_grade", ""),
            "structure_score": payload.get("structure_score", 0),
            "mass_regime": payload.get("mass_regime", payload.get("regime_mode", "")),
            "tags": payload.get("tags", []),
        }

        self._append_jsonl(day_dir / "signal_ledger.jsonl", record)

        if not self._initialized:
            logger.info(f"[DECISION-LOG] Signal ledger started: {day_dir / 'signal_ledger.jsonl'}")
            self._initialized = True

    def _log_signal_decision(self, event: Event, decision: str) -> None:
        """Log a gate/risk approval to signal_ledger as an update."""
        today = event.timestamp.date()
        day_dir = self._ensure_dir(today)

        payload = event.payload or {}
        record = {
            "event_id": event.event_id,
            "timestamp": event.timestamp.isoformat(),
            "symbol": event.symbol or payload.get("symbol", ""),
            "decision": decision,
            "correlation_id": event.correlation_id,
            "trade_id": event.trade_id,
            "details": {k: v for k, v in payload.items()
                       if k not in ("symbol",)},
        }

        self._append_jsonl(day_dir / "signal_ledger.jsonl", record)

    # -------------------------------------------------------------------------
    # Gating Blocks Ledger
    # -------------------------------------------------------------------------

    def _log_gating_block(self, event: Event, stage: str) -> None:
        """Log a rejected signal to gating_blocks.jsonl."""
        today = event.timestamp.date()
        day_dir = self._ensure_dir(today)

        payload = event.payload or {}
        record = {
            "event_id": event.event_id,
            "timestamp": event.timestamp.isoformat(),
            "symbol": event.symbol or payload.get("symbol", ""),
            "stage": stage,
            "reason": payload.get("reason", payload.get("veto_reason", "")),
            "strategy": payload.get("strategy", payload.get("strategy_name", "")),
            "direction": payload.get("direction", ""),
            "signal_type": payload.get("signal_type", ""),
            "confidence": payload.get("confidence", payload.get("score", 0)),
            "correlation_id": event.correlation_id,
            "trade_id": event.trade_id,
            # What was blocked
            "details": {k: v for k, v in payload.items()
                       if k not in ("symbol", "reason", "veto_reason")},
            # Market conditions at rejection
            "market_conditions": self._extract_market_conditions(payload),
            "structure_grade": payload.get("structure_grade", ""),
            "mass_regime": payload.get("mass_regime", payload.get("regime_mode", "")),
        }

        self._append_jsonl(day_dir / "gating_blocks.jsonl", record)
        logger.debug(f"[DECISION-LOG] Gating block: {record['symbol']} {stage} - {record['reason']}")

    # -------------------------------------------------------------------------
    # Trade Ledger
    # -------------------------------------------------------------------------

    def _log_trade(self, event: Event) -> None:
        """Log a completed trade to trade_ledger.jsonl with full context."""
        today = event.timestamp.date()
        day_dir = self._ensure_dir(today)

        payload = event.payload or {}
        record = {
            "trade_id": event.trade_id or payload.get("trade_id", ""),
            "event_id": event.event_id,
            "timestamp": event.timestamp.isoformat(),
            "symbol": event.symbol or payload.get("symbol", ""),
            "direction": payload.get("direction", ""),
            "entry_signal": payload.get("strategy", payload.get("strategy_name", "")),
            # Prices
            "entry_time": payload.get("entry_time", ""),
            "entry_price": payload.get("entry_price", 0),
            "exit_time": payload.get("exit_time", event.timestamp.isoformat()),
            "exit_price": payload.get("exit_price", 0),
            "shares": payload.get("shares", payload.get("quantity", 0)),
            # P&L
            "pnl": payload.get("pnl", payload.get("pnl_dollars", 0)),
            "pnl_percent": payload.get("pnl_percent", payload.get("pnl_pct", 0)),
            "hold_time_seconds": payload.get("hold_time_seconds", 0),
            # Extremes
            "max_gain_percent": payload.get("max_gain_percent", 0),
            "max_drawdown_percent": payload.get("max_drawdown_percent", 0),
            # Exit
            "exit_reason": payload.get("exit_reason", ""),
            "status": payload.get("status", "closed"),
            # MASS context
            "structure_grade": payload.get("structure_grade", payload.get("structure_grade_at_entry", "")),
            "structure_score": payload.get("structure_score", 0),
            "mass_regime": payload.get("mass_regime", payload.get("mass_regime_mode", "")),
            "volatility_regime": payload.get("volatility_regime", ""),
            # Market conditions at entry (IBKR secondary_triggers equivalent)
            "market_conditions_at_entry": self._extract_market_conditions(payload),
            # Correlation
            "correlation_id": event.correlation_id,
            # Momentum intelligence
            "entry_momentum_score": payload.get("entry_momentum_score", 0.0),
            "entry_momentum_state": payload.get("entry_momentum_state", "UNKNOWN"),
            "entry_confidence": payload.get("entry_confidence", 0.0),
            "exit_momentum_state": payload.get("exit_momentum_state", ""),
            "avg_slippage": payload.get("avg_slippage", 0.0),
            "execution_latency_ms": payload.get("execution_latency_ms", 0.0),
            "override_flag": payload.get("override_flag", False),
        }

        self._append_jsonl(day_dir / "trade_ledger.jsonl", record)
        logger.info(
            f"[DECISION-LOG] Trade closed: {record['symbol']} "
            f"PnL=${record['pnl']:+.2f} ({record['pnl_percent']:+.2f}%) "
            f"via {record['entry_signal']}"
        )

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def _extract_market_conditions(payload: dict[str, Any]) -> dict[str, Any]:
        """
        Extract market conditions from event payload.

        Maps to IBKR's secondary_triggers concept.
        Pulls from nested 'market_conditions' or 'features' or top-level fields.
        """
        # Check if there's already a market_conditions dict
        mc = payload.get("market_conditions", payload.get("market_conditions_at_entry", {}))
        if mc and isinstance(mc, dict):
            return mc

        # Build from available payload fields
        conditions: dict[str, Any] = {}

        # Spread
        if "spread_pct" in payload or "spread" in payload:
            conditions["spread_at_entry_pct"] = payload.get("spread_pct", payload.get("spread", 0))

        # Volume
        if "volume" in payload:
            conditions["volume_at_entry"] = payload.get("volume", 0)

        # VWAP
        if "vwap_position" in payload:
            conditions["vwap_position"] = payload["vwap_position"]
        elif "vwap" in payload and "last" in payload:
            last = payload.get("last", 0)
            vwap = payload.get("vwap", 0)
            if vwap > 0 and last > 0:
                conditions["vwap_position"] = "above" if last > vwap else "below"

        # EMAs
        for key in ("ema9", "ema20", "ema_9", "ema_20"):
            if key in payload:
                clean_key = key.replace("_", "")
                conditions[clean_key] = payload[key]

        # RSI
        if "rsi" in payload:
            conditions["rsi"] = payload["rsi"]

        # MACD
        if "macd_signal" in payload:
            conditions["macd_signal"] = payload["macd_signal"]
        elif "macd" in payload:
            conditions["macd"] = payload["macd"]

        # RVOL
        if "rvol" in payload:
            conditions["rvol"] = payload["rvol"]

        # Change %
        if "change_pct" in payload:
            conditions["change_pct"] = payload["change_pct"]

        # Market mode
        if "market_mode" in payload:
            conditions["market_mode"] = payload["market_mode"]

        # Time bucket
        if "time_bucket" in payload:
            conditions["time_bucket"] = payload["time_bucket"]

        # Scanner score
        if "scanner_score" in payload:
            conditions["scanner_score"] = payload["scanner_score"]

        # Catalyst
        if "has_catalyst" in payload:
            conditions["has_catalyst"] = payload["has_catalyst"]

        # Features sub-dict
        features = payload.get("features", {})
        if isinstance(features, dict):
            for key in ("spread_pct", "rvol", "rsi", "ema9", "ema20", "vwap",
                        "change_pct", "volume", "macd"):
                if key in features and key not in conditions:
                    conditions[key] = features[key]

        return conditions

    def get_reports_dir(self) -> Path:
        """Return the base reports directory."""
        return self._reports_dir

    def get_today_dir(self) -> Path:
        """Return today's report directory."""
        today = date.today()
        return self._ensure_dir(today)


# ---------------------------------------------------------------------------
# Global instance (lazy-loaded)
# ---------------------------------------------------------------------------

_logger: DecisionLogger | None = None


def get_decision_logger() -> DecisionLogger:
    """Get or create the global decision logger."""
    global _logger
    if _logger is None:
        _logger = DecisionLogger("./reports")
    return _logger


def reset_decision_logger() -> None:
    """Reset the global decision logger (useful for testing)."""
    global _logger
    _logger = None
