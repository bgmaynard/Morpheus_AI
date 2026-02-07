"""
Live Validation Logger - Rolling markdown + JSONL log for shadow/micro mode.

Hooks into emit_event() and records qualifying signals, gate failures,
completed trades, and execution blocks for live validation analysis.

Output files (monthly rolling):
  - D:/AI_BOT_DATA/logs/LIVE_VALIDATION_YYYY-MM.md   (human-readable)
  - D:/AI_BOT_DATA/logs/LIVE_VALIDATION_YYYY-MM.jsonl (machine-readable)
"""

from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from morpheus.core.events import Event, EventType

logger = logging.getLogger(__name__)

# Data root from env, fallback to D:/AI_BOT_DATA
DATA_ROOT = Path(os.environ.get("MORPHEUS_DATA_ROOT", "D:/AI_BOT_DATA"))
LOGS_DIR = DATA_ROOT / "logs"

# Events we listen to
TRACKED_EVENTS = {
    EventType.RISK_APPROVED,
    EventType.IGNITION_REJECTED,
    EventType.IGNITION_APPROVED,
    EventType.TRADE_EXITED,
    EventType.EXECUTION_BLOCKED,
}


class LiveValidationLogger:
    """
    Rolling validation logger for shadow/micro mode live validation.

    Thread-safe file appends (same pattern as DecisionLogger).
    """

    def __init__(self, logs_dir: Path | None = None) -> None:
        self._logs_dir = logs_dir or LOGS_DIR
        self._logs_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._event_count = 0
        logger.info(f"[LIVE_VALIDATION] Logger initialized -> {self._logs_dir}")

    def on_event(self, event: Event) -> None:
        """Process an event if it's one we track."""
        if event.event_type not in TRACKED_EVENTS:
            return

        try:
            record = self._build_record(event)
            if record:
                self._append(record)
                self._event_count += 1
        except Exception as e:
            logger.error(f"[LIVE_VALIDATION] Error processing {event.event_type.value}: {e}")

    def _build_record(self, event: Event) -> dict[str, Any] | None:
        """Build a structured record from an event."""
        payload = event.payload or {}
        ts = event.timestamp.isoformat() if event.timestamp else datetime.now(timezone.utc).isoformat()
        symbol = event.symbol or payload.get("symbol", "")

        base = {
            "timestamp": ts,
            "event_type": event.event_type.value,
            "symbol": symbol,
            "event_id": event.event_id,
        }

        if event.event_type == EventType.RISK_APPROVED:
            base["status"] = "QUALIFIED"
            base["strategy"] = payload.get("strategy", "")
            base["direction"] = payload.get("direction", "")
            base["score"] = payload.get("confidence", 0.0)
            base["momentum_snapshot"] = payload.get("momentum_snapshot", {})
            base["position_size"] = payload.get("shares", 0)

        elif event.event_type == EventType.IGNITION_REJECTED:
            base["status"] = "GATE_REJECTED"
            base["failures"] = payload.get("failures", [])
            base["checks_run"] = payload.get("checks_run", 0)
            base["momentum_snapshot"] = payload.get("momentum_snapshot", {})

        elif event.event_type == EventType.IGNITION_APPROVED:
            base["status"] = "GATE_PASSED"
            base["checks_run"] = payload.get("checks_run", 0)
            base["momentum_snapshot"] = payload.get("momentum_snapshot", {})

        elif event.event_type == EventType.TRADE_EXITED:
            base["status"] = "TRADE_CLOSED"
            base["exit_reason"] = payload.get("exit_reason", "")
            base["pnl"] = payload.get("pnl", 0.0)
            base["pnl_pct"] = payload.get("pnl_pct", 0.0)
            base["hold_time"] = payload.get("hold_time", 0)
            base["strategy"] = payload.get("strategy_name", "")
            base["trade_tag"] = payload.get("trade_tag", "")
            base["entry_momentum_score"] = payload.get("entry_momentum_score", 0.0)
            base["exit_momentum_state"] = payload.get("exit_momentum_state", "")

        elif event.event_type == EventType.EXECUTION_BLOCKED:
            base["status"] = "BLOCKED"
            base["block_reasons"] = payload.get("block_reasons", [])
            base["mode"] = payload.get("mode", "")

        else:
            return None

        return base

    def _append(self, record: dict[str, Any]) -> None:
        """Append record to both markdown and JSONL files (thread-safe)."""
        now = datetime.now(timezone.utc)
        month_str = now.strftime("%Y-%m")
        md_path = self._logs_dir / f"LIVE_VALIDATION_{month_str}.md"
        jsonl_path = self._logs_dir / f"LIVE_VALIDATION_{month_str}.jsonl"

        with self._lock:
            # JSONL append
            try:
                with open(jsonl_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(record, default=str) + "\n")
            except Exception as e:
                logger.error(f"[LIVE_VALIDATION] JSONL write error: {e}")

            # Markdown append
            try:
                with open(md_path, "a", encoding="utf-8") as f:
                    ts = record.get("timestamp", "")
                    symbol = record.get("symbol", "?")
                    status = record.get("status", "?")
                    f.write(f"\n### {ts} | {symbol} | {status}\n\n")

                    if status == "QUALIFIED":
                        f.write(f"- Strategy: {record.get('strategy', '')}\n")
                        f.write(f"- Direction: {record.get('direction', '')}\n")
                        f.write(f"- Score: {record.get('score', 0):.2f}\n")
                        snap = record.get("momentum_snapshot", {})
                        if snap:
                            f.write(f"- Momentum: score={snap.get('momentum_score', '?')}, "
                                    f"nofi={snap.get('nofi', '?')}, "
                                    f"l2={snap.get('l2_pressure', '?')}\n")

                    elif status == "GATE_REJECTED":
                        failures = record.get("failures", [])
                        f.write(f"- Checks run: {record.get('checks_run', 0)}\n")
                        f.write(f"- Failures ({len(failures)}):\n")
                        for fail in failures:
                            f.write(f"  - {fail}\n")

                    elif status == "GATE_PASSED":
                        f.write(f"- Checks run: {record.get('checks_run', 0)}\n")
                        snap = record.get("momentum_snapshot", {})
                        if snap:
                            f.write(f"- Momentum: score={snap.get('momentum_score', '?')}\n")

                    elif status == "TRADE_CLOSED":
                        f.write(f"- Exit reason: {record.get('exit_reason', '')}\n")
                        f.write(f"- PnL: ${record.get('pnl', 0):.2f} ({record.get('pnl_pct', 0):.2%})\n")
                        f.write(f"- Hold time: {record.get('hold_time', 0)}s\n")
                        f.write(f"- Strategy: {record.get('strategy', '')}\n")
                        f.write(f"- Mode tag: {record.get('trade_tag', '')}\n")

                    elif status == "BLOCKED":
                        f.write(f"- Reasons: {record.get('block_reasons', [])}\n")
                        f.write(f"- Mode: {record.get('mode', '')}\n")

                    f.write("\n---\n")
            except Exception as e:
                logger.error(f"[LIVE_VALIDATION] Markdown write error: {e}")

    def get_stats(self) -> dict[str, Any]:
        """Get logger stats for status endpoint."""
        return {
            "events_logged": self._event_count,
            "logs_dir": str(self._logs_dir),
        }
