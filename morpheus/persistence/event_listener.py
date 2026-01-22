"""
Event Listener - Hooks into pipeline to persist all events.

This listener subscribes to the event stream and persists:
- All signals detected
- All gate/risk decisions
- All trades (shadow and real)
- All regime changes
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from morpheus.core.events import Event, EventType
from morpheus.persistence.database import Database
from morpheus.persistence.repository import (
    SignalRepository,
    DecisionRepository,
    TradeRepository,
    WatchlistRepository,
    RegimeRepository,
    SignalRecord,
    DecisionRecord,
    TradeRecord,
    WatchlistRecord,
)

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    """Get current UTC timestamp as ISO string."""
    return datetime.now(timezone.utc).isoformat()


class PersistenceListener:
    """
    Listens to pipeline events and persists them to the database.

    Usage:
        db = Database()
        listener = PersistenceListener(db)

        # Hook into server's event emission
        async def emit_with_persistence(event: Event):
            await listener.on_event(event)
            # ... send to WebSocket etc
    """

    def __init__(self, db: Database):
        self.db = db
        self.signals = SignalRepository(db)
        self.decisions = DecisionRepository(db)
        self.trades = TradeRepository(db)
        self.watchlist = WatchlistRepository(db)
        self.regimes = RegimeRepository(db)

        # Track in-flight signals for correlation
        self._pending_signals: dict[str, str] = {}  # correlation_id -> signal_id

        logger.info("Persistence listener initialized")

    async def on_event(self, event: Event) -> None:
        """
        Process an event and persist it appropriately.

        This is the main entry point - call this for every event emitted.
        """
        try:
            handler = self._get_handler(event.event_type)
            if handler:
                await handler(event)
        except Exception as e:
            logger.error(f"Error persisting event {event.event_type}: {e}")

    def _get_handler(self, event_type: EventType):
        """Get the appropriate handler for an event type."""
        handlers = {
            EventType.SIGNAL_CANDIDATE: self._handle_signal_candidate,
            EventType.SIGNAL_SCORED: self._handle_signal_scored,
            EventType.META_APPROVED: self._handle_meta_approved,
            EventType.META_REJECTED: self._handle_meta_rejected,
            EventType.RISK_APPROVED: self._handle_risk_approved,
            EventType.RISK_VETO: self._handle_risk_veto,
            EventType.ORDER_SUBMITTED: self._handle_order_submitted,
            EventType.ORDER_CONFIRMED: self._handle_order_confirmed,
            EventType.FILL_RECEIVED: self._handle_fill_received,
            EventType.POSITION_CLOSED: self._handle_position_closed,
            EventType.REGIME_DETECTED: self._handle_regime_detected,
        }
        return handlers.get(event_type)

    async def _handle_signal_candidate(self, event: Event) -> None:
        """Persist a new signal candidate."""
        payload = event.payload

        # Generate a signal ID if not present
        signal_id = payload.get("signal_id") or f"sig_{event.timestamp[:19].replace(':', '').replace('-', '')}"

        record = SignalRecord(
            signal_id=signal_id,
            symbol=event.symbol or payload.get("symbol", "UNKNOWN"),
            strategy=payload.get("strategy", "unknown"),
            direction=payload.get("direction", "long"),
            detected_at=event.timestamp,
            entry_price=payload.get("entry_price"),
            stop_price=payload.get("stop_price"),
            target_price=payload.get("target_price"),
            raw_confidence=payload.get("confidence"),
            reasons=payload.get("reasons", []),
            market_regime=payload.get("regime"),
            session=payload.get("session"),
        )

        self.signals.save(record)

        # Track for correlation with later events
        if event.correlation_id:
            self._pending_signals[event.correlation_id] = signal_id

        logger.debug(f"Persisted signal candidate: {signal_id} for {record.symbol}")

    async def _handle_signal_scored(self, event: Event) -> None:
        """Update signal with AI score."""
        payload = event.payload
        signal_id = self._get_signal_id(event)

        if signal_id:
            self.signals.update_score(
                signal_id=signal_id,
                ai_score=payload.get("ai_score", 0),
                final_score=payload.get("final_score", 0),
            )
            logger.debug(f"Updated score for signal: {signal_id}")

    async def _handle_meta_approved(self, event: Event) -> None:
        """Persist meta gate approval."""
        await self._save_decision(event, "meta_gate", "approved")

    async def _handle_meta_rejected(self, event: Event) -> None:
        """Persist meta gate rejection."""
        await self._save_decision(event, "meta_gate", "rejected")

    async def _handle_risk_approved(self, event: Event) -> None:
        """Persist risk gate approval."""
        await self._save_decision(event, "risk_gate", "approved")

    async def _handle_risk_veto(self, event: Event) -> None:
        """Persist risk gate veto."""
        await self._save_decision(event, "risk_gate", "rejected")

    async def _save_decision(
        self,
        event: Event,
        decision_type: str,
        decision: str
    ) -> None:
        """Save a gate decision."""
        payload = event.payload
        signal_id = self._get_signal_id(event)

        if not signal_id:
            logger.warning(f"No signal_id for decision event: {event.event_type}")
            return

        record = DecisionRecord(
            signal_id=signal_id,
            decision_type=decision_type,
            decision=decision,
            decided_at=event.timestamp,
            reason=payload.get("reason") or payload.get("block_reason"),
            details={
                k: v for k, v in payload.items()
                if k not in ("reason", "block_reason", "signal_id")
            } or None,
        )

        self.decisions.save(record)
        logger.debug(f"Persisted {decision_type} {decision} for signal: {signal_id}")

    async def _handle_order_submitted(self, event: Event) -> None:
        """Persist a new trade when order is submitted."""
        payload = event.payload

        # Determine trade type based on trading mode
        trade_type = "shadow"  # Default to shadow
        if payload.get("trading_mode") == "live":
            trade_type = "full"
        elif payload.get("probe_mode"):
            trade_type = "probe"

        record = TradeRecord(
            trade_id=payload.get("order_id") or f"trade_{_now_iso()[:19].replace(':', '').replace('-', '')}",
            signal_id=self._get_signal_id(event),
            symbol=event.symbol or payload.get("symbol", "UNKNOWN"),
            direction=payload.get("direction", "long"),
            trade_type=trade_type,
            status="open",
            entry_price=payload.get("price"),
            entry_time=event.timestamp,
            shares=payload.get("quantity"),
            expected_entry=payload.get("expected_price"),
            actual_spread=payload.get("spread"),
        )

        self.trades.save(record)
        logger.debug(f"Persisted trade: {record.trade_id}")

    async def _handle_order_confirmed(self, event: Event) -> None:
        """Update trade with confirmation details."""
        # Order confirmed but not yet filled - no action needed for now
        pass

    async def _handle_fill_received(self, event: Event) -> None:
        """Update trade with fill information."""
        payload = event.payload
        trade_id = payload.get("order_id")

        if trade_id:
            # Calculate slippage if we have expected price
            slippage = None
            expected = payload.get("expected_price")
            actual = payload.get("fill_price")
            if expected and actual:
                slippage = actual - expected

            # For partial fills, we might update multiple times
            # For now, just log the fill
            logger.debug(f"Fill received for trade {trade_id}: {actual}")

    async def _handle_position_closed(self, event: Event) -> None:
        """Update trade with exit information."""
        payload = event.payload
        trade_id = payload.get("trade_id") or payload.get("order_id")

        if not trade_id:
            logger.warning("No trade_id in POSITION_CLOSED event")
            return

        # Calculate hold time
        entry_time = payload.get("entry_time")
        exit_time = event.timestamp
        hold_time_seconds = 0
        if entry_time:
            try:
                entry_dt = datetime.fromisoformat(entry_time.replace('Z', '+00:00'))
                exit_dt = datetime.fromisoformat(exit_time.replace('Z', '+00:00'))
                hold_time_seconds = int((exit_dt - entry_dt).total_seconds())
            except Exception:
                pass

        self.trades.update_exit(
            trade_id=trade_id,
            exit_price=payload.get("exit_price", 0),
            exit_time=exit_time,
            pnl=payload.get("pnl", 0),
            pnl_percent=payload.get("pnl_percent", 0),
            hold_time_seconds=hold_time_seconds,
            exit_reason=payload.get("exit_reason", "unknown"),
            slippage=payload.get("slippage"),
        )

        logger.info(
            f"Trade closed: {trade_id} | PnL: ${payload.get('pnl', 0):.2f} "
            f"({payload.get('pnl_percent', 0):.1f}%)"
        )

    async def _handle_regime_detected(self, event: Event) -> None:
        """Persist regime detection."""
        payload = event.payload

        self.regimes.save(
            regime_type=payload.get("regime_type", "market"),
            regime_value=payload.get("regime") or payload.get("value", "unknown"),
            confidence=payload.get("confidence"),
            symbol=event.symbol,
            details={
                k: v for k, v in payload.items()
                if k not in ("regime_type", "regime", "value", "confidence")
            } or None,
        )

        logger.debug(f"Persisted regime detection: {payload.get('regime')}")

    def _get_signal_id(self, event: Event) -> str | None:
        """Get signal ID from event or correlation tracking."""
        # Try direct signal_id first
        signal_id = event.payload.get("signal_id")
        if signal_id:
            return signal_id

        # Try correlation tracking
        if event.correlation_id and event.correlation_id in self._pending_signals:
            return self._pending_signals[event.correlation_id]

        return None

    # =========================================================================
    # Watchlist state changes
    # =========================================================================

    def record_watchlist_change(
        self,
        symbol: str,
        new_state: str,
        reason: str | None = None,
        score: float | None = None,
        rvol: float | None = None,
        price_change_pct: float | None = None,
        spread_avg: float | None = None,
    ) -> None:
        """Record a watchlist state change."""
        # Get previous state
        previous_state = self.watchlist.get_current_state(symbol)

        record = WatchlistRecord(
            symbol=symbol,
            state=new_state,
            state_changed_at=_now_iso(),
            discovery_reason=reason,
            discovery_score=score,
            previous_state=previous_state,
            rvol=rvol,
            price_change_pct=price_change_pct,
            spread_avg=spread_avg,
        )

        self.watchlist.save_state_change(record)
        logger.info(f"Watchlist: {symbol} {previous_state or 'None'} -> {new_state}")

    # =========================================================================
    # Query helpers
    # =========================================================================

    def get_today_stats(self) -> dict:
        """Get summary statistics for today."""
        from datetime import date
        today = date.today().isoformat()

        signals = self.signals.get_today_signals()
        rejections = self.decisions.get_rejections_today()
        trades = self.trades.get_today_trades()

        # Calculate stats
        trades_closed = [t for t in trades if t.get('status') == 'closed']
        pnl_total = sum(t.get('pnl', 0) for t in trades_closed)
        wins = len([t for t in trades_closed if (t.get('pnl') or 0) > 0])
        losses = len([t for t in trades_closed if (t.get('pnl') or 0) < 0])

        return {
            "date": today,
            "signals_detected": len(signals),
            "signals_rejected": len(rejections),
            "trades_total": len(trades),
            "trades_closed": len(trades_closed),
            "trades_won": wins,
            "trades_lost": losses,
            "win_rate": (wins / len(trades_closed) * 100) if trades_closed else 0,
            "total_pnl": pnl_total,
            "top_rejection_reasons": self.decisions.count_rejection_reasons(today),
        }
