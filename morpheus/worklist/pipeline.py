"""
Worklist Pipeline - Orchestrates symbol intake, scrutiny, scoring, and lifecycle.

This is the main integration point that:
    1. Receives scanner data from MAX_AI
    2. Runs scrutiny filters
    3. Computes priority scores
    4. Manages worklist entries
    5. Handles news integration
    6. Enforces session boundaries

Flow:
    Scanner Data → Scrutiny → Scoring → Worklist → Trading Selection
    News Data → Merge with existing or create new → Re-score → Worklist
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Awaitable, Optional

from morpheus.core.events import Event, EventType, create_event
from morpheus.worklist.store import (
    WorklistEntry,
    WorklistStatus,
    WorklistSource,
    WorklistStore,
    get_worklist_store,
)
from morpheus.worklist.scrutiny import (
    ScrutinyConfig,
    ScrutinyResult,
    WorklistScrutinizer,
    RejectionReason,
)
from morpheus.worklist.scoring import (
    ScoringConfig,
    WorklistScorer,
    classify_momentum_state,
    is_negative_news_category,
)

logger = logging.getLogger(__name__)


@dataclass
class WorklistPipelineConfig:
    """Configuration for the worklist pipeline."""

    # Scrutiny config
    scrutiny: ScrutinyConfig = None

    # Scoring config
    scoring: ScoringConfig = None

    # Selection config
    min_score_for_trading: float = 50.0  # Minimum score to be trade-eligible
    max_active_symbols: int = 20  # Maximum symbols to actively monitor

    # Session management
    auto_reset_on_new_day: bool = True  # Auto-reset worklist on new session
    archive_old_sessions: bool = True

    def __post_init__(self):
        if self.scrutiny is None:
            self.scrutiny = ScrutinyConfig()
        if self.scoring is None:
            self.scoring = ScoringConfig()


class WorklistPipeline:
    """
    Main worklist pipeline orchestrator.

    Responsibilities:
        - Process scanner data through scrutiny and scoring
        - Integrate news data into worklist
        - Enforce trading from worklist only
        - Manage session lifecycle

    Events emitted:
        - WORKLIST_REJECTED: Symbol failed scrutiny
        - WORKLIST_ADDED: New symbol added to worklist
        - WORKLIST_UPDATED: Existing symbol updated
        - WORKLIST_SCORED: Symbol scored/re-scored
        - WORKLIST_SELECTED_FOR_TRADE: Symbol selected for trading
        - WORKLIST_RESET: Session reset performed
    """

    def __init__(
        self,
        config: Optional[WorklistPipelineConfig] = None,
        emit_event: Optional[Callable[[Event], Awaitable[None]]] = None,
    ):
        """
        Initialize the worklist pipeline.

        Args:
            config: Pipeline configuration
            emit_event: Callback for emitting events
        """
        self._config = config or WorklistPipelineConfig()
        self._emit_event = emit_event

        # Initialize components
        self._store = get_worklist_store(emit_event=emit_event)
        self._scrutinizer = WorklistScrutinizer(
            config=self._config.scrutiny,
            emit_event=emit_event,
        )
        self._scorer = WorklistScorer(config=self._config.scoring)

        # Current regime (updated externally)
        self._current_regime = "NORMAL"

        logger.info("[WORKLIST_PIPELINE] Initialized")
        logger.info(f"[WORKLIST_PIPELINE] Min score for trading: {self._config.min_score_for_trading}")
        logger.info(f"[WORKLIST_PIPELINE] Max active symbols: {self._config.max_active_symbols}")

    # =========================================================================
    # SCANNER INTAKE (Deliverable 2)
    # =========================================================================

    async def process_scanner_row(
        self,
        symbol: str,
        scanner_score: float,
        gap_pct: float,
        rvol: float,
        volume: int,
        price: float,
        spread_pct: Optional[float] = None,
        is_halted: bool = False,
        data_timestamp: Optional[datetime] = None,
        # Extended scanner data for operator visibility
        float_shares: Optional[float] = None,
        avg_volume: Optional[int] = None,
        short_pct: Optional[float] = None,
        market_cap: Optional[float] = None,
        # Enhanced fields for professional scanner alignment
        rvol_5m: Optional[float] = None,
        halt_ts: Optional[str] = None,
        news_present: bool = False,
        # Scanner news indicator (AUTHORITATIVE - scanner says there's news)
        scanner_news_indicator: bool = False,
    ) -> Optional[WorklistEntry]:
        """
        Process a scanner row through scrutiny and scoring.

        Args:
            symbol: Stock symbol
            scanner_score: MAX_AI ai_score (0-100)
            gap_pct: Gap percentage
            rvol: Relative volume (daily)
            volume: Daily volume
            price: Current price
            spread_pct: Optional spread percentage
            is_halted: Whether halted
            data_timestamp: Data timestamp
            float_shares: Float size (millions)
            avg_volume: Average daily volume
            short_pct: Short interest percentage
            market_cap: Market capitalization (millions)
            rvol_5m: 5-minute RVOL (recent velocity)
            halt_ts: Halt timestamp
            news_present: Whether news exists for symbol
            scanner_news_indicator: AUTHORITATIVE scanner news flag (trusted)

        Returns:
            WorklistEntry if added/updated, None if rejected

        Note:
            Scanner news indicator is AUTHORITATIVE - if scanner says there's news,
            we trust it and apply boost. We do NOT re-infer or downgrade.
        """
        self._store.increment_scanner_count()

        # Check session date and auto-reset if needed
        if self._config.auto_reset_on_new_day and not self._store.check_session_date():
            await self._perform_session_reset()

        # Run scrutiny
        scrutiny_result = self._scrutinizer.check(
            symbol=symbol,
            price=price,
            volume=volume,
            rvol=rvol,
            scanner_score=scanner_score,
            spread_pct=spread_pct,
            is_halted=is_halted,
            data_timestamp=data_timestamp,
        )

        if not scrutiny_result.passed:
            # Emit rejection event
            await self._emit(
                EventType.WORKLIST_REJECTED,
                symbol=symbol,
                payload=scrutiny_result.to_event_payload(),
            )
            return None

        # Get existing entry for news score and alert count
        existing = self._store.get(symbol)
        news_score = existing.news_score if existing else None
        news_category = existing.news_category if existing else None
        alert_count = (existing.alert_count + 1) if existing else 1
        halt_status = "HALTED" if is_halted else (existing.halt_status if existing else None)

        # Determine news presence: scanner indicator is AUTHORITATIVE
        # If scanner says there's news, we trust it
        effective_news_present = (
            scanner_news_indicator or
            news_present or
            (existing.scanner_news_indicator if existing else False) or
            (existing.news_present if existing else False)
        )

        # Compute priority score with enhanced boosts
        combined_score = self._scorer.score(
            scanner_score=scanner_score,
            gap_pct=gap_pct,
            rvol=rvol,
            volume=volume,
            news_score=news_score,
            # Enhanced parameters
            rvol_5m=rvol_5m or rvol,  # Default to daily if 5m not available
            float_shares=float_shares,
            short_pct=short_pct,
            halt_status=halt_status,
            alert_count=alert_count,
            # Scanner news indicator (AUTHORITATIVE)
            scanner_news_indicator=scanner_news_indicator or (existing.scanner_news_indicator if existing else False),
            news_category=news_category,
        )

        # Classify momentum state (now returns 3 values including trigger_context)
        momentum_state, trigger_reason, trigger_context = classify_momentum_state(
            gap_pct=gap_pct,
            rvol=rvol,
            rvol_5m=rvol_5m or 0.0,
            short_pct=short_pct or 0.0,
            float_shares=float_shares or 0.0,
            halt_status=halt_status,
            alert_count=alert_count,
            news_present=effective_news_present,
        )

        now = datetime.now(timezone.utc).isoformat()

        if existing:
            # Update existing entry
            existing.scanner_score = scanner_score
            existing.gap_pct = gap_pct
            existing.rvol = rvol
            existing.rvol_5m = rvol_5m or existing.rvol_5m or 0.0
            existing.volume = volume
            existing.price = price
            existing.combined_priority_score = combined_score
            existing.last_update_ts = now
            existing.regime = self._current_regime
            existing.alert_count = alert_count
            existing.momentum_state = momentum_state
            existing.trigger_reason = trigger_reason
            existing.trigger_context = trigger_context
            # Update extended fields if provided
            if float_shares is not None:
                existing.float_shares = float_shares
            if avg_volume is not None:
                existing.avg_volume = avg_volume
            if short_pct is not None:
                existing.short_pct = short_pct
            if market_cap is not None:
                existing.market_cap = market_cap
            if spread_pct is not None:
                existing.spread_pct = spread_pct
            if halt_status:
                existing.halt_status = halt_status
            if halt_ts:
                existing.halt_ts = halt_ts
            # Scanner news indicator is AUTHORITATIVE - once set, stays set
            if scanner_news_indicator:
                existing.scanner_news_indicator = True
                existing.news_present = True
            elif news_present:
                existing.news_present = True

            self._store.add(existing)

            await self._emit(
                EventType.WORKLIST_UPDATED,
                symbol=symbol,
                payload={
                    "symbol": symbol,
                    "combined_score": combined_score,
                    "source": "scanner",
                    "momentum_state": momentum_state,
                    "alert_count": alert_count,
                },
            )

            return existing
        else:
            # Create new entry
            entry = WorklistEntry(
                symbol=symbol,
                session_date=self._store._session_date,
                first_seen_ts=now,
                last_update_ts=now,
                source=WorklistSource.SCANNER.value,
                scanner_score=scanner_score,
                gap_pct=gap_pct,
                rvol=rvol,
                rvol_5m=rvol_5m or 0.0,
                volume=volume,
                price=price,
                # Extended scanner data
                float_shares=float_shares,
                avg_volume=avg_volume,
                short_pct=short_pct,
                market_cap=market_cap,
                spread_pct=spread_pct,
                # Halt tracking
                halt_status=halt_status,
                halt_ts=halt_ts,
                # News (scanner indicator is AUTHORITATIVE)
                scanner_news_indicator=scanner_news_indicator,
                news_present=scanner_news_indicator or news_present,
                # Momentum classification
                momentum_state=momentum_state,
                trigger_context=trigger_context,
                trigger_reason=trigger_reason,
                alert_count=alert_count,
                # Scores
                combined_priority_score=combined_score,
                regime=self._current_regime,
                status=WorklistStatus.CANDIDATE.value,
            )

            self._store.add(entry)

            await self._emit(
                EventType.WORKLIST_ADDED,
                symbol=symbol,
                payload={
                    "symbol": symbol,
                    "combined_score": combined_score,
                    "source": "scanner",
                    "gap_pct": gap_pct,
                    "rvol": rvol,
                    "momentum_state": momentum_state,
                    "trigger_reason": trigger_reason,
                },
            )

            logger.info(
                f"[WORKLIST_PIPELINE] Added {symbol}: "
                f"score={combined_score:.1f}, gap={gap_pct:.1f}%, rvol={rvol:.1f}x, "
                f"state={momentum_state}"
            )

            return entry

    # =========================================================================
    # NEWS INTAKE (Deliverable 4)
    # =========================================================================

    async def process_news(
        self,
        symbol: str,
        news_score: float,
        headline: Optional[str] = None,
        news_timestamp: Optional[datetime] = None,
        news_category: Optional[str] = None,
    ) -> Optional[WorklistEntry]:
        """
        Process news data for a symbol.

        NEWS FEED ROLE (per instruction):
        - Context: attach headlines/categories to existing worklist symbols
        - Reinforcement: increment news_hits, extend freshness, re-score
        - Late Discovery: create entry if not exists (with neutral scanner defaults)

        Negative news categories (OFFERING, DILUTION, etc.) apply penalty.

        Args:
            symbol: Stock symbol
            news_score: News sentiment/importance score (0-100)
            headline: Optional headline text
            news_timestamp: When news was published
            news_category: Category for negative detection

        Returns:
            Updated or new WorklistEntry
        """
        self._store.increment_news_count()

        now = datetime.now(timezone.utc).isoformat()
        news_ts = news_timestamp.isoformat() if news_timestamp else now

        existing = self._store.get(symbol)

        if existing:
            # === REINFORCEMENT LOGIC ===
            # Increment news_hits, update timestamp, extend freshness
            existing.news_present = True
            existing.news_hits = (existing.news_hits or 0) + 1
            existing.news_score = news_score
            existing.news_headline = headline
            existing.news_category = news_category
            existing.news_ts = news_ts
            existing.last_update_ts = now  # Extend freshness

            # Re-score with news and all enhanced parameters
            combined_score = self._scorer.score(
                scanner_score=existing.scanner_score,
                gap_pct=existing.gap_pct,
                rvol=existing.rvol,
                volume=existing.volume,
                news_score=news_score,
                rvol_5m=existing.rvol_5m,
                float_shares=existing.float_shares,
                short_pct=existing.short_pct,
                halt_status=existing.halt_status,
                alert_count=existing.alert_count,
                scanner_news_indicator=existing.scanner_news_indicator,
                news_category=news_category,  # For negative penalty
            )
            existing.combined_priority_score = combined_score

            # Re-classify momentum state with news
            momentum_state, trigger_reason, trigger_context = classify_momentum_state(
                gap_pct=existing.gap_pct,
                rvol=existing.rvol,
                rvol_5m=existing.rvol_5m,
                short_pct=existing.short_pct or 0.0,
                float_shares=existing.float_shares or 0.0,
                halt_status=existing.halt_status,
                alert_count=existing.alert_count,
                news_present=True,
            )
            existing.momentum_state = momentum_state
            existing.trigger_reason = trigger_reason
            existing.trigger_context = trigger_context

            self._store.add(existing)

            # Log negative news detection
            if is_negative_news_category(news_category):
                logger.warning(
                    f"[WORKLIST_PIPELINE] NEGATIVE NEWS detected for {symbol}: "
                    f"category={news_category}, score capped"
                )

            await self._emit(
                EventType.WORKLIST_SCORED,
                symbol=symbol,
                payload={
                    "symbol": symbol,
                    "combined_score": combined_score,
                    "news_score": news_score,
                    "news_hits": existing.news_hits,
                    "momentum_state": momentum_state,
                    "negative_news": is_negative_news_category(news_category),
                    "reason": "news_reinforcement",
                },
            )

            logger.info(
                f"[WORKLIST_PIPELINE] News reinforcement {symbol}: "
                f"news_score={news_score:.1f}, hits={existing.news_hits}, "
                f"combined={combined_score:.1f}, state={momentum_state}"
            )

            return existing
        else:
            # === LATE DISCOVERY (Secondary) ===
            # Create entry with neutral scanner defaults, mark source = NEWS
            combined_score = self._scorer.score(
                scanner_score=50.0,  # Neutral scanner score
                gap_pct=0.0,
                rvol=1.0,
                volume=0,
                news_score=news_score,
                news_category=news_category,
            )

            # Classify as catalyst since it's news-driven
            momentum_state, trigger_reason, trigger_context = classify_momentum_state(
                gap_pct=0.0,
                rvol=1.0,
                news_present=True,
            )

            entry = WorklistEntry(
                symbol=symbol,
                session_date=self._store._session_date,
                first_seen_ts=now,
                last_update_ts=now,
                source=WorklistSource.NEWS.value,
                scanner_score=50.0,  # Placeholder - scanner will reinforce later
                news_present=True,
                news_score=news_score,
                news_headline=headline,
                news_category=news_category,
                news_ts=news_ts,
                news_hits=1,  # First hit
                momentum_state=momentum_state,
                trigger_context=trigger_context,
                trigger_reason=trigger_reason,
                alert_count=1,
                combined_priority_score=combined_score,
                regime=self._current_regime,
                status=WorklistStatus.CANDIDATE.value,
            )

            self._store.add(entry)

            await self._emit(
                EventType.WORKLIST_ADDED,
                symbol=symbol,
                payload={
                    "symbol": symbol,
                    "combined_score": combined_score,
                    "source": "news_late_discovery",
                    "news_score": news_score,
                    "momentum_state": momentum_state,
                    "negative_news": is_negative_news_category(news_category),
                },
            )

            logger.info(
                f"[WORKLIST_PIPELINE] Late discovery from NEWS {symbol}: "
                f"news_score={news_score:.1f}, combined={combined_score:.1f}, "
                f"category={news_category}"
            )

            return entry

    # =========================================================================
    # TRADING SELECTION (Deliverable 5)
    # =========================================================================

    def is_tradeable(self, symbol: str) -> bool:
        """
        Check if symbol is eligible for trading.

        CRITICAL: This is the ONLY way to verify trading eligibility.
        Trading without worklist approval is FORBIDDEN.

        Args:
            symbol: Symbol to check

        Returns:
            True if symbol can be traded
        """
        entry = self._store.get(symbol)
        if not entry:
            logger.warning(f"[WORKLIST_PIPELINE] Trade blocked: {symbol} not in worklist")
            return False

        if entry.session_date != self._store._session_date:
            logger.warning(f"[WORKLIST_PIPELINE] Trade blocked: {symbol} from old session")
            return False

        if entry.status not in (WorklistStatus.CANDIDATE.value, WorklistStatus.ACTIVE.value):
            logger.warning(f"[WORKLIST_PIPELINE] Trade blocked: {symbol} status={entry.status}")
            return False

        if entry.combined_priority_score < self._config.min_score_for_trading:
            logger.warning(
                f"[WORKLIST_PIPELINE] Trade blocked: {symbol} "
                f"score={entry.combined_priority_score:.1f} < {self._config.min_score_for_trading}"
            )
            return False

        return True

    async def select_for_trade(self, symbol: str) -> Optional[WorklistEntry]:
        """
        Select a symbol for trading.

        Marks symbol as ACTIVE and emits selection event.

        Args:
            symbol: Symbol to select

        Returns:
            WorklistEntry if selected, None if not eligible
        """
        if not self.is_tradeable(symbol):
            return None

        entry = self._store.get(symbol)
        if not entry:
            return None

        # Mark as active
        self._store.update_status(symbol, WorklistStatus.ACTIVE)

        await self._emit(
            EventType.WORKLIST_SELECTED,
            symbol=symbol,
            payload={
                "symbol": symbol,
                "combined_score": entry.combined_priority_score,
                "source": entry.source,
            },
        )

        logger.info(
            f"[WORKLIST_PIPELINE] Selected for trade: {symbol} "
            f"score={entry.combined_priority_score:.1f}"
        )

        return entry

    def get_top_candidates(
        self,
        n: int = 10,
        min_score: Optional[float] = None,
    ) -> list[WorklistEntry]:
        """
        Get top N trading candidates by score.

        Args:
            n: Number of candidates to return
            min_score: Minimum score (defaults to config)

        Returns:
            List of top candidates
        """
        threshold = min_score if min_score is not None else self._config.min_score_for_trading
        return self._store.get_top_n(n=n, min_score=threshold)

    async def mark_traded(self, symbol: str, result: Optional[str] = None) -> bool:
        """
        Mark symbol as traded.

        Args:
            symbol: Symbol that was traded
            result: Optional trade result description

        Returns:
            True if marked, False if not found
        """
        success = self._store.mark_traded(symbol, result)

        if success:
            await self._emit(
                EventType.WORKLIST_TRADED,
                symbol=symbol,
                payload={"symbol": symbol, "result": result},
            )

        return success

    # =========================================================================
    # SESSION MANAGEMENT (Deliverable 6)
    # =========================================================================

    async def reset_session(self) -> dict[str, Any]:
        """
        Reset worklist for new session.

        Archives old worklist and creates empty one for today.

        Returns:
            Summary of reset operation
        """
        return await self._perform_session_reset()

    async def _perform_session_reset(self) -> dict[str, Any]:
        """Internal session reset implementation."""
        summary = self._store.reset_session(archive=self._config.archive_old_sessions)

        # Reset scrutiny stats too
        self._scrutinizer.reset_stats()

        await self._emit(
            EventType.WORKLIST_RESET,
            payload={
                "session_date": summary["new_session"],
                "archived_count": summary["archived_count"],
                "old_session": summary["old_session"],
            },
        )

        logger.info(f"[WORKLIST_PIPELINE] Session reset: {summary}")

        return summary

    def check_session(self) -> bool:
        """Check if current session is valid."""
        return self._store.check_session_date()

    # =========================================================================
    # REGIME UPDATES
    # =========================================================================

    def set_regime(self, regime: str) -> None:
        """Update current regime for new entries."""
        self._current_regime = regime
        logger.debug(f"[WORKLIST_PIPELINE] Regime set to: {regime}")

    # =========================================================================
    # OBSERVABILITY (Deliverable 7)
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        """Get comprehensive pipeline status."""
        store_status = self._store.get_status()
        scrutiny_stats = self._scrutinizer.get_stats()

        return {
            "store": store_status,
            "scrutiny": scrutiny_stats,
            "config": {
                "min_score_for_trading": self._config.min_score_for_trading,
                "max_active_symbols": self._config.max_active_symbols,
                "auto_reset_on_new_day": self._config.auto_reset_on_new_day,
            },
            "current_regime": self._current_regime,
        }

    # =========================================================================
    # EVENT EMISSION
    # =========================================================================

    async def _emit(
        self,
        event_type: EventType,
        symbol: Optional[str] = None,
        payload: Optional[dict] = None,
    ) -> None:
        """Emit an event if callback is set."""
        if self._emit_event:
            event = create_event(
                event_type,
                symbol=symbol,
                payload=payload or {},
            )
            try:
                await self._emit_event(event)
            except Exception as e:
                logger.error(f"[WORKLIST_PIPELINE] Event emission error: {e}")


# Global pipeline instance
_global_pipeline: Optional[WorklistPipeline] = None


def get_worklist_pipeline(
    config: Optional[WorklistPipelineConfig] = None,
    emit_event: Optional[Callable[[Event], Awaitable[None]]] = None,
) -> WorklistPipeline:
    """Get or create global worklist pipeline."""
    global _global_pipeline
    if _global_pipeline is None:
        _global_pipeline = WorklistPipeline(config=config, emit_event=emit_event)
    return _global_pipeline


def reset_worklist_pipeline() -> None:
    """Reset global pipeline (for testing)."""
    global _global_pipeline
    _global_pipeline = None
