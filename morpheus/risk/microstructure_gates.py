"""
Market Microstructure Status Gates (Schwab-only)

Three gates that modify trading behavior based on market microstructure:

1. HALT_RECOVERY_GATE - Blocks entries during halts, manages post-halt cooldown
2. BORROWABILITY_GATE - Controls shorts based on ETB/HTB/NTB status
3. SSR_LIQUIDITY_GATE - Tightens entries and accelerates exits on SSR longs

Plus: DATA_STALE_GUARD - Prevents approval-then-block pattern by checking
staleness BEFORE risk approval (not just at execution).

Design Principle:
- SSR, HTB, HALT are LIQUIDITY REGIMES, not just flags
- Modify behavior early, exit earlier, never assume liquidity exists
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Awaitable

from morpheus.core.events import Event, EventType, create_event
from morpheus.core.symbol_state import (
    SymbolState,
    SymbolStateManager,
    SecurityStatus,
    BorrowStatus,
    MicrostructureConfig,
)
from morpheus.risk.base import VetoReason, RiskDecision
from morpheus.core.validation_mode import (
    is_validation_mode,
    get_validation_logger,
)

logger = logging.getLogger(__name__)


class GateDecision(str, Enum):
    """Gate decision outcomes."""
    ALLOW = "ALLOW"
    REJECT = "REJECT"
    MODIFY = "MODIFY"  # Allow with modifications (e.g., size reduction)


@dataclass
class GateResult:
    """Result from a microstructure gate check."""

    gate_name: str
    decision: GateDecision
    veto_reason: VetoReason | None = None
    reason_details: str = ""

    # Modifications to apply (if MODIFY decision)
    size_multiplier: float = 1.0  # e.g., 0.5 for 50% size reduction
    room_to_profit_relaxation: float = 0.0  # e.g., 0.15 for +15% threshold
    force_limit_order: bool = False
    force_day_only: bool = False
    disable_trailing: bool = False
    tags: list[str] = field(default_factory=list)

    evaluated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict[str, Any]:
        """Serialize for events."""
        return {
            "gate_name": self.gate_name,
            "decision": self.decision.value,
            "veto_reason": self.veto_reason.value if self.veto_reason else None,
            "reason_details": self.reason_details,
            "size_multiplier": self.size_multiplier,
            "room_to_profit_relaxation": self.room_to_profit_relaxation,
            "force_limit_order": self.force_limit_order,
            "force_day_only": self.force_day_only,
            "disable_trailing": self.disable_trailing,
            "tags": self.tags,
            "evaluated_at": self.evaluated_at.isoformat(),
        }


@dataclass
class MicrostructureResult:
    """Combined result from all microstructure gates."""

    symbol: str
    passed: bool
    gates_checked: list[str] = field(default_factory=list)
    gate_results: list[GateResult] = field(default_factory=list)

    # Aggregated modifications
    final_size_multiplier: float = 1.0
    final_room_to_profit_relaxation: float = 0.0
    force_limit_order: bool = False
    force_day_only: bool = False
    disable_trailing: bool = False
    all_tags: list[str] = field(default_factory=list)

    # Veto info if blocked
    veto_reason: VetoReason | None = None
    veto_details: str = ""

    evaluated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict[str, Any]:
        """Serialize for events."""
        return {
            "symbol": self.symbol,
            "passed": self.passed,
            "gates_checked": self.gates_checked,
            "gate_results": [g.to_dict() for g in self.gate_results],
            "final_size_multiplier": self.final_size_multiplier,
            "final_room_to_profit_relaxation": self.final_room_to_profit_relaxation,
            "force_limit_order": self.force_limit_order,
            "force_day_only": self.force_day_only,
            "disable_trailing": self.disable_trailing,
            "all_tags": self.all_tags,
            "veto_reason": self.veto_reason.value if self.veto_reason else None,
            "veto_details": self.veto_details,
            "evaluated_at": self.evaluated_at.isoformat(),
        }


class MicrostructureGates:
    """
    Evaluates all microstructure gates for a potential trade.

    Gate Order:
    1. DATA_STALE_GUARD - Is data fresh enough?
    2. HALT_RECOVERY_GATE - Is symbol halted or in cooldown?
    3. BORROWABILITY_GATE - Can we trade this direction?
    4. SSR_LIQUIDITY_GATE - Are entry conditions met for SSR?

    All gates must pass (or modify) for trade to proceed.
    """

    def __init__(
        self,
        state_manager: SymbolStateManager,
        config: MicrostructureConfig | None = None,
        emit_event: Callable[[Event], Awaitable[None]] | None = None,
    ):
        self._state_mgr = state_manager
        self._config = config or MicrostructureConfig()
        self._emit = emit_event

        # Track gate statistics for EOD report
        self._stats = {
            "data_stale_prevented": 0,
            "halt_blocked": 0,
            "post_halt_cooldown_blocked": 0,
            "post_halt_unstable_blocked": 0,
            "ntb_blocked": 0,
            "htb_market_short_blocked": 0,
            "htb_modified": 0,
            "ssr_blocked": 0,
            "ssr_modified": 0,
            "total_checked": 0,
            "total_passed": 0,
        }

        # Per-symbol tracking for EOD segmentation
        self._symbol_stats: dict[str, dict[str, int]] = {}

        logger.info("[MICROSTRUCTURE] Gates initialized")

    # ════════════════════════════════════════════════════════════════════
    # MAIN ENTRY POINT
    # ════════════════════════════════════════════════════════════════════

    async def check_entry(
        self,
        symbol: str,
        direction: str,  # "long" or "short"
        order_type: str = "LIMIT",  # "MARKET" or "LIMIT"
        time_in_force: str = "DAY",  # "DAY" or "GTC"
        momentum_slope: float | None = None,  # For SSR late extension check
        is_late_extension: bool = False,
        spread_pct: float | None = None,
        volume_expanding: bool = True,
    ) -> MicrostructureResult:
        """
        Run all microstructure gate checks for a potential entry.

        Returns MicrostructureResult with:
        - passed: True if all gates allow (possibly with modifications)
        - gate_results: Individual gate outcomes
        - Aggregated modifications to apply
        - veto_reason if blocked
        """
        self._stats["total_checked"] += 1
        self._ensure_symbol_stats(symbol)
        self._symbol_stats[symbol]["checked"] += 1

        result = MicrostructureResult(symbol=symbol, passed=True)

        # Get symbol state
        state = self._state_mgr.get_or_create(symbol)

        # 1. DATA_STALE_GUARD
        stale_result = self._check_data_stale(symbol, state)
        result.gate_results.append(stale_result)
        result.gates_checked.append("DATA_STALE_GUARD")

        # Validation logging for data freshness
        if is_validation_mode():
            val_logger = get_validation_logger()
            if val_logger:
                val_logger.log_data_fresh(symbol, state.data_stale)

        if stale_result.decision == GateDecision.REJECT:
            result.passed = False
            result.veto_reason = stale_result.veto_reason
            result.veto_details = stale_result.reason_details
            self._stats["data_stale_prevented"] += 1
            self._symbol_stats[symbol]["data_stale_prevented"] += 1
            await self._emit_gate_decision(symbol, stale_result)
            return result

        # 2. HALT_RECOVERY_GATE
        halt_result = self._check_halt_recovery(symbol, state)
        result.gate_results.append(halt_result)
        result.gates_checked.append("HALT_RECOVERY_GATE")

        # Validation logging for halt blocking
        if is_validation_mode():
            val_logger = get_validation_logger()
            if val_logger:
                val_logger.log_halt_block(symbol, state.halted, state.halt_code)

        if halt_result.decision == GateDecision.REJECT:
            result.passed = False
            result.veto_reason = halt_result.veto_reason
            result.veto_details = halt_result.reason_details

            if halt_result.veto_reason == VetoReason.HALT_ACTIVE:
                self._stats["halt_blocked"] += 1
                self._symbol_stats[symbol]["halt_blocked"] += 1
            elif halt_result.veto_reason == VetoReason.POST_HALT_COOLDOWN:
                self._stats["post_halt_cooldown_blocked"] += 1
            elif halt_result.veto_reason == VetoReason.POST_HALT_UNSTABLE:
                self._stats["post_halt_unstable_blocked"] += 1

            await self._emit_gate_decision(symbol, halt_result)
            return result

        # 3. BORROWABILITY_GATE (for shorts only)
        if direction == "short":
            borrow_result = self._check_borrowability(
                symbol, state, order_type, time_in_force
            )
            result.gate_results.append(borrow_result)
            result.gates_checked.append("BORROWABILITY_GATE")

            if borrow_result.decision == GateDecision.REJECT:
                result.passed = False
                result.veto_reason = borrow_result.veto_reason
                result.veto_details = borrow_result.reason_details

                # Validation logging for HTB short block
                if is_validation_mode():
                    val_logger = get_validation_logger()
                    if val_logger:
                        val_logger.log_htb_behavior(
                            symbol, direction,
                            htb=state.borrow_status == BorrowStatus.HTB,
                            blocked=True,
                        )

                if borrow_result.veto_reason == VetoReason.NTB_BLOCK_SHORT:
                    self._stats["ntb_blocked"] += 1
                    self._symbol_stats[symbol]["ntb_blocked"] += 1
                else:
                    self._stats["htb_market_short_blocked"] += 1
                    self._symbol_stats[symbol]["htb_blocked"] += 1

                await self._emit_gate_decision(symbol, borrow_result)
                return result

            # Apply modifications from borrow gate
            if borrow_result.decision == GateDecision.MODIFY:
                self._stats["htb_modified"] += 1
                result.final_size_multiplier *= borrow_result.size_multiplier
                result.force_limit_order = result.force_limit_order or borrow_result.force_limit_order
                result.force_day_only = result.force_day_only or borrow_result.force_day_only
                result.all_tags.extend(borrow_result.tags)

                # Validation logging for HTB modification
                if is_validation_mode():
                    val_logger = get_validation_logger()
                    if val_logger:
                        val_logger.log_htb_behavior(
                            symbol, direction,
                            htb=True,
                            size_reduced=borrow_result.size_multiplier < 1.0,
                        )

        # 4. BORROWABILITY_GATE for longs on HTB (capitalize on squeeze)
        if direction == "long" and state.borrow_status == BorrowStatus.HTB:
            htb_long_result = self._check_htb_long(symbol, state, is_late_extension)
            result.gate_results.append(htb_long_result)
            result.gates_checked.append("BORROWABILITY_GATE_LONG")

            if htb_long_result.decision == GateDecision.MODIFY:
                self._stats["htb_modified"] += 1
                result.final_room_to_profit_relaxation = max(
                    result.final_room_to_profit_relaxation,
                    htb_long_result.room_to_profit_relaxation
                )
                result.all_tags.extend(htb_long_result.tags)

                # Validation logging for HTB long modification
                if is_validation_mode():
                    val_logger = get_validation_logger()
                    if val_logger:
                        val_logger.log_htb_behavior(symbol, direction, htb=True)

        # 5. SSR_LIQUIDITY_GATE (for longs on SSR stocks)
        if direction == "long" and state.ssr_active:
            ssr_result = self._check_ssr_entry(
                symbol, state, momentum_slope, is_late_extension, spread_pct, volume_expanding
            )
            result.gate_results.append(ssr_result)
            result.gates_checked.append("SSR_LIQUIDITY_GATE")

            if ssr_result.decision == GateDecision.REJECT:
                result.passed = False
                result.veto_reason = ssr_result.veto_reason
                result.veto_details = ssr_result.reason_details
                self._stats["ssr_blocked"] += 1
                self._symbol_stats[symbol]["ssr_blocked"] += 1
                await self._emit_gate_decision(symbol, ssr_result)
                return result

            if ssr_result.decision == GateDecision.MODIFY:
                self._stats["ssr_modified"] += 1
                result.disable_trailing = True
                result.all_tags.extend(ssr_result.tags)

        # All gates passed
        self._stats["total_passed"] += 1
        self._symbol_stats[symbol]["passed"] += 1

        return result

    # ════════════════════════════════════════════════════════════════════
    # GATE 1: DATA STALE GUARD
    # ════════════════════════════════════════════════════════════════════

    def _check_data_stale(self, symbol: str, state: SymbolState) -> GateResult:
        """
        Check if symbol data is fresh enough for trading.

        CRITICAL: This fixes the "approval then block" pattern from EOD.
        By checking staleness here (in risk gate), we prevent signals from
        being approved only to be blocked at execution.
        """
        is_stale = self._state_mgr.check_staleness(symbol)

        if is_stale:
            age = 0
            if state.last_quote_ts:
                age = time.time() - state.last_quote_ts

            return GateResult(
                gate_name="DATA_STALE_GUARD",
                decision=GateDecision.REJECT,
                veto_reason=VetoReason.DATA_STALE_PREVENTED,
                reason_details=f"Quote data {age:.1f}s old (max {self._config.stale_quote_seconds}s)",
            )

        return GateResult(
            gate_name="DATA_STALE_GUARD",
            decision=GateDecision.ALLOW,
            reason_details="Data fresh",
        )

    # ════════════════════════════════════════════════════════════════════
    # GATE 2: HALT RECOVERY GATE
    # ════════════════════════════════════════════════════════════════════

    def _check_halt_recovery(self, symbol: str, state: SymbolState) -> GateResult:
        """
        Check halt status and post-halt stability.

        Blocks:
        - When halted
        - During post-halt cooldown
        - If post-halt stability checks fail
        """
        # Check if halted
        if state.halted or state.security_status == SecurityStatus.HALTED:
            return GateResult(
                gate_name="HALT_RECOVERY_GATE",
                decision=GateDecision.REJECT,
                veto_reason=VetoReason.HALT_ACTIVE,
                reason_details=f"Symbol halted (code={state.halt_code})",
            )

        # Check post-halt cooldown
        if self._state_mgr.is_in_post_halt_cooldown(symbol):
            remaining = (state.post_halt_cooldown_until or 0) - time.time()
            return GateResult(
                gate_name="HALT_RECOVERY_GATE",
                decision=GateDecision.REJECT,
                veto_reason=VetoReason.POST_HALT_COOLDOWN,
                reason_details=f"Post-halt cooldown ({remaining:.0f}s remaining)",
            )

        # TODO: Add post-halt stability checks
        # - spread < POST_HALT_MAX_SPREAD
        # - min prints since resume >= POST_HALT_MIN_PRINTS
        # - bid/ask not rapidly flipping

        return GateResult(
            gate_name="HALT_RECOVERY_GATE",
            decision=GateDecision.ALLOW,
            reason_details="No halt restrictions",
        )

    # ════════════════════════════════════════════════════════════════════
    # GATE 3: BORROWABILITY GATE (SHORTS)
    # ════════════════════════════════════════════════════════════════════

    def _check_borrowability(
        self,
        symbol: str,
        state: SymbolState,
        order_type: str,
        time_in_force: str,
    ) -> GateResult:
        """
        Check borrowability constraints for SHORT entries.

        NTB: Block all shorts
        HTB: Block MARKET, block GTC, require LIMIT DAY only, reduce size
        ETB: Allow all
        """
        # NTB - block all shorts
        if state.borrow_status == BorrowStatus.NTB:
            return GateResult(
                gate_name="BORROWABILITY_GATE",
                decision=GateDecision.REJECT,
                veto_reason=VetoReason.NTB_BLOCK_SHORT,
                reason_details="Not borrowable - cannot short",
            )

        # HTB - restrict shorts
        if state.borrow_status == BorrowStatus.HTB:
            # Block MARKET orders
            if order_type.upper() == "MARKET":
                return GateResult(
                    gate_name="BORROWABILITY_GATE",
                    decision=GateDecision.REJECT,
                    veto_reason=VetoReason.HTB_BLOCK_MARKET_SHORT,
                    reason_details="HTB - market shorts blocked, use limit",
                )

            # Block GTC orders
            if time_in_force.upper() == "GTC":
                return GateResult(
                    gate_name="BORROWABILITY_GATE",
                    decision=GateDecision.REJECT,
                    veto_reason=VetoReason.HTB_BLOCK_GTC_SHORT,
                    reason_details="HTB - GTC shorts blocked, use DAY only",
                )

            # Allow with modifications
            return GateResult(
                gate_name="BORROWABILITY_GATE",
                decision=GateDecision.MODIFY,
                reason_details="HTB - short allowed with restrictions",
                size_multiplier=1 - self._config.htb_short_size_haircut,
                force_limit_order=True,
                force_day_only=True,
                tags=["HTB_SHORT", "SQUEEZE_RISK"],
            )

        # ETB or UNKNOWN - allow
        return GateResult(
            gate_name="BORROWABILITY_GATE",
            decision=GateDecision.ALLOW,
            reason_details=f"Borrow status: {state.borrow_status.value}",
        )

    # ════════════════════════════════════════════════════════════════════
    # GATE 3b: BORROWABILITY GATE (LONGS ON HTB - SQUEEZE CAPITALIZE)
    # ════════════════════════════════════════════════════════════════════

    def _check_htb_long(
        self,
        symbol: str,
        state: SymbolState,
        is_late_extension: bool,
    ) -> GateResult:
        """
        Adjust long entries on HTB stocks.

        HTB longs can benefit from squeeze dynamics but need:
        - Early momentum entries preferred
        - Faster profit-taking
        - Relaxed room-to-profit threshold (HTB names move faster)
        """
        # Block late extensions on HTB
        if is_late_extension:
            return GateResult(
                gate_name="BORROWABILITY_GATE_LONG",
                decision=GateDecision.REJECT,
                veto_reason=VetoReason.SSR_LATE_EXTENSION,  # Reuse for HTB
                reason_details="HTB - late extension blocked, prefer early momentum",
            )

        # Modify entry for squeeze capitalization
        return GateResult(
            gate_name="BORROWABILITY_GATE_LONG",
            decision=GateDecision.MODIFY,
            reason_details="HTB long - squeeze mode, faster exits",
            room_to_profit_relaxation=self._config.htb_room_to_profit_relaxation,
            tags=["HTB_LONG", "SQUEEZE_RISK_ON", "FAST_PROFIT_TAKE"],
        )

    # ════════════════════════════════════════════════════════════════════
    # GATE 4: SSR LIQUIDITY GATE (LONGS)
    # ════════════════════════════════════════════════════════════════════

    def _check_ssr_entry(
        self,
        symbol: str,
        state: SymbolState,
        momentum_slope: float | None,
        is_late_extension: bool,
        spread_pct: float | None,
        volume_expanding: bool,
    ) -> GateResult:
        """
        Check SSR entry conditions for LONG positions.

        SSR creates artificial bid support from uptick shorts.
        When this support vanishes, price can collapse ("jackknife").

        Entry requirements:
        - NOT flattening momentum
        - NOT late extension
        - Spread below threshold
        - Volume expanding
        """
        rejection_reasons = []

        # Check momentum slope flattening
        if momentum_slope is not None and momentum_slope < 0:
            rejection_reasons.append(f"momentum_flattening ({momentum_slope:.3f})")

        # Check late extension
        if is_late_extension:
            rejection_reasons.append("late_extension")

        # Check spread
        if spread_pct is not None and spread_pct > self._config.ssr_spread_threshold_pct:
            rejection_reasons.append(
                f"spread_wide ({spread_pct:.2%} > {self._config.ssr_spread_threshold_pct:.2%})"
            )

        # Check volume
        if not volume_expanding:
            rejection_reasons.append("volume_not_expanding")

        # Must have expanding momentum AND expanding volume
        if rejection_reasons:
            return GateResult(
                gate_name="SSR_LIQUIDITY_GATE",
                decision=GateDecision.REJECT,
                veto_reason=VetoReason.SSR_ENTRY_TIGHTENED,
                reason_details=f"SSR entry rejected: {', '.join(rejection_reasons)}",
            )

        # Allow with modifications (disable trailing, early exits)
        return GateResult(
            gate_name="SSR_LIQUIDITY_GATE",
            decision=GateDecision.MODIFY,
            reason_details="SSR entry allowed - disable trailing, early exit on stall",
            disable_trailing=True,
            tags=["SSR_ACTIVE", "EXIT_ON_STALL", "NO_TRAILING"],
        )

    # ════════════════════════════════════════════════════════════════════
    # SSR EXIT MONITORING
    # ════════════════════════════════════════════════════════════════════

    def check_ssr_exit(
        self,
        symbol: str,
        state: SymbolState,
        velocity_roc: float | None = None,
        spread_pct: float | None = None,
        avg_spread_pct: float | None = None,
        consecutive_rejections: int = 0,
        seconds_since_new_high: float = 0,
    ) -> tuple[bool, str]:
        """
        Check if SSR long position should exit.

        Exit triggers (ANY one triggers):
        - Velocity/ROC decay below threshold
        - Consecutive uptick rejections
        - Spread widening (doubles)
        - Time-based failsafe (no new high in X seconds)

        Returns: (should_exit, exit_reason)
        """
        if not state.ssr_active:
            return False, ""

        # 1. Velocity decay
        if velocity_roc is not None:
            if velocity_roc < self._config.ssr_exit_velocity_decay_threshold:
                return True, f"SSR_MOMENTUM_DECAY (ROC={velocity_roc:.3f})"

        # 2. Consecutive rejections (failed continuation)
        if consecutive_rejections >= self._config.ssr_exit_consecutive_rejections:
            return True, f"SSR_UPTICK_EXHAUSTION ({consecutive_rejections} rejections)"

        # 3. Spread widening
        if spread_pct is not None and avg_spread_pct is not None and avg_spread_pct > 0:
            if spread_pct > avg_spread_pct * self._config.ssr_exit_spread_widen_multiplier:
                return True, f"SSR_SPREAD_BLOWOUT ({spread_pct:.2%} vs avg {avg_spread_pct:.2%})"

        # 4. Time-based failsafe
        if seconds_since_new_high >= self._config.ssr_max_hold_no_new_high_seconds:
            return True, f"SSR_TIME_FAILSAFE ({seconds_since_new_high:.0f}s no new high)"

        return False, ""

    # ════════════════════════════════════════════════════════════════════
    # EVENT EMISSION
    # ════════════════════════════════════════════════════════════════════

    async def _emit_gate_decision(self, symbol: str, gate_result: GateResult) -> None:
        """Emit GATE_DECISION event."""
        if not self._emit:
            return

        await self._emit(create_event(
            EventType.GATE_DECISION,
            payload={
                "gate": gate_result.gate_name,
                "decision": gate_result.decision.value,
                "reason": gate_result.veto_reason.value if gate_result.veto_reason else None,
                "details": gate_result.reason_details,
            },
            symbol=symbol,
        ))

    async def emit_status_update(self, symbol: str, state: SymbolState) -> None:
        """Emit SYMBOL_STATUS_UPDATE event."""
        if not self._emit:
            return

        await self._emit(create_event(
            EventType.SYMBOL_STATUS_UPDATE,
            payload={
                "security_status": state.security_status.value,
                "halted": state.halted,
                "borrow_status": state.borrow_status.value,
                "ssr_active": state.ssr_active,
                "data_stale": state.data_stale,
            },
            symbol=symbol,
        ))

    # ════════════════════════════════════════════════════════════════════
    # STATISTICS
    # ════════════════════════════════════════════════════════════════════

    def _ensure_symbol_stats(self, symbol: str) -> None:
        """Ensure symbol stats dict exists."""
        if symbol not in self._symbol_stats:
            self._symbol_stats[symbol] = {
                "checked": 0,
                "passed": 0,
                "data_stale_prevented": 0,
                "halt_blocked": 0,
                "htb_blocked": 0,
                "ntb_blocked": 0,
                "ssr_blocked": 0,
            }

    def get_stats(self) -> dict[str, Any]:
        """Get gate statistics for EOD report."""
        return {
            "global": dict(self._stats),
            "by_symbol": dict(self._symbol_stats),
        }

    def get_eod_segments(self) -> dict[str, Any]:
        """
        Get EOD report segmentation by status.

        Returns metrics segmented by:
        - SSR symbols
        - HTB symbols
        - Halt-affected symbols
        - Data-stale prevented entries
        """
        ssr_symbols = self._state_mgr.get_ssr_symbols()
        htb_symbols = self._state_mgr.get_htb_symbols()
        halted_symbols = self._state_mgr.get_halted_symbols()
        stale_symbols = self._state_mgr.get_stale_symbols()

        return {
            "ssr": {
                "symbols": ssr_symbols,
                "count": len(ssr_symbols),
                "blocked": sum(
                    self._symbol_stats.get(s, {}).get("ssr_blocked", 0)
                    for s in ssr_symbols
                ),
            },
            "htb": {
                "symbols": htb_symbols,
                "count": len(htb_symbols),
                "blocked": sum(
                    self._symbol_stats.get(s, {}).get("htb_blocked", 0) +
                    self._symbol_stats.get(s, {}).get("ntb_blocked", 0)
                    for s in htb_symbols
                ),
            },
            "halt_affected": {
                "symbols": halted_symbols,
                "count": len(halted_symbols),
                "blocked": self._stats["halt_blocked"],
            },
            "data_stale_prevented": {
                "count": self._stats["data_stale_prevented"],
                "symbols": [
                    s for s, stats in self._symbol_stats.items()
                    if stats.get("data_stale_prevented", 0) > 0
                ],
            },
        }

    def reset_daily(self) -> None:
        """Reset statistics for new day."""
        for key in self._stats:
            self._stats[key] = 0
        self._symbol_stats.clear()
        logger.info("[MICROSTRUCTURE] Daily stats reset")
