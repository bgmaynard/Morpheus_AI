"""
Phase 7 Tests - Execution Guard & Order Routing.

Tests for:
- Execution guards (spread/slippage/liquidity checks)
- Order routing (order construction, idempotency)
- Broker adapters (paper trading, live safety)
- Event emission
- Determinism
- Phase isolation
- Upstream respect (no override of risk/meta decisions)
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from morpheus.core.events import EventType
from morpheus.data.market_snapshot import create_snapshot, MarketSnapshot
from morpheus.risk.base import (
    RiskResult,
    RiskDecision,
    PositionSize,
    AccountState,
)
from morpheus.scoring.base import (
    GateResult,
    GateDecision,
    ScoredSignal,
)
from morpheus.strategies.base import SignalCandidate, SignalDirection
from morpheus.execution import (
    EXECUTION_SCHEMA_VERSION,
    TradingMode,
    ExecutionDecision,
    BlockReason,
    OrderType,
    OrderSide,
    OrderStatus,
    generate_order_intent_id,
    generate_client_order_id,
    ExecutionCheck,
    OrderRequest,
    OrderResult,
    ExecutionConfig,
    GuardConfig,
    StandardExecutionGuard,
    PermissiveExecutionGuard,
    StrictExecutionGuard,
    create_standard_guard,
    create_permissive_guard,
    create_strict_guard,
    RouterConfig,
    StandardOrderRouter,
    MarketOrderRouter,
    LimitOrderRouter,
    create_standard_router,
    create_market_router,
    create_limit_router,
    PaperBrokerAdapter,
    create_paper_adapter,
)


# ============================================================================
# Test Fixtures
# ============================================================================


def create_test_snapshot(
    symbol: str = "AAPL",
    bid: float = 149.95,
    ask: float = 150.05,
    last: float = 150.00,
    volume: int = 10000000,
    is_market_open: bool = True,
    is_tradeable: bool = True,
) -> MarketSnapshot:
    """Create a MarketSnapshot for testing."""
    return create_snapshot(
        symbol=symbol,
        bid=bid,
        ask=ask,
        last=last,
        volume=volume,
        timestamp=datetime.now(timezone.utc),
        source="test",
        is_market_open=is_market_open,
        is_tradeable=is_tradeable,
    )


def create_test_signal(
    symbol: str = "AAPL",
    direction: SignalDirection = SignalDirection.LONG,
) -> SignalCandidate:
    """Create a SignalCandidate for testing."""
    return SignalCandidate(
        symbol=symbol,
        direction=direction,
        strategy_name="test_strategy",
        triggering_features=("rsi_14",),
        entry_reference=150.0,
        tags=("momentum",),
        timestamp=datetime.now(timezone.utc),
    )


def create_test_scored_signal(
    symbol: str = "AAPL",
    direction: SignalDirection = SignalDirection.LONG,
    confidence: float = 0.7,
) -> ScoredSignal:
    """Create a ScoredSignal for testing."""
    signal = create_test_signal(symbol, direction)
    return ScoredSignal(
        signal=signal,
        confidence=confidence,
        model_name="test_scorer",
        model_version="1.0.0",
        score_rationale="Test scoring",
        contributing_factors=("trend",),
        feature_snapshot={"atr_pct": 1.5},
    )


def create_test_gate_result(
    symbol: str = "AAPL",
    decision: GateDecision = GateDecision.APPROVED,
) -> GateResult:
    """Create a GateResult for testing."""
    scored_signal = create_test_scored_signal(symbol)
    return GateResult(
        scored_signal=scored_signal,
        decision=decision,
        reasons=(),
        gate_name="test_gate",
        gate_version="1.0.0",
        min_confidence_required=0.5,
        confidence_met=True,
    )


def create_test_position_size(
    shares: int = 100,
    entry_price: Decimal = Decimal("150.00"),
    stop_price: Decimal | None = Decimal("147.00"),
) -> PositionSize:
    """Create a PositionSize for testing."""
    return PositionSize(
        shares=shares,
        notional_value=Decimal(shares) * entry_price,
        entry_price=entry_price,
        stop_price=stop_price,
        risk_per_share=abs(entry_price - stop_price) if stop_price else Decimal("0"),
        position_pct_of_account=0.05,
        risk_pct_of_account=0.01,
        sizer_name="test_sizer",
        sizer_version="1.0.0",
    )


def create_test_risk_result(
    symbol: str = "AAPL",
    decision: RiskDecision = RiskDecision.APPROVED,
    shares: int = 100,
) -> RiskResult:
    """Create a RiskResult for testing."""
    gate_result = create_test_gate_result(symbol)
    position_size = create_test_position_size(shares)
    account = AccountState(
        total_equity=Decimal("100000"),
        buying_power=Decimal("200000"),
    )
    return RiskResult(
        gate_result=gate_result,
        decision=decision,
        position_size=position_size,
        account_snapshot=account,
        overlay_name="test_overlay",
        overlay_version="1.0.0",
    )


def create_test_config(
    trading_mode: TradingMode = TradingMode.PAPER,
    live_armed: bool = False,
) -> ExecutionConfig:
    """Create an ExecutionConfig for testing."""
    return ExecutionConfig(
        trading_mode=trading_mode,
        live_trading_armed=live_armed,
        max_spread_pct=0.5,
        max_slippage_pct=0.5,
        min_volume=1000,
    )


# ============================================================================
# Execution Check Tests
# ============================================================================


class TestExecutionCheck:
    """Tests for ExecutionCheck dataclass."""

    def test_execution_check_immutable(self):
        """ExecutionCheck should be immutable."""
        check = ExecutionCheck(decision=ExecutionDecision.EXECUTE)
        with pytest.raises(Exception):
            check.decision = ExecutionDecision.BLOCK

    def test_execution_check_to_payload(self):
        """ExecutionCheck should serialize to payload."""
        check = ExecutionCheck(
            decision=ExecutionDecision.BLOCK,
            block_reasons=(BlockReason.SPREAD_TOO_WIDE,),
            spread_pct=0.8,
            guard_name="test_guard",
        )
        payload = check.to_event_payload()

        assert payload["decision"] == "block"
        assert "spread_too_wide" in payload["block_reasons"]
        assert payload["spread_pct"] == 0.8

    def test_is_executable_property(self):
        """is_executable should return True for EXECUTE decision."""
        executable = ExecutionCheck(decision=ExecutionDecision.EXECUTE)
        blocked = ExecutionCheck(decision=ExecutionDecision.BLOCK)

        assert executable.is_executable
        assert not executable.is_blocked
        assert blocked.is_blocked
        assert not blocked.is_executable


# ============================================================================
# Order Request Tests
# ============================================================================


class TestOrderRequest:
    """Tests for OrderRequest dataclass."""

    def test_order_request_immutable(self):
        """OrderRequest should be immutable."""
        request = OrderRequest(symbol="AAPL", quantity=100)
        with pytest.raises(Exception):
            request.quantity = 200

    def test_order_request_to_payload(self):
        """OrderRequest should serialize to payload."""
        request = OrderRequest(
            order_intent_id="intent_abc123",
            client_order_id="morph_abc123",
            symbol="AAPL",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=100,
            limit_price=Decimal("150.00"),
        )
        payload = request.to_event_payload()

        assert payload["order_intent_id"] == "intent_abc123"
        assert payload["client_order_id"] == "morph_abc123"
        assert payload["symbol"] == "AAPL"
        assert payload["side"] == "buy"
        assert payload["quantity"] == 100


# ============================================================================
# Order Result Tests
# ============================================================================


class TestOrderResult:
    """Tests for OrderResult dataclass."""

    def test_order_result_immutable(self):
        """OrderResult should be immutable."""
        result = OrderResult(status=OrderStatus.FILLED)
        with pytest.raises(Exception):
            result.status = OrderStatus.CANCELLED

    def test_order_result_to_event_filled(self):
        """Filled OrderResult should emit ORDER_FILL_RECEIVED event."""
        result = OrderResult(
            status=OrderStatus.FILLED,
            symbol="AAPL",
            filled_quantity=100,
            average_fill_price=Decimal("150.00"),
        )
        event = result.to_event()
        assert event.event_type == EventType.ORDER_FILL_RECEIVED

    def test_order_result_to_event_rejected(self):
        """Rejected OrderResult should emit ORDER_REJECTED event."""
        result = OrderResult(status=OrderStatus.REJECTED, symbol="AAPL")
        event = result.to_event()
        assert event.event_type == EventType.ORDER_REJECTED

    def test_order_result_to_event_unknown(self):
        """Unknown OrderResult should emit ORDER_STATUS_UNKNOWN event."""
        result = OrderResult(status=OrderStatus.UNKNOWN, symbol="AAPL")
        event = result.to_event()
        assert event.event_type == EventType.ORDER_STATUS_UNKNOWN

    def test_is_terminal_property(self):
        """is_terminal should identify terminal states."""
        filled = OrderResult(status=OrderStatus.FILLED)
        rejected = OrderResult(status=OrderStatus.REJECTED)
        working = OrderResult(status=OrderStatus.WORKING)

        assert filled.is_terminal
        assert rejected.is_terminal
        assert not working.is_terminal


# ============================================================================
# Standard Execution Guard Tests
# ============================================================================


class TestStandardExecutionGuard:
    """Tests for StandardExecutionGuard."""

    def test_allows_good_conditions(self):
        """Guard should allow execution with good market conditions."""
        guard = StandardExecutionGuard()
        risk_result = create_test_risk_result()
        snapshot = create_test_snapshot(
            bid=149.95,
            ask=150.05,  # 0.067% spread
            volume=10000000,
        )
        config = create_test_config()

        check = guard.check(risk_result, snapshot, config)

        assert check.is_executable
        assert len(check.block_reasons) == 0

    def test_blocks_wide_spread(self):
        """Guard should block when spread is too wide."""
        guard = StandardExecutionGuard(GuardConfig(max_spread_pct=0.1))
        risk_result = create_test_risk_result()
        snapshot = create_test_snapshot(
            bid=149.00,
            ask=151.00,  # ~1.33% spread
            volume=10000000,
        )
        config = create_test_config()

        check = guard.check(risk_result, snapshot, config)

        assert check.is_blocked
        assert BlockReason.SPREAD_TOO_WIDE in check.block_reasons

    def test_blocks_low_liquidity(self):
        """Guard should block when volume is too low."""
        guard = StandardExecutionGuard(GuardConfig(min_volume=100000))
        risk_result = create_test_risk_result()
        snapshot = create_test_snapshot(volume=500)  # Very low
        config = create_test_config()

        check = guard.check(risk_result, snapshot, config)

        assert check.is_blocked
        assert BlockReason.LOW_LIQUIDITY in check.block_reasons

    def test_blocks_market_closed(self):
        """Guard should block when market is closed."""
        guard = StandardExecutionGuard(GuardConfig(require_market_open=True))
        risk_result = create_test_risk_result()
        snapshot = create_test_snapshot(is_market_open=False)
        config = create_test_config()

        check = guard.check(risk_result, snapshot, config)

        assert check.is_blocked
        assert BlockReason.MARKET_CLOSED in check.block_reasons

    def test_blocks_symbol_halted(self):
        """Guard should block when symbol is halted."""
        guard = StandardExecutionGuard()
        risk_result = create_test_risk_result()
        snapshot = create_test_snapshot(is_tradeable=False)
        config = create_test_config()

        check = guard.check(risk_result, snapshot, config)

        assert check.is_blocked
        assert BlockReason.SYMBOL_HALTED in check.block_reasons

    def test_deterministic_same_inputs(self):
        """Guard should be deterministic - same inputs = same output."""
        guard = StandardExecutionGuard()
        risk_result = create_test_risk_result()
        snapshot = create_test_snapshot()
        config = create_test_config()

        check1 = guard.check(risk_result, snapshot, config)
        check2 = guard.check(risk_result, snapshot, config)

        assert check1.decision == check2.decision
        assert check1.block_reasons == check2.block_reasons
        assert check1.spread_pct == check2.spread_pct


class TestPermissiveExecutionGuard:
    """Tests for PermissiveExecutionGuard."""

    def test_allows_wide_spread(self):
        """Permissive guard should allow wide spreads."""
        guard = PermissiveExecutionGuard()
        risk_result = create_test_risk_result()
        snapshot = create_test_snapshot(bid=140.00, ask=160.00)  # 13% spread
        config = create_test_config()

        check = guard.check(risk_result, snapshot, config)

        assert check.is_executable

    def test_still_blocks_halted(self):
        """Permissive guard should still block halted symbols."""
        guard = PermissiveExecutionGuard()
        risk_result = create_test_risk_result()
        snapshot = create_test_snapshot(is_tradeable=False)
        config = create_test_config()

        check = guard.check(risk_result, snapshot, config)

        assert check.is_blocked
        assert BlockReason.SYMBOL_HALTED in check.block_reasons


class TestStrictExecutionGuard:
    """Tests for StrictExecutionGuard."""

    def test_blocks_moderate_spread(self):
        """Strict guard should block even moderate spreads."""
        guard = StrictExecutionGuard()
        risk_result = create_test_risk_result()
        snapshot = create_test_snapshot(
            bid=149.50,
            ask=150.50,  # ~0.67% spread - above strict 0.2%
        )
        config = create_test_config()

        check = guard.check(risk_result, snapshot, config)

        assert check.is_blocked
        assert BlockReason.SPREAD_TOO_WIDE in check.block_reasons


# ============================================================================
# Order Router Tests
# ============================================================================


class TestStandardOrderRouter:
    """Tests for StandardOrderRouter."""

    def test_creates_order_with_correct_fields(self):
        """Router should create order with all required fields."""
        router = StandardOrderRouter()
        risk_result = create_test_risk_result()
        snapshot = create_test_snapshot()
        config = create_test_config()

        order = router.route(risk_result, snapshot, config)

        assert order.symbol == "AAPL"
        assert order.side == OrderSide.BUY  # LONG signal
        assert order.quantity == 100
        assert order.order_intent_id != ""
        assert order.client_order_id != ""

    def test_idempotent_intent_id(self):
        """Router should generate same intent ID for same inputs."""
        router = StandardOrderRouter()
        risk_result = create_test_risk_result()
        snapshot = create_test_snapshot()
        config = create_test_config()

        order1 = router.route(risk_result, snapshot, config)
        order2 = router.route(risk_result, snapshot, config)

        # Same risk result should produce same intent ID
        assert order1.order_intent_id == order2.order_intent_id
        assert order1.client_order_id == order2.client_order_id

    def test_client_order_id_derived_from_intent(self):
        """Client order ID should be derived from intent ID."""
        router = StandardOrderRouter()
        risk_result = create_test_risk_result()
        snapshot = create_test_snapshot()
        config = create_test_config()

        order = router.route(risk_result, snapshot, config)

        # Client ID should contain part of intent ID
        assert "morph_" in order.client_order_id

    def test_limit_order_has_price(self):
        """Limit order should have limit price set."""
        router = StandardOrderRouter(RouterConfig(default_order_type=OrderType.LIMIT))
        risk_result = create_test_risk_result()
        snapshot = create_test_snapshot()
        config = ExecutionConfig(default_order_type=OrderType.LIMIT)

        order = router.route(risk_result, snapshot, config)

        assert order.order_type == OrderType.LIMIT
        assert order.limit_price is not None
        assert order.limit_price > 0


class TestMarketOrderRouter:
    """Tests for MarketOrderRouter."""

    def test_creates_market_order(self):
        """MarketOrderRouter should create market orders."""
        router = MarketOrderRouter()
        risk_result = create_test_risk_result()
        snapshot = create_test_snapshot()
        config = create_test_config()

        order = router.route(risk_result, snapshot, config)

        assert order.order_type == OrderType.MARKET
        assert order.limit_price is None


# ============================================================================
# Paper Broker Adapter Tests
# ============================================================================


class TestPaperBrokerAdapter:
    """Tests for PaperBrokerAdapter."""

    def test_submit_order_success(self):
        """Paper adapter should simulate successful submission."""
        adapter = PaperBrokerAdapter()
        order = OrderRequest(
            order_intent_id="intent_123",
            client_order_id="morph_123",
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=100,
        )
        config = create_test_config()

        result = adapter.submit_order(order, config)

        assert result.status == OrderStatus.WORKING
        assert result.broker_order_id.startswith("PAPER_")

    def test_idempotent_submission(self):
        """Paper adapter should handle duplicate submissions idempotently."""
        adapter = PaperBrokerAdapter()
        order = OrderRequest(
            order_intent_id="intent_123",
            client_order_id="morph_123",
            symbol="AAPL",
            quantity=100,
        )
        config = create_test_config()

        result1 = adapter.submit_order(order, config)
        result2 = adapter.submit_order(order, config)

        # Same order submitted twice should return same result
        assert result1.broker_order_id == result2.broker_order_id
        assert result1.status == result2.status

    def test_cancel_order(self):
        """Paper adapter should simulate order cancellation."""
        adapter = PaperBrokerAdapter()
        order = OrderRequest(
            order_intent_id="intent_123",
            client_order_id="morph_123",
            symbol="AAPL",
            quantity=100,
        )
        config = create_test_config()

        # Submit first
        adapter.submit_order(order, config)

        # Then cancel
        result = adapter.cancel_order("morph_123", config)

        assert result.status == OrderStatus.CANCELLED

    def test_simulate_fill(self):
        """Paper adapter should allow simulating fills."""
        adapter = PaperBrokerAdapter()
        order = OrderRequest(
            order_intent_id="intent_123",
            client_order_id="morph_123",
            symbol="AAPL",
            quantity=100,
        )
        config = create_test_config()

        adapter.submit_order(order, config)
        result = adapter.simulate_fill("morph_123", Decimal("150.25"))

        assert result.status == OrderStatus.FILLED
        assert result.filled_quantity == 100
        assert result.average_fill_price == Decimal("150.25")


class TestLiveTradingSafety:
    """Tests for live trading safety controls."""

    def test_live_not_armed_rejects(self):
        """Live mode without arming should reject orders."""
        adapter = PaperBrokerAdapter()

        # Create a custom adapter that checks live mode
        # (Using paper adapter's logic but with live config)
        # The real SchwabBrokerAdapter would reject

        # This test documents the expected behavior
        config = ExecutionConfig(
            trading_mode=TradingMode.LIVE,
            live_trading_armed=False,  # Not armed!
        )

        assert not config.is_live_enabled()

    def test_live_armed_enabled(self):
        """Live mode with arming should be enabled."""
        config = ExecutionConfig(
            trading_mode=TradingMode.LIVE,
            live_trading_armed=True,
        )

        assert config.is_live_enabled()

    def test_paper_mode_ignores_armed(self):
        """Paper mode should not care about arming."""
        config = ExecutionConfig(
            trading_mode=TradingMode.PAPER,
            live_trading_armed=False,
        )

        # Paper mode is always "safe" regardless of armed flag
        assert not config.is_live_enabled()  # Not live


# ============================================================================
# Determinism Tests
# ============================================================================


class TestDeterminism:
    """Tests for deterministic behavior."""

    def test_guard_deterministic(self):
        """Guard should produce same output for same input."""
        guard = StandardExecutionGuard()
        risk_result = create_test_risk_result()
        snapshot = create_test_snapshot()
        config = create_test_config()

        results = [guard.check(risk_result, snapshot, config) for _ in range(5)]

        # All results should be identical
        for result in results[1:]:
            assert result.decision == results[0].decision
            assert result.block_reasons == results[0].block_reasons

    def test_router_deterministic(self):
        """Router should produce same order ID for same input."""
        router = StandardOrderRouter()
        risk_result = create_test_risk_result()
        snapshot = create_test_snapshot()
        config = create_test_config()

        orders = [router.route(risk_result, snapshot, config) for _ in range(5)]

        # All intent IDs should be identical
        for order in orders[1:]:
            assert order.order_intent_id == orders[0].order_intent_id
            assert order.client_order_id == orders[0].client_order_id


# ============================================================================
# Upstream Respect Tests
# ============================================================================


class TestUpstreamRespect:
    """Tests that execution respects upstream decisions."""

    def test_guard_requires_position_size(self):
        """Guard should block if no position size in risk result."""
        guard = StandardExecutionGuard()
        # Create risk result without position size
        gate_result = create_test_gate_result()
        risk_result = RiskResult(
            gate_result=gate_result,
            decision=RiskDecision.APPROVED,
            position_size=None,  # No position size!
        )
        snapshot = create_test_snapshot()
        config = create_test_config()

        check = guard.check(risk_result, snapshot, config)

        assert check.is_blocked
        assert BlockReason.INVALID_ORDER in check.block_reasons

    def test_execution_should_only_process_approved_risk(self):
        """Execution should only be called with approved risk results."""
        # This is a design test - documenting expected behavior
        # The orchestrator should never call execution with vetoed risk

        approved = create_test_risk_result(decision=RiskDecision.APPROVED)
        vetoed = create_test_risk_result(decision=RiskDecision.VETOED)

        assert approved.is_approved
        assert vetoed.is_vetoed

        # Execution layer expects only approved results
        # If vetoed, the orchestrator should emit RISK_VETO and stop


# ============================================================================
# Event Completeness Tests
# ============================================================================


class TestEventCompleteness:
    """Tests that all paths emit appropriate events."""

    def test_blocked_execution_emits_event(self):
        """Blocked execution should emit EXECUTION_BLOCKED event."""
        guard = StandardExecutionGuard()
        risk_result = create_test_risk_result()
        snapshot = create_test_snapshot(is_tradeable=False)  # Halted
        config = create_test_config()

        check = guard.check(risk_result, snapshot, config)
        event = check.to_event("AAPL")

        assert event.event_type == EventType.EXECUTION_BLOCKED
        assert event.symbol == "AAPL"

    def test_filled_order_emits_event(self):
        """Filled order should emit ORDER_FILL_RECEIVED event."""
        result = OrderResult(
            status=OrderStatus.FILLED,
            symbol="AAPL",
            client_order_id="morph_123",
            filled_quantity=100,
        )
        event = result.to_event()

        assert event.event_type == EventType.ORDER_FILL_RECEIVED

    def test_rejected_order_emits_event(self):
        """Rejected order should emit ORDER_REJECTED event."""
        result = OrderResult(
            status=OrderStatus.REJECTED,
            symbol="AAPL",
            status_message="Insufficient funds",
        )
        event = result.to_event()

        assert event.event_type == EventType.ORDER_REJECTED

    def test_unknown_status_emits_event(self):
        """Unknown status should emit ORDER_STATUS_UNKNOWN event."""
        result = OrderResult(
            status=OrderStatus.UNKNOWN,
            symbol="AAPL",
            status_message="Timeout",
        )
        event = result.to_event()

        assert event.event_type == EventType.ORDER_STATUS_UNKNOWN


# ============================================================================
# Phase Isolation Tests
# ============================================================================


class TestPhaseIsolation:
    """Tests to ensure Phase 7 doesn't exceed its scope."""

    def test_no_fsm_imports_in_base(self):
        """execution/base.py should not import FSM directly."""
        import morpheus.execution.base as base_module
        import inspect

        source = inspect.getsource(base_module)

        # Check for FSM method calls (not imports, as FSM_TRANSITION_MAP references events)
        assert "fsm.transition" not in source.lower()
        assert "fsm.initiate" not in source.lower()

    def test_no_fsm_imports_in_guards(self):
        """execution/guards.py should not import FSM."""
        import morpheus.execution.guards as guards_module
        import inspect

        source = inspect.getsource(guards_module)

        assert "from morpheus.fsm" not in source
        assert "import morpheus.fsm" not in source

    def test_no_fsm_imports_in_router(self):
        """execution/router.py should not import FSM."""
        import morpheus.execution.router as router_module
        import inspect

        source = inspect.getsource(router_module)

        assert "from morpheus.fsm" not in source
        assert "import morpheus.fsm" not in source

    def test_no_fsm_imports_in_adapter(self):
        """execution/schwab_adapter.py should not import FSM."""
        import morpheus.execution.schwab_adapter as adapter_module
        import inspect

        source = inspect.getsource(adapter_module)

        assert "from morpheus.fsm" not in source
        assert "import morpheus.fsm" not in source

    def test_no_strategy_logic_in_execution(self):
        """Execution should not modify strategy/scoring logic."""
        import morpheus.execution.base as base_module
        import inspect

        source = inspect.getsource(base_module)

        # Should not import or modify scoring
        assert "from morpheus.scoring" not in source.replace(
            "from morpheus.scoring.base import", ""
        ).replace("from morpheus.risk.base import", "")


# ============================================================================
# Integration Tests
# ============================================================================


class TestExecutionPipeline:
    """Tests for complete execution pipeline."""

    def test_full_paper_trading_flow(self):
        """Test complete paper trading flow."""
        # Create components
        guard = create_standard_guard()
        router = create_standard_router()
        adapter = create_paper_adapter()

        # Create test data
        risk_result = create_test_risk_result()
        snapshot = create_test_snapshot()
        config = create_test_config(trading_mode=TradingMode.PAPER)

        # Step 1: Guard check
        check = guard.check(risk_result, snapshot, config)
        assert check.is_executable

        # Step 2: Route order
        order = router.route(risk_result, snapshot, config)
        assert order.quantity == 100
        assert order.symbol == "AAPL"

        # Step 3: Submit order
        result = adapter.submit_order(order, config)
        assert result.status == OrderStatus.WORKING

        # Step 4: Simulate fill
        fill = adapter.simulate_fill(order.client_order_id, Decimal("150.10"))
        assert fill.status == OrderStatus.FILLED
        assert fill.filled_quantity == 100

    def test_blocked_flow_does_not_submit(self):
        """Test that blocked execution doesn't proceed to submission."""
        guard = create_standard_guard()
        router = create_standard_router()
        adapter = create_paper_adapter()

        risk_result = create_test_risk_result()
        snapshot = create_test_snapshot(is_tradeable=False)  # Halted!
        config = create_test_config()

        # Step 1: Guard check - should block
        check = guard.check(risk_result, snapshot, config)
        assert check.is_blocked

        # Step 2: Emit EXECUTION_BLOCKED event
        event = check.to_event(snapshot.symbol)
        assert event.event_type == EventType.EXECUTION_BLOCKED

        # Step 3: Do NOT proceed to routing/submission
        # (In real orchestrator, this would stop here)


# ============================================================================
# Factory Function Tests
# ============================================================================


class TestFactoryFunctions:
    """Tests for factory functions."""

    def test_create_standard_guard(self):
        """create_standard_guard should return StandardExecutionGuard."""
        guard = create_standard_guard()
        assert isinstance(guard, StandardExecutionGuard)

    def test_create_permissive_guard(self):
        """create_permissive_guard should return PermissiveExecutionGuard."""
        guard = create_permissive_guard()
        assert isinstance(guard, PermissiveExecutionGuard)

    def test_create_strict_guard(self):
        """create_strict_guard should return StrictExecutionGuard."""
        guard = create_strict_guard()
        assert isinstance(guard, StrictExecutionGuard)

    def test_create_standard_router(self):
        """create_standard_router should return StandardOrderRouter."""
        router = create_standard_router()
        assert isinstance(router, StandardOrderRouter)

    def test_create_market_router(self):
        """create_market_router should return MarketOrderRouter."""
        router = create_market_router()
        assert isinstance(router, MarketOrderRouter)

    def test_create_limit_router(self):
        """create_limit_router should return LimitOrderRouter."""
        router = create_limit_router()
        assert isinstance(router, LimitOrderRouter)

    def test_create_paper_adapter(self):
        """create_paper_adapter should return PaperBrokerAdapter."""
        adapter = create_paper_adapter()
        assert isinstance(adapter, PaperBrokerAdapter)


# ============================================================================
# Schema Version Tests
# ============================================================================


class TestSchemaVersion:
    """Tests for schema versioning."""

    def test_schema_version_in_execution_check(self):
        """ExecutionCheck should include schema version."""
        check = ExecutionCheck()
        assert check.schema_version == EXECUTION_SCHEMA_VERSION

    def test_schema_version_in_order_request(self):
        """OrderRequest should include schema version."""
        request = OrderRequest()
        assert request.schema_version == EXECUTION_SCHEMA_VERSION

    def test_schema_version_in_order_result(self):
        """OrderResult should include schema version."""
        result = OrderResult()
        assert result.schema_version == EXECUTION_SCHEMA_VERSION
