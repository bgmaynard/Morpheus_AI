"""
Order Router - Constructs orders from approved risk results.

Routers:
- Generate idempotent order IDs
- Construct OrderRequest from RiskResult
- Determine order type and parameters
- Do NOT submit orders (that's the adapter's job)

All routing is DETERMINISTIC:
- Same inputs → same OrderRequest
- Stable order_intent_id generation

Phase 7 Scope:
- Order construction only
- No order submission
- No broker communication
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_UP

from morpheus.data.market_snapshot import MarketSnapshot
from morpheus.risk.base import RiskResult
from morpheus.strategies.base import SignalDirection
from morpheus.execution.base import (
    OrderRouter,
    OrderRequest,
    OrderType,
    OrderSide,
    ExecutionConfig,
    generate_order_intent_id,
    generate_client_order_id,
    EXECUTION_SCHEMA_VERSION,
)


@dataclass(frozen=True)
class RouterConfig:
    """
    Configuration for order router.

    Defines order construction parameters.
    """

    # Default order type
    default_order_type: OrderType = OrderType.LIMIT

    # Limit price offset (for limit orders)
    # Positive = more aggressive (buy higher, sell lower)
    limit_offset_pct: float = 0.1  # 0.1% offset from mid

    # Time in force
    default_time_in_force: str = "DAY"

    # Price rounding
    round_price_to: int = 2  # Decimal places


class StandardOrderRouter(OrderRouter):
    """
    Standard order router.

    Constructs orders with:
    - Idempotent intent IDs (deterministic from risk result)
    - Limit prices based on current market
    - Proper side determination from signal direction

    CRITICAL: Order intent ID is generated deterministically
    from the risk result's gate result, ensuring same approval
    always produces same order ID (idempotency).
    """

    def __init__(self, config: RouterConfig | None = None):
        """
        Initialize the order router.

        Args:
            config: Optional custom router configuration
        """
        self._config = config or RouterConfig()

    @property
    def name(self) -> str:
        return "standard_order_router"

    @property
    def version(self) -> str:
        return "1.0.0"

    def route(
        self,
        risk_result: RiskResult,
        snapshot: MarketSnapshot,
        config: ExecutionConfig,
    ) -> OrderRequest:
        """
        Construct an order request from risk result.

        CRITICAL: Generates stable order_intent_id for idempotency.
        """
        cfg = self._config

        # Extract components
        position_size = risk_result.position_size
        gate_result = risk_result.gate_result
        scored_signal = gate_result.scored_signal if gate_result else None
        signal = scored_signal.signal if scored_signal else None

        # Determine symbol
        symbol = signal.symbol if signal else snapshot.symbol

        # Determine side from signal direction
        direction = signal.direction if signal else SignalDirection.NONE
        side = self._direction_to_side(direction)

        # Generate idempotent order IDs
        # Intent ID is derived from the upstream decisions (deterministic)
        intent_id = self._generate_intent_id(risk_result, snapshot)
        client_order_id = generate_client_order_id(intent_id)

        # Determine order type
        order_type = config.default_order_type if config.default_order_type else cfg.default_order_type

        # Calculate limit price (if limit order)
        limit_price = None
        if order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT):
            limit_price = self._calculate_limit_price(snapshot, side, cfg)

        # Get stop price from position size (if provided)
        stop_price = position_size.stop_price if position_size else None

        # Get quantity
        quantity = position_size.shares if position_size else 0

        # Get signal ID for audit trail
        signal_id = ""
        if signal:
            # Use signal's timestamp + symbol as ID
            signal_id = f"{signal.symbol}_{signal.timestamp.isoformat()}"

        return OrderRequest(
            schema_version=EXECUTION_SCHEMA_VERSION,
            order_intent_id=intent_id,
            client_order_id=client_order_id,
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            limit_price=limit_price,
            stop_price=stop_price,
            time_in_force=config.default_time_in_force or cfg.default_time_in_force,
            risk_result_id=self._get_risk_result_id(risk_result),
            signal_id=signal_id,
            created_at=datetime.now(timezone.utc),
        )

    def _direction_to_side(self, direction: SignalDirection) -> OrderSide:
        """Convert signal direction to order side."""
        if direction == SignalDirection.LONG:
            return OrderSide.BUY
        elif direction == SignalDirection.SHORT:
            return OrderSide.SELL
        else:
            # NONE direction - default to BUY (shouldn't reach here)
            return OrderSide.BUY

    def _generate_intent_id(
        self,
        risk_result: RiskResult,
        snapshot: MarketSnapshot,
    ) -> str:
        """
        Generate deterministic intent ID from risk result.

        CRITICAL for idempotency: Same risk result + snapshot
        must always produce the same intent ID.

        Uses hash of:
        - Symbol
        - Signal timestamp
        - Confidence
        - Shares
        - Entry price
        """
        # Build deterministic string from upstream decisions
        parts = []

        # Symbol
        if risk_result.gate_result and risk_result.gate_result.scored_signal:
            signal = risk_result.gate_result.scored_signal.signal
            if signal:
                parts.append(f"sym:{signal.symbol}")
                parts.append(f"ts:{signal.timestamp.isoformat()}")
                parts.append(f"dir:{signal.direction.value}")

        # Confidence
        if risk_result.gate_result and risk_result.gate_result.scored_signal:
            parts.append(f"conf:{risk_result.gate_result.scored_signal.confidence:.4f}")

        # Position size
        if risk_result.position_size:
            parts.append(f"shares:{risk_result.position_size.shares}")
            parts.append(f"entry:{risk_result.position_size.entry_price}")

        # Create deterministic hash
        data = "|".join(parts)
        hash_hex = hashlib.sha256(data.encode()).hexdigest()[:16]

        return f"intent_{hash_hex}"

    def _get_risk_result_id(self, risk_result: RiskResult) -> str:
        """Generate an ID for the risk result (for audit trail)."""
        return f"risk_{risk_result.evaluated_at.isoformat()}"

    def _calculate_limit_price(
        self,
        snapshot: MarketSnapshot,
        side: OrderSide,
        cfg: RouterConfig,
    ) -> Decimal:
        """
        Calculate limit price based on current market.

        For BUY: slightly above mid (aggressive)
        For SELL: slightly below mid (aggressive)

        Aggressive pricing increases fill probability.
        """
        # Calculate mid price
        mid = (Decimal(str(snapshot.bid)) + Decimal(str(snapshot.ask))) / 2

        # Calculate offset
        offset = mid * Decimal(str(cfg.limit_offset_pct / 100))

        # Apply offset based on side
        if side == OrderSide.BUY:
            # Buy slightly higher (more aggressive)
            price = mid + offset
            # Round up for buys
            return price.quantize(
                Decimal(f"0.{'0' * cfg.round_price_to}"),
                rounding=ROUND_UP,
            )
        else:
            # Sell slightly lower (more aggressive)
            price = mid - offset
            # Round down for sells
            return price.quantize(
                Decimal(f"0.{'0' * cfg.round_price_to}"),
                rounding=ROUND_DOWN,
            )


class MarketOrderRouter(OrderRouter):
    """
    Simple router that creates market orders.

    For fast execution without price limits.
    Higher slippage risk but guaranteed fills.
    """

    @property
    def name(self) -> str:
        return "market_order_router"

    @property
    def version(self) -> str:
        return "1.0.0"

    def route(
        self,
        risk_result: RiskResult,
        snapshot: MarketSnapshot,
        config: ExecutionConfig,
    ) -> OrderRequest:
        """Create a market order."""
        # Use standard router but override order type
        standard = StandardOrderRouter()
        request = standard.route(risk_result, snapshot, config)

        # Create new request with MARKET type
        return OrderRequest(
            schema_version=request.schema_version,
            order_intent_id=request.order_intent_id,
            client_order_id=request.client_order_id,
            symbol=request.symbol,
            side=request.side,
            order_type=OrderType.MARKET,
            quantity=request.quantity,
            limit_price=None,  # No limit for market orders
            stop_price=request.stop_price,
            time_in_force="DAY",  # Market orders always DAY
            risk_result_id=request.risk_result_id,
            signal_id=request.signal_id,
            created_at=request.created_at,
        )


class LimitOrderRouter(OrderRouter):
    """
    Router that always creates limit orders.

    For controlled execution with price protection.
    May not fill if price moves.
    """

    def __init__(self, offset_pct: float = 0.1):
        """
        Initialize limit order router.

        Args:
            offset_pct: Limit price offset percentage from mid
        """
        self._config = RouterConfig(
            default_order_type=OrderType.LIMIT,
            limit_offset_pct=offset_pct,
        )
        self._standard = StandardOrderRouter(self._config)

    @property
    def name(self) -> str:
        return "limit_order_router"

    @property
    def version(self) -> str:
        return "1.0.0"

    def route(
        self,
        risk_result: RiskResult,
        snapshot: MarketSnapshot,
        config: ExecutionConfig,
    ) -> OrderRequest:
        """Create a limit order."""
        # Override config to force LIMIT
        limit_config = ExecutionConfig(
            trading_mode=config.trading_mode,
            live_trading_armed=config.live_trading_armed,
            default_order_type=OrderType.LIMIT,
        )
        return self._standard.route(risk_result, snapshot, limit_config)


def create_standard_router(config: RouterConfig | None = None) -> StandardOrderRouter:
    """Factory function to create a StandardOrderRouter."""
    return StandardOrderRouter(config)


def create_market_router() -> MarketOrderRouter:
    """Factory function to create a MarketOrderRouter."""
    return MarketOrderRouter()


def create_limit_router(offset_pct: float = 0.1) -> LimitOrderRouter:
    """Factory function to create a LimitOrderRouter."""
    return LimitOrderRouter(offset_pct)
