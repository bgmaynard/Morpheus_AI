"""
Schwab Broker Adapter - Submits orders to Schwab API.

The adapter:
- Respects paper/live mode settings
- Handles retries with same client_order_id (idempotent)
- Emits explicit events for all outcomes
- Never hides order status (unknown status = explicit event)

CRITICAL SAFETY:
- LIVE mode requires LIVE_TRADING_ARMED=True
- Paper mode simulates order submission
- All outcomes produce events

Phase 7 Scope:
- Order submission only
- No strategy/scoring logic
- No upstream overrides
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Protocol

from morpheus.broker.schwab_auth import SchwabAuth
from morpheus.execution.base import (
    BrokerAdapter,
    OrderRequest,
    OrderResult,
    OrderStatus,
    OrderSide,
    OrderType,
    ExecutionConfig,
    TradingMode,
    BlockReason,
    EXECUTION_SCHEMA_VERSION,
)


logger = logging.getLogger(__name__)


class HttpClient(Protocol):
    """Protocol for HTTP client (allows dependency injection)."""

    def get(self, url: str, **kwargs: Any) -> Any:
        ...

    def post(self, url: str, **kwargs: Any) -> Any:
        ...

    def delete(self, url: str, **kwargs: Any) -> Any:
        ...


class SchwabOrderError(Exception):
    """Raised when order operation fails."""

    pass


class SchwabBrokerAdapter(BrokerAdapter):
    """
    Schwab API broker adapter.

    Handles:
    - Order submission to Schwab Trading API
    - Order cancellation
    - Order status retrieval
    - Paper trading simulation

    CRITICAL SAFETY:
    - Paper mode: Simulates orders, no real API calls
    - Live mode: Requires live_trading_armed=True
    - All operations emit events

    IDEMPOTENCY:
    - Uses client_order_id for all operations
    - Retries reuse the same client_order_id
    - Duplicate submissions are detected
    """

    # Schwab Trading API endpoints
    BASE_URL = "https://api.schwabapi.com/trader/v1"

    def __init__(
        self,
        auth: SchwabAuth,
        http_client: HttpClient,
        account_hash: str,
    ):
        """
        Initialize the Schwab adapter.

        Args:
            auth: SchwabAuth instance for token management
            http_client: HTTP client for API requests
            account_hash: Schwab account hash (encrypted account ID)
        """
        self.auth = auth
        self.http = http_client
        self.account_hash = account_hash

        # Track submitted orders for idempotency
        self._submitted_orders: dict[str, OrderResult] = {}

    @property
    def name(self) -> str:
        return "schwab"

    @property
    def version(self) -> str:
        return "1.0.0"

    def _get_headers(self) -> dict[str, str]:
        """Get headers for API requests with valid auth token."""
        return {
            **self.auth.get_auth_header(self.http),
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def submit_order(
        self,
        order: OrderRequest,
        config: ExecutionConfig,
    ) -> OrderResult:
        """
        Submit an order to Schwab.

        SAFETY CHECKS:
        1. If PAPER mode, simulate submission
        2. If LIVE mode but not armed, reject with LIVE_NOT_ARMED
        3. Check for duplicate submissions (idempotency)

        Args:
            order: OrderRequest to submit
            config: ExecutionConfig with trading mode

        Returns:
            OrderResult with status
        """
        now = datetime.now(timezone.utc)

        # Check 1: Idempotency - already submitted?
        if order.client_order_id in self._submitted_orders:
            logger.info(
                f"Duplicate order submission detected: {order.client_order_id}"
            )
            return self._submitted_orders[order.client_order_id]

        # Check 2: Paper mode - simulate
        if config.trading_mode == TradingMode.PAPER:
            return self._simulate_submission(order)

        # Check 3: Live mode safety
        if config.trading_mode == TradingMode.LIVE:
            if not config.live_trading_armed:
                logger.warning(
                    f"Live trading not armed - rejecting order {order.client_order_id}"
                )
                result = OrderResult(
                    schema_version=EXECUTION_SCHEMA_VERSION,
                    order_intent_id=order.order_intent_id,
                    client_order_id=order.client_order_id,
                    broker_order_id="",
                    status=OrderStatus.REJECTED,
                    status_message="Live trading not armed. Set live_trading_armed=True to enable.",
                    symbol=order.symbol,
                    side=order.side,
                    order_type=order.order_type,
                    requested_quantity=order.quantity,
                    submitted_at=now,
                    updated_at=now,
                )
                self._submitted_orders[order.client_order_id] = result
                return result

        # Submit to Schwab API
        return self._submit_to_schwab(order, config)

    def _simulate_submission(self, order: OrderRequest) -> OrderResult:
        """
        Simulate order submission for paper trading.

        Simulates successful submission with immediate working status.
        """
        now = datetime.now(timezone.utc)

        # Generate simulated broker order ID
        broker_order_id = f"PAPER_{order.client_order_id}"

        result = OrderResult(
            schema_version=EXECUTION_SCHEMA_VERSION,
            order_intent_id=order.order_intent_id,
            client_order_id=order.client_order_id,
            broker_order_id=broker_order_id,
            status=OrderStatus.WORKING,
            status_message="Paper order submitted (simulated)",
            symbol=order.symbol,
            side=order.side,
            order_type=order.order_type,
            requested_quantity=order.quantity,
            submitted_at=now,
            updated_at=now,
        )

        # Track for idempotency
        self._submitted_orders[order.client_order_id] = result

        logger.info(
            f"Paper order submitted: {order.client_order_id} -> {broker_order_id}"
        )

        return result

    def _submit_to_schwab(
        self,
        order: OrderRequest,
        config: ExecutionConfig,
    ) -> OrderResult:
        """
        Submit order to Schwab Trading API.

        Handles retries and timeouts with explicit events.
        """
        now = datetime.now(timezone.utc)
        url = f"{self.BASE_URL}/accounts/{self.account_hash}/orders"

        # Build Schwab order payload
        payload = self._build_order_payload(order)

        # Attempt submission with retries
        last_error = None
        for attempt in range(config.max_retries):
            try:
                response = self.http.post(
                    url,
                    headers=self._get_headers(),
                    json=payload,
                    timeout=config.timeout_seconds,
                )

                # Parse response
                if response.status_code == 201:
                    # Success - extract order ID from Location header
                    location = response.headers.get("Location", "")
                    broker_order_id = self._extract_order_id(location)

                    result = OrderResult(
                        schema_version=EXECUTION_SCHEMA_VERSION,
                        order_intent_id=order.order_intent_id,
                        client_order_id=order.client_order_id,
                        broker_order_id=broker_order_id,
                        status=OrderStatus.SUBMITTED,
                        status_message="Order accepted by Schwab",
                        symbol=order.symbol,
                        side=order.side,
                        order_type=order.order_type,
                        requested_quantity=order.quantity,
                        submitted_at=now,
                        updated_at=datetime.now(timezone.utc),
                    )
                    self._submitted_orders[order.client_order_id] = result
                    return result

                elif response.status_code == 400:
                    # Bad request - order rejected
                    error_msg = self._extract_error(response)
                    result = OrderResult(
                        schema_version=EXECUTION_SCHEMA_VERSION,
                        order_intent_id=order.order_intent_id,
                        client_order_id=order.client_order_id,
                        status=OrderStatus.REJECTED,
                        status_message=f"Order rejected: {error_msg}",
                        symbol=order.symbol,
                        side=order.side,
                        order_type=order.order_type,
                        requested_quantity=order.quantity,
                        submitted_at=now,
                        updated_at=datetime.now(timezone.utc),
                    )
                    self._submitted_orders[order.client_order_id] = result
                    return result

                else:
                    # Other error - retry
                    last_error = f"HTTP {response.status_code}: {response.text}"
                    logger.warning(
                        f"Order submission attempt {attempt + 1} failed: {last_error}"
                    )

            except Exception as e:
                last_error = str(e)
                logger.warning(
                    f"Order submission attempt {attempt + 1} exception: {e}"
                )

            # Wait before retry
            if attempt < config.max_retries - 1:
                time.sleep(config.retry_delay_seconds)

        # All retries failed - return UNKNOWN status
        logger.error(
            f"Order submission failed after {config.max_retries} attempts: {last_error}"
        )

        result = OrderResult(
            schema_version=EXECUTION_SCHEMA_VERSION,
            order_intent_id=order.order_intent_id,
            client_order_id=order.client_order_id,
            status=OrderStatus.UNKNOWN,
            status_message=f"Submission failed after {config.max_retries} retries: {last_error}",
            symbol=order.symbol,
            side=order.side,
            order_type=order.order_type,
            requested_quantity=order.quantity,
            submitted_at=now,
            updated_at=datetime.now(timezone.utc),
        )
        self._submitted_orders[order.client_order_id] = result
        return result

    def _build_order_payload(self, order: OrderRequest) -> dict[str, Any]:
        """Build Schwab order payload from OrderRequest."""
        # Map order type
        order_type_map = {
            OrderType.MARKET: "MARKET",
            OrderType.LIMIT: "LIMIT",
            OrderType.STOP: "STOP",
            OrderType.STOP_LIMIT: "STOP_LIMIT",
        }

        # Map side to instruction
        instruction = "BUY" if order.side == OrderSide.BUY else "SELL"

        payload: dict[str, Any] = {
            "orderType": order_type_map.get(order.order_type, "LIMIT"),
            "session": "NORMAL",
            "duration": order.time_in_force,
            "orderStrategyType": "SINGLE",
            "orderLegCollection": [
                {
                    "instruction": instruction,
                    "quantity": order.quantity,
                    "instrument": {
                        "symbol": order.symbol,
                        "assetType": "EQUITY",
                    },
                }
            ],
        }

        # Add price for limit orders
        if order.limit_price and order.order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT):
            payload["price"] = str(order.limit_price)

        # Add stop price for stop orders
        if order.stop_price and order.order_type in (OrderType.STOP, OrderType.STOP_LIMIT):
            payload["stopPrice"] = str(order.stop_price)

        return payload

    def _extract_order_id(self, location: str) -> str:
        """Extract order ID from Location header."""
        # Location format: /v1/accounts/{hash}/orders/{orderId}
        if location:
            parts = location.rstrip("/").split("/")
            if parts:
                return parts[-1]
        return ""

    def _extract_error(self, response: Any) -> str:
        """Extract error message from response."""
        try:
            data = response.json()
            return data.get("message", data.get("error", str(data)))
        except Exception:
            return response.text[:200] if hasattr(response, "text") else "Unknown error"

    def cancel_order(
        self,
        client_order_id: str,
        config: ExecutionConfig,
    ) -> OrderResult:
        """
        Cancel an existing order.

        Args:
            client_order_id: Client order ID to cancel
            config: ExecutionConfig

        Returns:
            OrderResult with cancel status
        """
        now = datetime.now(timezone.utc)

        # Check if we know about this order
        if client_order_id not in self._submitted_orders:
            return OrderResult(
                schema_version=EXECUTION_SCHEMA_VERSION,
                client_order_id=client_order_id,
                status=OrderStatus.REJECTED,
                status_message="Order not found",
                updated_at=now,
            )

        existing = self._submitted_orders[client_order_id]

        # Paper mode - simulate cancellation
        if config.trading_mode == TradingMode.PAPER:
            result = OrderResult(
                schema_version=EXECUTION_SCHEMA_VERSION,
                order_intent_id=existing.order_intent_id,
                client_order_id=client_order_id,
                broker_order_id=existing.broker_order_id,
                status=OrderStatus.CANCELLED,
                status_message="Paper order cancelled (simulated)",
                symbol=existing.symbol,
                side=existing.side,
                order_type=existing.order_type,
                requested_quantity=existing.requested_quantity,
                submitted_at=existing.submitted_at,
                updated_at=now,
            )
            self._submitted_orders[client_order_id] = result
            return result

        # Live mode - call Schwab API
        if not existing.broker_order_id:
            return OrderResult(
                schema_version=EXECUTION_SCHEMA_VERSION,
                client_order_id=client_order_id,
                status=OrderStatus.REJECTED,
                status_message="No broker order ID to cancel",
                updated_at=now,
            )

        url = f"{self.BASE_URL}/accounts/{self.account_hash}/orders/{existing.broker_order_id}"

        try:
            response = self.http.delete(
                url,
                headers=self._get_headers(),
                timeout=config.timeout_seconds,
            )

            if response.status_code in (200, 204):
                result = OrderResult(
                    schema_version=EXECUTION_SCHEMA_VERSION,
                    order_intent_id=existing.order_intent_id,
                    client_order_id=client_order_id,
                    broker_order_id=existing.broker_order_id,
                    status=OrderStatus.CANCELLED,
                    status_message="Order cancelled",
                    symbol=existing.symbol,
                    side=existing.side,
                    order_type=existing.order_type,
                    requested_quantity=existing.requested_quantity,
                    submitted_at=existing.submitted_at,
                    updated_at=now,
                )
                self._submitted_orders[client_order_id] = result
                return result

            else:
                error_msg = self._extract_error(response)
                return OrderResult(
                    schema_version=EXECUTION_SCHEMA_VERSION,
                    order_intent_id=existing.order_intent_id,
                    client_order_id=client_order_id,
                    broker_order_id=existing.broker_order_id,
                    status=OrderStatus.UNKNOWN,
                    status_message=f"Cancel failed: {error_msg}",
                    symbol=existing.symbol,
                    updated_at=now,
                )

        except Exception as e:
            logger.error(f"Cancel order failed: {e}")
            return OrderResult(
                schema_version=EXECUTION_SCHEMA_VERSION,
                client_order_id=client_order_id,
                status=OrderStatus.UNKNOWN,
                status_message=f"Cancel failed: {e}",
                updated_at=now,
            )

    def get_order_status(
        self,
        client_order_id: str,
        config: ExecutionConfig,
    ) -> OrderResult:
        """
        Get current status of an order.

        Args:
            client_order_id: Client order ID to check
            config: ExecutionConfig

        Returns:
            OrderResult with current status
        """
        now = datetime.now(timezone.utc)

        # Check if we know about this order
        if client_order_id not in self._submitted_orders:
            return OrderResult(
                schema_version=EXECUTION_SCHEMA_VERSION,
                client_order_id=client_order_id,
                status=OrderStatus.UNKNOWN,
                status_message="Order not found in local cache",
                updated_at=now,
            )

        existing = self._submitted_orders[client_order_id]

        # Paper mode - return cached status
        if config.trading_mode == TradingMode.PAPER:
            return existing

        # Live mode - query Schwab API
        if not existing.broker_order_id:
            return existing

        url = f"{self.BASE_URL}/accounts/{self.account_hash}/orders/{existing.broker_order_id}"

        try:
            response = self.http.get(
                url,
                headers=self._get_headers(),
                timeout=config.timeout_seconds,
            )

            if response.status_code == 200:
                data = response.json()
                return self._parse_order_response(data, existing)

            else:
                error_msg = self._extract_error(response)
                return OrderResult(
                    schema_version=EXECUTION_SCHEMA_VERSION,
                    order_intent_id=existing.order_intent_id,
                    client_order_id=client_order_id,
                    broker_order_id=existing.broker_order_id,
                    status=OrderStatus.UNKNOWN,
                    status_message=f"Status check failed: {error_msg}",
                    symbol=existing.symbol,
                    updated_at=now,
                )

        except Exception as e:
            logger.error(f"Get order status failed: {e}")
            return OrderResult(
                schema_version=EXECUTION_SCHEMA_VERSION,
                client_order_id=client_order_id,
                status=OrderStatus.UNKNOWN,
                status_message=f"Status check failed: {e}",
                updated_at=now,
            )

    def _parse_order_response(
        self,
        data: dict[str, Any],
        existing: OrderResult,
    ) -> OrderResult:
        """Parse Schwab order response into OrderResult."""
        now = datetime.now(timezone.utc)

        # Map Schwab status to our status
        schwab_status = data.get("status", "UNKNOWN")
        status_map = {
            "AWAITING_PARENT_ORDER": OrderStatus.PENDING,
            "AWAITING_CONDITION": OrderStatus.PENDING,
            "AWAITING_MANUAL_REVIEW": OrderStatus.PENDING,
            "ACCEPTED": OrderStatus.SUBMITTED,
            "PENDING_ACTIVATION": OrderStatus.SUBMITTED,
            "QUEUED": OrderStatus.WORKING,
            "WORKING": OrderStatus.WORKING,
            "FILLED": OrderStatus.FILLED,
            "REJECTED": OrderStatus.REJECTED,
            "CANCELED": OrderStatus.CANCELLED,
            "EXPIRED": OrderStatus.EXPIRED,
        }
        status = status_map.get(schwab_status, OrderStatus.UNKNOWN)

        # Extract fill info
        filled_qty = int(data.get("filledQuantity", 0))
        avg_price = None
        if filled_qty > 0:
            activities = data.get("orderActivityCollection", [])
            if activities:
                # Get average from execution legs
                total_value = Decimal("0")
                total_qty = 0
                for activity in activities:
                    for leg in activity.get("executionLegs", []):
                        leg_qty = int(leg.get("quantity", 0))
                        leg_price = Decimal(str(leg.get("price", 0)))
                        total_value += leg_price * leg_qty
                        total_qty += leg_qty
                if total_qty > 0:
                    avg_price = total_value / total_qty

        result = OrderResult(
            schema_version=EXECUTION_SCHEMA_VERSION,
            order_intent_id=existing.order_intent_id,
            client_order_id=existing.client_order_id,
            broker_order_id=existing.broker_order_id,
            status=status,
            status_message=f"Schwab status: {schwab_status}",
            filled_quantity=filled_qty,
            average_fill_price=avg_price,
            symbol=existing.symbol,
            side=existing.side,
            order_type=existing.order_type,
            requested_quantity=existing.requested_quantity,
            submitted_at=existing.submitted_at,
            updated_at=now,
        )

        # Update cache
        self._submitted_orders[existing.client_order_id] = result

        return result


class PaperBrokerAdapter(BrokerAdapter):
    """
    Paper trading adapter that simulates order execution.

    For testing without any broker integration.
    Always simulates successful fills after a short delay.
    """

    def __init__(self, fill_delay_ms: int = 100):
        """
        Initialize paper adapter.

        Args:
            fill_delay_ms: Simulated fill delay in milliseconds
        """
        self._fill_delay_ms = fill_delay_ms
        self._orders: dict[str, OrderResult] = {}

    @property
    def name(self) -> str:
        return "paper"

    @property
    def version(self) -> str:
        return "1.0.0"

    def submit_order(
        self,
        order: OrderRequest,
        config: ExecutionConfig,
    ) -> OrderResult:
        """Simulate order submission."""
        now = datetime.now(timezone.utc)

        # Check for duplicates
        if order.client_order_id in self._orders:
            return self._orders[order.client_order_id]

        # Simulate submission
        broker_order_id = f"PAPER_{order.client_order_id}"

        result = OrderResult(
            schema_version=EXECUTION_SCHEMA_VERSION,
            order_intent_id=order.order_intent_id,
            client_order_id=order.client_order_id,
            broker_order_id=broker_order_id,
            status=OrderStatus.WORKING,
            status_message="Paper order working",
            symbol=order.symbol,
            side=order.side,
            order_type=order.order_type,
            requested_quantity=order.quantity,
            submitted_at=now,
            updated_at=now,
        )

        self._orders[order.client_order_id] = result
        return result

    def cancel_order(
        self,
        client_order_id: str,
        config: ExecutionConfig,
    ) -> OrderResult:
        """Simulate order cancellation."""
        now = datetime.now(timezone.utc)

        if client_order_id not in self._orders:
            return OrderResult(
                schema_version=EXECUTION_SCHEMA_VERSION,
                client_order_id=client_order_id,
                status=OrderStatus.REJECTED,
                status_message="Order not found",
                updated_at=now,
            )

        existing = self._orders[client_order_id]

        result = OrderResult(
            schema_version=EXECUTION_SCHEMA_VERSION,
            order_intent_id=existing.order_intent_id,
            client_order_id=client_order_id,
            broker_order_id=existing.broker_order_id,
            status=OrderStatus.CANCELLED,
            status_message="Paper order cancelled",
            symbol=existing.symbol,
            side=existing.side,
            order_type=existing.order_type,
            requested_quantity=existing.requested_quantity,
            submitted_at=existing.submitted_at,
            updated_at=now,
        )

        self._orders[client_order_id] = result
        return result

    def get_order_status(
        self,
        client_order_id: str,
        config: ExecutionConfig,
    ) -> OrderResult:
        """Get simulated order status."""
        now = datetime.now(timezone.utc)

        if client_order_id not in self._orders:
            return OrderResult(
                schema_version=EXECUTION_SCHEMA_VERSION,
                client_order_id=client_order_id,
                status=OrderStatus.UNKNOWN,
                status_message="Order not found",
                updated_at=now,
            )

        return self._orders[client_order_id]

    def simulate_fill(
        self,
        client_order_id: str,
        fill_price: Decimal,
    ) -> OrderResult:
        """
        Simulate a fill for testing.

        Args:
            client_order_id: Order to fill
            fill_price: Fill price

        Returns:
            Updated OrderResult
        """
        now = datetime.now(timezone.utc)

        if client_order_id not in self._orders:
            return OrderResult(
                schema_version=EXECUTION_SCHEMA_VERSION,
                client_order_id=client_order_id,
                status=OrderStatus.UNKNOWN,
                status_message="Order not found",
                updated_at=now,
            )

        existing = self._orders[client_order_id]

        result = OrderResult(
            schema_version=EXECUTION_SCHEMA_VERSION,
            order_intent_id=existing.order_intent_id,
            client_order_id=client_order_id,
            broker_order_id=existing.broker_order_id,
            status=OrderStatus.FILLED,
            status_message="Paper order filled",
            filled_quantity=existing.requested_quantity,
            average_fill_price=fill_price,
            fill_time=now,
            symbol=existing.symbol,
            side=existing.side,
            order_type=existing.order_type,
            requested_quantity=existing.requested_quantity,
            submitted_at=existing.submitted_at,
            updated_at=now,
        )

        self._orders[client_order_id] = result
        return result


def create_schwab_adapter(
    auth: SchwabAuth,
    http_client: HttpClient,
    account_hash: str,
) -> SchwabBrokerAdapter:
    """Factory function to create a SchwabBrokerAdapter."""
    return SchwabBrokerAdapter(auth, http_client, account_hash)


def create_paper_adapter(fill_delay_ms: int = 100) -> PaperBrokerAdapter:
    """Factory function to create a PaperBrokerAdapter."""
    return PaperBrokerAdapter(fill_delay_ms)
