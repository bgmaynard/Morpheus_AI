"""
Schwab Trading API Client.

Provides account, order, position, and transaction data from Schwab API.
This is READ-ONLY for now - fetches data to display in UI.

Schwab Trader API endpoints:
- GET /trader/v1/accounts - list accounts
- GET /trader/v1/accounts/{accountHash} - account details with positions
- GET /trader/v1/accounts/{accountHash}/orders - orders
- GET /trader/v1/accounts/{accountHash}/transactions - transactions/executions
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Protocol

from morpheus.broker.schwab_auth import SchwabAuth

logger = logging.getLogger(__name__)


class HttpClient(Protocol):
    """Protocol for HTTP client (allows dependency injection)."""

    def get(self, url: str, **kwargs: Any) -> Any:
        ...


class SchwabTradingError(Exception):
    """Raised when trading API call fails."""
    pass


@dataclass
class AccountInfo:
    """Schwab account information."""
    account_hash: str
    account_number: str
    account_type: str
    is_day_trader: bool


@dataclass
class PositionData:
    """Position data from Schwab."""
    symbol: str
    quantity: float
    avg_price: float
    current_price: float
    market_value: float
    unrealized_pnl: float
    unrealized_pnl_pct: float
    asset_type: str


@dataclass
class OrderData:
    """Order data from Schwab."""
    order_id: str
    symbol: str
    side: str  # 'BUY' or 'SELL'
    quantity: float
    filled_quantity: float
    order_type: str  # 'MARKET', 'LIMIT', etc.
    limit_price: float | None
    stop_price: float | None
    status: str  # 'WORKING', 'FILLED', 'CANCELED', etc.
    entered_time: datetime
    close_time: datetime | None


@dataclass
class TransactionData:
    """Transaction/execution data from Schwab."""
    transaction_id: str
    order_id: str
    symbol: str
    side: str
    quantity: float
    price: float
    timestamp: datetime
    transaction_type: str
    description: str


class SchwabTradingClient:
    """
    Client for Schwab Trader API.

    Provides read-only access to account, orders, positions, and transactions.
    """

    BASE_URL = "https://api.schwabapi.com/trader/v1"

    def __init__(self, auth: SchwabAuth, http_client: HttpClient):
        """
        Initialize the trading client.

        Args:
            auth: SchwabAuth instance for token management
            http_client: HTTP client for API requests
        """
        self.auth = auth
        self.http = http_client
        self._account_hash: str | None = None

    def _get_headers(self) -> dict[str, str]:
        """Get headers for API requests with valid auth token."""
        return {
            **self.auth.get_auth_header(self.http),
            "Accept": "application/json",
        }

    def get_accounts(self) -> list[AccountInfo]:
        """
        Get list of linked accounts.

        Returns:
            List of AccountInfo objects
        """
        try:
            # First get account numbers with hashes
            hash_url = f"{self.BASE_URL}/accounts/accountNumbers"
            hash_response = self.http.get(
                hash_url,
                headers=self._get_headers(),
            )

            if hash_response.status_code != 200:
                raise SchwabTradingError(
                    f"Account numbers request failed: {hash_response.status_code} - {hash_response.text}"
                )

            hash_data = hash_response.json()
            # Build mapping of account number to hash
            hash_map = {item["accountNumber"]: item["hashValue"] for item in hash_data}

            # Now get full account details
            url = f"{self.BASE_URL}/accounts"
            response = self.http.get(
                url,
                headers=self._get_headers(),
            )

            if response.status_code != 200:
                raise SchwabTradingError(
                    f"Accounts request failed: {response.status_code} - {response.text}"
                )

            data = response.json()
            accounts = []

            for item in data:
                acct = item.get("securitiesAccount", {})
                account_number = acct.get("accountNumber", "")
                # Look up hash from the accountNumbers response
                hash_value = hash_map.get(account_number, "")
                accounts.append(AccountInfo(
                    account_hash=hash_value,
                    account_number=account_number,
                    account_type=acct.get("type", ""),
                    is_day_trader=acct.get("isDayTrader", False),
                ))

            return accounts

        except SchwabTradingError:
            raise
        except Exception as e:
            logger.error(f"Failed to get accounts: {e}")
            raise SchwabTradingError(f"Accounts request failed: {e}") from e

    def get_account_hash(self) -> str:
        """
        Get the primary account hash (cached after first call).

        Returns:
            Account hash string for API calls
        """
        if self._account_hash is None:
            accounts = self.get_accounts()
            if not accounts:
                raise SchwabTradingError("No accounts found")
            self._account_hash = accounts[0].account_hash
            if not self._account_hash:
                raise SchwabTradingError(
                    "Account hash is empty - check Schwab API response structure"
                )
            logger.info(f"Using account hash: {self._account_hash[:8]}...")

        return self._account_hash

    def get_positions(self) -> list[PositionData]:
        """
        Get current positions for the account.

        Returns:
            List of PositionData objects
        """
        account_hash = self.get_account_hash()
        url = f"{self.BASE_URL}/accounts/{account_hash}"

        try:
            response = self.http.get(
                url,
                headers=self._get_headers(),
                params={"fields": "positions"},
            )

            if response.status_code != 200:
                raise SchwabTradingError(
                    f"Positions request failed: {response.status_code} - {response.text}"
                )

            data = response.json()

            # Handle different response formats
            # Single account: {"securitiesAccount": {...}}
            # Multiple accounts or list: [{"securitiesAccount": {...}}, ...]
            if isinstance(data, list):
                # List of accounts - get first one
                if data:
                    data = data[0]
                else:
                    return []

            positions_data = data.get("securitiesAccount", {}).get("positions", [])

            positions = []
            for pos in positions_data:
                instrument = pos.get("instrument", {})
                symbol = instrument.get("symbol", "")

                # Skip cash positions
                if instrument.get("assetType") == "CASH_EQUIVALENT":
                    continue

                quantity = float(pos.get("longQuantity", 0)) - float(pos.get("shortQuantity", 0))
                avg_price = float(pos.get("averagePrice", 0))
                current_price = float(pos.get("marketValue", 0)) / quantity if quantity != 0 else 0
                market_value = float(pos.get("marketValue", 0))

                # Calculate unrealized P&L
                cost_basis = avg_price * quantity
                unrealized_pnl = market_value - cost_basis
                unrealized_pnl_pct = (unrealized_pnl / cost_basis * 100) if cost_basis != 0 else 0

                positions.append(PositionData(
                    symbol=symbol,
                    quantity=quantity,
                    avg_price=avg_price,
                    current_price=current_price,
                    market_value=market_value,
                    unrealized_pnl=unrealized_pnl,
                    unrealized_pnl_pct=unrealized_pnl_pct,
                    asset_type=instrument.get("assetType", "EQUITY"),
                ))

            return positions

        except SchwabTradingError:
            raise
        except Exception as e:
            logger.error(f"Failed to get positions: {e}")
            raise SchwabTradingError(f"Positions request failed: {e}") from e

    def get_orders(
        self,
        status: str | None = None,
        from_date: datetime | None = None,
        to_date: datetime | None = None,
    ) -> list[OrderData]:
        """
        Get orders for the account.

        Args:
            status: Filter by status (WORKING, FILLED, CANCELED, etc.)
            from_date: Start date for order history
            to_date: End date for order history

        Returns:
            List of OrderData objects
        """
        account_hash = self.get_account_hash()
        url = f"{self.BASE_URL}/accounts/{account_hash}/orders"

        # Default to last 7 days if no dates specified
        if from_date is None:
            from_date = datetime.now(timezone.utc) - timedelta(days=7)
        if to_date is None:
            to_date = datetime.now(timezone.utc)

        params: dict[str, Any] = {
            "fromEnteredTime": from_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "toEnteredTime": to_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        }

        if status:
            params["status"] = status

        try:
            response = self.http.get(
                url,
                headers=self._get_headers(),
                params=params,
            )

            if response.status_code != 200:
                raise SchwabTradingError(
                    f"Orders request failed: {response.status_code} - {response.text}"
                )

            data = response.json()
            orders = []

            for order in data:
                # Get the first leg for symbol info
                legs = order.get("orderLegCollection", [])
                if not legs:
                    continue

                leg = legs[0]
                instrument = leg.get("instrument", {})

                # Parse timestamps
                entered_time = datetime.fromisoformat(
                    order.get("enteredTime", "").replace("Z", "+00:00")
                ) if order.get("enteredTime") else datetime.now(timezone.utc)

                close_time = None
                if order.get("closeTime"):
                    close_time = datetime.fromisoformat(
                        order["closeTime"].replace("Z", "+00:00")
                    )

                orders.append(OrderData(
                    order_id=str(order.get("orderId", "")),
                    symbol=instrument.get("symbol", ""),
                    side=leg.get("instruction", ""),  # BUY, SELL
                    quantity=float(order.get("quantity", 0)),
                    filled_quantity=float(order.get("filledQuantity", 0)),
                    order_type=order.get("orderType", ""),  # MARKET, LIMIT, etc.
                    limit_price=float(order.get("price", 0)) if order.get("price") else None,
                    stop_price=float(order.get("stopPrice", 0)) if order.get("stopPrice") else None,
                    status=order.get("status", ""),
                    entered_time=entered_time,
                    close_time=close_time,
                ))

            return orders

        except SchwabTradingError:
            raise
        except Exception as e:
            logger.error(f"Failed to get orders: {e}")
            raise SchwabTradingError(f"Orders request failed: {e}") from e

    def get_transactions(
        self,
        from_date: datetime | None = None,
        to_date: datetime | None = None,
        transaction_type: str = "TRADE",
    ) -> list[TransactionData]:
        """
        Get transactions (executions) for the account.

        Args:
            from_date: Start date
            to_date: End date
            transaction_type: Type filter (TRADE, DIVIDEND, etc.)

        Returns:
            List of TransactionData objects
        """
        account_hash = self.get_account_hash()
        url = f"{self.BASE_URL}/accounts/{account_hash}/transactions"

        # Default to today if no dates specified
        if from_date is None:
            from_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        if to_date is None:
            to_date = datetime.now(timezone.utc)

        params: dict[str, Any] = {
            "startDate": from_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "endDate": to_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "types": transaction_type,
        }

        try:
            response = self.http.get(
                url,
                headers=self._get_headers(),
                params=params,
            )

            if response.status_code != 200:
                raise SchwabTradingError(
                    f"Transactions request failed: {response.status_code} - {response.text}"
                )

            data = response.json()
            transactions = []

            for txn in data:
                # Get transfer items for trade details
                items = txn.get("transferItems", [])
                if not items:
                    continue

                item = items[0]
                instrument = item.get("instrument", {})

                # Parse timestamp
                timestamp = datetime.fromisoformat(
                    txn.get("transactionDate", "").replace("Z", "+00:00")
                ) if txn.get("transactionDate") else datetime.now(timezone.utc)

                # Determine side from position effect or amount
                amount = float(item.get("amount", 0))
                side = "BUY" if amount > 0 else "SELL"

                transactions.append(TransactionData(
                    transaction_id=str(txn.get("transactionId", "")),
                    order_id=str(txn.get("orderId", "")),
                    symbol=instrument.get("symbol", ""),
                    side=side,
                    quantity=abs(amount),
                    price=float(item.get("price", 0)),
                    timestamp=timestamp,
                    transaction_type=txn.get("type", ""),
                    description=txn.get("description", ""),
                ))

            return transactions

        except SchwabTradingError:
            raise
        except Exception as e:
            logger.error(f"Failed to get transactions: {e}")
            raise SchwabTradingError(f"Transactions request failed: {e}") from e
