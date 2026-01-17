"""
Tests for Schwab OAuth authentication.

Phase 2 test coverage:
- Token data handling
- Token storage and retrieval
- Token expiry detection
- Auth isolation (no market data logic)
"""

import pytest
import json
import time
import tempfile
from pathlib import Path
from unittest.mock import Mock, MagicMock

from morpheus.broker.schwab_auth import (
    SchwabAuth,
    TokenData,
    SchwabAuthError,
    TokenExpiredError,
    TokenNotFoundError,
)


class TestTokenData:
    """Tests for TokenData dataclass."""

    def test_token_data_creation(self):
        """TokenData should store all fields."""
        token = TokenData(
            access_token="access_123",
            refresh_token="refresh_456",
            token_type="Bearer",
            expires_in=1800,
            scope="readonly",
            issued_at=time.time(),
        )

        assert token.access_token == "access_123"
        assert token.refresh_token == "refresh_456"
        assert token.expires_in == 1800

    def test_token_expires_at(self):
        """expires_at should be issued_at + expires_in."""
        issued = 1000.0
        expires_in = 1800

        token = TokenData(
            access_token="a",
            refresh_token="r",
            token_type="Bearer",
            expires_in=expires_in,
            scope="",
            issued_at=issued,
        )

        assert token.expires_at == issued + expires_in

    def test_token_is_expired(self):
        """is_expired should return True when token is expired."""
        # Token issued in the past, already expired
        old_token = TokenData(
            access_token="old",
            refresh_token="r",
            token_type="Bearer",
            expires_in=1800,
            scope="",
            issued_at=time.time() - 2000,  # Issued 2000 seconds ago
        )

        assert old_token.is_expired is True

        # Token issued just now, should not be expired
        fresh_token = TokenData(
            access_token="fresh",
            refresh_token="r",
            token_type="Bearer",
            expires_in=1800,
            scope="",
            issued_at=time.time(),
        )

        assert fresh_token.is_expired is False

    def test_token_is_expired_with_buffer(self):
        """is_expired should include 60s buffer."""
        # Token that expires in 30 seconds should be considered expired
        # because of the 60s buffer
        token = TokenData(
            access_token="almost_expired",
            refresh_token="r",
            token_type="Bearer",
            expires_in=30,  # 30 seconds
            scope="",
            issued_at=time.time(),
        )

        assert token.is_expired is True

    def test_token_to_dict(self):
        """Token should serialize to dict."""
        token = TokenData(
            access_token="access",
            refresh_token="refresh",
            token_type="Bearer",
            expires_in=1800,
            scope="readonly",
            issued_at=12345.0,
        )

        data = token.to_dict()

        assert data["access_token"] == "access"
        assert data["refresh_token"] == "refresh"
        assert data["issued_at"] == 12345.0

    def test_token_from_dict(self):
        """Token should deserialize from dict."""
        data = {
            "access_token": "access",
            "refresh_token": "refresh",
            "token_type": "Bearer",
            "expires_in": 1800,
            "scope": "readonly",
            "issued_at": 12345.0,
        }

        token = TokenData.from_dict(data)

        assert token.access_token == "access"
        assert token.issued_at == 12345.0


class TestSchwabAuth:
    """Tests for SchwabAuth class."""

    def test_auth_initialization(self):
        """SchwabAuth should initialize with credentials."""
        with tempfile.TemporaryDirectory() as tmpdir:
            auth = SchwabAuth(
                client_id="test_client",
                client_secret="test_secret",
                token_path=Path(tmpdir) / "token.json",
            )

            assert auth.client_id == "test_client"
            assert auth.client_secret == "test_secret"

    def test_auth_creates_token_directory(self):
        """SchwabAuth should create token directory if needed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            token_path = Path(tmpdir) / "subdir" / "token.json"

            auth = SchwabAuth(
                client_id="test",
                client_secret="secret",
                token_path=token_path,
            )

            assert token_path.parent.exists()

    def test_get_authorization_url(self):
        """get_authorization_url should return valid OAuth URL."""
        auth = SchwabAuth(
            client_id="my_client_id",
            client_secret="secret",
            redirect_uri="https://localhost:8080/callback",
        )

        url = auth.get_authorization_url()

        assert "api.schwabapi.com" in url
        assert "client_id=my_client_id" in url
        assert "redirect_uri=" in url

    def test_save_and_load_token(self):
        """Token should persist to file and reload."""
        with tempfile.TemporaryDirectory() as tmpdir:
            token_path = Path(tmpdir) / "token.json"

            auth = SchwabAuth(
                client_id="test",
                client_secret="secret",
                token_path=token_path,
            )

            # Save token response
            response = {
                "access_token": "test_access",
                "refresh_token": "test_refresh",
                "token_type": "Bearer",
                "expires_in": 1800,
                "scope": "readonly",
            }

            token = auth.save_token_response(response)

            assert token.access_token == "test_access"
            assert token_path.exists()

            # Create new auth instance and load
            auth2 = SchwabAuth(
                client_id="test",
                client_secret="secret",
                token_path=token_path,
            )

            loaded_token = auth2.get_token()

            assert loaded_token.access_token == "test_access"
            assert loaded_token.refresh_token == "test_refresh"

    def test_token_not_found_error(self):
        """get_token should raise error if no token file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            auth = SchwabAuth(
                client_id="test",
                client_secret="secret",
                token_path=Path(tmpdir) / "nonexistent.json",
            )

            with pytest.raises(TokenNotFoundError):
                auth.get_token()

    def test_is_token_valid(self):
        """is_token_valid should check token existence and expiry."""
        with tempfile.TemporaryDirectory() as tmpdir:
            auth = SchwabAuth(
                client_id="test",
                client_secret="secret",
                token_path=Path(tmpdir) / "token.json",
            )

            # No token yet
            assert auth.is_token_valid() is False

            # Save valid token
            auth.save_token_response({
                "access_token": "valid",
                "refresh_token": "refresh",
                "expires_in": 1800,
            })

            assert auth.is_token_valid() is True

    def test_needs_refresh_when_expired(self):
        """needs_refresh should return True for expired tokens."""
        with tempfile.TemporaryDirectory() as tmpdir:
            token_path = Path(tmpdir) / "token.json"

            # Manually create an expired token file
            expired_token = {
                "access_token": "expired",
                "refresh_token": "refresh",
                "token_type": "Bearer",
                "expires_in": 1800,
                "scope": "",
                "issued_at": time.time() - 3600,  # 1 hour ago
            }

            with open(token_path, "w") as f:
                json.dump(expired_token, f)

            auth = SchwabAuth(
                client_id="test",
                client_secret="secret",
                token_path=token_path,
            )

            assert auth.needs_refresh() is True

    def test_get_access_token_raises_when_expired_no_client(self):
        """get_access_token should raise if expired and no http_client."""
        with tempfile.TemporaryDirectory() as tmpdir:
            token_path = Path(tmpdir) / "token.json"

            # Create expired token
            expired_token = {
                "access_token": "expired",
                "refresh_token": "refresh",
                "token_type": "Bearer",
                "expires_in": 1800,
                "scope": "",
                "issued_at": time.time() - 3600,
            }

            with open(token_path, "w") as f:
                json.dump(expired_token, f)

            auth = SchwabAuth(
                client_id="test",
                client_secret="secret",
                token_path=token_path,
            )

            with pytest.raises(TokenExpiredError):
                auth.get_access_token(http_client=None)

    def test_get_auth_header(self):
        """get_auth_header should return proper Authorization header."""
        with tempfile.TemporaryDirectory() as tmpdir:
            auth = SchwabAuth(
                client_id="test",
                client_secret="secret",
                token_path=Path(tmpdir) / "token.json",
            )

            auth.save_token_response({
                "access_token": "my_access_token",
                "refresh_token": "refresh",
                "expires_in": 1800,
            })

            header = auth.get_auth_header()

            assert header == {"Authorization": "Bearer my_access_token"}

    def test_clear_token(self):
        """clear_token should remove token file and memory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            token_path = Path(tmpdir) / "token.json"

            auth = SchwabAuth(
                client_id="test",
                client_secret="secret",
                token_path=token_path,
            )

            auth.save_token_response({
                "access_token": "to_clear",
                "refresh_token": "refresh",
                "expires_in": 1800,
            })

            assert token_path.exists()

            auth.clear_token()

            assert not token_path.exists()
            assert auth.is_token_valid() is False


class TestSchwabAuthIsolation:
    """Tests verifying auth is isolated from other concerns."""

    def test_auth_has_no_market_imports(self):
        """Auth module should not import market data modules."""
        from morpheus.broker import schwab_auth

        # Check the source code, not sys.modules (which includes test imports)
        module_source = open(schwab_auth.__file__).read()

        assert "market_snapshot" not in module_source
        assert "MarketSnapshot" not in module_source
        assert "schwab_market" not in module_source

    def test_auth_has_no_fsm_imports(self):
        """Auth module should not import FSM (Phase 1)."""
        from morpheus.broker import schwab_auth

        # The module should not have any trade-related imports
        module_source = open(schwab_auth.__file__).read()

        assert "trade_fsm" not in module_source
        assert "TradeLifecycle" not in module_source
