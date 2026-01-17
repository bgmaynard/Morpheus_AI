"""
Schwab OAuth Authentication Handler.

Manages OAuth token lifecycle:
- Initial authorization (manual, one-time)
- Token storage and retrieval
- Automatic refresh before expiry
- Secure token handling

Phase 2 Scope:
- Auth is isolated from market data logic
- No auth logic leaks into snapshot logic
- Tokens stored at configurable path
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

logger = logging.getLogger(__name__)


class SchwabAuthError(Exception):
    """Raised when authentication fails."""

    pass


class TokenExpiredError(SchwabAuthError):
    """Raised when token is expired and refresh fails."""

    pass


class TokenNotFoundError(SchwabAuthError):
    """Raised when no token file exists."""

    pass


@dataclass
class TokenData:
    """OAuth token data container."""

    access_token: str
    refresh_token: str
    token_type: str
    expires_in: int  # seconds
    scope: str
    issued_at: float  # Unix timestamp when token was issued

    @property
    def expires_at(self) -> float:
        """Unix timestamp when access token expires."""
        return self.issued_at + self.expires_in

    @property
    def is_expired(self) -> bool:
        """Check if access token is expired (with 60s buffer)."""
        return time.time() >= (self.expires_at - 60)

    @property
    def seconds_until_expiry(self) -> float:
        """Seconds until access token expires."""
        return max(0, self.expires_at - time.time())

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "token_type": self.token_type,
            "expires_in": self.expires_in,
            "scope": self.scope,
            "issued_at": self.issued_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TokenData:
        """Create from dictionary."""
        return cls(
            access_token=data["access_token"],
            refresh_token=data["refresh_token"],
            token_type=data.get("token_type", "Bearer"),
            expires_in=data.get("expires_in", 1800),
            scope=data.get("scope", ""),
            issued_at=data.get("issued_at", time.time()),
        )


class SchwabAuth:
    """
    Schwab OAuth authentication manager.

    Handles token storage, retrieval, and refresh.
    Does NOT handle the initial OAuth flow (that's manual).

    Usage:
        auth = SchwabAuth(client_id, client_secret, token_path)

        # After manual OAuth flow, save the token
        auth.save_token(token_response)

        # Get valid access token (auto-refreshes if needed)
        token = auth.get_access_token()
    """

    # Schwab OAuth endpoints
    TOKEN_URL = "https://api.schwabapi.com/v1/oauth/token"
    AUTH_URL = "https://api.schwabapi.com/v1/oauth/authorize"

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        redirect_uri: str = "https://localhost:8080/callback",
        token_path: Path | str = "./tokens/schwab_token.json",
    ):
        """
        Initialize Schwab auth manager.

        Args:
            client_id: Schwab app client ID
            client_secret: Schwab app client secret
            redirect_uri: OAuth redirect URI
            token_path: Path to store token file
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.token_path = Path(token_path)
        self._token: TokenData | None = None

        # Ensure token directory exists
        self.token_path.parent.mkdir(parents=True, exist_ok=True)

    def get_authorization_url(self) -> str:
        """
        Get the OAuth authorization URL for initial setup.

        User must visit this URL, authorize, and provide the callback code.
        """
        params = {
            "response_type": "code",
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "scope": "readonly",
        }
        return f"{self.AUTH_URL}?{urlencode(params)}"

    def _load_token(self) -> TokenData | None:
        """Load token from file if it exists."""
        if not self.token_path.exists():
            return None

        try:
            with open(self.token_path, "r") as f:
                data = json.load(f)
                return TokenData.from_dict(data)
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Failed to load token: {e}")
            return None

    def _save_token(self, token: TokenData) -> None:
        """Save token to file."""
        with open(self.token_path, "w") as f:
            json.dump(token.to_dict(), f, indent=2)
        logger.info(f"Token saved to {self.token_path}")

    def save_token_response(self, response: dict[str, Any]) -> TokenData:
        """
        Save a token response from Schwab OAuth.

        Call this after the initial OAuth flow or token refresh.

        Args:
            response: The JSON response from Schwab's token endpoint

        Returns:
            TokenData object
        """
        token = TokenData(
            access_token=response["access_token"],
            refresh_token=response["refresh_token"],
            token_type=response.get("token_type", "Bearer"),
            expires_in=response.get("expires_in", 1800),
            scope=response.get("scope", ""),
            issued_at=time.time(),
        )
        self._save_token(token)
        self._token = token
        return token

    def get_token(self) -> TokenData:
        """
        Get the current token, loading from file if needed.

        Raises:
            TokenNotFoundError: If no token file exists
        """
        if self._token is None:
            self._token = self._load_token()

        if self._token is None:
            raise TokenNotFoundError(
                f"No token found at {self.token_path}. "
                "Complete OAuth flow first."
            )

        return self._token

    def is_token_valid(self) -> bool:
        """Check if we have a valid (non-expired) token."""
        try:
            token = self.get_token()
            return not token.is_expired
        except TokenNotFoundError:
            return False

    def needs_refresh(self) -> bool:
        """Check if token needs refresh (expired or expiring soon)."""
        try:
            token = self.get_token()
            return token.is_expired
        except TokenNotFoundError:
            return True

    def get_access_token(self, http_client: Any = None) -> str:
        """
        Get a valid access token, refreshing if necessary.

        Args:
            http_client: Optional HTTP client for refresh requests.
                        If None, returns current token without refresh.

        Returns:
            Valid access token string

        Raises:
            TokenExpiredError: If token is expired and no http_client provided
            TokenNotFoundError: If no token exists
        """
        token = self.get_token()

        if token.is_expired:
            if http_client is None:
                raise TokenExpiredError(
                    "Token expired. Provide http_client for refresh."
                )
            token = self.refresh_token(http_client)

        return token.access_token

    def refresh_token(self, http_client: Any) -> TokenData:
        """
        Refresh the access token using the refresh token.

        Args:
            http_client: HTTP client with a post() method (e.g., httpx.Client)

        Returns:
            New TokenData with refreshed access token

        Raises:
            SchwabAuthError: If refresh fails
        """
        token = self.get_token()

        logger.info("Refreshing Schwab access token...")

        try:
            response = http_client.post(
                self.TOKEN_URL,
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": token.refresh_token,
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            if response.status_code != 200:
                raise SchwabAuthError(
                    f"Token refresh failed: {response.status_code} - {response.text}"
                )

            data = response.json()
            new_token = self.save_token_response(data)
            logger.info(
                f"Token refreshed. Expires in {new_token.expires_in}s"
            )
            return new_token

        except Exception as e:
            logger.error(f"Token refresh failed: {e}")
            raise SchwabAuthError(f"Token refresh failed: {e}") from e

    def exchange_code(self, http_client: Any, auth_code: str) -> TokenData:
        """
        Exchange authorization code for tokens (initial OAuth flow).

        Args:
            http_client: HTTP client with a post() method
            auth_code: The authorization code from OAuth callback

        Returns:
            TokenData with access and refresh tokens
        """
        logger.info("Exchanging authorization code for tokens...")

        try:
            response = http_client.post(
                self.TOKEN_URL,
                data={
                    "grant_type": "authorization_code",
                    "code": auth_code,
                    "redirect_uri": self.redirect_uri,
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            if response.status_code != 200:
                raise SchwabAuthError(
                    f"Code exchange failed: {response.status_code} - {response.text}"
                )

            data = response.json()
            token = self.save_token_response(data)
            logger.info("Successfully obtained tokens")
            return token

        except Exception as e:
            logger.error(f"Code exchange failed: {e}")
            raise SchwabAuthError(f"Code exchange failed: {e}") from e

    def get_auth_header(self, http_client: Any = None) -> dict[str, str]:
        """
        Get the Authorization header for API requests.

        Args:
            http_client: Optional HTTP client for token refresh

        Returns:
            Dict with Authorization header
        """
        token = self.get_access_token(http_client)
        return {"Authorization": f"Bearer {token}"}

    def clear_token(self) -> None:
        """Clear stored token (for testing or re-auth)."""
        self._token = None
        if self.token_path.exists():
            self.token_path.unlink()
            logger.info(f"Token cleared: {self.token_path}")
