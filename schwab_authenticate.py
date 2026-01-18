"""
Schwab OAuth Authentication Script for Morpheus.

Run this script to authenticate with Schwab and obtain API tokens.
Based on the working implementation from AI_Project_bot.

Usage:
    python schwab_authenticate.py
"""

import base64
import json
import os
import webbrowser
from pathlib import Path
from urllib.parse import urlencode, urlparse, parse_qs

import httpx
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration from .env
APP_KEY = os.environ.get("SCHWAB_CLIENT_ID")
APP_SECRET = os.environ.get("SCHWAB_CLIENT_SECRET")
CALLBACK_URL = os.environ.get("SCHWAB_REDIRECT_URI", "https://127.0.0.1:6969")
TOKEN_PATH = os.environ.get("SCHWAB_TOKEN_PATH", "./tokens/schwab_token.json")


def get_auth_url() -> str:
    """Build the Schwab authorization URL."""
    params = {
        "response_type": "code",
        "client_id": APP_KEY,
        "redirect_uri": CALLBACK_URL,
        "scope": "readonly",
    }
    return f"https://api.schwabapi.com/v1/oauth/authorize?{urlencode(params)}"


def exchange_code_for_tokens(auth_code: str) -> dict:
    """Exchange authorization code for access/refresh tokens."""
    # Create Basic auth header with base64 encoded credentials
    credentials = f"{APP_KEY}:{APP_SECRET}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()

    headers = {
        "Authorization": f"Basic {encoded_credentials}",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    data = {
        "grant_type": "authorization_code",
        "code": auth_code,
        "redirect_uri": CALLBACK_URL,
    }

    with httpx.Client(timeout=30.0) as client:
        response = client.post(
            "https://api.schwabapi.com/v1/oauth/token",
            headers=headers,
            data=data,
        )

        if response.status_code != 200:
            print(f"Error: {response.status_code}")
            print(response.text)
            raise Exception(f"Token exchange failed: {response.status_code}")

        return response.json()


def save_tokens(token_data: dict):
    """Save tokens to file."""
    token_path = Path(TOKEN_PATH)
    token_path.parent.mkdir(parents=True, exist_ok=True)

    with open(token_path, "w") as f:
        json.dump(token_data, f, indent=2)

    print(f"\nTokens saved to: {token_path.absolute()}")


def main():
    print("=" * 60)
    print("SCHWAB OAUTH AUTHENTICATION")
    print("=" * 60)

    if not APP_KEY or not APP_SECRET:
        print("\nError: SCHWAB_CLIENT_ID and SCHWAB_CLIENT_SECRET not found in .env")
        print("Please configure these values in .env file")
        return

    print(f"\nApp Key: {APP_KEY[:8]}...")
    print(f"Callback URL: {CALLBACK_URL}")
    print(f"Token Path: {TOKEN_PATH}")

    # Build and display auth URL
    auth_url = get_auth_url()
    print("\n" + "=" * 60)
    print("STEP 1: Opening authorization URL in browser...")
    print("=" * 60)
    print(f"\nURL: {auth_url}")

    # Try to open browser
    try:
        webbrowser.open(auth_url)
        print("\n(Browser should open automatically)")
    except Exception:
        print("\n(Please copy and paste the URL into your browser)")

    print("\n" + "=" * 60)
    print("STEP 2: Log in to Schwab and authorize the application")
    print("=" * 60)
    print("\nAfter authorizing, you will be redirected to a URL like:")
    print(f"  {CALLBACK_URL}?code=AUTHORIZATION_CODE&session=...")
    print("\nCopy the ENTIRE redirect URL from your browser's address bar.")

    print("\n" + "=" * 60)
    print("STEP 3: Paste the redirect URL below")
    print("=" * 60)

    redirect_url = input("\nPaste redirect URL: ").strip()

    # Extract authorization code from URL
    try:
        parsed = urlparse(redirect_url)
        params = parse_qs(parsed.query)

        if "code" not in params:
            print("\nError: No 'code' parameter found in URL")
            print("Make sure you copied the complete redirect URL")
            return

        auth_code = params["code"][0]
        print(f"\nExtracted auth code: {auth_code[:20]}...")

    except Exception as e:
        print(f"\nError parsing URL: {e}")
        return

    print("\n" + "=" * 60)
    print("STEP 4: Exchanging code for tokens...")
    print("=" * 60)

    try:
        token_data = exchange_code_for_tokens(auth_code)
        save_tokens(token_data)

        print("\n" + "=" * 60)
        print("SUCCESS!")
        print("=" * 60)
        print(f"\nAccess token expires in: {token_data.get('expires_in', 'unknown')} seconds")
        print("\nYou can now restart Morpheus to use real market data.")

    except Exception as e:
        print(f"\nError: {e}")
        return


if __name__ == "__main__":
    main()
