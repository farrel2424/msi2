"""
Motorsights SSO Authentication Client
Generates and caches bearer tokens for EPC API access.

EPC API base: https://dev-gateway.motorsights.com/api/epc/
"""

import logging
import threading
from datetime import datetime, timedelta
from typing import Optional

import requests


class MotorsightsAuthClient:
    """Thread-safe SSO authentication client with automatic token caching."""

    def __init__(
        self,
        gateway_url: str = "https://dev-gateway.motorsights.com",
        email: Optional[str] = None,
        password: Optional[str] = None,
    ):
        if not email or not password:
            raise ValueError("Email and password are required for SSO authentication.")

        self.gateway_url = gateway_url.rstrip("/")
        self.email = email
        self.password = password
        self.logger = logging.getLogger(__name__)

        self._token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None
        self._lock = threading.Lock()

    def get_bearer_token(self, force_refresh: bool = False) -> str:
        """
        Return a valid bearer token, fetching a new one when necessary.

        Args:
            force_refresh: Skip cache and always fetch a fresh token.

        Returns:
            Valid SSO bearer token string.
        """
        with self._lock:
            if (
                not force_refresh
                and self._token
                and self._token_expiry
                and datetime.now() < self._token_expiry
            ):
                self.logger.debug("Using cached bearer token.")
                return self._token
            return self._fetch_new_token()

    def _fetch_new_token(self) -> str:
        """Fetch a new SSO token from the gateway and cache it."""
        url = f"{self.gateway_url}/api/auth/sso/login"
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
            ),
            "Referer": "https://apps.motorsights.com/",
            "sec-ch-ua-platform": "macOS",
            "sec-ch-ua": '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
        }

        try:
            response = requests.post(
                url, json={"email": self.email, "password": self.password},
                headers=headers, timeout=30,
            )
            response.raise_for_status()
            data = response.json()

            # Token may be at data.data.oauth.sso_token or top-level fallbacks
            sso_token = (
                data.get("data", {}).get("oauth", {}).get("sso_token")
                or data.get("sso_token")
                or data.get("token")
                or data.get("access_token")
            )
            if not sso_token:
                raise ValueError(f"SSO token not found in response: {data}")

            expires_in = (
                data.get("data", {}).get("oauth", {}).get("expires_in")
                or data.get("expires_in", 82800)  # default: 23 hours
            )
            self._token = sso_token
            self._token_expiry = datetime.now() + timedelta(seconds=expires_in)

            self.logger.info("Obtained bearer token (expires in %ss).", expires_in)
            self.logger.debug("Token prefix: %s...", sso_token[:20])
            return sso_token

        except requests.exceptions.RequestException as e:
            if hasattr(e, "response") and e.response is not None:
                self.logger.error("SSO response: %s", e.response.text)
            raise Exception(f"Failed to obtain bearer token: {e}") from e

    def invalidate_token(self) -> None:
        """Clear the cached token, forcing a fresh fetch on the next request."""
        with self._lock:
            self._token = None
            self._token_expiry = None
            self.logger.info("Bearer token invalidated.")


def get_bearer_token(
    email: str,
    password: str,
    gateway_url: str = "https://dev-gateway.motorsights.com",
) -> str:
    """Convenience function: create a client and return a single token."""
    return MotorsightsAuthClient(
        gateway_url=gateway_url, email=email, password=password
    ).get_bearer_token()