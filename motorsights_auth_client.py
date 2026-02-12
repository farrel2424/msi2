"""
Motorsights SSO Authentication Client
Handles dynamic bearer token generation for EPC API access

IMPORTANT: EPC API endpoint is https://dev-gateway.motorsights.com/api/epc/
(not https://dev-epc.motorsights.com/)
"""

import requests
import logging
from typing import Optional, Dict
from datetime import datetime, timedelta
import threading


class MotorsightsAuthClient:
    """Client for Motorsights SSO authentication"""
    
    def __init__(
        self, 
        gateway_url: str = "https://dev-gateway.motorsights.com",
        email: Optional[str] = None,
        password: Optional[str] = None
    ):
        """
        Initialize auth client
        
        Args:
            gateway_url: Motorsights gateway base URL
            email: SSO login email
            password: SSO login password
        """
        self.gateway_url = gateway_url.rstrip('/')
        self.email = email
        self.password = password
        self.logger = logging.getLogger(__name__)
        
        # Token storage
        self._token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None
        self._lock = threading.Lock()
        
        # Validate credentials
        if not self.email or not self.password:
            raise ValueError("Email and password are required for SSO authentication")
    
    def get_bearer_token(self, force_refresh: bool = False) -> str:
        """
        Get valid bearer token (SSO token)
        Automatically refreshes if expired
        
        Args:
            force_refresh: Force token refresh even if not expired
        
        Returns:
            Valid bearer token string
        
        Raises:
            Exception: If authentication fails
        """
        with self._lock:
            # Check if we have a valid token
            if not force_refresh and self._token and self._token_expiry:
                if datetime.now() < self._token_expiry:
                    self.logger.debug("Using cached bearer token")
                    return self._token
            
            # Need to get new token
            self.logger.info("Fetching new bearer token via SSO login")
            return self._fetch_new_token()
    
    def _fetch_new_token(self) -> str:
        """
        Fetch new bearer token from SSO endpoint
        
        Returns:
            Bearer token (sso_token)
        """
        url = f"{self.gateway_url}/api/auth/sso/login"
        
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json, text/plain, */*',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Referer': 'https://apps.motorsights.com/',
            'sec-ch-ua-platform': 'macOS',
            'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
            'sec-ch-ua-mobile': '?0'
        }
        
        payload = {
            'email': self.email,
            'password': self.password
        }
        
        try:
            response = requests.post(
                url,
                json=payload,
                headers=headers,
                timeout=30
            )
            
            response.raise_for_status()
            data = response.json()
            
            # Extract sso_token
            sso_token = data.get('sso_token')
            if not sso_token:
                # Try alternate response formats
                sso_token = data.get('token') or data.get('access_token')
            
            if not sso_token:
                raise ValueError(f"SSO token not found in response: {data}")
            
            # Store token with expiry (assume 24 hours if not specified)
            self._token = sso_token
            
            # Try to get expiry from response, default to 23 hours (to be safe)
            expires_in = data.get('expires_in', 82800)  # 23 hours in seconds
            self._token_expiry = datetime.now() + timedelta(seconds=expires_in)
            
            self.logger.info(f"Successfully obtained bearer token (expires in {expires_in}s)")
            self.logger.debug(f"Token: {sso_token[:20]}...")
            
            return sso_token
        
        except requests.exceptions.RequestException as e:
            self.logger.error(f"SSO login failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                self.logger.error(f"Response: {e.response.text}")
            raise Exception(f"Failed to obtain bearer token: {e}")
        
        except Exception as e:
            self.logger.error(f"Unexpected error during SSO login: {e}")
            raise
    
    def invalidate_token(self):
        """Invalidate current token to force refresh on next request"""
        with self._lock:
            self._token = None
            self._token_expiry = None
            self.logger.info("Bearer token invalidated")


# Convenience function for quick token generation
def get_bearer_token(
    email: str,
    password: str,
    gateway_url: str = "https://dev-gateway.motorsights.com"
) -> str:
    """
    Quick function to get a bearer token
    
    Args:
        email: SSO email
        password: SSO password
        gateway_url: Gateway URL
    
    Returns:
        Bearer token string
    """
    client = MotorsightsAuthClient(
        gateway_url=gateway_url,
        email=email,
        password=password
    )
    return client.get_bearer_token()