"""
Motorsights EPC API Client
Handles all interactions with the Motorsights EPC API via SSO-authenticated requests.
"""

import logging
from typing import Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from motorsights_auth_client import MotorsightsAuthClient


class MotorsightsEPCClient:
    """Client for the Motorsights Electronic Product Catalog API."""

    def __init__(
        self,
        base_url: str,
        bearer_token: Optional[str] = None,
        auth_client: Optional[MotorsightsAuthClient] = None,
        max_retries: int = 3,
    ):
        if not bearer_token and not auth_client:
            raise ValueError("Either bearer_token or auth_client must be provided.")

        self.base_url = base_url.rstrip("/")
        self.bearer_token = bearer_token
        self.auth_client = auth_client
        self.logger = logging.getLogger(__name__)
        self.session = self._create_session(max_retries)

    # ------------------------------------------------------------------
    # Session & auth helpers
    # ------------------------------------------------------------------

    def _create_session(self, max_retries: int) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=max_retries,
            backoff_factor=2.0,
            # 409 intentionally excluded — handled explicitly in create_category
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST", "PUT", "DELETE"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _get_bearer_token(self) -> str:
        return self.auth_client.get_bearer_token() if self.auth_client else self.bearer_token

    def _get_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_bearer_token()}",
            "Content-Type": "application/json",
        }

    def _with_401_retry(self, func, *args, **kwargs):
        """Execute func; on 401 invalidate the SSO token and retry once."""
        try:
            return func(*args, **kwargs)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401 and self.auth_client:
                self.logger.warning("Got 401, refreshing token and retrying...")
                self.auth_client.invalidate_token()
                return func(*args, **kwargs)
            raise

    # ------------------------------------------------------------------
    # Generic request helper
    # ------------------------------------------------------------------

    def _api_request(
        self,
        method: str,
        endpoint: str,
        json_data: Optional[Dict] = None,
    ) -> Tuple[bool, Optional[Dict]]:
        """
        Make an authenticated API request with automatic 401 token refresh.

        Args:
            method:    HTTP method ("GET" or "POST").
            endpoint:  Path appended to base_url (e.g. "/categories/get").
            json_data: Optional JSON body.

        Returns:
            (success, response_json) — (False, None) on any request error.
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        def _call():
            resp = self.session.request(
                method, url,
                json=json_data,
                headers=self._get_headers(),
                timeout=30,
            )
            resp.raise_for_status()
            return True, resp.json()

        try:
            return self._with_401_retry(_call)
        except requests.exceptions.RequestException as e:
            self.logger.error("Request failed [%s %s]: %s", method.upper(), endpoint, e)
            return False, None

    # ------------------------------------------------------------------
    # Master Category endpoints
    # ------------------------------------------------------------------

    def get_master_categories(self, filters: Optional[Dict] = None) -> Tuple[bool, Optional[Dict]]:
        return self._api_request("POST", "master_category/get", json_data=filters or {})

    def create_master_category(self, data: Dict) -> Tuple[bool, Optional[Dict]]:
        return self._api_request("POST", "master_category/create", json_data=data)

    def get_master_category_by_id(self, category_id: str) -> Tuple[bool, Optional[Dict]]:
        return self._api_request("GET", f"master_category/{category_id}")

    # ------------------------------------------------------------------
    # Type Category endpoints
    # ------------------------------------------------------------------

    def get_type_categories(self, filters: Optional[Dict] = None) -> Tuple[bool, Optional[Dict]]:
        return self._api_request("POST", "type_category/get", json_data=filters or {})

    def create_type_category(self, data: Dict) -> Tuple[bool, Optional[Dict]]:
        success, result = self._api_request("POST", "type_category/create", json_data=data)
        if success and result and not result.get("success", False):
            self.logger.error("API returned error: %s", result.get("error", "Unknown"))
            return False, result
        if success:
            self.logger.info(
                "Created type category: %s",
                result.get("data", {}).get("type_category_name_en"),
            )
        return success, result

    def get_type_category_by_id(self, type_category_id: str) -> Tuple[bool, Optional[Dict]]:
        return self._api_request("GET", f"type_category/{type_category_id}")

    # ------------------------------------------------------------------
    # Categories endpoints
    # ------------------------------------------------------------------

    def get_categories(self, filters: Optional[Dict] = None) -> Tuple[bool, Optional[Dict]]:
        return self._api_request("POST", "categories/get", json_data=filters or {})

    def create_category(self, data: Dict) -> Tuple[bool, Optional[Dict], bool]:
        """
        Create a category (with optional nested type categories).

        Returns:
            (success, response_data, was_skipped)
            was_skipped=True means a 409 conflict — the category already exists
            and was skipped gracefully; this does NOT count as an error.
        """
        url = f"{self.base_url}/categories/create"

        def _call():
            resp = self.session.post(url, json=data, headers=self._get_headers(), timeout=30)

            if resp.status_code == 409:
                name = data.get("category_name_en", "(unknown)")
                msg = resp.json().get("message", "Conflict")
                self.logger.warning("Skipping duplicate category '%s': %s", name, msg)
                return True, {"skipped": True, "message": msg, "data": data}, True

            resp.raise_for_status()
            result = resp.json()

            if not result.get("success", False):
                self.logger.error("API returned error: %s", result.get("error", "Unknown"))
                return False, result, False

            self.logger.info(
                "Created category: %s",
                result.get("data", {}).get("category_name_en", "(unknown)"),
            )
            return True, result, False

        try:
            return self._with_401_retry(_call)
        except requests.exceptions.RequestException as e:
            self.logger.error("Failed to create category: %s", e)
            if hasattr(e, "response") and e.response is not None:
                self.logger.error("Response: %s", e.response.text)
            return False, None, False

    def get_category_by_id(self, category_id: str) -> Tuple[bool, Optional[Dict]]:
        return self._api_request("GET", f"categories/{category_id}")

    # ------------------------------------------------------------------
    # Products endpoints
    # ------------------------------------------------------------------

    def create_product(self, data: Dict) -> Tuple[bool, Optional[Dict]]:
        return self._api_request("POST", "products/create", json_data=data)

    # ------------------------------------------------------------------
    # Batch operations
    # ------------------------------------------------------------------

    def _empty_batch_results(self) -> Dict:
        return {
            "categories_created": [],
            "categories_skipped": [],
            "type_categories_created": [],
            "errors": [],
        }

    def _handle_category_response(
        self,
        success: bool,
        cat_response: Optional[Dict],
        was_skipped: bool,
        category_name_en: str,
        results: Dict,
    ) -> None:
        """Update results dict based on a single create_category call outcome."""
        if success and was_skipped:
            results["categories_skipped"].append({
                "category_name_en": category_name_en,
                "message": cat_response.get("message", "Already exists"),
            })
            self.logger.info("Skipped existing category '%s'", category_name_en)
        elif success:
            data = cat_response.get("data", {})
            results["categories_created"].append(data)
            nested = data.get("data_type", [])
            results["type_categories_created"].extend(nested)
            self.logger.info(
                "Created category '%s' with %d type categories",
                category_name_en, len(nested),
            )
        else:
            error = cat_response.get("error") if cat_response else "Unknown error"
            results["errors"].append({"type": "category", "error": error})
            self.logger.error("Failed to create category '%s': %s", category_name_en, error)

    def batch_create_type_categories_and_categories(
        self,
        catalog_data: Dict,
        master_category_id: str,
        master_category_name_en: Optional[str] = None,
    ) -> Tuple[bool, Dict]:
        """
        Batch-create categories WITH nested type categories (Cabin & Chassis / Axle Drive).
        3-level hierarchy: Master Category → Category → Type Category.
        Duplicate categories (409) are skipped gracefully.
        """
        if not master_category_id:
            raise ValueError("master_category_id is required.")

        results = self._empty_batch_results()

        for pdf_cat in catalog_data.get("categories", []):
            data_type = [
                {
                    "type_category_name_en": tc.get("type_category_name_en", ""),
                    "type_category_name_cn": tc.get("type_category_name_cn", ""),
                    "type_category_description": tc.get("type_category_description", ""),
                }
                for tc in pdf_cat.get("data_type", [])
            ]

            cat_request = {
                "master_category_id": master_category_id,
                "master_category_name_en": master_category_name_en or "",
                "category_name_en": pdf_cat.get("category_name_en", ""),
                "category_name_cn": pdf_cat.get("category_name_cn", ""),
                "category_description": pdf_cat.get(
                    "category_description",
                    f"Category for {pdf_cat.get('category_name_en', '')}",
                ),
                "data_type": data_type,
            }

            success, cat_response, was_skipped = self.create_category(cat_request)
            self._handle_category_response(
                success, cat_response, was_skipped,
                pdf_cat.get("category_name_en", ""), results,
            )

        self._log_batch_summary(results)
        return len(results["errors"]) == 0, results

    def batch_create_flat_categories(
        self,
        catalog_data: Dict,
        master_category_id: str,
        master_category_name_en: Optional[str] = None,
    ) -> Tuple[bool, Dict]:
        """
        Batch-create flat categories WITHOUT type categories (Engine / Transmission).
        2-level hierarchy: Master Category → Category.
        Duplicate categories (409) are skipped gracefully.
        """
        if not master_category_id:
            raise ValueError("master_category_id is required.")

        results = self._empty_batch_results()

        for cat in catalog_data.get("categories", []):
            cat_request = {
                "master_category_id": master_category_id,
                "master_category_name_en": master_category_name_en or "",
                "category_name_en": cat.get("category_name_en", ""),
                "category_name_cn": cat.get("category_name_cn", ""),
                "category_description": cat.get("category_description", ""),
                "data_type": [],
            }

            success, cat_response, was_skipped = self.create_category(cat_request)
            self._handle_category_response(
                success, cat_response, was_skipped,
                cat.get("category_name_en", ""), results,
            )

        self._log_batch_summary(results)
        return len(results["errors"]) == 0, results

    def _log_batch_summary(self, results: Dict) -> None:
        self.logger.info(
            "Batch complete: %d created · %d skipped · %d type categories · %d errors",
            len(results["categories_created"]),
            len(results["categories_skipped"]),
            len(results["type_categories_created"]),
            len(results["errors"]),
        )