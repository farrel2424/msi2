"""
motorsights_epc_client.py
─────────────────────────────────────────────────────────────────────────────
Motorsights EPC API Client

Handles all interactions with the Motorsights EPC API including:
  - Master Categories, Categories, Type Categories
  - Item Categories (Parts Management)
  - Batch operations for both hierarchy paths
  - Parts Management: create_item_category_with_parts, batch_submit_parts
  - T-number sequencing: get_next_target_id_start

CORRECTED based on actual network inspection — simplified format without codes.
"""

from __future__ import annotations

import json
import re
import requests
from typing import Dict, List, Optional, Tuple
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from motorsights_auth_client import MotorsightsAuthClient


class MotorsightsEPCClient:
    """Client for Motorsights Electronic Product Catalog API"""

    def __init__(
        self,
        base_url: str,
        bearer_token: Optional[str] = None,
        auth_client: Optional[MotorsightsAuthClient] = None,
        max_retries: int = 3
    ):
        self.base_url     = base_url.rstrip("/")
        self.bearer_token = bearer_token
        self.auth_client  = auth_client
        self.logger       = logging.getLogger(__name__)
        self.session      = self._create_session(max_retries)

        if not bearer_token and not auth_client:
            raise ValueError("Either bearer_token or auth_client must be provided")

    def _create_session(self, max_retries: int) -> requests.Session:
        """Create requests session with retry configuration"""
        session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=2.0,
            # 409 intentionally excluded — handled manually
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST", "PUT", "DELETE"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _get_bearer_token(self) -> str:
        if self.auth_client:
            return self.auth_client.get_bearer_token()
        return self.bearer_token

    def _get_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_bearer_token()}",
            "Content-Type":  "application/json"
        }

    def _handle_401_retry(self, func, *args, **kwargs):
        try:
            return func(*args, **kwargs)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401 and self.auth_client:
                self.logger.warning("Got 401, refreshing bearer token and retrying…")
                self.auth_client.invalidate_token()
                return func(*args, **kwargs)
            else:
                raise

    # =========================================================================
    # MASTER CATEGORY ENDPOINTS
    # =========================================================================

    def get_master_categories(self, filters: Optional[Dict] = None) -> Tuple[bool, Optional[Dict]]:
        url = f"{self.base_url}/master_category/get"
        def _request():
            r = self.session.post(url, json=filters or {}, headers=self._get_headers(), timeout=30)
            r.raise_for_status()
            return True, r.json()
        try:
            return self._handle_401_retry(_request)
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get master categories: {e}")
            return False, None

    def create_master_category(self, data: Dict) -> Tuple[bool, Optional[Dict]]:
        url = f"{self.base_url}/master_category/create"
        def _request():
            r = self.session.post(url, json=data, headers=self._get_headers(), timeout=30)
            r.raise_for_status()
            return True, r.json()
        try:
            return self._handle_401_retry(_request)
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to create master category: {e}")
            return False, None

    def get_master_category_by_id(self, category_id: str) -> Tuple[bool, Optional[Dict]]:
        url = f"{self.base_url}/master_category/{category_id}"
        def _request():
            r = self.session.get(url, headers=self._get_headers(), timeout=30)
            r.raise_for_status()
            return True, r.json()
        try:
            return self._handle_401_retry(_request)
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get master category {category_id}: {e}")
            return False, None

    # =========================================================================
    # TYPE CATEGORY ENDPOINTS
    # =========================================================================

    def get_type_categories(self, filters: Optional[Dict] = None) -> Tuple[bool, Optional[Dict]]:
        url = f"{self.base_url}/type_category/get"
        def _request():
            r = self.session.post(url, json=filters or {}, headers=self._get_headers(), timeout=30)
            r.raise_for_status()
            return True, r.json()
        try:
            return self._handle_401_retry(_request)
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get type categories: {e}")
            return False, None

    def create_type_category(self, data: Dict) -> Tuple[bool, Optional[Dict]]:
        url = f"{self.base_url}/type_category/create"
        def _request():
            r = self.session.post(url, json=data, headers=self._get_headers(), timeout=30)
            r.raise_for_status()
            result = r.json()
            if not result.get("success", False):
                self.logger.error(f"API returned error: {result.get('error', 'Unknown')}")
                return False, result
            self.logger.info(
                f"Created type category: "
                f"{result.get('data', {}).get('type_category_name_en')}"
            )
            return True, result
        try:
            return self._handle_401_retry(_request)
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to create type category: {e}")
            if hasattr(e, "response") and e.response is not None:
                self.logger.error(f"Response: {e.response.text}")
            return False, None

    def get_type_category_by_id(self, type_category_id: str) -> Tuple[bool, Optional[Dict]]:
        url = f"{self.base_url}/type_category/{type_category_id}"
        def _request():
            r = self.session.get(url, headers=self._get_headers(), timeout=30)
            r.raise_for_status()
            return True, r.json()
        try:
            return self._handle_401_retry(_request)
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get type category {type_category_id}: {e}")
            return False, None

    # =========================================================================
    # CATEGORIES ENDPOINTS
    # =========================================================================

    def get_categories(self, filters: Optional[Dict] = None) -> Tuple[bool, Optional[Dict]]:
        url = f"{self.base_url}/categories/get"
        def _request():
            r = self.session.post(url, json=filters or {}, headers=self._get_headers(), timeout=30)
            r.raise_for_status()
            return True, r.json()
        try:
            return self._handle_401_retry(_request)
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get categories: {e}")
            return False, None

    def create_category(self, data: Dict) -> Tuple[bool, Optional[Dict], bool]:
        """
        Create a category with optional type categories.

        Returns:
            Tuple (success, response_data, was_skipped)
            was_skipped=True means the category already existed (409).
        """
        url = f"{self.base_url}/categories/create"

        def _request():
            r = self.session.post(url, json=data, headers=self._get_headers(), timeout=30)
            # Handle 409 conflict (duplicate category name) gracefully
            if r.status_code == 409:
                self.logger.warning(
                    f"Category already exists (409): "
                    f"{data.get('category_name_en', 'unknown')}"
                )
                return True, {"message": "Already exists", "skipped": True}, True
            r.raise_for_status()
            result = r.json()
            if not result.get("success", False):
                self.logger.error(f"API returned error: {result.get('error', 'Unknown')}")
                return False, result, False
            self.logger.info(
                f"Created category: {result.get('data', {}).get('category_name_en')}"
            )
            return True, result, False

        try:
            return self._handle_401_retry(_request)
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to create category: {e}")
            if hasattr(e, "response") and e.response is not None:
                self.logger.error(f"Response: {e.response.text}")
            return False, None, False

    def get_category_by_id(self, category_id: str) -> Tuple[bool, Optional[Dict]]:
        url = f"{self.base_url}/categories/{category_id}"
        def _request():
            r = self.session.get(url, headers=self._get_headers(), timeout=30)
            r.raise_for_status()
            return True, r.json()
        try:
            return self._handle_401_retry(_request)
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get category {category_id}: {e}")
            return False, None

    # =========================================================================
    # ITEM CATEGORY ENDPOINTS (Parts Management)
    # =========================================================================

    def get_item_category_by_id(self, item_category_id: str) -> Tuple[bool, Optional[Dict]]:
        """Fetch a single item_category with its details (parts list)."""
        url = f"{self.base_url}/item_category/{item_category_id}"
        def _request():
            r = self.session.get(url, headers=self._get_headers(), timeout=30)
            r.raise_for_status()
            return True, r.json()
        try:
            return self._handle_401_retry(_request)
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get item_category {item_category_id}: {e}")
            return False, None

    def get_next_target_id_start(self, item_category_id: str) -> int:
        """
        Query the existing parts for an item_category and return the next
        available T-number index (1-based integer).

        Examples:
          • No existing parts → 1   (first part gets T001)
          • Last part is T029 → 30  (next part gets T030)

        Args:
            item_category_id: UUID of the item_category to inspect.

        Returns:
            int — the index to pass as target_id_start to the extractor.
        """
        success, data = self.get_item_category_by_id(item_category_id)
        if not success or not data:
            self.logger.warning(
                "Could not fetch item_category %s — defaulting to T001",
                item_category_id
            )
            return 1

        details = (data.get("data") or {}).get("details") or []
        if not details:
            return 1

        max_t = 0
        for item in details:
            tid = item.get("target_id") or ""
            m = re.match(r"^T(\d+)$", tid)
            if m:
                max_t = max(max_t, int(m.group(1)))

        next_index = max_t + 1
        self.logger.info(
            "item_category %s: last T-ID is T%03d → next index is %d",
            item_category_id, max_t, next_index
        )
        return next_index

    def create_item_category_with_parts(
        self,
        master_category_id: str,
        category_id: Optional[str],
        type_category_id: Optional[str],
        item_category_name_en: str,
        item_category_name_cn: str,
        item_category_description: str,
        dokumen_name: str,
        parts: List[Dict]
    ) -> Tuple[bool, Optional[Dict]]:
        """
        Create one item_category (Parts Management entry) with its parts rows.

        Calls POST /item_category/create as multipart/form-data.
        The `data_items` field is a JSON-encoded string of parts rows.

        Hierarchy routing (per API docs):
          • If type_category_id provided → 3-level (subtype present)
          • Else if category_id provided → 2-level (no subtype)

        Args:
            master_category_id:         UUID of master category.
            category_id:                UUID of category (2-level fallback).
            type_category_id:           UUID of type/subtype category (preferred).
            item_category_name_en:      English name (usually == subtype name).
            item_category_name_cn:      Chinese name.
            item_category_description:  Description string (may be empty).
            dokumen_name:               Document/partbook name.
            parts:                      List of part dicts:
                                          target_id, part_number,
                                          catalog_item_name_en, catalog_item_name_ch,
                                          quantity, description, unit

        Returns:
            Tuple (success: bool, response_data: Optional[Dict])
        """
        url = f"{self.base_url}/item_category/create"

        if not type_category_id and not category_id:
            raise ValueError(
                "Either type_category_id or category_id must be provided"
            )

        # Build data_items JSON array (the parts rows)
        data_items = []
        for p in parts:
            data_items.append({
                "target_id":            p.get("target_id", ""),
                "part_number":          p.get("part_number", ""),
                "catalog_item_name_en": p.get("catalog_item_name_en", ""),
                "catalog_item_name_ch": p.get("catalog_item_name_ch", ""),
                "description":          p.get("description", ""),
                "quantity":             int(p.get("quantity", 1)),
                "unit":                 p.get("unit", "pcs"),
            })

        # Build multipart form fields
        # NOTE: requests multipart format is {field: (filename, value, content_type)}
        # For text fields use (None, value) — no filename, no content_type.
        form_data = {
            "dokumen_name":              (None, dokumen_name),
            "master_category_id":        (None, master_category_id),
            "item_category_name_en":     (None, item_category_name_en),
            "item_category_name_cn":     (None, item_category_name_cn),
            "item_category_description": (None, item_category_description),
            "data_items":                (None, json.dumps(data_items, ensure_ascii=False)),
        }

        # Route hierarchy
        if type_category_id:
            form_data["type_category_id"] = (None, type_category_id)
            self.logger.debug(
                "create_item_category_with_parts: type_category_id=%s (3-level)",
                type_category_id
            )
        else:
            form_data["category_id"] = (None, category_id)
            self.logger.debug(
                "create_item_category_with_parts: category_id=%s (2-level)",
                category_id
            )

        def _request():
            # Multipart: do NOT send Content-Type manually —
            # requests sets it with the boundary automatically.
            headers = {"Authorization": f"Bearer {self._get_bearer_token()}"}
            r = self.session.post(url, files=form_data, headers=headers, timeout=60)
            r.raise_for_status()
            return True, r.json()

        try:
            return self._handle_401_retry(_request)
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response else "?"
            body   = e.response.text[:500] if e.response else ""
            self.logger.error(
                "create_item_category_with_parts HTTP %s: %s", status, body
            )
            return False, {"error": f"HTTP {status}: {body}"}
        except Exception as e:
            self.logger.error("create_item_category_with_parts failed: %s", e)
            return False, {"error": str(e)}

    # =========================================================================
    # PRODUCTS ENDPOINTS
    # =========================================================================

    def create_product(self, data: Dict) -> Tuple[bool, Optional[Dict]]:
        url = f"{self.base_url}/products/create"
        def _request():
            r = self.session.post(url, json=data, headers=self._get_headers(), timeout=30)
            r.raise_for_status()
            return True, r.json()
        try:
            return self._handle_401_retry(_request)
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to create product: {e}")
            return False, None

    # =========================================================================
    # BATCH OPERATIONS — CATEGORY STRUCTURE
    # =========================================================================

    def batch_create_type_categories_and_categories(
        self,
        catalog_data: Dict,
        master_category_id: str,
        master_category_name_en: Optional[str] = None
    ) -> Tuple[bool, Dict]:
        """
        Batch create categories WITH nested type categories (Cabin & Chassis /
        Axle Drive).

        3-level hierarchy:  Master Category → Category → Type Category (subtype)

        409 conflicts are skipped gracefully and counted separately.
        """
        if not master_category_id:
            raise ValueError("master_category_id is required and must be a valid UUID")

        results = {
            "categories_created":      [],
            "categories_skipped":      [],
            "type_categories_created": [],
            "errors":                  []
        }

        for pdf_category in catalog_data.get("categories", []):
            data_type = [
                {
                    "type_category_name_en":  tc.get("type_category_name_en", ""),
                    "type_category_name_cn":  tc.get("type_category_name_cn", ""),
                    "type_category_description": tc.get("type_category_description", ""),
                }
                for tc in pdf_category.get("data_type", [])
            ]

            category_request = {
                "master_category_id":   master_category_id,
                "master_category_name_en": master_category_name_en or "",
                "category_name_en":     pdf_category.get("category_name_en", ""),
                "category_name_cn":     pdf_category.get("category_name_cn", ""),
                "category_description": pdf_category.get(
                    "category_description",
                    f"Category for {pdf_category.get('category_name_en', '')}"
                ),
                "data_type": data_type
            }

            self.logger.debug(f"Creating category: {category_request}")
            success, cat_response, was_skipped = self.create_category(category_request)

            if success and was_skipped:
                results["categories_skipped"].append({
                    "category_name_en": pdf_category.get("category_name_en", ""),
                    "message":          cat_response.get("message", "Already exists")
                })
                self.logger.info(
                    "Skipped existing category '%s'",
                    pdf_category.get("category_name_en", "")
                )
            elif success:
                results["categories_created"].append(cat_response.get("data", {}))
                nested_types = cat_response.get("data", {}).get("data_type", [])
                results["type_categories_created"].extend(nested_types)
                self.logger.info(
                    "Created category '%s' with %d type categories",
                    pdf_category.get("category_name_en", ""), len(nested_types)
                )
            else:
                err = cat_response.get("error") if cat_response else "Unknown error"
                results["errors"].append({
                    "type": "category", "data": category_request, "error": err
                })
                self.logger.error("Failed to create category: %s", err)

        overall_success = len(results["errors"]) == 0
        self.logger.info(
            "Batch complete: %d created, %d skipped, %d type cats, %d errors",
            len(results["categories_created"]),
            len(results["categories_skipped"]),
            len(results["type_categories_created"]),
            len(results["errors"])
        )
        return overall_success, results

    def batch_create_flat_categories(
        self,
        catalog_data: Dict,
        master_category_id: str,
        master_category_name_en: Optional[str] = None
    ) -> Tuple[bool, Dict]:
        """
        Batch create flat categories WITHOUT type categories (Engine / Transmission).

        2-level hierarchy: Master Category → Category
        """
        if not master_category_id:
            raise ValueError("master_category_id is required and must be a valid UUID")

        results = {
            "categories_created":      [],
            "categories_skipped":      [],
            "type_categories_created": [],
            "errors":                  []
        }

        for cat in catalog_data.get("categories", []):
            category_request = {
                "master_category_id":    master_category_id,
                "master_category_name_en": master_category_name_en or "",
                "category_name_en":      cat.get("category_name_en", ""),
                "category_name_cn":      cat.get("category_name_cn", ""),
                "category_description":  cat.get("category_description", ""),
                "data_type":             []
            }

            success, cat_response, was_skipped = self.create_category(category_request)

            if success and was_skipped:
                results["categories_skipped"].append({
                    "category_name_en": cat.get("category_name_en", ""),
                    "message":          cat_response.get("message", "Already exists")
                })
                self.logger.info("Skipped '%s'", cat.get("category_name_en", ""))
            elif success:
                results["categories_created"].append(cat_response.get("data", {}))
                self.logger.info("Created '%s'", cat.get("category_name_en", ""))
            else:
                err = cat_response.get("error") if cat_response else "Unknown error"
                results["errors"].append({
                    "type": "category", "data": category_request, "error": err
                })
                self.logger.error(
                    "Failed to create '%s': %s", cat.get("category_name_en", ""), err
                )

        overall_success = len(results["errors"]) == 0
        self.logger.info(
            "Flat batch complete: %d created, %d skipped, %d errors",
            len(results["categories_created"]),
            len(results["categories_skipped"]),
            len(results["errors"])
        )
        return overall_success, results

# =========================================================================
    # BATCH OPERATIONS — PARTS MANAGEMENT
    # =========================================================================

    def resolve_category_id_by_name(
        self,
        category_name_en: str,
        master_category_id: Optional[str] = None,
    ) -> Optional[str]:
        """Look up category_id by English name."""
        url = f"{self.base_url}/categories/get"
        # Per API docs, master_category_id is a supported filter field.
        # Passing it scopes results to that master category — required to get results.
        payload = {
            "page": 1,
            "limit": 500,
            "search": "",
            "master_category_id": master_category_id,  # null = all, UUID = scoped
        }

        def _request():
            r = self.session.post(url, json=payload, headers=self._get_headers(), timeout=30)
            r.raise_for_status()
            return True, r.json()
        try:
            success, result = self._handle_401_retry(_request)
            if not success or not result:
                return None
        except requests.exceptions.RequestException as e:
            self.logger.error("resolve_category_id_by_name failed: %s", e)
            return None

        data  = result.get("data", []) if isinstance(result, dict) else result
        items = data.get("items", [])  if isinstance(data, dict)   else data
        if not isinstance(items, list):
            items = []

        self.logger.debug(
            "resolve_category_id_by_name('%s'): %d categories returned",
            category_name_en, len(items)
        )

        for item in items:
            en = (item.get("category_name_en") or "").strip().lower()
            if en == category_name_en.strip().lower():
                if master_category_id is None or item.get("master_category_id") == master_category_id:
                    return item.get("category_id")

        self.logger.warning(
            "resolve_category_id_by_name: '%s' not found among %d categories. "
            "Available: %s",
            category_name_en,
            len(items),
            [i.get("category_name_en") for i in items[:20]],
        )
        return None

    def resolve_type_category_id_by_name(
        self,
        type_category_name_en: str,
        category_id: Optional[str] = None,
        subtype_code: Optional[str] = None,
    ) -> Optional[str]:
        """Look up type_category_id by English name, with code-prefix fallback."""
        candidates = [type_category_name_en.strip()]
        if subtype_code:
            candidates.append(f"{subtype_code} {type_category_name_en}".strip())

        url = f"{self.base_url}/type_category/get"
        # Per API docs, category_id is a supported filter field.
        # Passing it scopes results to that category's subtypes.
        payload = {
            "page": 1,
            "limit": 500,
            "search": "",
            "category_id": category_id,  # null = all, UUID = scoped
        }

        def _request():
            r = self.session.post(url, json=payload, headers=self._get_headers(), timeout=30)
            r.raise_for_status()
            return True, r.json()
        try:
            success, result = self._handle_401_retry(_request)
            if not success or not result:
                return None
        except requests.exceptions.RequestException as e:
            self.logger.error("resolve_type_category_id_by_name failed: %s", e)
            return None

        data  = result.get("data", []) if isinstance(result, dict) else result
        items = data.get("items", [])  if isinstance(data, dict)   else data
        if not isinstance(items, list):
            items = []

        self.logger.debug(
            "resolve_type_category_id_by_name('%s'): %d type categories returned",
            type_category_name_en, len(items)
        )

        for item in items:
            en = (item.get("type_category_name_en") or "").strip()
            if any(en.lower() == c.lower() for c in candidates):
                if category_id is None or item.get("category_id") == category_id:
                    return item.get("type_category_id")

        self.logger.warning(
            "resolve_type_category_id_by_name: '%s' not found among %d type categories. "
            "Available: %s",
            type_category_name_en,
            len(items),
            [i.get("type_category_name_en") for i in items[:20]],
        )
        return None

    def resolve_type_category_id_by_name(
        self,
        type_category_name_en: str,
        category_id: Optional[str] = None,
        subtype_code: Optional[str] = None,
    ) -> Optional[str]:
        """Look up type_category_id by English name, with code-prefix fallback."""
        candidates = [type_category_name_en.strip()]
        if subtype_code:
            candidates.append(f"{subtype_code} {type_category_name_en}".strip())
        url = f"{self.base_url}/type_category/get"
        def _request():
            r = self.session.post(
                url,
                json={"page": 1, "limit": 200, "search": type_category_name_en},
                headers=self._get_headers(),
                timeout=30,
            )
            r.raise_for_status()
            return True, r.json()
        try:
            success, result = self._handle_401_retry(_request)
            if not success or not isinstance(result, dict):
                return None
        except requests.exceptions.RequestException as e:
            self.logger.error("resolve_type_category_id_by_name failed: %s", e)
            return None
        if not success or not result:
            return None
        for item in result.get("data", {}).get("items", []):
            en = (item.get("type_category_name_en") or "").strip()
            if any(en.lower() == c.lower() for c in candidates):
                if category_id is None or item.get("category_id") == category_id:
                    return item.get("type_category_id")
        return None

    def batch_submit_parts(
        self,
        parts_data: List[Dict],
        master_category_id: str,
        dokumen_name: str,
        category_id: Optional[str] = None,
        subtype_id_map: Optional[Dict[str, str]] = None,
    ) -> Tuple[bool, Dict]:
        """
        Submit all parts groups from extract_cabin_chassis_parts() to the API.

        For each subtype group:
          1. Resolve type_category_id — first via subtype_id_map, then by
             looking up category_name_en → category_id → type_category_id
             from the DB (supports plain name AND code-prefixed name).
          2. Call create_item_category_with_parts().
          3. Track results (created / skipped / errors).

        Args:
            parts_data:        List of subtype groups from the extractor.
            master_category_id: UUID of the master category.
            dokumen_name:       Document name passed to the API.
            category_id:        UUID of the Category (2-level fallback).
            subtype_id_map:     Optional map subtype_code/name → type_category_id.

        Returns:
            Tuple (overall_success: bool, results: Dict)
        """
        results = {
            "created": [],
            "skipped": [],
            "total_parts_submitted": 0,
            "errors":                []
        }

        # Cache category_id lookups — many subtypes share the same parent category
        category_id_cache: Dict[str, Optional[str]] = {}

        for group in parts_data:
            subtype_code    = group.get("subtype_code", "")
            subtype_name_en = group.get("subtype_name_en", "")
            subtype_name_cn = group.get("subtype_name_cn", "")
            cat_en          = (group.get("category_name_en") or "").strip()
            parts           = group.get("parts", [])

            if not parts:
                self.logger.info("Subtype '%s': no parts, skipping", subtype_name_en)
                continue

            # ── Step 1: resolve type_category_id ─────────────────────────
            type_cat_id = None

            # 1a. Explicit map (highest priority)
            if subtype_id_map:
                type_cat_id = (
                    subtype_id_map.get(subtype_code)
                    or subtype_id_map.get(subtype_name_en)
                    or subtype_id_map.get(subtype_name_cn)
                )

            # ✅ FIX Bug 1: use a per-iteration local variable instead of
            #    overwriting the function parameter `category_id`.
            resolved_category_id: Optional[str] = category_id

            # 1b. Resolve via category_name_en → category_id → type_category_id
            if not type_cat_id and cat_en:
                resolved_cat_id = category_id_cache.get(cat_en)
                if resolved_cat_id is None:
                    resolved_cat_id = self.resolve_category_id_by_name(
                        cat_en, master_category_id=master_category_id
                    )
                    category_id_cache[cat_en] = resolved_cat_id

                if resolved_cat_id:
                    type_cat_id = self.resolve_type_category_id_by_name(
                        subtype_name_en,
                        category_id=resolved_cat_id,
                        subtype_code=subtype_code,
                    )
                    # ✅ FIX: store into local var, NOT into the parameter
                    if not resolved_category_id:
                        resolved_category_id = resolved_cat_id
                else:
                    self.logger.warning(
                        "Category '%s' not found in DB for subtype '%s'",
                        cat_en, subtype_name_en,
                    )

            # ── Step 2: guard — must have at least one ID ─────────────────
            # ✅ FIX: check resolved_category_id, not category_id
            if not type_cat_id and not resolved_category_id:
                self.logger.error(
                    "Cannot resolve type_category_id or category_id for '%s' — skipped",
                    subtype_name_en,
                )
                results["errors"].append({
                    "subtype_name_en": subtype_name_en,
                    "error": "Could not resolve type_category_id or category_id from DB",
                })
                continue

            self.logger.info(
                "Submitting '%s' (%s): %d parts …",
                subtype_name_en, subtype_code, len(parts)
            )

            # ── Step 3: submit ────────────────────────────────────────────
            # ✅ FIX: pass resolved_category_id (per-iteration local), not category_id
            success, response = self.create_item_category_with_parts(
                master_category_id        = master_category_id,
                category_id               = resolved_category_id,
                type_category_id          = type_cat_id,
                item_category_name_en     = subtype_name_en,
                item_category_name_cn     = subtype_name_cn,
                item_category_description = "",
                dokumen_name              = dokumen_name,
                parts                     = parts
            )

            if success:
                data = (response or {}).get("data", {})
                results["created"].append({
                    "subtype_code":     subtype_code,
                    "subtype_name_en":  subtype_name_en,
                    "parts_count":      len(parts),
                    "item_category_id": data.get("item_category_id", ""),
                })
                results["total_parts_submitted"] += len(parts)
                self.logger.info("✓ '%s': %d parts submitted", subtype_name_en, len(parts))
            else:
                err = str((response or {}).get("error", ""))
                if "409" in err or "duplicate" in err.lower() or "already" in err.lower():
                    results["skipped"].append({
                        "subtype_name_en": subtype_name_en,
                        "reason":          "Already exists (409)"
                    })
                    self.logger.info("⚠ '%s': already exists, skipped", subtype_name_en)
                else:
                    results["errors"].append({
                        "subtype_name_en": subtype_name_en,
                        "error":           err
                    })
                    self.logger.error("✗ '%s': %s", subtype_name_en, err)

        overall_success = len(results["errors"]) == 0
        self.logger.info(
            "batch_submit_parts complete — created: %d, skipped: %d, "
            "total parts: %d, errors: %d",
            len(results["created"]),
            len(results["skipped"]),
            results["total_parts_submitted"],
            len(results["errors"])
        )
        return overall_success, results