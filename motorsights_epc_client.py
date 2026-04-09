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

_EMPTY_DISPLAY_VALUES = {"tidak ada", "null", "none", "-", ""}

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
            if e.response is not None and e.response.status_code == 401 and self.auth_client:
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


    def resolve_type_category_id_by_name(
        self,
        type_category_name_en: str,
        category_id: Optional[str] = None,
        subtype_code: Optional[str] = None,
    ) -> Tuple[Optional[str], Optional[str]]:
        candidates = [type_category_name_en.strip()]
        if subtype_code:
            candidates.append(f"{subtype_code} {type_category_name_en}".strip().lower())
            # Tambahkan kandidat dengan suffix huruf di akhir kode dibuang
            # contoh: "DC93259000339V" → "DC93259000339"
            import re as _re
            clean_code = _re.sub(r'[A-Z]+$', '', subtype_code.upper()).strip()
            if clean_code != subtype_code.upper():
                candidates.append(f"{clean_code} {type_category_name_en}".strip().lower())
        else:
            candidates.append(type_category_name_en.strip().lower())

        url = f"{self.base_url}/type_category/get"
        payload = {"page": 1, "limit": 100, "search": type_category_name_en}

        def _request():
            r = self.session.post(url, json=payload, headers=self._get_headers(), timeout=30)
            r.raise_for_status()
            return True, r.json()

        try:
            success, result = self._handle_401_retry(_request)
        except requests.exceptions.RequestException as e:
            self.logger.error("resolve_type_category_id_by_name failed: %s", e)
            return None, None

        if not success or not result:
            return None, None

        self.logger.debug(
            "resolve_type_category_id_by_name: result type=%s", type(result).__name__
        )

        self.logger.info(
            "resolve_type_category raw result: %s",
            str(result)[:500]
        )

        if isinstance(result, list):
            items = result
        elif isinstance(result, dict):
            data = result.get("data", [])
            if isinstance(data, list):
                items = data                      # ← data langsung list
            else:
                items = data.get("items", [])     # ← data adalah dict dengan key items
        else:
            return None

        for item in items:
            en = (item.get("type_category_name_en") or "").strip()

            self.logger.info(   # ← tambahkan ini
                "resolve: checking DB item='%s' vs candidates=%s",
                en, candidates
            )
            if any(en.lower() == c.lower() for c in candidates):
                if category_id is None or item.get("category_id") == category_id:
                    return item.get("type_category_id"), item.get("category_id")
        return None, None

    def _get_category_id_by_name(
        self, category_name_en: str, master_category_id: str
    ) -> Optional[str]:
        """Look up an existing category_id by name."""
        url = f"{self.base_url}/categories/get"
        payload = {
            "page": 1, "limit": 10,
            "master_category_id": master_category_id,
            "search": category_name_en,
        }
        def _request():
            r = self.session.post(url, json=payload, headers=self._get_headers(), timeout=30)
            r.raise_for_status()
            return True, r.json()
        try:
            success, result = self._handle_401_retry(_request)
            if not success or not result:
                return None
            items = (result.get("data") or {}).get("items") or []
            for item in items:
                if item.get("category_name_en", "").lower() == category_name_en.lower():
                    return item.get("category_id")
            return None
        except Exception as e:
            self.logger.error("_get_category_id_by_name failed: %s", e)
            return None

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
    

    def _sync_parts_with_db_conflicts(   
        self,
        parts: List[Dict],
        error_body: str,
    ) -> List[Dict]:
        """
        Parse respons 400 "description berbeda" dan sinkronkan bagian yang konflik
        dengan data yang sudah ada di database.
 
        Pola pesan error dari API:
          'Part number "BSQH50-4211502" (nama: "Double-Ended Stud", description: "-")
           sudah ada di database dengan description yang berbeda
           (nama: "Stud bolt", description: "Q33210 ... : 1")'
 
        Untuk setiap part yang konflik, field catalog_item_name_en dan description
        diganti dengan nilai dari database, lalu dikembalikan sebagai list baru.
        """
        conflicts: Dict[str, Dict] = {}
        try:
            data = json.loads(error_body)
            for err_str in data.get("errors", []):
                # Ekstrak nomor part yang dikirim
                pn_m = re.search(r'Part number "([^"]+)"', err_str)
                # Ekstrak nama dan deskripsi yang sudah ada di database
                db_m = re.search(
                    r"sudah ada di database dengan description yang berbeda"
                    r' \(nama: "([^"]+)", description: "([^"]*)"\)',
                    err_str,
                )
                if pn_m and db_m:
                    pn = pn_m.group(1)
                    raw_desc = db_m.group(2).strip()
                    conflicts[pn] = {
                        "db_name":        db_m.group(1),
                        "db_description": raw_desc,
                    }
                    self.logger.info(
                        "Conflict sync: PN=%-30s  db_name='%s'  db_desc='%s'",
                        pn, db_m.group(1), db_m.group(2),
                    )
        except Exception as exc:
            self.logger.warning("Gagal parse conflict error body: %s", exc)
            return parts   # kembalikan list asli agar tidak ada data yang hilang
 
        if not conflicts:
            return parts
 
        updated: List[Dict] = []
        for p in parts:
            pn = p.get("part_number", "")
            if pn in conflicts:
                synced = dict(p)   # shallow copy, aman karena nilai-nilainya primitif
                synced["catalog_item_name_en"] = conflicts[pn]["db_name"]
                # Gunakan deskripsi DB; fallback ke nilai asli jika DB kosong
                synced["description"] = conflicts[pn]["db_description"] 
                updated.append(synced)
                self.logger.info(
                    "  ↳ Synced PN=%-28s  name: '%s' → '%s'  desc: '%s' → '%s'",
                    pn,
                    p.get("catalog_item_name_en", ""),
                    synced["catalog_item_name_en"],
                    p.get("description", ""),
                    synced["description"],
                )
            else:
                updated.append(p)
 
        self.logger.info(
            "Auto-sync selesai: %d part(s) diperbarui dengan data DB.",
            len(conflicts),
        )
        return updated

    def create_item_category_with_parts(
        self,
        master_category_id: str,
        category_id: Optional[str],
        type_category_id: Optional[str],
        item_category_name_en: str,
        item_category_name_cn: str,
        item_category_description: str,
        dokumen_name: str,
        parts: List[Dict],
        _retry: bool = True,   # ← flag internal; cegah rekursi tak terbatas
    ) -> Tuple[bool, Optional[Dict]]:
        """
        Create one item_category (Parts Management entry) with its parts rows.
 
        Calls POST /item_category/create as multipart/form-data.
        The `data_items` field is a JSON-encoded string of parts rows.
 
        AUTO-SYNC CONFLICT:
          Jika API menolak karena Part Number sudah ada dengan description berbeda
          (HTTP 400 "description yang berbeda"), metode ini akan:
            1. Parse daftar konflik dari respons error
            2. Perbarui catalog_item_name_en dan description dengan data DB
            3. Kirim ulang sekali dengan data yang sudah sinkron (_retry=False)
 
        Hierarchy routing (per API docs):
          • If type_category_id provided → 3-level (subtype present)
          • Else if category_id provided → 2-level (no subtype)
        """
        url = f"{self.base_url}/item_category/create"
 
        if not type_category_id and not category_id:
            raise ValueError("Either type_category_id or category_id must be provided")
 
        # Build data_items JSON array (the parts rows)
        data_items = []
        for p in parts:
            data_items.append({
                "target_id":             p.get("target_id", ""),
                "diagram_serial_number": p.get("diagram_serial_number", ""),
                "part_number":           p.get("part_number", ""),
                "catalog_item_name_en":  p.get("catalog_item_name_en", ""),
                "catalog_item_name_ch":  p.get("catalog_item_name_ch", ""),
                "description":           p.get("description") if p.get("description") is not None else "-",
                "quantity":              int(p.get("quantity") or 1),
                "unit":                  p.get("unit", ""),
            })
 
        # Build multipart form fields
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
            if category_id:
                form_data["category_id"] = (None, category_id)
            self.logger.debug(
                "create_item_category_with_parts: type_category_id=%s (3-level)",
                type_category_id,
            )
        else:
            if category_id:
                form_data["category_id"] = (None, category_id)
            self.logger.debug(
                "create_item_category_with_parts: category_id=%s (2-level)",
                category_id,
            )
 
        # ── _SENTINEL untuk conflict detection ───────────────────────────────
        _CONFLICT_SENTINEL = "__PART_CONFLICT__"
 
        def _request():
            headers = {"Authorization": f"Bearer {self._get_bearer_token()}"}
            r = self.session.post(url, files=form_data, headers=headers, timeout=60)
 
            if not r.ok:
                self.logger.error(
                    "Server rejected POST /item_category/create [%s]: %s",
                    r.status_code, r.text[:1000],
                )
            
            if r.status_code == 400 and "Kombinasi" in r.text and "sudah ada" in r.text:
                self.logger.info("Item category combination already exists — treated as skipped")
                return True, {"skipped": True, "message": r.text[:500]}
 
            if r.status_code == 400 and "sudah ada di database" in r.text:
                body = r.text
                # ── CONFLICT: Part Number exists with DIFFERENT description ──
                if "dengan description yang berbeda" in body and _retry:
                    self.logger.warning(
                        "Part number conflict (description mismatch) — "
                        "akan auto-sync dengan data DB dan coba ulang."
                    )
                    return _CONFLICT_SENTINEL, body   # ← sentinel, bukan exception
 
                # ── SIMPLE DUPLICATE: bagian yang persis sama sudah ada ──────
                self.logger.info("Parts already exist in DB — treated as skipped")
                return True, {"skipped": True, "message": body[:500]}
 
            r.raise_for_status()
            return True, r.json()
 
        try:
            raw = self._handle_401_retry(_request)
 
            # ── Handle conflict sentinel ──────────────────────────────────────
            if isinstance(raw, tuple) and len(raw) == 2 and raw[0] == _CONFLICT_SENTINEL:
                conflict_body = raw[1]
                try:
                    n_conflicts = len(json.loads(conflict_body).get("errors", []))
                except Exception:
                    n_conflicts = "?"
                self.logger.info(
                    "Auto-syncing %s conflicting part(s) dengan data DB …",
                    n_conflicts,
                )
                synced_parts = self._sync_parts_with_db_conflicts(parts, conflict_body)
                # ── Retry sekali dengan data yang sudah disinkronkan ──────────
                return self.create_item_category_with_parts(
                    master_category_id        = master_category_id,
                    category_id               = category_id,
                    type_category_id          = type_category_id,
                    item_category_name_en     = item_category_name_en,
                    item_category_name_cn     = item_category_name_cn,
                    item_category_description = item_category_description,
                    dokumen_name              = dokumen_name,
                    parts                     = synced_parts,
                    _retry                    = False,   # ← cegah loop tak terbatas
                )
 
            return raw
 
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response else "?"
            body   = e.response.text[:500]  if e.response else ""
            if status == 400 and "sudah ada di database" in body:
                self.logger.info("Parts already exist in DB — treated as skipped")
                return True, {"skipped": True, "message": body}
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
        Batch create categories WITH nested type categories (Cabin & Chassis / Axle Drive).
        3-level hierarchy: Master Category → Category → Type Category (subtype)
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
            nested_types = pdf_category.get("data_type", [])

            category_request = {
                "master_category_id":      master_category_id,
                "master_category_name_en": master_category_name_en or "",
                "category_name_en":        pdf_category.get("category_name_en", ""),
                "category_name_cn":        pdf_category.get("category_name_cn", ""),
                "category_description":    pdf_category.get("category_description", ""),
                "data_type": [
                    {
                        "type_category_name_en":     tc.get("type_category_name_en", ""),
                        "type_category_name_cn":     tc.get("type_category_name_cn", ""),
                        "type_category_description": tc.get("type_category_description", ""),
                    }
                    for tc in nested_types
                ]
            }

            success, cat_response, was_skipped = self.create_category(category_request)

            if success and was_skipped:
                results["categories_skipped"].append({
                "category_name_en": pdf_category.get("category_name_en", ""),
                "message":          cat_response.get("message", "Already exists")
                })
                self.logger.info(
                    "Skipped '%s' — updating via PUT /categories/{id}",
                    pdf_category.get("category_name_en", "")
                )

                if nested_types:
                    category_id = self._get_category_id_by_name(
                        pdf_category.get("category_name_en", ""), master_category_id
                    )
                    if category_id:
                        # ✅ Gunakan PUT, bukan loop POST /type_category/create
                        put_payload = {
                            "master_category_id":      master_category_id,
                            "master_category_name_en": master_category_name_en or "",
                            "category_name_cn":        pdf_category.get("category_name_cn", ""),
                            "category_description":    pdf_category.get("category_description", ""),
                            "data_type": [
                                {
                                    "type_category_name_en":     tc.get("type_category_name_en", ""),
                                    "type_category_name_cn":     tc.get("type_category_name_cn", ""),
                                    "type_category_description": tc.get("type_category_description", ""),
                                }
                                for tc in nested_types
                            ]
                        }
                        put_success, put_response = self.update_category(category_id, put_payload)
                        if put_success:
                            updated_types = (put_response.get("data") or {}).get("data_type", [])
                            results["type_categories_created"].extend(updated_types)
                            self.logger.info(
                                "  Updated '%s' with %d type categories via PUT",
                                pdf_category.get("category_name_en", ""), len(nested_types)
                            )
                        else:
                            self.logger.error(
                                "  Failed to update '%s' via PUT",
                                pdf_category.get("category_name_en", "")
                            )
                    else:
                        self.logger.warning(
                            "Could not resolve category_id for '%s'",
                            pdf_category.get("category_name_en", "")
                        )

            elif success:
                results["categories_created"].append(cat_response.get("data", {}))
                results["type_categories_created"].extend(
                    (cat_response.get("data") or {}).get("data_type", [])
                )
                self.logger.info(
                    "Created '%s' with %d type categories",
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
    
    def update_category(self, category_id: str, data: Dict) -> Tuple[bool, Optional[Dict]]:
        url = f"{self.base_url}/categories/{category_id}"

        def _request():
            r = self.session.put(url, json=data, headers=self._get_headers(), timeout=30)
            r.raise_for_status()
            result = r.json()
            return True, result

        try:
            return self._handle_401_retry(_request)
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to update category {category_id}: {e}")
            if hasattr(e, "response") and e.response is not None:
                self.logger.error(f"Response: {e.response.text}")
            return False, None

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
                "master_category_id":      master_category_id,
                "master_category_name_en": master_category_name_en or "",
                "category_name_en":        cat.get("category_name_en", ""),
                "category_name_cn":        cat.get("category_name_cn", ""),
                "category_description":    cat.get("category_description", ""),
                "data_type":               []
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

    def _get_dokumen_id_by_name(self, dokumen_name: str) -> Optional[str]:
        """
        Look up dokumen_id by dokumen_name via POST /item_category/get.
        Returns the dokumen_id UUID or None if not found.
        """
        url = f"{self.base_url}/item_category/get"
        payload = {"page": 1, "limit": 1, "dokumen_name": dokumen_name}

        def _request():
            r = self.session.post(url, json=payload, headers=self._get_headers(), timeout=30)
            r.raise_for_status()
            return True, r.json()
        try:
            success, result = self._handle_401_retry(_request)
            if not success or not result:
                return None
        except requests.exceptions.RequestException as e:
            self.logger.error("_get_dokumen_id_by_name failed: %s", e)
            return None

        # Response: {"data": {"items": [{"dokumen_id": ..., "master_categories": [...]}]}}
        items = (result.get("data") or {}).get("items") or []
        if not items:
            self.logger.warning("No dokumen found with name '%s'", dokumen_name)
            return None

        # dokumen_id is on the first item inside master_categories → items
        for doc in items:
            dokumen_id = doc.get("dokumen_id")
            if dokumen_id:
                self.logger.info("Resolved dokumen_id=%s for '%s'", dokumen_id, dokumen_name)
                return dokumen_id
        return None

    def _get_all_item_categories_for_dokumen(
        self, dokumen_id: str
    ) -> Dict[str, str]:
        """
        Fetch ALL item_categories for a dokumen via GET /item_category/dokumen/{id}.
        Returns a dict mapping type_category_name_en (lowered) → item_category_id.
        Handles pagination automatically.
        """
        mapping: Dict[str, str] = {}
        page = 1
        limit = 100

        while True:
            url = f"{self.base_url}/item_category/dokumen/{dokumen_id}?page={page}&limit={limit}"

            def _request(u=url):
                r = self.session.get(u, headers=self._get_headers(), timeout=30)
                r.raise_for_status()
                return True, r.json()
            try:
                success, result = self._handle_401_retry(_request)
                if not success or not result:
                    break
            except requests.exceptions.RequestException as e:
                self.logger.error("_get_all_item_categories_for_dokumen page %d failed: %s", page, e)
                break

            data  = result.get("data") or {}
            items = data.get("items") or []

            for item in items:
                name = (item.get("type_category_name_en") or "").strip()
                iid  = item.get("item_category_id")
                if name and iid:
                    mapping[name.lower()] = iid

            pagination  = data.get("pagination") or {}
            total_pages = pagination.get("totalPages", 1)
            if page >= total_pages:
                break
            page += 1

        self.logger.info(
            "Loaded %d item_categories from dokumen %s", len(mapping), dokumen_id
        )
        return mapping

    def update_item_category_with_parts(
        self,
        item_category_id: str,
        master_category_id: str,
        category_id: Optional[str],
        type_category_id: Optional[str],
        dokumen_name: str,
        parts: List[Dict],
    ) -> Tuple[bool, Optional[Dict]]:
        """
        PUT /item_category/{item_category_id} with updated parts rows.
        This is what the UI does — it updates an existing item_category, not creates.
        """
        url = f"{self.base_url}/item_category/{item_category_id}"

        data_items = []
        for p in parts:
            data_items.append({
                "target_id":             p.get("target_id", ""),
                "diagram_serial_number": p.get("diagram_serial_number", ""),
                "part_number":           p.get("part_number", ""),
                "catalog_item_name_en":  p.get("catalog_item_name_en", ""),
                "catalog_item_name_ch":  p.get("catalog_item_name_ch", ""),
                "description":           p.get("description") or "-",
                "quantity":              int(p.get("quantity") or 1),
            })

        form_data = {
            "dokumen_name":       (None, dokumen_name),
            "master_category_id": (None, master_category_id),
            "data_items":         (None, json.dumps(data_items, ensure_ascii=False)),
        }
        if type_category_id:
            form_data["type_category_id"] = (None, type_category_id)
        if category_id:
            form_data["category_id"] = (None, category_id)

        def _request():
            headers = {"Authorization": f"Bearer {self._get_bearer_token()}"}
            r = self.session.put(url, files=form_data, headers=headers, timeout=60)
            r.raise_for_status()
            return True, r.json()

        try:
            return self._handle_401_retry(_request)
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response else "?"
            body   = e.response.text[:500]  if e.response else ""
            if status == 400 and "sudah ada di database" in body:
                self.logger.info(
                    "Parts already exist in DB — treated as skipped: %s", body[:200]
                )
                return True, {"skipped": True, "message": "Parts already exist"}
            
            self.logger.error("update_item_category_with_parts HTTP %s: %s", status, body)
            return False, {"error": f"HTTP {status}: {body}"}
        
        except requests.exceptions.RequestException as e:
            self.logger.error("update_item_category_with_parts connection error: %s", e)
            return False, {"error": f"Connection error: {e}"}
        except Exception as e:
            self.logger.error("update_item_category_with_parts failed: %s", e)
            return False, {"error": str(e)}

    def batch_submit_parts(
        self,
        parts_data: List[Dict],
        master_category_id: str,
        dokumen_name: str,
        category_id: Optional[str] = None,
        subtype_id_map: Optional[Dict[str, str]] = None,
    ) -> Tuple[bool, Dict]:
        """
        Submit all parts groups to the API.
 
        Supports two hierarchy modes automatically:
          • 3-level (Cabin & Chassis): Master → Category → TypeCategory → ItemCategory
            subtype_code is non-empty; looks up existing item_categories by type_category_name
            and PUTs into them.
          • 2-level (Transmission): Master → Category → ItemCategory
            subtype_code is ""; resolves category_id by category_name_en and POSTs
            a new item_category directly under the Category.
 
        FIX 1: No longer aborts when item_cat_map is empty.
               Empty map is normal for Transmission (Stage 1 creates only flat
               Categories, never item_categories). The per-group fallback handles it.
        FIX 2: When subtype_code is "" (Transmission) the fallback now resolves
               category_id by category_name_en and creates via POST (2-level path)
               instead of erroring out.
        """
        results = {
            "created":               [],
            "skipped":               [],
            "updated":               [],
            "errors":                [],
            "total_parts_submitted": 0,
        }
 
        # ── Step 1: resolve dokumen_id ────────────────────────────────────
        dokumen_id = self._get_dokumen_id_by_name(dokumen_name)
        if not dokumen_id:
            self.logger.error(
                "Cannot find dokumen_id for '%s' — aborting batch_submit_parts", dokumen_name
            )
            results["errors"].append({
                "error": f"Dokumen '{dokumen_name}' not found in DB"
            })
            return False, results
 
        # ── Step 2: fetch existing item_categories for this dokumen ───────
        # For Cabin & Chassis: this returns the item_categories created in Stage 1
        # For Transmission:    this returns {} (empty) — that is OK, handled per-group
        item_cat_map = self._get_all_item_categories_for_dokumen(dokumen_id)
        # FIX 1: removed early-return when item_cat_map is empty.
        # Transmission has no pre-existing item_categories; the per-group logic
        # will create them via POST (2-level path).
        if not item_cat_map:
            self.logger.info(
                "No existing item_categories found for dokumen_id=%s — "
                "will create all groups from scratch (Transmission / first run).",
                dokumen_id,
            )
 
        # ── Step 3: process each parts group ─────────────────────────────
        for group in parts_data:
            subtype_code    = (group.get("subtype_code")    or "").strip()
            subtype_name_en = (group.get("subtype_name_en") or "").strip()
            subtype_name_cn = (group.get("subtype_name_cn") or "").strip()
            # category_name_en is populated by transmission_parts_extractor
            # (equals subtype_name_en for transmission; separate for cabin_chassis)
            group_cat_en    = (group.get("category_name_en") or "").strip()
            parts           = group.get("parts", [])
 
            if not parts:
                self.logger.debug("Skipping empty group '%s'", subtype_name_en)
                continue
 
            # Build lookup candidates for existing item_cat_map
            candidates = []
            if subtype_code:
                candidates.append(f"{subtype_code} {subtype_name_en}".lower())
            candidates.append(subtype_name_en.lower())
 
            item_category_id: Optional[str] = None
            for candidate in candidates:
                item_category_id = item_cat_map.get(candidate)
                if item_category_id:
                    break
 
            self.logger.info(
                "Group: subtype_name_en='%s' subtype_code='%s' → item_category_id=%s",
                subtype_name_en, subtype_code, item_category_id or "not found",
            )
 
            # ── Path A: existing item_category found → PUT ────────────────
            if item_category_id:
                self.logger.info(
                    "Updating '%s' (%s): item_category_id=%s, %d parts …",
                    subtype_name_en, subtype_code, item_category_id, len(parts),
                )
                success, response = self.update_item_category_with_parts(
                    item_category_id  = item_category_id,
                    master_category_id = master_category_id,
                    category_id       = category_id,
                    type_category_id  = None,
                    dokumen_name      = dokumen_name,
                    parts             = parts,
                )
                if success:
                    results["updated"].append({
                        "subtype_code":     subtype_code,
                        "subtype_name_en":  subtype_name_en,
                        "parts_count":      len(parts),
                        "item_category_id": item_category_id,
                    })
                    results["total_parts_submitted"] += len(parts)
                    self.logger.info("✓ '%s': %d parts updated", subtype_name_en, len(parts))
                else:
                    err = str((response or {}).get("error", ""))
                    results["errors"].append({"subtype_name_en": subtype_name_en, "error": err})
                    self.logger.error("✗ '%s': %s", subtype_name_en, err)
                continue
 
            # ── Path B: no existing item_category → POST (create new) ─────
            self.logger.warning(
                "No existing item_category for '%s' — attempting to create via POST.",
                subtype_name_en,
            )
 
            # ------------------------------------------------------------------
            # FIX 2: TRANSMISSION branch (subtype_code is "")
            # Hierarchy: Master Category → Category → Item Category  (2-level)
            # Resolve category_id by category_name_en; no type_category needed.
            # ------------------------------------------------------------------
            if not subtype_code:
                # category_name_en == subtype_name_en for transmission (same field aliased)
                lookup_name = group_cat_en or subtype_name_en
                self.logger.info(
                    "Transmission 2-level path: looking up category_id for '%s'", lookup_name
                )
                resolved_cat_id = self._get_category_id_by_name(lookup_name, master_category_id)
 
                if not resolved_cat_id:
                    self.logger.error(
                        "Cannot resolve category_id for '%s' — "
                        "make sure Stage 1 (category submission) ran first.",
                        lookup_name,
                    )
                    results["errors"].append({
                        "subtype_name_en": subtype_name_en,
                        "error": (
                            f"category '{lookup_name}' not found in DB — "
                            "run Stage 1 first"
                        ),
                    })
                    continue
 
                self.logger.info(
                    "Resolved category_id=%s for '%s' — creating item_category (2-level)",
                    resolved_cat_id, lookup_name,
                )
                ok, resp = self.create_item_category_with_parts(
                    master_category_id        = master_category_id,
                    category_id               = resolved_cat_id,
                    type_category_id          = None,   # ← no subtype for Transmission
                    item_category_name_en     = subtype_name_en,
                    item_category_name_cn     = subtype_name_cn,
                    item_category_description = "",
                    dokumen_name              = dokumen_name,
                    parts                     = parts,
                )
                if ok and (resp or {}).get("skipped"):
                    # Jangan lewati — cari item_category_id lalu update via PUT
                    self.logger.info(
                        "↷ '%s': sudah ada di DB — mencari item_category_id untuk di-update …",
                        subtype_name_en,
                    )
                    # Refresh item_cat_map untuk dokumen ini
                    fresh_map = self._get_all_item_categories_for_dokumen(dokumen_id)
                    candidates_lookup = []
                    candidates_lookup.append(subtype_name_en.lower())
                    found_id = None
                    for c in candidates_lookup:
                        found_id = fresh_map.get(c)
                        if found_id:
                            break

                    if found_id:
                        self.logger.info(
                            "  → Ditemukan item_category_id=%s, melakukan PUT …", found_id
                        )
                        ok2, resp2 = self.update_item_category_with_parts(
                            item_category_id   = found_id,
                            master_category_id = master_category_id,
                            category_id        = resolved_cat_id,
                            type_category_id   = None,
                            dokumen_name       = dokumen_name,
                            parts              = parts,
                        )
                        if ok2:
                            results["updated"].append({
                                "subtype_name_en":  subtype_name_en,
                                "parts_count":      len(parts),
                                "item_category_id": found_id,
                            })
                            results["total_parts_submitted"] += len(parts)
                            self.logger.info("✓ '%s': %d parts diperbarui via PUT", subtype_name_en, len(parts))
                        else:
                            results["errors"].append({
                                "subtype_name_en": subtype_name_en,
                                "error": str((resp2 or {}).get("error", "PUT gagal")),
                            })
                    else:
                        self.logger.warning(
                            "  → item_category_id tidak ditemukan bahkan setelah refresh — benar-benar dilewati."
                        )
                        results["skipped"].append({
                            "subtype_name_en": subtype_name_en,
                            "reason": "Sudah ada tapi ID tidak ditemukan",
                        })
                elif ok:
                    results["created"].append({
                        "subtype_name_en": subtype_name_en,
                        "parts_count":     len(parts),
                        "action":          "created (2-level / transmission)",
                    })
                    results["total_parts_submitted"] += len(parts)
                    self.logger.info(
                        "✓ Created item_category + %d parts for '%s' (transmission)",
                        len(parts), subtype_name_en,
                    )
                
                else:
                    results["errors"].append({
                        "subtype_name_en": subtype_name_en,
                        "error": str((resp or {}).get("error", "create failed")),
                    })
                continue
 
            # ------------------------------------------------------------------
            # CABIN & CHASSIS branch (subtype_code is present)
            # Hierarchy: Master → Category → TypeCategory → ItemCategory (3-level)
            # Resolve type_category_id from DB, then POST.
            # ------------------------------------------------------------------
            type_cat_id, resolved_cat_id = self.resolve_type_category_id_by_name(
                subtype_name_en,
                subtype_code=subtype_code,
            )
            self.logger.info(
                "Resolved: type_cat_id=%s, resolved_cat_id=%s",
                type_cat_id, resolved_cat_id,
            )
 
            if not type_cat_id:
                self.logger.error(
                    "Cannot resolve type_category_id for '%s' — skipped", subtype_name_en
                )
                results["errors"].append({
                    "subtype_name_en": subtype_name_en,
                    "error": "type_category not found in DB — run Stage 1 first",
                })
                continue
 
            ok, resp = self.create_item_category_with_parts(
                master_category_id        = master_category_id,
                category_id               = resolved_cat_id,
                type_category_id          = type_cat_id,
                item_category_name_en     = subtype_name_en,
                item_category_name_cn     = subtype_name_cn,
                item_category_description = "",
                dokumen_name              = dokumen_name,
                parts                     = parts,
            )

            if ok and (resp or {}).get("skipped"):
                results["skipped"].append({
                    "subtype_name_en": subtype_name_en,
                    "reason": "Parts already exist in DB",
                })
                self.logger.info("↷ '%s': already exists, skipped", subtype_name_en)
            elif ok:
                results["created"].append({
                    "subtype_name_en": subtype_name_en,
                    "parts_count":     len(parts),
                    "action":          "created (3-level / cabin-chassis)",
                })
                results["total_parts_submitted"] += len(parts)
                self.logger.info(
                    "✓ Created new item_category + %d parts for '%s'",
                    len(parts), subtype_name_en,
                )
            else:
                results["errors"].append({
                    "subtype_name_en": subtype_name_en,
                    "error": str((resp or {}).get("error", "create failed")),
                })
 
        overall_success = len(results["errors"]) == 0
        self.logger.info(
            "batch_submit_parts complete — created: %d, updated: %d, "
            "total parts: %d, errors: %d",
            len(results["created"]),
            len(results["updated"]),
            results["total_parts_submitted"],
            len(results["errors"]),
        )
        return overall_success, results