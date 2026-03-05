"""
epc_client_parts_extension.py
─────────────────────────────────────────────────────────────────────────────
Parts Management extension methods for MotorsightsEPCClient.

HOW TO INTEGRATE:
  Copy the methods inside _PartsManagementMixin directly into the
  MotorsightsEPCClient class body in motorsights_epc_client.py.
  Also copy the module-level helpers (_get_next_target_index) to the
  bottom of that file.

Handles:
  - Multipart POST/PUT for /item_category/create and /item_category/{id}
  - Lookup of existing item_category by type_category_id or category_id
  - Extraction of last target_id from existing details for T-ID continuity
  - Full batch submission of parts for all subtypes in a Cabin & Chassis
    partbook — reads category_name_en PER GROUP (not one global value)
"""

import json
import re
import time
from typing import Dict, List, Optional, Tuple

import requests


# ==========================================================================
# Methods to add to MotorsightsEPCClient
# ==========================================================================

class _PartsManagementMixin:
    """
    Mixin — paste these methods directly into MotorsightsEPCClient.
    (This class only exists for IDE completion / documentation purposes.)
    """

    # ------------------------------------------------------------------
    # Low-level multipart helper
    # ------------------------------------------------------------------

    def _api_request_multipart(
        self,
        method: str,
        endpoint: str,
        form_data: Dict,
        file_path: Optional[str] = None,
    ) -> Tuple[bool, Optional[Dict]]:
        """
        Make an authenticated multipart/form-data request.
        Used for /item_category/create and /item_category/{id} (PUT).

        Args:
            method:     "POST" or "PUT"
            endpoint:   Path after base_url (e.g. "item_category/create")
            form_data:  Dict of string fields to include in the form
            file_path:  Optional path to a photo file (file_foto field)

        Returns:
            (success, response_json)
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        # Authorization only — let requests set Content-Type with multipart boundary
        headers = {"Authorization": f"Bearer {self._get_bearer_token()}"}

        files = {}
        if file_path:
            files["file_foto"] = open(file_path, "rb")

        def _call():
            resp = self.session.request(
                method,
                url,
                data=form_data,
                files=files if files else None,
                headers=headers,
                timeout=60,
            )
            resp.raise_for_status()
            return True, resp.json()

        try:
            return self._handle_401_retry(_call)
        except requests.exceptions.RequestException as e:
            self.logger.error(
                "Multipart request failed [%s %s]: %s", method.upper(), endpoint, e
            )
            if hasattr(e, "response") and e.response is not None:
                self.logger.error("Response body: %s", e.response.text[:500])
            return False, None
        finally:
            for f in files.values():
                try:
                    f.close()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Item Category — lookup
    # ------------------------------------------------------------------

    def get_item_category_by_type_category(
        self,
        type_category_id: str,
        dokumen_name: Optional[str] = None,
    ) -> Optional[Dict]:
        """
        Find an existing item_category matching a type_category_id.
        Returns the first matching item_category dict, or None if not found.
        """
        filters: Dict = {
            "page": 1,
            "limit": 100,
            "type_category_name_en": "",
        }
        success, result = self._api_request("POST", "item_category/get", json_data=filters)
        if not success or not result:
            return None

        for dokumen in result.get("data", {}).get("items", []):
            for master in dokumen.get("master_categories", []):
                for item in master.get("items", []):
                    if item.get("type_category_id") == type_category_id:
                        if dokumen_name is None or item.get("dokumen_name") == dokumen_name:
                            return item
        return None

    def get_item_category_by_category(
        self,
        category_id: str,
        dokumen_name: Optional[str] = None,
    ) -> Optional[Dict]:
        """
        Find an existing item_category matching a category_id (no subtype path).
        """
        filters: Dict = {"page": 1, "limit": 100}
        success, result = self._api_request("POST", "item_category/get", json_data=filters)
        if not success or not result:
            return None

        for dokumen in result.get("data", {}).get("items", []):
            for master in dokumen.get("master_categories", []):
                for item in master.get("items", []):
                    if (
                        item.get("category_id") == category_id
                        and not item.get("type_category_id")
                    ):
                        if dokumen_name is None or item.get("dokumen_name") == dokumen_name:
                            return item
        return None

    def get_item_category_details(self, item_category_id: str) -> List[Dict]:
        """
        Fetch all existing item_category_details (parts) for a given
        item_category_id. Used to determine the last T-ID for continuation.
        """
        success, result = self._api_request("GET", f"item_category/{item_category_id}")
        if not success or not result:
            return []
        return result.get("data", {}).get("details", [])

    # ------------------------------------------------------------------
    # Item Category — create / update with parts
    # ------------------------------------------------------------------

    def create_item_category_with_parts(
        self,
        master_category_id: str,
        category_id: Optional[str],
        type_category_id: Optional[str],
        item_category_name_en: str,
        item_category_name_cn: str,
        dokumen_name: str,
        data_items: List[Dict],
        item_category_description: str = "",
    ) -> Tuple[bool, Optional[Dict]]:
        """
        POST multipart/form-data to /item_category/create.

        data_items format:
          [
            {
              "target_id": "T001",
              "part_number": "Q150B1016TF2",
              "catalog_item_name_en": "Bolt",
              "catalog_item_name_ch": "螺栓",
              "description": "",
              "quantity": 4,
              "unit": "pcs"
            }
          ]
        """
        if not type_category_id and not category_id:
            raise ValueError("Either type_category_id or category_id must be provided.")

        form_data = {
            "master_category_id":        master_category_id,
            "dokumen_name":              dokumen_name,
            "item_category_name_en":     item_category_name_en,
            "item_category_name_cn":     item_category_name_cn,
            "item_category_description": item_category_description,
            "data_items":                json.dumps(data_items, ensure_ascii=False),
        }
        if type_category_id:
            form_data["type_category_id"] = type_category_id
        elif category_id:
            form_data["category_id"] = category_id

        success, result = self._api_request_multipart("POST", "item_category/create", form_data)
        if success:
            self.logger.info(
                "Created item_category '%s' with %d part(s).",
                item_category_name_en,
                len(data_items),
            )
        return success, result

    def update_item_category_with_parts(
        self,
        item_category_id: str,
        master_category_id: str,
        category_id: Optional[str],
        type_category_id: Optional[str],
        item_category_name_en: str,
        item_category_name_cn: str,
        dokumen_name: str,
        data_items: List[Dict],
        item_category_description: str = "",
    ) -> Tuple[bool, Optional[Dict]]:
        """
        PUT multipart/form-data to /item_category/{id}.
        Use to append parts to an existing item_category.
        """
        form_data = {
            "master_category_id":        master_category_id,
            "dokumen_name":              dokumen_name,
            "item_category_name_en":     item_category_name_en,
            "item_category_name_cn":     item_category_name_cn,
            "item_category_description": item_category_description,
            "data_items":                json.dumps(data_items, ensure_ascii=False),
        }
        if type_category_id:
            form_data["type_category_id"] = type_category_id
        elif category_id:
            form_data["category_id"] = category_id

        success, result = self._api_request_multipart(
            "PUT", f"item_category/{item_category_id}", form_data
        )
        if success:
            self.logger.info(
                "Updated item_category '%s' (%s) with %d part(s).",
                item_category_name_en,
                item_category_id,
                len(data_items),
            )
        return success, result

    # ------------------------------------------------------------------
    # Resolve category / type_category IDs by name
    # ------------------------------------------------------------------

    def resolve_category_id_by_name(
        self,
        category_name_en: str,
        master_category_id: Optional[str] = None,
    ) -> Optional[str]:
        """Look up category_id by English name."""
        success, result = self._api_request(
            "POST", "categories/get",
            json_data={"page": 1, "limit": 200, "search": category_name_en},
        )
        if not success or not result:
            return None

        for item in result.get("data", {}).get("items", []):
            en = (item.get("category_name_en") or "").strip().lower()
            if en == category_name_en.strip().lower():
                if master_category_id is None or item.get("master_category_id") == master_category_id:
                    return item.get("category_id")
        return None

    def resolve_type_category_id_by_name(
        self,
        type_category_name_en: str,
        category_id: Optional[str] = None,
        subtype_code: Optional[str] = None,
    ) -> Optional[str]:
        """Look up type_category_id by English name, with code-prefix fallback."""

        # Build both candidate names to try:
        # 1. Plain name as stored by Stage 2: "Front Accessories Of Frame"
        # 2. Code-prefixed as stored by Stage 1: "DC97259800020 Front Accessories Of Frame"
        candidates = [type_category_name_en.strip()]
        if subtype_code:
            candidates.append(f"{subtype_code} {type_category_name_en}".strip())

        success, result = self._api_request(
            "POST", "type_category/get",
            json_data={"page": 1, "limit": 200, "search": type_category_name_en},
        )
        
        if not success or not result:
            return None

        for item in result.get("data", {}).get("items", []):
            en = (item.get("type_category_name_en") or "").strip()
            if any(en.lower() == c.lower() for c in candidates):
                if category_id is None or item.get("category_id") == category_id:
                    return item.get("type_category_id")
                    
        return None

    # ------------------------------------------------------------------
    # Batch parts submission
    # ------------------------------------------------------------------

    def batch_submit_parts(
        self,
        parts_data: List[Dict],
        master_category_id: str,
        dokumen_name: str,
        category_id: Optional[str] = None,
        subtype_id_map: Optional[Dict[str, str]] = None,
    ) -> Tuple[bool, Dict]:

        results = {
            "item_categories_created": [],
            "item_categories_skipped": [],
            "total_parts_submitted":   0,
            "errors":                  []
        }

    # Cache category_id lookups so we don't repeat the same API call
    # for every subtype under the same category.
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

        # ── 1. Resolve type_category_id ──────────────────────────────────
            type_cat_id = None

        # Try subtype_id_map first (explicit override)
            if subtype_id_map:
                type_cat_id = (
                    subtype_id_map.get(subtype_code)
                    or subtype_id_map.get(subtype_name_en)
                    or subtype_id_map.get(subtype_name_cn)
                )

        # Fall back: resolve category_id by name, then type_category_id by name
            if not type_cat_id and cat_en:
                if cat_en not in category_id_cache:
                    category_id_cache[cat_en] = self.resolve_category_id_by_name(
                        cat_en, master_category_id
                    )
                resolved_cat_id = category_id_cache[cat_en]

                if resolved_cat_id:
                # Try plain name first, then code-prefixed name (Stage 1 stores
                # type categories as "DC97259800020 Front Accessories Of Frame")
                    type_cat_id = self.resolve_type_category_id_by_name(
                        subtype_name_en, resolved_cat_id, subtype_code=subtype_code
                    )
                    if type_cat_id:
                        category_id = resolved_cat_id  # keep in sync for 2-level fallback
                else:
                    self.logger.warning(
                        "Category '%s' not found in DB — will attempt 2-level fallback",
                        cat_en,
                    )

        # ── 2. Final fallback: use category_id directly (2-level) ────────
            resolved_category_id = category_id
            if not type_cat_id and not resolved_category_id and cat_en:
                resolved_category_id = category_id_cache.get(cat_en)

            if not type_cat_id and not resolved_category_id:
                self.logger.error(
                    "Cannot resolve any ID for subtype '%s' — skipped", subtype_name_en
                )
                results["errors"].append({
                    "subtype_name_en": subtype_name_en,
                    "error": "Could not resolve type_category_id or category_id from DB"
                })
                continue

            self.logger.info(
                "Submitting '%s' (%s): %d parts …",
                subtype_name_en, subtype_code, len(parts)
            )

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
                results["item_categories_created"].append({
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
                    results["item_categories_skipped"].append({
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
            len(results["item_categories_created"]),
            len(results["item_categories_skipped"]),
            results["total_parts_submitted"],
            len(results["errors"])
        )
        return overall_success, results


# ==========================================================================
# Module-level helpers
# ==========================================================================

def _get_next_target_index(existing_details: List[Dict]) -> int:
    """Return the next T-ID integer index from existing item_category_details."""
    if not existing_details:
        return 1
    max_t = 0
    for item in existing_details:
        tid = item.get("target_id") or ""
        m = re.match(r"^T(\d+)$", tid)
        if m:
            max_t = max(max_t, int(m.group(1)))
    return max_t + 1