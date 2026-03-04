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
        default_unit: str = "pcs",
    ) -> Tuple[bool, Dict]:
        """
        Submit all parts from a Cabin & Chassis partbook extraction result.

        Each group in parts_data must carry:
          - category_name_en   ← used to resolve category_id per group
          - subtype_name_en / subtype_name_cn
          - parts: List[Dict]  ← already have target_id, part_number, etc.

        This replaces the old signature that took a single global
        category_name_en parameter — the correct category is now read
        directly from each subtype group, supporting partbooks that contain
        multiple Categories (e.g. "Frame System", "Brake System", ...).

        Args:
            parts_data:         List of subtype-group dicts from
                                extract_cabin_chassis_parts().
            master_category_id: UUID of the Cabin & Chassis master category.
            dokumen_name:       Document name for the item_category record.
            default_unit:       Unit string (partbook has no unit column).

        Returns:
            (overall_success, results_dict)
            results_dict keys: created, updated, errors
        """
        results: Dict = {
            "created": [],
            "updated": [],
            "errors":  [],
        }

        # Cache resolved category_ids to avoid repeated API calls for the
        # same category (many subtypes share one parent Category).
        category_id_cache: Dict[str, Optional[str]] = {}

        for group in parts_data:
            cat_en          = (group.get("category_name_en") or "").strip()
            subtype_name_en = (group.get("subtype_name_en")  or "").strip()
            subtype_name_cn = (group.get("subtype_name_cn")  or "").strip()
            raw_parts       = group.get("parts", [])

            if not raw_parts:
                self.logger.warning("Subtype '%s': no parts — skipped", subtype_name_en)
                continue

            # ── Validate category_name_en ────────────────────────────────
            if not cat_en:
                self.logger.error(
                    "Subtype '%s' has no category_name_en — skipped", subtype_name_en
                )
                results["errors"].append({
                    "subtype": subtype_name_en,
                    "error":   "Missing category_name_en in parts data",
                })
                continue

            # ── Resolve category_id (cached) ─────────────────────────────
            if cat_en not in category_id_cache:
                category_id_cache[cat_en] = self.resolve_category_id_by_name(
                    cat_en, master_category_id
                )

            category_id = category_id_cache[cat_en]

            if not category_id:
                self.logger.error(
                    "Category '%s' not found in DB — skipping subtype '%s'",
                    cat_en, subtype_name_en,
                )
                results["errors"].append({
                    "subtype": subtype_name_en,
                    "error":   f"Category '{cat_en}' not found in database",
                })
                continue

            # ── Resolve type_category_id ─────────────────────────────────
            type_category_id = self.resolve_type_category_id_by_name(
                subtype_name_en, 
                category_id,
                subtype_code=group.get("subtype_code", ""),
            )

            if not type_category_id:
                self.logger.warning(
                    "type_category '%s' not found under '%s' — using category_id only",
                    subtype_name_en, cat_en,
                )

            self.logger.info(
                "Processing '%s' → '%s' (%d parts)…",
                cat_en, subtype_name_en, len(raw_parts),
            )

            # ── Check if item_category already exists ────────────────────
            if type_category_id:
                existing = self.get_item_category_by_type_category(
                    type_category_id, dokumen_name
                )
            else:
                existing = self.get_item_category_by_category(category_id, dokumen_name)

            # ── Build data_items from the already-processed parts list ───
            # Parts from extract_cabin_chassis_parts() already have target_id,
            # catalog_item_name_en, catalog_item_name_ch, quantity, etc.
            data_items = [
                {
                    "target_id":            p.get("target_id", ""),
                    "part_number":          p.get("part_number", ""),
                    "catalog_item_name_en": p.get("catalog_item_name_en", ""),
                    "catalog_item_name_ch": p.get("catalog_item_name_ch", ""),
                    "description":          p.get("description", ""),
                    "quantity":             p.get("quantity", 1),
                    "unit":                 p.get("unit", default_unit),
                }
                for p in raw_parts
                if (p.get("part_number") or "").strip()
            ]

            if not data_items:
                self.logger.warning(
                    "Subtype '%s': all parts filtered out — skipped", subtype_name_en
                )
                continue

            # ── Submit ───────────────────────────────────────────────────
            if existing:
                existing_details = self.get_item_category_details(
                    existing["item_category_id"]
                )
                # Re-number T-IDs from DB continuation point
                start_idx = _get_next_target_index(existing_details)
                self.logger.info(
                    "item_category exists (%s). Continuing from T%03d.",
                    existing["item_category_id"], start_idx,
                )
                for i, item in enumerate(data_items, start=start_idx):
                    item["target_id"] = f"T{i:03d}"

                success, resp = self.update_item_category_with_parts(
                    item_category_id      = existing["item_category_id"],
                    master_category_id    = master_category_id,
                    category_id           = category_id,
                    type_category_id      = type_category_id,
                    item_category_name_en = existing.get("item_category_name_en", subtype_name_en),
                    item_category_name_cn = existing.get("item_category_name_cn", subtype_name_cn),
                    dokumen_name          = dokumen_name,
                    data_items            = data_items,
                )
                if success:
                    results["updated"].append(subtype_name_en)
                    self.logger.info("✓ Updated '%s' with %d parts", subtype_name_en, len(data_items))
                else:
                    results["errors"].append({
                        "subtype":  subtype_name_en,
                        "error":    "PUT failed",
                        "response": resp,
                    })
                    self.logger.error("✗ PUT failed for '%s': %s", subtype_name_en, resp)

            else:
                success, resp = self.create_item_category_with_parts(
                    master_category_id        = master_category_id,
                    category_id               = category_id if not type_category_id else None,
                    type_category_id          = type_category_id,
                    item_category_name_en     = subtype_name_en,
                    item_category_name_cn     = subtype_name_cn,
                    item_category_description = "",
                    dokumen_name              = dokumen_name,
                    data_items                = data_items,
                )
                if success:
                    results["created"].append(subtype_name_en)
                    self.logger.info("✓ Created '%s' with %d parts", subtype_name_en, len(data_items))
                else:
                    results["errors"].append({
                        "subtype":  subtype_name_en,
                        "error":    "POST failed",
                        "response": resp,
                    })
                    self.logger.error("✗ POST failed for '%s': %s", subtype_name_en, resp)

            # Polite delay between requests
            time.sleep(0.5)

        overall_success = len(results["errors"]) == 0
        self.logger.info(
            "Parts submission complete — Created: %d | Updated: %d | Errors: %d",
            len(results["created"]),
            len(results["updated"]),
            len(results["errors"]),
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