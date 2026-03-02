"""
EPC Client — Parts Management Extension
=========================================
Add these methods to motorsights_epc_client.py (MotorsightsEPCClient class).

Handles:
  - Multipart POST/PUT for /item_category/create and /item_category/{id}
  - Lookup of existing item_category by type_category_id or category_id
  - Extraction of last target_id from existing details for T-ID continuity
  - Full batch submission of parts for all subtypes in a Cabin & Chassis partbook

HOW TO INTEGRATE:
  Copy the methods below into the MotorsightsEPCClient class body in
  motorsights_epc_client.py. No other changes to that file are required.
"""

import json
import logging
import re
import time
from typing import Dict, List, Optional, Tuple

# NOTE: These are additional imports needed in motorsights_epc_client.py
# (requests is already imported there)
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
            return self._with_401_retry(_call)
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
        Fetch all existing item_category_details (parts) for a given item_category_id.
        Used to determine the last T-ID for continuation.
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
            "master_category_id": master_category_id,
            "dokumen_name": dokumen_name,
            "item_category_name_en": item_category_name_en,
            "item_category_name_cn": item_category_name_cn,
            "item_category_description": item_category_description,
            "data_items": json.dumps(data_items, ensure_ascii=False),
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
            "master_category_id": master_category_id,
            "dokumen_name": dokumen_name,
            "item_category_name_en": item_category_name_en,
            "item_category_name_cn": item_category_name_cn,
            "item_category_description": item_category_description,
            "data_items": json.dumps(data_items, ensure_ascii=False),
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
    ) -> Optional[str]:
        """Look up type_category_id by English name."""
        success, result = self._api_request(
            "POST", "type_category/get",
            json_data={"page": 1, "limit": 200, "search": type_category_name_en},
        )
        if not success or not result:
            return None

        for item in result.get("data", {}).get("items", []):
            en = (item.get("type_category_name_en") or "").strip().lower()
            if en == type_category_name_en.strip().lower():
                if category_id is None or item.get("category_id") == category_id:
                    return item.get("type_category_id")
        return None

    # ------------------------------------------------------------------
    # Batch parts submission
    # ------------------------------------------------------------------

    def batch_submit_parts(
        self,
        parts_data: Dict,
        master_category_id: str,
        category_name_en: str,
        dokumen_name: str,
        default_unit: str = "pcs",
    ) -> Tuple[bool, Dict]:
        """
        Submit all parts from a Cabin & Chassis partbook extraction result.

        Args:
            parts_data:        Output of CabinChassisPartsExtractor.extract_from_pdf()
                               Format: {"subtypes": [{subtype_name_en, subtype_name_cn, parts}]}
            master_category_id: UUID of the Cabin & Chassis master category
            category_name_en:   English name of the Category (e.g. "Frame System")
            dokumen_name:       Document name for the item_category (e.g. "Cabin & Chassis Manual")
            default_unit:       Unit string to use since partbook has no unit column

        Returns:
            (overall_success, results_dict)
        """
        results = {
            "created": [],
            "updated": [],
            "errors": [],
        }

        # Resolve parent category_id once
        category_id = self.resolve_category_id_by_name(category_name_en, master_category_id)
        if not category_id:
            msg = f"Category '{category_name_en}' not found in DB. Create it first."
            self.logger.error(msg)
            results["errors"].append({"subtype": "N/A", "error": msg})
            return False, results

        for subtype in parts_data.get("subtypes", []):
            subtype_name_en = subtype.get("subtype_name_en", "")
            subtype_name_cn = subtype.get("subtype_name_cn", "")
            raw_parts = subtype.get("parts", [])

            if not raw_parts:
                self.logger.warning("Subtype '%s' has no parts, skipping.", subtype_name_en)
                continue

            self.logger.info(
                "Processing subtype '%s' (%d parts)...", subtype_name_en, len(raw_parts)
            )

            # Resolve type_category_id (Path 1) or fall back to category_id only (Path 2)
            type_category_id = self.resolve_type_category_id_by_name(
                subtype_name_en, category_id
            )
            if not type_category_id:
                self.logger.warning(
                    "type_category '%s' not found — using category_id only (Path 2).",
                    subtype_name_en,
                )

            # Check if item_category already exists
            existing = None
            if type_category_id:
                existing = self.get_item_category_by_type_category(
                    type_category_id, dokumen_name
                )
            else:
                existing = self.get_item_category_by_category(category_id, dokumen_name)

            # Build data_items list with correct T-IDs
            if existing:
                existing_details = self.get_item_category_details(
                    existing["item_category_id"]
                )
                start_idx = _get_next_target_index(existing_details)
                self.logger.info(
                    "item_category exists (%s). Continuing from T%03d.",
                    existing["item_category_id"], start_idx,
                )
            else:
                existing_details = []
                start_idx = 1

            data_items = _build_data_items(raw_parts, start_idx, default_unit)

            if not data_items:
                self.logger.warning("No valid parts for '%s', skipping.", subtype_name_en)
                continue

            # Submit
            if existing:
                success, resp = self.update_item_category_with_parts(
                    item_category_id=existing["item_category_id"],
                    master_category_id=master_category_id,
                    category_id=category_id,
                    type_category_id=type_category_id,
                    item_category_name_en=existing.get("item_category_name_en", subtype_name_en),
                    item_category_name_cn=existing.get("item_category_name_cn", subtype_name_cn),
                    dokumen_name=dokumen_name,
                    data_items=data_items,
                )
                if success:
                    results["updated"].append(subtype_name_en)
                else:
                    results["errors"].append({
                        "subtype": subtype_name_en,
                        "error": "PUT failed",
                        "response": resp,
                    })
            else:
                success, resp = self.create_item_category_with_parts(
                    master_category_id=master_category_id,
                    category_id=category_id if not type_category_id else None,
                    type_category_id=type_category_id,
                    item_category_name_en=subtype_name_en,
                    item_category_name_cn=subtype_name_cn,
                    dokumen_name=dokumen_name,
                    data_items=data_items,
                )
                if success:
                    results["created"].append(subtype_name_en)
                else:
                    results["errors"].append({
                        "subtype": subtype_name_en,
                        "error": "POST failed",
                        "response": resp,
                    })

            # Polite delay between requests
            time.sleep(0.5)

        total = len(results["created"]) + len(results["updated"])
        overall_success = len(results["errors"]) == 0
        self.logger.info(
            "Parts submission complete — Created: %d | Updated: %d | Errors: %d",
            len(results["created"]),
            len(results["updated"]),
            len(results["errors"]),
        )
        return overall_success, results


# ==========================================================================
# Module-level helpers (used inside the mixin methods)
# ==========================================================================

def _get_next_target_index(existing_details: List[Dict]) -> int:
    """Return the next T-ID integer index from existing item_category_details."""
    if not existing_details:
        return 1
    max_idx = 0
    for detail in existing_details:
        match = re.match(r"^T(\d+)$", str(detail.get("target_id", "")))
        if match:
            max_idx = max(max_idx, int(match.group(1)))
    return max_idx + 1


def _build_data_items(
    parts: List[Dict],
    start_index: int,
    default_unit: str = "pcs",
) -> List[Dict]:
    """
    Convert cleaned parts list to the API data_items format.

    Input parts already have target_id assigned by the extractor.
    If start_index != 1, we re-assign T-IDs here to continue from the DB.
    """
    result = []
    for i, part in enumerate(parts, start=start_index):
        result.append({
            "target_id": f"T{i:03d}",
            "part_number": part.get("part_number", ""),
            "catalog_item_name_en": part.get("name_en", ""),
            "catalog_item_name_ch": part.get("name_cn", ""),
            "description": part.get("description", ""),
            "quantity": int(part.get("quantity", 1)),
            "unit": default_unit,
        })
    return result