"""
Cabin & Chassis Parts Extractor
Extracts parts management data (item_category details) from a Cabin & Chassis
partbook that has already been converted to markdown by pymupdf4llm.

Table structure (6 columns):
  序号 | 编码 | 名称 (CN) | Name (EN) | 数量 | 备注

Data is grouped by Subtype Category headers that appear above each table.
"""

import json
import logging
import re
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# AI prompt
# ---------------------------------------------------------------------------

PARTS_EXTRACTION_PROMPT = """\
You are a data extraction expert for automotive parts catalogs.

You will receive markdown text converted from a Cabin & Chassis partbook PDF.
Extract ALL parts tables from the markdown.

TABLE STRUCTURE (6 columns, left to right):
  1. 序号  — Serial Number (integer or blank)
  2. 编码  — Part Number / Encoding (alphanumeric code)
  3. 名称  — Chinese name
  4. Name  — English name
  5. 数量  — Quantity (integer)
  6. 备注  — Remarks / Description (may be empty)

SUBTYPE CATEGORY HEADERS:
  Before each table there is a header line identifying the Subtype Category,
  typically like: "DC97259190594 Air Intake System 进气系统"
  or just:        "Frame System 车架系统"
  Extract both the English and Chinese names from this header.

RULES:
  - One entry per data row. Ignore image-only pages and diagram pages.
  - If a table is interrupted by a diagram and continues on the next page,
    treat it as ONE continuous table under the same Subtype Category.
  - If you encounter identical pages (duplicate content), skip the duplicate.
  - If 序号 is blank for a row, still extract it (target_id is auto-assigned).
  - Extract quantity as an integer. Default to 1 if blank.
  - Extract 备注 as a string; use "" if empty.

RETURN ONLY valid JSON — no markdown fences, no preamble:
{
  "subtypes": [
    {
      "subtype_name_en": "Air Intake System",
      "subtype_name_cn": "进气系统",
      "parts": [
        {
          "serial_no": 1,
          "part_number": "DC97259190594",
          "name_cn": "空气滤清器总成",
          "name_en": "Air Filter Assembly",
          "quantity": 1,
          "description": ""
        }
      ]
    }
  ]
}
"""


# ---------------------------------------------------------------------------
# Extractor class
# ---------------------------------------------------------------------------

class CabinChassisPartsExtractor:
    """
    Extracts parts management data from a Cabin & Chassis partbook markdown.

    Usage:
        extractor = CabinChassisPartsExtractor(sumopod_client=client)
        result    = extractor.extract_from_markdown(markdown_text)
        # result  = {"subtypes": [...]}
    """

    def __init__(self, sumopod_client, max_retries: int = 3):
        self.sumopod = sumopod_client
        self.max_retries = max_retries

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract_from_markdown(self, markdown: str) -> Dict:
        """
        Run AI extraction on a markdown string.
        Returns {"subtypes": [...]} with deduplicated, T-ID-assigned parts.
        """
        raw = self._call_ai(markdown)
        processed = []
        for subtype in raw.get("subtypes", []):
            parts = process_subtype_parts(subtype.get("parts", []))
            processed.append({
                "subtype_name_en": subtype.get("subtype_name_en", ""),
                "subtype_name_cn":  subtype.get("subtype_name_cn", ""),
                "parts": parts,
            })
        return {"subtypes": processed}

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _call_ai(self, markdown: str) -> Dict:
        """Call the Sumopod AI with retries; return parsed JSON dict."""
        last_exc = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.sumopod.client.chat.completions.create(
                    model=self.sumopod.model,
                    messages=[
                        {"role": "system", "content": PARTS_EXTRACTION_PROMPT},
                        {"role": "user",   "content": markdown},
                    ],
                    temperature=0.0,
                    max_tokens=8000,
                    timeout=120,
                )
                raw_text = response.choices[0].message.content.strip()
                return _parse_json(raw_text)
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "Parts extraction attempt %d/%d failed: %s",
                    attempt, self.max_retries, exc,
                )
        raise RuntimeError(f"Parts extraction failed after {self.max_retries} attempts") from last_exc


# ---------------------------------------------------------------------------
# Processing helpers (pure functions — no AI required)
# ---------------------------------------------------------------------------

def deduplicate_parts(parts: List[Dict]) -> List[Dict]:
    """
    Within a subtype, merge rows that share the same part_number:
      - Sum their quantities
      - Keep the first occurrence's names and description
    """
    seen: Dict[str, Dict] = {}
    no_pn_counter = 0
    for part in parts:
        pn = (part.get("part_number") or "").strip()
        if not pn:
            key = f"__no_pn_{no_pn_counter}"
            no_pn_counter += 1
            seen[key] = dict(part)
            continue
        if pn in seen:
            seen[pn]["quantity"] = (
                int(seen[pn].get("quantity") or 0) + int(part.get("quantity") or 0)
            )
        else:
            seen[pn] = dict(part)
    return list(seen.values())


def assign_target_ids(parts: List[Dict], start_index: int = 1) -> List[Dict]:
    """
    Assign sequential T-IDs (T001, T002 …) starting from start_index.
    Always consecutive regardless of original serial_no values.
    """
    result = []
    for i, part in enumerate(parts, start=start_index):
        p = dict(part)
        p["target_id"] = f"T{i:03d}"
        result.append(p)
    return result


def process_subtype_parts(parts: List[Dict], start_index: int = 1) -> List[Dict]:
    """Deduplicate then assign sequential T-IDs."""
    return assign_target_ids(deduplicate_parts(parts), start_index=start_index)


def parts_to_data_items(parts: List[Dict]) -> List[Dict]:
    """
    Convert processed parts list into the data_items JSON array expected by the
    POST /item_category/create and PUT /item_category/{id} API endpoints.
    """
    return [
        {
            "target_id":            p.get("target_id", ""),
            "part_number":          p.get("part_number", ""),
            "catalog_item_name_en": p.get("name_en", ""),
            "catalog_item_name_ch": p.get("name_cn", ""),
            "quantity":             int(p.get("quantity") or 1),
            "unit":                 p.get("unit", "pcs"),
            "description":          p.get("description", ""),
        }
        for p in parts
    ]


# ---------------------------------------------------------------------------
# JSON parsing helper
# ---------------------------------------------------------------------------

def _parse_json(text: str) -> Dict:
    """Strip optional markdown code fences and parse JSON."""
    text = re.sub(r"^```(?:json)?\s*", "", text.strip(), flags=re.MULTILINE)
    text = re.sub(r"\s*```$",           "", text.strip(), flags=re.MULTILINE)
    return json.loads(text.strip())