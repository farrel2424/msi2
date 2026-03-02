"""
cabin_chassis_parts_extractor.py
─────────────────────────────────────────────────────────────────────────────
Extracts PARTS DATA (the actual line-items in each table) from a Cabin &
Chassis partbook — a standard, pure PDF file.

Strategy
─────────
Each page is rendered to a JPEG via PyMuPDF (fitz) at 150 DPI, then sent to
vision AI.  Pages are classified as:

  • Diagram page  — shows an exploded-view illustration, no parts table
  • Table page    — shows the 6-column parts table with a bilingual header

Diagram pages are skipped automatically.  Duplicate pages (identical pixel
content — e.g. the same page reprinted at a different page number) are
detected by SHA-256 hash and skipped.

Table structure (6 columns, left → right)
──────────────────────────────────────────
  序号 (Serial) │ 编码 (Part Number) │ 名称 (CN Name) │
  NAME (EN Name) │ 数量 (Quantity) │ 备注 (Remarks)

Subtype header (one line above each table)
──────────────────────────────────────────
  <code>  <English name>  <Chinese name>
  e.g.  "DC97259190594  Air Intake System  进气系统"

Tables that span multiple pages share the same header — they are
concatenated into one continuous parts list before deduplication.

Deduplication rules (per spec)
───────────────────────────────
  • Same encoding + same CN name  →  merge: sum quantities, keep first row
  • Same encoding + different CN  →  distinct rows (keep both)
  • Empty encoding               →  skip (padding / blank rows)

T-number assignment
───────────────────
  Sequential T001, T002 … across ALL subtype groups in the document.
  Gaps in the partbook's own serial numbers are corrected.
  Starts from target_id_start (1 = fresh, or last-DB-T + 1 for appends).

Output
──────
List of subtype-group dicts:
[
  {
    "subtype_code":    "DC97259190594",
    "subtype_name_en": "Air Intake System",
    "subtype_name_cn": "进气系统",
    "parts": [
      {
        "target_id":            "T001",
        "part_number":          "Q151B1440TF3",
        "catalog_item_name_en": "Hexagon Head Bolt-Fine Thread",
        "catalog_item_name_ch": "六角头螺栓-细牙*",
        "quantity":             2,
        "description":          "",
        "unit":                 "pcs"
      },
      ...
    ]
  },
  ...
]
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import re
from collections import OrderedDict
from typing import Dict, List, Optional

import fitz  # PyMuPDF

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Vision prompt
# ─────────────────────────────────────────────────────────────────────────────

_PARTS_SYSTEM_PROMPT = """\
You are a precise data-extraction engine for automotive parts catalog pages.

Each TABLE page contains:
1. A bilingual header ABOVE the table in the format:
      <code>  <English name>  <Chinese name>
   Example: "DC97259190594  Air Intake System  进气系统"
   The code is alphanumeric (starts with letters like DC, DZ, Q followed by digits).

2. A table with exactly 6 columns (left to right):
   序号 | 编码 | 名称 | NAME | 数量 | 备注

DIAGRAM pages show only an exploded-view illustration — no table is present.

RULES:
• If this is a DIAGRAM page, return {"page_type": "diagram"}.
• If this is a TABLE page:
  - Extract the subtype header (code, English name, Chinese name).
  - Extract ONLY rows where 编码 (part number) is non-empty.
  - Ignore blank/padding rows at the bottom of the table.
  - serial_no: the integer in 序号, or null if the cell is blank.
  - quantity: positive integer; use 1 if unreadable.
  - remarks: empty string "" if the cell is blank.
  - Do NOT invent data. Blank cell → empty string or null.
• Return ONLY valid JSON — no markdown fences, no explanation.

OUTPUT FORMAT (table page):
{
  "page_type": "table",
  "subtype_code": "<code, spaces removed>",
  "subtype_name_en": "<English name from header>",
  "subtype_name_cn": "<Chinese name from header>",
  "parts": [
    {
      "serial_no": <integer or null>,
      "part_number": "<编码>",
      "name_cn": "<名称>",
      "name_en": "<NAME>",
      "quantity": <integer>,
      "remarks": "<备注 or empty string>"
    }
  ]
}

OUTPUT FORMAT (diagram page):
{
  "page_type": "diagram"
}"""


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _render_page_to_b64(doc: fitz.Document, page_index: int, dpi: int = 150) -> str:
    """Render one PDF page to a JPEG and return as base64."""
    page = doc[page_index]
    mat  = fitz.Matrix(dpi / 72, dpi / 72)
    pix  = page.get_pixmap(matrix=mat, alpha=False)
    return base64.b64encode(pix.tobytes("jpeg")).decode("utf-8")


def _image_hash(b64: str) -> str:
    """SHA-256 of raw JPEG bytes — used for duplicate-page detection."""
    return hashlib.sha256(base64.b64decode(b64)).hexdigest()


def _call_vision(b64: str, sumopod_client) -> Optional[Dict]:
    """Send one rendered page to vision AI and return the parsed dict."""
    raw = ""
    try:
        resp = sumopod_client.client.chat.completions.create(
            model=sumopod_client.model,
            messages=[
                {"role": "system", "content": _PARTS_SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {
                                "url":    f"data:image/jpeg;base64,{b64}",
                                "detail": "high",
                            },
                        },
                        {
                            "type": "text",
                            "text": (
                                "Classify this page and extract all parts data. "
                                "Follow the system prompt exactly."
                            ),
                        },
                    ],
                },
            ],
            temperature=0.0,
            max_tokens=4096,
            timeout=120,
        )

        raw = resp.choices[0].message.content.strip()
        # Strip markdown fences if the model adds them
        if raw.startswith("```"):
            raw = re.sub(r"^```[a-zA-Z]*\n?", "", raw)
            raw = re.sub(r"\n?```$", "", raw)
        return json.loads(raw.strip())

    except json.JSONDecodeError as exc:
        logger.warning("Vision AI returned non-JSON: %s … (%s)", raw[:300], exc)
        return None
    except Exception as exc:
        logger.warning("Vision AI call failed: %s", exc)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Deduplication & T-ID assignment
# ─────────────────────────────────────────────────────────────────────────────

def _merge_parts(raw_parts: List[Dict]) -> List[Dict]:
    """
    Deduplicate and merge parts within one subtype group.

    Key = (part_number, name_cn):
      • Same key           → sum quantities, keep first row's other fields
      • Different CN name  → distinct entry (different key)
      • Empty part_number  → skip (padding row)
    """
    merged: OrderedDict = OrderedDict()

    for p in raw_parts:
        pn = (p.get("part_number") or "").strip()
        if not pn:
            continue
        cn  = (p.get("name_cn") or "").strip()
        key = (pn, cn)

        try:
            qty = int(p.get("quantity") or 1)
        except (ValueError, TypeError):
            qty = 1

        if key in merged:
            merged[key]["quantity"] += qty
        else:
            merged[key] = {
                "part_number":          pn,
                "catalog_item_name_en": (p.get("name_en") or "").strip(),
                "catalog_item_name_ch": cn,
                "quantity":             qty,
                "description":          (p.get("remarks") or "").strip(),
                "unit":                 "pcs",
            }

    return list(merged.values())


def _assign_target_ids(parts: List[Dict], start: int) -> List[Dict]:
    """Add sequential target_id (T001, T002 …) starting at `start` (1-based)."""
    for i, part in enumerate(parts, start=start):
        part["target_id"] = f"T{i:03d}"
    return parts


# ─────────────────────────────────────────────────────────────────────────────
# Category (structure) extraction for image-based PDFs
# ─────────────────────────────────────────────────────────────────────────────

_CATEGORY_SYSTEM_PROMPT = """\
You are reading a Cabin & Chassis automotive parts catalog page image.

Each TABLE page has a single bilingual subtype header printed above the table,
in this exact format on one line:
    <code>  <English name>  <Chinese name>
Examples:
    "DC97259800020  Front Accessories Of Frame  车架前端附件"
    "DC97259190594  Air Intake System  进气系统"
    "DC95259200037  Oil Pan Protection Shield  油底壳保护罩"

Rules:
- The code is alphanumeric: letters (DC, DZ, Q, C, D followed by digits).
  It may appear with spaces between letters and digits in the image (e.g.
  "D C97259800020") — remove those spaces when you return it.
- The English name follows the code (Title Case words).
- The Chinese name follows the English name (Chinese characters).
- DIAGRAM pages show only an exploded-view illustration with NO table and NO
  parts list — they sometimes show the same header line at the top but have
  no table columns below it.
- TABLE pages show column headers: 序号 | 编码 | 名称 | NAME | 数量 | 备注

Return ONLY valid JSON, no markdown fences, no explanation.

If this is a TABLE page:
{
  "page_type": "table",
  "subtype_code": "<code with spaces removed>",
  "subtype_name_en": "<English name only>",
  "subtype_name_cn": "<Chinese name only>"
}

If this is a DIAGRAM page OR the page has no subtype header:
{
  "page_type": "other"
}"""


def _call_category_vision(b64: str, sumopod_client) -> Optional[Dict]:
    """Send one rendered page to vision AI to read the subtype header only."""
    raw = ""
    try:
        resp = sumopod_client.client.chat.completions.create(
            model=sumopod_client.model,
            messages=[
                {"role": "system", "content": _CATEGORY_SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {
                                "url":    f"data:image/jpeg;base64,{b64}",
                                "detail": "low",  # header-only read, low detail is enough
                            },
                        },
                        {
                            "type": "text",
                            "text": "Classify this page and extract the subtype header. Follow the system prompt exactly.",
                        },
                    ],
                },
            ],
            temperature=0.0,
            max_tokens=256,
            timeout=60,
        )

        raw = resp.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = re.sub(r"^```[a-zA-Z]*\n?", "", raw)
            raw = re.sub(r"\n?```$", "", raw)
        return json.loads(raw.strip())

    except json.JSONDecodeError as exc:
        logger.warning("Category vision AI returned non-JSON: %s (%s)", raw[:200], exc)
        return None
    except Exception as exc:
        logger.warning("Category vision AI call failed: %s", exc)
        return None


def extract_cabin_chassis_categories(
    pdf_path: str,
    sumopod_client,
    category_name_en: str = "Cabin & Chassis",
    category_name_cn: str = "\u9a7e\u9a76\u5ba4\u548c\u5e95\u76d8",
    dpi: int = 150,
) -> Dict:
    """
    Extract Category / Type-Category structure from an image-based Cabin &
    Chassis PDF (fallback when pymupdf4llm returns empty markdown).

    Renders each page with fitz, reads the bilingual subtype header
    (code + English + Chinese) directly from the image — no translation
    step needed because EN is already present in the header.

    Returns the same dict shape as the other extractors:
    {
      "categories": [
        {
          "category_name_en": "Cabin & Chassis",
          "category_name_cn": "...",
          "category_description": "",
          "data_type": [
            {
              "type_category_name_en": "DC97259800020 Front Accessories Of Frame",
              "type_category_name_cn": "...",
              "type_category_description": ""
            },
            ...
          ]
        }
      ]
    }
    """
    logger.info("Cabin & Chassis category extractor: opening '%s'", pdf_path)

    doc = fitz.open(pdf_path)
    total_pages = len(doc)
    logger.info("%d pages to scan", total_pages)

    seen_hashes: set = set()
    # Use OrderedDict to preserve page order and deduplicate by code
    seen_codes: OrderedDict = OrderedDict()

    for idx in range(total_pages):
        page_num = idx + 1
        try:
            b64 = _render_page_to_b64(doc, idx, dpi=dpi)
        except Exception as exc:
            logger.warning("Page %d: render failed: %s", page_num, exc)
            continue

        h = _image_hash(b64)
        if h in seen_hashes:
            logger.debug("Page %d: duplicate image - skipped", page_num)
            continue
        seen_hashes.add(h)

        logger.info("Page %d/%d: reading subtype header ...", page_num, total_pages)
        result = _call_category_vision(b64, sumopod_client)

        if result is None or result.get("page_type") != "table":
            logger.debug("Page %d: not a table page - skipped", page_num)
            continue

        code    = (result.get("subtype_code")    or "").replace(" ", "").strip()
        name_en = (result.get("subtype_name_en") or "").strip()
        name_cn = (result.get("subtype_name_cn") or "").strip()

        if not name_en and not code:
            logger.warning("Page %d: table page but no header extracted - skipped", page_num)
            continue

        # Use code as dedup key; fall back to EN name if code missing
        dedup_key = code or name_en
        if dedup_key in seen_codes:
            logger.debug("Page %d: duplicate subtype '%s' - skipped", page_num, dedup_key)
            continue

        # type_category_name_en includes the code prefix (same convention as
        # the existing markdown prompt: "DC97259800020 Front Accessories Of Frame")
        if code:
            display_en = f"{code} {name_en}".strip()
        else:
            display_en = name_en

        seen_codes[dedup_key] = {
            "type_category_name_en": display_en,
            "type_category_name_cn": name_cn,
            "type_category_description": "",
        }
        logger.info("Page %d: new subtype: '%s'", page_num, display_en)

    doc.close()

    subtypes = list(seen_codes.values())
    logger.info(
        "Cabin & Chassis category extraction complete: %d unique subtypes", len(subtypes)
    )

    return {
        "categories": [
            {
                "category_name_en":      category_name_en,
                "category_name_cn":      category_name_cn,
                "category_description":  "",
                "data_type":             subtypes,
            }
        ]
    }


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def extract_cabin_chassis_parts(
    pdf_path: str,
    sumopod_client,
    target_id_start: int = 1,
    dpi: int = 150,
) -> List[Dict]:
    """
    Extract all parts from a Cabin & Chassis partbook PDF.

    Args:
        pdf_path:        Path to the pure PDF partbook file.
        sumopod_client:  Initialised SumopodClient with vision capability.
        target_id_start: T-number index to start from.
                         Pass 1 for a fresh item_category, or call
                         epc_client.get_next_target_id_start(item_category_id)
                         to continue from an existing DB sequence.
        dpi:             Render resolution (150 DPI is sufficient for table OCR).

    Returns:
        List of subtype-group dicts, each with:
          {
            "subtype_code":    str,
            "subtype_name_en": str,
            "subtype_name_cn": str,
            "parts":           List[Dict]  # T-IDs assigned, quantities merged
          }
        T-IDs are sequential across ALL groups (global sequence).
    """
    logger.info("Cabin & Chassis parts extractor: opening '%s'", pdf_path)

    doc = fitz.open(pdf_path)
    total_pages = len(doc)
    logger.info("%d pages in PDF", total_pages)

    # groups: subtype_code (or name_en fallback) → {meta, raw_parts}
    groups: OrderedDict = OrderedDict()
    seen_hashes: set    = set()

    for idx in range(total_pages):
        page_num = idx + 1
        try:
            b64 = _render_page_to_b64(doc, idx, dpi=dpi)
        except Exception as exc:
            logger.warning("Page %d: render failed: %s", page_num, exc)
            continue

        # Duplicate-page detection (same pixel content at different page offsets)
        h = _image_hash(b64)
        if h in seen_hashes:
            logger.info("Page %d: duplicate content — skipped", page_num)
            continue
        seen_hashes.add(h)

        logger.info("Page %d/%d: calling vision AI …", page_num, total_pages)
        result = _call_vision(b64, sumopod_client)

        if result is None:
            logger.warning("Page %d: no result from vision AI, skipping", page_num)
            continue

        page_type = result.get("page_type")

        if page_type == "diagram":
            logger.debug("Page %d: diagram — skipped", page_num)
            continue

        if page_type != "table":
            logger.warning(
                "Page %d: unexpected page_type=%r — skipped", page_num, page_type
            )
            continue

        code    = (result.get("subtype_code")    or "").replace(" ", "").strip()
        name_en = (result.get("subtype_name_en") or "").strip()
        name_cn = (result.get("subtype_name_cn") or "").strip()
        rows    = result.get("parts") or []

        if not code and not name_en:
            logger.warning("Page %d: table page but no header found — skipped", page_num)
            continue

        logger.info(
            "Page %d: subtype '%s' ('%s') — %d raw rows",
            page_num, name_en or code, code, len(rows)
        )

        # Pages with the same header belong to one split table → same group
        group_key = code or name_en
        if group_key not in groups:
            groups[group_key] = {
                "subtype_code":    code,
                "subtype_name_en": name_en,
                "subtype_name_cn": name_cn,
                "raw_parts":       [],
            }
        groups[group_key]["raw_parts"].extend(rows)

    doc.close()

    # ── Deduplicate, merge quantities, assign T-IDs ──────────────────────────
    output: List[Dict] = []
    current_t = target_id_start

    for group in groups.values():
        merged    = _merge_parts(group["raw_parts"])
        merged    = _assign_target_ids(merged, current_t)
        current_t += len(merged)

        output.append({
            "subtype_code":    group["subtype_code"],
            "subtype_name_en": group["subtype_name_en"],
            "subtype_name_cn": group["subtype_name_cn"],
            "parts":           merged,
        })

        logger.info(
            "Subtype '%s': %d unique parts (T%03d – T%03d)",
            group["subtype_name_en"],
            len(merged),
            current_t - len(merged),
            current_t - 1,
        )

    logger.info(
        "Extraction complete: %d subtype groups, %d total unique parts",
        len(output),
        sum(len(g["parts"]) for g in output),
    )
    return output