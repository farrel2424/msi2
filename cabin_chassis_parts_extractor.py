"""
cabin_chassis_parts_extractor.py
─────────────────────────────────────────────────────────────────────────────
Extracts PARTS DATA and CATEGORY STRUCTURE from a Cabin & Chassis partbook.

Hierarchy (correct):
  Master Category  →  Category        →  Type Category (Subtype)
  "Cabin & Chassis"   "Frame System"     "Front Accessories Of Frame"

Strategy
─────────
Each page is rendered to a JPEG via PyMuPDF (fitz) at 150 DPI, then sent to
vision AI.  Pages are classified as:

  • Diagram page  — exploded-view illustration, no parts table → skip
  • TOC page      — numbered table of contents listing Categories + Subtypes
  • Table page    — the 6-column parts table with a bilingual subtype header

Table structure (6 columns, left → right)
──────────────────────────────────────────
  序号 (Serial) │ 编码 (Part Number) │ 名称 (CN Name) │
  NAME (EN Name) │ 数量 (Quantity) │ 备注 (Remarks)

Subtype header (one line above each table)
──────────────────────────────────────────
  <code>  <English name>  <Chinese name>
  e.g.  "DC97259190594  Air Intake System  进气系统"

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

Output of extract_cabin_chassis_parts()
────────────────────────────────────────
List of subtype-group dicts:
[
  {
    "category_name_en": "Frame System",
    "category_name_cn": "车架系统",
    "subtype_code":     "DC97259190594",
    "subtype_name_en":  "Air Intake System",
    "subtype_name_cn":  "进气系统",
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

Output of extract_cabin_chassis_categories()
─────────────────────────────────────────────
{
  "categories": [
    {
      "category_name_en": "Frame System",
      "category_name_cn": "车架系统",
      "category_description": "",
      "data_type": [
        {
          "type_category_name_en": "DC97259880020 Front Accessories Of Frame",
          "type_category_name_cn": "车架前端附件",
          "type_category_description": ""
        },
        ...
      ]
    },
    ...
  ],
  "code_to_category": {
    "DC97259880020": "Frame System",
    ...
  }
}
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
# Vision prompt — PARTS extraction (table body)
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

TOC pages show a numbered list of section headings and part entries with page numbers.

RULES:
• If this is a DIAGRAM page, return {"page_type": "diagram"}.
• If this is a TOC page, return {"page_type": "toc", "categories": [...]}.
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

OUTPUT FORMAT (toc page):
{
  "page_type": "toc",
  "categories": [
    {
      "category_name_en": "Frame System",
      "category_name_cn": "车架系统",
      "subtypes": [
        {"code": "DC97259880020", "name_en": "Front Accessories Of Frame", "name_cn": "车架前端附件"},
        {"code": "DC95259510002", "name_en": "Transmission Auxiliary Crossbeam", "name_cn": "变速器辅助横梁"}
      ]
    }
  ]
}

OUTPUT FORMAT (diagram page):
{
  "page_type": "diagram"
}"""


# ─────────────────────────────────────────────────────────────────────────────
# Vision prompt — CATEGORY extraction (header + TOC only)
# ─────────────────────────────────────────────────────────────────────────────

_CATEGORY_SYSTEM_PROMPT = """\
You are reading a Cabin & Chassis automotive parts catalog page image.

The partbook has this hierarchy:
  Master Category → Category → Type Category (Subtype)
  "Cabin & Chassis"  "Frame System"  "Front Accessories Of Frame"

THREE types of pages exist:

1. TOC PAGE — shows a numbered table of contents, e.g.:
   10   Frame System   车架系统   1
        DC97259880020  Front Accessories Of Frame  车架前端附件  ...4
        DC95259510002  Transmission Auxiliary Crossbeam  变速器辅助横梁  ...6
        DC95259980037  Frame Assembly  车架总成  ...8
   These pages identify Category names AND which subtypes belong to them.

2. TABLE PAGE — shows column headers 序号 | 编码 | 名称 | NAME | 数量 | 备注
   Has ONE bilingual subtype header printed above the table columns, e.g.:
   "DC97259800020  Front Accessories Of Frame  车架前端附件"

3. DIAGRAM PAGE — shows only an exploded-view illustration, no table columns.

Rules for codes:
- Codes are alphanumeric (DC, DZ, Q, C, D followed by digits).
- Spaces inside a code in the image (e.g. "D C97259800020") must be removed.

For a TOC PAGE return:
{
  "page_type": "toc",
  "categories": [
    {
      "category_name_en": "Frame System",
      "category_name_cn": "车架系统",
      "subtypes": [
        {"code": "DC97259880020", "name_en": "Front Accessories Of Frame", "name_cn": "车架前端附件"},
        {"code": "DC95259510002", "name_en": "Transmission Auxiliary Crossbeam", "name_cn": "变速器辅助横梁"},
        {"code": "DC95259980037", "name_en": "Frame Assembly", "name_cn": "车架总成"}
      ]
    }
  ]
}

For a TABLE PAGE return:
{
  "page_type": "table",
  "subtype_code": "DC97259800020",
  "subtype_name_en": "Front Accessories Of Frame",
  "subtype_name_cn": "车架前端附件"
}

For a DIAGRAM PAGE return:
{"page_type": "diagram"}

Return ONLY valid JSON, no markdown fences, no explanation.
"""


# ─────────────────────────────────────────────────────────────────────────────
# Low-level helpers
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
    """Send one page to vision AI for full parts extraction."""
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


def _call_category_vision(b64: str, sumopod_client) -> Optional[Dict]:
    """Send one page to vision AI to read only the header / TOC structure."""
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
                                "detail": "low",   # header-only read — low detail is enough
                            },
                        },
                        {
                            "type": "text",
                            "text": (
                                "Classify this page and extract the category/subtype header. "
                                "Follow the system prompt exactly."
                            ),
                        },
                    ],
                },
            ],
            temperature=0.0,
            max_tokens=1024,
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
# Public API — Stage 1: Category structure extraction
# ─────────────────────────────────────────────────────────────────────────────

def extract_cabin_chassis_categories(
    pdf_path: str,
    sumopod_client,
    category_name_en: str = "Cabin & Chassis",
    category_name_cn: str = "驾驶室和底盘",
    dpi: int = 150,
) -> Dict:
    """
    Extract Category + Type Category structure from a Cabin & Chassis PDF.

    Strategy:
      - TOC pages  → establish which Category each subtype belongs to
      - TABLE pages → confirm subtypes exist; fall back to "Uncategorized"
                      bucket if no TOC was seen yet for that code

    Returns:
    {
      "categories": [
        {
          "category_name_en": "Frame System",
          "category_name_cn": "车架系统",
          "category_description": "",
          "data_type": [
            {
              "type_category_name_en": "DC97259880020 Front Accessories Of Frame",
              "type_category_name_cn": "车架前端附件",
              "type_category_description": ""
            },
            ...
          ]
        },
        ...
      ],
      "code_to_category": {
        "DC97259880020": "Frame System",
        ...
      }
    }
    """
    logger.info("Cabin & Chassis category extractor: opening '%s'", pdf_path)

    doc = fitz.open(pdf_path)
    total_pages = len(doc)
    logger.info("%d pages to scan", total_pages)

    seen_hashes: set = set()

    # category_name_en → {"cn": str, "subtypes": OrderedDict{dedup_key: {...}}}
    categories: OrderedDict = OrderedDict()

    # Maps subtype dedup_key (code or name_en) → parent category_name_en
    # Populated by TOC pages so TABLE pages can find their parent.
    code_to_category: Dict[str, str] = {}
    # Maps category_name_en → category_name_cn (for cat_cn lookup)
    cat_cn_map: Dict[str, str] = {}

    for idx in range(total_pages):
        page_num = idx + 1
        try:
            b64 = _render_page_to_b64(doc, idx, dpi=dpi)
        except Exception as exc:
            logger.warning("Page %d: render failed: %s", page_num, exc)
            continue

        h = _image_hash(b64)
        if h in seen_hashes:
            logger.debug("Page %d: duplicate image — skipped", page_num)
            continue
        seen_hashes.add(h)

        logger.info("Page %d/%d: reading page type …", page_num, total_pages)
        result = _call_category_vision(b64, sumopod_client)

        if result is None:
            logger.debug("Page %d: no result from vision AI — skipped", page_num)
            continue

        page_type = result.get("page_type")

        # ── TOC page: builds category → subtypes mapping ──────────────────
        if page_type == "toc":
            for cat in result.get("categories", []):
                cat_en = (cat.get("category_name_en") or "").strip()
                cat_cn = (cat.get("category_name_cn") or "").strip()
                if not cat_en:
                    continue

                if cat_en not in categories:
                    categories[cat_en] = {"cn": cat_cn, "subtypes": OrderedDict()}
                    cat_cn_map[cat_en] = cat_cn
                    logger.info("Page %d: new category from TOC: '%s'", page_num, cat_en)

                for sub in cat.get("subtypes", []):
                    code    = (sub.get("code")    or "").replace(" ", "").strip()
                    name_en = (sub.get("name_en") or "").strip()
                    name_cn = (sub.get("name_cn") or "").strip()
                    if not name_en and not code:
                        continue

                    dedup_key = code or name_en
                    code_to_category[dedup_key] = cat_en

                    if dedup_key not in categories[cat_en]["subtypes"]:
                        display_en = f"{code} {name_en}".strip() if code else name_en
                        categories[cat_en]["subtypes"][dedup_key] = {
                            "type_category_name_en": display_en,
                            "type_category_name_cn": name_cn,
                            "type_category_description": "",
                        }
                        logger.info(
                            "Page %d: TOC subtype '%s' → category '%s'",
                            page_num, display_en, cat_en,
                        )

        # ── TABLE page: confirm subtype under its parent category ─────────
        elif page_type == "table":
            code    = (result.get("subtype_code")    or "").replace(" ", "").strip()
            name_en = (result.get("subtype_name_en") or "").strip()
            name_cn = (result.get("subtype_name_cn") or "").strip()

            if not name_en and not code:
                logger.warning(
                    "Page %d: table page but no header extracted — skipped", page_num
                )
                continue

            dedup_key = code or name_en
            parent_cat_en = code_to_category.get(dedup_key)

            if not parent_cat_en:
                # TOC not seen yet / subtype missing from TOC → fallback bucket
                parent_cat_en = "Uncategorized"
                if parent_cat_en not in categories:
                    categories[parent_cat_en] = {"cn": "", "subtypes": OrderedDict()}
                    logger.warning(
                        "Page %d: subtype '%s' has no TOC parent — placed in 'Uncategorized'",
                        page_num, name_en or code,
                    )

            if dedup_key not in categories[parent_cat_en]["subtypes"]:
                display_en = f"{code} {name_en}".strip() if code else name_en
                categories[parent_cat_en]["subtypes"][dedup_key] = {
                    "type_category_name_en": display_en,
                    "type_category_name_cn": name_cn,
                    "type_category_description": "",
                }
                logger.info(
                    "Page %d: table confirmed subtype '%s' under '%s'",
                    page_num, display_en, parent_cat_en,
                )
            else:
                logger.debug(
                    "Page %d: subtype '%s' already in '%s' — skipped",
                    page_num, dedup_key, parent_cat_en,
                )

        elif page_type == "diagram":
            logger.debug("Page %d: diagram — skipped", page_num)

        else:
            logger.debug("Page %d: unknown page_type=%r — skipped", page_num, page_type)

    doc.close()

    # Build final output
    output_categories = []
    for cat_en, cat_data in categories.items():
        output_categories.append({
            "category_name_en":     cat_en,
            "category_name_cn":     cat_data["cn"],
            "category_description": "",
            "data_type":            list(cat_data["subtypes"].values()),
        })

    logger.info(
        "Cabin & Chassis category extraction complete: %d categories, %d total subtypes",
        len(output_categories),
        sum(len(c["data_type"]) for c in output_categories),
    )

    return {
        "categories":       output_categories,
        "code_to_category": code_to_category,  # passed to extract_cabin_chassis_parts
    }


# ─────────────────────────────────────────────────────────────────────────────
# Public API — Stage 2: Parts extraction
# ─────────────────────────────────────────────────────────────────────────────

def extract_cabin_chassis_parts(
    pdf_path: str,
    sumopod_client,
    target_id_start: int = 1,
    dpi: int = 150,
    code_to_category: Optional[Dict[str, str]] = None,
) -> List[Dict]:
    """
    Extract all parts from a Cabin & Chassis partbook PDF.

    Args:
        pdf_path:           Path to the pure PDF partbook file.
        sumopod_client:     Initialised SumopodClient with vision capability.
        target_id_start:    T-number index to start from (1 = fresh).
        dpi:                Render resolution (150 DPI is sufficient).
        code_to_category:   Optional dict mapping subtype dedup_key (code or
                            name_en) → category_name_en.  Built by
                            extract_cabin_chassis_categories() in Stage 1.
                            When provided, each output group is tagged with
                            category_name_en / category_name_cn.

    Returns:
        List of subtype-group dicts, each with:
          {
            "category_name_en": "Frame System",
            "category_name_cn": "车架系统",
            "subtype_code":     "DC97259880020",
            "subtype_name_en":  "Front Accessories Of Frame",
            "subtype_name_cn":  "车架前端附件",
            "parts": [ { target_id, part_number, catalog_item_name_en,
                         catalog_item_name_ch, quantity, description, unit } ]
          }
        T-IDs are sequential across ALL subtype groups (global sequence).
    """
    logger.info("Cabin & Chassis parts extractor: opening '%s'", pdf_path)

    doc = fitz.open(pdf_path)
    total_pages = len(doc)
    logger.info("%d pages in PDF", total_pages)

    # groups: subtype dedup_key → {meta + raw_parts}
    groups: OrderedDict = OrderedDict()
    seen_hashes: set    = set()

    # Start with any code_to_category passed from Stage 1; supplement from TOC pages
    _code_to_category: Dict[str, str] = dict(code_to_category or {})
    # category_name_en → category_name_cn
    _cat_cn_map: Dict[str, str] = {}

    for idx in range(total_pages):
        page_num = idx + 1
        try:
            b64 = _render_page_to_b64(doc, idx, dpi=dpi)
        except Exception as exc:
            logger.warning("Page %d: render failed: %s", page_num, exc)
            continue

        h = _image_hash(b64)
        if h in seen_hashes:
            logger.info("Page %d: duplicate content — skipped", page_num)
            continue
        seen_hashes.add(h)

        logger.info("Page %d/%d: calling vision AI …", page_num, total_pages)
        result = _call_vision(b64, sumopod_client)

        if result is None:
            logger.warning("Page %d: no result from vision AI — skipped", page_num)
            continue

        page_type = result.get("page_type")

        # ── TOC page: update code→category map on the fly ─────────────────
        if page_type == "toc":
            for cat in result.get("categories", []):
                cat_en = (cat.get("category_name_en") or "").strip()
                cat_cn = (cat.get("category_name_cn") or "").strip()
                if cat_en and cat_en not in _cat_cn_map:
                    _cat_cn_map[cat_en] = cat_cn
                for sub in cat.get("subtypes", []):
                    code    = (sub.get("code")    or "").replace(" ", "").strip()
                    name_en = (sub.get("name_en") or "").strip()
                    dedup   = code or name_en
                    if dedup and dedup not in _code_to_category:
                        _code_to_category[dedup] = cat_en
            logger.debug("Page %d: TOC — updated code_to_category map", page_num)
            continue

        if page_type == "diagram":
            logger.debug("Page %d: diagram — skipped", page_num)
            continue

        if page_type != "table":
            logger.warning(
                "Page %d: unexpected page_type=%r — skipped", page_num, page_type
            )
            continue

        # ── TABLE page: extract subtype header + parts rows ───────────────
        code    = (result.get("subtype_code")    or "").replace(" ", "").strip()
        name_en = (result.get("subtype_name_en") or "").strip()
        name_cn = (result.get("subtype_name_cn") or "").strip()
        rows    = result.get("parts") or []

        if not code and not name_en:
            logger.warning("Page %d: table page but no header found — skipped", page_num)
            continue

        logger.info(
            "Page %d: subtype '%s' ('%s') — %d raw rows",
            page_num, name_en or code, code, len(rows),
        )

        group_key = code or name_en

        # Resolve parent category
        cat_en = _code_to_category.get(group_key, "Uncategorized")
        cat_cn = _cat_cn_map.get(cat_en, "")

        if group_key not in groups:
            groups[group_key] = {
                "category_name_en": cat_en,
                "category_name_cn": cat_cn,
                "subtype_code":     code,
                "subtype_name_en":  name_en,
                "subtype_name_cn":  name_cn,
                "raw_parts":        [],
            }

        # Pages with the same header = one split table → accumulate rows
        groups[group_key]["raw_parts"].extend(rows)

    doc.close()

    # ── Deduplicate, merge quantities, assign T-IDs ──────────────────────
    output: List[Dict] = []
    current_t = target_id_start

    for group in groups.values():
        merged    = _merge_parts(group["raw_parts"])
        merged    = _assign_target_ids(merged, current_t)
        current_t += len(merged)

        output.append({
            "category_name_en": group["category_name_en"],
            "category_name_cn": group["category_name_cn"],
            "subtype_code":     group["subtype_code"],
            "subtype_name_en":  group["subtype_name_en"],
            "subtype_name_cn":  group["subtype_name_cn"],
            "parts":            merged,
        })

        logger.info(
            "Subtype '%s' [%s]: %d unique parts (T%03d – T%03d)",
            group["subtype_name_en"],
            group["category_name_en"],
            len(merged),
            current_t - len(merged),
            current_t - 1,
        )

    logger.info(
        "Parts extraction complete: %d subtype groups, %d total unique parts",
        len(output),
        sum(len(g["parts"]) for g in output),
    )
    return output