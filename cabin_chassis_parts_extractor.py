

from __future__ import annotations

import base64
import hashlib
import json
import logging
import re
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

import fitz

from pdf_utils import extract_response_text  # PyMuPDF

logger = logging.getLogger(__name__)

# Maximum number of pages to process in parallel.
# Keep at or below 5 to respect Sumopod rate limits.
_MAX_WORKERS = 5


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


def _call_vision(b64: str, sumopod_client, detail: str = "high",
                 system_prompt: Optional[str] = None) -> Optional[Dict]:
    """
    Send one page to vision AI for full parts extraction.

    Args:
        b64:            Base64-encoded JPEG of the page.
        sumopod_client: Initialised SumopodClient.
        detail:         OpenAI image detail level — "high" (default) or "low".
                        Use "low" for clean-print PDFs to save tokens & latency.
    """
    raw = ""
    _prompt = system_prompt or _PARTS_SYSTEM_PROMPT
    try:
        resp = sumopod_client.client.chat.completions.create(
            model=sumopod_client.model,
            messages=[
                {"role": "system", "content": _prompt},
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {
                                "url":    f"data:image/jpeg;base64,{b64}",
                                "detail": detail,
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
        raw = extract_response_text(resp)

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
    """
    Send one page to vision AI to read only the header / TOC structure.
    Uses low detail and small token budget — cheap pre-screening call.
    """
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
                                "detail": "low",  # header-only read — low detail is enough
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
        raw = extract_response_text(resp)
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
                "unit":                 "",
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

    Uses the cheap _call_category_vision() on every page (low detail, 1024 tokens).
    Diagram pages are discarded after classification.

    Returns:
        {
          "categories": [ { category_name_en, category_name_cn,
                            category_description, data_type: [...] } ],
          "code_to_category": { "<subtype_code>": "<category_name_en>", ... }
        }
    """
    logger.info("Cabin & Chassis category extractor: opening '%s'", pdf_path)

    doc         = fitz.open(pdf_path)
    total_pages = len(doc)
    logger.info("%d pages in PDF", total_pages)

    # category_name_en → { cn, subtypes: OrderedDict{dedup_key → subtype_dict} }
    categories:       OrderedDict       = OrderedDict()
    code_to_category: Dict[str, str]    = {}
    cat_cn_map:       Dict[str, str]    = {}
    seen_hashes:      set               = set()

    # ── Render all pages up-front (fast, no network) ──────────────────────
    page_b64s: Dict[int, str] = {}
    for idx in range(total_pages):
        try:
            b64 = _render_page_to_b64(doc, idx, dpi=dpi)
            h   = _image_hash(b64)
            if h in seen_hashes:
                logger.debug("Page %d: duplicate image — skipped", idx + 1)
                continue
            seen_hashes.add(h)
            page_b64s[idx] = b64
        except Exception as exc:
            logger.warning("Page %d: render failed: %s", idx + 1, exc)

    doc.close()

    # ── Classify pages in parallel ────────────────────────────────────────
    def _classify(idx: int) -> Tuple[int, Optional[Dict]]:
        return idx, _call_category_vision(page_b64s[idx], sumopod_client)

    page_results: Dict[int, Optional[Dict]] = {}
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
        futures = {executor.submit(_classify, idx): idx for idx in page_b64s}
        for future in as_completed(futures):
            idx, result = future.result()
            page_results[idx] = result

    # ── Process results in document order ─────────────────────────────────
    for idx in sorted(page_results):
        page_num = idx + 1
        result   = page_results[idx]

        if result is None:
            logger.debug("Page %d: no result from vision AI — skipped", page_num)
            continue

        page_type = result.get("page_type")

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

                    dedup_key  = code or name_en
                    display_en = f"{code} {name_en}".strip() if code else name_en
                    code_to_category[dedup_key] = cat_en

                    if dedup_key not in categories[cat_en]["subtypes"]:
                        categories[cat_en]["subtypes"][dedup_key] = {
                            "type_category_name_en": display_en,
                            "type_category_name_cn": name_cn,
                            "type_category_description": "",
                        }

        elif page_type == "table":
            code    = (result.get("subtype_code")    or "").replace(" ", "").strip()
            name_en = (result.get("subtype_name_en") or "").strip()
            name_cn = (result.get("subtype_name_cn") or "").strip()

            if not name_en and not code:
                logger.debug("Page %d: table page with no subtype header — skipped", page_num)
                continue

            dedup_key      = code or name_en
            display_en     = f"{code} {name_en}".strip() if code else name_en
            parent_cat_en  = code_to_category.get(dedup_key, category_name_en)

            if parent_cat_en not in categories:
                categories[parent_cat_en] = {
                    "cn":      cat_cn_map.get(parent_cat_en, category_name_cn),
                    "subtypes": OrderedDict(),
                }

            if dedup_key not in categories[parent_cat_en]["subtypes"]:
                categories[parent_cat_en]["subtypes"][dedup_key] = {
                    "type_category_name_en": display_en,
                    "type_category_name_cn": name_cn,
                    "type_category_description": "",
                }
                logger.info(
                    "Page %d: table confirmed subtype '%s' under '%s'",
                    page_num, display_en, parent_cat_en,
                )

        elif page_type == "diagram":
            logger.debug("Page %d: diagram — skipped", page_num)

        else:
            logger.debug("Page %d: unknown page_type=%r — skipped", page_num, page_type)

    # ── Build final output ─────────────────────────────────────────────────
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
        "code_to_category": code_to_category,
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
    vision_detail: str = "high",
    custom_prompt: Optional[str] = None
) -> List[Dict]:
    """
    Extract all parts from a Cabin & Chassis partbook PDF.

    Speed improvements vs v1:
      • Each page is first screened with the cheap _call_category_vision()
        (low detail, 1024 tokens) to identify and skip diagram pages before
        spending a full _call_vision() call (high detail, 4096 tokens).
      • Pages are processed in parallel using ThreadPoolExecutor (max 5 workers).

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
        vision_detail:      Detail level for the full parts extraction call.
                            "high" (default) for scanned/photo PDFs.
                            "low" for clean digital prints — faster & cheaper.

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

    doc         = fitz.open(pdf_path)
    total_pages = len(doc)
    logger.info("%d pages in PDF", total_pages)

    _code_to_category: Dict[str, str] = dict(code_to_category or {})
    _cat_cn_map:       Dict[str, str] = {}
    seen_hashes:       set            = set()

    # ── Render all pages up-front ─────────────────────────────────────────
    page_b64s: Dict[int, str] = {}
    for idx in range(total_pages):
        try:
            b64 = _render_page_to_b64(doc, idx, dpi=dpi)
            h   = _image_hash(b64)
            if h in seen_hashes:
                logger.info("Page %d: duplicate content — skipped", idx + 1)
                continue
            seen_hashes.add(h)
            page_b64s[idx] = b64
        except Exception as exc:
            logger.warning("Page %d: render failed: %s", idx + 1, exc)

    doc.close()
    logger.info("%d unique pages to process", len(page_b64s))

    # ── Parallel: pre-screen then extract ────────────────────────────────
    def _process_page(idx: int) -> Tuple[int, Optional[Dict]]:
        """
        1. Cheap classification call to detect diagrams.
        2. Full parts extraction call only on non-diagram pages.
        """
        b64      = page_b64s[idx]
        page_num = idx + 1

        # Step 1 — cheap classification (low detail, 1024 tokens)
        classification = _call_category_vision(b64, sumopod_client)
        if classification is None:
            logger.warning("Page %d: classification failed — skipped", page_num)
            return idx, None

        page_type = classification.get("page_type")

        if page_type == "diagram":
            logger.debug("Page %d: diagram — skipped (pre-screen)", page_num)
            return idx, None

        if page_type == "toc":
            # TOC pages don't need the expensive parts call; return the
            # classification result directly so the grouping loop can update
            # the code_to_category map from any TOC pages in this pass.
            logger.info("Page %d: TOC page detected", page_num)
            return idx, classification

        # Step 2 — full parts extraction (configurable detail, 4096 tokens)
        logger.info(
            "Page %d/%d: table page confirmed — running full extraction …",
            page_num, total_pages,
        )
        result = _call_vision(b64, sumopod_client, detail=vision_detail, system_prompt=custom_prompt)
        return idx, result

    page_results: Dict[int, Optional[Dict]] = {}
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
        futures = {executor.submit(_process_page, idx): idx for idx in page_b64s}
        for future in as_completed(futures):
            idx, result = future.result()
            page_results[idx] = result

    # ── Process results in document order ─────────────────────────────────
    # groups: subtype dedup_key → { meta + raw_parts list }
    groups: OrderedDict = OrderedDict()

    for idx in sorted(page_results):
        page_num = idx + 1
        result   = page_results[idx]

        if result is None:
            continue

        page_type = result.get("page_type")

        # ── TOC pages: update code→category map on the fly ────────────────
        if page_type == "toc":
            for cat in result.get("categories", []):
                cat_en = (cat.get("category_name_en") or "").strip()
                cat_cn = (cat.get("category_name_cn") or "").strip()
                if cat_en:
                    _cat_cn_map[cat_en] = cat_cn
                for sub in cat.get("subtypes", []):
                    code    = (sub.get("code")    or "").replace(" ", "").strip()
                    name_en = (sub.get("name_en") or "").strip()
                    if code and cat_en:
                        _code_to_category.setdefault(code, cat_en)
                    if name_en and cat_en:
                        _code_to_category.setdefault(name_en, cat_en)
            continue

        # ── Table pages: accumulate parts into groups ──────────────────────
        if page_type == "table":
            raw_code = (result.get("subtype_code")    or "").replace(" ", "").strip()
            name_en  = (result.get("subtype_name_en") or "").strip()
            name_cn  = (result.get("subtype_name_cn") or "").strip()
            parts    = result.get("parts", [])

            if not name_en and not raw_code:
                logger.debug("Page %d: table with no subtype header — skipped", page_num)
                continue

            dedup_key     = raw_code or name_en
            parent_cat_en = _code_to_category.get(dedup_key, "")
            parent_cat_cn = _cat_cn_map.get(parent_cat_en, "")

            if dedup_key not in groups:
                groups[dedup_key] = {
                    "category_name_en": parent_cat_en,
                    "category_name_cn": parent_cat_cn,
                    "subtype_code":     raw_code,
                    "subtype_name_en":  name_en,
                    "subtype_name_cn":  name_cn,
                    "raw_parts":        [],
                }
                logger.info(
                    "Page %d: new subtype group '%s' (category: '%s')",
                    page_num, name_en, parent_cat_en,
                )

            groups[dedup_key]["raw_parts"].extend(parts)
            logger.info("Page %d: added %d raw parts to '%s'", page_num, len(parts), name_en)

        else:
            logger.debug("Page %d: page_type=%r — skipped", page_num, page_type)

    # ── Deduplicate, merge quantities, assign T-IDs ────────────────────────
    output:   List[Dict] = []
    t_cursor: int        = target_id_start

    for dedup_key, grp in groups.items():
        merged = _merge_parts(grp["raw_parts"])
        if not merged:
            logger.info("Subtype '%s': all parts filtered out — skipped", grp["subtype_name_en"])
            continue

        merged    = _assign_target_ids(merged, start=t_cursor)

        output.append({
            "category_name_en": grp["category_name_en"],
            "category_name_cn": grp["category_name_cn"],
            "subtype_code":     grp["subtype_code"],
            "subtype_name_en":  grp["subtype_name_en"],
            "subtype_name_cn":  grp["subtype_name_cn"],
            "parts":            merged,
        })

    total_parts = sum(len(g["parts"]) for g in output)
    logger.info(
        "Parts extraction complete: %d subtype groups, %d total parts (T%03d–T%03d)",
        len(output), total_parts, target_id_start, t_cursor - 1,
    )

    return output