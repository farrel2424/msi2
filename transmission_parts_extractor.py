"""
transmission_parts_extractor.py
================================
Stage 2 — Parts Management extractor for Transmission partbooks.

Pipeline position
-----------------
  Stage 1 (already done) → produces ``code_to_category`` map
      { "category_name_cn": "category_name_en" }

  Stage 2 (this file) → reads every page of the PDF with Vision AI,
      groups extracted rows by category section, deduplicates, assigns
      T-IDs, and returns a list of category-group dicts ready for the
      EPC API.

Hierarchy
---------
  Master Category  →  Category                    →  Parts
  "Transmission"       "离合器和变速器壳体总成"          [list of parts]

  No Subtype level.  Every section header is directly a Category.

Table structure (4 columns)
----------------------------
  代号 (serial_no) | 零件号 (part_number) | 零件名称. (name_cn) | 数量 (quantity)

Usage
-----
  from transmission_parts_extractor import extract_transmission_parts

  results = extract_transmission_parts(
      pdf_path       = "G34780__transmission.pdf",
      sumopod_client = client,               # initialised SumopodClient
      target_id_start = 1,
      category_map   = code_to_category,     # from Stage 1
  )
"""

from __future__ import annotations

import json
import logging
import re
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

from pdf_utils import extract_response_text, pdf_page_to_base64

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

_PART_NUMBER_PREFIX = "BS"

def _is_valid_section_header(raw: str) -> bool:
    """
    A real section header must contain the Chinese enumeration mark 、(U+3001)
    e.g. "一、离合器和变速器壳体总成" or "十五、QH50 取力器总成"
    Anything without 、 is a misread bold part name, not a section title.
    """
    return "\u3001" in raw
# ─────────────────────────────────────────────────────────────────────────────
# Vision AI system prompt
# ─────────────────────────────────────────────────────────────────────────────

_PARTS_SYSTEM_PROMPT = """\
You are a precise data-extraction engine for a Chinese-only automotive
transmission parts catalog.

PAGE TYPES:
1. COVER page — shows document title only.
   Return {"page_type": "cover"}.
2. TOC page — shows a numbered table of contents in Chinese.
   Return {"page_type": "toc"}.
3. CONTENT page — contains a parts table (4 columns) and optionally a
   section title header and/or an exploded-view diagram.

SECTION HEADER (marks a new category):
  A large bold CENTERED title ABOVE the table columns, in the format:
    <Chinese/number ordinal><、><Chinese section name>
  Examples: "一、离合器和变速器壳体总成", "十五、QH50 取力器总成"
  CRITICAL: The 、character MUST be present. Bold rows INSIDE the table
  (assembly headers with a part number) are NOT section headers — return
  section_header: null for those.
  If not present, set section_header to null.

TABLE STRUCTURE (4 columns, left to right):
  代号 | 零件号 | 零件名称. | 数量

EXTRACTION RULES:
- Extract ONLY rows where 零件号 (part number) is non-empty.
- 代号 (serial_no): extract as string (may be "8/9" or empty → null).
- 零件号 (part_number): extract as-is, remove surrounding spaces.
- 零件名称. (name_cn): full Chinese name including any parenthetical notes.
- 数量 (quantity): integer if numeric; null if "按需" or unreadable.
- Bold rows (assembly headers) without 代号 are VALID parts — extract them.
- Do NOT invent data. Blank cell → null or empty string.
- Return ONLY valid JSON — no markdown fences, no explanation.
- If a row has no 代号 AND its 零件号, 零件名称, and 数量 are bold and
  center-aligned, mark it as the assembly header: set "is_assembly_header": true.
- All other rows with empty 代号 are regular parts: "is_assembly_header": false.

OUTPUT FORMAT (content page):
{
  "page_type": "content",
  "section_header": "<section title string, or null>",
  "parts": [
    {
      "serial_no": "<string or null>",
      "part_number": "<零件号>",
      "name_cn": "<零件名称>",
      "quantity": <integer or null>,
      "is_assembly_header": false
    }
  ]
}

OUTPUT FORMAT (cover or toc):
{
  "page_type": "cover"
}
{
  "page_type": "toc"
}
"""

# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _call_vision(b64: str, sumopod_client, detail: str = "high") -> Optional[Dict]:
    """
    Send one PDF page (as base64 JPEG) to Vision AI and parse the result.

    Args:
        b64:            Base64-encoded JPEG of the rendered page.
        sumopod_client: Initialised SumopodClient with vision capability.
        detail:         OpenAI image detail level — "high" or "low".

    Returns:
        Parsed dict from the model, or None if parsing / API call fails.
    """
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
                                "url": f"data:image/jpeg;base64,{b64}",
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

        # Strip accidental markdown fences (model should not add them, but
        # just in case it does).
        if raw.startswith("```"):
            raw = re.sub(r"^```[a-zA-Z]*\n?", "", raw)
            raw = re.sub(r"\n?```$", "", raw)

        return json.loads(raw.strip())

    except json.JSONDecodeError as exc:
        logger.warning(
            "Vision AI returned non-JSON on page (truncated): %s … (%s)",
            raw[:300],
            exc,
        )
        return None
    except Exception as exc:
        logger.warning("Vision AI call failed: %s", exc)
        return None


def _add_prefix(part_number: str) -> str:
    """
    Prepend ``BS`` prefix to *part_number*.

    Idempotent — will not double-prefix if already starts with "BS".
    Applied in Python after extraction so the Vision AI prompt stays clean.

    Examples
    --------
    >>> _add_prefix("A-4740-1")
    'BSA-4740-1'
    >>> _add_prefix("BSQ1701032")   # already prefixed — no-op
    'BSQ1701032'
    """
    if not part_number:
        return part_number
    if part_number.startswith(_PART_NUMBER_PREFIX):
        return part_number
    return f"{_PART_NUMBER_PREFIX}{part_number}"


def _strip_section_number(section_header: str) -> str:
    """
    Remove the leading Chinese/Latin ordinal and 、 from a section header,
    returning only the Chinese category name.

    Examples
    --------
    >>> _strip_section_number("一、离合器和变速器壳体总成")
    '离合器和变速器壳体总成'
    >>> _strip_section_number("十五、QH50 取力器总成")
    'QH50 取力器总成'
    >>> _strip_section_number("二、二轴总成")
    '二轴总成'
    """
    # Pattern: optional leading chars (Chinese numerals, digits, Latin), then 、
    return re.sub(r"^[^\u3001]+\u3001", "", section_header).strip()


def _merge_parts(raw_parts: List[Dict]) -> List[Dict]:
    """
    Deduplicate and merge parts within one category group.

    Dedup key  = (part_number, name_cn)
    ─────────────────────────────────────────────────────
    • Same key            → merge: sum quantities (skip null qty), keep
                            all other fields from the FIRST occurrence.
    • Same part_number,
      different name_cn   → keep as separate rows.
    • Empty part_number   → skip entirely.
    • quantity = null     → keep the row; do NOT add to any sum.

    Returns
    -------
    List of deduplicated part dicts (order of first appearance preserved).
    """
    # OrderedDict preserves insertion order (first-seen wins for metadata).
    merged: OrderedDict[Tuple[str, str], Dict] = OrderedDict()

    for part in raw_parts:
        pn = (part.get("part_number") or "").strip()
        if not pn:
            logger.debug("Skipping part with empty part_number: %s", part)
            continue

        name = (part.get("name_cn") or "").strip()
        key = (pn, name)
        qty = part.get("quantity")

        if key not in merged:
            # First occurrence — store a clean copy.
            merged[key] = {
                "serial_no": part.get("serial_no"),
                "part_number": pn,
                "name_cn": name,
                "quantity": qty,
                "is_assembly_header": bool(part.get("is_assembly_header", False)),
            }
        else:
            # Duplicate — accumulate quantity only if both values are integers.
            existing_qty = merged[key]["quantity"]
            if existing_qty is not None and qty is not None:
                merged[key]["quantity"] = existing_qty + qty
            # If either side is null (按需), leave quantity as-is (null wins).

    return list(merged.values())


def _assign_target_ids(
    parts: List[Dict],
    counter_start: int = 1,
) -> Tuple[List[Dict], int]:
    """
    Assign T-IDs to a list of already-deduplicated parts.

    Rules
    -----
    • Assembly header row (``is_assembly_header=True``) → ``T000``.
      Only the FIRST such row per group receives T000; subsequent ones
      (shouldn't exist, but defensive) get sequential IDs.
    • All other rows receive sequential IDs: T001, T002, … continuing
      from *counter_start*.

    Args:
        parts:         Deduplicated part dicts for ONE category group.
        counter_start: The integer at which the sequential counter begins
                       (allows continuation across categories when
                       ``target_id_start`` > 1).

    Returns:
        (tagged_parts, next_counter_value)
    """
    tagged: List[Dict] = []
    counter = counter_start
    assembly_header_assigned = False

    for part in parts:
        result = {k: v for k, v in part.items() if k != "is_assembly_header"}

        if part.get("is_assembly_header") and not assembly_header_assigned:
            result["target_id"] = "T000"
            assembly_header_assigned = True
        else:
            result["target_id"] = f"T{counter:03d}"
            counter += 1

        tagged.append(result)

    return tagged, counter


def _build_output_part(part: Dict, cn_to_en: Optional[Dict[str, str]] = None) -> Dict:
    """
    Map an internal part dict (with target_id) to the canonical EPC output
    schema.

    Input keys  : target_id, part_number, name_cn, quantity, serial_no
    Output keys : target_id, part_number, catalog_item_name_en,
                  catalog_item_name_ch, quantity, description, unit

    Args:
        part:       Internal part dict (with target_id already assigned).
        cn_to_en:   Translation lookup built by _translate_part_names().
                    When provided, catalog_item_name_en is filled from it.
    """
    name_cn = part.get("name_cn") or ""
    name_en = (cn_to_en or {}).get(name_cn, "")
    return {
        "target_id": part["target_id"],
        "part_number": _add_prefix(part["part_number"]),
        "catalog_item_name_en": name_en,
        "catalog_item_name_ch": name_cn,
        "quantity": part.get("quantity"),
        "description": "",                   # no remarks column
        "unit": "",                          # no unit column
    }


# ─────────────────────────────────────────────────────────────────────────────
# Batch name translation
# ─────────────────────────────────────────────────────────────────────────────

_PARTS_TRANSLATION_PROMPT = """\
You are a professional automotive parts catalog translator (Chinese → English).
Translate each Chinese part name into clear, concise English.
These are spare part names from a heavy-truck transmission parts catalog.

Return ONLY valid JSON — no markdown, no explanation:
{
  "translations": [
    { "cn": "<original Chinese>", "en": "<English translation>" }
  ]
}

Rules:
- Same order and count as the input list.
- Use standard automotive / mechanical terminology.
- Keep abbreviations where widely accepted (e.g. "O-Ring", "Bolt M10×1.25").
- Do NOT add explanations or parenthetical notes unless they were in the original.
- Do NOT skip any item; every cn must have an en.
"""


def _translate_part_names(
    cn_names: List[str],
    sumopod_client,
    batch_size: int = 120,
) -> Dict[str, str]:
    """
    Translate a list of unique Chinese part names to English in one or more
    batched API calls.

    Args:
        cn_names:       Deduplicated list of Chinese name strings.
        sumopod_client: Initialised SumopodClient.
        batch_size:     Max items per API call (default 120 keeps well inside
                        4 k token budget per request).

    Returns:
        Dict mapping cn → en.  Falls back to the original cn on any error.
    """
    if not cn_names:
        return {}

    cn_to_en: Dict[str, str] = {}

    # Split into batches so very large catalogs don't hit token limits.
    for i in range(0, len(cn_names), batch_size):
        batch = cn_names[i : i + batch_size]
        user_msg = (
            "Translate these Chinese transmission part names to English:\n\n"
            + json.dumps(batch, ensure_ascii=False, indent=2)
        )
        try:
            resp = sumopod_client.client.chat.completions.create(
                model=sumopod_client.model,
                messages=[
                    {"role": "system", "content": _PARTS_TRANSLATION_PROMPT},
                    {"role": "user",   "content": user_msg},
                ],
                temperature=0.1,
                max_tokens=4096,
                timeout=120,
            )
            raw = extract_response_text(resp)
            if raw.startswith("```"):
                raw = re.sub(r"^```[a-zA-Z]*\n?", "", raw)
                raw = re.sub(r"\n?```$", "", raw)

            translations = json.loads(raw.strip()).get("translations", [])
            for item in translations:
                cn = (item.get("cn") or "").strip()
                en = (item.get("en") or "").strip()
                if cn:
                    cn_to_en[cn] = en

            logger.debug(
                "Translation batch %d–%d: %d items translated.",
                i + 1, i + len(batch), len(translations),
            )

        except Exception as exc:
            logger.warning(
                "Translation batch %d–%d failed (%s) — falling back to CN names.",
                i + 1, i + len(batch), exc,
            )
            # Fallback: use the Chinese name as-is so no data is lost.
            for cn in batch:
                cn_to_en.setdefault(cn, cn)

    # Guarantee every input name has a mapping (in case the model skipped some).
    for cn in cn_names:
        cn_to_en.setdefault(cn, cn)

    return cn_to_en


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────

def extract_transmission_parts(
    pdf_path: str,
    sumopod_client,
    target_id_start: int = 1,
    dpi: int = 150,
    category_map: Optional[Dict[str, str]] = None,
    vision_detail: str = "high",
) -> List[Dict]:
    """
    Extract all parts from a Transmission partbook PDF.

    Implements a two-phase approach:
      1. Parallel Vision AI calls (one per page) render and classify each page.
      2. Sequential pass (in document order) tracks the current category and
         accumulates parts, then deduplicates and assigns T-IDs per group.

    Args:
        pdf_path:         Path to the PDF file.
        sumopod_client:   Initialised SumopodClient with vision capability.
        target_id_start:  T-number index to start from (1 = fresh document;
                          or ``last_DB_T + 1`` when appending to an existing
                          database).
        dpi:              Render resolution for page-to-image conversion.
                          150 DPI is sufficient for clean digital prints.
        category_map:     Dict mapping category_name_cn → category_name_en.
                          Produced by Stage 1.  Used to tag each output group
                          with its English name.  Pass ``None`` to leave EN
                          names empty.
        vision_detail:    "high" (default) or "low" for clean digital prints.

    Returns:
        List of category-group dicts, each shaped as::

            {
                "category_name_cn": "离合器和变速器壳体总成",
                "category_name_en": "Clutch And Transmission Housing Assembly",
                "parts": [
                    {
                        "target_id":            "T000",
                        "part_number":          "BS10JSD160T-1707080",
                        "catalog_item_name_en": "",
                        "catalog_item_name_ch": "倒档中间轴总成",
                        "quantity":             2,
                        "description":          "",
                        "unit":                 "",
                    },
                    ...
                ]
            }
    """
    import fitz  # PyMuPDF — available in the standard container environment

    category_map = category_map or {}

    # ── 1. Open PDF and build a list of (page_index, b64_jpeg) ────────────────
    doc = fitz.open(pdf_path)
    total_pages = len(doc)
    doc.close()
    logger.info("Transmission extractor: %d pages in '%s'", total_pages, pdf_path)

    # ── 2. Parallel Vision AI calls ──────────────────────────────────────────
    #   Each worker renders its page and calls Vision AI.  Results come back
    #   out-of-order; we re-sort by page index before the sequential pass.
    page_results: Dict[int, Optional[Dict]] = {}   # page_index → parsed result

    def _process_page(page_idx: int) -> Tuple[int, Optional[Dict]]:
        """Render page and call Vision AI.  Returns (page_idx, result)."""
        b64 = pdf_page_to_base64(pdf_path, page_idx, dpi=dpi)
        result = _call_vision(b64, sumopod_client, detail=vision_detail)
        return page_idx, result

    logger.info("Launching parallel Vision AI calls (max_workers=5) …")
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(_process_page, idx): idx
            for idx in range(total_pages)
        }
        for future in as_completed(futures):
            idx, result = future.result()
            page_results[idx] = result
            page_type = (result or {}).get("page_type", "error")
            logger.debug("Page %d/%d → %s", idx + 1, total_pages, page_type)

    logger.info("All Vision AI calls complete.")

    # ── Diagnostic: print per-page classification summary ────────────────────
    _type_counts: Dict[str, int] = {}
    for _r in page_results.values():
        _pt = (_r or {}).get("page_type", "error/none")
        _type_counts[_pt] = _type_counts.get(_pt, 0) + 1
    logger.info(
        "Page classification summary: %s",
        ", ".join(f"{k}={v}" for k, v in sorted(_type_counts.items())),
    )
    for _idx in sorted(page_results):
        _r  = page_results[_idx]
        _pt = (_r or {}).get("page_type", "none")
        _hd = (_r or {}).get("section_header") or ""
        _np = len((_r or {}).get("parts") or [])
        logger.info(
            "  pg %03d | %-8s | header=%-45s | parts=%d",
            _idx + 1, _pt, (repr(_hd) if _hd else "-"), _np,
        )

    # ── 3. Sequential pass: stateful category tracking ───────────────────────
    #   We process pages in document order so that a section header on page N
    #   correctly governs all content pages that follow until the next header.

    # Raw accumulator: category_cn → list of raw part dicts
    # Using OrderedDict preserves the order categories were first encountered.
    category_parts: OrderedDict[str, List[Dict]] = OrderedDict()
    current_category_cn: Optional[str] = None

    for page_idx in sorted(page_results.keys()):
        result = page_results[page_idx]

        if result is None:
            logger.warning("Page %d: Vision AI returned no result — skipping.", page_idx + 1)
            continue

        page_type = result.get("page_type", "")

        # Skip non-content pages immediately.
        if page_type in ("cover", "toc"):
            logger.info("Page %d: %s — skipped.", page_idx + 1, page_type)
            continue

        if page_type != "content":
            logger.warning(
                "Page %d: unexpected page_type=%r — skipping.", page_idx + 1, page_type
            )
            continue

        # ── 3a. Validate section header (but don't apply it yet) ─────────────
        raw_header = result.get("section_header")
        if raw_header:
            if not _is_valid_section_header(raw_header):
                logger.warning(
                    "Page %d: section_header '%s' rejected — "
                    "missing 、 mark, likely a misread bold table row.",
                    page_idx + 1, raw_header
                )
                raw_header = None

        # Guard: if we've never seen a section header yet AND there's no new
        # header on this page, we can't assign parts to any category.
        if current_category_cn is None and not raw_header:
            parts_on_page = result.get("parts") or []
            if parts_on_page:
                logger.warning(
                    "Page %d: %d parts found but no category established yet — "
                    "skipping these parts.",
                    page_idx + 1,
                    len(parts_on_page),
                )
            continue

        # ── 3b. Accumulate parts FIRST under the CURRENT category ────────────
        # Parts on this page belong to the section that was active BEFORE
        # this page's header — e.g. page has section 三's table at the top
        # and section 四's header at the bottom: parts go to 三, not 四.
        parts_on_page: List[Dict] = result.get("parts") or []
        if current_category_cn is not None and parts_on_page:
            category_parts[current_category_cn].extend(parts_on_page)
            logger.info(
                "Page %d: +%d parts → '%s' (total %d)",
                page_idx + 1,
                len(parts_on_page),
                current_category_cn,
                len(category_parts[current_category_cn]),
            )

        # ── 3c. NOW update section pointer for the NEXT page's parts ─────────
        if raw_header:
            current_category_cn = _strip_section_number(raw_header)
            if current_category_cn not in category_parts:
                category_parts[current_category_cn] = []
            logger.info(
                "Page %d: new section → '%s'", page_idx + 1, current_category_cn
            )

    logger.info(
        "Sequential pass complete: %d categories found: %s",
        len(category_parts),
        list(category_parts.keys()),
    )

    # ── 4. Deduplicate, assign T-IDs, and build final output ─────────────────
    output: List[Dict] = []
    global_counter = target_id_start   # sequential T-ID counter across the whole doc

    # Collect all tagged parts across every category first, so we can run
    # a single batch translation call for all unique CN names at once.
    # This is cheaper (1 API call total) than translating per-category.
    staged: List[Tuple[str, str, List[Dict]]] = []   # (category_cn, category_en, tagged_parts)

    for category_cn, raw_parts in category_parts.items():
        if not raw_parts:
            # Keep 0-part categories in staged so they appear in the output
            # (avoids the "missing category" symptom when Vision AI extracts
            # the header but no rows — e.g. diagram-heavy pages).
            logger.warning(
                "Category '%s' has 0 parts — included with empty parts list.", category_cn
            )

        # Step A: deduplicate within this category.
        merged = _merge_parts(raw_parts) if raw_parts else []

        # Step B: assign T-IDs (per-category reset after the first group).
        tagged, _ = _assign_target_ids(merged, counter_start=global_counter)
        global_counter = 1   # reset for next category

        # Step D: look up the English category name from Stage 1 category_map.
        # Try exact match first, then normalised (strip & lower) as fallback.
        category_en = category_map.get(category_cn, "")
        if not category_en:
            # Normalised fallback — handles minor whitespace / encoding drift
            # between what Stage 1 stored and what Vision AI returned.
            _norm = category_cn.strip()
            for k, v in category_map.items():
                if k.strip() == _norm:
                    category_en = v
                    break
        if not category_en:
            logger.warning(
                "No EN mapping in category_map for '%s' — will auto-translate.", category_cn
            )

        staged.append((category_cn, category_en, tagged))

    # ── 5. Batch-translate all unique CN names in one API call ────────────────
    #   Two groups of CN strings to translate:
    #     (a) Part names  — fills catalog_item_name_en on every part row.
    #     (b) Category names with no EN mapping from Stage 1 — fills
    #         category_name_en / subtype_name_en so the frontend shows English.
    all_part_cn_names: List[str] = list({
        part.get("name_cn") or ""
        for _, _, tagged in staged
        for part in tagged
        if part.get("name_cn")
    })
    missing_cat_cn: List[str] = [
        cat_cn for cat_cn, cat_en, _ in staged if not cat_en
    ]

    strings_to_translate = list(dict.fromkeys(all_part_cn_names + missing_cat_cn))
    logger.info(
        "Translating %d unique strings (%d part names + %d category names without EN mapping) …",
        len(strings_to_translate), len(all_part_cn_names), len(missing_cat_cn),
    )
    cn_to_en: Dict[str, str] = _translate_part_names(strings_to_translate, sumopod_client)
    logger.info("Translation complete.")

    # Back-fill category_en for any that were missing from Stage 1 map.
    staged = [
        (cat_cn, cat_en or cn_to_en.get(cat_cn, cat_cn), tagged)
        for cat_cn, cat_en, tagged in staged
    ]

    # ── 6. Build final output using the translation lookup ────────────────────
    for category_cn, category_en, tagged in staged:
        # Step C: map to output schema (add BS prefix, fill EN name, rename fields).
        output_parts = [_build_output_part(p, cn_to_en=cn_to_en) for p in tagged]

        output.append(
            {
                # ── Primary keys (transmission-native) ──────────────────
                "category_name_cn": category_cn,
                "category_name_en": category_en,
                # ── Alias keys (cabin_chassis-compatible) ───────────────
                # The frontend (populatePartsSection) and batch_submit_parts()
                # both read subtype_name_en / subtype_name_cn / subtype_code.
                # Transmission has no subtype level, so we alias the category
                # fields into those keys so every downstream consumer works
                # without a schema branch.
                "subtype_name_en":  category_en,
                "subtype_name_cn":  category_cn,
                "subtype_code":     "",   # no code in transmission partbooks
                "parts": output_parts,
            }
        )

    logger.info(
        "Transmission parts extraction complete: %d categories, %d total parts.",
        len(output),
        sum(len(g["parts"]) for g in output),
    )
    return output