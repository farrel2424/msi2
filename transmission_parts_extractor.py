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
  A large bold centered title in the format:
    <number>、<Chinese section name>
  Examples: "一、离合器和变速器壳体总成", "十五、QH50 取力器总成"
  If present, extract it verbatim (include the leading number and 、).
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


def _build_output_part(part: Dict) -> Dict:
    """
    Map an internal part dict (with target_id) to the canonical EPC output
    schema.

    Input keys  : target_id, part_number, name_cn, quantity, serial_no
    Output keys : target_id, part_number, catalog_item_name_en,
                  catalog_item_name_ch, quantity, description, unit
    """
    return {
        "target_id": part["target_id"],
        "part_number": _add_prefix(part["part_number"]),
        "catalog_item_name_en": "",          # no EN name in this partbook
        "catalog_item_name_ch": part.get("name_cn") or "",
        "quantity": part.get("quantity"),
        "description": "",                   # no remarks column
        "unit": "",                          # no unit column
    }


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
            logger.debug("Page %d: %s — skipped.", page_idx + 1, page_type)
            continue

        if page_type != "content":
            logger.warning(
                "Page %d: unexpected page_type=%r — skipping.", page_idx + 1, page_type
            )
            continue

        # ── 3a. Update current category if a new section header is present ──
        raw_header = result.get("section_header")
        if raw_header:
            current_category_cn = _strip_section_number(raw_header)
            # Ensure the category bucket exists even if this page has no parts.
            if current_category_cn not in category_parts:
                category_parts[current_category_cn] = []
            logger.debug(
                "Page %d: new section → '%s'", page_idx + 1, current_category_cn
            )

        # Guard: if we've never seen a section header yet, we can't assign
        # parts to any category.  Log and skip.
        if current_category_cn is None:
            parts_on_page = result.get("parts") or []
            if parts_on_page:
                logger.warning(
                    "Page %d: %d parts found but no category established yet — "
                    "skipping these parts.",
                    page_idx + 1,
                    len(parts_on_page),
                )
            continue

        # ── 3b. Accumulate parts into the current category bucket ────────────
        parts_on_page: List[Dict] = result.get("parts") or []
        category_parts[current_category_cn].extend(parts_on_page)
        logger.debug(
            "Page %d: accumulated %d parts into '%s'.",
            page_idx + 1,
            len(parts_on_page),
            current_category_cn,
        )

    logger.info(
        "Sequential pass complete: %d category/categories found.",
        len(category_parts),
    )

    # ── 4. Deduplicate, assign T-IDs, and build final output ─────────────────
    output: List[Dict] = []
    global_counter = target_id_start   # sequential T-ID counter across the whole doc

    for category_cn, raw_parts in category_parts.items():
        if not raw_parts:
            logger.debug("Category '%s' has 0 parts — skipping.", category_cn)
            continue

        # Step A: deduplicate within this category.
        merged = _merge_parts(raw_parts)

        # Step B: assign T-IDs.
        #   Note: counter resets to target_id_start *per category*, not
        #   globally, because the spec says "sequential and reset per category."
        #   We still accept target_id_start as the starting point for the
        #   first category (append mode), then reset to 1 for subsequent ones.
        tagged, _ = _assign_target_ids(merged, counter_start=global_counter)
        # Per-category reset: after the first category, sequential IDs restart
        # at 1 (T001) for every new category.
        global_counter = 1   # reset for next category

        # Step C: map to output schema (add BS prefix, rename fields).
        output_parts = [_build_output_part(p) for p in tagged]

        # Step D: look up the English name from Stage 1 category_map.
        category_en = category_map.get(category_cn, "")
        if not category_en:
            logger.debug(
                "No EN translation found in category_map for '%s'.", category_cn
            )

        output.append(
            {
                "category_name_cn": category_cn,
                "category_name_en": category_en,
                "parts": output_parts,
            }
        )

    logger.info(
        "Transmission parts extraction complete: %d categories, %d total parts.",
        len(output),
        sum(len(g["parts"]) for g in output),
    )
    return output