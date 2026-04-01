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

Page layout patterns
---------------------
  Pattern A — header at top of page, parts below:
      [Section header 四、一轴总成]
      [diagram]
      [parts table for section 四]

  Pattern B — parts at top (tail of previous section), header below:
      [parts table, finishing section 三]
      [Section header 四、一轴总成]
      [diagram, start of section 四]

  The Vision AI distinguishes these by returning two separate lists:
    • parts_before_header  — rows from the table ABOVE the section header
                             (still belong to the PREVIOUS section)
    • parts                — rows from the table BELOW / with the header
                             (belong to the CURRENT / new section)

T-ID assignment rules
---------------------
  1. First bold row (assembly total, is_assembly_header=True) → T000
  2. All other rows → sequential T001, T002, T003 …
  3. Exception — serial_no (代号) contains "/" (shared catalog entry):
       serial_no "9/10"  →  target_id "T009/T010"
       serial_no "8/9"   →  target_id "T008/T009"
     The numbers from serial_no are used directly (T-prefixed, zero-padded).
     The sequential counter advances past the highest number.
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
    A real section header must contain the Chinese enumeration mark 、(U+3001).
    e.g. "一、离合器和变速器壳体总成"
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

══════════════════════════════════════════════════════════════════
SECTION HEADER — how to identify it
══════════════════════════════════════════════════════════════════
A section header is a LARGE, BOLD, CENTERED title printed OUTSIDE
the table, in the format:
    <ordinal><、><Chinese section name>
Examples: "一、离合器和变速器壳体总成"  "三、左、右中间轴总成"
          "四、一轴总成"               "十五、QH50 取力器总成"

CRITICAL: The 、character (U+3001, distinct from comma ,) MUST appear.
Bold rows that are INSIDE the table (assembly total rows that have a
part number) are NOT section headers. If no section header is visible
on this page, set section_header to null.

══════════════════════════════════════════════════════════════════
TABLE STRUCTURE (4 columns, left → right)
══════════════════════════════════════════════════════════════════
  代号 | 零件号 | 零件名称. | 数量

EXTRACTION RULES:
- Extract ONLY rows where 零件号 (part number) is non-empty.
- 代号 (serial_no): string; may be "8/9", "9/10", or blank → null.
- 零件号 (part_number): verbatim, trim spaces.
- 零件名称. (name_cn): full Chinese text including notes in parentheses.
- 数量 (quantity): integer; "按需" or illegible → null.
- ASSEMBLY HEADER ROW: the FIRST row of each table section is printed
  in BOLD and has NO 代号 (serial number). It is the assembly total.
  Mark it "is_assembly_header": true.
  Every other row: "is_assembly_header": false.
- Do NOT invent data. Blank cell → null or "".
- Return ONLY valid JSON — no markdown fences, no explanation.

══════════════════════════════════════════════════════════════════
BOUNDARY PAGE RULE  ← READ THIS CAREFULLY
══════════════════════════════════════════════════════════════════
One page often contains rows from TWO different sections:

  ABOVE the section header  →  still belong to the PREVIOUS section
  BELOW the section header  →  belong to the NEW section

You MUST split them into two separate arrays.

CONCRETE EXAMPLE (this exact layout appears in this catalog):

  ┌──────┬─────────────────┬──────────────────────────┬──────┐
  │ 代号 │      零件号      │        零件名称.          │ 数量 │
  ├──────┼─────────────────┼──────────────────────────┼──────┤
  │      │ 10JS160-1701047 │   **中间轴总成**          │  2   │  ← BOLD, is_assembly_header=true
  │  1   │ Q151B1645M      │ 预涂胶六角头螺栓…         │  2   │
  │  2   │ 19668           │ 中间轴轴承档板            │  2   │
  │ ...  │ ...             │ ...                      │ ...  │
  │  16  │ Q43145          │ 止动环                   │  2   │
  └──────┴─────────────────┴──────────────────────────┴──────┘
  ↑ ALL ROWS ABOVE THIS LINE → parts_before_header (previous section)

              四、一轴总成          ← section_header (new section)

  [exploded-view diagram — no table rows follow on this page]
  ↑ ROWS BELOW THIS LINE → parts (new section, empty here)

Correct output for this page:
{
  "page_type": "content",
  "section_header": "四、一轴总成",
  "parts_before_header": [
    {"serial_no": null, "part_number": "10JS160-1701047", "name_cn": "中间轴总成",          "quantity": 2, "is_assembly_header": true},
    {"serial_no": "1",  "part_number": "Q151B1645M",      "name_cn": "预涂胶六角头螺栓和弹簧垫圈组合件", "quantity": 2, "is_assembly_header": false},
    {"serial_no": "2",  "part_number": "19668",           "name_cn": "中间轴轴承档板",       "quantity": 2, "is_assembly_header": false},
    ... (all 16 numbered rows) ...
    {"serial_no": "16", "part_number": "Q43145",          "name_cn": "止动环",              "quantity": 2, "is_assembly_header": false}
  ],
  "parts": []
}

WRONG — do NOT do this:
  parts_before_header: []       ← leaving it empty is INCORRECT
  parts: [all 16 rows]          ← rows above the header must NOT go here

RULE SUMMARY:
  parts_before_header = every row whose table position is ABOVE the
                        section_header on this page.
                        ALWAYS [] when section_header is null.
  parts               = every row whose table position is BELOW the
                        section_header, OR all rows when no header exists.

══════════════════════════════════════════════════════════════════
OUTPUT FORMAT (content page)
══════════════════════════════════════════════════════════════════
{
  "page_type": "content",
  "section_header": "<title containing 、, or null>",
  "parts_before_header": [
    {
      "serial_no": "<string or null>",
      "part_number": "<零件号>",
      "name_cn": "<零件名称>",
      "quantity": <integer or null>,
      "is_assembly_header": <true or false>
    }
  ],
  "parts": [
    {
      "serial_no": "<string or null>",
      "part_number": "<零件号>",
      "name_cn": "<零件名称>",
      "quantity": <integer or null>,
      "is_assembly_header": <true or false>
    }
  ]
}

OUTPUT FORMAT (cover or toc):
{"page_type": "cover"}
{"page_type": "toc"}
"""

# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _call_vision(b64: str, sumopod_client, detail: str = "high",
                 system_prompt: Optional[str] = None) -> Optional[Dict]:
    """Send one PDF page (as base64 JPEG) to Vision AI and parse the result."""
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
                            "image_url": {"url": f"data:image/jpeg;base64,{b64}",
                                          "detail": detail},
                        },
                        {"type": "text",
                         "text": "Classify this page and extract all parts data. "
                                 "Follow the system prompt exactly."},
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
        logger.warning("Vision AI non-JSON (truncated): %s … (%s)", raw[:300], exc)
        return None
    except Exception as exc:
        logger.warning("Vision AI call failed: %s", exc)
        return None


def _add_prefix(part_number: str) -> str:
    """Prepend BS prefix to part_number. Idempotent."""
    if not part_number:
        return part_number
    if part_number.startswith(_PART_NUMBER_PREFIX):
        return part_number
    return f"{_PART_NUMBER_PREFIX}{part_number}"


def _strip_section_number(section_header: str) -> str:
    """
    Remove the leading ordinal and 、 marker from a section header.
    "一、离合器和变速器壳体总成" → "离合器和变速器壳体总成"
    """
    return re.sub(r"^[^\u3001]+\u3001", "", section_header).strip()


def _merge_parts(raw_parts: List[Dict]) -> List[Dict]:
    """
    Deduplicate parts within one category group.
    Key = (part_number, name_cn).  Duplicate keys → sum quantities.
    Rows with empty part_number are skipped.
    """
    merged: OrderedDict[Tuple[str, str], Dict] = OrderedDict()
    for part in raw_parts:
        pn = (part.get("part_number") or "").strip()
        if not pn:
            continue
        name = (part.get("name_cn") or "").strip()
        key  = (pn, name)
        qty  = part.get("quantity")
        if key not in merged:
            merged[key] = {
                "serial_no":          part.get("serial_no"),
                "part_number":        pn,
                "name_cn":            name,
                "quantity":           qty,
                "is_assembly_header": bool(part.get("is_assembly_header", False)),
            }
        else:
            eq = merged[key]["quantity"]
            if eq is not None and qty is not None:
                merged[key]["quantity"] = eq + qty
    return list(merged.values())


def _assign_target_ids(
    parts: List[Dict],
    counter_start: int = 1,
) -> Tuple[List[Dict], int]:
    """
    Assign T-IDs to deduplicated parts.

    Rules
    -----
    1. First bold assembly-header row (is_assembly_header=True) → T000.
    2. All other rows → sequential: T001, T002, T003 …
    3. When serial_no contains "/" (shared catalog entry, e.g. "9/10"):
         → target_id = "T009/T010"  (numbers taken directly from serial_no)
         → sequential counter advances past the highest number used.

    Args:
        parts:         Deduplicated part dicts for ONE category group.
        counter_start: Starting integer for the sequential counter (default 1).

    Returns:
        (tagged_parts, next_counter_value)
    """
    tagged: List[Dict] = []
    counter = counter_start
    assembly_header_assigned = False

    for part in parts:
        result     = {k: v for k, v in part.items() if k != "is_assembly_header"}
        serial_raw = (part.get("serial_no") or "").strip()

        # Rule 1: assembly header → T000
        if part.get("is_assembly_header") and not assembly_header_assigned:
            result["target_id"]       = "T000"
            assembly_header_assigned  = True
            tagged.append(result)
            continue

        # Rule 3: slash serial_no → derive T-IDs from the numbers
        if "/" in serial_raw:
            segments = [s.strip() for s in serial_raw.split("/")]
            try:
                nums = [int(s) for s in segments if s]
                if nums:
                    result["target_id"] = "/".join(f"T{n:03d}" for n in nums)
                    counter = max(nums) + 1
                    tagged.append(result)
                    continue
            except ValueError:
                pass  # fall through to sequential

        # Rule 2: sequential
        result["target_id"] = f"T{counter:03d}"
        counter += 1
        tagged.append(result)

    return tagged, counter


def _build_output_part(part: Dict, cn_to_en: Optional[Dict[str, str]] = None) -> Dict:
    """Map internal part dict to canonical EPC output schema."""
    name_cn = part.get("name_cn") or ""
    name_en = (cn_to_en or {}).get(name_cn, "")
    return {
        "target_id":            part["target_id"],
        "part_number":          _add_prefix(part["part_number"]),
        "catalog_item_name_en": name_en,
        "catalog_item_name_ch": name_cn,
        "quantity":             part.get("quantity"),
        "description":          "",
        "unit":                 "",
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
- Same order and count as input. Do NOT skip any item.
- Use standard automotive / mechanical terminology.
- Keep abbreviations where widely accepted (e.g. "O-Ring", "Bolt M10×1.25").
"""


def _translate_part_names(
    cn_names: List[str],
    sumopod_client,
    batch_size: int = 120,
) -> Dict[str, str]:
    """Translate unique CN part names to English in batched API calls."""
    if not cn_names:
        return {}
    cn_to_en: Dict[str, str] = {}
    for i in range(0, len(cn_names), batch_size):
        batch = cn_names[i : i + batch_size]
        user_msg = ("Translate these Chinese transmission part names to English:\n\n"
                    + json.dumps(batch, ensure_ascii=False, indent=2))
        try:
            resp = sumopod_client.client.chat.completions.create(
                model=sumopod_client.model,
                messages=[
                    {"role": "system", "content": _PARTS_TRANSLATION_PROMPT},
                    {"role": "user",   "content": user_msg},
                ],
                temperature=0.1, max_tokens=4096, timeout=120,
            )
            raw = extract_response_text(resp)
            if raw.startswith("```"):
                raw = re.sub(r"^```[a-zA-Z]*\n?", "", raw)
                raw = re.sub(r"\n?```$", "", raw)
            for item in json.loads(raw.strip()).get("translations", []):
                cn = (item.get("cn") or "").strip()
                en = (item.get("en") or "").strip()
                if cn:
                    cn_to_en[cn] = en
            logger.debug("Translation batch %d–%d: done.", i + 1, i + len(batch))
        except Exception as exc:
            logger.warning("Translation batch %d–%d failed (%s) — using CN names.", i + 1, i + len(batch), exc)
            for cn in batch:
                cn_to_en.setdefault(cn, cn)
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
    custom_prompt: Optional[str] = None,
) -> List[Dict]:
    """
    Extract all parts from a Transmission partbook PDF.

    Two-phase approach
    ------------------
    Phase 1 — Parallel Vision AI: each page returns structured JSON with
    ``parts_before_header`` (rows belonging to the PREVIOUS section, above
    the new header) and ``parts`` (rows for the CURRENT section).

    Phase 2 — Sequential pass: processes pages in document order using a
    three-step rule per page:

      Step A  add parts_before_header → PREVIOUS (still-active) section
      Step B  if section header found, update pointer to NEW section
      Step C  add parts → CURRENT section (may have just changed in B)

    This correctly handles ALL layout patterns without ambiguity.
    """
    import fitz  # PyMuPDF

    category_map = category_map or {}

    doc = fitz.open(pdf_path)
    total_pages = len(doc)
    doc.close()
    logger.info("Transmission extractor: %d pages in '%s'", total_pages, pdf_path)

    # ── Phase 1: parallel Vision AI calls ────────────────────────────────────
    page_results: Dict[int, Optional[Dict]] = {}

    def _process_page(page_idx: int) -> Tuple[int, Optional[Dict]]:
        b64 = pdf_page_to_base64(pdf_path, page_idx, dpi=dpi)
        result = _call_vision(b64, sumopod_client, detail=vision_detail,
                              system_prompt=custom_prompt)
        return page_idx, result

    logger.info("Launching parallel Vision AI calls (max_workers=5) …")
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(_process_page, idx): idx for idx in range(total_pages)}
        for future in as_completed(futures):
            idx, result = future.result()
            page_results[idx] = result
            logger.debug("Page %d/%d → %s", idx + 1, total_pages,
                         (result or {}).get("page_type", "error"))

    logger.info("All Vision AI calls complete.")

    # Diagnostic log
    _tc: Dict[str, int] = {}
    for _r in page_results.values():
        _pt = (_r or {}).get("page_type", "error/none")
        _tc[_pt] = _tc.get(_pt, 0) + 1
    logger.info("Page classification: %s",
                ", ".join(f"{k}={v}" for k, v in sorted(_tc.items())))
    for _idx in sorted(page_results):
        _r   = page_results[_idx]
        _pt  = (_r or {}).get("page_type", "none")
        _hd  = (_r or {}).get("section_header") or ""
        _nbh = len((_r or {}).get("parts_before_header") or [])
        _np  = len((_r or {}).get("parts") or [])
        logger.info("  pg %03d | %-8s | header=%-40s | before_hdr=%d | parts=%d",
                    _idx + 1, _pt, (repr(_hd) if _hd else "-"), _nbh, _np)

    # ── Phase 2: sequential pass ──────────────────────────────────────────────
    category_parts: OrderedDict[str, List[Dict]] = OrderedDict()
    current_category_cn: Optional[str] = None

    for page_idx in sorted(page_results.keys()):
        result = page_results[page_idx]
        if result is None:
            logger.warning("Page %d: no result — skipping.", page_idx + 1)
            continue

        page_type = result.get("page_type", "")
        if page_type in ("cover", "toc"):
            logger.info("Page %d: %s — skipped.", page_idx + 1, page_type)
            continue
        if page_type != "content":
            logger.warning("Page %d: unexpected page_type=%r — skipping.",
                           page_idx + 1, page_type)
            continue

        # Validate header
        raw_header = result.get("section_header")
        if raw_header and not _is_valid_section_header(raw_header):
            logger.warning("Page %d: header '%s' rejected (missing 、).",
                           page_idx + 1, raw_header)
            raw_header = None

        parts_before = result.get("parts_before_header") or []
        parts_after  = result.get("parts") or []

        # Step A: parts_before_header → belongs to PREVIOUS section
        if parts_before:
            if current_category_cn is not None:
                category_parts[current_category_cn].extend(parts_before)
                logger.info(
                    "Page %d: +%d parts (before header) → '%s' (total %d)",
                    page_idx + 1, len(parts_before), current_category_cn,
                    len(category_parts[current_category_cn]),
                )
            else:
                logger.warning(
                    "Page %d: %d parts_before_header but no section yet — skipping.",
                    page_idx + 1, len(parts_before),
                )

        # Step B: update section pointer
        if raw_header:
            current_category_cn = _strip_section_number(raw_header)
            if current_category_cn not in category_parts:
                category_parts[current_category_cn] = []
            logger.info("Page %d: new section → '%s'", page_idx + 1, current_category_cn)

        # Guard: no section established yet
        if current_category_cn is None:
            if parts_after:
                logger.warning(
                    "Page %d: %d parts but no section established — skipping.",
                    page_idx + 1, len(parts_after),
                )
            continue

        # Step C: parts → CURRENT section
        if parts_after:
            category_parts[current_category_cn].extend(parts_after)
            logger.info(
                "Page %d: +%d parts → '%s' (total %d)",
                page_idx + 1, len(parts_after), current_category_cn,
                len(category_parts[current_category_cn]),
            )

    logger.info("Sequential pass complete: %d categories: %s",
                len(category_parts), list(category_parts.keys()))

    # ── Build staged list (dedup + T-IDs) ────────────────────────────────────
    staged: List[Tuple[str, str, List[Dict]]] = []
    global_counter = target_id_start

    for category_cn, raw_parts in category_parts.items():
        if not raw_parts:
            logger.warning("Category '%s' has 0 parts — kept with empty list.", category_cn)
        merged = _merge_parts(raw_parts) if raw_parts else []
        tagged, _ = _assign_target_ids(merged, counter_start=global_counter)
        global_counter = 1  # T-IDs restart from T001 for each category

        category_en = category_map.get(category_cn, "")
        if not category_en:
            for k, v in category_map.items():
                if k.strip() == category_cn.strip():
                    category_en = v
                    break
        if not category_en:
            logger.warning("No EN mapping for '%s' — will auto-translate.", category_cn)

        staged.append((category_cn, category_en, tagged))

    # ── Batch translate all unique CN strings ─────────────────────────────────
    all_part_cn = list({
        p.get("name_cn") or ""
        for _, _, tg in staged for p in tg if p.get("name_cn")
    })
    missing_cat_cn = [cn for cn, en, _ in staged if not en]
    to_translate = list(dict.fromkeys(all_part_cn + missing_cat_cn))
    logger.info("Translating %d unique strings …", len(to_translate))
    cn_to_en = _translate_part_names(to_translate, sumopod_client)
    logger.info("Translation complete.")

    staged = [(cn, en or cn_to_en.get(cn, cn), tg) for cn, en, tg in staged]

    # ── Build final output ────────────────────────────────────────────────────
    output: List[Dict] = []
    for category_cn, category_en, tagged in staged:
        output.append({
            "category_name_cn": category_cn,
            "category_name_en": category_en,
            "subtype_name_en":  category_en,
            "subtype_name_cn":  category_cn,
            "subtype_code":     "",
            "parts":            [_build_output_part(p, cn_to_en) for p in tagged],
        })

    logger.info("Done: %d categories, %d total parts.",
                len(output), sum(len(g["parts"]) for g in output))
    return output