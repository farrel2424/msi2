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

PATCH (2026-04-09):
  _is_valid_section_header() now also accepts English-only section titles.
  Some bilingual PDFs have the AI return English section headers (no 、),
  which previously caused ALL sections to be skipped and produced 0 parts.
  Chinese headers still require 、 to prevent misreading bold part rows.

PATCH (2026-04-09b):
  Added _normalize_part() to canonicalize field names returned by Vision AI.
  Vision AI may return Chinese field names (零件号, 零件名称, 数量, 代号) or
  English variants (Part No., part_no, qty, No.) instead of the canonical
  names (part_number, name_cn, quantity, serial_no) expected by _merge_parts.
  Without normalization, all parts were silently dropped → 0 total parts.
  Fix is purely additive: only _normalize_part() is new; no existing logic changed.
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
    Validate a section header string returned by Vision AI.

    Rules
    -----
    1. Chinese headers MUST contain 、 (U+3001) — the ordinal separator used
       in Chinese-language transmission manuals (e.g. "一、离合器和变速器壳体总成").
       This guard prevents misreading bold assembly-total rows inside the
       parts table as new section headers.

    2. English-only (or alphanumeric+English) headers are accepted as-is.
       Bilingual PDFs sometimes have the AI extract the English section title
       directly (e.g. "Clutch and transmission housing assembly") without 、.
       Since bold part rows inside the table are never pure English prose,
       accepting English headers is safe.

    3. Empty strings are always rejected.

    4. Bare part-number tokens (e.g. "BS123456") are rejected even if
       they contain no Chinese characters.
    """
    if not raw or not raw.strip():
        return False

    # Rule 1 — Chinese header: must have 、
    if re.search(r"[\u4e00-\u9fff]", raw):
        return "\u3001" in raw

    # Rule 4 — reject bare part-number patterns (e.g. "BS12345678")
    if re.match(r"^[A-Z]{0,2}\d{5,}", raw.strip().upper()):
        return False

    # Rule 2 — English-only header: accept
    return bool(re.search(r"[A-Za-z]", raw))


# ─────────────────────────────────────────────────────────────────────────────
# Field-name normalizer (NEW — PATCH 2026-04-09b)
# ─────────────────────────────────────────────────────────────────────────────

# Canonical name → list of aliases the Vision AI might return
_FIELD_ALIASES: Dict[str, List[str]] = {
    "part_number": [
        "part_number", "零件号", "part_no", "Part No.", "partno",
        "PART NO", "part no", "件号", "零件编号",
    ],
    "name_cn": [
        "name_cn", "零件名称", "名称", "零件名", "零件名称.",
    ],
    "name_en": [
        "name_en", "Part Name", "NAME", "English Name", "english_name",
    ],
    # NOTE: "name", "part_name", "description" intentionally excluded —
    # they are ambiguous; Chinese text often comes back in these fields.
    # _normalize_part() handles language detection as a post-normalization step.
    "quantity": [
        "quantity", "数量", "qty", "Qty", "QTY", "Qty.",
        "数量.", "count",
    ],
    "serial_no": [
        "serial_no", "代号", "No.", "no", "序号", "item",
        "serial", "item_no", "No",
    ],
    "is_assembly_header": [
        "is_assembly_header", "assembly_header", "is_bold",
    ],
}

# Reverse map: alias → canonical name (built once at import time)
_ALIAS_TO_CANONICAL: Dict[str, str] = {}
for _canonical, _aliases in _FIELD_ALIASES.items():
    for _alias in _aliases:
        _ALIAS_TO_CANONICAL[_alias] = _canonical
        _ALIAS_TO_CANONICAL[_alias.lower()] = _canonical


_CN_CHAR_RE = re.compile(r"[一-鿿]")

# Ordinal abbreviations to expand (e.g. "2nd" → "Second")
_ORDINAL_MAP = {
    "1st": "First",  "2nd": "Second",  "3rd": "Third",   "4th": "Fourth",
    "5th": "Fifth",  "6th": "Sixth",   "7th": "Seventh", "8th": "Eighth",
    "9th": "Ninth",  "10th": "Tenth",
}
# Articles/prepositions kept lowercase in Title Case (except position 0)
_KEEP_LOWER = {"and", "or", "the", "a", "an", "of", "in", "on", "for", "with", "to"}


def _fix_ordinals(text: str) -> str:
    """Replace abbreviated ordinals: '2nd' → 'Second'."""
    for abbr, word in _ORDINAL_MAP.items():
        text = re.sub(r"\b" + re.escape(abbr) + r"\b", word, text, flags=re.IGNORECASE)
    return text


def _title_case_automotive(text: str) -> str:
    """
    Title-case for automotive part/assembly names.
    Articles and prepositions stay lowercase unless they are the first word.
    Ordinal abbreviations are expanded first.
    """
    if not text:
        return text
    text = _fix_ordinals(text)
    words = text.split()
    result = []
    for i, w in enumerate(words):
        if i == 0 or w.lower() not in _KEEP_LOWER:
            result.append(w[0].upper() + w[1:] if w else w)
        else:
            result.append(w.lower())
    return " ".join(result)


def _normalize_part(raw: Dict) -> Dict:
    """
    Normalize a raw part dict from Vision AI to canonical field names.

    Vision AI may return any combination of Chinese/English field names.
    This function maps them all to the canonical names that the rest of
    the pipeline (``_merge_parts``, ``_assign_target_ids``,
    ``_build_output_part``) expects:

        part_number, name_cn, name_en, quantity, serial_no, is_assembly_header

    Unknown fields are preserved as-is so no data is lost.

    Post-normalization language detection:
    Ambiguous field aliases like "name", "part_name", "description" are first
    stored under their best-guess canonical name.  After all fields are
    mapped, the function detects if ``name_en`` actually contains Chinese
    characters and moves that value to ``name_cn`` (and vice-versa), so that
    catalog_item_name_en / catalog_item_name_ch are always correct regardless
    of which column the Vision AI chose to populate.
    """
    # Step 1: map field names using alias table + ambiguous fallbacks
    normalized: Dict = {}
    for key, value in raw.items():
        canonical = _ALIAS_TO_CANONICAL.get(key) or _ALIAS_TO_CANONICAL.get(key.lower())
        if canonical is None:
            # Ambiguous aliases not in the alias table: "name", "part_name", "description"
            # Defer language detection to Step 2 below
            if key.lower() in ("name", "part_name", "description", "零件名称."):
                canonical = "__ambiguous_name__"
        out_key = canonical if canonical else key
        # For quantity: coerce to int when possible
        if out_key == "quantity":
            if isinstance(value, str):
                value = value.strip()
                try:
                    value = int(value)
                except (ValueError, TypeError):
                    value = None
        normalized[out_key] = value

    # Step 2: resolve ambiguous name field by language detection
    ambiguous = normalized.pop("__ambiguous_name__", None)
    if ambiguous and isinstance(ambiguous, str):
        ambiguous = ambiguous.strip()
        if ambiguous:
            if _CN_CHAR_RE.search(ambiguous):
                # Value is Chinese — goes to name_cn (unless already set)
                if not normalized.get("name_cn"):
                    normalized["name_cn"] = ambiguous
            else:
                # Value is Latin/English — goes to name_en (unless already set)
                if not normalized.get("name_en"):
                    normalized["name_en"] = ambiguous

    # Step 3: cross-check name_en / name_cn for misrouted values
    # If name_en contains Chinese text, swap it to name_cn
    name_en_val = normalized.get("name_en") or ""
    name_cn_val = normalized.get("name_cn") or ""
    if name_en_val and _CN_CHAR_RE.search(name_en_val):
        if not name_cn_val:
            normalized["name_cn"] = name_en_val
        normalized["name_en"] = ""
    # If name_cn contains only Latin text and no Chinese, swap to name_en
    if name_cn_val and not _CN_CHAR_RE.search(name_cn_val):
        if not name_en_val:
            normalized["name_en"] = name_cn_val
        normalized["name_cn"] = ""

    return normalized


# ─────────────────────────────────────────────────────────────────────────────
# Vision AI system prompt
# ─────────────────────────────────────────────────────────────────────────────

_PARTS_SYSTEM_PROMPT = """\
You are a precise data-extraction engine for a BILINGUAL (Chinese + English)
automotive transmission parts catalog.

PAGE TYPES:
1. COVER page — shows document title only.
   Return {"page_type": "cover"}.
2. TOC page — shows a table of contents.
   Return {"page_type": "toc"}.
3. CONTENT page — contains a parts table and optionally a section title
   header and/or an exploded-view diagram.

SECTION HEADER — how to identify it:
A section header is a LARGE, BOLD, CENTERED title printed OUTSIDE the table.
May appear in Chinese only, English only, or both.
CRITICAL: For Chinese-style headers the 、character (U+3001) MUST appear.
Bold rows INSIDE the table are NOT section headers — return section_header: null for those pages.
If no section header exists, set section_header to null.

TABLE STRUCTURE (4 or 5 columns):
  代号 | 零件号 | 零件名称. | 数量
  or: No. | Part No. | Part Name | 零件名称 | Qty

Extract ONLY rows where Part No. / 零件号 is non-empty.
Return ONLY valid JSON — no markdown fences.

OUTPUT FORMAT (content page):
{
  "page_type": "content",
  "section_header": "<title containing 、, or English title, or null>",
  "parts_before_header": [
    {
      "serial_no": "<代号 or null>",
      "part_number": "<零件号>",
      "name_cn": "<零件名称>",
      "quantity": <integer or null>,
      "is_assembly_header": false
    }
  ],
  "parts": [
    {
      "serial_no": "<代号 or null>",
      "part_number": "<零件号>",
      "name_cn": "<零件名称>",
      "quantity": <integer or null>,
      "is_assembly_header": false
    }
  ]
}"""

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
    "二轴总成、"                → "二轴总成"  (trailing 、 cleaned up)
    English headers are returned as-is (no ordinal to strip).

    FIX: Pola lama ^[^\u3001]+\u3001 menghapus SEMUA karakter sebelum 、,
    termasuk nama seksi itu sendiri jika 、 ada di akhir (mis. "二轴总成、").
    Sekarang hanya strip ordinal Cina sejati (字符 numerals + 、 di awal).
    """
    # Strip hanya jika awalan adalah angka ordinal Cina (一、二、十五、dst.)
    result = re.sub(r"^[零一二三四五六七八九十百]+\u3001", "", section_header).strip()
    # Bersihkan 、 yang mungkin tersisa di akhir (AI salah taruh)
    result = result.rstrip("\u3001").strip()
    return result if result else section_header.rstrip("\u3001").strip()


def _merge_parts(raw_parts: List[Dict]) -> List[Dict]:
    """
    Deduplicate parts within one category group.
    Key = (part_number, name_cn or name_en).  Duplicate keys → sum quantities.
    Rows with empty part_number are skipped.

    Both name_cn and name_en are preserved so _build_output_part can use
    name_en as a direct fallback when name_cn is empty or untranslatable.
    """
    merged: OrderedDict[Tuple[str, str], Dict] = OrderedDict()
    for part in raw_parts:
        pn = (part.get("part_number") or "").strip()
        if not pn:
            continue
        name_cn = (part.get("name_cn") or "").strip()
        name_en = (part.get("name_en") or "").strip()
        # Use name_cn as dedup key; fall back to name_en when name_cn absent
        key = (pn, name_cn or name_en)
        qty = part.get("quantity")
        if key not in merged:
            merged[key] = {
                "serial_no":          part.get("serial_no"),
                "part_number":        pn,
                "name_cn":            name_cn,
                "name_en":            name_en,   # preserve for direct fallback
                "quantity":           qty,
                "is_assembly_header": bool(part.get("is_assembly_header", False)),
            }
        else:
            eq = merged[key]["quantity"]
            if eq is not None and qty is not None:
                merged[key]["quantity"] = eq + qty
            # Fill in missing names if the first occurrence lacked them
            if not merged[key]["name_cn"] and name_cn:
                merged[key]["name_cn"] = name_cn
            if not merged[key]["name_en"] and name_en:
                merged[key]["name_en"] = name_en
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
        serial_raw = str(part.get("serial_no") or "").strip()

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
    """Map internal part dict to canonical EPC output schema.

    Name resolution priority for catalog_item_name_en:
      1. Translation of name_cn (from AI translation batch)
      2. name_en field preserved directly from Vision AI output
      3. Empty string (no name available)
    """
    name_cn = part.get("name_cn") or ""
    name_en_direct = part.get("name_en") or ""   # from Vision AI directly
    name_en_translated = (cn_to_en or {}).get(name_cn, "")
    # Use translated name if available; fall back to AI-supplied EN name
    name_en = _title_case_automotive(name_en_translated or name_en_direct)
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

_PARTS_TRANSLATION_PROMPT = """You are a professional automotive parts catalog translator (Chinese → English).
Translate each Chinese part/assembly name into clear, professional English.
These are spare part and assembly names from a heavy-truck transmission parts catalog.

Return ONLY valid JSON — no markdown, no explanation:
{
  "translations": [
    { "cn": "<original Chinese>", "en": "<English translation>" }
  ]
}

Rules:
- Same order and count as input. Do NOT skip any item.
- Use Title Case: capitalize every word except articles/prepositions in the middle.
  Example: "Main Shaft Bearing Cover", "Left and Right Intermediate Shaft Assembly".
- Write out ordinals in full: "Second" not "2nd", "Third" not "3rd", "First" not "1st".
- Do NOT use abbreviations. Write "Assembly" not "Assy", "Intermediate" not "Inter.",
  "Transmission" not "Trans.", "Left" not "L.", "Right" not "R.".
- Use standard heavy-truck / drivetrain terminology:
    变速器壳体 → Transmission Housing
    离合器 → Clutch
    一轴 → Primary Shaft
    二轴 → Secondary Shaft
    中间轴 → Intermediate Shaft
    倒档 → Reverse Gear
    上盖 → Top Cover
    操纵装置 → Control Mechanism
    副箱 → Auxiliary Gearbox
    后盖 → Rear Cover
    气缸 → Cylinder
    取力器 → Power Take-Off
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

        # ── NORMALIZE field names before accumulating (PATCH 2026-04-09b) ──
        parts_before = [_normalize_part(p) for p in (result.get("parts_before_header") or [])]
        parts_after  = [_normalize_part(p) for p in (result.get("parts") or [])]

        # Log how many parts survived normalization (debug aid)
        raw_before_count = len(result.get("parts_before_header") or [])
        raw_after_count  = len(result.get("parts") or [])
        if parts_before or parts_after:
            logger.debug(
                "Page %d: normalized %d before-header parts, %d after-header parts",
                page_idx + 1, len(parts_before), len(parts_after),
            )

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

        # Log parts count after merge for debugging
        logger.info(
            "Category '%s': %d raw parts → %d after dedup",
            category_cn, len(raw_parts), len(merged),
        )

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
        # If category_cn has no Chinese characters it is actually English text
        # (AI returned an English section header). Move it to category_en and
        # clear category_cn so the CN name field is not filled with English.
        if category_cn and not _CN_CHAR_RE.search(category_cn):
            if not category_en:
                category_en = category_cn
            category_cn = ""

        # Apply Title Case + ordinal expansion to English category name
        category_en = _title_case_automotive(category_en)

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