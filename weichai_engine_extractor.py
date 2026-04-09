"""
weichai_engine_extractor.py
============================
Stage 1 & Stage 2 extractor for Weichai Engine partbooks (WP10 series et al.).

KEY DIFFERENCE from Cummins/Xi'an extractor:
  Weichai PDFs are TEXT-BASED — PyMuPDF extracts text directly.
  NO Vision AI required → faster, cheaper, offline-capable.

HIERARCHY (3-level):
  Master Category → Category (bold in TOC) → Type Category (indented in TOC)
  "Engine"          "Engine Block Group"      "Crankcase Assembly"

TOC page structure:
  **机体结合组(Engine Block Group)**  ............... 1    ← bold → Category
      机体总成(crankcase assembly)  ................. 3    ← indented → Type Category
        气缸体预装配(Cylinder Block Preassembly)  .. 5    ← deeper indent → Type Category
  **油封结合组(Oil Seal Group)**  ................... 7    ← bold, no children → Category (empty data_type)

Table page structure (for Stage 2):
  Header : catalog title + section title in Chinese(English)
  Columns: 图序号 | 件号 | 数量 | 中文名称 | Part Name

Stage 1 output (compatible with batch_create_type_categories_and_categories):
  {
    "categories": [
      {
        "category_name_en":     "Engine Block Group",
        "category_name_cn":     "机体结合组",
        "category_description": "",
        "data_type": [
          { "type_category_name_en": "Crankcase Assembly",
            "type_category_name_cn": "机体总成",
            "type_category_description": "" },
          { "type_category_name_en": "Cylinder Block Preassembly",
            "type_category_name_cn": "气缸体预装配",
            "type_category_description": "" }
        ]
      },
      {
        "category_name_en": "Oil Seal Group",
        "category_name_cn": "油封结合组",
        "category_description": "",
        "data_type": []
      }
    ],
    "code_to_category": { "Engine Block Group": "Engine Block Group", ... }
  }

Stage 2 output (compatible with batch_submit_parts):
  [
    {
      "category_name_en":  "Engine Block Group",
      "category_name_cn":  "机体结合组",
      "subtype_name_en":   "Engine Block Group",
      "subtype_name_cn":   "机体结合组",
      "subtype_code":      "",
      "parts": [
        { "target_id": "T001", "part_number": "612630010015",
          "catalog_item_name_en": "Cylinder Liner",
          "catalog_item_name_ch": "气缸套",
          "quantity": 6, "description": "", "unit": "" }
      ]
    }
  ]
"""

from __future__ import annotations

import re
import logging
from collections import OrderedDict
from typing import Dict, List, Optional, Tuple

import fitz  # PyMuPDF

logger = logging.getLogger(__name__)

# ── Compile-time patterns ──────────────────────────────────────────────────

# Matches: "Chinese名称(English Name)" — the bilingual TOC/section entry
_BILINGUAL_RE = re.compile(
    r'([\u4e00-\u9fff][\u4e00-\u9fff\u3000-\u303f（）【】、，。\s]*?)'
    r'\s*[（(]([A-Za-z][^)）\n]{2,})[)）]'
)

# Lines to filter out (watermarks, boilerplate, page numbers)
_NOISE_PATTERNS = [
    r'wangmd', r'zhangzhi', r'shacman', r'王明德',
    r'\d{4}/\d{2}/\d{2}',    # date stamps like 2023/09/22
    r'^\d+:\d+:\d+',          # time stamps like 13:18:08
]
_NOISE_RES = [re.compile(p) for p in _NOISE_PATTERNS]

# Signals that tell us a page has a parts table (Stage 2 only)
_TABLE_SIGNALS = ('图序号', 'Pos.')

# Catalog-title line that precedes the section title in the footer
_CATALOG_TITLE_SIGNAL = 'WP10 SERIES ENGINE PARTS CATALOGUE'

# x0 threshold: TOC entries at or below this coordinate are top-level Categories.
# Entries to the right of this are Type Categories (sub-entries).
_X_CATEGORY_MAX = 50

# Watermark / metadata lines to skip during TOC parsing
_TOC_SKIP_WORDS = ["wangmd", "2023/", "2024/", "shacman.com", "zhangzhi",
                   "CONTENTS", "目录", "王明德"]


# ── Helpers ────────────────────────────────────────────────────────────────

def _is_noise_line(line: str) -> bool:
    """True if line is a watermark / page-number / boilerplate."""
    for r in _NOISE_RES:
        if r.search(line):
            return True
    return False


def _is_toc_skip(text: str) -> bool:
    """True if span text should be ignored during TOC parsing."""
    return any(p in text for p in _TOC_SKIP_WORDS)


def _clean_en_label(en: str) -> str:
    """
    Strip parentheses and deduplicate adjacent identical tokens.
    "EVB EVB Bracket Assembly" → "EVB Bracket Assembly"
    """
    en = re.sub(r"[()]", "", en)
    parts = en.split()
    deduped: List[str] = []
    for p in parts:
        if deduped and deduped[-1].upper() == p.upper():
            continue
        deduped.append(p)
    return " ".join(deduped).strip()


def _title_case(text: str) -> str:
    """Convert to Title Case, preserving known abbreviations."""
    return " ".join(
        w if w.isupper() and len(w) > 1 else w.capitalize()
        for w in text.split()
    )


def _split_cn_en(text: str) -> Tuple[str, str]:
    """
    Split a mixed Chinese-English string at the last Chinese character.
    "气缸套 Cylinder Liner" → ("气缸套", "Cylinder Liner")
    """
    last_cn = max(
        (i for i, c in enumerate(text) if '\u4e00' <= c <= '\u9fff'),
        default=-1,
    )
    if last_cn == -1:
        return '', text.strip()
    return text[:last_cn + 1].strip(), text[last_cn + 1:].strip()


def _clean_lines(text: str) -> List[str]:
    """Return non-empty, non-noise lines from raw page text."""
    lines = []
    for raw in text.split('\n'):
        stripped = raw.strip()
        if stripped and not _is_noise_line(stripped):
            lines.append(stripped)
    return lines


# ── TOC parsing (Stage 1) ─────────────────────────────────────────────────
#
# FIX: Previously, extract_weichai_engine_categories() read TABLE pages
# (pages containing '图序号'/'Pos.') and extracted only flat section titles.
# This produced a flat list with no data_type hierarchy.
#
# Now: we parse the TOC pages using PyMuPDF's font/position dict to detect:
#   Bold font (flags & 16, or "Bold" in font name) + x0 ≤ 50 → Category
#   All other bilingual entries under a Category             → Type Category
#
# This matches the visual structure of the Weichai DHL TOC exactly.

def _parse_bilingual_entry(cn_text: str, en_text: str) -> Tuple[str, str]:
    """
    Given raw Chinese and English strings from a TOC span, return (en, cn).
    Applies Title Case to English. Strips parentheses.
    """
    cn = cn_text.strip()
    en = _clean_en_label(en_text.strip())
    en = _title_case(en)
    return en, cn


def _extract_toc_from_page(page: fitz.Page) -> List[Tuple[float, bool, str, str]]:
    """
    Parse one page using PyMuPDF font-dict and return a list of:
        (x0, is_bold_en, cn_text, en_text)
    for each bilingual TOC entry found.
    """
    entries: List[Tuple[float, bool, str, str]] = []

    for block in page.get_text("dict")["blocks"]:
        if block.get("type") != 0:
            continue
        for line in block.get("lines", []):
            spans = line.get("spans", [])
            cn_parts: List[str] = []
            en_parts: List[str] = []
            x0_min = 9999.0
            en_is_bold = False

            for span in spans:
                text = span["text"].strip()
                if not text or _is_toc_skip(text):
                    continue
                if re.match(r"^\.{3,}$", text):   # dot leaders
                    continue
                if re.match(r"^\d+$", text):        # page numbers
                    continue

                flags = span.get("flags", 0)
                font  = span.get("font", "")
                is_bold = bool(flags & 16) or ("Bold" in font)
                x0 = span["bbox"][0]
                x0_min = min(x0_min, x0)

                if re.search(r"[\u4e00-\u9fff]", text):
                    cn_parts.append(text)
                elif re.search(r"[A-Za-z]", text):
                    en_parts.append(text)
                    if is_bold:
                        en_is_bold = True

            cn = "".join(cn_parts).strip()
            en = " ".join(en_parts).strip()

            if (cn or en) and x0_min < 200:   # exclude far-right artefacts
                entries.append((x0_min, en_is_bold, cn, en))

    return entries


# ── Public API — Stage 1 ───────────────────────────────────────────────────

def extract_weichai_engine_categories(
    pdf_path: str,
    sumopod_client=None,   # kept for API compatibility; not used
) -> Dict:
    """
    Extract 3-level category structure from a Weichai engine partbook TOC.

    FIXED: Now parses TOC pages using PyMuPDF font/position data to build
    the correct Master → Category → Type Category hierarchy:
      - Bold entry (en_is_bold=True) at x0 ≤ _X_CATEGORY_MAX → Category
      - All bilingual entries directly under a Category       → Type Categories

    Previously this function read TABLE pages and returned only a flat list.
    It now reads ALL pages, detects TOC entries by font, and returns
    data_type-populated categories compatible with
    batch_create_type_categories_and_categories().

    Args:
        pdf_path:       Path to the Weichai engine partbook PDF.
        sumopod_client: Unused — kept for API compatibility.

    Returns:
        {
          "categories": [
            { "category_name_en": "Engine Block Group",
              "category_name_cn": "机体结合组",
              "category_description": "",
              "data_type": [
                { "type_category_name_en": "Crankcase Assembly",
                  "type_category_name_cn": "机体总成",
                  "type_category_description": "" }
              ] },
            { "category_name_en": "Oil Seal Group",
              "category_name_cn": "油封结合组",
              "category_description": "",
              "data_type": [] },
            ...
          ],
          "code_to_category": { "Engine Block Group": "Engine Block Group", ... }
        }
    """
    logger.info("Weichai Stage 1 (TOC): opening '%s'", pdf_path)

    doc = fitz.open(pdf_path)
    total_pages = len(doc)

    # Ordered dict: category_en → { cn, data_type: OrderedDict{dedup_key → entry} }
    categories: OrderedDict[str, Dict] = OrderedDict()
    current_cat_en: Optional[str] = None

    for page_idx in range(total_pages):
        page = doc[page_idx]
        entries = _extract_toc_from_page(page)

        for x0, en_is_bold, cn, en in entries:
            if not cn and not en:
                continue

            en_clean, cn_clean = _parse_bilingual_entry(cn, en)
            if not en_clean and not cn_clean:
                continue

            # Determine level: bold EN at x0 ≤ threshold → Category
            is_category = en_is_bold and x0 <= _X_CATEGORY_MAX

            if is_category:
                if en_clean not in categories:
                    categories[en_clean] = {
                        "cn":       cn_clean,
                        "subtypes": OrderedDict(),
                    }
                    logger.info(
                        "Page %d: [CAT] '%s' / '%s'",
                        page_idx + 1, en_clean, cn_clean,
                    )
                current_cat_en = en_clean

            else:
                # Type Category — belongs to the current Category
                if current_cat_en is None:
                    logger.debug(
                        "Page %d: subtype '%s' before first category — skipped",
                        page_idx + 1, en_clean,
                    )
                    continue

                dedup_key = cn_clean or en_clean
                if dedup_key not in categories[current_cat_en]["subtypes"]:
                    categories[current_cat_en]["subtypes"][dedup_key] = {
                        "type_category_name_en":     en_clean,
                        "type_category_name_cn":     cn_clean,
                        "type_category_description": "",
                    }
                    logger.debug(
                        "Page %d:   └─ [SUBTYPE] '%s' / '%s' → '%s'",
                        page_idx + 1, en_clean, cn_clean, current_cat_en,
                    )

    doc.close()

    # Build final output
    output_categories = []
    code_to_category: Dict[str, str] = {}

    for cat_en, cat_data in categories.items():
        cat_cn = cat_data["cn"]
        subtypes = list(cat_data["subtypes"].values())

        output_categories.append({
            "category_name_en":     cat_en,
            "category_name_cn":     cat_cn,
            "category_description": "",
            "data_type":            subtypes,
        })

        # Index both EN and CN names for code_to_category lookups
        code_to_category[cat_en] = cat_en
        if cat_cn:
            code_to_category[cat_cn] = cat_en
        for st in subtypes:
            st_en = st.get("type_category_name_en", "")
            st_cn = st.get("type_category_name_cn", "")
            if st_en:
                code_to_category[st_en] = cat_en
            if st_cn:
                code_to_category[st_cn] = cat_en

    total_subtypes = sum(len(c["data_type"]) for c in output_categories)
    logger.info(
        "Weichai Stage 1 complete: %d categories, %d subtypes (from %d pages)",
        len(output_categories), total_subtypes, total_pages,
    )

    return {
        "categories":       output_categories,
        "code_to_category": code_to_category,
    }


# ── Section title extraction (Stage 2 helpers) ────────────────────────────

def _extract_section_title(text: str) -> Optional[Tuple[str, str]]:
    """
    Extract (en_name, cn_name) from a table page.
    Primary: footer pattern (after catalog-title line).
    Fallback: early lines of the page.
    """
    lines = text.split('\n')
    cleaned = [l.strip() for l in lines]

    for i, line in enumerate(cleaned):
        if _CATALOG_TITLE_SIGNAL in line:
            for j in range(i + 1, min(i + 5, len(cleaned))):
                candidate = cleaned[j]
                if not candidate or _is_noise_line(candidate):
                    continue
                m = _BILINGUAL_RE.search(candidate)
                if m:
                    cn = m.group(1).strip()
                    en = m.group(2).strip()
                    if cn and en:
                        return en, cn
            break

    for line in _clean_lines(text)[:12]:
        if any(skip in line for skip in (_CATALOG_TITLE_SIGNAL, '图序号', 'Pos.', 'Part Number')):
            continue
        m = _BILINGUAL_RE.search(line)
        if m:
            cn = m.group(1).strip()
            en = m.group(2).strip()
            if cn and en and len(cn) >= 3:
                return en, cn

    return None


# ── Parts-row parsing (Stage 2) ───────────────────────────────────────────

def _parse_five_lines(window: List[str]) -> Optional[Dict]:
    """Try to interpret 5 consecutive lines as one parts-table row."""
    if len(window) < 5:
        return None
    item_raw, pn_raw, qty_raw, cn_raw, en_raw = window[:5]
    if not re.match(r'^\d+$', item_raw):
        return None
    if len(pn_raw) < 6 or not re.match(r'^[A-Z0-9][A-Z0-9\-\.]*$', pn_raw, re.IGNORECASE):
        return None
    if not re.match(r'^\d+$', qty_raw):
        return None
    cn_name = cn_raw.strip()
    en_name = en_raw.strip()
    if not cn_name and not en_name:
        return None
    return {'item_no': item_raw, 'part_number': pn_raw, 'qty': int(qty_raw),
            'name_cn': cn_name, 'name_en': en_name}


def _parse_part_row(line: str) -> Optional[Dict]:
    """Parse single-line parts-table row (fallback for non-cell-per-line PDFs)."""
    tokens = line.split()
    if len(tokens) < 4:
        return None
    if not re.match(r'^\d+$', tokens[0]):
        return None
    pn = tokens[1]
    if len(pn) < 6 or not re.match(r'^[A-Z0-9][A-Z0-9\-\.]*$', pn, re.IGNORECASE):
        return None
    if not re.match(r'^\d+$', tokens[2]):
        return None
    qty = int(tokens[2])
    rest = ' '.join(tokens[3:])
    cn_name, en_name = _split_cn_en(rest)
    if not cn_name and not en_name:
        return None
    return {'item_no': tokens[0], 'part_number': pn, 'qty': qty,
            'name_cn': cn_name, 'name_en': en_name}


def _parse_parts_table(text: str) -> List[Dict]:
    """Extract parts rows from a Weichai table page."""
    clean: List[str] = []
    for raw in text.split('\n'):
        s = raw.strip()
        if s and not _is_noise_line(s):
            clean.append(s)

    body_start = 0
    for i, line in enumerate(clean):
        if line.startswith('Qty.') or line == 'Qty.':
            body_start = i + 1
            break

    header_lines = set(clean[:body_start])
    body: List[str] = []
    counting_header = True
    skipped = 0
    for raw in text.split('\n'):
        s = raw.strip()
        if not s or _is_noise_line(s):
            continue
        if counting_header:
            if s in header_lines and skipped < body_start:
                skipped += 1
                continue
            else:
                counting_header = False
        body.append(s)

    parts: List[Dict] = []

    # Primary: 5-line windows (one cell per line layout)
    i = 0
    while i <= len(body) - 5:
        row = _parse_five_lines(body[i:i + 5])
        if row:
            parts.append(row)
            i += 5
        else:
            i += 1

    # Fallback: single-line rows
    if not parts:
        for line in body:
            part = _parse_part_row(line)
            if part:
                parts.append(part)

    return parts


# ── Deduplication (Stage 2) ───────────────────────────────────────────────

def _merge_parts(raw_parts: List[Dict]) -> List[Dict]:
    """Deduplicate by (part_number, name_cn); sum quantities for duplicates."""
    merged: OrderedDict = OrderedDict()
    for p in raw_parts:
        pn = (p.get('part_number') or '').strip()
        if not pn:
            continue
        cn = (p.get('name_cn') or '').strip()
        key = (pn, cn)
        qty = p.get('qty', 1)
        if key not in merged:
            merged[key] = {
                'part_number': pn,
                'name_cn':     cn,
                'name_en':     (p.get('name_en') or '').strip(),
                'qty':         qty,
            }
        else:
            merged[key]['qty'] += qty
    return list(merged.values())


# ── Public API — Stage 2 ───────────────────────────────────────────────────

def extract_weichai_engine_parts(
    pdf_path: str,
    sumopod_client=None,
    target_id_start: int = 1,
    category_map: Optional[Dict[str, str]] = None,
    custom_prompt: Optional[str] = None,
) -> List[Dict]:
    """
    Extract all parts from a Weichai engine partbook PDF.
    Pure text extraction — no Vision AI, no network calls.
    T-IDs reset to T001 for each category group.
    """
    logger.info("Weichai Stage 2: opening '%s'", pdf_path)

    doc = fitz.open(pdf_path)
    total_pages = len(doc)
    groups: OrderedDict[str, Dict] = OrderedDict()

    for page_idx in range(total_pages):
        text = doc[page_idx].get_text('text')

        if not any(sig in text for sig in _TABLE_SIGNALS):
            continue

        title = _extract_section_title(text)
        if not title:
            logger.debug("Page %d: table page, no title found — skipped", page_idx + 1)
            continue

        en_name, cn_name = title
        raw_parts = _parse_parts_table(text)

        if en_name not in groups:
            groups[en_name] = {
                "category_name_en": en_name,
                "category_name_cn": cn_name,
                "subtype_name_en":  en_name,
                "subtype_name_cn":  cn_name,
                "subtype_code":     "",
                "raw_parts":        [],
            }

        groups[en_name]["raw_parts"].extend(raw_parts)
        logger.info(
            "Page %d: '%s' — +%d rows (total %d)",
            page_idx + 1, en_name, len(raw_parts),
            len(groups[en_name]["raw_parts"]),
        )

    doc.close()

    output: List[Dict] = []
    for en_name, grp in groups.items():
        merged = _merge_parts(grp["raw_parts"])
        if not merged:
            logger.info("'%s': all rows filtered — skipped", en_name)
            continue

        tagged = [
            {
                "target_id":            f"T{i:03d}",
                "part_number":          p["part_number"],
                "catalog_item_name_en": p["name_en"],
                "catalog_item_name_ch": p["name_cn"],
                "quantity":             p["qty"],
                "description":          "",
                "unit":                 "",
            }
            for i, p in enumerate(merged, start=1)
        ]

        output.append({
            "category_name_en": grp["category_name_en"],
            "category_name_cn": grp["category_name_cn"],
            "subtype_name_en":  grp["subtype_name_en"],
            "subtype_name_cn":  grp["subtype_name_cn"],
            "subtype_code":     "",
            "parts":            tagged,
        })

        logger.info(
            "'%s': %d parts (from %d raw rows)",
            en_name, len(tagged), len(grp["raw_parts"]),
        )

    total_parts = sum(len(g["parts"]) for g in output)
    logger.info(
        "Weichai Stage 2 complete: %d categories, %d total parts",
        len(output), total_parts,
    )
    return output