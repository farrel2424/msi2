"""
weichai_engine_extractor.py
============================
Stage 1 & Stage 2 extractor for Weichai Engine partbooks (WP10 series et al.).

KEY DIFFERENCE from Cummins/Xi'an extractor:
  Weichai PDFs are TEXT-BASED — PyMuPDF extracts text directly.
  NO Vision AI required → faster, cheaper, offline-capable.

PDF structure per Weichai standard:
  - Cover + Foreword pages  (skip)
  - TOC pages               (skip for parts; use for category list)
  - Diagram pages           (skip — odd catalog pages)
  - Table pages             (extract!)
      • Header  : catalog title + section title in Chinese(English)
      • Columns : 图序号 | 件号 | 数量 | 中文名称 | Part Name

Section title location:
  Every table page footer contains:
    "WP10系列发动机零件图册  WP10 SERIES ENGINE PARTS CATALOGUE"
    "<Chinese section name>(<English section name>)"

Stage 1 output (compatible with batch_create_flat_categories):
  {
    "categories": [
      { "category_name_en": "Engine Block Group",
        "category_name_cn": "机体结合组",
        "category_description": "" }
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

# Matches: "Chinese名称(English Name)" — the bilingual section header
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
# NOTE: We deliberately do NOT filter short digit strings here.
# Table item numbers (1,2,3…) and quantities (6,22…) are single/dual digits
# that would be incorrectly removed by a broad \d{1,4} pattern.
_NOISE_RES = [re.compile(p) for p in _NOISE_PATTERNS]

# Signals that tell us a page has a parts table
_TABLE_SIGNALS = ('图序号', 'Pos.')

# Catalog-title line that precedes the section title in the footer
_CATALOG_TITLE_SIGNAL = 'WP10 SERIES ENGINE PARTS CATALOGUE'

# ── Helpers ────────────────────────────────────────────────────────────────

def _is_noise_line(line: str) -> bool:
    """True if line is a watermark / page-number / boilerplate."""
    for r in _NOISE_RES:
        if r.search(line):
            return True
    return False


def _split_cn_en(text: str) -> Tuple[str, str]:
    """
    Split a mixed Chinese-English string at the last Chinese character.

    Examples:
      "气缸套 Cylinder Liner"          → ("气缸套", "Cylinder Liner")
      "机体总成 crankcase assembly"    → ("机体总成", "crankcase assembly")
      "Washer"                         → ("", "Washer")
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


# ── Section title extraction ───────────────────────────────────────────────

def _extract_section_title(text: str) -> Optional[Tuple[str, str]]:
    """
    Extract (en_name, cn_name) from a table page.

    Primary path  : find the line immediately after the catalog-title line.
    Fallback path : search first 12 lines of the page for a bilingual title.

    Returns (en, cn) or None.
    """
    lines = text.split('\n')
    cleaned = [l.strip() for l in lines]

    # ── Primary: footer pattern ──────────────────────────────────────────
    for i, line in enumerate(cleaned):
        if _CATALOG_TITLE_SIGNAL in line:
            # Next non-empty, non-noise line should be the section title
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

    # ── Fallback: early lines of the page ────────────────────────────────
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


# ── Parts-row parsing ──────────────────────────────────────────────────────
#
# IMPORTANT: PyMuPDF extracts each TABLE CELL as a separate line.
# A 5-column row spreads across 5 consecutive lines:
#   Line 0 : 图序号  (item number)   Line 1 : 件号 (part number)
#   Line 2 : 数量    (quantity)      Line 3 : 中文名称 (Chinese name)
#   Line 4 : Part Name (English name)
# We walk the clean line list in windows of 5.


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


# Keep for single-line fallback
def _parse_part_row(line: str) -> Optional[Dict]:
    """
    Parse one parts-table row.

    Expected token layout:
      [0] item_no   – digits only, e.g. "1", "13"
      [1] part_no   – alphanumeric ≥6 chars, e.g. "612630010015", "61460070011S"
      [2] qty       – digits only, e.g. "6", "22"
      [3..] cn + en – mixed Chinese + English, e.g. "气缸套 Cylinder Liner"
    """
    tokens = line.split()
    if len(tokens) < 4:
        return None

    # Token 0: numeric item number
    if not re.match(r'^\d+$', tokens[0]):
        return None

    # Token 1: part number — alphanumeric, minimum 6 chars
    pn = tokens[1]
    if len(pn) < 6 or not re.match(r'^[A-Z0-9][A-Z0-9\-\.]*$', pn, re.IGNORECASE):
        return None

    # Token 2: quantity
    if not re.match(r'^\d+$', tokens[2]):
        return None
    qty = int(tokens[2])

    # Remaining tokens: "cn_name [en_name]"
    rest = ' '.join(tokens[3:])
    cn_name, en_name = _split_cn_en(rest)

    if not cn_name and not en_name:
        return None

    return {
        'item_no':     tokens[0],
        'part_number': pn,
        'qty':         qty,
        'name_cn':     cn_name,
        'name_en':     en_name,
    }


def _parse_parts_table(text: str) -> List[Dict]:
    """
    Extract parts from a Weichai table page.

    Primary path  : 5-lines-per-row (PyMuPDF one-cell-per-line layout).
    Fallback path : single-line rows (future-proof for other PDF layouts).
    """
    # Strip noise and blank lines from the WHOLE page (header area uses full filter)
    clean: List[str] = []
    for raw in text.split('\n'):
        s = raw.strip()
        if s and not _is_noise_line(s):
            clean.append(s)

    # Find body start: line after 'Qty.'
    body_start = 0
    for i, line in enumerate(clean):
        if line.startswith('Qty.') or line == 'Qty.':
            body_start = i + 1
            break
    
    # Re-build body WITHOUT digit-string filtering so item numbers & qtys survive
    # We re-scan from the raw text, skipping the header portion we already found.
    # This is necessary because _is_noise_line now preserves short digit strings,
    # but we still need to skip header rows (图序号, Pos., etc.) above body_start.
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

    # ── Primary: 5-line windows ───────────────────────────────────────────
    i = 0
    while i <= len(body) - 5:
        row = _parse_five_lines(body[i:i + 5])
        if row:
            parts.append(row)
            i += 5
        else:
            i += 1

    # ── Fallback: single-line rows (e.g. if PDF returns full text per row) ─
    if not parts:
        for line in body:
            part = _parse_part_row(line)
            if part:
                parts.append(part)

    return parts


# ── Deduplication ──────────────────────────────────────────────────────────

def _merge_parts(raw_parts: List[Dict]) -> List[Dict]:
    """
    Deduplicate by (part_number, name_cn); sum quantities for duplicates.
    Rows with empty part_number are dropped.
    """
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


# ── Public API — Stage 1 ───────────────────────────────────────────────────

def extract_weichai_engine_categories(
    pdf_path: str,
    sumopod_client=None,   # kept for API compatibility; not used
) -> Dict:
    """
    Extract flat category list from a Weichai engine partbook.

    Scans every page; collects unique bilingual section titles from
    table pages only. No AI call needed.

    Returns dict compatible with EPCAutomation.submit_to_epc() and
    batch_create_flat_categories().
    """
    logger.info("Weichai Stage 1: opening '%s'", pdf_path)

    doc = fitz.open(pdf_path)
    total_pages = len(doc)
    seen: OrderedDict[str, Tuple[str, str]] = OrderedDict()  # en → (en, cn)

    for page_idx in range(total_pages):
        text = doc[page_idx].get_text('text')

        # Skip pages without a parts table
        if not any(sig in text for sig in _TABLE_SIGNALS):
            continue

        title = _extract_section_title(text)
        if not title:
            logger.debug("Page %d: table page but no section title found", page_idx + 1)
            continue

        en_name, cn_name = title
        if en_name not in seen:
            seen[en_name] = (en_name, cn_name)
            logger.info("Page %d: category '%s' / '%s'", page_idx + 1, en_name, cn_name)

    doc.close()

    categories = [
        {
            "category_name_en":  en,
            "category_name_cn":  cn,
            "category_description": "",
        }
        for en, cn in seen.values()
    ]

    # Build lookup map: both CN→EN and EN→EN
    code_to_category: Dict[str, str] = {}
    for en, cn in seen.values():
        code_to_category[en] = en
        if cn:
            code_to_category[cn] = en

    logger.info(
        "Weichai Stage 1 complete: %d categories extracted from %d pages",
        len(categories), total_pages,
    )
    return {"categories": categories, "code_to_category": code_to_category}


# ── Public API — Stage 2 ───────────────────────────────────────────────────

def extract_weichai_engine_parts(
    pdf_path: str,
    sumopod_client=None,    # kept for API compatibility; not used
    target_id_start: int = 1,
    category_map: Optional[Dict[str, str]] = None,
    custom_prompt: Optional[str] = None,  # not used; text-based
) -> List[Dict]:
    """
    Extract all parts from a Weichai engine partbook PDF.

    Pure text extraction — no Vision AI, no network calls.

    T-IDs reset to T001 for each category group (same convention as
    the Cummins engine extractor).

    Returns a list of group dicts compatible with batch_submit_parts().
    """
    logger.info("Weichai Stage 2: opening '%s'", pdf_path)

    doc = fitz.open(pdf_path)
    total_pages = len(doc)
    groups: OrderedDict[str, Dict] = OrderedDict()

    for page_idx in range(total_pages):
        text = doc[page_idx].get_text('text')

        # Skip non-table pages
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

    # Deduplicate, merge quantities, assign T-IDs
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