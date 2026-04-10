"""
axle_drive_parts_extractor.py
==============================
Stage 1 & Stage 2 — Text-based extractor untuk katalog suku cadang Axle Drive
(Shaanxi Hande Axle Co., Ltd.). TANPA Vision AI — membaca layer teks PDF langsung.

FUNGSI PUBLIK:
  extract_axle_drive_categories_text()  →  Stage 1: struktur Kategori + TypeCategory
  extract_axle_drive_parts()            →  Stage 2: detail suku cadang per SubKategori

Stage 2 — Parts extractor untuk katalog suku cadang Axle Drive (Shaanxi Hande).

STRATEGI: Text-based extraction (PyMuPDF) — TANPA Vision AI.
PDF Hande Axle memiliki layer teks yang bisa dibaca langsung.

FORMAT PDF:
  - Halaman Cover  : logo Shaanxi Hande + "Spare Parts" → ABAIKAN
  - Halaman Diagram: gambar exploded view → ABAIKAN
  - Halaman Tabel  : judul "表N <nama>" + tabel suku cadang → EKSTRAK

IDENTIFIKASI SUB-KATEGORI:
  Judul tabel (center-aligned, diawali "表N"):
    "表1 贯通式驱动桥主减速器总成爆炸图对应备件目录"
      → SubKategori: "贯通式驱动桥主减速器总成爆炸图对应备件目录"
    "表2 贯通式驱动桥主减速器总成爆炸图对应备件目录(续)"
      → SubKategori: "贯通式驱动桥主减速器总成爆炸图对应备件目录"  ← (续) stripped → gabung ke grup yang sama

KOLOM TABEL:
  序号/Item | 汉德零件号/HanDe part nr. | English Description | 中文描述 | 数量/Qty | 备注/Remarks

OUTPUT (kompatibel dengan batch_submit_parts):
  [
    {
      "category_name_en":  "",
      "category_name_cn":  "",
      "subtype_name_en":   "Drive Axle Final Reducer Assembly Parts List",
      "subtype_name_cn":   "贯通式驱动桥主减速器总成爆炸图对应备件目录",
      "subtype_code":      "",
      "parts": [
        {
          "target_id":            "T001",
          "part_number":          "DZ95149320054",
          "catalog_item_name_en": "Locknut",
          "catalog_item_name_ch": "十二角螺母",
          "quantity":             2,
          "description":          "",
          "unit":                 ""
        }
      ]
    }
  ]
"""

from __future__ import annotations

import json
import logging
import re
from collections import OrderedDict
from typing import Dict, List, Optional, Tuple

import fitz  # PyMuPDF

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Patterns & constants
# ─────────────────────────────────────────────────────────────────────────────

# Table title: "表1 <text>" or "表 1 <text>"
_TABLE_TITLE_RE = re.compile(
    r'表\s*[\d一二三四五六七八九十]+\s+(.+)',
    re.UNICODE
)

# Continuation marker at end of title: (续) or （续）
_CONTINUATION_RE = re.compile(r'\s*[（(]续[）)]\s*$', re.UNICODE)

# Part number pattern: alphanumeric, at least 6 chars
_PART_NUMBER_RE = re.compile(r'^[A-Z0-9][A-Z0-9\.\-]{4,}$', re.IGNORECASE)

# Column X-position thresholds (fraction of page width)
# Matches Hande Axle catalog layout:
#   序号 | 汉德零件号 | English | 中文 | 数量 | 备注
_COL_ITEM_MAX   = 0.09   # 序号/Item  (left edge)
_COL_PARTNO_MAX = 0.32   # 汉德零件号
_COL_EN_MAX     = 0.57   # English description
_COL_CN_MAX     = 0.80   # 中文描述
_COL_QTY_MAX    = 0.91   # 数量/Qty
# > 0.91 → 备注/Remarks

# Words that indicate a header row (skip these rows)
_HEADER_TOKENS = {
    '序号', 'item', '汉德零件号', 'hande', 'part', 'nr.', 'nr',
    '描述', 'description', '数量', 'qty', '备注', 'remarks',
    'english', '中文',
}

# Qty values that mean "as needed" or "optional" → store as None
_VARIABLE_QTY = {'按需', '选用', 'ar', 'ref', '-', ''}

# Cover page signals — must have multiple of these to be classified as cover
_COVER_SIGNALS = {
    'spare', 'parts', '备件', '汉德编号', '制动器型式',
    'shaanxi', 'shaan', 'hande', 'axle', '汉德',
}

# Diagram page signal: has exploded-view title label at top, no table title
_DIAGRAM_SIGNAL = '爆炸图'
_FIGURE_TITLE_RE = re.compile(r'图\s*\d+', re.UNICODE)  # "图1", "图 2"


# ─────────────────────────────────────────────────────────────────────────────
# Page classification
# ─────────────────────────────────────────────────────────────────────────────

def _is_cover_page(page: fitz.Page) -> bool:
    """
    True if this is the cover page (Shaanxi Hande logo + Spare Parts).
    Cover pages have multiple cover signals but NO table title ("表N ...").
    """
    text = page.get_text("text")
    text_lower = text.lower()
    hits = sum(1 for sig in _COVER_SIGNALS if sig in text_lower)
    has_table_title = bool(_TABLE_TITLE_RE.search(text))
    return hits >= 3 and not has_table_title


def _is_diagram_page(page: fitz.Page) -> bool:
    text = page.get_text("text")
    
    # Jika ada judul tabel → PASTI bukan diagram, jangan cek lebih lanjut
    if _TABLE_TITLE_RE.search(text):
        return False
    
    # Jika ada kata kunci kolom tabel → bukan diagram
    TABLE_COLUMN_SIGNALS = ('序号', 'Item', '汉德零件号', 'HanDe', '数量', 'Qty')
    if any(sig in text for sig in TABLE_COLUMN_SIGNALS):
        return False

    blocks = page.get_text("dict")["blocks"]
    has_image = any(b.get("type") == 1 for b in blocks)
    has_diagram_title = _FIGURE_TITLE_RE.search(text) is not None
    
    # Hanya klasifikasi sebagai diagram jika benar-benar gambar + sangat sedikit teks
    return has_image and has_diagram_title and len(text.strip()) < 300


# ─────────────────────────────────────────────────────────────────────────────
# Table title extraction
# ─────────────────────────────────────────────────────────────────────────────

def _extract_table_title(page: fitz.Page) -> Optional[str]:
    """
    Extract the SubKategori name from the table title line.

    Raw:     "表1 贯通式驱动桥主减速器总成爆炸图对应备件目录"
    Returns: "贯通式驱动桥主减速器总成爆炸图对应备件目录"

    Raw:     "表2 贯通式驱动桥主减速器总成爆炸图对应备件目录(续)"
    Returns: "贯通式驱动桥主减速器总成爆炸图对应备件目录(续)"
    """
    text = page.get_text("text")
    m = _TABLE_TITLE_RE.search(text)
    if not m:
        return None
    return m.group(1).strip()


def _group_key_from_title(title: str) -> str:
    """
    Strip the continuation suffix (续) to get a canonical group key.

    "贯通式驱动桥主减速器总成爆炸图对应备件目录(续)" → "贯通式驱动桥主减速器总成爆炸图对应备件目录"
    "贯通式驱动桥主减速器总成爆炸图对应备件目录"       → "贯通式驱动桥主减速器总成爆炸图对应备件目录"
    """
    return _CONTINUATION_RE.sub('', title).strip()


# ─────────────────────────────────────────────────────────────────────────────
# Table row parsing (word-level, column-aware)
# ─────────────────────────────────────────────────────────────────────────────

def _parse_table_rows(page: fitz.Page) -> List[Dict]:
    """
    Parse parts table rows from a table page using word X-positions.

    Words are grouped into rows by Y-coordinate (±3pt tolerance),
    then classified into columns by X-position fraction.
    """
    W = page.rect.width
    if W == 0:
        return []

    # (x0, y0, x1, y1, word, block_no, line_no, word_no)
    words = page.get_text("words")
    if not words:
        return []

    # Group words into rows by Y (round to nearest 3pt bucket)
    _Y_TOL = 3
    rows_by_y: Dict[int, List] = {}
    for w in words:
        y_key = round(w[1] / _Y_TOL) * _Y_TOL
        rows_by_y.setdefault(y_key, []).append(w)

    parts = []
    for y_key in sorted(rows_by_y):
        row_words = sorted(rows_by_y[y_key], key=lambda w: w[0])

        item_w, partno_w, en_w, cn_w, qty_w, rem_w = [], [], [], [], [], []

        for w in row_words:
            x_frac = w[0] / W
            tok = w[4].strip()
            if not tok:
                continue

            if x_frac < _COL_ITEM_MAX:
                item_w.append(tok)
            elif x_frac < _COL_PARTNO_MAX:
                partno_w.append(tok)
            elif x_frac < _COL_EN_MAX:
                en_w.append(tok)
            elif x_frac < _COL_CN_MAX:
                cn_w.append(tok)
            elif x_frac < _COL_QTY_MAX:
                qty_w.append(tok)
            else:
                rem_w.append(tok)

        item_str   = " ".join(item_w).strip()
        partno_str = " ".join(partno_w).strip()
        en_str     = " ".join(en_w).strip()
        cn_str     = " ".join(cn_w).strip()
        qty_str    = " ".join(qty_w).strip()
        rem_str    = " ".join(rem_w).strip()

        # Skip header rows
        if item_str.lower() in {'序号', 'item'} or partno_str.lower() in _HEADER_TOKENS:
            continue
        if not partno_str:
            continue

        # Must look like a part number (alphanumeric, ≥ 6 chars)
        if not _PART_NUMBER_RE.match(partno_str):
            continue

        # Parse quantity
        qty = None
        if qty_str and qty_str.lower() not in _VARIABLE_QTY:
            # Handle numeric qty
            m = re.match(r'^\d+', qty_str)
            if m:
                try:
                    qty = int(m.group())
                except ValueError:
                    pass

        parts.append({
            'serial_no':   item_str,
            'part_number': partno_str,
            'name_en':     en_str,
            'name_cn':     cn_str,
            'quantity':    qty,
            'remarks':     rem_str,
        })

    return parts


# ─────────────────────────────────────────────────────────────────────────────
# Deduplication
# ─────────────────────────────────────────────────────────────────────────────

def _merge_parts(raw_parts: List[Dict]) -> List[Dict]:
    """
    Deduplicate by (part_number, name_cn); sum quantities for duplicates.
    Rows with no valid part number are discarded.
    """
    merged: OrderedDict[Tuple[str, str], Dict] = OrderedDict()
    for p in raw_parts:
        pn = (p.get('part_number') or '').strip()
        if not pn:
            continue
        cn  = (p.get('name_cn') or '').strip()
        key = (pn, cn)
        qty = p.get('quantity')

        if key not in merged:
            merged[key] = dict(p)
        else:
            existing_qty = merged[key]['quantity']
            if existing_qty is not None and qty is not None:
                merged[key]['quantity'] = existing_qty + qty
            # Fill missing fields
            if not merged[key]['name_en'] and p.get('name_en'):
                merged[key]['name_en'] = p['name_en']
    return list(merged.values())


# ─────────────────────────────────────────────────────────────────────────────
# Translation (batch, optional)
# ─────────────────────────────────────────────────────────────────────────────

_TRANSLATION_PROMPT = """\
You are a professional automotive parts catalog translator (Chinese → English).
Translate these Chinese axle drive catalog section titles to English.
Use standard heavy-truck / axle drive terminology. Title Case.

Return ONLY valid JSON — no markdown:
{
  "translations": [
    { "cn": "<original Chinese>", "en": "<English translation>" }
  ]
}"""


def _translate_titles(cn_titles, sumopod_client):
    if not cn_titles or sumopod_client is None:
        return {}
    try:
        from pdf_utils import extract_response_text
        resp = sumopod_client.client.chat.completions.create(
            model=sumopod_client.model,
            messages=[
                {"role": "system", "content": _TRANSLATION_PROMPT},
                {"role": "user", "content": json.dumps(cn_titles, ensure_ascii=False, indent=2)},
            ],
            temperature=0.1,
            max_tokens=1000,   # ← naik dari 500
            timeout=60,        # ← naik dari 30
        )
        raw = extract_response_text(resp)

        # Strip markdown fence
        if raw.startswith("```"):
            raw = re.sub(r"^```[a-zA-Z]*\n?", "", raw)
            raw = re.sub(r"\n?```$", "", raw)

        data = json.loads(raw.strip())
        result = {t['cn']: t['en'] for t in data.get('translations', []) if t.get('cn')}
        logger.info("Translation: %d/%d berhasil", len(result), len(cn_titles))
        return result
    except Exception as exc:
        logger.warning("Translation failed: %s", exc)
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────

def extract_axle_drive_parts(
    pdf_path: str,
    sumopod_client=None,
    target_id_start: int = 1,
    code_to_category: Optional[Dict[str, str]] = None,
    custom_prompt: Optional[str] = None,   # kept for API compat; not used (text-based)
) -> List[Dict]:
    """
    Extract all parts from a Shaanxi Hande Axle Drive partbook PDF.

    Pure text extraction — no Vision AI calls, no network cost.

    Page handling:
      • Cover pages (logo + "Spare Parts") → skipped automatically
      • Diagram pages (exploded-view images, no table) → skipped
      • Table pages ("表N <title>" + parts table) → extracted

    SubKategori grouping:
      • Table title after stripping "表N " prefix = SubKategori name
      • "(续)" suffix is stripped before grouping, so continuation pages
        are merged into the same SubKategori group

    Args:
        pdf_path:         Path to the axle drive partbook PDF.
        sumopod_client:   Optional — used only to translate SubKategori CN→EN.
                          If None, CN name is used as-is for subtype_name_en.
        target_id_start:  Ignored (T-IDs restart at T001 per SubKategori).
        code_to_category: Optional map SubKategori→parent category name.
        custom_prompt:    Ignored (text-based; no prompt needed).

    Returns:
        List of SubKategori-group dicts compatible with batch_submit_parts().
    """
    logger.info("Axle Drive parts extractor (text-based): opening '%s'", pdf_path)

    doc         = fitz.open(pdf_path)
    total_pages = len(doc)
    logger.info("Total pages: %d", total_pages)

    # ── Pass 1: classify and extract ─────────────────────────────────────────
    # groups: group_key → { subtype_name_cn, raw_full_title, raw_parts }
    groups: OrderedDict[str, Dict] = OrderedDict()

    for page_idx in range(total_pages):
        page     = doc[page_idx]
        page_num = page_idx + 1

        if _is_cover_page(page):
            logger.info("Page %d: cover → skipped", page_num)
            continue

        if _is_diagram_page(page):
            logger.info("Page %d: diagram → skipped", page_num)
            continue

        title = _extract_table_title(page)
        if not title:
            logger.debug("Page %d: no table title found → skipped", page_num)
            continue

        group_key = _group_key_from_title(title)

        if group_key not in groups:
            groups[group_key] = {
                'subtype_name_cn': group_key,
                'raw_parts':       [],
            }
            logger.info("Page %d: new SubKategori '%s'", page_num, group_key)
        else:
            logger.info("Page %d: continuation of '%s'", page_num, group_key)

        rows = _parse_table_rows(page)
        groups[group_key]['raw_parts'].extend(rows)
        logger.info("Page %d: +%d rows (total %d in group)",
                    page_num, len(rows), len(groups[group_key]['raw_parts']))

    doc.close()

    if not groups:
        logger.warning("No table pages found in '%s'", pdf_path)
        return []

    # ── Pass 2: translate SubKategori titles CN→EN ────────────────────────────
    cn_titles   = [grp['subtype_name_cn'] for grp in groups.values()]
    translations = _translate_titles(cn_titles, sumopod_client)

    # ── Pass 3: build output ──────────────────────────────────────────────────
    output: List[Dict] = []

    for group_key, grp in groups.items():
        raw_parts = grp['raw_parts']
        cn_name   = grp['subtype_name_cn']
        en_name   = translations.get(cn_name) or cn_name

        if not raw_parts:
            logger.info("SubKategori '%s': no parts — skipped", cn_name)
            continue

        merged = _merge_parts(raw_parts)
        if not merged:
            logger.info("SubKategori '%s': all rows filtered — skipped", cn_name)
            continue

        tagged = [
            {
                'target_id':            f"T{i:03d}",
                'part_number':          p['part_number'],
                'catalog_item_name_en': p.get('name_en', ''),
                'catalog_item_name_ch': p.get('name_cn', ''),
                'quantity':             p.get('quantity'),
                'description':          p.get('remarks', ''),
                'unit':                 '',
            }
            for i, p in enumerate(merged, start=1)
        ]

        # Resolve parent category from code_to_category map
        cat_map = code_to_category or {}
        cat_en  = cat_map.get(cn_name) or cat_map.get(en_name) or ''

        output.append({
            'category_name_en': cat_en,
            'category_name_cn': '',
            'subtype_name_en':  en_name,
            'subtype_name_cn':  cn_name,
            'subtype_code':     '',
            'parts':            tagged,
        })

        logger.info("SubKategori '%s': %d parts (dari %d raw rows)",
                    cn_name, len(tagged), len(raw_parts))

    total_parts = sum(len(g['parts']) for g in output)
    logger.info(
        "Axle Drive extraction complete: %d SubKategori group(s), %d total parts",
        len(output), total_parts,
    )
    return output


# ─────────────────────────────────────────────────────────────────────────────
# Filename-based category inference (same logic as axle_drive_extractor.py)
# ─────────────────────────────────────────────────────────────────────────────

_FILENAME_CATEGORY_MAP = {
    "driveaxle":     ("Drive Axle",    "驱动桥"),
    "drive_axle":    ("Drive Axle",    "驱动桥"),
    "steeringaxle":  ("Steering Axle", "转向桥"),
    "steering_axle": ("Steering Axle", "转向桥"),
    "hdz":           ("Drive Axle",    "驱动桥"),   # Hande HDZ series
    "hande":         ("Drive Axle",    "驱动桥"),
}


def _infer_category_from_filename(pdf_path: str) -> Tuple[str, str]:
    """
    Derive category (EN, CN) from the PDF filename.
    Falls back to ("Drive Axle", "驱动桥") if nothing matches.
    """
    from pathlib import Path
    stem = Path(pdf_path).stem.lower().replace("-", "").replace(" ", "").replace("_", "")
    for key, names in _FILENAME_CATEGORY_MAP.items():
        if key in stem:
            return names
    logger.warning(
        "Cannot infer axle category from filename '%s' — defaulting to 'Drive Axle'",
        pdf_path,
    )
    return "Drive Axle", "驱动桥"


# ─────────────────────────────────────────────────────────────────────────────
# Stage 1: Category structure extraction (text-based)
# ─────────────────────────────────────────────────────────────────────────────

def extract_axle_drive_categories_text(
    pdf_path: str,
    sumopod_client=None,
    category_name_en: Optional[str] = None,
    category_name_cn: Optional[str] = None,
) -> Dict:
    """
    Stage 1 — Extract Kategori + TypeCategory structure dari PDF Hande Axle.

    STRATEGI (text-based, tanpa Vision AI):
    1. Buka PDF dengan PyMuPDF
    2. Abaikan halaman Cover dan Diagram
    3. Baca judul "表N <teks>" dari setiap halaman tabel
    4. Strip "(续)" → grup judul yang sama jadi satu TypeCategory
    5. Terjemahkan CN→EN (satu batch API call ke Sumopod)
    6. Return struktur kompatibel dengan batch_create_type_categories_and_categories()

    Args:
        pdf_path:         Path ke PDF axle drive.
        sumopod_client:   Opsional — hanya untuk terjemahan CN→EN.
                          Jika None, nama CN dipakai langsung sebagai EN.
        category_name_en: Override nama Kategori EN (default: dari filename).
        category_name_cn: Override nama Kategori CN (default: dari filename).

    Returns:
        {
          "categories": [
            {
              "category_name_en":     "Drive Axle",
              "category_name_cn":     "驱动桥",
              "category_description": "",
              "data_type": [
                {
                  "type_category_name_en":     "Drive Axle Final Reducer Assembly Parts List",
                  "type_category_name_cn":     "贯通式驱动桥主减速器总成爆炸图对应备件目录",
                  "type_category_description": ""
                },
                ...
              ]
            }
          ]
        }
    """
    logger.info("Axle Drive Stage 1 (text-based): opening '%s'", pdf_path)

    # Resolve category name from filename if not provided
    fn_en, fn_cn = _infer_category_from_filename(pdf_path)
    category_name_en = category_name_en or fn_en
    category_name_cn = category_name_cn or fn_cn

    doc         = fitz.open(pdf_path)
    total_pages = len(doc)
    logger.info("Total pages: %d  |  Category: '%s' / '%s'",
                total_pages, category_name_en, category_name_cn)

    # Collect unique SubKategori titles in document order
    # OrderedDict preserves insertion order; value = canonical CN title
    seen: OrderedDict[str, str] = OrderedDict()  # group_key → cn_title

    for page_idx in range(total_pages):
        page     = doc[page_idx]
        page_num = page_idx + 1

        if _is_cover_page(page):
            logger.info("Page %d: cover → skipped", page_num)
            continue

        if _is_diagram_page(page):
            logger.info("Page %d: diagram → skipped", page_num)
            continue

        title = _extract_table_title(page)
        
        if not title:
            snippet = page.get_text("text")[:100].replace('\n', ' ')
            logger.warning("Page %d: no table title → skipped | text: '%s'", page_num, snippet)
            continue

        group_key = _group_key_from_title(title)  # strip (续)
        if group_key not in seen:
            seen[group_key] = group_key
            logger.info("Page %d: new TypeCategory '%s'", page_num, group_key)
        else:
            logger.debug("Page %d: continuation of '%s'", page_num, group_key)

    doc.close()

    cn_titles = list(seen.keys())
    logger.info("Found %d unique TypeCategory title(s): %s", len(cn_titles), cn_titles)

    if not cn_titles:
        logger.warning("No table titles found in '%s' — Stage 1 result is empty", pdf_path)

    # Translate all CN titles to EN in one batch call
    translations = _translate_titles(cn_titles, sumopod_client)

    data_type = [
        {
            "type_category_name_en":     translations.get(cn, cn),
            "type_category_name_cn":     cn,
            "type_category_description": "",
        }
        for cn in cn_titles
    ]

    result = {
        "categories": [
            {
                "category_name_en":     category_name_en,
                "category_name_cn":     category_name_cn,
                "category_description": "",
                "data_type":            data_type,
            }
        ]
    }

    logger.info(
        "Axle Drive Stage 1 complete: 1 category ('%s'), %d TypeCategory entries",
        category_name_en, len(data_type),
    )
    return result