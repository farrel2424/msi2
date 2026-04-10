"""
axle_drive_parts_extractor.py
==============================
Stage 1 & Stage 2 — Text-based extractor untuk katalog suku cadang Axle Drive
(Shaanxi Hande Axle Co., Ltd.). TANPA Vision AI — membaca layer teks PDF langsung.

FUNGSI PUBLIK:
  extract_axle_drive_categories_text()  →  Stage 1: struktur Kategori + TypeCategory
  extract_axle_drive_parts()            →  Stage 2: detail suku cadang per SubKategori

PDF Hande Axle memiliki TEPAT 4 SubKategori:
  1. 贯通式驱动桥主减速器总成爆炸图对应备件目录        (~80 parts)
  2. 贯通式驱动桥桥壳总成爆炸图( STR悬架)对应备件目录  (~16 parts)
  3. 驱动桥轮边爆炸图对应备件目录                      (~48 parts)
  4. 驱动桥轮边总成爆炸图对应备件目录                  (~27 parts, 序号 49-76)

KOLOM TABEL:
  序号/Item | 汉德零件号/HanDe part nr. | English Description | 中文描述 | 数量/Qty | 备注/Remarks

PEMETAAN FIELD OUTPUT:
  数量 (Qty)    → quantity   (field `quantity` di parts)
  备注 (Remarks)→ keterangan (field `description` di parts)

PROSES HALAMAN: SEQUENTIAL (berurutan dari halaman 1 s.d. terakhir),
  BUKAN paralel/acak — agar urutan SubKategori dan nomor T-ID terjaga.

FIXES (vs versi sebelumnya):
  B. Halaman diproses URUT (range loop biasa, bukan ThreadPoolExecutor).
  C. 数量/Qty  → quantity   — threshold kolom diperlebar + fallback parser.
  D. 备注/Remarks → description — field `remarks` selalu diteruskan ke output.
  E. Halaman (续): jika judul mengandung (续)/(续), gunakan nama SubKategori
     SEBELUMNYA (bukan nama di judul itu sendiri). Ini menangani kasus di mana
     halaman lanjutan memiliki judul yang sedikit berbeda dari halaman utamanya.
  PLUS: _is_diagram_page() lebih konservatif agar halaman tabel tidak terlewat.
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

# Continuation marker at end of title: (续)、（续）、or bare 续 preceded by space
# Handles: "...目录(续)"  "...目录（续）"  "...目录 续"
_CONTINUATION_RE = re.compile(r'\s*[（(]续[）)]\s*$|\s+续\s*$', re.UNICODE)

# Part number pattern: alphanumeric, at least 6 chars
_PART_NUMBER_RE = re.compile(r'^[A-Z0-9][A-Z0-9\.\-]{4,}$', re.IGNORECASE)

# ── Column X-position thresholds (fraction of page width) ────────────────────
# Hande Axle catalog column layout (empirically tuned):
#   序号/Item | 汉德零件号/HanDe part nr. | English Desc | 中文描述 | 数量/Qty | 备注/Remarks
#
# FIX C: _COL_QTY_MAX raised from 0.91 → 0.93 to catch qty values that sit
#         slightly to the right; rem_w (备注) threshold adjusted accordingly.
_COL_ITEM_MAX   = 0.10   # 序号/Item        (leftmost)
_COL_PARTNO_MAX = 0.35   # 汉德零件号       (widened from 0.32 for safety)
_COL_EN_MAX     = 0.58   # English desc
_COL_CN_MAX     = 0.81   # 中文描述
_COL_QTY_MAX    = 0.93   # 数量/Qty  ← FIX C: was 0.91
# x > 0.93 → 备注/Remarks  → description

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

# Figure/diagram titles (used in diagram detection)
_FIGURE_TITLE_RE = re.compile(r'图\s*\d+', re.UNICODE)

# Signals that a page definitely contains a parts table
_TABLE_COLUMN_SIGNALS = ('序号', 'Item', '汉德零件号', 'HanDe', '数量', 'Qty')


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
    """
    True ONLY if this page is purely a diagram with NO parts table.

    FIX: More conservative than before — a page is only considered a diagram
    if ALL three conditions are true:
      1. No table title (表N ...) — hard guard
      2. No table-column signal words
      3. Has an image block AND very little text (< 200 chars)

    Previously threshold was 300 chars, which accidentally classified some
    short table pages (e.g. subcategory 2 with only ~16 parts) as diagrams.
    Lowered to 200 chars AND requiring both image + figure title.
    """
    text = page.get_text("text")

    # Hard guard 1: if the page has a table title, it's definitely NOT a diagram
    if _TABLE_TITLE_RE.search(text):
        return False

    # Hard guard 2: if the page has table column keywords, it's NOT a diagram
    if any(sig in text for sig in _TABLE_COLUMN_SIGNALS):
        return False

    blocks = page.get_text("dict")["blocks"]
    has_image = any(b.get("type") == 1 for b in blocks)

    # Only classify as diagram if: has image + figure title + very little text
    has_diagram_title = _FIGURE_TITLE_RE.search(text) is not None
    stripped_len = len(text.strip())

    return has_image and has_diagram_title and stripped_len < 200  # FIX: was 300


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
             (续 is stripped in _group_key_from_title, NOT here)

    Note: titles with (STR悬架) in the MIDDLE are preserved as-is —
    the continuation regex only strips (续) at the END.
    """
    text = page.get_text("text")
    m = _TABLE_TITLE_RE.search(text)
    if not m:
        return None
    return m.group(1).strip()


def _group_key_from_title(title: str) -> str:
    """
    Strip the continuation suffix (续) to get a canonical group key.

    "贯通式驱动桥主减速器总成爆炸图对应备件目录(续)"
        → "贯通式驱动桥主减速器总成爆炸图对应备件目录"

    "贯通式驱动桥桥壳总成爆炸图( STR悬架)对应备件目录"
        → "贯通式驱动桥桥壳总成爆炸图( STR悬架)对应备件目录"  ← preserved (不di-strip)
    """
    return _CONTINUATION_RE.sub('', title).strip()


# Detects (续)、（续）、or bare 续 preceded by space — anywhere in the title
_HAS_CONTINUATION_RE = re.compile(r'[（(]续[）)]|\s+续\s*$', re.UNICODE)


def _is_continuation_title(title: str) -> bool:
    """
    Return True if the title contains a continuation marker.
    Matches: (续)  （续）  or trailing space+续 (e.g. "...目录 续").

    FIX E: The caller strips (续) to get the base/canonical key, then checks
    whether that base name already exists in the group dict:
      - If yes  → merge into existing group (true continuation page)
      - If no   → create new group using the base name
        (the first page of this subcategory happens to carry a 续 marker)
    """
    return bool(_HAS_CONTINUATION_RE.search(title))


# ─────────────────────────────────────────────────────────────────────────────
# Table row parsing (word-level, column-aware)
# ─────────────────────────────────────────────────────────────────────────────

def _parse_table_rows(page: fitz.Page) -> List[Dict]:
    """
    Parse parts table rows from a table page using word X-positions.

    Words are grouped into rows by Y-coordinate (±4pt tolerance),
    then classified into columns by X-position fraction:

      x < 10%          → 序号/Item (serial number)
      10–35%           → 汉德零件号/HanDe part number
      35–58%           → English Description
      58–81%           → 中文描述 Chinese description
      81–93%           → 数量/Qty  → quantity        (FIX C)
      > 93%            → 备注/Remarks → description   (FIX D)

    Both 数量 and 备注 are always returned in the dict so the output
    builder can correctly map them to `quantity` and `description`.
    """
    W = page.rect.width
    if W == 0:
        return []

    # (x0, y0, x1, y1, word, block_no, line_no, word_no)
    words = page.get_text("words")
    if not words:
        return []

    # FIX B (secondary): group words into rows by Y — use 4pt tolerance
    # (was 3pt, relaxed slightly to handle slight baseline variations)
    _Y_TOL = 4
    rows_by_y: Dict[int, List] = {}
    for w in words:
        y_key = round(w[1] / _Y_TOL) * _Y_TOL
        rows_by_y.setdefault(y_key, []).append(w)

    parts = []
    for y_key in sorted(rows_by_y):   # ← sorted() ensures top-to-bottom order
        row_words = sorted(rows_by_y[y_key], key=lambda w: w[0])  # left-to-right

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
                qty_w.append(tok)   # FIX C: 数量 column
            else:
                rem_w.append(tok)   # FIX D: 备注 column

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

        # ── FIX C: Parse 数量/Qty → quantity ────────────────────────────────
        qty = None
        if qty_str and qty_str.lower() not in _VARIABLE_QTY:
            # Primary: leading integer
            m = re.match(r'^\d+', qty_str)
            if m:
                try:
                    qty = int(m.group())
                except ValueError:
                    pass
            # Fallback: if no leading integer found, try the whole string
            if qty is None:
                try:
                    qty = int(qty_str)
                except ValueError:
                    pass

        # ── FIX D: 备注/Remarks → remarks (will become description in output) ─
        parts.append({
            'serial_no':   item_str,
            'part_number': partno_str,
            'name_en':     en_str,
            'name_cn':     cn_str,
            'quantity':    qty,       # FIX C: 数量
            'remarks':     rem_str,   # FIX D: 备注 → output maps this to description
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
            # FIX D: preserve remarks from first occurrence (don't overwrite with empty)
            if not merged[key].get('remarks') and p.get('remarks'):
                merged[key]['remarks'] = p['remarks']
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
            max_tokens=1000,
            timeout=60,
        )
        raw = extract_response_text(resp)

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
# Public entry point — Stage 2
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

    PROSES HALAMAN: SEQUENTIAL (halaman 1 → terakhir), BUKAN paralel.
    Ini penting agar:
      - SubKategori muncul dalam urutan dokumen aslinya
      - Halaman continuation (续) digabungkan ke grup yang benar

    PEMETAAN KOLOM:
      数量 (Qty)    → field `quantity`    di output  (FIX C)
      备注 (Remarks)→ field `description` di output  (FIX D)

    Args:
        pdf_path:         Path to the axle drive partbook PDF.
        sumopod_client:   Optional — used only to translate SubKategori CN→EN.
                          If None, CN name is used as-is.
        target_id_start:  Ignored (T-IDs restart at T001 per SubKategori).
        code_to_category: Optional map SubKategori CN/EN → parent category name.
        custom_prompt:    Ignored (text-based; no prompt needed).

    Returns:
        List of SubKategori-group dicts compatible with batch_submit_parts().
        Expected: 4 groups for the standard Hande Axle Drive catalog.
    """
    logger.info("Axle Drive parts extractor (text-based, sequential): opening '%s'", pdf_path)

    doc         = fitz.open(pdf_path)
    total_pages = len(doc)
    logger.info("Total pages: %d", total_pages)

    # ── FIX B: Sequential loop — halaman 1 sampai terakhir ──────────────────
    groups: OrderedDict[str, Dict] = OrderedDict()

    for page_idx in range(total_pages):   # ← FIX B: always sequential
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

        # ── FIX E: resolve group key ─────────────────────────────────────────
        # Always strip (续) first to get the base/canonical name.
        # If the base name already exists → merge (continuation page).
        # If it does NOT exist yet → create new group with the base name.
        # This correctly handles:
        #   "贯通式驱动桥主减速器...（续）" → merges into existing group 1
        #   "驱动桥轮边总成...（续）"       → creates NEW group 4 (base name not seen before)
        group_key = _group_key_from_title(title)  # strips (续) suffix

        if _is_continuation_title(title):
            if group_key in groups:
                logger.info(
                    "Page %d: (续) detected, base='%s' found → merging",
                    page_num, group_key,
                )
            else:
                logger.info(
                    "Page %d: (续) detected, base='%s' NOT found → new SubKategori",
                    page_num, group_key,
                )

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
        logger.info(
            "Page %d: +%d rows (total %d in '%s')",
            page_num, len(rows), len(groups[group_key]['raw_parts']), group_key,
        )

    doc.close()

    logger.info(
        "SubKategori ditemukan: %d — %s",
        len(groups),
        list(groups.keys()),
    )

    if not groups:
        logger.warning("No table pages found in '%s'", pdf_path)
        return []

    # ── Translate SubKategori titles CN→EN ────────────────────────────────────
    cn_titles    = [grp['subtype_name_cn'] for grp in groups.values()]
    translations = _translate_titles(cn_titles, sumopod_client)

    # ── Build output ──────────────────────────────────────────────────────────
    output: List[Dict] = []

    for group_key, grp in groups.items():
        raw_parts = grp['raw_parts']
        cn_name   = grp['subtype_name_cn']

        stage1_en = (code_to_category or {}).get(cn_name, "")
        en_name   = stage1_en or translations.get(cn_name) or cn_name

        if stage1_en:
            logger.info(
                "SubKategori '%s': menggunakan nama Stage 1 '%s'", cn_name, stage1_en
            )

        if not raw_parts:
            logger.info("SubKategori '%s': no parts — skipped", cn_name)
            continue

        merged = _merge_parts(raw_parts)
        if not merged:
            logger.info("SubKategori '%s': all rows filtered — skipped", cn_name)
            continue

        # ── FIX C + D: quantity = 数量, description = 备注 ──────────────────
        tagged = [
            {
                'target_id':            f"T{i:03d}",
                'part_number':          p['part_number'],
                'catalog_item_name_en': p.get('name_en', ''),
                'catalog_item_name_ch': p.get('name_cn', ''),
                'quantity':             p.get('quantity'),    # FIX C: 数量
                'description':          p.get('remarks', ''), # FIX D: 备注
                'unit':                 '',
            }
            for i, p in enumerate(merged, start=1)
        ]

        # Resolve parent category
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

        logger.info(
            "SubKategori '%s' → '%s': %d parts (dari %d raw rows)",
            cn_name, en_name, len(tagged), len(raw_parts),
        )

    # ── Summary log ───────────────────────────────────────────────────────────
    total_parts = sum(len(g['parts']) for g in output)
    logger.info(
        "Axle Drive extraction complete: %d SubKategori group(s), %d total parts",
        len(output), total_parts,
    )
    for i, g in enumerate(output, 1):
        logger.info(
            "  [%d] '%s' → %d parts", i, g['subtype_name_cn'], len(g['parts'])
        )

    return output


# ─────────────────────────────────────────────────────────────────────────────
# Filename-based category inference
# ─────────────────────────────────────────────────────────────────────────────

_FILENAME_CATEGORY_MAP = {
    "driveaxle":     ("Drive Axle",    "驱动桥"),
    "drive_axle":    ("Drive Axle",    "驱动桥"),
    "steeringaxle":  ("Steering Axle", "转向桥"),
    "steering_axle": ("Steering Axle", "转向桥"),
    "hdz":           ("Drive Axle",    "驱动桥"),
    "hande":         ("Drive Axle",    "驱动桥"),
}


def _infer_category_from_filename(pdf_path: str) -> Tuple[str, str]:
    """Derive category (EN, CN) from the PDF filename."""
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

    PROSES HALAMAN: SEQUENTIAL (berurutan), bukan paralel.

    Mengembalikan struktur yang kompatibel dengan
    batch_create_type_categories_and_categories().
    """
    logger.info("Axle Drive Stage 1 (text-based, sequential): opening '%s'", pdf_path)

    fn_en, fn_cn = _infer_category_from_filename(pdf_path)
    category_name_en = category_name_en or fn_en
    category_name_cn = category_name_cn or fn_cn

    doc         = fitz.open(pdf_path)
    total_pages = len(doc)
    logger.info(
        "Total pages: %d  |  Category: '%s' / '%s'",
        total_pages, category_name_en, category_name_cn,
    )

    # ── FIX B: Sequential loop ───────────────────────────────────────────────
    seen: OrderedDict[str, str] = OrderedDict()  # group_key → cn_title

    for page_idx in range(total_pages):   # ← sequential
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
            snippet = page.get_text("text")[:80].replace('\n', ' ')
            logger.debug("Page %d: no table title → skipped | '%s'", page_num, snippet)
            continue

        # ── FIX E: strip (续) → use base name as key ─────────────────────
        # Same logic as Stage 2: base name may or may not exist yet.
        group_key = _group_key_from_title(title)

        if _is_continuation_title(title):
            action = "merging into existing" if group_key in seen else "new SubKategori (first page is a 续)"
            logger.info("Page %d: (续) detected, base='%s' → %s", page_num, group_key, action)

        if group_key not in seen:
            seen[group_key] = group_key
            logger.info("Page %d: new TypeCategory '%s'", page_num, group_key)
        else:
            logger.debug("Page %d: continuation of '%s'", page_num, group_key)

        last_seen_key = group_key  # keep for reference (informational only)

    doc.close()

    cn_titles = list(seen.keys())
    logger.info(
        "Found %d unique TypeCategory title(s): %s", len(cn_titles), cn_titles
    )

    if not cn_titles:
        logger.warning("No table titles found in '%s' — Stage 1 result is empty", pdf_path)

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