"""
axle_drive_parts_extractor.py
==============================
Stage 1 & Stage 2 — Text-based extractor untuk katalog suku cadang Axle Drive
(Shaanxi Hande Axle Co., Ltd.). TANPA Vision AI — membaca layer teks PDF langsung.

FUNGSI PUBLIK:
  extract_axle_drive_categories_text()  →  Stage 1: struktur Kategori + TypeCategory
  extract_axle_drive_parts()            →  Stage 2: detail suku cadang per SubKategori

KOLOM TABEL (6 kolom):
  序号/Item | 汉德零件号/HanDe part nr. | English | 中文 | 数量/Qty | 备注/Remarks

PEMETAAN FIELD OUTPUT:
  序号/Item        → target_id   (T001, T002, … berdasarkan nilai 序号)
  汉德零件号        → part_number
  English          → catalog_item_name_en
  中文             → catalog_item_name_ch
  数量 (Qty)       → quantity   (integer, "optional" jika 选用, "As Needed" jika 按需)
  备注 (Remarks)   → description

PROSES HALAMAN: SEQUENTIAL (berurutan dari halaman 1 s.d. terakhir)
  BUKAN paralel/acak — agar urutan SubKategori dan nomor T-ID terjaga.

REVISI (2026):
  A. Threshold kolom dikalibrasi ulang dari layout PDF asli:
       序号:     x < 6%   (sebelumnya 10%)
       汉德零件号: x < 26%  (sebelumnya 35%) ← root-cause bug utama
       English:  x < 57%  (sebelumnya 58%)
       中文:     x < 82%  (tidak berubah)
       数量:     x < 92%  (sebelumnya 93%)
       备注:     x > 92%
  B. Target ID diambil dari nilai 序号 (T001, T002, …), bukan counter sekuensial.
     Sub-baris (序号 kosong) mewarisi serial terakhir dengan suffix -2, -3, …
  C. Qty spesial: 选用 → "optional", 按需 → "As Needed" (bukan None).
  D. Baris sub-item (序号 kosong): target_id inherit dari serial terakhir + suffix.
"""

from __future__ import annotations

import json
import logging
import re
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Tuple

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
_CONTINUATION_RE = re.compile(r'\s*[（(]续[）)]\s*$|\s+续\s*$', re.UNICODE)

# Continuation marker anywhere in title
_HAS_CONTINUATION_RE = re.compile(r'[（(]续[）)]|\s+续\s*$', re.UNICODE)

# Part number pattern: alphanumeric, at least 5 chars, may contain dots/dashes
# Widened from {4,} to {3,} to catch short part numbers like "31313"
_PART_NUMBER_RE = re.compile(r'^[A-Z0-9][A-Z0-9\.\-]{3,}$', re.IGNORECASE)

# ── Column X-position thresholds (fraction of page width) ────────────────────
# Recalibrated from actual Hande Axle Drive PDF layout (A3 landscape, 987 pts wide):
#
#   Col             x-start  x-end   fraction-end
#   ─────────────────────────────────────────────
#   序号/Item           0       48       0.049
#   汉德零件号           48      212       0.215
#   English            212      516       0.523
#   中文               516      785       0.795
#   数量/Qty            785      877       0.889
#   备注/Remarks        877      987       1.000
#
# Safety margins (+0.01 to +0.04) added to each boundary:
_COL_ITEM_MAX   = 0.06   # 序号/Item        (was 0.10 — too wide, caused misclassification)
_COL_PARTNO_MAX = 0.26   # 汉德零件号        (was 0.35 — ROOT CAUSE BUG: EN words leaked in)
_COL_EN_MAX     = 0.57   # English desc     (was 0.58 — minor adjustment)
_COL_CN_MAX     = 0.82   # 中文描述          (unchanged)
_COL_QTY_MAX    = 0.92   # 数量/Qty         (was 0.93 — minor adjustment)
# x > 0.92 → 备注/Remarks → description

# Words that indicate a header row (skip these rows)
_HEADER_TOKENS = {
    '序号', 'item', '汉德零件号', 'hande', 'part', 'nr.', 'nr',
    '描述', 'description', '数量', 'qty', '备注', 'remarks',
    'english', '中文',
}

# ── Qty special-value mapping (REVISION C) ───────────────────────────────────
# Maps non-numeric qty cell values to canonical English strings.
# Key lookup is case-insensitive (values are checked after .lower()).
_QTY_SPECIAL_MAP: Dict[str, str] = {
    '选用':       'optional',
    'optional':  'optional',
    '按需':       'As Needed',
    'as needed': 'As Needed',
    'as-needed': 'As Needed',
}
# Values that mean "not applicable / unknown" → stored as None
_QTY_NONE_VALUES = {'ar', 'ref', '-', ''}

# Cover page signals — must have multiple of these to be classified as cover
_COVER_SIGNALS = {
    'spare', 'parts', '备件', '汉德编号', '制动器型式',
    'shaanxi', 'shaan', 'hande', 'axle', '汉德',
}

# Figure/diagram titles
_FIGURE_TITLE_RE = re.compile(r'图\s*\d+', re.UNICODE)

# Signals that a page definitely contains a parts table
_TABLE_COLUMN_SIGNALS = ('序号', 'Item', '汉德零件号', 'HanDe', '数量', 'Qty')


# ─────────────────────────────────────────────────────────────────────────────
# Page classification
# ─────────────────────────────────────────────────────────────────────────────

def _is_cover_page(page: fitz.Page) -> bool:
    """True if this is the cover page (Shaanxi Hande logo + Spare Parts)."""
    text = page.get_text("text")
    text_lower = text.lower()
    hits = sum(1 for sig in _COVER_SIGNALS if sig in text_lower)
    has_table_title = bool(_TABLE_TITLE_RE.search(text))
    return hits >= 3 and not has_table_title


def _is_diagram_page(page: fitz.Page) -> bool:
    """
    True ONLY if this page is purely a diagram with NO parts table.

    A page is classified as diagram only when ALL three conditions hold:
      1. No table title (表N ...) — hard guard
      2. No table-column signal words
      3. Has an image block AND very little text (< 200 chars)
    """
    text = page.get_text("text")

    if _TABLE_TITLE_RE.search(text):
        return False

    if any(sig in text for sig in _TABLE_COLUMN_SIGNALS):
        return False

    blocks = page.get_text("dict")["blocks"]
    has_image = any(b.get("type") == 1 for b in blocks)
    has_diagram_title = _FIGURE_TITLE_RE.search(text) is not None
    stripped_len = len(text.strip())

    return has_image and has_diagram_title and stripped_len < 200


# ─────────────────────────────────────────────────────────────────────────────
# Table title extraction
# ─────────────────────────────────────────────────────────────────────────────

def _extract_table_title(page: fitz.Page) -> Optional[str]:
    """
    Extract the SubKategori name from the table title line.

    Raw:     "表1 贯通式驱动桥主减速器总成爆炸图对应备件目录"
    Returns: "贯通式驱动桥主减速器总成爆炸图对应备件目录"
    """
    text = page.get_text("text")
    m = _TABLE_TITLE_RE.search(text)
    if not m:
        return None
    return m.group(1).strip()


def _group_key_from_title(title: str) -> str:
    """Strip continuation suffix (续) to get canonical group key."""
    return _CONTINUATION_RE.sub('', title).strip()


def _is_continuation_title(title: str) -> bool:
    """Return True if the title contains a continuation marker (续)."""
    return bool(_HAS_CONTINUATION_RE.search(title))


# ─────────────────────────────────────────────────────────────────────────────
# Qty parsing helper (REVISION C)
# ─────────────────────────────────────────────────────────────────────────────

def _parse_qty(qty_str: str) -> Any:
    """
    Parse a raw qty cell value into:
      - int           for normal numeric quantities
      - "optional"    when cell contains 选用
      - "As Needed"   when cell contains 按需
      - None          for blank / "ar" / "ref" / "-"

    Examples:
      "2"    → 2
      "选用"  → "optional"
      "按需"  → "As Needed"
      ""     → None
      "AR"   → None
    """
    raw = qty_str.strip()
    lower = raw.lower()

    # Special named values
    if lower in _QTY_SPECIAL_MAP:
        return _QTY_SPECIAL_MAP[lower]

    # Blank / non-applicable
    if lower in _QTY_NONE_VALUES:
        return None

    # Try leading integer (handles "2 pcs", "12", etc.)
    m = re.match(r'^\d+', raw)
    if m:
        try:
            return int(m.group())
        except ValueError:
            pass

    # Fallback: not parseable → None
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Dynamic column boundary detection (REVISION A)
# ─────────────────────────────────────────────────────────────────────────────

def _detect_col_boundaries(page: fitz.Page) -> Optional[Dict[str, float]]:
    """
    Detect column boundaries dynamically from the header row of the table.

    Reads x-positions of the known header keywords ("English", "中文", "Qty",
    "Remarks", "Item", "HanDe") and computes midpoints between adjacent
    columns as the column right-edge thresholds.

    This makes the extractor robust to different page widths and column
    proportions across different PDF editions of the same catalog series.

    Returns a dict mapping column names to their right-edge X fraction, or
    None if fewer than 4 headers are found (fallback to constants).
    """
    W = page.rect.width
    words = page.get_text("words")
    found: Dict[str, float] = {}

    # Chinese aliases → canonical column key
    _ALIASES = {'数量': 'Qty', '备注': 'Remarks', '序号': 'Item', '汉德零件号': 'HanDe'}
    _DIRECT  = {'English', '中文', 'Qty', 'Remarks', 'Item', 'HanDe'}

    for w in words:
        text = w[4].strip()
        if text in _DIRECT and text not in found:
            found[text] = w[0]
        elif text in _ALIASES and _ALIASES[text] not in found:
            found[_ALIASES[text]] = w[0]

    col_order = ['Item', 'HanDe', 'English', '中文', 'Qty', 'Remarks']
    positions = [(k, found[k]) for k in col_order if k in found]
    if len(positions) < 4:
        return None

    boundaries: Dict[str, float] = {}
    for i in range(len(positions) - 1):
        key_a, x_a = positions[i]
        _, x_b = positions[i + 1]
        boundaries[key_a] = (x_a + x_b) / 2 / W
    return boundaries


# ─────────────────────────────────────────────────────────────────────────────
# Table row parsing (word-level, column-aware) — REVISION A + B
# ─────────────────────────────────────────────────────────────────────────────

def _group_words_by_gap(words: List, y_gap: float = 6.0) -> List[List]:
    """
    Group words into logical rows by sorting on Y then splitting on Y-gaps.

    This replaces the old round-to-grid approach (round(y/tol)*tol) which
    was fragile because Chinese and Latin text in the same row have a
    consistent ~0.38pt baseline difference. That tiny gap would sometimes
    land exactly on a grid boundary, splitting one logical row into two.

    With gap-based grouping:
      • Same-row baseline difference (0.38pt) << y_gap (6pt)  → merged  ✅
      • Serial-number cell offset     (11.6pt) >  y_gap (6pt) → split   ✅
      • Adjacent row spacing          (~24pt)  >  y_gap (6pt) → split   ✅

    Returns a list of word groups, each sorted left-to-right by X.
    """
    if not words:
        return []
    sw = sorted(words, key=lambda w: (w[1], w[0]))
    rows: List[List] = []
    cur = [sw[0]]
    for w in sw[1:]:
        max_y_in_cur = max(c[1] for c in cur)
        if w[1] - max_y_in_cur > y_gap:
            rows.append(sorted(cur, key=lambda c: c[0]))
            cur = [w]
        else:
            cur.append(w)
    if cur:
        rows.append(sorted(cur, key=lambda c: c[0]))
    return rows


def _parse_table_rows(page: fitz.Page) -> List[Dict]:
    """
    Parse parts table rows from a table page.

    Column boundaries are detected dynamically from the header row via
    _detect_col_boundaries(). If detection fails, hardcoded fallback
    constants (_COL_*_MAX) are used.

    Words are grouped into logical rows using _group_words_by_gap() (y_gap=6)
    which handles the ~0.38pt Chinese/Latin baseline difference correctly.

    Post-processing: if a Qty-column word is not a short number or a known
    special value (选用/按需), it is moved to the Remarks bucket. This handles
    long Remarks text that starts just inside the Qty column boundary.

    Returns raw dicts with keys:
        serial_no, part_number, name_en, name_cn, quantity, remarks
    target_id is assigned later from serial_no by _assign_target_id_from_serial().
    """
    W = page.rect.width
    if W == 0:
        return []

    words = page.get_text("words")
    if not words:
        return []

    # Dynamic column boundaries, fallback to hardcoded constants
    bounds = _detect_col_boundaries(page) or {}
    item_max   = bounds.get('Item',    _COL_ITEM_MAX)
    partno_max = bounds.get('HanDe',   _COL_PARTNO_MAX)
    en_max     = bounds.get('English', _COL_EN_MAX)
    cn_max     = bounds.get('中文',     _COL_CN_MAX)
    qty_max    = bounds.get('Qty',     _COL_QTY_MAX)

    row_groups = _group_words_by_gap(words, y_gap=6.0)

    parts = []
    for row in row_groups:
        item_w, partno_w, en_w, cn_w, qty_w, rem_w = [], [], [], [], [], []

        for w in row:
            x = w[0] / W
            tok = w[4].strip()
            if not tok:
                continue
            if x < item_max:      item_w.append(tok)
            elif x < partno_max:  partno_w.append(tok)
            elif x < en_max:      en_w.append(tok)
            elif x < cn_max:      cn_w.append(tok)
            elif x < qty_max:     qty_w.append(tok)
            else:                 rem_w.append(tok)

        # ── Post-process qty: non-numeric / non-special text → Remarks ───────
        # Qty values are always short integers (1–4 digits) or special keywords.
        # Longer text that lands in the Qty column boundary (e.g. Remarks that
        # start just before qty_max) is moved to rem_w to preserve accuracy.
        real_qty: List[str] = []
        spill:    List[str] = []
        for tok in qty_w:
            if re.match(r'^\d{1,4}$', tok) or tok.lower() in _QTY_SPECIAL_MAP:
                real_qty.append(tok)
            else:
                spill.append(tok)
        qty_w = real_qty
        rem_w = spill + rem_w   # prepend spilled text to keep L→R order

        item_str   = " ".join(item_w).strip()
        partno_str = " ".join(partno_w).strip()
        en_str     = " ".join(en_w).strip()
        cn_str     = " ".join(cn_w).strip()
        qty_str    = " ".join(qty_w).strip()
        rem_str    = " ".join(rem_w).strip()

        # Skip header rows
        if item_str.lower() in {'序号', 'item'}:
            continue
        if partno_str.lower() in _HEADER_TOKENS:
            continue
        if not partno_str:
            continue

        # Part number must look valid (alphanumeric, ≥ 4 chars after first)
        if not _PART_NUMBER_RE.match(partno_str):
            continue

        parts.append({
            'serial_no':   item_str,
            'part_number': partno_str,
            'name_en':     en_str,
            'name_cn':     cn_str,
            'quantity':    _parse_qty(qty_str),
            'remarks':     rem_str,
        })

    return parts


# ─────────────────────────────────────────────────────────────────────────────
# Deduplication
# ─────────────────────────────────────────────────────────────────────────────

def _merge_parts(raw_parts: List[Dict]) -> List[Dict]:
    """
    Deduplicate by (part_number, name_cn); merge quantities for duplicates.

    Quantity merging rules:
      int + int   → sum
      str + any   → keep first (string special value like "optional" / "As Needed")
      None + X    → use X
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
            merged[key] = dict(p)   # preserves serial_no, remarks, etc.
        else:
            existing_qty = merged[key]['quantity']
            if isinstance(existing_qty, int) and isinstance(qty, int):
                merged[key]['quantity'] = existing_qty + qty
            elif existing_qty is None and qty is not None:
                merged[key]['quantity'] = qty
            # else: keep existing (string special value, or first-wins)

            # Fill missing name fields
            if not merged[key].get('name_en') and p.get('name_en'):
                merged[key]['name_en'] = p['name_en']
            if not merged[key].get('remarks') and p.get('remarks'):
                merged[key]['remarks'] = p['remarks']

    return list(merged.values())


# ─────────────────────────────────────────────────────────────────────────────
# Target-ID assignment from 序号 (REVISION B)
# ─────────────────────────────────────────────────────────────────────────────

def _assign_target_id_from_serial(merged: List[Dict]) -> List[Dict]:
    """
    Assign target_id to each part based on its 序号/serial_no value.

    Rules:
      • 序号 = "1", "2", … → target_id = "T001", "T002", …
      • 序号 blank/empty (sub-row sharing the previous item's number):
            → inherit last serial number, append "-2", "-3", … suffix
      • Non-numeric 序号 (e.g. bracket items "(1)"):
            → extract digits and format as T{n:03d}

    Example:
      序号 38 row 1 → T038
      序号 ""  row 2 → T038-2    (same assembly item, different variant)

    This function adds a 'target_id' key to each dict and returns the list.
    """
    last_serial: int = 0
    sub_counters: Dict[int, int] = {}   # serial → sub-row count beyond first

    output = []
    for p in merged:
        result = dict(p)
        raw = str(p.get('serial_no') or '').strip()

        # Extract digits from the raw serial value
        digits = re.sub(r'\D', '', raw)

        if digits:
            sn = int(digits)
            last_serial = sn
            # Reset sub-counter for this serial
            sub_counters[sn] = sub_counters.get(sn, 0) + 1
            count = sub_counters[sn]
            if count == 1:
                result['target_id'] = f"T{sn:03d}"
            else:
                result['target_id'] = f"T{sn:03d}-{count}"
        else:
            # Blank serial_no: sub-row inherits last serial
            if last_serial > 0:
                sub_counters[last_serial] = sub_counters.get(last_serial, 0) + 1
                count = sub_counters[last_serial]
                if count == 1:
                    result['target_id'] = f"T{last_serial:03d}"
                else:
                    result['target_id'] = f"T{last_serial:03d}-{count}"
            else:
                # No serial seen yet (shouldn't normally happen) — sequential fallback
                result['target_id'] = f"T{len(output) + 1:03d}"

        output.append(result)

    return output


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
    target_id_start: int = 1,          # kept for API compat; T-IDs come from 序号
    code_to_category: Optional[Dict[str, str]] = None,
    subtype_name_map: Optional[Dict[str, str]] = None,
    custom_prompt: Optional[str] = None,   # kept for API compat; not used (text-based)
) -> List[Dict]:
    """
    Extract all parts from a Shaanxi Hande Axle Drive partbook PDF.

    Pure text extraction — no Vision AI calls, no network cost.

    PROSES HALAMAN: SEQUENTIAL (halaman 1 → terakhir), BUKAN paralel.

    PEMETAAN KOLOM (REVISION A):
      序号/Item       → target_id    (T001 dst. dari nilai 序号)
      汉德零件号       → part_number
      English         → catalog_item_name_en
      中文            → catalog_item_name_ch
      数量 (Qty)      → quantity    (int / "optional" / "As Needed" / None)
      备注 (Remarks)  → description

    Args:
        pdf_path:         Path to the axle drive partbook PDF.
        sumopod_client:   Optional — used only to translate SubKategori CN→EN.
        target_id_start:  Legacy param, ignored. T-IDs derived from 序号.
        code_to_category: Optional map SubKategori CN/EN → parent category name.
        custom_prompt:    Ignored (text-based; no prompt needed).

    Returns:
        List of SubKategori-group dicts compatible with batch_submit_parts().
    """
    logger.info(
        "Axle Drive parts extractor (text-based, sequential, rev2): opening '%s'",
        pdf_path,
    )

    doc         = fitz.open(pdf_path)
    total_pages = len(doc)
    logger.info("Total pages: %d", total_pages)

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

        # ── Continuation pages: always merge into PREVIOUS subcategory ──────
        if _is_continuation_title(title):
            if groups:
                group_key = list(groups.keys())[-1]
                logger.info(
                    "Page %d: (续) detected → merging into previous SubKat '%s'",
                    page_num, group_key,
                )
            else:
                group_key = _group_key_from_title(title)
                logger.warning(
                    "Page %d: (续) but no previous SubKat yet → fallback to '%s'",
                    page_num, group_key,
                )
        else:
            group_key = title.strip()

        if group_key not in groups:
            groups[group_key] = {
                'subtype_name_cn': group_key,
                'raw_parts':       [],
            }
            logger.info("Page %d: new SubKategori '%s'", page_num, group_key)
        else:
            logger.info("Page %d: appending to '%s'", page_num, group_key)

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

    fn_en, _ = _infer_category_from_filename(pdf_path)

    for group_key, grp in groups.items():
        raw_parts = grp['raw_parts']
        cn_name   = grp['subtype_name_cn']

        # Prefer Stage 1 EN name; fallback to translation or CN
        stage1_en = (subtype_name_map or {}).get(cn_name, "")
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

        # ── REVISION B: Target-ID from 序号 value ────────────────────────────
        tagged_with_tid = _assign_target_id_from_serial(merged)

        tagged = [
            {
                'target_id':            p['target_id'],
                'part_number':          p['part_number'],
                'catalog_item_name_en': p.get('name_en', ''),
                'catalog_item_name_ch': p.get('name_cn', ''),
                'quantity':             p.get('quantity'),    # int / str / None
                'description':          p.get('remarks', ''), # 备注 → description
                'unit':                 '',
            }
            for p in tagged_with_tid
        ]

        # Resolve parent category from code_to_category map
        cat_map = code_to_category or {}
        fn_en, _ = _infer_category_from_filename(pdf_path)
        cat_en  = cat_map.get(cn_name) or cat_map.get(en_name) or fn_en

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
    logger.info(
        "Axle Drive Stage 1 (text-based, sequential, rev2): opening '%s'", pdf_path
    )

    fn_en, fn_cn = _infer_category_from_filename(pdf_path)
    category_name_en = category_name_en or fn_en
    category_name_cn = category_name_cn or fn_cn

    doc         = fitz.open(pdf_path)
    total_pages = len(doc)
    logger.info(
        "Total pages: %d  |  Category: '%s' / '%s'",
        total_pages, category_name_en, category_name_cn,
    )

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
            logger.debug("Page %d: no table title → skipped", page_num)
            continue

        # Continuation pages: merge into previous subcategory
        if _is_continuation_title(title):
            if seen:
                group_key = list(seen.keys())[-1]
                logger.info(
                    "Page %d: (续) detected → continuation of previous '%s'",
                    page_num, group_key,
                )
            else:
                group_key = _group_key_from_title(title)
                logger.warning(
                    "Page %d: (续) but no previous SubKat yet → fallback to '%s'",
                    page_num, group_key,
                )
        else:
            group_key = title.strip()

        if group_key not in seen:
            seen[group_key] = group_key
            logger.info("Page %d: new TypeCategory '%s'", page_num, group_key)
        else:
            logger.debug("Page %d: continuation of '%s'", page_num, group_key)

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