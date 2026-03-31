"""
engine_parts_extractor.py
─────────────────────────────────────────────────────────────────────────────
Stage 2 — Parts Extractor untuk Engine partbook.

STRATEGI OTOMATIS (auto-detect):
─────────────────────────────────
Program ini memeriksa PDF terlebih dahulu sebelum memilih strategi:

  ┌─────────────────────────────────────────────────────────────┐
  │  PDF punya teks?  →  YA  →  Ekstrak teks langsung (CEPAT) │
  │  (text-based PDF)           parse layout → gratis           │
  │                                                             │
  │  PDF punya teks?  →  TIDAK →  Vision AI per halaman (LAMBAT)│
  │  (image/scan PDF)             pakai token AI → berbayar     │
  └─────────────────────────────────────────────────────────────┘

Deteksi dilakukan dengan menghitung karakter teks di halaman pertama:
  > 50 karakter  →  text-based PDF  →  TEXT PATH
  <= 50 karakter →  image-based PDF →  VISION PATH

FORMAT PDF ENGINE (Cummins / serupa):
─────────────────────────────────────
  Pojok KIRI ATAS  : kode halaman, mis. "FH 2471"
  Pojok KANAN ATAS : label bilingual, mis. "飞轮壳HOUSING,FLYWHEEL"
  Tengah           : diagram exploded-view
  Tabel parts      : kolom Item | Part Number | Name EN | Name CN | Qty

OUTPUT (kompatibel dengan batch_submit_parts):
──────────────────────────────────────────────
  [
    {
      "category_name_en": "Housing Flywheel",
      "category_name_cn": "飞轮壳",
      "subtype_name_en":  "Housing Flywheel",
      "subtype_name_cn":  "飞轮壳",
      "subtype_code":     "",
      "parts": [
        {
          "target_id":            "T001",
          "part_number":          "127316X",
          "catalog_item_name_en": "WASHER, PLAIN",
          "catalog_item_name_ch": "平垫圈",
          "quantity":             12,
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
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_TEXT_THRESHOLD = 50  # chars minimum for text-based PDF detection


# ─────────────────────────────────────────────────────────────────────────────
# Auto-detect
# ─────────────────────────────────────────────────────────────────────────────

def _is_text_based_pdf(pdf_path: str, sample_pages: int = 3) -> bool:
    """
    True jika PDF memiliki layer teks yang bisa diekstrak (tidak perlu Vision AI).
    """
    import fitz
    try:
        doc = fitz.open(pdf_path)
        total_chars = sum(
            len(doc[i].get_text("text").strip())
            for i in range(min(sample_pages, len(doc)))
        )
        doc.close()
        is_text = total_chars > _TEXT_THRESHOLD
        logger.info(
            "PDF detection: %d chars → %s",
            total_chars,
            "TEXT PATH" if is_text else "VISION PATH",
        )
        return is_text
    except Exception as exc:
        logger.warning("PDF detection failed: %s — defaulting to VISION PATH", exc)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Label parser: "飞轮壳HOUSING,FLYWHEEL" → (en, cn)
# ─────────────────────────────────────────────────────────────────────────────

def _parse_bilingual_label(raw: str) -> Tuple[str, str]:
    """
    Pisahkan label bilingual engine menjadi (English, Chinese).

    Format: <Chinese><English>
      "飞轮壳HOUSING,FLYWHEEL"  → ("Housing Flywheel", "飞轮壳")
    """
    raw = (raw or "").strip()
    if not raw:
        return "", ""

    match = re.search(r"([\u4e00-\u9fff\u3000-\u303f])([\x21-\x7E])", raw)
    if not match:
        if re.search(r"[\u4e00-\u9fff]", raw):
            return "", raw.strip()
        return raw.strip(), ""

    split_idx = match.start() + 1
    cn_part   = raw[:split_idx].strip()
    en_raw    = raw[split_idx:].strip()
    en_clean  = " ".join(
        w.capitalize() for w in en_raw.replace(",", " ").split() if w
    )
    return en_clean, cn_part


# ─────────────────────────────────────────────────────────────────────────────
# PATH A: TEXT-BASED PDF — ekstrak via PyMuPDF text blocks
# ─────────────────────────────────────────────────────────────────────────────

def _extract_parts_from_text(pdf_path: str) -> List[Dict]:
    """
    Ekstrak parts dari PDF yang memiliki layer teks.
    Tidak ada panggilan AI — sepenuhnya gratis dan cepat.

    Strategi per halaman:
    1. Ambil semua text blocks dengan koordinat (x, y)
    2. Blok di pojok kanan atas (x > 40%, y < 15%) = label kategori
    3. Blok di bawahnya = baris tabel, di-parse berdasarkan posisi x
    """
    import fitz

    logger.info("TEXT PATH: extracting via PyMuPDF text layout")

    doc = fitz.open(pdf_path)
    groups: OrderedDict[str, Dict] = OrderedDict()

    for page_idx in range(len(doc)):
        page = doc[page_idx]
        blocks = page.get_text("blocks")  # (x0, y0, x1, y1, text, block_no, type)

        if not blocks:
            continue

        W = page.rect.width
        H = page.rect.height

        # ── Label dari pojok kanan atas ──────────────────────────────────────
        right_top = [
            b for b in blocks
            if b[0] > W * 0.4 and b[1] < H * 0.15 and b[6] == 0
        ]
        if not right_top:
            continue

        raw_label = sorted(right_top, key=lambda b: (b[1], -b[0]))[0][4]
        raw_label = raw_label.strip().replace("\n", " ")

        en, cn = _parse_bilingual_label(raw_label)
        key = cn or en
        if not key:
            continue

        if key not in groups:
            groups[key] = {"category_name_en": en, "category_name_cn": cn, "raw_parts": []}
            logger.info("Page %d: category '%s' / '%s'", page_idx + 1, en, cn)

        # ── Parse tabel (blok di bawah area header) ──────────────────────────
        table_blocks = [b for b in blocks if b[1] > H * 0.12 and b[6] == 0]
        parts = _parse_table_from_blocks(table_blocks, W)
        if parts:
            groups[key]["raw_parts"].extend(parts)
            logger.info("Page %d: +%d parts → '%s'", page_idx + 1, len(parts), en)

    doc.close()
    return _finalize_groups(groups)


def _parse_table_from_blocks(
    blocks: List[tuple],
    page_width: float,
    y_tol: float = 5.0,
) -> List[Dict]:
    """
    Parse baris-baris tabel dari kumpulan text blocks.

    Kelompokkan blok ke "baris" berdasarkan koordinat Y yang berdekatan (±5pt),
    lalu interpretasikan kolom berdasarkan posisi X relatif terhadap lebar halaman:
      x < 12%          → item/serial number
      12–45%           → part number
      45–72%           → nama pertama (EN atau CN)
      72–90%           → nama kedua
      > 75% (kanan)    → quantity (angka)
    """
    if not blocks:
        return []

    SKIP_WORDS = {
        "item", "part", "number", "name", "qty", "quantity", "no", "no.",
        "ref", "description", "序号", "件号", "零件号", "名称", "数量", "备注",
    }

    # Sort dan group ke baris
    sorted_b = sorted(blocks, key=lambda b: (round(b[1] / y_tol), b[0]))
    rows: List[List[tuple]] = []
    cur_row: List[tuple] = []
    cur_y = None

    for b in sorted_b:
        y0 = b[1]
        if cur_y is None or abs(y0 - cur_y) <= y_tol:
            cur_row.append(b)
            cur_y = y0 if cur_y is None else min(cur_y, y0)
        else:
            if cur_row:
                rows.append(sorted(cur_row, key=lambda x: x[0]))
            cur_row = [b]
            cur_y = y0
    if cur_row:
        rows.append(sorted(cur_row, key=lambda x: x[0]))

    raw_parts = []
    for row in rows:
        if len(row) < 2:
            continue

        cols = [(b[0] / page_width, b[4].strip().replace("\n", " ")) for b in row]

        item_no = part_number = name_en = name_cn = ""
        qty = None
        is_header = False

        for x_ratio, text in cols:
            t = text.strip()
            if not t:
                continue
            tl = t.lower()

            if tl in SKIP_WORDS:
                is_header = True
                break

            if x_ratio < 0.12 and re.match(r"^\d{1,3}[A-Z]?$", t):
                item_no = t
            elif re.match(r"^[A-Z0-9][A-Z0-9\-\.]{3,}$", t) and not part_number and tl not in SKIP_WORDS:
                part_number = t
            elif x_ratio > 0.75 and re.match(r"^\d+$", t):
                try:
                    qty = int(t)
                except ValueError:
                    pass
            elif re.search(r"[\u4e00-\u9fff]", t):
                name_cn = t
            elif re.match(r"^[A-Z][A-Z0-9\s,\-\.\/\(\)]{2,}$", t) and tl not in SKIP_WORDS:
                name_en = t

        if not is_header and part_number:
            raw_parts.append({
                "item_no":     item_no,
                "part_number": part_number,
                "name_en":     name_en,
                "name_cn":     name_cn,
                "quantity":    qty,
            })

    return raw_parts


# ─────────────────────────────────────────────────────────────────────────────
# PATH B: IMAGE-BASED PDF — Vision AI per halaman
# ─────────────────────────────────────────────────────────────────────────────

_ENGINE_PARTS_VISION_PROMPT = """\
You are a precise data-extraction engine for an engine parts catalog page image.

PAGE LAYOUT:
  TOP-LEFT  : short page code (e.g. "FH 2471")
  TOP-RIGHT : bilingual label — Chinese chars immediately followed by English
              e.g. "飞轮壳HOUSING,FLYWHEEL"  /  "缸体管路PLUMBING,CYLINDER BLOCK"
  CENTER    : exploded-view diagram with circled reference numbers
  BOTTOM    : parts table

PARTS TABLE COLUMNS (may vary):
  Item | Part Number | Name (English) | Name (Chinese) | Qty
  or
  Item | Part Number | Name (Chinese) | Name (English) | Qty

PAGE TYPES:
  "table"   — has a parts table (may also have diagram)
  "diagram" — diagram only, no parts table
  "skip"    — cover, TOC, or blank

RULES:
  - Return EXACT top-right text as category_label_raw (do NOT split it).
  - Extract ONLY rows where Part Number is non-empty.
  - item_no: reference number string, or null.
  - quantity: integer. "AR"/"REF" cells -> null.
  - Blank cell -> empty string or null.
  - Return ONLY valid JSON — no markdown fences.

OUTPUT (table page):
{
  "page_type": "table",
  "category_label_raw": "<exact top-right bilingual text>",
  "page_code": "<top-left code>",
  "parts": [
    {
      "item_no": "<string or null>",
      "part_number": "<code>",
      "name_en": "<ENGLISH NAME>",
      "name_cn": "<Chinese name or empty>",
      "quantity": <integer or null>
    }
  ]
}

OUTPUT (diagram): {"page_type": "diagram", "category_label_raw": "<or null>"}
OUTPUT (skip):    {"page_type": "skip"}
"""


def _vision_call(b64: str, sumopod_client, detail: str = "high",
                 system_prompt: Optional[str] = None) -> Optional[Dict]:
    """Kirim satu halaman ke Vision AI dan parse hasilnya."""
    from pdf_utils import extract_response_text

    raw = ""
    _prompt = system_prompt or _ENGINE_PARTS_VISION_PROMPT
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
                            "image_url": {"url": f"data:image/jpeg;base64,{b64}", "detail": detail},
                        },
                        {"type": "text", "text": "Extract parts data. Follow the system prompt."},
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
            raw = re.sub(r"\n?```$",           "", raw)
        return json.loads(raw.strip())
    except json.JSONDecodeError as exc:
        logger.warning("Vision AI non-JSON: %s … (%s)", raw[:200], exc)
        return None
    except Exception as exc:
        logger.warning("Vision AI call failed: %s", exc)
        return None


def _extract_parts_from_vision(
    pdf_path: str,
    sumopod_client,
    vision_detail: str = "high",
    max_workers: int = 5,
    custom_prompt: Optional[str] = None,
) -> List[Dict]:
    """Ekstrak parts dari image-based PDF menggunakan Vision AI (paralel)."""
    from pdf_utils import pdf_page_to_base64
    import fitz

    logger.info("VISION PATH: extracting via Vision AI")

    doc = fitz.open(pdf_path)
    total = len(doc)
    doc.close()

    page_b64s: Dict[int, str] = {}
    for idx in range(total):
        try:
            page_b64s[idx] = pdf_page_to_base64(pdf_path, idx, dpi=150)
        except Exception as exc:
            logger.warning("Page %d render failed: %s", idx + 1, exc)

    def _process(idx: int) -> Tuple[int, Optional[Dict]]:
        result = _vision_call(page_b64s[idx], sumopod_client, detail=vision_detail, system_prompt=custom_prompt)
        logger.info(
            "Page %d → %s | label: %s | parts: %d",
            idx + 1,
            (result or {}).get("page_type", "none"),
            (result or {}).get("category_label_raw", "—"),
            len((result or {}).get("parts", [])),
        )
        return idx, result

    page_results: Dict[int, Optional[Dict]] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_process, idx): idx for idx in page_b64s}
        for future in as_completed(futures):
            idx, result = future.result()
            page_results[idx] = result

    groups: OrderedDict[str, Dict] = OrderedDict()
    for idx in sorted(page_results):
        result = page_results[idx]
        if not result:
            continue
        page_type = result.get("page_type", "")
        label_raw = (result.get("category_label_raw") or "").strip()
        if not label_raw or page_type == "skip":
            continue
        en, cn = _parse_bilingual_label(label_raw)
        key = cn or en
        if not key:
            continue
        if key not in groups:
            groups[key] = {"category_name_en": en, "category_name_cn": cn, "raw_parts": []}
        if page_type == "table":
            groups[key]["raw_parts"].extend(result.get("parts", []))

    return _finalize_groups(groups)


# ─────────────────────────────────────────────────────────────────────────────
# Shared: dedup, T-ID, output format
# ─────────────────────────────────────────────────────────────────────────────

def _merge_parts(raw_parts: List[Dict]) -> List[Dict]:
    merged: OrderedDict = OrderedDict()
    for p in raw_parts:
        pn = (p.get("part_number") or "").strip()
        if not pn:
            continue
        cn  = (p.get("name_cn") or "").strip()
        key = (pn, cn)
        qty = p.get("quantity")
        if key not in merged:
            merged[key] = {"part_number": pn, "name_en": (p.get("name_en") or "").strip(), "name_cn": cn, "quantity": qty}
        else:
            eq = merged[key]["quantity"]
            if eq is not None and qty is not None:
                merged[key]["quantity"] = eq + qty
    return list(merged.values())


def _finalize_groups(groups: OrderedDict) -> List[Dict]:
    output: List[Dict] = []
    for _, grp in groups.items():
        en, cn = grp["category_name_en"], grp["category_name_cn"]
        merged = _merge_parts(grp["raw_parts"])
        tagged = [
            {
                "target_id":            f"T{i:03d}",
                "part_number":          p["part_number"],
                "catalog_item_name_en": p.get("name_en", ""),
                "catalog_item_name_ch": p.get("name_cn", ""),
                "quantity":             p.get("quantity"),
                "description":          "",
                "unit":                 "",
            }
            for i, p in enumerate(merged, start=1)
        ]
        output.append({
            "category_name_en": en,
            "category_name_cn": cn,
            "subtype_name_en":  en,
            "subtype_name_cn":  cn,
            "subtype_code":     "",
            "parts":            tagged,
        })
        logger.info("Category '%s': %d parts (dari %d raw rows)", en, len(tagged), len(grp["raw_parts"]))

    logger.info("Done: %d kategori, %d total parts.", len(output), sum(len(g["parts"]) for g in output))
    return output


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def extract_engine_parts(
    pdf_path: str,
    sumopod_client=None,
    target_id_start: int = 1,
    dpi: int = 150,
    vision_detail: str = "high",
    max_workers: int = 5,
    force_vision: bool = False,
    force_text: bool = False,
    custom_prompt: Optional[str] = None, 
) -> List[Dict]:
    """
    Ekstrak semua parts dari Engine partbook PDF.

    OTOMATIS mendeteksi strategi terbaik:
      - PDF punya teks → text extraction (cepat, gratis)
      - PDF image-based → Vision AI per halaman (butuh sumopod_client)

    Args:
        pdf_path:         Path ke PDF engine partbook.
        sumopod_client:   SumopodClient (wajib untuk image-based PDF).
        target_id_start:  Tidak dipakai langsung — T-ID per kategori selalu
                          mulai dari T001. Parameter ini disiapkan untuk
                          konsistensi API dengan extractor lain.
        dpi:              Resolusi render untuk image-based PDF.
        vision_detail:    "high" atau "low" untuk Vision AI.
        max_workers:      Thread paralel untuk Vision AI.
        force_vision:     Paksa Vision AI walau PDF punya teks.
        force_text:       Paksa text extraction walau PDF terdeteksi image-based.

    Returns:
        List of category-group dicts (lihat docstring modul di atas).
    """
    logger.info("extract_engine_parts: '%s'", pdf_path)

    if force_text:
        use_vision = False
    elif force_vision:
        use_vision = True
    else:
        use_vision = not _is_text_based_pdf(pdf_path)

    if use_vision:
        if sumopod_client is None:
            raise ValueError(
                "sumopod_client wajib untuk image-based PDF. "
                "Atau paksa dengan force_text=True jika yakin PDF punya layer teks."
            )
        return _extract_parts_from_vision(
            pdf_path=pdf_path,
            sumopod_client=sumopod_client,
            vision_detail=vision_detail,
            max_workers=max_workers,
            custom_prompt=custom_prompt,
        )
    else:
        return _extract_parts_from_text(pdf_path)


# ─────────────────────────────────────────────────────────────────────────────
# CLI Runner
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse, sys, os

    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stderr)],
    )

    parser = argparse.ArgumentParser(
        description="Engine Parts Extractor — auto-detect text vs Vision AI",
        epilog="""
Contoh:
  python engine_parts_extractor.py --pdf engine.pdf --dry-run
  python engine_parts_extractor.py --pdf engine.pdf --force-text --dry-run
  python engine_parts_extractor.py --pdf engine.pdf --force-vision --dry-run
  python engine_parts_extractor.py --pdf engine.pdf --save-json hasil.json
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--pdf",          required=True)
    parser.add_argument("--dry-run",      action="store_true")
    parser.add_argument("--save-json",    default=None)
    parser.add_argument("--model",        default=None)
    parser.add_argument("--detail",       default="high", choices=["high", "low"])
    parser.add_argument("--force-vision", action="store_true")
    parser.add_argument("--force-text",   action="store_true")
    args = parser.parse_args()

    sumopod = None
    api_key = os.getenv("SUMOPOD_API_KEY", "")
    if api_key:
        try:
            from sumopod_client import SumopodClient
            sumopod = SumopodClient(
                base_url=os.getenv("SUMOPOD_BASE_URL", "https://ai.sumopod.com/v1"),
                api_key=api_key,
                model=args.model or os.getenv("SUMOPOD_MODEL", "gpt-4o"),
            )
            print(f"✅ Sumopod client ready")
        except ImportError:
            print("⚠️  sumopod_client.py tidak ditemukan.")
    else:
        print("⚠️  SUMOPOD_API_KEY tidak di-set — hanya text-based PDF bisa diproses.")

    results = extract_engine_parts(
        pdf_path=args.pdf,
        sumopod_client=sumopod,
        vision_detail=args.detail,
        force_vision=args.force_vision,
        force_text=args.force_text,
    )

    if args.save_json:
        import pathlib
        pathlib.Path(args.save_json).write_text(
            json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        print(f"\n✅ Hasil disimpan: {args.save_json}")

    print(f"\n{'='*60}\nHASIL EKSTRAKSI\n{'='*60}")
    print(f"Kategori: {len(results)}  |  Total Parts: {sum(len(g['parts']) for g in results)}\n")
    for g in results:
        print(f"  📁 {g['category_name_en']} — {g['category_name_cn']}  ({len(g['parts'])} parts)")
        for p in g["parts"][:5]:
            print(f"     {p['target_id']} | {p['part_number']:15s} | qty:{str(p['quantity'] or '?'):>4} | {p['catalog_item_name_en'][:28]:28s} | {p['catalog_item_name_ch']}")
        if len(g["parts"]) > 5:
            print(f"     ... +{len(g['parts'])-5} lainnya")
        print()