"""
engine_transmission_extractor.py  — with Weichai 3-level support
=================================================================
Changelog vs previous version:
  NEW: extract_weichai_engine_toc()
  NEW: is_weichai_bilingual_toc()
  CHANGED: extract_engine_or_transmission()

FIX (2026-04-10):
  _split_bilingual_label() — filter garbled EN tokens (slash, Chinese chars
    that leaked into the EN portion) so that e.g. "/批准APPROVAL,AGENCY"
    still produces "Approval Agency" instead of "/批准Approval Agency".
  _process_engine_pages() — added EN-based deduplication on top of the
    existing CN-based dedup, so two pages whose label was mis-read with
    different CN but identical EN are not stored as separate categories.
"""

import json
import logging
import re
import shutil
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple


from pdf_utils import (
    extract_response_text,
    extract_zip_pdf,
    image_to_base64,
    is_zip_pdf,
    parse_llm_json,
    pdf_page_to_base64,
)

logger = logging.getLogger(__name__)


# ===========================================================================
# ── WEICHAI BILINGUAL TOC EXTRACTOR (text-based, no AI) ───────────────────
# ===========================================================================

_X_CATEGORY_MAX = 50
_WEICHAI_SKIP = ["wangmd", "2023/", "shacman.com", "zhangzhi", "CONTENTS", "目录"]


def _is_weichai_skip(text: str) -> bool:
    return any(p in text for p in _WEICHAI_SKIP)


def _clean_en_label(en: str) -> str:
    en = re.sub(r"[()]", "", en)
    parts = en.split()
    deduped: List[str] = []
    for p in parts:
        if deduped and deduped[-1].upper() == p.upper():
            continue
        deduped.append(p)
    return " ".join(deduped).strip()


def is_weichai_bilingual_toc(pdf_path: str, sample_pages: int = 2) -> bool:
    try:
        import fitz
        doc = fitz.open(pdf_path)
        bold_hits = 0
        cn_hits = 0

        for page_idx in range(min(sample_pages, len(doc))):
            page = doc[page_idx]
            for block in page.get_text("dict")["blocks"]:
                if block.get("type") != 0:
                    continue
                for line in block.get("lines", []):
                    for span in line.get("spans", []):
                        font = span.get("font", "")
                        text = span["text"].strip()
                        if "BoldMT" in font or "Bold" in font:
                            bold_hits += 1
                        if re.search(r"[\u4e00-\u9fff]", text):
                            cn_hits += 1

        doc.close()
        result = bold_hits >= 3 and cn_hits >= 5
        logger.info(
            "is_weichai_bilingual_toc('%s'): bold_hits=%d, cn_hits=%d → %s",
            Path(pdf_path).name, bold_hits, cn_hits, result,
        )
        return result

    except Exception as exc:
        logger.warning("is_weichai_bilingual_toc failed: %s", exc)
        return False


import re
import logging
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_X_CATEGORY_MAX = 50

_WEICHAI_SKIP = [
    "wangmd", "2023/", "2024/", "shacman.com", "zhangzhi",
    "任志军", "renzj", "CONTENTS", "目录",
]

_TOC_START_KEYWORDS = ("目录", "CONTENTS")

_BOUNDARY_SIGNALS = (
    "WP10 SERIES ENGINE PARTS CATALOGUE",
    "图序号",
    "Pos.",
    "Part Number",
    "件号",
)

_DIAGRAM_IMAGE_THRESHOLD = 1
_DIAGRAM_TEXT_MAX_CHARS  = 200


def _is_toc_marker_page_et(page) -> bool:
    text = page.get_text("text")
    return any(kw in text for kw in _TOC_START_KEYWORDS)


def _is_boundary_page_et(page) -> bool:
    blocks = page.get_text("dict")["blocks"]
    image_block_count = sum(1 for b in blocks if b.get("type") == 1)
    raw_text = page.get_text("text")

    if any(sig in raw_text for sig in _BOUNDARY_SIGNALS):
        return True

    stripped = raw_text.strip()
    if image_block_count >= _DIAGRAM_IMAGE_THRESHOLD and len(stripped) < _DIAGRAM_TEXT_MAX_CHARS:
        return True

    return False


def _is_weichai_skip_et(text: str) -> bool:
    return any(p in text for p in _WEICHAI_SKIP)


def _clean_en_label_et(en: str) -> str:
    en = re.sub(r"[()]", "", en)
    parts = en.split()
    deduped: List[str] = []
    for p in parts:
        if deduped and deduped[-1].upper() == p.upper():
            continue
        deduped.append(p)
    return " ".join(deduped).strip()


def extract_weichai_engine_toc(pdf_path: str) -> Dict:
    import fitz
    from collections import OrderedDict

    doc = fitz.open(pdf_path)
    total_pages = len(doc)
    logger.info(
        "Weichai TOC extractor v2: %d page(s) in '%s'", total_pages, pdf_path
    )

    toc_start = None
    for idx in range(total_pages):
        if _is_toc_marker_page_et(doc[idx]):
            toc_start = idx
            logger.info("TOC marker found at page %d", idx + 1)
            break

    if toc_start is None:
        logger.warning("No TOC marker (目录/CONTENTS) found — falling back to page 1.")
        toc_start = 0

    categories: OrderedDict[str, Dict] = OrderedDict()
    current_category: Optional[Dict] = None
    toc_pages_processed = 0

    for page_idx in range(toc_start, total_pages):
        page = doc[page_idx]

        is_first_page = (page_idx == toc_start)
        if not is_first_page and _is_boundary_page_et(page):
            logger.info(
                "Page %d: boundary detected — stopping after %d TOC page(s).",
                page_idx + 1, toc_pages_processed,
            )
            break

        toc_pages_processed += 1

        for block in page.get_text("dict")["blocks"]:
            if block.get("type") != 0:
                continue
            for line in block.get("lines", []):
                spans = line.get("spans", [])
                cn_parts: List[str] = []
                en_parts: List[str] = []
                x0_min   = 9999.0
                en_is_bold = False

                for span in spans:
                    text = span["text"].strip()

                    if not text or _is_weichai_skip_et(text):
                        continue
                    if re.match(r"^\.{3,}$", text):
                        continue
                    if re.match(r"^\d+$", text):
                        continue

                    flags = span.get("flags", 0)
                    is_bold = bool(flags & 16)
                    x0 = span["bbox"][0]
                    x0_min = min(x0_min, x0)

                    if re.search(r"[\u4e00-\u9fff]", text):
                        cn_parts.append(text)
                    elif re.search(r"[A-Za-z]", text):
                        en_parts.append(text)
                        if is_bold:
                            en_is_bold = True

                cn = "".join(cn_parts).strip()
                en = _clean_en_label_et(" ".join(en_parts))

                if not (cn or en) or x0_min >= 200:
                    continue

                is_category = (x0_min <= _X_CATEGORY_MAX) and en_is_bold

                if is_category:
                    cat_key = cn or en
                    if cat_key not in categories:
                        categories[cat_key] = {
                            "category_name_en":     en,
                            "category_name_cn":     cn,
                            "category_description": "",
                            "subtypes":             OrderedDict(),
                        }
                        logger.info("Page %d: [CAT] '%s' / '%s'", page_idx + 1, en, cn)
                    current_category = categories[cat_key]

                else:
                    if current_category is None:
                        logger.debug(
                            "Page %d: subtype '%s' has no parent yet — skipped",
                            page_idx + 1, en or cn,
                        )
                        continue

                    dedup_key = cn or en
                    if dedup_key and dedup_key not in current_category["subtypes"]:
                        current_category["subtypes"][dedup_key] = {
                            "type_category_name_en":     en,
                            "type_category_name_cn":     cn,
                            "type_category_description": "",
                        }

    doc.close()

    if toc_pages_processed == 0:
        logger.warning("No TOC pages processed.")

    output_categories = []
    for cat_data in categories.values():
        output_categories.append({
            "category_name_en":     cat_data["category_name_en"],
            "category_name_cn":     cat_data["category_name_cn"],
            "category_description": "",
            "data_type":            list(cat_data["subtypes"].values()),
        })

    logger.info(
        "Weichai TOC v2 complete: %d categories, %d total subtypes "
        "(processed %d TOC page(s) / %d total pages)",
        len(output_categories),
        sum(len(c["data_type"]) for c in output_categories),
        toc_pages_processed,
        total_pages,
    )
    return {"categories": output_categories}


# ── SHARED VISION HELPER ───────────────────────────────────────────────────

def _vision_call(b64_image: str, system_prompt: str, user_text: str,
                 sumopod_client, max_tokens: int = 200, detail: str = "low") -> Optional[str]:
    try:
        response = sumopod_client.client.chat.completions.create(
            model=sumopod_client.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {"url": f"data:image/jpeg;base64,{b64_image}", "detail": detail},
                        },
                        {"type": "text", "text": user_text},
                    ],
                },
            ],
            temperature=0.0,
            max_tokens=max_tokens,
            timeout=60,
        )
        return extract_response_text(response)
    except Exception as e:
        logger.warning("Vision call failed: %s", e)
        return None


# ── ENGINE EXTRACTOR (Cummins / vision-based) ─────────────────────────────

_ENGINE_SYSTEM_PROMPT = """\
You are reading a Cummins engine parts catalog page image.
Extract the category label from the TOP-RIGHT corner of the page.

The label is bilingual: Chinese characters immediately followed by English text.
Examples: "燃油泵PUMP,FUEL" · "缸体管路PLUMBING,CYLINDER BLOCK"

Return ONLY valid JSON, no markdown:
{ "header": "<exact bilingual text>" }

If the top-right has no category label, return:
{ "header": null }"""


def _extract_engine_header(b64: str, sumopod_client) -> Optional[str]:
    raw = _vision_call(b64, _ENGINE_SYSTEM_PROMPT,
                       "Extract the category label from the top-right of this page.",
                       sumopod_client, max_tokens=100)
    if not raw:
        return None
    try:
        return parse_llm_json(raw).get("header")
    except Exception:
        return None


def _split_bilingual_label(raw: str) -> Optional[Dict[str, str]]:
    """
    Split "燃油泵PUMP,FUEL" → { category_name_en: "Pump Fuel", category_name_cn: "燃油泵" }.
    Commas in the English portion act as word separators.

    Robustness fixes:
      • Replace commas, slashes, and underscores with spaces before tokenising.
      • Keep only tokens that start with A-Z / a-z, dropping Chinese chars or
        punctuation that Vision AI occasionally leaks into the EN portion
        (e.g. "/批准APPROVAL,AGENCY" → "Approval Agency").
    """
    raw = raw.strip()
    if not raw:
        return None

    match = re.search(r"([\u4e00-\u9fff])([\x21-\x7E])", raw)
    if not match:
        return {"category_name_en": raw.title(), "category_name_cn": "", "category_description": ""}

    split_idx = match.start() + 1
    cn = raw[:split_idx].strip()

    # Normalise all common separators (comma, slash, underscore) to spaces,
    # then keep only tokens starting with an ASCII letter.
    en_raw = raw[split_idx:].replace(",", " ").replace("/", " ").replace("_", " ")
    en_tokens = [
        p.capitalize()
        for p in en_raw.split()
        if p and re.match(r'^[A-Za-z]', p)
    ]
    en_clean = " ".join(en_tokens)

    if not en_clean:
        return None

    return {"category_name_en": en_clean, "category_name_cn": cn, "category_description": ""}


def _en_word_set(en: str) -> frozenset:
    """Normalise an EN category name to a frozenset of lowercase words."""
    return frozenset(re.sub(r'[^a-z\s]', ' ', en.lower()).split())


def _process_engine_pages(pages_b64: List[tuple], sumopod_client) -> Dict:
    """
    Shared loop for both ZIP and real-PDF engine extraction (Cummins/vision).

    Deduplication strategy — two levels:
    1. CN-name exact match (original behaviour).
    2. EN word-set subset match (new):
         • If the new entry's EN words are a SUBSET of an existing entry's
           words → the new entry is less complete; skip it.
         • If the new entry's EN words are a SUPERSET of an existing entry's
           words → the new entry is more complete; REPLACE the existing one.
         • If there is any overlap but neither is a subset → keep both
           (different categories that happen to share one word).

    This handles the case where Vision AI reads the same page label
    differently across diagram/table page pairs, e.g.:
      Pass 1 (diagram page):  "参数/Agency"      → words {"agency"}
      Pass 2 (table page):    "审批/Approval Agency" → words {"approval","agency"}
    {"agency"} ⊂ {"approval","agency"} → replace pass-1 entry with pass-2.
    Result: one entry "Approval Agency" (the more complete reading).
    """
    seen_cn: Dict[str, bool] = {}
    seen_en_sets: List[frozenset] = []   # parallel to `categories`
    categories: List[Dict] = []

    for page_label, b64 in pages_b64:
        raw_header = _extract_engine_header(b64, sumopod_client)
        if not raw_header:
            logger.debug("Page %s: no header found", page_label)
            continue

        parsed = _split_bilingual_label(raw_header)
        if not parsed:
            logger.debug("Page %s: could not parse header '%s'", page_label, raw_header)
            continue

        key_cn   = parsed["category_name_cn"] or parsed["category_name_en"]
        new_wset = _en_word_set(parsed["category_name_en"])

        # ── CN-based exact dedup ──────────────────────────────────────────
        if key_cn in seen_cn:
            logger.debug("Page %s: duplicate CN '%s', skipping", page_label, key_cn)
            continue

        # ── EN word-set subset dedup ──────────────────────────────────────
        replaced = False
        skip     = False
        for i, existing_wset in enumerate(seen_en_sets):
            if not (new_wset & existing_wset):
                continue  # no overlap at all → different categories

            if new_wset <= existing_wset:
                # new is a subset (less complete) → discard new
                logger.debug(
                    "Page %s: EN '%s' is subset of existing '%s' — skipped",
                    page_label, parsed["category_name_en"],
                    categories[i]["category_name_en"],
                )
                skip = True
                break
            elif existing_wset < new_wset:
                # existing is a proper subset → replace with more complete entry
                logger.info(
                    "Page %s: EN '%s' supersedes existing '%s' — replacing",
                    page_label, parsed["category_name_en"],
                    categories[i]["category_name_en"],
                )
                seen_en_sets[i] = new_wset
                categories[i]   = parsed
                seen_cn[key_cn] = True
                replaced = True
                break

        if skip or replaced:
            continue

        seen_cn[key_cn] = True
        seen_en_sets.append(new_wset)
        categories.append(parsed)
        logger.info("Page %s: new category: '%s' / '%s'",
                    page_label, parsed["category_name_en"], parsed["category_name_cn"])

    return {"categories": categories}


def extract_engine_categories(pdf_path: str, sumopod_client) -> Dict:
    """Extract Engine partbook categories (Cummins-style: ZIP or real PDF, vision-based)."""
    if is_zip_pdf(pdf_path):
        return _extract_engine_from_zip(pdf_path, sumopod_client)
    return _extract_engine_from_real_pdf(pdf_path, sumopod_client)


def _extract_engine_from_zip(pdf_path: str, sumopod_client) -> Dict:
    logger.info("Engine (ZIP): extracting from '%s'", pdf_path)
    tmp_dir = tempfile.mkdtemp(prefix="engine_extract_")
    try:
        manifest = extract_zip_pdf(pdf_path, tmp_dir)
        pages = manifest.get("pages", [])
        table_pages = [p for p in pages if not p.get("has_visual_content", True)] or pages
        logger.info("Engine: %d table page(s) to process", len(table_pages))

        pages_b64 = []
        for page_info in table_pages:
            image_path = page_info.get("image", {}).get("path")
            if image_path:
                pages_b64.append((
                    page_info.get("page_number", "?"),
                    image_to_base64(str(Path(tmp_dir) / image_path)),
                ))

        result = _process_engine_pages(pages_b64, sumopod_client)
        logger.info("Engine (ZIP): extracted %d unique categories", len(result["categories"]))
        return result
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _extract_engine_from_real_pdf(pdf_path: str, sumopod_client) -> Dict:
    import fitz
    logger.info("Engine (real PDF, vision): extracting from '%s'", pdf_path)
    doc = fitz.open(pdf_path)
    total = len(doc)
    doc.close()

    pages_b64 = [
        (i + 1, pdf_page_to_base64(pdf_path, i))
        for i in range(total)
    ]
    result = _process_engine_pages(pages_b64, sumopod_client)
    logger.info("Engine (real PDF): extracted %d unique categories", len(result["categories"]))
    return result


# ===========================================================================
# ── TRANSMISSION EXTRACTOR ────────────────────────────────────────────────
# ===========================================================================

_TRANSMISSION_VISION_PROMPT = """\
You are reading a Chinese-language transmission parts catalog page.
Identify every category name on this page.
Ignore page numbers, dot leaders, section numbers, and table headers.

Return ONLY valid JSON, no markdown:
{ "categories_cn": ["<category 1>", "<category 2>", ...] }

If none found: { "categories_cn": [] }"""

_TRANSMISSION_TRANSLATION_PROMPT = """\
You are a professional automotive parts catalog translator (Chinese to English).
Translate each Chinese transmission category name into clear, professional English.

Return ONLY valid JSON, no markdown:
{
  "categories": [
    { "category_name_en": "<English>", "category_name_cn": "<Chinese>", "category_description": "" }
  ]
}

Rules: same order as input · standard automotive terminology · no duplicates · no extra fields."""

_TOC_EXTRACTION_PROMPT = """\
You are a bilingual automotive parts catalog translator.
Extract and translate all category names from this Chinese transmission ToC text.

Return ONLY valid JSON:
{
  "categories": [
    { "category_name_en": "<English>", "category_name_cn": "<Chinese>", "category_description": "" }
  ]
}"""


def _extract_cn_from_transmission_image(b64: str, sumopod_client) -> List[str]:
    raw = _vision_call(b64, _TRANSMISSION_VISION_PROMPT,
                       "Extract all Chinese category names from this page.",
                       sumopod_client, max_tokens=500, detail="high")
    if not raw:
        return []
    try:
        return parse_llm_json(raw).get("categories_cn", [])
    except Exception:
        return []


def _translate_cn_categories(cn_list: List[str], sumopod_client) -> List[Dict]:
    if not cn_list:
        return []

    user_msg = ("Translate these Chinese transmission category names to English:\n\n"
                + json.dumps(cn_list, ensure_ascii=False, indent=2))
    try:
        resp = sumopod_client.client.chat.completions.create(
            model=sumopod_client.model,
            messages=[
                {"role": "system", "content": _TRANSMISSION_TRANSLATION_PROMPT},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.1,
            max_tokens=2000,
            timeout=60,
        )
        raw = resp.choices[0].message.content.strip()
        categories = parse_llm_json(raw).get("categories", [])
        for cat in categories:
            cat.setdefault("category_description", "")
        return categories
    except Exception as e:
        logger.warning("Transmission translation failed: %s", e)
        return [{"category_name_en": cn, "category_name_cn": cn, "category_description": ""}
                for cn in cn_list]


def _translate_toc_text(toc_text: str, sumopod_client) -> Dict:
    resp = sumopod_client.client.chat.completions.create(
        model=sumopod_client.model,
        messages=[
            {"role": "system", "content": _TOC_EXTRACTION_PROMPT},
            {"role": "user", "content":
                "Extract and translate all category names from this "
                "Chinese-only transmission parts manual ToC:\n\n" + toc_text},
        ],
        temperature=0.2,
        max_tokens=2000,
        timeout=60,
    )
    extracted = parse_llm_json(extract_response_text(resp))
    for cat in extracted.get("categories", []):
        cat.setdefault("category_description", "")
    logger.info("Transmission (text): extracted %d categories",
                len(extracted.get("categories", [])))
    return extracted


def _collect_unique_cn(pages_b64: List[tuple], sumopod_client) -> List[str]:
    seen: Dict[str, bool] = {}
    all_cn: List[str] = []
    for page_label, b64 in pages_b64:
        cn_list = _extract_cn_from_transmission_image(b64, sumopod_client)
        logger.info("Page %s: found %d categories", page_label, len(cn_list))
        for cn in cn_list:
            cn = cn.strip()
            if cn and cn not in seen:
                seen[cn] = True
                all_cn.append(cn)
    return all_cn


def extract_transmission_categories(pdf_path: str, sumopod_client,
                                    max_toc_pages: int = 10) -> Dict:
    if is_zip_pdf(pdf_path):
        return _extract_transmission_from_zip(pdf_path, sumopod_client, max_toc_pages)
    return _extract_transmission_from_real_pdf(pdf_path, sumopod_client, max_toc_pages)


def _extract_transmission_from_zip(pdf_path: str, sumopod_client,
                                   max_toc_pages: int) -> Dict:
    logger.info("Transmission (ZIP): extracting from '%s'", pdf_path)
    tmp_dir = tempfile.mkdtemp(prefix="transmission_extract_")
    try:
        manifest = extract_zip_pdf(pdf_path, tmp_dir)
        pages = manifest.get("pages", [])
        table_pages = ([p for p in pages if not p.get("has_visual_content", True)] or pages)
        table_pages = table_pages[:max_toc_pages]
        logger.info("Transmission: processing %d page(s) via vision", len(table_pages))

        pages_b64 = []
        for p in table_pages:
            img_path = p.get("image", {}).get("path")
            if img_path:
                pages_b64.append((p.get("page_number", "?"),
                                  image_to_base64(str(Path(tmp_dir) / img_path))))

        all_cn = _collect_unique_cn(pages_b64, sumopod_client)
        logger.info("Transmission: %d unique CN categories, translating...", len(all_cn))
        categories = _translate_cn_categories(all_cn, sumopod_client)
        logger.info("Transmission (ZIP): extracted %d categories", len(categories))
        return {"categories": categories}
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _extract_transmission_from_real_pdf(pdf_path: str, sumopod_client,
                                        max_toc_pages: int) -> Dict:
    import fitz

    logger.info("Transmission (real PDF): extracting from '%s'", pdf_path)
    doc = fitz.open(pdf_path)
    total_pages = len(doc)

    toc_text = "\n\n".join(
        f"--- Page {i + 1} ---\n{doc[i].get_text('text').strip()}"
        for i in range(min(max_toc_pages, total_pages))
        if doc[i].get_text("text").strip()
    )
    doc.close()

    if toc_text:
        logger.info("Transmission (real PDF): text found (%d chars), using text path",
                    len(toc_text))
        return _translate_toc_text(toc_text, sumopod_client)

    logger.info("Transmission (real PDF): no text found, falling back to vision AI")
    pages_b64 = [
        (i + 1, pdf_page_to_base64(pdf_path, i))
        for i in range(min(max_toc_pages, total_pages))
    ]
    all_cn = _collect_unique_cn(pages_b64, sumopod_client)
    logger.info("Transmission: %d unique CN categories, translating...", len(all_cn))
    categories = _translate_cn_categories(all_cn, sumopod_client)
    logger.info("Transmission (real PDF): extracted %d categories", len(categories))
    return {"categories": categories}


# ===========================================================================
# ── UNIFIED ENTRY POINT ────────────────────────────────────────────────────
# ===========================================================================

def extract_engine_or_transmission(pdf_path: str, partbook_type: str,
                                   sumopod_client=None,
                                   max_toc_pages: int = 10) -> Dict:
    """
    Unified extraction entry point for Engine and Transmission partbooks.

    Engine auto-detection priority:
      1. Weichai bilingual TOC (text-based)  → extract_weichai_engine_toc()
      2. ZIP archive (Cummins-style)         → vision AI on JPEG pages
      3. Standard PDF                        → vision AI page-by-page
    """
    if sumopod_client is None and partbook_type != "engine":
        raise ValueError("sumopod_client is required for transmission extraction.")

    partbook_type = partbook_type.lower().strip()

    if partbook_type == "engine":
        if not is_zip_pdf(pdf_path) and is_weichai_bilingual_toc(pdf_path):
            logger.info(
                "Engine: Weichai bilingual TOC detected — using text extraction"
            )
            result = extract_weichai_engine_toc(pdf_path)
            code_to_category: Dict[str, str] = {}
            for cat in result.get("categories", []):
                cat_en = cat.get("category_name_en", "")
                cat_cn = cat.get("category_name_cn", "")
                if cat_cn and cat_en:
                    code_to_category[cat_cn] = cat_en
                if cat_en:
                    code_to_category[cat_en] = cat_en
            result["code_to_category"] = code_to_category
            return result

        if sumopod_client is None:
            raise ValueError(
                "sumopod_client is required for vision-based engine extraction."
            )
        logger.info("Engine: using vision-based extraction (Cummins / non-Weichai)")
        return extract_engine_categories(pdf_path, sumopod_client)

    if partbook_type == "transmission":
        return extract_transmission_categories(pdf_path, sumopod_client, max_toc_pages)

    raise ValueError(f"Unknown partbook_type '{partbook_type}'. Use 'engine' or 'transmission'.")