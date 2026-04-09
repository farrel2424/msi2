"""
engine_transmission_extractor.py  — with Weichai 3-level support
=================================================================
Changelog vs previous version:
  NEW: extract_weichai_engine_toc()
       Pure-text TOC parser for Weichai/bilingual Engine partbooks.
       Produces a 3-level hierarchy (Category → Type Category) from the
       bold/indentation signals in the PDF's text layer — no AI tokens used.

  NEW: is_weichai_bilingual_toc()
       Auto-detection: returns True when the PDF contains a bilingual TOC
       whose EN labels use TimesNewRomanPS-BoldMT (characteristic of Weichai).

  CHANGED: extract_engine_or_transmission()
       When partbook_type == "engine" AND the PDF is detected as a Weichai
       bilingual TOC, the new text-path is used instead of the vision path.
       All other engine PDFs continue to use the original vision approach.

Everything else is unchanged. No Transmission or Axle logic is touched.
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
# ── WEICHAI BILINGUAL TOC EXTRACTOR (NEW — text-based, no AI) ─────────────
# ===========================================================================

# x0 threshold: TOC entries at or below this x-coordinate are top-level Categories.
_X_CATEGORY_MAX = 50

# Watermark / metadata patterns to skip
_WEICHAI_SKIP = ["wangmd", "2023/", "shacman.com", "zhangzhi", "CONTENTS", "目录"]


def _is_weichai_skip(text: str) -> bool:
    return any(p in text for p in _WEICHAI_SKIP)


def _clean_en_label(en: str) -> str:
    """
    Strip parentheses and deduplicate adjacent identical tokens.
    e.g. "EVB EVB Bracket Assembly" → "EVB Bracket Assembly"
    """
    en = re.sub(r"[()]", "", en)
    parts = en.split()
    deduped: List[str] = []
    for p in parts:
        if deduped and deduped[-1].upper() == p.upper():
            continue
        deduped.append(p)
    return " ".join(deduped).strip()


def is_weichai_bilingual_toc(pdf_path: str, sample_pages: int = 2) -> bool:
    """
    Heuristic: return True when the first few pages of the PDF contain
    TimesNewRomanPS-BoldMT spans (the bold EN font used in Weichai TOCs)
    AND the PDF has extractable text (not image-based).

    This distinguishes Weichai bilingual TOC PDFs from:
      • Cummins engine PDFs  (vision-based, top-right bilingual label)
      • Scanned image PDFs   (no text layer)
    """
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


def extract_weichai_engine_toc(pdf_path: str) -> Dict:
    """
    Parse a Weichai Engine bilingual TOC PDF and return a 3-level category
    structure compatible with batch_create_type_categories_and_categories().

    Detection rules (per TOC line):
      • EN font bold (TimesNewRomanPS-BoldMT) AND x0 ≤ 50 → Category
      • Any other line with CN or EN text               → Type Category (subtype)

    No AI calls are made — extraction is 100% rule-based from the text layer.

    Args:
        pdf_path: Path to the Weichai Engine TOC PDF.

    Returns:
        {
          "categories": [
            {
              "category_name_en":     "Engine Block Group",
              "category_name_cn":     "机体结合组",
              "category_description": "",
              "data_type": [
                {
                  "type_category_name_en":     "Crankcase Assembly",
                  "type_category_name_cn":     "机体总成",
                  "type_category_description": ""
                }
              ]
            }
          ]
        }
    """
    import fitz

    doc = fitz.open(pdf_path)
    logger.info(
        "Weichai TOC extractor: %d page(s) in '%s'", len(doc), Path(pdf_path).name
    )

    raw_entries: List[Tuple[float, bool, str, str]] = []  # (x0, bold_en, cn, en)

    for page_idx in range(len(doc)):
        page = doc[page_idx]
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
                    if not text or _is_weichai_skip(text):
                        continue
                    if re.match(r"^\.{3,}$", text):   # dot leaders
                        continue
                    if re.match(r"^\d+$", text):        # page numbers
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
                en = _clean_en_label(" ".join(en_parts))

                if (cn or en) and x0_min < 200:   # exclude far-right artefacts
                    raw_entries.append((x0_min, en_is_bold, cn, en))

    doc.close()
    logger.info("Weichai TOC: %d raw entries collected", len(raw_entries))

    # ── Assemble 3-level hierarchy ─────────────────────────────────────────
    categories: List[Dict] = []
    current_category: Optional[Dict] = None

    for x0, bold_en, cn, en in raw_entries:
        is_category = (x0 <= _X_CATEGORY_MAX) and bold_en

        if is_category:
            current_category = {
                "category_name_en":     en,
                "category_name_cn":     cn,
                "category_description": "",
                "data_type":            [],
            }
            categories.append(current_category)
            logger.info("  [CAT] '%s' / '%s'", en, cn)
        else:
            if current_category is None:
                logger.debug("Skipped subtype before first category: '%s'", en or cn)
                continue
            current_category["data_type"].append({
                "type_category_name_en":     en,
                "type_category_name_cn":     cn,
                "type_category_description": "",
            })
            logger.debug("        └─ '%s' / '%s'", en, cn)

    logger.info(
        "Weichai TOC complete: %d categories, %d total subtypes",
        len(categories),
        sum(len(c["data_type"]) for c in categories),
    )
    return {"categories": categories}


# ===========================================================================
# ── SHARED VISION HELPER ───────────────────────────────────────────────────
# ===========================================================================

def _vision_call(b64_image: str, system_prompt: str, user_text: str,
                 sumopod_client, max_tokens: int = 200, detail: str = "low") -> Optional[str]:
    """Single vision API call; returns raw text content or None on failure."""
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


# ===========================================================================
# ── ENGINE EXTRACTOR (Cummins / vision-based) ─────────────────────────────
# ===========================================================================

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
    """
    raw = raw.strip()
    if not raw:
        return None

    match = re.search(r"([\u4e00-\u9fff])([\x21-\x7E])", raw)
    if not match:
        return {"category_name_en": raw.title(), "category_name_cn": "", "category_description": ""}

    split_idx = match.start() + 1
    cn = raw[:split_idx].strip()
    en_clean = " ".join(
        p.capitalize() for p in raw[split_idx:].replace(",", " ").split() if p
    )
    return {"category_name_en": en_clean, "category_name_cn": cn, "category_description": ""}


def _process_engine_pages(pages_b64: List[tuple], sumopod_client) -> Dict:
    """Shared loop for both ZIP and real-PDF engine extraction (Cummins/vision)."""
    seen: Dict[str, bool] = {}
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

        key = parsed["category_name_cn"] or parsed["category_name_en"]
        if key and key not in seen:
            seen[key] = True
            categories.append(parsed)
            logger.info("Page %s: new category: '%s' / '%s'",
                        page_label, parsed["category_name_en"], parsed["category_name_cn"])
        else:
            logger.debug("Page %s: duplicate '%s', skipping", page_label, key)

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
    """Send raw ToC text to AI for extraction + translation (text-based PDFs)."""
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
    """Gather unique Chinese category names from a set of (label, b64) image pairs."""
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
    """Extract Transmission partbook categories (ZIP or real PDF)."""
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
      1. Weichai bilingual TOC (text-based, bold/indent signals)
         → extract_weichai_engine_toc()  — FREE, no AI tokens
      2. ZIP archive (Cummins-style) → vision AI on JPEG pages
      3. Standard PDF → vision AI page-by-page

    Args:
        pdf_path:       Path to the partbook PDF (ZIP or real PDF).
        partbook_type:  "engine" or "transmission".
        sumopod_client: SumopodClient instance (required for vision paths).
        max_toc_pages:  Max pages to scan (transmission only).

    Returns:
        Dict with "categories" list.
        For Weichai engine: includes "data_type" subtypes per category.
        For Cummins/vision engine: flat categories (no data_type).
        For Transmission: flat categories (no data_type).
    """
    if sumopod_client is None and partbook_type != "engine":
        raise ValueError("sumopod_client is required for transmission extraction.")

    partbook_type = partbook_type.lower().strip()

    if partbook_type == "engine":
        # ── Path 1: Weichai bilingual TOC (text-based) ────────────────────
        if not is_zip_pdf(pdf_path) and is_weichai_bilingual_toc(pdf_path):
            logger.info(
                "Engine: Weichai bilingual TOC detected — using text extraction "
                "(no AI tokens needed)"
            )
            result = extract_weichai_engine_toc(pdf_path)
            # Build code_to_category map (EN → EN) for compatibility with epc_automation.py
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

        # ── Path 2 & 3: Cummins / vision-based ───────────────────────────
        if sumopod_client is None:
            raise ValueError(
                "sumopod_client is required for vision-based engine extraction. "
                "If this is a Weichai bilingual TOC PDF, ensure the file has an "
                "extractable text layer."
            )
        logger.info("Engine: using vision-based extraction (Cummins / non-Weichai)")
        return extract_engine_categories(pdf_path, sumopod_client)

    if partbook_type == "transmission":
        return extract_transmission_categories(pdf_path, sumopod_client, max_toc_pages)

    raise ValueError(f"Unknown partbook_type '{partbook_type}'. Use 'engine' or 'transmission'.")