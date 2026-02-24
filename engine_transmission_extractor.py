"""
Engine & Transmission Partbook Extractor
=========================================
Handles two PDF formats:
  1. ZIP archive (.pdf that is actually a ZIP with JPEG images + manifest.json)
  2. Real PDF (may contain embedded images / scanned pages with no extractable text)

ENGINE strategy:
  - Detect format (ZIP vs real PDF)
  - For ZIP: extract JPEGs from manifest table pages, send each to vision AI
  - For real PDF: render each page to image via PyMuPDF, send to vision AI
  - Vision AI reads the bilingual top-right header e.g. "燃油泵PUMP,FUEL"
  - Split CN/EN with regex, deduplicate, return flat category list.

TRANSMISSION strategy:
  - Same format detection
  - Vision AI extracts Chinese category names from each page
  - One batch translation call CN -> EN
  - Return flat bilingual category list.

Output format (flat - no type_categories):
{
  "categories": [
    {
      "category_name_en": "Pump Fuel",
      "category_name_cn": "燃油泵",
      "category_description": ""
    }
  ]
}
"""

import base64
import io
import json
import logging
import re
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Format detection
# ---------------------------------------------------------------------------

def _is_zip_pdf(pdf_path: str) -> bool:
    """Detect ZIP format using magic bytes (PK = 0x504B)."""
    try:
        with open(pdf_path, "rb") as f:
            magic = f.read(4)
        is_zip = magic[:2] == b"PK"
        logger.info("Format detection '%s': magic=%s is_zip=%s", pdf_path, magic.hex(), is_zip)
        return is_zip
    except Exception as e:
        logger.warning("Format detection failed for '%s': %s", pdf_path, e)
        return False


# ---------------------------------------------------------------------------
# ZIP helpers
# ---------------------------------------------------------------------------

def _extract_zip_pdf(pdf_path: str, dest_dir: str) -> dict:
    """Extract the ZIP-format PDF to dest_dir and return the parsed manifest."""
    with zipfile.ZipFile(pdf_path, "r") as zf:
        zf.extractall(dest_dir)
    manifest_path = Path(dest_dir) / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError("manifest.json not found in %s" % pdf_path)
    with open(manifest_path, "r", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Image helpers
# ---------------------------------------------------------------------------

def _image_file_to_base64(image_path: str) -> str:
    """Encode an image file to base64 string."""
    with open(image_path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


def _pdf_page_to_base64(pdf_path: str, page_index: int, dpi: int = 150) -> str:
    """
    Render a single PDF page to a JPEG image and return as base64.
    Uses PyMuPDF (fitz).
    """
    import fitz
    doc = fitz.open(pdf_path)
    page = doc[page_index]
    mat = fitz.Matrix(dpi / 72, dpi / 72)
    pix = page.get_pixmap(matrix=mat, alpha=False)
    img_bytes = pix.tobytes("jpeg")
    doc.close()
    return base64.b64encode(img_bytes).decode("utf-8")


def _parse_json_from_llm(text: str) -> Dict:
    """Strip markdown fences and parse JSON."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        text = "\n".join(
            line for line in lines if not line.strip().startswith("```")
        ).strip()
    return json.loads(text)


# ---------------------------------------------------------------------------
# ENGINE extractor
# ---------------------------------------------------------------------------

ENGINE_HEADER_SYSTEM_PROMPT = """\
You are reading a Cummins engine parts catalog page image.

Your ONLY job is to extract the category label from the TOP-RIGHT corner of the page.

The label is a bilingual string: Chinese characters immediately followed by English
text with no space between them. Examples:
  - "燃油泵PUMP,FUEL"
  - "附件驱动皮带轮PULLEY,ACCESSORY DRIVE"
  - "缸体管路PLUMBING,CYLINDER BLOCK"
  - "参数牌APPROVAL,AGENCY"

Return ONLY a valid JSON object, no markdown, no explanation:
{
  "header": "<the exact bilingual text from the top-right corner>"
}

If the top-right has no category label (diagram or cover page), return:
{
  "header": null
}"""


def _extract_engine_header_from_image(b64_image: str, sumopod_client) -> Optional[str]:
    """
    Use vision AI to read the bilingual category header from an engine page.
    b64_image: base64-encoded JPEG.
    Returns raw header string or None.
    """
    try:
        response = sumopod_client.client.chat.completions.create(
            model=sumopod_client.model,
            messages=[
                {"role": "system", "content": ENGINE_HEADER_SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": "data:image/jpeg;base64," + b64_image,
                                "detail": "low"
                            }
                        },
                        {
                            "type": "text",
                            "text": "Extract the category label from the top-right of this page."
                        }
                    ]
                }
            ],
            temperature=0.0,
            max_tokens=100,
            timeout=60
        )
        raw = response.choices[0].message.content.strip()
        logger.debug("Engine vision raw response: %s", raw)
        parsed = _parse_json_from_llm(raw)
        return parsed.get("header")

    except Exception as e:
        logger.warning("Engine vision call failed: %s", e)
        return None


def _split_bilingual_engine_label(raw: str) -> Optional[Dict[str, str]]:
    """
    Split a raw Engine header label into Chinese and English parts.
    Format: <CJK chars><ASCII ENGLISH,WITH,COMMAS>
    e.g. "燃油泵PUMP,FUEL" => cn="燃油泵", en="Pump Fuel"
    """
    raw = raw.strip()
    if not raw:
        return None

    match = re.search(r'([\u4e00-\u9fff])([\x21-\x7E])', raw)
    if not match:
        return {
            "category_name_en": raw.strip().title(),
            "category_name_cn": "",
            "category_description": ""
        }

    split_idx = match.start() + 1
    cn = raw[:split_idx].strip()
    en_raw = raw[split_idx:].strip()

    # Commas act as word separators: "PUMP,FUEL" => "Pump Fuel"
    en_parts = [p.strip() for p in en_raw.replace(",", " ").split()]
    en_clean = " ".join(p.capitalize() for p in en_parts if p)

    return {
        "category_name_en": en_clean,
        "category_name_cn": cn,
        "category_description": ""
    }


def extract_engine_categories(pdf_path: str, sumopod_client=None) -> Dict:
    """
    Extract Engine partbook categories.
    Handles both ZIP-format and real PDF (scanned image) files via vision AI.
    """
    if _is_zip_pdf(pdf_path):
        if sumopod_client is None:
            raise ValueError("sumopod_client is required for ZIP-format engine PDFs.")
        return _extract_engine_from_zip(pdf_path, sumopod_client)
    else:
        if sumopod_client is None:
            raise ValueError("sumopod_client is required for image-based engine PDFs.")
        return _extract_engine_from_real_pdf(pdf_path, sumopod_client)


def _extract_engine_from_zip(pdf_path: str, sumopod_client) -> Dict:
    """Extract engine categories from a ZIP-format PDF via vision AI."""
    logger.info("Engine (ZIP): extracting from '%s'", pdf_path)

    tmp_dir = tempfile.mkdtemp(prefix="engine_extract_")
    try:
        manifest = _extract_zip_pdf(pdf_path, tmp_dir)
        pages = manifest.get("pages", [])
        logger.info("Engine: %d pages in manifest", len(pages))

        table_pages = [p for p in pages if not p.get("has_visual_content", True)]
        if not table_pages:
            logger.warning("Engine: no table pages found, processing all pages")
            table_pages = pages

        logger.info("Engine: %d table page(s) to process via vision", len(table_pages))

        seen: Dict[str, bool] = {}
        categories: List[Dict] = []

        for page_info in table_pages:
            image_filename = page_info.get("image", {}).get("path")
            if not image_filename:
                continue

            image_path = str(Path(tmp_dir) / image_filename)
            page_num = page_info.get("page_number", "?")

            b64 = _image_file_to_base64(image_path)
            raw_header = _extract_engine_header_from_image(b64, sumopod_client)
            if not raw_header:
                logger.debug("Page %s: no header found", page_num)
                continue

            parsed = _split_bilingual_engine_label(raw_header)
            if not parsed:
                logger.debug("Page %s: could not parse header '%s'", page_num, raw_header)
                continue

            dedup_key = parsed["category_name_cn"] or parsed["category_name_en"]
            if dedup_key and dedup_key not in seen:
                seen[dedup_key] = True
                categories.append(parsed)
                logger.info(
                    "Page %s: new category: '%s' / '%s'",
                    page_num, parsed["category_name_en"], parsed["category_name_cn"]
                )
            else:
                logger.debug("Page %s: duplicate '%s', skipping", page_num, dedup_key)

        logger.info("Engine (ZIP): extracted %d unique categories", len(categories))
        return {"categories": categories}

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _extract_engine_from_real_pdf(pdf_path: str, sumopod_client) -> Dict:
    """
    Extract engine categories from a real PDF (scanned/image-based).
    Renders each page via PyMuPDF and sends to vision AI.
    """
    import fitz

    logger.info("Engine (real PDF): extracting from '%s'", pdf_path)

    doc = fitz.open(pdf_path)
    total_pages = len(doc)
    doc.close()
    logger.info("Engine: %d pages in PDF", total_pages)

    seen: Dict[str, bool] = {}
    categories: List[Dict] = []

    for page_index in range(total_pages):
        page_num = page_index + 1
        try:
            b64 = _pdf_page_to_base64(pdf_path, page_index, dpi=150)
        except Exception as e:
            logger.warning("Page %d: failed to render: %s", page_num, e)
            continue

        raw_header = _extract_engine_header_from_image(b64, sumopod_client)
        if not raw_header:
            logger.debug("Page %d: no header found", page_num)
            continue

        parsed = _split_bilingual_engine_label(raw_header)
        if not parsed:
            logger.debug("Page %d: could not parse header '%s'", page_num, raw_header)
            continue

        dedup_key = parsed["category_name_cn"] or parsed["category_name_en"]
        if dedup_key and dedup_key not in seen:
            seen[dedup_key] = True
            categories.append(parsed)
            logger.info(
                "Page %d: new category: '%s' / '%s'",
                page_num, parsed["category_name_en"], parsed["category_name_cn"]
            )
        else:
            logger.debug("Page %d: duplicate '%s', skipping", page_num, dedup_key)

    logger.info("Engine (real PDF): extracted %d unique categories", len(categories))
    return {"categories": categories}


# ---------------------------------------------------------------------------
# TRANSMISSION extractor
# ---------------------------------------------------------------------------

TRANSMISSION_VISION_SYSTEM_PROMPT = """\
You are reading a Chinese-language transmission parts catalog page.

Your task: identify every category name on this page.
Ignore page numbers, dot leaders, section numbers, and table headers.

Return ONLY a valid JSON object, no markdown, no explanation:
{
  "categories_cn": ["<category 1 in Chinese>", "<category 2 in Chinese>", ...]
}

If no category names are found, return:
{
  "categories_cn": []
}"""

TRANSMISSION_TRANSLATION_SYSTEM_PROMPT = """\
You are a professional automotive parts catalog translator (Chinese to English).

Translate each Chinese transmission category name into clear, professional English.

Return ONLY a valid JSON object, no markdown, no explanation:
{
  "categories": [
    {
      "category_name_en": "<English translation>",
      "category_name_cn": "<original Chinese>",
      "category_description": ""
    }
  ]
}

Rules:
- Keep the same order as the input.
- Use standard automotive/transmission terminology.
- Do NOT include duplicates.
- Do NOT add any extra fields."""


def _extract_categories_from_transmission_image(
    b64_image: str, sumopod_client
) -> List[str]:
    """
    Use vision AI to read Chinese category names from a transmission page.
    b64_image: base64-encoded JPEG.
    Returns list of Chinese category strings.
    """
    try:
        response = sumopod_client.client.chat.completions.create(
            model=sumopod_client.model,
            messages=[
                {"role": "system", "content": TRANSMISSION_VISION_SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": "data:image/jpeg;base64," + b64_image,
                                "detail": "high"
                            }
                        },
                        {
                            "type": "text",
                            "text": "Extract all Chinese category names from this page."
                        }
                    ]
                }
            ],
            temperature=0.1,
            max_tokens=500,
            timeout=60
        )
        raw = response.choices[0].message.content.strip()
        logger.debug("Transmission vision raw: %s", raw[:200])
        parsed = _parse_json_from_llm(raw)
        return parsed.get("categories_cn", [])

    except Exception as e:
        logger.warning("Transmission vision call failed: %s", e)
        return []


def _translate_transmission_categories(
    cn_categories: List[str], sumopod_client
) -> List[Dict]:
    """Translate a list of Chinese transmission category names to English in one call."""
    if not cn_categories:
        return []

    user_message = (
        "Translate these Chinese transmission category names to English:\n\n"
        + json.dumps(cn_categories, ensure_ascii=False, indent=2)
    )

    try:
        response = sumopod_client.client.chat.completions.create(
            model=sumopod_client.model,
            messages=[
                {"role": "system", "content": TRANSMISSION_TRANSLATION_SYSTEM_PROMPT},
                {"role": "user", "content": user_message}
            ],
            temperature=0.1,
            max_tokens=2000,
            timeout=60
        )
        raw = response.choices[0].message.content.strip()
        parsed = _parse_json_from_llm(raw)
        categories = parsed.get("categories", [])
        for cat in categories:
            cat.setdefault("category_description", "")
        return categories

    except Exception as e:
        logger.warning("Transmission translation call failed: %s", e)
        return [
            {"category_name_en": cn, "category_name_cn": cn, "category_description": ""}
            for cn in cn_categories
        ]


def extract_transmission_categories(
    pdf_path: str,
    sumopod_client=None,
    max_toc_pages: int = 10
) -> Dict:
    """
    Extract Transmission partbook categories.
    Handles both ZIP-format and real PDF files via vision AI.
    """
    if _is_zip_pdf(pdf_path):
        return _extract_transmission_from_zip(pdf_path, sumopod_client, max_toc_pages)
    else:
        return _extract_transmission_from_real_pdf(pdf_path, sumopod_client, max_toc_pages)


def _extract_transmission_from_zip(
    pdf_path: str, sumopod_client, max_toc_pages: int = 10
) -> Dict:
    """Extract transmission categories from a ZIP-format PDF via vision AI."""
    logger.info("Transmission (ZIP): extracting from '%s'", pdf_path)

    tmp_dir = tempfile.mkdtemp(prefix="transmission_extract_")
    try:
        manifest = _extract_zip_pdf(pdf_path, tmp_dir)
        pages = manifest.get("pages", [])
        logger.info("Transmission: %d pages in manifest", len(pages))

        table_pages = [p for p in pages if not p.get("has_visual_content", True)]
        if not table_pages:
            table_pages = pages
        table_pages = table_pages[:max_toc_pages]

        logger.info("Transmission: processing %d page(s) via vision", len(table_pages))

        all_cn: List[str] = []
        seen: Dict[str, bool] = {}

        for page_info in table_pages:
            image_filename = page_info.get("image", {}).get("path")
            if not image_filename:
                continue
            image_path = str(Path(tmp_dir) / image_filename)
            page_num = page_info.get("page_number", "?")

            b64 = _image_file_to_base64(image_path)
            cn_list = _extract_categories_from_transmission_image(b64, sumopod_client)
            logger.info("Page %s: found %d categories", page_num, len(cn_list))

            for cn in cn_list:
                cn = cn.strip()
                if cn and cn not in seen:
                    seen[cn] = True
                    all_cn.append(cn)

        logger.info("Transmission: %d unique CN categories, translating...", len(all_cn))
        categories = _translate_transmission_categories(all_cn, sumopod_client)
        logger.info("Transmission (ZIP): extracted %d categories", len(categories))
        return {"categories": categories}

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _extract_transmission_from_real_pdf(
    pdf_path: str, sumopod_client, max_toc_pages: int = 10
) -> Dict:
    """
    Extract transmission categories from a real PDF (scanned/image-based).
    First tries text extraction; if empty, falls back to vision AI per page.
    """
    import fitz

    logger.info("Transmission (real PDF): extracting from '%s'", pdf_path)

    doc = fitz.open(pdf_path)
    total_pages = len(doc)

    # Try text extraction first
    blocks = []
    for i in range(min(max_toc_pages, total_pages)):
        text = doc[i].get_text("text").strip()
        if text:
            blocks.append("--- Page %d ---\n%s" % (i + 1, text))
    doc.close()

    toc_text = "\n\n".join(blocks)

    if toc_text.strip():
        # Text-based PDF — use AI text translation
        logger.info("Transmission (real PDF): text found (%d chars), using text path", len(toc_text))
        return _translate_toc_text(toc_text, sumopod_client)
    else:
        # Image-based PDF — render pages and use vision AI
        logger.info("Transmission (real PDF): no text found, falling back to vision AI")

        all_cn: List[str] = []
        seen: Dict[str, bool] = {}

        for page_index in range(min(max_toc_pages, total_pages)):
            page_num = page_index + 1
            try:
                b64 = _pdf_page_to_base64(pdf_path, page_index, dpi=150)
            except Exception as e:
                logger.warning("Page %d: failed to render: %s", page_num, e)
                continue

            cn_list = _extract_categories_from_transmission_image(b64, sumopod_client)
            logger.info("Page %d: found %d categories", page_num, len(cn_list))

            for cn in cn_list:
                cn = cn.strip()
                if cn and cn not in seen:
                    seen[cn] = True
                    all_cn.append(cn)

        logger.info("Transmission: %d unique CN categories, translating...", len(all_cn))
        categories = _translate_transmission_categories(all_cn, sumopod_client)
        logger.info("Transmission (real PDF): extracted %d categories", len(categories))
        return {"categories": categories}


def _translate_toc_text(toc_text: str, sumopod_client) -> Dict:
    """Send raw ToC text to AI for extraction + translation."""
    SYSTEM_PROMPT = """\
You are a bilingual automotive parts catalog translator.
Extract and translate all category names from this Chinese transmission ToC text.

Return ONLY a valid JSON object:
{
  "categories": [
    {
      "category_name_en": "<English translation>",
      "category_name_cn": "<original Chinese>",
      "category_description": ""
    }
  ]
}"""

    response = sumopod_client.client.chat.completions.create(
        model=sumopod_client.model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    "Extract and translate all category names from this "
                    "Chinese-only transmission parts manual ToC:\n\n" + toc_text
                )
            }
        ],
        temperature=0.2,
        max_tokens=2000,
        timeout=60
    )

    raw = response.choices[0].message.content.strip()
    extracted = _parse_json_from_llm(raw)
    for cat in extracted.get("categories", []):
        cat.setdefault("category_description", "")
    logger.info("Transmission (text): extracted %d categories", len(extracted.get("categories", [])))
    return extracted


# ---------------------------------------------------------------------------
# Unified entry point
# ---------------------------------------------------------------------------

def extract_engine_or_transmission(
    pdf_path: str,
    partbook_type: str,
    sumopod_client=None,
    max_toc_pages: int = 10
) -> Dict:
    """
    Unified extraction entry point for Engine and Transmission partbooks.

    Args:
        pdf_path:       Path to the partbook PDF (ZIP or real PDF).
        partbook_type:  "engine" or "transmission".
        sumopod_client: SumopodClient instance. Required for all formats.
        max_toc_pages:  Max pages to scan (transmission only).

    Returns:
        Dict with "categories" list containing:
            category_name_en, category_name_cn, category_description
    """
    partbook_type = partbook_type.lower().strip()

    if partbook_type == "engine":
        return extract_engine_categories(pdf_path, sumopod_client=sumopod_client)

    elif partbook_type == "transmission":
        if sumopod_client is None:
            raise ValueError("sumopod_client is required for transmission extraction.")
        return extract_transmission_categories(
            pdf_path, sumopod_client=sumopod_client, max_toc_pages=max_toc_pages
        )

    else:
        raise ValueError(
            "Unknown partbook_type '%s'. Use 'engine' or 'transmission'." % partbook_type
        )