"""
Axle (Drive Axle) Partbook Extractor
======================================
Handles extraction for the Drive Axle partbook type.

Supports two PDF formats:
  1. ZIP-format PDF  — contains JPEG images + manifest.json
  2. Real PDF        — standard PDF rendered page-by-page via PyMuPDF + vision AI

Structure of the Axle partbook PDF:
  - Each PDF maps to ONE Category (e.g. "Drive Axle"), name comes from filename
  - Diagram pages  = has_visual_content: true  (or pages with no table title)
  - Table pages    = has_visual_content: false (or pages with a title above the table)
  - Each table page has a title at the top center above the Description header
    Format: N <Chinese Title>(continued)
    Examples:
      1 Pass-Through Drive Axle Main Reducer Assembly Parts Catalog
      2 Pass-Through Drive Axle Main Reducer Assembly Parts Catalog (continued)

Strategy:
  ZIP format:
    1. Detect the ZIP-format PDF and extract to a temp directory.
    2. Read manifest.json to identify table pages (has_visual_content = false).
    3. For each table page, send the JPEG to vision AI to extract the title.
    4. Strip the table number prefix and continuation suffix, then deduplicate.
    5. Translate each unique Chinese title to English in one batch call.

  Real PDF format:
    1. Detect non-ZIP PDF and open with PyMuPDF.
    2. Render each page to JPEG at 150 DPI.
    3. Send each page to vision AI to extract the title.
    4. Strip prefix/suffix, deduplicate, then translate in one batch call.

Output format (3-level: Master -> Category -> Type Category):
{
  "categories": [
    {
      "category_name_en": "Drive Axle",
      "category_name_cn": "驱动桥",
      "category_description": "",
      "data_type": [
        {
          "type_category_name_en": "Pass-Through Drive Axle Main Reducer Assembly Parts",
          "type_category_name_cn": "贯通式驱动桥主减速器总成爆炸图对应备件目录",
          "type_category_description": ""
        }
      ]
    }
  ]
}
"""

import base64
import json
import logging
import re
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Regex helpers
# ---------------------------------------------------------------------------

# Matches leading table index: 表1, 表2, 表 3, etc.
_TABLE_PREFIX_RE = re.compile(r"^\u88681?\s*\d+\s*", flags=re.UNICODE)

# Matches trailing continuation markers
_CONTINUATION_RE = re.compile(r"[\uff08(]\u7eed[\uff09)]\s*$", flags=re.UNICODE)


def _normalise_title(raw: str) -> str:
    """Strip table-number prefix and continuation suffix from a raw table title."""
    title = raw.strip()
    title = _TABLE_PREFIX_RE.sub("", title)
    title = _CONTINUATION_RE.sub("", title)
    return title.strip()


# ---------------------------------------------------------------------------
# ZIP helpers
# ---------------------------------------------------------------------------

def _is_zip_pdf(pdf_path: str) -> bool:
    """Detect ZIP format using magic bytes."""
    try:
        with open(pdf_path, "rb") as f:
            magic = f.read(4)
        return magic[:2] == b"PK"
    except Exception:
        return False


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

def _image_to_base64(image_path: str) -> str:
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


# ---------------------------------------------------------------------------
# Vision AI helpers
# ---------------------------------------------------------------------------

TITLE_EXTRACTION_SYSTEM_PROMPT = """\
You are an assistant that reads Chinese automotive parts catalog images.
Your ONLY job is to extract the title text from the top of the page,
which appears above the table columns.

The title format is usually: table-number followed by Chinese text, optionally
ending with a continuation marker.

Return ONLY a valid JSON object, no markdown, no explanation:
{
  "raw_title": "<the full title text exactly as it appears>"
}

If this page is a diagram (no table), return:
{
  "raw_title": null
}"""

TRANSLATION_SYSTEM_PROMPT = """\
You are a professional automotive parts catalog translator (Chinese to English).
Translate each Chinese title in the input list into clear, professional English.
These are table titles from a heavy-truck axle parts catalog.

Return ONLY a valid JSON object, no markdown, no explanation:
{
  "translations": [
    {
      "cn": "<original Chinese title>",
      "en": "<English translation>"
    }
  ]
}

Rules:
- Keep the same order as the input.
- Use standard automotive terminology.
- Do NOT add any extra fields."""


def _extract_title_from_b64(b64: str, sumopod_client) -> Optional[str]:
    """
    Call vision AI to extract the table title from a base64-encoded JPEG.
    Returns the raw title string, or None if no table title found.
    """
    try:
        response = sumopod_client.client.chat.completions.create(
            model=sumopod_client.model,
            messages=[
                {"role": "system", "content": TITLE_EXTRACTION_SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": "data:image/jpeg;base64," + b64,
                                "detail": "low"
                            }
                        },
                        {
                            "type": "text",
                            "text": "Extract the table title from this image."
                        }
                    ]
                }
            ],
            temperature=0.0,
            max_tokens=200,
            timeout=60
        )

        raw_response = response.choices[0].message.content.strip()
        logger.debug("Vision response: %s", raw_response[:300])

        # Strip markdown fences if present
        if raw_response.startswith("```"):
            lines = raw_response.splitlines()
            raw_response = "\n".join(
                ln for ln in lines if not ln.strip().startswith("```")
            ).strip()

        parsed = json.loads(raw_response)
        return parsed.get("raw_title")

    except Exception as e:
        logger.warning("Vision call failed: %s", e)
        return None


def _extract_title_from_image(image_path: str, sumopod_client) -> Optional[str]:
    """
    Call vision AI to extract the table title from a JPEG file path.
    Returns the raw title string, or None if no table title found.
    """
    logger.info("Vision call starting for %s", Path(image_path).name)
    result = _extract_title_from_b64(_image_to_base64(image_path), sumopod_client)
    logger.info("Vision call completed for %s", Path(image_path).name)
    return result


def _translate_titles(cn_titles: List[str], sumopod_client) -> List[Dict]:
    """
    Translate a list of Chinese table titles to English in a single AI call.
    Returns list of {"cn": ..., "en": ...} dicts.
    """
    if not cn_titles:
        return []

    user_message = (
        "Translate these Chinese axle parts catalog table titles to English:\n"
        + json.dumps(cn_titles, ensure_ascii=False, indent=2)
    )

    try:
        response = sumopod_client.client.chat.completions.create(
            model=sumopod_client.model,
            messages=[
                {"role": "system", "content": TRANSLATION_SYSTEM_PROMPT},
                {"role": "user", "content": user_message}
            ],
            temperature=0.1,
            max_tokens=1000,
            timeout=60
        )

        raw_response = response.choices[0].message.content.strip()
        logger.debug("Translation response: %s", raw_response[:300])

        if raw_response.startswith("```"):
            lines = raw_response.splitlines()
            raw_response = "\n".join(
                ln for ln in lines if not ln.strip().startswith("```")
            ).strip()

        parsed = json.loads(raw_response)
        return parsed.get("translations", [])

    except Exception as e:
        logger.error("Translation failed: %s", e)
        return [{"cn": t, "en": t} for t in cn_titles]


def _build_result(category_name_en: str, category_name_cn: str, unique_cn_titles: List[str], sumopod_client) -> Dict:
    """Shared final step: translate titles and build the output dict."""
    logger.info("Axle Drive: %d unique subtype(s) found, translating...", len(unique_cn_titles))
    translations = _translate_titles(unique_cn_titles, sumopod_client)
    cn_to_en = {t["cn"]: t["en"] for t in translations}

    data_type = [
        {
            "type_category_name_en": cn_to_en.get(cn, cn),
            "type_category_name_cn": cn,
            "type_category_description": ""
        }
        for cn in unique_cn_titles
    ]

    result = {
        "categories": [
            {
                "category_name_en": category_name_en,
                "category_name_cn": category_name_cn,
                "category_description": "",
                "data_type": data_type
            }
        ]
    }

    logger.info(
        "Axle Drive extraction complete: 1 category, %d subtype(s)", len(data_type)
    )
    return result


# ---------------------------------------------------------------------------
# Category name helpers
# ---------------------------------------------------------------------------

_FILENAME_TO_CATEGORY = {
    "driveaxle":     ("Drive Axle",     "\u9a71\u52a8\u6865"),
    "drive_axle":    ("Drive Axle",     "\u9a71\u52a8\u6865"),
    "steeringaxle":  ("Steering Axle",  "\u8f6c\u5411\u6865"),
    "steering_axle": ("Steering Axle",  "\u8f6c\u5411\u6865"),
}


def _infer_category_from_filename(pdf_path: str) -> Tuple[str, str]:
    """
    Derive category_name_en and category_name_cn from the PDF filename.
    Falls back to ("Drive Axle", "驱动桥") if no match found.
    """
    stem = Path(pdf_path).stem.lower().replace("-", "").replace(" ", "")
    for key, (name_en, name_cn) in _FILENAME_TO_CATEGORY.items():
        if key in stem:
            return name_en, name_cn
    logger.warning(
        "Could not infer axle category from filename '%s'. Defaulting to 'Drive Axle'.",
        pdf_path
    )
    return "Drive Axle", "\u9a71\u52a8\u6865"


# ---------------------------------------------------------------------------
# ZIP extraction path
# ---------------------------------------------------------------------------

def _extract_axle_drive_from_zip(
    pdf_path: str,
    sumopod_client,
    category_name_en: str,
    category_name_cn: str,
) -> Dict:
    """Extract axle drive categories from a ZIP-format PDF."""
    tmp_dir = tempfile.mkdtemp(prefix="axle_extract_")
    try:
        logger.info("Axle Drive (ZIP): extracting archive from '%s'", pdf_path)
        manifest = _extract_zip_pdf(pdf_path, tmp_dir)
        pages = manifest.get("pages", [])
        logger.info("Axle Drive (ZIP): %d pages found in manifest", len(pages))

        # Identify table pages (has_visual_content = false)
        table_pages = [p for p in pages if not p.get("has_visual_content", True)]
        logger.info("Axle Drive (ZIP): %d table pages to process", len(table_pages))

        seen: dict = {}
        for page_info in table_pages:
            image_filename = page_info.get("image", {}).get("path")
            if not image_filename:
                continue

            image_path = str(Path(tmp_dir) / image_filename)
            page_num = page_info.get("page_number", "?")

            raw_title = _extract_title_from_image(image_path, sumopod_client)
            if not raw_title:
                logger.debug("Page %s: no title found (diagram or blank)", page_num)
                continue

            normalised = _normalise_title(raw_title)
            if not normalised:
                continue

            if normalised not in seen:
                seen[normalised] = True
                logger.info("Page %s: new subtype: '%s'", page_num, normalised)
            else:
                logger.debug("Page %s: duplicate title, skipping", page_num)

        return _build_result(category_name_en, category_name_cn, list(seen.keys()), sumopod_client)

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Real PDF extraction path
# ---------------------------------------------------------------------------

def _extract_axle_drive_from_real_pdf(
    pdf_path: str,
    sumopod_client,
    category_name_en: str,
    category_name_cn: str,
    dpi: int = 150,
) -> Dict:
    """
    Extract axle drive categories from a standard (non-ZIP) PDF.
    Renders each page to JPEG via PyMuPDF, then uses vision AI to read the title.
    """
    import fitz

    doc = fitz.open(pdf_path)
    total_pages = len(doc)
    logger.info("Axle Drive (real PDF): %d pages to scan in '%s'", total_pages, pdf_path)

    seen: dict = {}
    mat = fitz.Matrix(dpi / 72, dpi / 72)

    for page_index in range(total_pages):
        page = doc[page_index]
        pix = page.get_pixmap(matrix=mat, alpha=False)
        b64 = base64.b64encode(pix.tobytes("jpeg")).decode("utf-8")

        logger.info(
            "Axle Drive (real PDF): vision call for page %d/%d",
            page_index + 1, total_pages
        )
        raw_title = _extract_title_from_b64(b64, sumopod_client)
        logger.info(
            "Axle Drive (real PDF): vision complete for page %d/%d — raw_title=%r",
            page_index + 1, total_pages, raw_title
        )

        if not raw_title:
            logger.debug("Page %d: no title (diagram or blank)", page_index + 1)
            continue

        normalised = _normalise_title(raw_title)
        if not normalised:
            continue

        if normalised not in seen:
            seen[normalised] = True
            logger.info("Page %d: new subtype: '%s'", page_index + 1, normalised)
        else:
            logger.debug("Page %d: duplicate title, skipping", page_index + 1)

    doc.close()
    return _build_result(category_name_en, category_name_cn, list(seen.keys()), sumopod_client)


# ---------------------------------------------------------------------------
# Main extraction function
# ---------------------------------------------------------------------------

def extract_axle_drive_categories(
    pdf_path: str,
    sumopod_client,
    category_name_en: Optional[str] = None,
    category_name_cn: Optional[str] = None,
) -> Dict:
    """
    Extract Drive Axle subtype categories from a partbook PDF.

    Automatically detects whether the PDF is ZIP-format or a real PDF and
    applies the appropriate extraction strategy.

    Args:
        pdf_path:         Path to the axle partbook PDF (ZIP or real).
        sumopod_client:   SumopodClient instance (must support vision).
        category_name_en: Override for the category English name.
        category_name_cn: Override for the category Chinese name.

    Returns:
        Dict with "categories" list containing one entry with data_type subtypes.
    """
    if not category_name_en or not category_name_cn:
        fn_en, fn_cn = _infer_category_from_filename(pdf_path)
        category_name_en = category_name_en or fn_en
        category_name_cn = category_name_cn or fn_cn

    logger.info(
        "Axle Drive: processing '%s' as category '%s' / '%s'",
        pdf_path, category_name_en, category_name_cn
    )

    if _is_zip_pdf(pdf_path):
        logger.info("Axle Drive: ZIP format detected")
        return _extract_axle_drive_from_zip(
            pdf_path, sumopod_client, category_name_en, category_name_cn
        )
    else:
        logger.info("Axle Drive: real PDF format detected, using PyMuPDF page rendering")
        return _extract_axle_drive_from_real_pdf(
            pdf_path, sumopod_client, category_name_en, category_name_cn
        )