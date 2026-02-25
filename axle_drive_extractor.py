"""
Axle (Drive Axle) Partbook Extractor
======================================
Handles extraction for the Drive Axle partbook type.

Supports two PDF formats:
  - ZIP-format PDF  : JPEG images + manifest.json
  - Real PDF        : standard PDF rendered page-by-page via PyMuPDF + vision AI

Each PDF maps to ONE Category (e.g. "Drive Axle") with multiple Type Categories,
one per unique table title found across the pages.

Output format (3-level: Master → Category → Type Category):
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

import json
import logging
import re
import shutil
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from pdf_utils import (
    extract_zip_pdf,
    image_to_base64,
    is_zip_pdf,
    parse_llm_json,
    pdf_page_to_base64,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Title normalisation
# ---------------------------------------------------------------------------

_TABLE_PREFIX_RE = re.compile(r"^表1?\s*\d+\s*", flags=re.UNICODE)
_CONTINUATION_RE = re.compile(r"[（(]续[）)]\s*$", flags=re.UNICODE)


def _normalise_title(raw: str) -> str:
    """Strip table-number prefix and continuation suffix from a raw table title."""
    title = _TABLE_PREFIX_RE.sub("", raw.strip())
    return _CONTINUATION_RE.sub("", title).strip()


# ---------------------------------------------------------------------------
# Category name inference from filename
# ---------------------------------------------------------------------------

_FILENAME_CATEGORY_MAP = {
    "driveaxle":     ("Drive Axle",    "驱动桥"),
    "drive_axle":    ("Drive Axle",    "驱动桥"),
    "steeringaxle":  ("Steering Axle", "转向桥"),
    "steering_axle": ("Steering Axle", "转向桥"),
}


def _infer_category_from_filename(pdf_path: str) -> Tuple[str, str]:
    """Derive category names from the PDF filename; defaults to Drive Axle."""
    stem = Path(pdf_path).stem.lower().replace("-", "").replace(" ", "")
    for key, (name_en, name_cn) in _FILENAME_CATEGORY_MAP.items():
        if key in stem:
            return name_en, name_cn
    logger.warning(
        "Could not infer axle category from filename '%s'. Defaulting to 'Drive Axle'.",
        pdf_path,
    )
    return "Drive Axle", "驱动桥"


# ---------------------------------------------------------------------------
# Vision AI — title extraction
# ---------------------------------------------------------------------------

_TITLE_EXTRACTION_PROMPT = """\
You are an assistant that reads Chinese automotive parts catalog images.
Extract the title text from the top of the page, above the table columns.

Format: table-number followed by Chinese text, optionally ending with a continuation marker.

Return ONLY valid JSON, no markdown:
{ "raw_title": "<full title text>" }

If this page is a diagram (no table), return:
{ "raw_title": null }"""

_TRANSLATION_PROMPT = """\
You are a professional automotive parts catalog translator (Chinese to English).
Translate each Chinese title into clear, professional English.
These are table titles from a heavy-truck axle parts catalog.

Return ONLY valid JSON, no markdown:
{
  "translations": [
    { "cn": "<original Chinese>", "en": "<English translation>" }
  ]
}

Rules: same order as input · standard automotive terminology · no extra fields."""


def _extract_title_from_b64(b64: str, sumopod_client) -> Optional[str]:
    """Call vision AI to extract a table title from a base64-encoded JPEG."""
    try:
        resp = sumopod_client.client.chat.completions.create(
            model=sumopod_client.model,
            messages=[
                {"role": "system", "content": _TITLE_EXTRACTION_PROMPT},
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {"url": f"data:image/jpeg;base64,{b64}", "detail": "low"},
                        },
                        {"type": "text", "text": "Extract the table title from this image."},
                    ],
                },
            ],
            temperature=0.0,
            max_tokens=200,
            timeout=60,
        )
        raw = resp.choices[0].message.content.strip()
        logger.debug("Vision response: %s", raw[:300])
        return parse_llm_json(raw).get("raw_title")
    except Exception as e:
        logger.warning("Vision call failed: %s", e)
        return None


def _translate_titles(cn_titles: List[str], sumopod_client) -> List[Dict]:
    """Translate a list of Chinese table titles to English in a single AI call."""
    if not cn_titles:
        return []

    user_msg = ("Translate these Chinese axle parts catalog table titles to English:\n"
                + json.dumps(cn_titles, ensure_ascii=False, indent=2))
    try:
        resp = sumopod_client.client.chat.completions.create(
            model=sumopod_client.model,
            messages=[
                {"role": "system", "content": _TRANSLATION_PROMPT},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.1,
            max_tokens=1000,
            timeout=60,
        )
        raw = resp.choices[0].message.content.strip()
        logger.debug("Translation response: %s", raw[:300])
        return parse_llm_json(raw).get("translations", [])
    except Exception as e:
        logger.error("Translation failed: %s", e)
        return [{"cn": t, "en": t} for t in cn_titles]


# ---------------------------------------------------------------------------
# Shared result builder
# ---------------------------------------------------------------------------

def _build_result(category_name_en: str, category_name_cn: str,
                  unique_cn_titles: List[str], sumopod_client) -> Dict:
    """Translate unique CN titles and assemble the final output dict."""
    logger.info("Axle Drive: %d unique subtype(s) found, translating...", len(unique_cn_titles))
    translations = _translate_titles(unique_cn_titles, sumopod_client)
    cn_to_en = {t["cn"]: t["en"] for t in translations}

    data_type = [
        {
            "type_category_name_en": cn_to_en.get(cn, cn),
            "type_category_name_cn": cn,
            "type_category_description": "",
        }
        for cn in unique_cn_titles
    ]

    logger.info("Axle Drive extraction complete: 1 category, %d subtype(s)", len(data_type))
    return {
        "categories": [
            {
                "category_name_en": category_name_en,
                "category_name_cn": category_name_cn,
                "category_description": "",
                "data_type": data_type,
            }
        ]
    }


# ---------------------------------------------------------------------------
# Shared page-scanning loop
# ---------------------------------------------------------------------------

def _collect_unique_titles(pages_b64: List[Tuple], sumopod_client) -> List[str]:
    """Scan (label, b64) image pairs and return deduplicated normalised CN titles."""
    seen: Dict[str, bool] = {}
    for page_label, b64 in pages_b64:
        raw_title = _extract_title_from_b64(b64, sumopod_client)
        if not raw_title:
            logger.debug("Page %s: no title (diagram or blank)", page_label)
            continue

        normalised = _normalise_title(raw_title)
        if not normalised:
            continue

        if normalised not in seen:
            seen[normalised] = True
            logger.info("Page %s: new subtype: '%s'", page_label, normalised)
        else:
            logger.debug("Page %s: duplicate title, skipping", page_label)

    return list(seen.keys())


# ---------------------------------------------------------------------------
# ZIP extraction path
# ---------------------------------------------------------------------------

def _extract_axle_drive_from_zip(pdf_path: str, sumopod_client,
                                  category_name_en: str, category_name_cn: str) -> Dict:
    tmp_dir = tempfile.mkdtemp(prefix="axle_extract_")
    try:
        logger.info("Axle Drive (ZIP): extracting archive from '%s'", pdf_path)
        manifest = extract_zip_pdf(pdf_path, tmp_dir)
        pages = manifest.get("pages", [])
        table_pages = [p for p in pages if not p.get("has_visual_content", True)]
        logger.info("Axle Drive (ZIP): %d table page(s) to process", len(table_pages))

        pages_b64 = []
        for p in table_pages:
            img_path = p.get("image", {}).get("path")
            if img_path:
                pages_b64.append((
                    p.get("page_number", "?"),
                    image_to_base64(str(Path(tmp_dir) / img_path)),
                ))

        unique_titles = _collect_unique_titles(pages_b64, sumopod_client)
        return _build_result(category_name_en, category_name_cn, unique_titles, sumopod_client)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Real PDF extraction path
# ---------------------------------------------------------------------------

def _extract_axle_drive_from_real_pdf(pdf_path: str, sumopod_client,
                                       category_name_en: str, category_name_cn: str,
                                       dpi: int = 150) -> Dict:
    import fitz

    doc = fitz.open(pdf_path)
    total_pages = len(doc)
    doc.close()
    logger.info("Axle Drive (real PDF): %d pages to scan in '%s'", total_pages, pdf_path)

    pages_b64 = [
        (i + 1, pdf_page_to_base64(pdf_path, i, dpi=dpi))
        for i in range(total_pages)
    ]
    unique_titles = _collect_unique_titles(pages_b64, sumopod_client)
    return _build_result(category_name_en, category_name_cn, unique_titles, sumopod_client)


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

    Automatically detects ZIP vs. real PDF format and applies the correct strategy.

    Args:
        pdf_path:         Path to the axle partbook PDF.
        sumopod_client:   SumopodClient instance (must support vision).
        category_name_en: Override for the category English name (inferred from filename if omitted).
        category_name_cn: Override for the category Chinese name (inferred from filename if omitted).

    Returns:
        Dict with "categories" list containing one entry with data_type subtypes.
    """
    fn_en, fn_cn = _infer_category_from_filename(pdf_path)
    category_name_en = category_name_en or fn_en
    category_name_cn = category_name_cn or fn_cn

    logger.info("Axle Drive: processing '%s' as '%s' / '%s'",
                pdf_path, category_name_en, category_name_cn)

    if is_zip_pdf(pdf_path):
        logger.info("Axle Drive: ZIP format detected")
        return _extract_axle_drive_from_zip(pdf_path, sumopod_client,
                                             category_name_en, category_name_cn)

    logger.info("Axle Drive: real PDF format detected")
    return _extract_axle_drive_from_real_pdf(pdf_path, sumopod_client,
                                              category_name_en, category_name_cn)