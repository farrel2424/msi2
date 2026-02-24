"""
Axle (Drive Axle) Partbook Extractor
======================================
Handles extraction for the Drive Axle partbook type.

Structure of the Axle partbook PDF:
  - The PDF is actually a ZIP archive containing JPEG images + a manifest.json
  - Each PDF maps to ONE Category (e.g. "Drive Axle"), whose name comes from the filename
  - Odd pages  = exploded-view diagrams
  - Even pages = parts table pages — identified by has_visual_content: false in manifest
  - Each table page has a title at the top center, above the "Description" header
    Format: 表N <Chinese Title>(续)
    Examples:
      表1 贯通式驱动桥主减速器总成爆炸图对应备件目录
      表2 贯通式驱动桥主减速器总成爆炸图对应备件目录(续)
      表5 贯通式驱动桥桥壳总成爆炸图（STR悬架）对应备件目录

Strategy:
  1. Detect the ZIP-format PDF and extract it to a temp directory.
  2. Read manifest.json to identify table pages (has_visual_content = false).
  3. For each table page, send the JPEG to the Sumopod vision-capable model
     (gpt4o) with a targeted prompt to extract only the title text.
  4. Strip the 表N prefix and (续)/(续) suffix, then deduplicate — preserving
     first-seen order.
  5. Translate each unique Chinese title to English (same AI call).
  6. Return a single-category structure with the subtypes as data_type entries.

Output format (3-level: Master → Category → Type Category):
{
  "categories": [
    {
      "category_name_en": "Drive Axle",
      "category_name_cn": "驱动桥",
      "category_description": "",
      "data_type": [
        {
          "type_category_name_en": "Pass-Through Drive Axle Main Reducer Assembly Exploded View Parts Catalog",
          "type_category_name_cn": "贯通式驱动桥主减速器总成爆炸图对应备件目录",
          "type_category_description": ""
        },
        ...
      ]
    }
  ]
}

AI token cost: one vision call per table page (typically 4–8 pages), each call
is tiny — just "what is the title of this table?" — so cost is minimal.
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
_TABLE_PREFIX_RE = re.compile(r"^表\s*\d+\s*", flags=re.UNICODE)

# Matches trailing continuation markers: (续), （续）, (续), （续）
_CONTINUATION_RE = re.compile(r"[（(]续[）)]\s*$", flags=re.UNICODE)


def _normalise_title(raw: str) -> str:
    """Strip 表N prefix and (续) suffix from a raw table title."""
    title = raw.strip()
    title = _TABLE_PREFIX_RE.sub("", title)
    title = _CONTINUATION_RE.sub("", title)
    return title.strip()


# ---------------------------------------------------------------------------
# ZIP extraction
# ---------------------------------------------------------------------------

def _is_zip_pdf(pdf_path: str) -> bool:
    """Return True if the file is actually a ZIP archive (Axle format)."""
    try:
        return zipfile.is_zipfile(pdf_path)
    except Exception:
        return False


def _extract_zip_pdf(pdf_path: str, dest_dir: str) -> dict:
    """
    Extract the ZIP-format PDF to dest_dir and return the parsed manifest.

    Returns:
        Parsed manifest dict from manifest.json.
    """
    with zipfile.ZipFile(pdf_path, "r") as zf:
        zf.extractall(dest_dir)

    manifest_path = Path(dest_dir) / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"manifest.json not found in {pdf_path}")

    with open(manifest_path, "r", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Vision AI call — extract the table title from a JPEG
# ---------------------------------------------------------------------------

TITLE_EXTRACTION_SYSTEM_PROMPT = """\
You are an assistant that reads Chinese automotive parts catalog images.
Your ONLY job is to extract the title text from the top center of the page,
which appears above the "Description" / "描述 Description" column header.

The title format is: 表N <Chinese text>(续)
Examples:
  表1 贯通式驱动桥主减速器总成爆炸图对应备件目录
  表3 贯通式驱动桥主减速器总成爆炸图对应备件目录(续)
  表5 贯通式驱动桥桥壳总成爆炸图（STR悬架）对应备件目录

Return ONLY a valid JSON object — no markdown, no explanation:
{
  "raw_title": "<the full title text exactly as it appears, including 表N and (续) if present>"
}

If this page is a diagram (no table), return:
{
  "raw_title": null
}"""


def _image_to_base64(image_path: str) -> str:
    """Encode an image file to base64 string."""
    with open(image_path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


def _extract_title_from_image(image_path: str, sumopod_client) -> Optional[str]:
    """
    Call Sumopod vision API to extract the table title from a JPEG page image.

    Args:
        image_path:     Path to the JPEG image file.
        sumopod_client: SumopodClient instance (gpt4o supports vision).

    Returns:
        The raw title string, or None if the page has no table title.
    """
    b64 = _image_to_base64(image_path)

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
                                "url": f"data:image/jpeg;base64,{b64}",
                                "detail": "low"   # Low detail = lower cost; title is large text
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
            max_tokens=200
        )

        raw_response = response.choices[0].message.content.strip()
        logger.debug(f"Vision response for {Path(image_path).name}: {raw_response}")

        # Strip markdown fences if present
        if raw_response.startswith("```"):
            lines = raw_response.splitlines()
            raw_response = "\n".join(
                ln for ln in lines if not ln.strip().startswith("```")
            ).strip()

        parsed = json.loads(raw_response)
        return parsed.get("raw_title")  # May be None for diagram pages

    except Exception as e:
        logger.warning(f"Vision call failed for {image_path}: {e}")
        return None


# ---------------------------------------------------------------------------
# Translation — CN → EN for deduplicated titles
# ---------------------------------------------------------------------------

TRANSLATION_SYSTEM_PROMPT = """\
You are a professional automotive parts catalog translator (Chinese → English).

Translate each Chinese title in the input list into clear, professional English.
These are table titles from a heavy-truck axle parts catalog.

Return ONLY a valid JSON object — no markdown, no explanation:
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
- Do NOT include prefixes like 表N or suffixes like (续) — those have already been removed.
- Do NOT add any extra fields."""


def _translate_titles(cn_titles: List[str], sumopod_client) -> List[Dict]:
    """
    Translate a list of Chinese table titles to English in a single AI call.

    Returns:
        List of {"cn": ..., "en": ...} dicts.
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
            max_tokens=1000
        )

        raw_response = response.choices[0].message.content.strip()
        logger.debug(f"Translation response: {raw_response[:300]}")

        if raw_response.startswith("```"):
            lines = raw_response.splitlines()
            raw_response = "\n".join(
                ln for ln in lines if not ln.strip().startswith("```")
            ).strip()

        parsed = json.loads(raw_response)
        return parsed.get("translations", [])

    except Exception as e:
        logger.error(f"Translation failed: {e}")
        # Fallback: return CN as EN placeholder
        return [{"cn": t, "en": t} for t in cn_titles]


# ---------------------------------------------------------------------------
# Category name helpers
# ---------------------------------------------------------------------------

# Mapping from filename keywords to category name (EN + CN)
# Extend this dict as new axle categories are added.
_FILENAME_TO_CATEGORY = {
    "driveaxle":    ("Drive Axle",    "驱动桥"),
    "drive_axle":   ("Drive Axle",    "驱动桥"),
    "steeringaxle": ("Steering Axle", "转向桥"),
    "steering_axle":("Steering Axle", "转向桥"),
}


def _infer_category_from_filename(pdf_path: str) -> Tuple[str, str]:
    """
    Derive category_name_en and category_name_cn from the PDF filename.

    Falls back to ("Drive Axle", "驱动桥") if no match is found.
    """
    stem = Path(pdf_path).stem.lower().replace("-", "").replace(" ", "")
    for key, (name_en, name_cn) in _FILENAME_TO_CATEGORY.items():
        if key in stem:
            return name_en, name_cn
    logger.warning(
        f"Could not infer axle category from filename '{pdf_path}'. "
        f"Defaulting to 'Drive Axle'."
    )
    return "Drive Axle", "驱动桥"


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
    Extract Drive Axle subtype categories (type_categories) from a ZIP-format PDF.

    Algorithm
    ---------
    1. Extract the ZIP to a temp directory.
    2. Parse manifest.json to identify table pages (has_visual_content = false).
    3. For each table page JPEG, send to Sumopod vision to extract the raw title.
    4. Normalise: strip 表N prefix and (续) suffix.
    5. Deduplicate (preserve first-seen order).
    6. Translate all unique CN titles to EN in one batch AI call.
    7. Build and return the structured output.

    Args:
        pdf_path:         Path to the ZIP-format axle partbook PDF.
        sumopod_client:   SumopodClient instance (must support vision / gpt4o).
        category_name_en: Override for the category English name (default: from filename).
        category_name_cn: Override for the category Chinese name (default: from filename).

    Returns:
        Dict with "categories" list containing one entry (the axle category)
        with its type_categories as "data_type".
    """
    if not _is_zip_pdf(pdf_path):
        raise ValueError(
            f"Expected a ZIP-format axle PDF, but '{pdf_path}' is not a ZIP archive."
        )

    # Infer category name from filename if not supplied
    if not category_name_en or not category_name_cn:
        fn_en, fn_cn = _infer_category_from_filename(pdf_path)
        category_name_en = category_name_en or fn_en
        category_name_cn = category_name_cn or fn_cn

    logger.info(
        f"Axle Drive: processing '{pdf_path}' as category "
        f"'{category_name_en}' / '{category_name_cn}'"
    )

    tmp_dir = tempfile.mkdtemp(prefix="axle_extract_")
    try:
        # Step 1: Extract ZIP
        manifest = _extract_zip_pdf(pdf_path, tmp_dir)
        pages = manifest.get("pages", [])
        logger.info(f"Axle Drive: {len(pages)} pages found in manifest")

        # Step 2: Identify table pages (has_visual_content = false)
        table_pages = [p for p in pages if not p.get("has_visual_content", True)]
        logger.info(f"Axle Drive: {len(table_pages)} table pages to process")

        # Step 3 & 4: Extract and normalise titles
        seen: dict = {}   # normalised_cn → True  (ordered dedup)
        for page_info in table_pages:
            image_filename = page_info.get("image", {}).get("path")
            if not image_filename:
                continue

            image_path = str(Path(tmp_dir) / image_filename)
            page_num = page_info.get("page_number", "?")

            raw_title = _extract_title_from_image(image_path, sumopod_client)
            if not raw_title:
                logger.debug(f"Page {page_num}: no title found (diagram or blank)")
                continue

            normalised = _normalise_title(raw_title)
            if not normalised:
                continue

            if normalised not in seen:
                seen[normalised] = True
                logger.info(f"Page {page_num}: new subtype → '{normalised}'")
            else:
                logger.debug(f"Page {page_num}: duplicate title, skipping")

        unique_cn_titles = list(seen.keys())
        logger.info(
            f"Axle Drive: {len(unique_cn_titles)} unique subtype(s) found: "
            + str(unique_cn_titles)
        )

        # Step 5: Translate CN → EN
        translations = _translate_titles(unique_cn_titles, sumopod_client)
        cn_to_en = {t["cn"]: t["en"] for t in translations}

        # Step 6: Build output structure
        data_type = []
        for cn_title in unique_cn_titles:
            en_title = cn_to_en.get(cn_title, cn_title)
            data_type.append({
                "type_category_name_en": en_title,
                "type_category_name_cn": cn_title,
                "type_category_description": ""
            })

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
            f"Axle Drive extraction complete: "
            f"1 category, {len(data_type)} subtype(s)"
        )
        return result

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)