"""
Engine & Transmission Partbook Extractor
=========================================
Handles extraction for partbook types that have a simpler flat hierarchy:
    Master Category → Category  (no Subtype/Type Category level)

Two extraction strategies:

  1. ENGINE  — No ToC. Category name appears in the TOP-RIGHT corner of every
               page as a single bilingual string, e.g.
                   "缸体管路PLUMBING,CYLINDER BLOCK"
               Strategy: crop only that region with PyMuPDF (zero AI tokens for
               the diagram body), collect unique labels, then split Chinese/English
               with regex. No AI call needed.

  2. TRANSMISSION — Chinese-only Table of Contents on the first pages.
               Strategy: extract text from those pages only, send to AI once for
               translation, return bilingual category list.

Output format for both (flat structure — no type_categories):
{
  "categories": [
    {
      "category_name_en": "Plumbing, Cylinder Block",
      "category_name_cn": "缸体管路",
      "category_description": ""
    },
    ...
  ]
}
"""

import re
import json
import logging
from pathlib import Path
from typing import List, Dict, Optional

import fitz  # PyMuPDF


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

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

# The top-right header spans roughly the right 55% of the page width and
# the top 6% of the page height. Adjust these ratios if needed for other
# Cummins partbook variants.
ENGINE_HEADER_X_RATIO = 0.42   # crop starts at 42% of page width
ENGINE_HEADER_Y_RATIO = 0.07   # crop ends at 7% of page height


def _crop_top_right_text(page: fitz.Page) -> str:
    """
    Extract raw text from only the top-right region of a PyMuPDF page.

    This is the core token-saving technique for Engine partbooks:
    instead of converting the full page to markdown and sending it to the
    LLM, we extract ~20-50 characters of header text directly from the PDF
    geometry — zero AI tokens consumed.
    """
    w = page.rect.width
    h = page.rect.height
    clip = fitz.Rect(
        ENGINE_HEADER_X_RATIO * w,   # x0 — start at ~42% width
        0,                            # y0 — top edge
        w,                            # x1 — right edge
        ENGINE_HEADER_Y_RATIO * h     # y1 — ~7% down
    )
    return page.get_textbox(clip).strip()


def _split_bilingual_engine_label(raw: str) -> Optional[Dict[str, str]]:
    """
    Split a raw Engine header label into its Chinese and English parts.

    The format observed in Cummins Engine partbooks is:
        <Chinese><ENGLISH,WITH,COMMAS>
    Examples:
        "缸体管路PLUMBING,CYLINDER BLOCK"
        "进气预热器AID,AIR HEATER STARTING"
        "燃油泵PUMP,FUEL"
        "参数牌APPROVAL,AGENCY"

    The transition point is found where CJK characters end and ASCII begins.
    """
    raw = raw.strip()
    if not raw:
        return None

    # Find the index where ASCII (English) segment starts after CJK characters
    match = re.search(r'([\u4e00-\u9fff])([\x21-\x7E])', raw)
    if not match:
        # No CJK found — treat entire string as English
        return {
            "category_name_en": raw.strip(),
            "category_name_cn": "",
            "category_description": ""
        }

    split_idx = match.start() + 1  # character after last CJK
    cn = raw[:split_idx].strip()
    en_raw = raw[split_idx:].strip()

    # Normalise English: commas in these labels act as word separators
    # e.g. "PLUMBING,CYLINDER BLOCK" → "Plumbing, Cylinder Block"
    en_parts = [p.strip() for p in en_raw.replace(",", " ").split()]
    en_clean = " ".join(p.capitalize() for p in en_parts if p)

    return {
        "category_name_en": en_clean,
        "category_name_cn": cn,
        "category_description": ""
    }


def extract_engine_categories(pdf_path: str) -> Dict:
    """
    Extract Engine partbook categories using top-right region cropping.

    Algorithm
    ---------
    1. Open PDF with PyMuPDF.
    2. For each page, crop the top-right header region and read text directly
       from the PDF — NO AI tokens consumed at this stage.
    3. Deduplicate (preserve first-seen order).
    4. Split each bilingual label into Chinese + English via regex.
    5. Return flat category list.

    AI token cost: ZERO (pure PDF geometry text extraction + regex).
    """
    doc = fitz.open(pdf_path)
    seen: dict = {}      # ordered dedup (Python 3.7+ dicts preserve insertion order)
    raw_labels: List[str] = []

    for page_num, page in enumerate(doc):
        raw = _crop_top_right_text(page)
        if not raw:
            continue

        # Header may span two lines; the category name is typically the last line
        lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
        if not lines:
            continue
        label = lines[-1]  # e.g. "缸体管路PLUMBING,CYLINDER BLOCK"

        if label and label not in seen:
            seen[label] = True
            raw_labels.append(label)
            logger.debug(f"Page {page_num + 1}: unique header '{label}'")

    doc.close()

    logger.info(
        f"Engine: found {len(raw_labels)} unique category labels "
        f"from '{pdf_path}' — 0 AI tokens consumed"
    )

    categories = []
    for label in raw_labels:
        parsed = _split_bilingual_engine_label(label)
        if parsed:
            categories.append(parsed)

    return {"categories": categories}


# ---------------------------------------------------------------------------
# TRANSMISSION extractor
# ---------------------------------------------------------------------------

TRANSMISSION_TOC_SYSTEM_PROMPT = """You are a bilingual (Chinese–English) automotive parts catalog translator.

You will receive raw text extracted from the Table of Contents of a Chinese-language
transmission parts manual. The ToC lists category names in Chinese only.

Your task:
1. Identify every category name listed in the ToC. Ignore page numbers, section
   numbers, dot leaders, headers, footers, and any non-category text.
2. Translate each Chinese category name into clear, professional English.
3. Return ONLY a valid JSON object — no markdown fences, no explanation.

OUTPUT FORMAT (strictly):
{
  "categories": [
    {
      "category_name_en": "<English translation>",
      "category_name_cn": "<original Chinese text>",
      "category_description": ""
    }
  ]
}

RULES:
- Preserve the original order from the ToC.
- Do NOT include duplicates.
- Do NOT add type_categories or any nested arrays.
- Use concise, accurate automotive/transmission terminology in English.
- Return ONLY the raw JSON object — no markdown code blocks."""


def _extract_toc_pages_text(pdf_path: str, max_toc_pages: int = 10) -> str:
    """
    Extract plain text from the first N pages of the PDF.

    For most Transmission partbooks the ToC spans pages 1–5. We cap at
    max_toc_pages to avoid sending diagram content to the AI.
    This keeps the AI payload small — typically a few hundred characters.
    """
    doc = fitz.open(pdf_path)
    total = len(doc)
    pages_to_read = min(max_toc_pages, total)

    blocks = []
    for i in range(pages_to_read):
        text = doc[i].get_text("text").strip()
        if text:
            blocks.append(f"--- Page {i + 1} ---\n{text}")

    doc.close()
    return "\n\n".join(blocks)


def extract_transmission_categories(
    pdf_path: str,
    sumopod_client,
    max_toc_pages: int = 10
) -> Dict:
    """
    Extract Transmission partbook categories from a Chinese-only Table of Contents.

    Algorithm
    ---------
    1. Open PDF with PyMuPDF and extract plain text from only the first
       max_toc_pages pages (where the ToC lives). Payload is typically
       300–800 characters — not the full PDF.
    2. Send that text to the Sumopod AI with a specialised translation prompt.
    3. Parse the JSON response.
    4. Return the flat bilingual category list.

    AI token cost: one small call (~300–800 input tokens + ~500 output tokens).
    """
    logger.info(
        f"Transmission: extracting ToC text from first {max_toc_pages} "
        f"pages of '{pdf_path}'"
    )

    toc_text = _extract_toc_pages_text(pdf_path, max_toc_pages=max_toc_pages)

    if not toc_text.strip():
        raise ValueError(
            "No text found in the first pages of the PDF. "
            "Ensure the PDF is not scanned/image-only and the ToC is present."
        )

    logger.info(
        f"Transmission: ToC text extracted ({len(toc_text)} chars). "
        f"Sending to AI for translation…"
    )

    user_message = (
        "Here is the raw text extracted from the Table of Contents of a "
        "Chinese-only transmission parts manual. "
        "Please extract and translate all category names as instructed:\n\n"
        + toc_text
    )

    response = sumopod_client.client.chat.completions.create(
        model=sumopod_client.model,
        messages=[
            {"role": "system", "content": TRANSMISSION_TOC_SYSTEM_PROMPT},
            {"role": "user",   "content": user_message}
        ],
        temperature=0.2,   # Low temperature for deterministic translation
        max_tokens=2000
    )

    raw_response = response.choices[0].message.content.strip()
    logger.debug(f"Transmission AI response preview: {raw_response[:300]}…")

    extracted = _parse_json_from_llm(raw_response)

    # Ensure description field is present on every category
    for cat in extracted.get("categories", []):
        cat.setdefault("category_description", "")

    logger.info(
        f"Transmission: extracted "
        f"{len(extracted.get('categories', []))} categories from ToC"
    )

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
        pdf_path:       Path to the partbook PDF.
        partbook_type:  "engine" or "transmission".
        sumopod_client: SumopodClient instance (required for transmission only).
        max_toc_pages:  Max pages to scan for the ToC (transmission only).

    Returns:
        Dict with "categories" list, each entry containing:
            category_name_en, category_name_cn, category_description
    """
    partbook_type = partbook_type.lower().strip()

    if partbook_type == "engine":
        return extract_engine_categories(pdf_path)

    elif partbook_type == "transmission":
        if sumopod_client is None:
            raise ValueError("sumopod_client is required for transmission extraction.")
        return extract_transmission_categories(
            pdf_path,
            sumopod_client=sumopod_client,
            max_toc_pages=max_toc_pages
        )

    else:
        raise ValueError(
            f"Unknown partbook_type '{partbook_type}'. "
            f"Use 'engine' or 'transmission'."
        )