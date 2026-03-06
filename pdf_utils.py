"""
Shared PDF utility functions used across all extractor modules.
Centralises: ZIP detection, archive extraction, image encoding, JSON parsing,
and — critically — safe extraction of text from Sumopod API responses.
"""

import base64
import json
import logging
import zipfile
from pathlib import Path

logger = logging.getLogger(__name__)


def is_zip_pdf(pdf_path: str) -> bool:
    """Detect a ZIP-format PDF using magic bytes (PK = 0x504B)."""
    try:
        with open(pdf_path, "rb") as f:
            return f.read(2) == b"PK"
    except OSError as e:
        logger.warning("Format detection failed for '%s': %s", pdf_path, e)
        return False


def extract_zip_pdf(pdf_path: str, dest_dir: str) -> dict:
    """Extract a ZIP-format PDF to dest_dir and return the parsed manifest."""
    with zipfile.ZipFile(pdf_path, "r") as zf:
        zf.extractall(dest_dir)

    manifest_path = Path(dest_dir) / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"manifest.json not found in '{pdf_path}'")

    return json.loads(manifest_path.read_text(encoding="utf-8"))


def image_to_base64(image_path: str) -> str:
    """Encode an image file to a base64 string."""
    return base64.b64encode(Path(image_path).read_bytes()).decode("utf-8")


def pdf_page_to_base64(pdf_path: str, page_index: int, dpi: int = 150) -> str:
    """
    Render a single PDF page to JPEG and return as base64.
    Requires PyMuPDF (fitz); imported lazily so the module stays lightweight.
    """
    import fitz

    doc = fitz.open(pdf_path)
    pix = doc[page_index].get_pixmap(
        matrix=fitz.Matrix(dpi / 72, dpi / 72), alpha=False
    )
    data = base64.b64encode(pix.tobytes("jpeg")).decode("utf-8")
    doc.close()
    return data


def parse_llm_json(text: str) -> dict:
    """Strip markdown code fences from an LLM response, then parse JSON."""
    text = text.strip()
    if text.startswith("```"):
        text = "\n".join(
            ln for ln in text.splitlines() if not ln.strip().startswith("```")
        ).strip()
    return json.loads(text)


def extract_response_text(response) -> str:
    """
    Safely extract the text content from a Sumopod/OpenAI API response.

    The Sumopod gateway sometimes returns a *list* of ChatCompletion objects
    instead of a single object, so calling .choices directly on the return
    value raises AttributeError.  This helper handles all known shapes:

      Shape 1 — standard OpenAI object  : response.choices[0].message.content
      Shape 2 — list of ChatCompletion  : response[0].choices[0].message.content
      Shape 3 — list of dicts           : response[0]["message"]["content"]
      Shape 4 — plain dict              : response["choices"][0]["message"]["content"]

    Raises ValueError if the content cannot be extracted.
    """
    # Shape 1: standard OpenAI SDK object
    if hasattr(response, "choices"):
        return response.choices[0].message.content.strip()

    # Shape 2 & 3: raw list
    if isinstance(response, list):
        if not response:
            raise ValueError("Sumopod returned an empty list response.")
        first = response[0]
        # Shape 2: list of ChatCompletion objects
        if hasattr(first, "choices"):
            return first.choices[0].message.content.strip()
        # Shape 3: list of dicts
        if isinstance(first, dict):
            if "message" in first:
                return first["message"]["content"].strip()
            if "content" in first:
                return first["content"].strip()
            if "text" in first:
                return first["text"].strip()
        raise ValueError(
            f"Cannot extract content from list item of type {type(first)}: {first!r}"
        )

    # Shape 4: plain dict
    if isinstance(response, dict):
        choices = response.get("choices", [])
        if choices:
            return choices[0]["message"]["content"].strip()
        if "content" in response:
            return response["content"].strip()

    raise ValueError(
        f"Cannot extract content from Sumopod response of type {type(response)}: {response!r}"
    )