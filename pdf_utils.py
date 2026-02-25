"""
Shared PDF utility functions used across all extractor modules.
Centralises: ZIP detection, archive extraction, image encoding, and JSON parsing.
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