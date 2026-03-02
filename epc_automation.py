"""
epc_automation.py
─────────────────────────────────────────────────────────────────────────────
Motorsights EPC PDF Automation - Main Orchestrator

SUPPORTED PARTBOOK TYPES & EXTRACTION STRATEGIES
──────────────────────────────────────────────────
  cabin_chassis → pymupdf4llm markdown → Sumopod full-text extraction (category
                  extraction) + fitz vision per page (parts management)
  engine        → PyMuPDF top-right crop → regex split (0 AI tokens)
  transmission  → PyMuPDF ToC pages → Sumopod translation (1 small AI call)
  axle_drive    → ZIP-of-JPEGs → vision AI per table page (1 call/page)

TWO-STAGE WORKFLOW (Cabin & Chassis)
──────────────────────────────────────
  Stage 1 – Category/Structure extraction (existing)
    Reads each table page header → Category + Type Category names
    Submits via POST /categories/create

  Stage 2 – Parts Management (new)
    Reads each table page body → parts rows grouped by subtype
    Deduplicates, merges quantities, assigns T-IDs
    Submits via POST /item_category/create  (multipart, with data_items)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pymupdf4llm

from sumopod_client import SumopodClient
from motorsights_epc_client import MotorsightsEPCClient
from motorsights_auth_client import MotorsightsAuthClient
from engine_transmission_extractor import extract_engine_or_transmission
from axle_drive_extractor import extract_axle_drive_categories
from cabin_chassis_parts_extractor import (
    extract_cabin_chassis_parts,
    extract_cabin_chassis_categories,
)


# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

class EPCAutomationConfig:
    """Configuration for EPC automation"""

    def __init__(
        self,
        # Sumopod AI Gateway
        sumopod_base_url: str = "https://ai.sumopod.com/v1",
        sumopod_api_key: Optional[str] = None,
        sumopod_model: str = "gpt4o",
        sumopod_temperature: float = 0.7,
        sumopod_max_tokens: int = 2000,
        sumopod_custom_prompt: Optional[str] = None,

        # Motorsights SSO Authentication
        sso_gateway_url: str = "https://dev-gateway.motorsights.com",
        sso_email: Optional[str] = None,
        sso_password: Optional[str] = None,

        # Motorsights EPC API
        epc_base_url: str = "https://dev-gateway.motorsights.com/api/epc",
        epc_bearer_token: Optional[str] = None,  # Deprecated - use SSO

        # Processing options
        max_retries: int = 3,
        enable_review_mode: bool = True,
        master_category_id: Optional[str] = None,
        master_category_name_en: Optional[str] = None,

        # Partbook type - controls extraction strategy
        # "cabin_chassis" | "engine" | "transmission" | "axle_drive"
        partbook_type: str = "cabin_chassis",

        # Logging
        processed_log_file: str = "epc_processed_files.json"
    ):
        self.sumopod_base_url   = sumopod_base_url or os.getenv("SUMOPOD_BASE_URL", "https://ai.sumopod.com/v1")
        self.sumopod_api_key    = sumopod_api_key or os.getenv("SUMOPOD_API_KEY")
        self.sumopod_model      = sumopod_model or os.getenv("SUMOPOD_MODEL", "gpt4o")
        self.sumopod_custom_prompt = sumopod_custom_prompt

        try:
            self.sumopod_temperature = float(os.getenv("SUMOPOD_TEMPERATURE", str(sumopod_temperature)))
        except (ValueError, TypeError):
            self.sumopod_temperature = sumopod_temperature

        try:
            self.sumopod_max_tokens = int(os.getenv("SUMOPOD_MAX_TOKENS", str(sumopod_max_tokens)))
        except (ValueError, TypeError):
            self.sumopod_max_tokens = sumopod_max_tokens

        self.sso_gateway_url = sso_gateway_url or os.getenv("SSO_GATEWAY_URL", "https://dev-gateway.motorsights.com")
        self.sso_email       = sso_email or os.getenv("SSO_EMAIL")
        self.sso_password    = sso_password or os.getenv("SSO_PASSWORD")

        self.epc_base_url     = epc_base_url or os.getenv("EPC_API_BASE_URL", "https://dev-gateway.motorsights.com/api/epc")
        self.epc_bearer_token = epc_bearer_token

        self.max_retries          = max_retries
        self.enable_review_mode   = enable_review_mode
        self.master_category_id   = master_category_id
        self.master_category_name_en = master_category_name_en
        self.partbook_type        = partbook_type
        self.processed_log_file   = processed_log_file


# ─────────────────────────────────────────────────────────────────────────────
# Processed-files tracker
# ─────────────────────────────────────────────────────────────────────────────

class ProcessedFilesTracker:
    def __init__(self, log_file: str):
        self.log_file = log_file
        self.processed_files = self._load_log()

    def _load_log(self) -> Dict:
        if Path(self.log_file).exists():
            try:
                with open(self.log_file, "r") as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def _save_log(self):
        with open(self.log_file, "w") as f:
            json.dump(self.processed_files, f, indent=2)

    def get_file_hash(self, filepath: Path) -> str:
        sha = hashlib.sha256()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                sha.update(chunk)
        return sha.hexdigest()

    def is_processed(self, filepath: Path) -> bool:
        filename = str(filepath)
        file_hash = self.get_file_hash(filepath)
        if filename in self.processed_files:
            if self.processed_files[filename].get("hash") == file_hash:
                logging.info(f"File already processed: {filename}")
                return True
            logging.info(f"File modified since last processing: {filename}")
            return False
        return False

    def mark_processed(self, filepath: Path, success: bool, details: Optional[Dict] = None):
        filename = str(filepath)
        self.processed_files[filename] = {
            "hash":      self.get_file_hash(filepath),
            "timestamp": datetime.now().isoformat(),
            "success":   success,
            "details":   details or {}
        }
        self._save_log()


# ─────────────────────────────────────────────────────────────────────────────
# Main automation class
# ─────────────────────────────────────────────────────────────────────────────

class EPCPDFAutomation:
    """Main orchestrator for EPC PDF automation"""

    def __init__(self, config: EPCAutomationConfig):
        self.config  = config
        self.logger  = self._setup_logging()
        self.tracker = ProcessedFilesTracker(config.processed_log_file)

        # Initialise Sumopod AI client
        self.sumopod = SumopodClient(
            base_url=config.sumopod_base_url,
            api_key=config.sumopod_api_key,
            model=config.sumopod_model,
            temperature=config.sumopod_temperature,
            max_tokens=config.sumopod_max_tokens,
            custom_system_prompt=config.sumopod_custom_prompt
        )

        # Initialise EPC client with SSO auth
        auth_client = None
        if config.sso_email and config.sso_password:
            auth_client = MotorsightsAuthClient(
                gateway_url=config.sso_gateway_url,
                email=config.sso_email,
                password=config.sso_password
            )

        self.epc_client = MotorsightsEPCClient(
            base_url=config.epc_base_url,
            auth_client=auth_client,
            bearer_token=config.epc_bearer_token
        )

    def _setup_logging(self) -> logging.Logger:
        import sys
        import io
        # Force UTF-8 on the console stream so non-ASCII chars don't crash
        # on Windows (cp1252 console).  Falls back if reconfiguring fails.
        try:
            utf8_stream = io.TextIOWrapper(
                sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True
            )
        except AttributeError:
            utf8_stream = sys.stdout  # already a text stream (redirected)

        fmt = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        stream_handler = logging.StreamHandler(utf8_stream)
        stream_handler.setFormatter(fmt)

        file_handler = logging.FileHandler("epc_automation.log", encoding="utf-8")
        file_handler.setFormatter(fmt)

        root = logging.getLogger()
        if not root.handlers:
            root.setLevel(logging.INFO)
            root.addHandler(stream_handler)
            root.addHandler(file_handler)

        return logging.getLogger(__name__)

    # ──────────────────────────────────────────────────────────────────────────
    # Stage 1: Category / Type-Category extraction (structure)
    # ──────────────────────────────────────────────────────────────────────────

    def _extract_data(self, pdf_path: Path, custom_prompt: Optional[str] = None) -> Dict:
        """
        Route category-structure extraction to the correct strategy.

        cabin_chassis → pymupdf4llm markdown → Sumopod full-text extraction
        engine        → PyMuPDF top-right crop → regex split (0 AI tokens)
        transmission  → PyMuPDF ToC pages → Sumopod translation (1 AI call)
        axle_drive    → ZIP-of-JPEGs → Sumopod vision per table page
        """
        ptype = self.config.partbook_type

        if ptype == "cabin_chassis":
            self.logger.info("Strategy: Cabin & Chassis - markdown extraction via pymupdf4llm")
            markdown_text = pymupdf4llm.to_markdown(str(pdf_path))
            self.logger.info("Converted to markdown (%d chars)", len(markdown_text))

            if markdown_text.strip():
                # Text-layer PDF - use the fast markdown path
                return self.sumopod.extract_catalog_data(
                    markdown_text,
                    custom_prompt=custom_prompt
                )
            else:
                # Image-based PDF (no embedded text layer) - fall back to
                # rendering each page with fitz and reading the bilingual
                # subtype header (code + EN + CN) directly from the image.
                # Uses a cabin_chassis-specific prompt that splits the header
                # correctly — no translation step needed since EN is present.
                self.logger.info(
                    "Markdown is empty - PDF is image-based. "
                    "Falling back to cabin_chassis vision extraction."
                )
                cat_name_en = self.config.master_category_name_en or "Cabin & Chassis"
                return extract_cabin_chassis_categories(
                    pdf_path=str(pdf_path),
                    sumopod_client=self.sumopod,
                    category_name_en=cat_name_en,
                    category_name_cn="驾驶室和底盘",
                )

        elif ptype in ("engine", "transmission"):
            self.logger.info(
                f"Strategy: {ptype.title()} - "
                + ("top-right crop (0 AI tokens)" if ptype == "engine"
                   else "ToC translation (1 AI call)")
            )
            return extract_engine_or_transmission(
                pdf_path=str(pdf_path),
                partbook_type=ptype,
                sumopod_client=self.sumopod
            )

        elif ptype == "axle_drive":
            self.logger.info(
                "Strategy: Axle Drive - ZIP/JPEG vision extraction "
                "(1 vision call per table page + 1 translation call)"
            )
            return extract_axle_drive_categories(
                pdf_path=str(pdf_path),
                sumopod_client=self.sumopod
            )

        else:
            raise ValueError(f"Unknown partbook_type: '{ptype}'")

    # ──────────────────────────────────────────────────────────────────────────
    # Stage 2: Parts Management extraction & submission
    # ──────────────────────────────────────────────────────────────────────────

    def process_parts(
        self,
        pdf_path: Path,
        master_category_id: Optional[str] = None,
        category_id: Optional[str] = None,
        dokumen_name: Optional[str] = None,
        subtype_id_map: Optional[Dict[str, str]] = None,
        target_id_start: int = 1,
        auto_submit: bool = True,
    ) -> Dict:
        """
        Stage 2 - Extract parts rows from the partbook and submit them to
        POST /item_category/create.

        Supports Cabin & Chassis partbooks (ZIP-of-JPEGs format).

        Args:
            pdf_path:           Path to the partbook PDF (ZIP format).
            master_category_id: UUID of the master category.
            category_id:        UUID of the Category (2-level fallback if no
                                subtype_id_map entries).
            dokumen_name:       Document name for the EPC record. Defaults to
                                the PDF filename stem.
            subtype_id_map:     Dict mapping subtype_code (or name) →
                                type_category_id UUID. Build this from the
                                results of Stage 1 (submit_to_epc).
            target_id_start:    The T-number index to start from. Pass 1 for
                                a fresh item_category, or call
                                epc_client.get_next_target_id_start(id) for
                                existing ones.
            auto_submit:        If True, submits directly. If False, returns
                                extracted data for review without posting.

        Returns:
            Dict with keys:
              success, stage, parts_data, epc_submission (if auto_submit)
        """
        pdf_path = Path(pdf_path)
        result: Dict = {"success": False, "stage": "init", "pdf": str(pdf_path)}

        if master_category_id is None:
            master_category_id = self.config.master_category_id
        if not master_category_id:
            raise ValueError("master_category_id is required for Parts Management")

        if dokumen_name is None:
            dokumen_name = pdf_path.stem  # e.g. "CabinChassis_v2"

        try:
            # ── Extract parts rows via vision AI ──────────────────────────
            result["stage"] = "extracting_parts"
            self.logger.info(
                "Stage 2 - Parts extraction from '%s' (start T%03d)",
                pdf_path.name, target_id_start
            )

            if self.config.partbook_type != "cabin_chassis":
                raise ValueError(
                    f"process_parts() only supports cabin_chassis partbooks. "
                    f"Got partbook_type='{self.config.partbook_type}'"
                )

            parts_data = extract_cabin_chassis_parts(
                pdf_path=str(pdf_path),
                sumopod_client=self.sumopod,
                target_id_start=target_id_start
            )

            result["parts_data"] = parts_data
            total_parts = sum(len(g["parts"]) for g in parts_data)
            self.logger.info(
                "Extracted %d subtype groups, %d total parts",
                len(parts_data), total_parts
            )

            if not auto_submit:
                result["stage"]           = "pending_review"
                result["review_required"] = True
                result["success"]         = True
                self.logger.info("Parts extraction complete - awaiting manual review")
                return result

            # ── Submit to EPC API ─────────────────────────────────────────
            result["stage"] = "submitting_parts"
            self.logger.info("Submitting parts to EPC ...")

            success, epc_results = self.epc_client.batch_submit_parts(
                parts_data         = parts_data,
                master_category_id = master_category_id,
                dokumen_name       = dokumen_name,
                category_id        = category_id,
                subtype_id_map     = subtype_id_map,
            )

            result["epc_submission"] = epc_results

            if success:
                result["success"] = True
                result["stage"]   = "completed"
                self.logger.info(
                    "[OK] Parts submission complete - %d item categories, %d parts",
                    len(epc_results.get("item_categories_created", [])),
                    epc_results.get("total_parts_submitted", 0)
                )
            else:
                errors = epc_results.get("errors", [])
                result["error"] = f"Parts submission had {len(errors)} error(s)"
                self.logger.error("[FAIL] %s", result["error"])

        except Exception as e:
            result["error"] = str(e)
            self.logger.error(
                "[FAIL] Error at stage '%s': %s", result["stage"], e, exc_info=True
            )

        return result

    # ──────────────────────────────────────────────────────────────────────────
    # Stage 1: Structure (Category) processing - unchanged from original
    # ──────────────────────────────────────────────────────────────────────────

    def process_pdf(
        self,
        pdf_path: Path,
        master_category_id: Optional[str] = None,
        master_category_name_en: Optional[str] = None,
        custom_prompt: Optional[str] = None,
        auto_submit: Optional[bool] = None,
    ) -> Dict:
        """
        Stage 1 - Extract and submit Category / Type Category structure.
        (Parts entry is handled separately via process_parts().)
        """
        pdf_path = Path(pdf_path)
        if auto_submit is None:
            auto_submit = not self.config.enable_review_mode

        result: Dict = {"success": False, "stage": "init", "pdf": str(pdf_path)}

        if master_category_id is None:
            master_category_id = self.config.master_category_id
        if master_category_name_en is None:
            master_category_name_en = self.config.master_category_name_en

        try:
            # ── Extraction stage ──────────────────────────────────────────
            result["stage"] = "extracting"
            self.logger.info("Stage 1 - Extracting categories from '%s'", pdf_path.name)

            extracted_data = self._extract_data(pdf_path, custom_prompt=custom_prompt)
            result["extracted_data"] = extracted_data
            self.logger.info(
                "Extracted %d categories",
                len(extracted_data.get("categories", []))
            )

            if not auto_submit:
                result["stage"]           = "pending_review"
                result["review_required"] = True
                result["success"]         = True
                self.logger.info("[OK] Extraction complete - awaiting manual review")
                return result

            # ── Submission stage ──────────────────────────────────────────
            result["stage"] = "submitting"
            self.logger.info("Stage 1 - Submitting to EPC API")
            success, epc_results = self.submit_to_epc(
                extracted_data,
                master_category_id   = master_category_id,
                master_category_name_en = master_category_name_en
            )
            result["epc_submission"] = epc_results

            if success:
                result["success"] = True
                result["stage"]   = "completed"
                self.logger.info("[OK] EPC category submission successful")
                self.tracker.mark_processed(
                    pdf_path, success=True, details={"epc_results": epc_results}
                )
            else:
                result["error"] = f"EPC submission had {len(epc_results.get('errors', []))} errors"
                self.logger.error("[FAIL] %s", result["error"])
                self.tracker.mark_processed(pdf_path, success=False)

        except Exception as e:
            result["error"] = str(e)
            self.logger.error(
                "[FAIL] Error at stage '%s': %s", result["stage"], e, exc_info=True
            )
            self.tracker.mark_processed(pdf_path, success=False, details={"error": str(e)})

        return result

    def submit_to_epc(
        self,
        extracted_data: Dict,
        master_category_id: Optional[str] = None,
        master_category_name_en: Optional[str] = None,
    ) -> Tuple[bool, Dict]:
        """Submit extracted category/type-category data to Motorsights EPC."""
        self.logger.info("Submitting reviewed data to Motorsights EPC")

        if master_category_id is None:
            master_category_id = self.config.master_category_id
        if master_category_name_en is None:
            master_category_name_en = self.config.master_category_name_en
        if not master_category_id:
            raise ValueError("Master Category ID is required for EPC submission")

        if self.config.partbook_type in ("engine", "transmission"):
            # Flat structure - categories only, no type_categories
            return self.epc_client.batch_create_flat_categories(
                catalog_data=extracted_data,
                master_category_id=master_category_id,
                master_category_name_en=master_category_name_en
            )
        else:
            # Full structure - categories + type_categories
            # (cabin_chassis AND axle_drive both use this path)
            return self.epc_client.batch_create_type_categories_and_categories(
                catalog_data=extracted_data,
                master_category_id=master_category_id,
                master_category_name_en=master_category_name_en
            )

    def process_directory(
        self,
        directory: Path,
        recursive: bool = False,
        master_category_id: Optional[str] = None,
        master_category_name_en: Optional[str] = None,
        auto_submit: bool = None
    ) -> List[Dict]:
        self.logger.info("Starting batch processing of directory: %s", directory)
        pattern    = "**/*.pdf" if recursive else "*.pdf"
        pdf_files  = list(directory.glob(pattern))
        self.logger.info("Found %d PDF files", len(pdf_files))

        results = []
        for idx, pdf_path in enumerate(pdf_files, 1):
            self.logger.info("\nProcessing file %d/%d", idx, len(pdf_files))
            result = self.process_pdf(
                pdf_path,
                master_category_id      = master_category_id,
                master_category_name_en = master_category_name_en,
                auto_submit             = auto_submit
            )
            results.append(result)
            if idx < len(pdf_files):
                time.sleep(1)

        successful = sum(1 for r in results if r["success"])
        failed     = len(results) - successful
        pending    = sum(1 for r in results if r.get("review_required"))

        self.logger.info("\n" + "=" * 80)
        self.logger.info("BATCH PROCESSING SUMMARY")
        self.logger.info("=" * 80)
        self.logger.info(
            "Total: %d | Success: %d | Failed: %d | Pending Review: %d",
            len(results), successful, failed, pending
        )
        return results