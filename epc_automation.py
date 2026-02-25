"""
Motorsights EPC PDF Automation
Main orchestrator: PDF extraction → optional review → EPC submission.

Supported partbook types:
  cabin_chassis — full markdown extraction (3-level hierarchy)
  engine        — top-right header vision extraction (flat, 0 AI tokens for crop)
  transmission  — ToC translation via vision/text (flat, 1 AI call)
  axle_drive    — ZIP/JPEG vision extraction per table page (3-level hierarchy)
"""

import hashlib
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pymupdf4llm

from axle_drive_extractor import extract_axle_drive_categories
from engine_transmission_extractor import extract_engine_or_transmission
from motorsights_auth_client import MotorsightsAuthClient
from motorsights_epc_client import MotorsightsEPCClient
from sumopod_client import SumopodClient

_VALID_PARTBOOK_TYPES = {"cabin_chassis", "engine", "transmission", "axle_drive"}
_FLAT_PARTBOOK_TYPES = {"engine", "transmission"}


class EPCAutomationConfig:
    """Configuration for EPC automation — values fall back to environment variables."""

    def __init__(
        self,
        # Sumopod AI Gateway
        sumopod_base_url: str = "https://ai.sumopod.com/v1",
        sumopod_api_key: Optional[str] = None,
        sumopod_model: str = "gpt4o",
        sumopod_temperature: float = 0.7,
        sumopod_max_tokens: int = 2000,
        sumopod_custom_prompt: Optional[str] = None,
        # Motorsights SSO
        sso_gateway_url: str = "https://dev-gateway.motorsights.com",
        sso_email: Optional[str] = None,
        sso_password: Optional[str] = None,
        # EPC API
        epc_base_url: str = "https://dev-gateway.motorsights.com/api/epc",
        epc_bearer_token: Optional[str] = None,  # deprecated; prefer SSO
        # Processing
        max_retries: int = 3,
        enable_review_mode: bool = True,
        master_category_id: Optional[str] = None,
        master_category_name_en: Optional[str] = None,
        partbook_type: str = "cabin_chassis",
        processed_log_file: str = "epc_processed_files.json",
    ):
        self.sumopod_base_url = sumopod_base_url or os.getenv("SUMOPOD_BASE_URL", "https://ai.sumopod.com/v1")
        self.sumopod_api_key = sumopod_api_key or os.getenv("SUMOPOD_API_KEY")
        self.sumopod_model = sumopod_model or os.getenv("SUMOPOD_MODEL", "gpt4o")
        self.sumopod_custom_prompt = sumopod_custom_prompt

        self.sumopod_temperature = self._coerce(
            float, os.getenv("SUMOPOD_TEMPERATURE", str(sumopod_temperature)), sumopod_temperature
        )
        self.sumopod_max_tokens = self._coerce(
            int, os.getenv("SUMOPOD_MAX_TOKENS", str(sumopod_max_tokens)), sumopod_max_tokens
        )

        self.sso_gateway_url = sso_gateway_url or os.getenv("SSO_GATEWAY_URL", "https://dev-gateway.motorsights.com")
        self.sso_email = sso_email or os.getenv("SSO_EMAIL")
        self.sso_password = sso_password or os.getenv("SSO_PASSWORD")

        self.epc_base_url = epc_base_url or os.getenv("EPC_API_BASE_URL", "https://dev-gateway.motorsights.com/api/epc")
        self.epc_bearer_token = epc_bearer_token or os.getenv("EPC_BEARER_TOKEN")

        self.max_retries = max_retries
        self.enable_review_mode = enable_review_mode
        self.master_category_id = master_category_id or os.getenv("DEFAULT_MASTER_CATEGORY_ID")
        self.master_category_name_en = master_category_name_en

        partbook_type = partbook_type.lower().strip()
        if partbook_type not in _VALID_PARTBOOK_TYPES:
            raise ValueError(
                f"Invalid partbook_type '{partbook_type}'. Must be one of: {_VALID_PARTBOOK_TYPES}"
            )
        self.partbook_type = partbook_type
        self.processed_log_file = processed_log_file

        if not self.sso_email or not self.sso_password:
            logging.warning("SSO credentials not set — automatic token refresh unavailable.")
        if not self.master_category_id:
            raise ValueError(
                "master_category_id is required. "
                "Provide via parameter or DEFAULT_MASTER_CATEGORY_ID env variable."
            )

    @staticmethod
    def _coerce(cast, value, default):
        try:
            return cast(value)
        except (ValueError, TypeError):
            return default


class ProcessedFilesTracker:
    """Tracks processed files by content hash to ensure idempotency."""

    def __init__(self, log_file: str):
        self.log_file = Path(log_file)
        self.processed: Dict[str, dict] = {}
        self._load()

    def _load(self) -> None:
        if self.log_file.exists():
            try:
                self.processed = json.loads(self.log_file.read_text(encoding="utf-8"))
                logging.info("Loaded %d processed file record(s).", len(self.processed))
            except Exception as e:
                logging.warning("Could not load processed files log: %s", e)

    def _save(self) -> None:
        try:
            self.log_file.write_text(json.dumps(self.processed, indent=2, ensure_ascii=False), encoding="utf-8")
        except Exception as e:
            logging.error("Could not save processed files log: %s", e)

    def _hash(self, filepath: Path) -> str:
        return hashlib.sha256(filepath.read_bytes()).hexdigest()

    def is_processed(self, filepath: Path) -> bool:
        key = str(filepath)
        if key in self.processed:
            if self.processed[key].get("hash") == self._hash(filepath):
                logging.info("Already processed: %s", key)
                return True
            logging.info("File modified since last processing: %s", key)
        return False

    def mark_processed(self, filepath: Path, success: bool, details: Optional[dict] = None) -> None:
        self.processed[str(filepath)] = {
            "hash": self._hash(filepath),
            "timestamp": datetime.now().isoformat(),
            "success": success,
            "details": details or {},
        }
        self._save()


class EPCPDFAutomation:
    """Main orchestrator for EPC PDF extraction and submission."""

    def __init__(self, config: EPCAutomationConfig):
        self.config = config
        self.logger = self._setup_logging()
        self.tracker = ProcessedFilesTracker(config.processed_log_file)

        self.sumopod = SumopodClient(
            base_url=config.sumopod_base_url,
            api_key=config.sumopod_api_key,
            model=config.sumopod_model,
            temperature=config.sumopod_temperature,
            max_tokens=config.sumopod_max_tokens,
            custom_system_prompt=config.sumopod_custom_prompt,
        )

        self.epc_client = MotorsightsEPCClient(
            base_url=config.epc_base_url,
            auth_client=(
                MotorsightsAuthClient(
                    gateway_url=config.sso_gateway_url,
                    email=config.sso_email,
                    password=config.sso_password,
                )
                if config.sso_email and config.sso_password
                else None
            ),
            bearer_token=config.epc_bearer_token,
        )

    @staticmethod
    def _setup_logging() -> logging.Logger:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler(), logging.FileHandler("epc_automation.log", encoding="utf-8")],
        )
        return logging.getLogger(__name__)

    # ------------------------------------------------------------------
    # Extraction routing
    # ------------------------------------------------------------------

    def _extract_data(self, pdf_path: Path, custom_prompt: Optional[str] = None) -> Dict:
        """Route to the correct extraction strategy based on partbook_type."""
        ptype = self.config.partbook_type

        if ptype == "cabin_chassis":
            self.logger.info("Strategy: Cabin & Chassis — full markdown extraction")
            md = pymupdf4llm.to_markdown(str(pdf_path))
            self.logger.info("Converted to markdown (%d chars).", len(md))
            return self.sumopod.extract_catalog_data(md, custom_prompt=custom_prompt)

        if ptype in _FLAT_PARTBOOK_TYPES:
            self.logger.info("Strategy: %s extraction", ptype.title())
            return extract_engine_or_transmission(
                pdf_path=str(pdf_path),
                partbook_type=ptype,
                sumopod_client=self.sumopod,
            )

        if ptype == "axle_drive":
            self.logger.info("Strategy: Axle Drive — vision extraction")
            return extract_axle_drive_categories(
                pdf_path=str(pdf_path), sumopod_client=self.sumopod
            )

        raise ValueError(f"Unknown partbook_type: '{ptype}'")

    # ------------------------------------------------------------------
    # Main processing
    # ------------------------------------------------------------------

    def process_pdf(
        self,
        pdf_path: Path,
        master_category_id: Optional[str] = None,
        master_category_name_en: Optional[str] = None,
        auto_submit: Optional[bool] = None,
        custom_prompt: Optional[str] = None,
    ) -> Dict:
        self.logger.info("=" * 80)
        self.logger.info("Processing PDF: %s  [type=%s]", pdf_path, self.config.partbook_type)

        master_category_id = master_category_id or self.config.master_category_id
        master_category_name_en = master_category_name_en or self.config.master_category_name_en
        if auto_submit is None:
            auto_submit = not self.config.enable_review_mode

        result = {
            "filename": str(pdf_path),
            "partbook_type": self.config.partbook_type,
            "success": False,
            "stage": None,
            "error": None,
            "extracted_data": None,
            "epc_submission": None,
            "review_required": False,
        }

        if not master_category_id:
            result["error"] = "master_category_id is required but not provided."
            self.logger.error(result["error"])
            return result

        try:
            if self.tracker.is_processed(pdf_path):
                return {**result, "stage": "skipped", "success": True,
                        "error": "Already processed (idempotency check)."}

            result["stage"] = "extraction"
            extracted_data = self._extract_data(pdf_path, custom_prompt=custom_prompt)
            result["extracted_data"] = extracted_data
            self.logger.info("Extracted %d categories.", len(extracted_data.get("categories", [])))

            if not auto_submit:
                result.update(stage="pending_review", review_required=True, success=True)
                self.logger.info("✓ Extraction complete — awaiting manual review.")
                return result

            result["stage"] = "submitting"
            success, epc_results = self.submit_to_epc(
                extracted_data,
                master_category_id=master_category_id,
                master_category_name_en=master_category_name_en,
            )
            result["epc_submission"] = epc_results

            if success:
                result.update(success=True, stage="completed")
                self.logger.info("✓ EPC submission successful.")
                self.tracker.mark_processed(pdf_path, success=True,
                                            details={"epc_results": epc_results})
            else:
                result["error"] = f"EPC submission had {len(epc_results['errors'])} error(s)."
                self.logger.error("✗ %s", result["error"])
                self.tracker.mark_processed(pdf_path, success=False)

        except Exception as e:
            result["error"] = str(e)
            self.logger.error("✗ Error at stage '%s': %s", result["stage"], e, exc_info=True)
            self.tracker.mark_processed(pdf_path, success=False, details={"error": str(e)})

        return result

    def submit_to_epc(
        self,
        extracted_data: Dict,
        master_category_id: Optional[str] = None,
        master_category_name_en: Optional[str] = None,
    ) -> Tuple[bool, Dict]:
        """Submit reviewed extracted data to the Motorsights EPC API."""
        self.logger.info("Submitting data to Motorsights EPC.")
        master_category_id = master_category_id or self.config.master_category_id
        master_category_name_en = master_category_name_en or self.config.master_category_name_en

        if not master_category_id:
            raise ValueError("master_category_id is required for EPC submission.")

        kwargs = dict(
            catalog_data=extracted_data,
            master_category_id=master_category_id,
            master_category_name_en=master_category_name_en,
        )

        if self.config.partbook_type in _FLAT_PARTBOOK_TYPES:
            return self.epc_client.batch_create_flat_categories(**kwargs)
        return self.epc_client.batch_create_type_categories_and_categories(**kwargs)

    def process_directory(
        self,
        directory: Path,
        recursive: bool = False,
        master_category_id: Optional[str] = None,
        master_category_name_en: Optional[str] = None,
        auto_submit: Optional[bool] = None,
    ) -> List[Dict]:
        """Process all PDFs in a directory (optionally recursive)."""
        pattern = "**/*.pdf" if recursive else "*.pdf"
        pdf_files = list(directory.glob(pattern))
        self.logger.info("Found %d PDF file(s) in '%s'.", len(pdf_files), directory)

        results = []
        for idx, pdf_path in enumerate(pdf_files, 1):
            self.logger.info("\nFile %d/%d: %s", idx, len(pdf_files), pdf_path.name)
            results.append(self.process_pdf(
                pdf_path,
                master_category_id=master_category_id,
                master_category_name_en=master_category_name_en,
                auto_submit=auto_submit,
            ))
            if idx < len(pdf_files):
                time.sleep(1)

        successful = sum(1 for r in results if r["success"])
        pending = sum(1 for r in results if r.get("review_required"))
        self.logger.info(
            "\n%s\nBATCH SUMMARY — Total: %d | Success: %d | Failed: %d | Pending Review: %d\n%s",
            "=" * 80, len(results), successful, len(results) - successful, pending, "=" * 80,
        )
        return results