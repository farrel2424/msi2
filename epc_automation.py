"""
Motorsights EPC PDF Automation
Main orchestrator for PDF extraction and EPC submission.

Updated to support three partbook types:
  - cabin_chassis  : Original flow (ToC with EN+CN, 3-level hierarchy)
  - engine         : Top-right header cropping, flat 2-level hierarchy
  - transmission   : Chinese-only ToC translation, flat 2-level hierarchy
"""

import os
import json
import time
import logging
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from datetime import datetime
import hashlib

import pymupdf4llm

from sumopod_client import SumopodClient
from motorsights_epc_client import MotorsightsEPCClient
from motorsights_auth_client import MotorsightsAuthClient
from engine_transmission_extractor import extract_engine_or_transmission


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
        epc_bearer_token: Optional[str] = None,  # Deprecated — use SSO

        # Processing options
        max_retries: int = 3,
        enable_review_mode: bool = True,
        master_category_id: Optional[str] = None,
        master_category_name_en: Optional[str] = None,

        # NEW: Partbook type — controls extraction strategy
        partbook_type: str = "cabin_chassis",   # "cabin_chassis" | "engine" | "transmission"

        # Logging
        processed_log_file: str = "epc_processed_files.json"
    ):
        self.sumopod_base_url = sumopod_base_url or os.getenv("SUMOPOD_BASE_URL", "https://ai.sumopod.com/v1")
        self.sumopod_api_key = sumopod_api_key or os.getenv("SUMOPOD_API_KEY")
        self.sumopod_model = sumopod_model or os.getenv("SUMOPOD_MODEL", "gpt4o")
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
        self.sso_email = sso_email or os.getenv("SSO_EMAIL")
        self.sso_password = sso_password or os.getenv("SSO_PASSWORD")

        self.epc_base_url = epc_base_url or os.getenv("EPC_API_BASE_URL", "https://dev-gateway.motorsights.com/api/epc")
        self.epc_bearer_token = epc_bearer_token or os.getenv("EPC_BEARER_TOKEN")

        self.max_retries = max_retries
        self.enable_review_mode = enable_review_mode
        self.master_category_id = master_category_id or os.getenv("DEFAULT_MASTER_CATEGORY_ID")
        self.master_category_name_en = master_category_name_en

        # Validate partbook_type
        valid_types = {"cabin_chassis", "engine", "transmission"}
        self.partbook_type = partbook_type.lower().strip()
        if self.partbook_type not in valid_types:
            raise ValueError(
                f"Invalid partbook_type '{partbook_type}'. "
                f"Must be one of: {valid_types}"
            )

        self.processed_log_file = processed_log_file

        if not self.sso_email or not self.sso_password:
            logging.warning(
                "SSO credentials not provided. "
                "SSO authentication is recommended for automatic token refresh."
            )

        if not self.master_category_id:
            raise ValueError(
                "Master Category ID is REQUIRED for EPC API. "
                "Provide via parameter or DEFAULT_MASTER_CATEGORY_ID env variable."
            )


class ProcessedFilesTracker:
    """Tracks processed files to ensure idempotency"""

    def __init__(self, log_file: str):
        self.log_file = Path(log_file)
        self.processed_files: Dict[str, dict] = {}
        self._load_log()

    def _load_log(self):
        if self.log_file.exists():
            try:
                with open(self.log_file, 'r') as f:
                    self.processed_files = json.load(f)
                logging.info(f"Loaded {len(self.processed_files)} processed file records")
            except Exception as e:
                logging.warning(f"Could not load processed files log: {e}")
                self.processed_files = {}

    def _save_log(self):
        try:
            with open(self.log_file, 'w') as f:
                json.dump(self.processed_files, f, indent=2)
        except Exception as e:
            logging.error(f"Could not save processed files log: {e}")

    def get_file_hash(self, filepath: Path) -> str:
        hasher = hashlib.sha256()
        with open(filepath, 'rb') as f:
            hasher.update(f.read())
        return hasher.hexdigest()

    def is_processed(self, filepath: Path) -> bool:
        file_hash = self.get_file_hash(filepath)
        filename = str(filepath)
        if filename in self.processed_files:
            if self.processed_files[filename].get('hash') == file_hash:
                logging.info(f"File already processed: {filename}")
                return True
            logging.info(f"File modified since last processing: {filename}")
            return False
        return False

    def mark_processed(self, filepath: Path, success: bool, details: Optional[dict] = None):
        filename = str(filepath)
        self.processed_files[filename] = {
            'hash': self.get_file_hash(filepath),
            'timestamp': datetime.now().isoformat(),
            'success': success,
            'details': details or {}
        }
        self._save_log()


class EPCPDFAutomation:
    """Main orchestrator for EPC PDF automation"""

    def __init__(self, config: EPCAutomationConfig):
        self.config = config
        self.logger = self._setup_logging()
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
        self.epc_client = MotorsightsEPCClient(
            base_url=config.epc_base_url,
            auth_client=MotorsightsAuthClient(
                gateway_url=config.sso_gateway_url,
                email=config.sso_email,
                password=config.sso_password
            ) if config.sso_email and config.sso_password else None,
            bearer_token=config.epc_bearer_token
        )

    def _setup_logging(self) -> logging.Logger:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('epc_automation.log')
            ]
        )
        return logging.getLogger(__name__)

    # ------------------------------------------------------------------
    # Extraction routing — the key new method
    # ------------------------------------------------------------------

    def _extract_data(self, pdf_path: Path, custom_prompt: Optional[str] = None) -> Dict:
        """
        Route extraction to the correct strategy based on partbook_type.

        cabin_chassis → original pymupdf4llm → Sumopod markdown extraction
        engine        → PyMuPDF top-right crop → regex split (0 AI tokens)
        transmission  → PyMuPDF ToC pages → Sumopod translation (1 small AI call)
        """
        ptype = self.config.partbook_type

        if ptype == "cabin_chassis":
            self.logger.info("Strategy: Cabin & Chassis — full markdown extraction via Sumopod")
            markdown_text = pymupdf4llm.to_markdown(str(pdf_path))
            self.logger.info(f"Converted to markdown ({len(markdown_text)} chars)")
            return self.sumopod.extract_catalog_data(
                markdown_text,
                custom_prompt=custom_prompt
            )

        elif ptype in ("engine", "transmission"):
            self.logger.info(
                f"Strategy: {ptype.title()} — "
                + ("top-right crop (0 AI tokens)" if ptype == "engine"
                   else "ToC translation (1 AI call)")
            )
            return extract_engine_or_transmission(
                pdf_path=str(pdf_path),
                partbook_type=ptype,
                sumopod_client=self.sumopod
            )

        else:
            raise ValueError(f"Unknown partbook_type: '{ptype}'")

    # ------------------------------------------------------------------
    # Main processing
    # ------------------------------------------------------------------

    def process_pdf(
        self,
        pdf_path: Path,
        master_category_id: Optional[str] = None,
        master_category_name_en: Optional[str] = None,
        auto_submit: bool = None,
        custom_prompt: Optional[str] = None
    ) -> Dict:
        self.logger.info("=" * 80)
        self.logger.info(f"Processing PDF: {pdf_path}  [type={self.config.partbook_type}]")

        result = {
            'filename': str(pdf_path),
            'partbook_type': self.config.partbook_type,
            'success': False,
            'stage': None,
            'error': None,
            'extracted_data': None,
            'epc_submission': None,
            'review_required': False
        }

        if master_category_id is None:
            master_category_id = self.config.master_category_id
        if master_category_name_en is None:
            master_category_name_en = self.config.master_category_name_en
        if not master_category_id:
            result['error'] = "Master Category ID is required but not provided"
            self.logger.error(result['error'])
            return result

        if auto_submit is None:
            auto_submit = not self.config.enable_review_mode

        try:
            if self.tracker.is_processed(pdf_path):
                result['stage'] = 'skipped'
                result['success'] = True
                result['error'] = 'Already processed (idempotency check)'
                return result

            # ---- Extraction stage ----
            result['stage'] = 'extraction'
            self.logger.info(f"Stage: extraction ({self.config.partbook_type})")
            extracted_data = self._extract_data(pdf_path, custom_prompt=custom_prompt)
            result['extracted_data'] = extracted_data
            self.logger.info(
                f"Extracted {len(extracted_data.get('categories', []))} categories"
            )

            if not auto_submit:
                result['stage'] = 'pending_review'
                result['review_required'] = True
                result['success'] = True
                self.logger.info("✓ Extraction complete — awaiting manual review")
                return result

            # ---- Submission stage ----
            result['stage'] = 'submitting'
            self.logger.info("Stage: submitting to EPC API")
            success, epc_results = self.submit_to_epc(
                extracted_data,
                master_category_id=master_category_id,
                master_category_name_en=master_category_name_en
            )
            result['epc_submission'] = epc_results

            if success:
                result['success'] = True
                result['stage'] = 'completed'
                self.logger.info("✓ EPC submission successful")
                self.tracker.mark_processed(pdf_path, success=True, details={'epc_results': epc_results})
            else:
                result['error'] = f"EPC submission had {len(epc_results['errors'])} errors"
                self.logger.error(f"✗ {result['error']}")
                self.tracker.mark_processed(pdf_path, success=False)

        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"✗ Error at stage '{result['stage']}': {e}", exc_info=True)
            self.tracker.mark_processed(pdf_path, success=False, details={'error': str(e)})

        return result

    def submit_to_epc(
        self,
        extracted_data: Dict,
        master_category_id: Optional[str] = None,
        master_category_name_en: Optional[str] = None
    ) -> Tuple[bool, Dict]:
        """Submit extracted data to Motorsights EPC."""
        self.logger.info("Submitting reviewed data to Motorsights EPC")

        if master_category_id is None:
            master_category_id = self.config.master_category_id
        if master_category_name_en is None:
            master_category_name_en = self.config.master_category_name_en
        if not master_category_id:
            raise ValueError("Master Category ID is required for EPC submission")

        # Route to the correct EPC client method based on partbook type
        if self.config.partbook_type in ("engine", "transmission"):
            # Flat structure — categories only, no type_categories
            return self.epc_client.batch_create_flat_categories(
                catalog_data=extracted_data,
                master_category_id=master_category_id,
                master_category_name_en=master_category_name_en
            )
        else:
            # Full structure — categories + type_categories (cabin_chassis)
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
        self.logger.info(f"Starting batch processing of directory: {directory}")
        pattern = "**/*.pdf" if recursive else "*.pdf"
        pdf_files = list(directory.glob(pattern))
        self.logger.info(f"Found {len(pdf_files)} PDF files")

        results = []
        for idx, pdf_path in enumerate(pdf_files, 1):
            self.logger.info(f"\nProcessing file {idx}/{len(pdf_files)}")
            result = self.process_pdf(
                pdf_path,
                master_category_id=master_category_id,
                master_category_name_en=master_category_name_en,
                auto_submit=auto_submit
            )
            results.append(result)
            if idx < len(pdf_files):
                time.sleep(1)

        successful = sum(1 for r in results if r['success'])
        failed = len(results) - successful
        pending = sum(1 for r in results if r.get('review_required'))

        self.logger.info("\n" + "=" * 80)
        self.logger.info("BATCH PROCESSING SUMMARY")
        self.logger.info("=" * 80)
        self.logger.info(f"Total: {len(results)} | Success: {successful} | "
                         f"Failed: {failed} | Pending Review: {pending}")

        return results