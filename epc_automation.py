"""
epc_automation.py  — with Stage 2 custom_prompt support
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
from transmission_parts_extractor import extract_transmission_parts
from engine_parts_extractor import extract_engine_parts


class EPCAutomationConfig:
    def __init__(
        self,
        sumopod_base_url: str = "https://ai.sumopod.com/v1",
        sumopod_api_key: Optional[str] = None,
        sumopod_model: str = "gpt4o",
        sumopod_temperature: float = 0.7,
        sumopod_max_tokens: int = 2000,
        sumopod_custom_prompt: Optional[str] = None,
        sso_gateway_url: str = "https://dev-gateway.motorsights.com",
        sso_email: Optional[str] = None,
        sso_password: Optional[str] = None,
        epc_base_url: str = "https://dev-gateway.motorsights.com/api/epc",
        epc_bearer_token: Optional[str] = None,
        max_retries: int = 3,
        enable_review_mode: bool = True,
        master_category_id: Optional[str] = None,
        master_category_name_en: Optional[str] = None,
        partbook_type: str = "cabin_chassis",
        processed_log_file: str = "epc_processed_files.json"
    ):
        self.sumopod_base_url      = sumopod_base_url or os.getenv("SUMOPOD_BASE_URL", "https://ai.sumopod.com/v1")
        self.sumopod_api_key       = sumopod_api_key or os.getenv("SUMOPOD_API_KEY")
        self.sumopod_model         = sumopod_model or os.getenv("SUMOPOD_MODEL", "gpt4o")
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

        self.max_retries             = max_retries
        self.enable_review_mode      = enable_review_mode
        self.master_category_id      = master_category_id
        self.master_category_name_en = master_category_name_en
        self.partbook_type           = partbook_type
        self.processed_log_file      = processed_log_file


class ProcessedFilesTracker:
    def __init__(self, log_file: str):
        self.log_file        = log_file
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
        filename  = str(filepath)
        file_hash = self.get_file_hash(filepath)
        if filename in self.processed_files:
            if self.processed_files[filename].get("hash") == file_hash:
                logging.info(f"File already processed: {filename}")
                return True
            logging.info(f"File modified since last processing: {filename}")
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


class EPCPDFAutomation:

    def __init__(self, config: EPCAutomationConfig):
        self.config  = config
        self.logger  = self._setup_logging()
        self.tracker = ProcessedFilesTracker(config.processed_log_file)

        self.sumopod = SumopodClient(
            base_url=config.sumopod_base_url,
            api_key=config.sumopod_api_key,
            model=config.sumopod_model,
            temperature=config.sumopod_temperature,
            max_tokens=config.sumopod_max_tokens,
            custom_system_prompt=config.sumopod_custom_prompt
        )

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
        import logging.handlers

        fmt = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        root = logging.getLogger()

        for h in root.handlers[:]:
            try:
                h.flush()
                h.close()
            except Exception:
                pass
            root.removeHandler(h)

        root.setLevel(logging.INFO)

        fh = logging.handlers.RotatingFileHandler(
            "epc_automation.log", maxBytes=10_000_000, backupCount=3, encoding="utf-8"
        )
        fh.setFormatter(fmt)
        root.addHandler(fh)

        ch = logging.StreamHandler(sys.stderr)
        ch.setFormatter(fmt)
        root.addHandler(ch)

        return logging.getLogger(__name__)

    def _extract_data(self, pdf_path: Path, custom_prompt: Optional[str] = None) -> Dict:
        ptype = self.config.partbook_type

        if ptype == "cabin_chassis":
            self.logger.info("Strategy: Cabin & Chassis - markdown extraction via pymupdf4llm")
            markdown_text = pymupdf4llm.to_markdown(str(pdf_path))
            self.logger.info("Converted to markdown (%d chars)", len(markdown_text))

            if markdown_text.strip():
                result = self.sumopod.extract_catalog_data(
                    markdown_text,
                    custom_prompt=custom_prompt
                )
                code_to_category: Dict[str, str] = {}
                for cat in result.get("categories", []):
                    cat_en = cat.get("category_name_en", "")
                    for subtype in cat.get("data_type", []):
                        name_en = subtype.get("type_category_name_en", "")
                        parts = name_en.split(" ", 1)
                        code  = parts[0] if len(parts) > 1 else ""
                        if code:
                            code_to_category[code] = cat_en
                        code_to_category[name_en] = cat_en
                result["code_to_category"] = code_to_category
                return result
            else:
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
            result = extract_engine_or_transmission(
                pdf_path=str(pdf_path),
                partbook_type=ptype,
                sumopod_client=self.sumopod
            )

            # ── FIX OPSI 1 ────────────────────────────────────────────────────
            # Bangun peta CN → EN dari hasil ekstraksi Tahap 1.
            # Peta ini akan disimpan di job_status["code_to_category"] dan
            # di-override lagi oleh api_approve_structure() dengan data yang
            # sudah diedit/diapprove user — sehingga Tahap 2 SELALU memakai
            # nama yang persis sama dengan yang sudah masuk database.
            code_to_category = {}
            for cat in result.get("categories", []):
                cn = cat.get("category_name_cn", "")
                en = cat.get("category_name_en", "")
                if cn and en:
                    code_to_category[cn] = en   # key: CN name  → value: EN name
                if en:
                    code_to_category[en] = en   # key: EN name  → value: EN name (exact-match fallback)
            result["code_to_category"] = code_to_category
            self.logger.info(
                "_extract_data (%s): built code_to_category with %d entries",
                ptype, len(code_to_category),
            )
            return result
            # ─────────────────────────────────────────────────────────────────

        elif ptype == "axle_drive":
            return extract_axle_drive_categories(
                pdf_path=str(pdf_path),
                sumopod_client=self.sumopod
            )

        else:
            raise ValueError(f"Unknown partbook_type: '{ptype}'")

    def process_pdf(
        self,
        pdf_path: Path,
        master_category_id: Optional[str] = None,
        master_category_name_en: Optional[str] = None,
        custom_prompt: Optional[str] = None,
        auto_submit: Optional[bool] = None,
    ) -> Dict:
        pdf_path = Path(pdf_path)
        if auto_submit is None:
            auto_submit = not self.config.enable_review_mode

        result: Dict = {"success": False, "stage": "init", "pdf": str(pdf_path)}

        if master_category_id is None:
            master_category_id = self.config.master_category_id
        if master_category_name_en is None:
            master_category_name_en = self.config.master_category_name_en

        try:
            result["stage"] = "extracting"
            self.logger.info("Stage 1 - Extracting categories from '%s'", pdf_path.name)

            extracted_data = self._extract_data(pdf_path, custom_prompt=custom_prompt)

            result["code_to_category"] = extracted_data.pop("code_to_category", {})
            result["extracted_data"]   = extracted_data

            self.logger.info(
                "Extracted %d categories",
                len(extracted_data.get("categories", []))
            )

            if not auto_submit:
                result["stage"]           = "pending_review"
                result["review_required"] = True
                result["success"]         = True
                return result

            result["stage"] = "submitting"
            success, epc_results = self.submit_to_epc(
                extracted_data,
                master_category_id      = master_category_id,
                master_category_name_en = master_category_name_en
            )
            result["epc_submission"] = epc_results

            if success:
                result["success"] = True
                result["stage"]   = "completed"
                self.tracker.mark_processed(
                    pdf_path, success=True, details={"epc_results": epc_results}
                )
            else:
                result["error"] = f"EPC submission had {len(epc_results.get('errors', []))} errors"
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
        if master_category_id is None:
            master_category_id = self.config.master_category_id
        if master_category_name_en is None:
            master_category_name_en = self.config.master_category_name_en
        if not master_category_id:
            raise ValueError("Master Category ID is required for EPC submission")

        if self.config.partbook_type in ("engine", "transmission"):
            return self.epc_client.batch_create_flat_categories(
                catalog_data            = extracted_data,
                master_category_id      = master_category_id,
                master_category_name_en = master_category_name_en
            )
        else:
            return self.epc_client.batch_create_type_categories_and_categories(
                catalog_data            = extracted_data,
                master_category_id      = master_category_id,
                master_category_name_en = master_category_name_en
            )

    # ── Stage 2: Parts Management ────────────────────────────────────────────

    def process_parts(
        self,
        pdf_path: Path,
        master_category_id: Optional[str] = None,
        dokumen_name: Optional[str] = None,
        target_id_start: int = 1,
        auto_submit: bool = True,
        code_to_category: Optional[Dict[str, str]] = None,
        custom_prompt: Optional[str] = None,
    ) -> Dict:
        """
        Stage 2 — Extract parts rows and optionally submit to EPC.

        Args:
            code_to_category: Peta CN → EN (atau EN → EN) yang dibangun dari
                               data Tahap 1 yang sudah diapprove.  Dikirim dari
                               api_approve_structure() di epc_web_ui.py sehingga
                               nama kategori di Tahap 2 SELALU sama dengan yang
                               sudah tersimpan di database.
            custom_prompt:    Optional override untuk Vision AI system prompt.
        """
        pdf_path = Path(pdf_path)
        result: Dict = {"success": False, "stage": "init", "pdf": str(pdf_path)}

        if master_category_id is None:
            master_category_id = self.config.master_category_id
        if not master_category_id:
            raise ValueError("master_category_id is required for Parts Management")

        if dokumen_name is None:
            dokumen_name = pdf_path.stem

        try:
            result["stage"] = "extracting_parts"
            self.logger.info(
                "Stage 2 - Parts extraction from '%s' (start T%03d, custom_prompt=%s, "
                "code_to_category entries=%d)",
                pdf_path.name, target_id_start,
                "yes" if custom_prompt else "no",
                len(code_to_category) if code_to_category else 0,
            )

            ptype = self.config.partbook_type

            if ptype == "cabin_chassis":
                parts_data = extract_cabin_chassis_parts(
                    pdf_path         = str(pdf_path),
                    sumopod_client   = self.sumopod,
                    target_id_start  = target_id_start,
                    code_to_category = code_to_category or {},
                    custom_prompt    = custom_prompt,
                )
            elif ptype == "transmission":
                parts_data = extract_transmission_parts(
                    pdf_path         = str(pdf_path),
                    sumopod_client   = self.sumopod,
                    target_id_start  = target_id_start,
                    category_map     = code_to_category or {},
                    custom_prompt    = custom_prompt,
                )
            elif ptype == "engine":
                parts_data = extract_engine_parts(
                    pdf_path        = str(pdf_path),
                    sumopod_client  = self.sumopod,
                    target_id_start = target_id_start,
                    custom_prompt   = custom_prompt,
                )
            else:
                raise ValueError(
                    f"process_parts() does not support partbook_type='{ptype}'. "
                    f"Supported: 'cabin_chassis', 'transmission', 'engine'"
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
                return result

            result["stage"] = "submitting_parts"

            success, epc_results = self.epc_client.batch_submit_parts(
                parts_data         = parts_data,
                master_category_id = master_category_id,
                dokumen_name       = dokumen_name,
            )

            result["epc_submission"] = epc_results

            if success:
                result["success"] = True
                result["stage"]   = "completed"
            else:
                errors = epc_results.get("errors", [])
                result["error"] = f"Parts submission had {len(errors)} error(s)"

        except Exception as e:
            result["error"] = str(e)
            self.logger.error(
                "[FAIL] Error at stage '%s': %s", result["stage"], e, exc_info=True
            )

        return result

    def process_directory(
        self,
        directory: Path,
        recursive: bool = False,
        master_category_id: Optional[str] = None,
        master_category_name_en: Optional[str] = None,
        auto_submit: bool = None
    ) -> List[Dict]:
        self.logger.info("Starting batch processing of directory: %s", directory)
        pattern   = "**/*.pdf" if recursive else "*.pdf"
        pdf_files = list(directory.glob(pattern))
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

        self.logger.info(
            "BATCH SUMMARY: Total=%d | Success=%d | Failed=%d | Pending=%d",
            len(results), successful, failed, pending
        )
        return results