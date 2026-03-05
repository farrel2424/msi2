import json
import logging
import os
from pathlib import Path
from typing import Dict, Optional, Tuple

from cabin_chassis_parts_extractor import CabinChassisPartsExtractor
from motorsights_auth_client import MotorsightsAuthClient
from motorsights_epc_client import MotorsightsEPCClient
from sumopod_client import SumopodClient


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

class PartsSubmitterConfig:
    """Configuration — falls back to environment variables."""

    def __init__(
        self,
        master_category_id: Optional[str] = None,
        dokumen_name: Optional[str] = None,
        epc_base_url: Optional[str] = None,
        sso_email: Optional[str] = None,
        sso_password: Optional[str] = None,
        sso_gateway_url: Optional[str] = None,
        sumopod_base_url: Optional[str] = None,
        sumopod_api_key: Optional[str] = None,
        sumopod_model: Optional[str] = None,
        default_unit: str = "pcs",
        dry_run: bool = False,
    ):
        self.master_category_id = (
            master_category_id
            or os.getenv("MASTER_CATEGORY_CABIN_CHASSIS_ID", "")
        )
        self.dokumen_name = dokumen_name or os.getenv(
            "CABIN_CHASSIS_DOKUMEN_NAME", "Cabin & Chassis Manual"
        )
        self.epc_base_url = epc_base_url or os.getenv(
            "EPC_API_BASE_URL", "https://dev-gateway.motorsights.com/api/epc"
        )
        self.sso_email = sso_email or os.getenv("SSO_EMAIL", "")
        self.sso_password = sso_password or os.getenv("SSO_PASSWORD", "")
        self.sso_gateway_url = sso_gateway_url or os.getenv(
            "SSO_GATEWAY_URL", "https://dev-gateway.motorsights.com"
        )
        self.sumopod_base_url = sumopod_base_url or os.getenv(
            "SUMOPOD_BASE_URL", "https://ai.sumopod.com/v1"
        )
        self.sumopod_api_key = sumopod_api_key or os.getenv("SUMOPOD_API_KEY", "")
        self.sumopod_model = sumopod_model or os.getenv("SUMOPOD_MODEL", "gpt-4o")
        self.default_unit = default_unit
        self.dry_run = dry_run

        if not self.master_category_id:
            raise ValueError(
                "master_category_id is required. "
                "Set MASTER_CATEGORY_CABIN_CHASSIS_ID in .env or pass directly."
            )


# ---------------------------------------------------------------------------
# Main submitter class
# ---------------------------------------------------------------------------

class CabinChassisPartsSubmitter:
    """
    Runs the full Cabin & Chassis parts management pipeline:
      1. Extract parts tables from PDF via AI
      2. Deduplicate and assign T-IDs
      3. Optionally preview (dry-run) or submit to EPC API
    """

    def __init__(self, config: PartsSubmitterConfig):
        self.config = config
        self.logger = self._setup_logging()

        self.sumopod = SumopodClient(
            base_url=config.sumopod_base_url,
            api_key=config.sumopod_api_key,
            model=config.sumopod_model,
        )

        auth_client = None
        if config.sso_email and config.sso_password:
            auth_client = MotorsightsAuthClient(
                gateway_url=config.sso_gateway_url,
                email=config.sso_email,
                password=config.sso_password,
            )

        self.epc = MotorsightsEPCClient(
            base_url=config.epc_base_url,
            auth_client=auth_client,
        )

        self.extractor = CabinChassisPartsExtractor(sumopod_client=self.sumopod)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        pdf_path: str,
        category_name_en: str,
        dokumen_name: Optional[str] = None,
        save_extracted_json: Optional[str] = None,
    ) -> Dict:
        """
        Full pipeline run for a single PDF.

        Args:
            pdf_path:            Path to the partbook PDF
            category_name_en:    Category name to submit under (e.g. "Frame System")
            dokumen_name:        Override dokumen_name from config
            save_extracted_json: If provided, save extracted JSON to this path

        Returns:
            Result dict with keys: success, stage, extracted_data, submission_results, error
        """
        dokumen = dokumen_name or self.config.dokumen_name
        result = {
            "success": False,
            "stage": None,
            "extracted_data": None,
            "submission_results": None,
            "error": None,
        }

        # ── Stage 1: Extract ──────────────────────────────────────────
        try:
            result["stage"] = "extraction"
            self.logger.info("=" * 70)
            self.logger.info("PDF: %s  |  Category: %s", pdf_path, category_name_en)
            self.logger.info("=" * 70)

            extracted = self.extractor.extract_from_pdf(pdf_path)
            result["extracted_data"] = extracted

            # extracted = {"subtypes": [...]}  — unwrap for the summary log
            total_parts = sum(len(s["parts"]) for s in extracted.get("subtypes", []))
            self.logger.info(
                "Extracted %d subtype(s), %d total part(s).",
                len(extracted.get("subtypes", [])),
                total_parts,
            )

            if save_extracted_json:
                Path(save_extracted_json).write_text(
                    json.dumps(extracted, indent=2, ensure_ascii=False), encoding="utf-8"
                )
                self.logger.info("Extracted data saved to: %s", save_extracted_json)

            self._log_extraction_summary(extracted)

        except Exception as e:
            result["error"] = str(e)
            self.logger.error("Extraction failed: %s", e, exc_info=True)
            return result

        # ── Stage 2: Submit (or dry-run) ──────────────────────────────
        if self.config.dry_run:
            self.logger.info("DRY RUN — skipping API submission.")
            result.update(success=True, stage="dry_run")
            return result

        try:
            result["stage"] = "submission"

            # ✅ FIX 1: removed invalid kwargs `category_name_en` and `default_unit`
            #    Real signature: batch_submit_parts(parts_data, master_category_id,
            #                    dokumen_name, category_id=None, subtype_id_map=None)
            #
            # ✅ FIX 2: pass extracted.get("subtypes", []) — the List[Dict] that
            #    batch_submit_parts iterates over, NOT the whole dict.
            overall_success, sub_results = self.epc.batch_submit_parts(
                parts_data=extracted.get("subtypes", []),
                master_category_id=self.config.master_category_id,
                dokumen_name=dokumen,
            )
            result["submission_results"] = sub_results

            if overall_success:
                result.update(success=True, stage="completed")
                self.logger.info("All parts submitted successfully.")
            else:
                errors = sub_results.get("errors", [])
                result["error"] = f"{len(errors)} subtype(s) failed."
                self.logger.error("%s", result["error"])

        except Exception as e:
            result["error"] = str(e)
            self.logger.error("Submission failed: %s", e, exc_info=True)

        return result

    def run_from_extracted_json(
        self,
        json_path: str,
        category_name_en: str,
        dokumen_name: Optional[str] = None,
    ) -> Dict:
        """
        Submit from a previously saved extracted JSON file.
        Useful for reviewing data before submitting.
        """
        extracted = json.loads(Path(json_path).read_text(encoding="utf-8"))
        dokumen = dokumen_name or self.config.dokumen_name

        if self.config.dry_run:
            self._log_extraction_summary(extracted)
            return {"success": True, "stage": "dry_run", "extracted_data": extracted}

        # ✅ FIX 1 & 2 applied here as well
        overall_success, sub_results = self.epc.batch_submit_parts(
            parts_data=extracted.get("subtypes", []),
            master_category_id=self.config.master_category_id,
            dokumen_name=dokumen,
        )
        return {
            "success": overall_success,
            "stage": "completed" if overall_success else "partial",
            "submission_results": sub_results,
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _log_extraction_summary(self, extracted: Dict) -> None:
        """Print a human-readable table of extracted parts per subtype."""
        self.logger.info("\n%s\nEXTRACTION SUMMARY\n%s", "─" * 60, "─" * 60)
        for subtype in extracted.get("subtypes", []):
            en = subtype.get("subtype_name_en", "(unknown)")
            cn = subtype.get("subtype_name_cn", "")
            parts = subtype.get("parts", [])
            self.logger.info("  %-40s %s  ->  %d part(s)", en, cn, len(parts))
            for p in parts[:3]:
                self.logger.info(
                    "    %s  |  %-20s |  %-30s |  qty:%s",
                    p.get("target_id", "?"),
                    p.get("part_number", ""),
                    p.get("name_en", ""),
                    p.get("quantity", ""),
                )
            if len(parts) > 3:
                self.logger.info("    ... and %d more.", len(parts) - 3)
        self.logger.info("─" * 60)

    @staticmethod
    def _setup_logging() -> logging.Logger:
        logger = logging.getLogger("CabinChassisPartsSubmitter")
        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
            )
            logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger