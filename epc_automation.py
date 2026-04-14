"""
epc_automation.py
FIXES:
  1. EPCAutomationConfig: engine_manufacturer restored (required by epc_web_ui.py)
  2. _extract_data() engine/weichai: calls extract_weichai_engine_categories()
     which now parses TOC pages and returns 3-level hierarchy (data_type)
  3. _extract_data() engine/cummins: UNCHANGED from original
  4. submit_to_epc() engine:
       weichai -> batch_create_type_categories_and_categories (3-level)
       cummins -> batch_create_flat_categories                (2-level, unchanged)

FIX (2026-04-10): process_parts() engine/cummins now passes force_vision=True
  to extract_engine_parts(). Cummins PDFs contain text on the cover/foreword
  pages (528+ chars) that causes auto-detect to choose TEXT PATH. However,
  the TEXT PATH layout parser is designed for a different header format and
  returns 0 categories for Cummins. Vision AI correctly handles all Cummins
  page types (diagram-only, text-table, mixed). force_vision=True bypasses
  the auto-detect for Cummins only; all other paths are unaffected.

FIX v4 (2026-04-14): cabin_chassis Stage 1 per-page TOC extraction.
  ROOT CAUSE: joining all TOC pages into one string and calling
  extract_catalog_data() once generated 5 000–8 000+ output tokens for a
  13-category catalog. The default sumopod_max_tokens=2000 silently truncated
  the response → only the first ~3 categories were returned.
  FIX: process each TOC page individually and accumulate categories with
  deduplication. Each call now outputs ≈500-1 000 tokens (one page worth of
  categories). Fallback path and all other partbook types are unaffected.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
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
from axle_drive_parts_extractor import (
    extract_axle_drive_categories_text,
    extract_axle_drive_parts,
)
from cabin_chassis_parts_extractor import (
    extract_cabin_chassis_parts,
    extract_cabin_chassis_categories,
)
from transmission_parts_extractor import extract_transmission_parts
from engine_parts_extractor import extract_engine_parts
from axle_drive_parts_extractor import extract_axle_drive_parts


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
        engine_manufacturer: str = "cummins",
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
        self.engine_manufacturer     = engine_manufacturer.lower().strip()
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

    # ─────────────────────────────────────────────────────────────────────────
    # Internal helper: per-page TOC extraction + accumulation (cabin_chassis)
    # ─────────────────────────────────────────────────────────────────────────

    def _extract_cabin_chassis_categories_from_toc_pages(
        self,
        toc_texts: List[str],
        custom_prompt: Optional[str] = None,
    ) -> Dict:
        """
        FIX v4: Process each TOC page individually and accumulate categories.

        PROBLEM SOLVED:
          Sending all TOC pages as one string generated 5 000–8 000+ JSON output
          tokens for a 13-category catalog. The API cap (sumopod_max_tokens,
          default 2 000) silently truncated the response → only the first ~3
          categories were returned with no error.

        APPROACH:
          • Filter out non-TOC pages (cover, foreword, section dividers) by
            checking for at least one DC-coded entry (regex DC\\d{8,}).
          • Call extract_catalog_data() once per TOC page. Each call produces
            ≈500-1 000 tokens — comfortably within any token limit.
          • Accumulate categories with deduplication:
              - New category_name_en → append to list.
              - Known category_name_en → merge any new data_type subtypes.

        Returns: {"categories": [...]}  (same shape as extract_catalog_data)
        """
        _DC_CODE_RE = re.compile(r'DC\d{8,}')

        all_categories: List[Dict] = []
        seen_cat_en: Dict[str, int] = {}   # cat_en → index in all_categories

        for pg_n, pg_text in enumerate(toc_texts, 1):
            if not pg_text.strip():
                continue

            # Skip non-TOC pages: cover, foreword, section-divider pages.
            # A genuine TOC page always has at least one DC-part-code entry.
            if not _DC_CODE_RE.search(pg_text):
                self.logger.debug(
                    "Cabin TOC page %d: no DC codes detected — skipped "
                    "(cover / foreword / section divider)",
                    pg_n,
                )
                continue

            self.logger.info(
                "Cabin TOC page %d/%d: calling LLM …", pg_n, len(toc_texts)
            )
            try:
                pg_result = self.sumopod.extract_catalog_data(
                    pg_text, custom_prompt=custom_prompt
                )
            except Exception as exc:
                self.logger.warning(
                    "Cabin TOC page %d extraction failed — skipping: %s", pg_n, exc
                )
                continue

            for cat in pg_result.get("categories", []):
                cat_en = (cat.get("category_name_en") or "").strip()
                if not cat_en:
                    continue

                if cat_en not in seen_cat_en:
                    # Brand-new category from this page
                    seen_cat_en[cat_en] = len(all_categories)
                    all_categories.append(cat)
                    self.logger.info(
                        "  [CAT+] '%s' — %d subtype(s)",
                        cat_en, len(cat.get("data_type", [])),
                    )
                else:
                    # Category already seen; merge any new subtypes
                    existing = all_categories[seen_cat_en[cat_en]]
                    existing_tc_names = {
                        t.get("type_category_name_en", "")
                        for t in existing.get("data_type", [])
                    }
                    added = 0
                    for tc in cat.get("data_type", []):
                        tc_en = tc.get("type_category_name_en", "")
                        if tc_en not in existing_tc_names:
                            existing.setdefault("data_type", []).append(tc)
                            existing_tc_names.add(tc_en)
                            added += 1
                    if added:
                        self.logger.info(
                            "  [CAT~] '%s' — merged %d new subtype(s)", cat_en, added
                        )

        self.logger.info(
            "Cabin & Chassis Stage 1 complete: %d categories extracted "
            "across %d TOC page(s).",
            len(all_categories), len(toc_texts),
        )
        return {"categories": all_categories}

    # ─────────────────────────────────────────────────────────────────────────
    # Main extraction dispatcher
    # ─────────────────────────────────────────────────────────────────────────

    def _extract_data(self, pdf_path: Path, custom_prompt: Optional[str] = None) -> Dict:
        ptype        = self.config.partbook_type
        manufacturer = self.config.engine_manufacturer

        # ── CABIN & CHASSIS ───────────────────────────────────────────────────
        if ptype == "cabin_chassis":
            self.logger.info("Strategy: Cabin & Chassis - per-page TOC extraction (v4)")

            # Step 1: collect raw text from pages up to (but not including)
            # the first actual parts-table page.
            # Stop-signal: page contains ALL THREE column headers at once.
            import fitz as _fitz_toc
            _doc_toc = _fitz_toc.open(str(pdf_path))
            _total_pdf_pages = len(_doc_toc)
            _PARTS_SIGNALS = ("序号", "编码", "名称")
            _toc_texts: List[str] = []

            for _pg_idx in range(min(20, _total_pdf_pages)):
                _pg_text = _doc_toc[_pg_idx].get_text("text")
                if all(_sig in _pg_text for _sig in _PARTS_SIGNALS):
                    self.logger.info(
                        "Parts table detected at page %d — TOC collection stops here.",
                        _pg_idx + 1,
                    )
                    break
                _toc_texts.append(_pg_text)

            _doc_toc.close()

            if _toc_texts:
                self.logger.info(
                    "Collected %d page(s) before first parts table (full PDF: %d pages).",
                    len(_toc_texts), _total_pdf_pages,
                )

                # Step 2: per-page LLM extraction (FIX v4 — avoids token truncation)
                result = self._extract_cabin_chassis_categories_from_toc_pages(
                    _toc_texts, custom_prompt=custom_prompt
                )

                # If per-page extraction returned nothing (e.g. very short PDF
                # with no DC codes before page 1), try the legacy single-call path.
                if not result.get("categories"):
                    self.logger.warning(
                        "Per-page extraction returned 0 categories — "
                        "retrying with legacy single-block call."
                    )
                    markdown_text = "\n\n".join(_toc_texts)
                    result = self.sumopod.extract_catalog_data(
                        markdown_text, custom_prompt=custom_prompt
                    )

            else:
                # No pre-parts pages detected → image-based or very short PDF.
                # Try pymupdf4llm first, then vision extraction as last resort.
                self.logger.warning(
                    "No TOC pages collected — falling back to full PDF markdown."
                )
                markdown_text = pymupdf4llm.to_markdown(str(pdf_path))
                self.logger.info("Converted to markdown (%d chars)", len(markdown_text))

                if markdown_text.strip():
                    result = self.sumopod.extract_catalog_data(
                        markdown_text, custom_prompt=custom_prompt
                    )
                else:
                    self.logger.info(
                        "Markdown empty — image-based PDF, switching to vision extraction."
                    )
                    cat_name_en = self.config.master_category_name_en or "Cabin & Chassis"
                    return extract_cabin_chassis_categories(
                        pdf_path=str(pdf_path),
                        sumopod_client=self.sumopod,
                        category_name_en=cat_name_en,
                        category_name_cn="驾驶室和底盘",
                    )

            # Build code_to_category map (shared for all cabin_chassis paths)
            code_to_category: Dict[str, str] = {}
            for cat in result.get("categories", []):
                cat_en = cat.get("category_name_en", "")
                for subtype in cat.get("data_type", []):
                    name_en = subtype.get("type_category_name_en", "")
                    parts   = name_en.split(" ", 1)
                    code    = parts[0] if len(parts) > 1 else ""
                    if code:
                        code_to_category[code] = cat_en
                    code_to_category[name_en] = cat_en
            result["code_to_category"] = code_to_category
            return result

        # ── ENGINE ────────────────────────────────────────────────────────────
        elif ptype == "engine":
            if manufacturer == "weichai":
                self.logger.info(
                    "Strategy: Engine / Weichai — TOC-based 3-level extraction"
                )
                from weichai_engine_extractor import extract_weichai_engine_categories
                result = extract_weichai_engine_categories(
                    pdf_path=str(pdf_path),
                    sumopod_client=self.sumopod,
                )
            else:
                self.logger.info(
                    "Strategy: Engine / Xian Cummins — vision AI per page (flat)"
                )
                result = extract_engine_or_transmission(
                    pdf_path=str(pdf_path),
                    partbook_type="engine",
                    sumopod_client=self.sumopod,
                )

            code_to_category = {}
            for cat in result.get("categories", []):
                cn = cat.get("category_name_cn", "")
                en = cat.get("category_name_en", "")
                if cn and en:
                    code_to_category[cn] = en
                if en:
                    code_to_category[en] = en
                for tc in cat.get("data_type", []):
                    tc_en = tc.get("type_category_name_en", "")
                    tc_cn = tc.get("type_category_name_cn", "")
                    if tc_cn and en:
                        code_to_category[tc_cn] = en
                    if tc_en and en:
                        code_to_category[tc_en] = en

            result["code_to_category"] = code_to_category
            self.logger.info(
                "_extract_data (engine/%s): %d categories, %d subtypes, %d map entries",
                manufacturer,
                len(result.get("categories", [])),
                sum(len(c.get("data_type", [])) for c in result.get("categories", [])),
                len(code_to_category),
            )
            return result

        # ── TRANSMISSION ──────────────────────────────────────────────────────
        elif ptype == "transmission":
            result = extract_engine_or_transmission(
                pdf_path=str(pdf_path),
                partbook_type="transmission",
                sumopod_client=self.sumopod
            )
            code_to_category = {}
            for cat in result.get("categories", []):
                cn = cat.get("category_name_cn", "")
                en = cat.get("category_name_en", "")
                if cn and en:
                    code_to_category[cn] = en
                if en:
                    code_to_category[en] = en
            result["code_to_category"] = code_to_category
            self.logger.info(
                "_extract_data (transmission): %d entries in code_to_category",
                len(code_to_category),
            )
            return result

        # ── AXLE DRIVE ────────────────────────────────────────────────────────
        elif ptype == "axle_drive":
            import fitz as _fitz
            _doc = _fitz.open(str(pdf_path))
            _total_chars = sum(
                len(_doc[i].get_text("text").strip())
                for i in range(min(3, len(_doc)))
            )
            _doc.close()
            _is_text_pdf = _total_chars > 50

            if _is_text_pdf:
                self.logger.info(
                    "_extract_data (axle_drive): text-based PDF detected "
                    "(%d chars) — using text extractor (no Vision AI)", _total_chars
                )
                result = extract_axle_drive_categories_text(
                    pdf_path=str(pdf_path),
                    sumopod_client=self.sumopod,
                )
            else:
                self.logger.info(
                    "_extract_data (axle_drive): image-based PDF detected "
                    "— falling back to vision extractor"
                )
                result = extract_axle_drive_categories(
                    pdf_path=str(pdf_path),
                    sumopod_client=self.sumopod,
                )

            code_to_category = {}
            for cat in result.get("categories", []):
                cat_en = cat.get("category_name_en", "")
                cat_cn = cat.get("category_name_cn", "")
                if cat_en:
                    code_to_category[cat_en] = cat_en
                if cat_cn:
                    code_to_category[cat_cn] = cat_en
                for tc in cat.get("data_type", []):
                    tc_en = tc.get("type_category_name_en", "")
                    tc_cn = tc.get("type_category_name_cn", "")
                    if tc_en:
                        code_to_category[tc_en] = cat_en
                    if tc_cn:
                        code_to_category[tc_cn] = cat_en

            subtype_cn_to_en: Dict[str, str] = {}
            for cat in result.get("categories", []):
                for tc in cat.get("data_type", []):
                    tc_en = tc.get("type_category_name_en", "")
                    tc_cn = tc.get("type_category_name_cn", "")
                    if tc_cn and tc_en and tc_cn != tc_en:
                        subtype_cn_to_en[tc_cn] = tc_en

            result["code_to_category"] = code_to_category
            result["subtype_cn_to_en"] = subtype_cn_to_en
            self.logger.info(
                "_extract_data (axle_drive): %d categories, %d subtypes, %d map entries",
                len(result.get("categories", [])),
                sum(len(c.get("data_type", [])) for c in result.get("categories", [])),
                len(code_to_category),
            )
            return result

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
            result["subtype_cn_to_en"] = extracted_data.pop("subtype_cn_to_en", {})
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

        ptype        = self.config.partbook_type
        manufacturer = self.config.engine_manufacturer

        if ptype == "transmission":
            return self.epc_client.batch_create_flat_categories(
                catalog_data            = extracted_data,
                master_category_id      = master_category_id,
                master_category_name_en = master_category_name_en
            )

        elif ptype == "engine":
            if manufacturer == "weichai":
                self.logger.info(
                    "Engine submit (Weichai) → 3-level "
                    "(batch_create_type_categories_and_categories)"
                )
                return self.epc_client.batch_create_type_categories_and_categories(
                    catalog_data            = extracted_data,
                    master_category_id      = master_category_id,
                    master_category_name_en = master_category_name_en
                )
            else:
                self.logger.info(
                    "Engine submit (Xian Cummins) → 2-level flat "
                    "(batch_create_flat_categories)"
                )
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

    # ── Stage 2: Parts Management ─────────────────────────────────────────────

    def process_parts(
        self,
        pdf_path: Path,
        master_category_id: Optional[str] = None,
        dokumen_name: Optional[str] = None,
        target_id_start: int = 1,
        auto_submit: bool = True,
        code_to_category: Optional[Dict[str, str]] = None,
        subtype_name_map: Optional[Dict[str, str]] = None,
        custom_prompt: Optional[str] = None,
    ) -> Dict:
        pdf_path     = Path(pdf_path)
        manufacturer = self.config.engine_manufacturer
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
                "Stage 2 - Parts extraction from '%s' (start T%03d, "
                "manufacturer=%s, custom_prompt=%s, code_to_category entries=%d)",
                pdf_path.name, target_id_start,
                manufacturer if self.config.partbook_type == "engine" else "n/a",
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
                if manufacturer == "weichai":
                    self.logger.info(
                        "Stage 2 / Engine / Weichai — text-based parts extraction"
                    )
                    from weichai_engine_extractor import extract_weichai_engine_parts
                    parts_data = extract_weichai_engine_parts(
                        pdf_path        = str(pdf_path),
                        sumopod_client  = self.sumopod,
                        target_id_start = target_id_start,
                        category_map    = code_to_category or {},
                    )
                else:
                    self.logger.info(
                        "Stage 2 / Engine / Xian Cummins — Vision AI parts extraction "
                        "(force_vision=True: bypasses false TEXT PATH detection)"
                    )
                    parts_data = extract_engine_parts(
                        pdf_path        = str(pdf_path),
                        sumopod_client  = self.sumopod,
                        target_id_start = target_id_start,
                        custom_prompt   = custom_prompt,
                        force_vision    = True,
                    )

            elif ptype == "axle_drive":
                self.logger.info(
                    "Stage 2 / Axle Drive — text-based parts extraction "
                    "(axle_drive_parts_extractor, no Vision AI)"
                )
                parts_data = extract_axle_drive_parts(
                    pdf_path        = str(pdf_path),
                    sumopod_client  = self.sumopod,
                    target_id_start = target_id_start,
                    code_to_category = code_to_category or {},
                    subtype_name_map = subtype_name_map or {},
                    custom_prompt   = custom_prompt,
                )

            else:
                raise ValueError(
                    f"process_parts() does not support partbook_type='{ptype}'. "
                    f"Supported: 'cabin_chassis', 'transmission', 'engine', 'axle_drive'"
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