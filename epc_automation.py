"""
Motorsights EPC PDF Automation
Main orchestrator for PDF extraction and EPC submission
Uses Maia Router for AI and Motorsights EPC API for data submission
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

from maia_router_client import MaiaRouterClient
from motorsights_epc_client import MotorsightsEPCClient


class EPCAutomationConfig:
    """Configuration for EPC automation"""
    
    def __init__(
        self,
        # Maia Router (AI Gateway)
        maia_endpoint: str = "https://maia.motorsights.com/v1/chat/completions",
        maia_api_key: Optional[str] = None,
        maia_model: str = "gpt-4o",
        
        # Motorsights EPC API
        epc_base_url: str = "https://dev-epc.motorsights.com",
        epc_bearer_token: Optional[str] = None,
        
        # Processing options
        max_retries: int = 3,
        enable_review_mode: bool = True,
        master_category_id: Optional[str] = None,  # Now required as UUID string
        
        # Logging
        processed_log_file: str = "epc_processed_files.json"
    ):
        self.maia_endpoint = maia_endpoint
        self.maia_api_key = maia_api_key or os.getenv("MAIA_ROUTER_API_KEY")
        self.maia_model = maia_model or os.getenv("MAIA_ROUTER_MODEL", "gpt-4o")
        
        self.epc_base_url = epc_base_url or os.getenv("EPC_API_BASE_URL")
        self.epc_bearer_token = epc_bearer_token or os.getenv("EPC_BEARER_TOKEN")
        
        self.max_retries = max_retries
        self.enable_review_mode = enable_review_mode
        self.master_category_id = master_category_id or os.getenv("DEFAULT_MASTER_CATEGORY_ID")
        
        self.processed_log_file = processed_log_file
        
        # Validate configuration
        if not self.maia_api_key:
            raise ValueError("Maia Router API key must be provided via parameter or MAIA_ROUTER_API_KEY env variable")
        if not self.epc_bearer_token:
            raise ValueError("EPC Bearer token must be provided via parameter or EPC_BEARER_TOKEN env variable")
        
        # CRITICAL: Master category ID is now REQUIRED for EPC API
        if not self.master_category_id:
            raise ValueError(
                "Master Category ID is REQUIRED for EPC API. "
                "Provide via parameter or DEFAULT_MASTER_CATEGORY_ID env variable. "
                "Format: UUID string (e.g., '123e4567-e89b-12d3-a456-426614174000')"
            )


class ProcessedFilesTracker:
    """Tracks processed files to ensure idempotency"""
    
    def __init__(self, log_file: str):
        self.log_file = Path(log_file)
        self.processed_files: Dict[str, dict] = {}
        self._load_log()
    
    def _load_log(self):
        """Load processed files log"""
        if self.log_file.exists():
            try:
                with open(self.log_file, 'r') as f:
                    self.processed_files = json.load(f)
                logging.info(f"Loaded {len(self.processed_files)} processed file records")
            except Exception as e:
                logging.warning(f"Could not load processed files log: {e}")
                self.processed_files = {}
    
    def _save_log(self):
        """Save processed files log"""
        try:
            with open(self.log_file, 'w') as f:
                json.dump(self.processed_files, f, indent=2)
        except Exception as e:
            logging.error(f"Could not save processed files log: {e}")
    
    def get_file_hash(self, filepath: Path) -> str:
        """Generate hash for file to detect changes"""
        hasher = hashlib.sha256()
        with open(filepath, 'rb') as f:
            hasher.update(f.read())
        return hasher.hexdigest()
    
    def is_processed(self, filepath: Path) -> bool:
        """Check if file has already been processed"""
        file_hash = self.get_file_hash(filepath)
        filename = str(filepath)
        
        if filename in self.processed_files:
            if self.processed_files[filename].get('hash') == file_hash:
                logging.info(f"File already processed: {filename}")
                return True
            else:
                logging.info(f"File modified since last processing: {filename}")
                return False
        return False
    
    def mark_processed(self, filepath: Path, success: bool, details: Optional[dict] = None):
        """Mark file as processed"""
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
        
        # Initialize clients
        self.maia_client = MaiaRouterClient(
            endpoint=config.maia_endpoint,
            api_key=config.maia_api_key,
            model=config.maia_model,
            max_retries=config.max_retries
        )
        
        self.epc_client = MotorsightsEPCClient(
            base_url=config.epc_base_url,
            bearer_token=config.epc_bearer_token,
            max_retries=config.max_retries
        )
    
    def _setup_logging(self) -> logging.Logger:
        """Configure logging"""
        log_level = os.getenv('LOG_LEVEL', 'INFO')
        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('epc_automation.log'),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(__name__)
    
    def process_pdf(
        self, 
        pdf_path: Path,
        master_category_id: Optional[str] = None,  # UUID string
        auto_submit: bool = None
    ) -> Dict:
        """
        Process a single PDF file
        
        Args:
            pdf_path: Path to PDF file
            master_category_id: Required master category UUID string
            auto_submit: Override review mode for this file
        
        Returns:
            Processing result dictionary with EPC submission details
        """
        self.logger.info(f"=" * 80)
        self.logger.info(f"Processing PDF: {pdf_path}")
        
        result = {
            'filename': str(pdf_path),
            'success': False,
            'stage': None,
            'error': None,
            'extracted_data': None,
            'epc_submission': None,
            'review_required': False
        }
        
        # Use config default if not specified
        if master_category_id is None:
            master_category_id = self.config.master_category_id
        
        # Validate master category ID
        if not master_category_id:
            result['error'] = "Master Category ID is required but not provided"
            self.logger.error(result['error'])
            return result
        
        if auto_submit is None:
            auto_submit = not self.config.enable_review_mode
        
        try:
            # Check if already processed
            if self.tracker.is_processed(pdf_path):
                result['stage'] = 'skipped'
                result['success'] = True
                result['error'] = 'Already processed (idempotency check)'
                return result
            
            # Stage 1: Convert PDF to Markdown
            result['stage'] = 'pdf_conversion'
            self.logger.info("Stage 1: Converting PDF to Markdown")
            markdown_text = pymupdf4llm.to_markdown(str(pdf_path))
            self.logger.info(f"Converted to markdown ({len(markdown_text)} characters)")
            
            # Stage 2: Extract data using Maia Router
            result['stage'] = 'ai_extraction'
            self.logger.info("Stage 2: Extracting catalog data via Maia Router")
            extracted_data = self.maia_client.extract_catalog_data(markdown_text)
            result['extracted_data'] = extracted_data
            
            categories_count = len(extracted_data.get('categories', []))
            subcategories_count = sum(
                len(cat.get('subcategories', [])) 
                for cat in extracted_data.get('categories', [])
            )
            
            self.logger.info(
                f"Extracted {categories_count} type categories "
                f"with {subcategories_count} subcategories"
            )
            
            # Stage 3: Review or Auto-submit
            if auto_submit:
                result['stage'] = 'epc_submission'
                self.logger.info("Stage 3: Submitting to Motorsights EPC")
                
                success, epc_results = self.epc_client.batch_create_type_categories_and_categories(
                    catalog_data=extracted_data,
                    master_category_id=master_category_id
                )
                
                result['epc_submission'] = epc_results
                
                if success:
                    result['success'] = True
                    result['stage'] = 'completed'
                    self.logger.info("✓ PDF processing and EPC submission completed successfully")
                    
                    # Mark as processed
                    self.tracker.mark_processed(
                        pdf_path,
                        success=True,
                        details={'epc_results': epc_results}
                    )
                else:
                    result['error'] = f"EPC submission had {len(epc_results['errors'])} errors"
                    self.logger.error(f"✗ EPC submission completed with errors: {result['error']}")
                    self.tracker.mark_processed(pdf_path, success=False)
            else:
                # Review mode - don't auto-submit
                result['stage'] = 'pending_review'
                result['review_required'] = True
                result['success'] = True
                self.logger.info("✓ Extraction complete - awaiting manual review")
                
                # Don't mark as processed yet - wait for review
        
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"✗ Error processing PDF at stage '{result['stage']}': {e}", exc_info=True)
            self.tracker.mark_processed(
                pdf_path,
                success=False,
                details={'error': str(e), 'stage': result['stage']}
            )
        
        return result
    
    def submit_to_epc(
        self,
        extracted_data: Dict,
        master_category_id: Optional[str] = None  # UUID string
    ) -> Tuple[bool, Dict]:
        """
        Submit extracted data to Motorsights EPC (for manual review workflow)
        
        Args:
            extracted_data: Previously extracted catalog data
            master_category_id: Required master category UUID string
        
        Returns:
            Tuple of (success, epc_results)
        """
        self.logger.info("Submitting reviewed data to Motorsights EPC")
        
        if master_category_id is None:
            master_category_id = self.config.master_category_id
        
        if not master_category_id:
            raise ValueError("Master Category ID is required for EPC submission")
        
        return self.epc_client.batch_create_type_categories_and_categories(
            catalog_data=extracted_data,
            master_category_id=master_category_id
        )
    
    def process_directory(
        self, 
        directory: Path, 
        recursive: bool = False,
        master_category_id: Optional[str] = None,  # UUID string
        auto_submit: bool = None
    ) -> List[Dict]:
        """
        Process all PDFs in a directory
        
        Args:
            directory: Directory containing PDF files
            recursive: Whether to search subdirectories
            master_category_id: Optional master category ID
            auto_submit: Override review mode
        
        Returns:
            List of processing results
        """
        self.logger.info(f"Starting batch processing of directory: {directory}")
        
        # Find all PDF files
        pattern = "**/*.pdf" if recursive else "*.pdf"
        pdf_files = list(directory.glob(pattern))
        
        self.logger.info(f"Found {len(pdf_files)} PDF files")
        
        results = []
        for idx, pdf_path in enumerate(pdf_files, 1):
            self.logger.info(f"\nProcessing file {idx}/{len(pdf_files)}")
            result = self.process_pdf(
                pdf_path,
                master_category_id=master_category_id,
                auto_submit=auto_submit
            )
            results.append(result)
            
            # Brief pause between files to avoid rate limiting
            if idx < len(pdf_files):
                time.sleep(1)
        
        # Summary
        self.logger.info("\n" + "=" * 80)
        self.logger.info("BATCH PROCESSING SUMMARY")
        self.logger.info("=" * 80)
        
        successful = sum(1 for r in results if r['success'])
        failed = len(results) - successful
        pending_review = sum(1 for r in results if r.get('review_required'))
        
        self.logger.info(f"Total files: {len(results)}")
        self.logger.info(f"Successful: {successful}")
        self.logger.info(f"Failed: {failed}")
        self.logger.info(f"Pending Review: {pending_review}")
        
        if failed > 0:
            self.logger.info("\nFailed files:")
            for result in results:
                if not result['success'] and not result.get('review_required'):
                    self.logger.info(f"  - {result['filename']}: {result['error']}")
        
        return results


def main():
    """Main entry point for EPC automation"""
    # Configuration
    config = EPCAutomationConfig(
        maia_endpoint="https://maia.motorsights.com/v1/chat/completions",
        # maia_api_key will be read from MAIA_ROUTER_API_KEY env variable
        maia_model="gpt-4o",  # or gpt-4.1, claude-3-sonnet
        
        epc_base_url="https://dev-epc.motorsights.com",
        # epc_bearer_token will be read from EPC_BEARER_TOKEN env variable
        
        max_retries=3,
        enable_review_mode=True,  # Set to False for auto-submit
        
        # REQUIRED: Master Category UUID (get from your mentor)
        master_category_id="123e4567-e89b-12d3-a456-426614174000"  # Replace with actual UUID
    )
    
    # Create automation orchestrator
    automation = EPCPDFAutomation(config)
    
    # Example: Process single file
    # result = automation.process_pdf(Path("catalog.pdf"))
    
    # Example: Process directory
    results = automation.process_directory(
        Path("./pdfs"),
        recursive=False,
        auto_submit=False  # Review mode enabled
    )
    
    # Save results
    with open('epc_processing_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print("\n✓ Processing complete. Check epc_automation.log for details.")


if __name__ == "__main__":
    main()