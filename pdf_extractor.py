"""
PDF Data Extraction and API Submission Automation
Extracts structured data from PDFs using GPT-4 and submits to internal API
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
from openai import OpenAI
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class PDFExtractorConfig:
    """Configuration for PDF extractor"""
    
    def __init__(
        self,
        api_endpoint: str = "https://internal-api.example.com/upload",
        bearer_token: Optional[str] = None,
        openai_api_key: Optional[str] = None,
        openai_model: str = "gpt-4",
        max_retries: int = 3,
        retry_backoff_factor: float = 2.0,
        processed_log_file: str = "processed_files.json"
    ):
        self.api_endpoint = api_endpoint
        self.bearer_token = bearer_token or os.getenv("API_BEARER_TOKEN")
        self.openai_api_key = openai_api_key or os.getenv("OPENAI_API_KEY")
        self.openai_model = openai_model
        self.max_retries = max_retries
        self.retry_backoff_factor = retry_backoff_factor
        self.processed_log_file = processed_log_file
        
        # Validate configuration
        if not self.bearer_token:
            raise ValueError("Bearer token must be provided via parameter or API_BEARER_TOKEN env variable")
        if not self.openai_api_key:
            raise ValueError("OpenAI API key must be provided via parameter or OPENAI_API_KEY env variable")


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


class LLMExtractor:
    """Handles LLM-based data extraction with validation and self-correction"""
    
    SYSTEM_PROMPT = """You are a data extraction expert. Extract structured information from PDF markdown text.

Rules:
1. **Bold text** (surrounded by **) = Category
2. Normal text = Sub-category
3. Bilingual format "English / Chinese" on same line = split into separate fields
4. Return ONLY valid JSON, no markdown formatting, no explanations

Output JSON schema:
{
  "categories": [
    {
      "category_name_en": "string",
      "category_name_zh": "string (if present)",
      "subcategories": [
        {
          "subcategory_name_en": "string",
          "subcategory_name_zh": "string (if present)"
        }
      ]
    }
  ]
}"""
    
    def __init__(self, config: PDFExtractorConfig):
        self.config = config
        self.client = OpenAI(api_key=config.openai_api_key)
        self.logger = logging.getLogger(__name__)
    
    def extract_data(self, markdown_text: str, attempt: int = 1) -> Dict:
        """
        Extract structured data from markdown using LLM
        
        Args:
            markdown_text: PDF content converted to markdown
            attempt: Current attempt number (for retry logic)
        
        Returns:
            Validated JSON data structure
        """
        self.logger.info(f"Starting LLM extraction (attempt {attempt}/{self.config.max_retries})")
        
        try:
            # Prepare user prompt
            user_prompt = f"Extract structured data from this PDF markdown:\n\n{markdown_text}"
            
            # Call OpenAI API
            response = self.client.chat.completions.create(
                model=self.config.openai_model,
                messages=[
                    {"role": "system", "content": self.SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.1,  # Low temperature for consistency
                max_tokens=4000
            )
            
            # Extract response text
            response_text = response.choices[0].message.content.strip()
            self.logger.debug(f"LLM response: {response_text[:200]}...")
            
            # Parse JSON
            extracted_data = self._parse_json_response(response_text)
            
            # Validate data
            validation_result = self._validate_extracted_data(extracted_data)
            
            if validation_result['valid']:
                self.logger.info("Data extraction successful and validated")
                return extracted_data
            else:
                # Self-correction: Re-prompt with error details
                if attempt < self.config.max_retries:
                    self.logger.warning(f"Validation failed: {validation_result['errors']}")
                    return self._retry_with_correction(
                        markdown_text, 
                        validation_result['errors'], 
                        attempt + 1
                    )
                else:
                    raise ValueError(f"Max retries reached. Validation errors: {validation_result['errors']}")
        
        except json.JSONDecodeError as e:
            if attempt < self.config.max_retries:
                self.logger.warning(f"JSON parsing failed: {e}")
                return self._retry_with_correction(
                    markdown_text,
                    [f"JSON parsing error: {str(e)}"],
                    attempt + 1
                )
            else:
                raise ValueError(f"Max retries reached. Could not parse valid JSON: {e}")
        
        except Exception as e:
            self.logger.error(f"Extraction error: {e}")
            raise
    
    def _parse_json_response(self, response_text: str) -> Dict:
        """Parse JSON from LLM response, handling markdown code blocks"""
        # Remove markdown code blocks if present
        if response_text.startswith("```"):
            # Extract JSON from code block
            lines = response_text.split('\n')
            json_lines = []
            in_code_block = False
            
            for line in lines:
                if line.startswith("```"):
                    in_code_block = not in_code_block
                    continue
                if in_code_block or (not line.startswith("```") and json_lines):
                    json_lines.append(line)
            
            response_text = '\n'.join(json_lines).strip()
        
        return json.loads(response_text)
    
    def _validate_extracted_data(self, data: Dict) -> Dict[str, any]:
        """
        Validate extracted data structure
        
        Returns:
            Dict with 'valid' boolean and 'errors' list
        """
        errors = []
        
        # Check top-level structure
        if not isinstance(data, dict):
            errors.append("Root must be a dictionary")
            return {'valid': False, 'errors': errors}
        
        if 'categories' not in data:
            errors.append("Missing 'categories' field")
            return {'valid': False, 'errors': errors}
        
        if not isinstance(data['categories'], list):
            errors.append("'categories' must be a list")
            return {'valid': False, 'errors': errors}
        
        if len(data['categories']) == 0:
            errors.append("'categories' list is empty")
            return {'valid': False, 'errors': errors}
        
        # Validate each category
        for idx, category in enumerate(data['categories']):
            if not isinstance(category, dict):
                errors.append(f"Category {idx} is not a dictionary")
                continue
            
            # Check required fields
            if 'category_name_en' not in category:
                errors.append(f"Category {idx} missing 'category_name_en'")
            
            if 'subcategories' not in category:
                errors.append(f"Category {idx} missing 'subcategories'")
            elif not isinstance(category['subcategories'], list):
                errors.append(f"Category {idx} 'subcategories' must be a list")
            else:
                # Validate subcategories
                for sub_idx, subcategory in enumerate(category['subcategories']):
                    if not isinstance(subcategory, dict):
                        errors.append(f"Category {idx}, subcategory {sub_idx} is not a dictionary")
                        continue
                    
                    if 'subcategory_name_en' not in subcategory:
                        errors.append(f"Category {idx}, subcategory {sub_idx} missing 'subcategory_name_en'")
        
        return {
            'valid': len(errors) == 0,
            'errors': errors
        }
    
    def _retry_with_correction(self, markdown_text: str, errors: List[str], attempt: int) -> Dict:
        """Retry extraction with error feedback for self-correction"""
        self.logger.info(f"Retrying extraction with error feedback (attempt {attempt})")
        
        error_message = "\n".join(f"- {error}" for error in errors)
        corrective_prompt = f"""The previous extraction had these errors:
{error_message}

Please extract the data again, ensuring:
1. Valid JSON format (no markdown code blocks)
2. All required fields are present
3. Correct data types (lists, dictionaries, strings)

Original markdown text:
{markdown_text}"""
        
        try:
            response = self.client.chat.completions.create(
                model=self.config.openai_model,
                messages=[
                    {"role": "system", "content": self.SYSTEM_PROMPT},
                    {"role": "user", "content": corrective_prompt}
                ],
                temperature=0.1,
                max_tokens=4000
            )
            
            response_text = response.choices[0].message.content.strip()
            extracted_data = self._parse_json_response(response_text)
            
            # Validate again
            validation_result = self._validate_extracted_data(extracted_data)
            
            if validation_result['valid']:
                self.logger.info("Self-correction successful")
                return extracted_data
            else:
                if attempt < self.config.max_retries:
                    return self._retry_with_correction(
                        markdown_text,
                        validation_result['errors'],
                        attempt + 1
                    )
                else:
                    raise ValueError(f"Max retries reached. Final errors: {validation_result['errors']}")
        
        except Exception as e:
            self.logger.error(f"Retry failed: {e}")
            raise


class APISubmitter:
    """Handles API submission with retry logic and exponential backoff"""
    
    def __init__(self, config: PDFExtractorConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        """Create requests session with retry configuration"""
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.retry_backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def submit(self, data: Dict, filename: str) -> Tuple[bool, Optional[Dict]]:
        """
        Submit extracted data to API
        
        Args:
            data: Validated JSON data
            filename: Original PDF filename for tracking
        
        Returns:
            Tuple of (success: bool, response_data: Optional[Dict])
        """
        self.logger.info(f"Submitting data to API: {self.config.api_endpoint}")
        
        headers = {
            "Authorization": f"Bearer {self.config.bearer_token}",
            "Content-Type": "application/json",
            "X-Source-File": filename
        }
        
        try:
            response = self.session.post(
                self.config.api_endpoint,
                json=data,
                headers=headers,
                timeout=30
            )
            
            response.raise_for_status()
            
            self.logger.info(f"API submission successful (status: {response.status_code})")
            
            try:
                response_data = response.json()
            except:
                response_data = {"status": "success", "raw_response": response.text}
            
            return True, response_data
        
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"API HTTP error: {e}")
            self.logger.error(f"Response: {e.response.text if e.response else 'N/A'}")
            return False, None
        
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed: {e}")
            return False, None


class PDFDataExtractor:
    """Main orchestrator for PDF data extraction and submission"""
    
    def __init__(self, config: PDFExtractorConfig):
        self.config = config
        self.logger = self._setup_logging()
        self.tracker = ProcessedFilesTracker(config.processed_log_file)
        self.llm_extractor = LLMExtractor(config)
        self.api_submitter = APISubmitter(config)
    
    def _setup_logging(self) -> logging.Logger:
        """Configure logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('pdf_extractor.log'),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(__name__)
    
    def process_pdf(self, pdf_path: Path) -> Dict:
        """
        Process a single PDF file
        
        Args:
            pdf_path: Path to PDF file
        
        Returns:
            Processing result dictionary
        """
        self.logger.info(f"=" * 80)
        self.logger.info(f"Processing PDF: {pdf_path}")
        
        result = {
            'filename': str(pdf_path),
            'success': False,
            'stage': None,
            'error': None,
            'data': None
        }
        
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
            self.logger.debug(f"Markdown preview: {markdown_text[:500]}...")
            
            # Stage 2: Extract data using LLM
            result['stage'] = 'llm_extraction'
            self.logger.info("Stage 2: Extracting data with LLM")
            extracted_data = self.llm_extractor.extract_data(markdown_text)
            result['data'] = extracted_data
            self.logger.info(f"Extracted {len(extracted_data.get('categories', []))} categories")
            
            # Stage 3: Submit to API
            result['stage'] = 'api_submission'
            self.logger.info("Stage 3: Submitting to API")
            success, api_response = self.api_submitter.submit(extracted_data, pdf_path.name)
            
            if success:
                result['success'] = True
                result['stage'] = 'completed'
                result['api_response'] = api_response
                self.logger.info("✓ PDF processing completed successfully")
                
                # Mark as processed
                self.tracker.mark_processed(
                    pdf_path,
                    success=True,
                    details={'api_response': api_response}
                )
            else:
                result['error'] = 'API submission failed'
                self.logger.error("✗ API submission failed")
                self.tracker.mark_processed(pdf_path, success=False)
        
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"✗ Error processing PDF at stage '{result['stage']}': {e}", exc_info=True)
            self.tracker.mark_processed(
                pdf_path,
                success=False,
                details={'error': str(e), 'stage': result['stage']}
            )
        
        return result
    
    def process_directory(self, directory: Path, recursive: bool = False) -> List[Dict]:
        """
        Process all PDFs in a directory
        
        Args:
            directory: Directory containing PDF files
            recursive: Whether to search subdirectories
        
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
            result = self.process_pdf(pdf_path)
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
        
        self.logger.info(f"Total files: {len(results)}")
        self.logger.info(f"Successful: {successful}")
        self.logger.info(f"Failed: {failed}")
        
        if failed > 0:
            self.logger.info("\nFailed files:")
            for result in results:
                if not result['success']:
                    self.logger.info(f"  - {result['filename']}: {result['error']}")
        
        return results


def main():
    """Main entry point for the script"""
    # Configuration
    config = PDFExtractorConfig(
        api_endpoint="https://internal-api.example.com/upload",
        # bearer_token will be read from API_BEARER_TOKEN env variable
        # openai_api_key will be read from OPENAI_API_KEY env variable
        openai_model="gpt-4",
        max_retries=3,
        retry_backoff_factor=2.0
    )
    
    # Create extractor
    extractor = PDFDataExtractor(config)
    
    # Example: Process single file
    # result = extractor.process_pdf(Path("sample.pdf"))
    
    # Example: Process directory
    results = extractor.process_directory(
        Path("./pdfs"),
        recursive=False
    )
    
    # Save results
    with open('processing_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print("\nProcessing complete. Check pdf_extractor.log for details.")


if __name__ == "__main__":
    main()