# PDF Data Extraction & API Submission Automation

A robust Python automation script that extracts structured data from PDFs using GPT-4 and submits to an internal API with validation, self-correction, and retry mechanisms.

## Features

✅ **PDF to Markdown Conversion** - Preserves bold text metadata without OCR  
✅ **LLM-Powered Extraction** - Uses GPT-4 for intelligent data parsing  
✅ **Deterministic Layout Detection** - Identifies categories (bold) and subcategories (normal text)  
✅ **Bilingual Support** - Splits "English / Chinese" formats automatically  
✅ **Self-Correction** - Automatically retries with error feedback on validation failures  
✅ **API Submission** - Bearer token auth with exponential backoff retry logic  
✅ **Idempotency** - Tracks processed files to prevent duplicate submissions  
✅ **Batch Processing** - Process entire directories with isolated error handling  
✅ **Comprehensive Logging** - Detailed logs for debugging and audit trails  

## Installation

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

Or install individually:

```bash
pip install pymupdf4llm openai requests urllib3 python-dotenv
```

### 2. Set Environment Variables

Create a `.env` file or export environment variables:

```bash
# Required
export OPENAI_API_KEY="sk-..."
export API_BEARER_TOKEN="your-bearer-token-here"
```

Or create a `.env` file:

```
OPENAI_API_KEY=sk-...
API_BEARER_TOKEN=your-bearer-token-here
```

## Usage

### Basic Usage

```python
from pdf_extractor import PDFDataExtractor, PDFExtractorConfig
from pathlib import Path

# Configure
config = PDFExtractorConfig(
    api_endpoint="https://internal-api.example.com/upload",
    openai_model="gpt-4",
    max_retries=3
)

# Create extractor
extractor = PDFDataExtractor(config)

# Process single PDF
result = extractor.process_pdf(Path("document.pdf"))

# Process entire directory
results = extractor.process_directory(Path("./pdfs"), recursive=False)
```

### Command Line Usage

Modify the `main()` function in `pdf_extractor.py`:

```python
def main():
    config = PDFExtractorConfig(
        api_endpoint="https://internal-api.example.com/upload"
    )
    
    extractor = PDFDataExtractor(config)
    
    # Process directory
    results = extractor.process_directory(
        Path("./pdfs"),
        recursive=True  # Search subdirectories
    )
```

Then run:

```bash
python pdf_extractor.py
```

## Configuration Options

```python
PDFExtractorConfig(
    api_endpoint="https://internal-api.example.com/upload",  # API endpoint
    bearer_token=None,  # Or set via API_BEARER_TOKEN env var
    openai_api_key=None,  # Or set via OPENAI_API_KEY env var
    openai_model="gpt-4",  # GPT model to use
    max_retries=3,  # Max retry attempts for validation/API
    retry_backoff_factor=2.0,  # Exponential backoff multiplier
    processed_log_file="processed_files.json"  # Idempotency log
)
```

## Data Format

### Expected PDF Layout

The script expects PDFs with this structure:

```
**Category Name** or **English Category / 中文类别**
Normal subcategory text or English Subcategory / 中文子类别
    Another subcategory

**Another Category**
Subcategory here
```

### Output JSON Schema

```json
{
  "categories": [
    {
      "category_name_en": "Electronics",
      "category_name_zh": "电子产品",
      "subcategories": [
        {
          "subcategory_name_en": "Mobile Phones",
          "subcategory_name_zh": "手机"
        },
        {
          "subcategory_name_en": "Laptops",
          "subcategory_name_zh": "笔记本电脑"
        }
      ]
    }
  ]
}
```

## Architecture

### Class Structure

```
PDFExtractorConfig
├── Configuration container

ProcessedFilesTracker
├── Tracks processed files (idempotency)
├── SHA-256 hashing for change detection

LLMExtractor
├── Converts PDF → Markdown → Structured JSON
├── Validation logic
├── Self-correction with error feedback

APISubmitter
├── HTTP submission with Bearer auth
├── Exponential backoff retry logic

PDFDataExtractor (Main Orchestrator)
├── Coordinates all components
├── Batch processing
├── Error isolation
└── Comprehensive logging
```

### Processing Pipeline

```
PDF File
  ↓
[PDF → Markdown] (pymupdf4llm)
  ↓
[LLM Extraction] (GPT-4)
  ↓
[JSON Validation]
  ↓ (if invalid)
[Self-Correction] ← Error feedback
  ↓
[API Submission] with retry
  ↓
[Mark Processed] (idempotency)
```

## Error Handling

### Isolated Failures

Each PDF is processed independently. If one fails, others continue:

```python
# Bad PDF won't stop the batch
results = extractor.process_directory(Path("./pdfs"))

# Check individual results
for result in results:
    if not result['success']:
        print(f"Failed: {result['filename']} - {result['error']}")
```

### Validation & Self-Correction

The script automatically retries with corrective prompts:

```
Attempt 1: Extract data → Validation fails (missing field)
  ↓
Attempt 2: Re-prompt with error → Extract data → Validation fails (wrong format)
  ↓
Attempt 3: Re-prompt with error → Extract data → Validation succeeds ✓
```

### HTTP Retry Logic

API requests use exponential backoff:

```
Request 1: Fails (503) → Wait 2s
Request 2: Fails (503) → Wait 4s
Request 3: Succeeds ✓
```

## Logging

### Log Files

- `pdf_extractor.log` - Detailed processing logs
- `processed_files.json` - Idempotency tracker
- `processing_results.json` - Batch results summary

### Log Levels

```python
import logging

# Change log level
logging.getLogger().setLevel(logging.DEBUG)  # More verbose
```

## Idempotency

Files are tracked by SHA-256 hash:

```json
{
  "document.pdf": {
    "hash": "a1b2c3...",
    "timestamp": "2026-01-27T10:30:00",
    "success": true,
    "details": {}
  }
}
```

Running the script multiple times on the same file will skip processing unless the file has changed.

## API Requirements

### Endpoint Expectations

```
POST https://internal-api.example.com/upload
Headers:
  Authorization: Bearer <token>
  Content-Type: application/json
  X-Source-File: <filename>
Body: <extracted JSON>
```

### Expected Responses

```json
// Success
{
  "status": "success",
  "id": "12345"
}

// Error
{
  "status": "error",
  "message": "Validation failed"
}
```

## Advanced Usage

### Custom Validation Rules

Extend the `LLMExtractor._validate_extracted_data()` method:

```python
def _validate_extracted_data(self, data: Dict) -> Dict[str, any]:
    errors = []
    
    # Custom validation
    for category in data.get('categories', []):
        if len(category['subcategories']) == 0:
            errors.append(f"Category '{category['category_name_en']}' has no subcategories")
    
    return {'valid': len(errors) == 0, 'errors': errors}
```

### Custom System Prompt

Modify the `LLMExtractor.SYSTEM_PROMPT`:

```python
SYSTEM_PROMPT = """Your custom extraction instructions..."""
```

### Different LLM Models

```python
config = PDFExtractorConfig(
    openai_model="gpt-4-turbo-preview"  # Use different model
)
```

## Troubleshooting

### Issue: "Bearer token must be provided"

**Solution:** Set the environment variable:
```bash
export API_BEARER_TOKEN="your-token"
```

### Issue: "OpenAI API key must be provided"

**Solution:** Set the environment variable:
```bash
export OPENAI_API_KEY="sk-..."
```

### Issue: PDFs not processing

**Check:**
1. PDF is valid (not corrupted)
2. PDF contains text (not scanned images - OCR not supported)
3. Check logs: `cat pdf_extractor.log`

### Issue: JSON validation always fails

**Solution:** Review the PDF structure. Enable debug logging:
```python
logging.getLogger().setLevel(logging.DEBUG)
```

### Issue: API submission fails

**Check:**
1. API endpoint is correct
2. Bearer token is valid
3. Network connectivity
4. API rate limits

## Performance

- **Single PDF:** ~5-15 seconds (depends on size and API latency)
- **Batch Processing:** Processes sequentially with 1-second pause between files
- **Memory:** Minimal (~50-100MB per PDF)

## Security Notes

⚠️ **Never commit API keys or tokens to version control**

Use environment variables or secret management systems:
- AWS Secrets Manager
- Azure Key Vault
- HashiCorp Vault
- `.env` files (add to `.gitignore`)

## License

This script is provided as-is for internal use.

## Support

For issues or questions:
1. Check logs: `pdf_extractor.log`
2. Review processed files: `processed_files.json`
3. Enable debug logging for more details