# Testing Guide

## Quick Setup Validation

Run the setup check script to validate your environment:

```bash
python setup_check.py
```

This will check:
- Python version (3.8+)
- Required dependencies
- Environment variables
- File structure

## Manual Testing

### 1. Test PDF Conversion

```python
import pymupdf4llm
from pathlib import Path

# Test PDF to Markdown conversion
pdf_path = Path("test.pdf")
markdown = pymupdf4llm.to_markdown(str(pdf_path))

print(f"Converted {len(markdown)} characters")
print("\nPreview:")
print(markdown[:500])

# Check for bold text markers
if "**" in markdown:
    print("\n✓ Bold text detected")
else:
    print("\n✗ No bold text found (check PDF formatting)")
```

### 2. Test Environment Variables

```python
import os

# Check environment variables
required_vars = ['OPENAI_API_KEY', 'API_BEARER_TOKEN']

for var in required_vars:
    value = os.getenv(var)
    if value:
        print(f"✓ {var}: {value[:8]}...")
    else:
        print(f"✗ {var}: Not set")
```

### 3. Test OpenAI Connection

```python
from openai import OpenAI
import os

client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

try:
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "user", "content": "Say 'test successful'"}],
        max_tokens=10
    )
    print("✓ OpenAI connection successful")
    print(f"Response: {response.choices[0].message.content}")
except Exception as e:
    print(f"✗ OpenAI connection failed: {e}")
```

### 4. Test JSON Validation

```python
from pdf_extractor import LLMExtractor, PDFExtractorConfig

config = PDFExtractorConfig(
    api_endpoint="https://test.example.com",
    bearer_token="test",
    openai_api_key="test"
)

extractor = LLMExtractor(config)

# Test valid JSON
valid_data = {
    "categories": [
        {
            "category_name_en": "Test",
            "subcategories": [
                {"subcategory_name_en": "SubTest"}
            ]
        }
    ]
}

result = extractor._validate_extracted_data(valid_data)
print(f"Valid data test: {result}")

# Test invalid JSON
invalid_data = {"categories": []}
result = extractor._validate_extracted_data(invalid_data)
print(f"Invalid data test: {result}")
```

### 5. Test File Tracking

```python
from pdf_extractor import ProcessedFilesTracker
from pathlib import Path

tracker = ProcessedFilesTracker("test_processed.json")

# Create a test file
test_file = Path("test.txt")
test_file.write_text("test content")

# Test tracking
print(f"Is processed: {tracker.is_processed(test_file)}")

tracker.mark_processed(test_file, success=True)
print(f"Marked as processed")

print(f"Is processed now: {tracker.is_processed(test_file)}")

# Cleanup
test_file.unlink()
```

## Integration Testing

### Test with Sample PDF

Create a sample PDF with this content:

```
**Electronics / 电子产品**
Mobile Phones / 手机
Laptops / 笔记本电脑

**Clothing / 服装**
Shirts / 衬衫
Pants / 裤子
```

Then run:

```python
from pdf_extractor import PDFDataExtractor, PDFExtractorConfig
from pathlib import Path

config = PDFExtractorConfig(
    api_endpoint="https://internal-api.example.com/upload"
)

extractor = PDFDataExtractor(config)
result = extractor.process_pdf(Path("sample.pdf"))

print(f"Success: {result['success']}")
if result['data']:
    import json
    print(json.dumps(result['data'], indent=2))
```

## Mock Testing (Without API Calls)

For testing without making actual API calls, you can mock the API responses:

```python
from unittest.mock import Mock, patch
from pdf_extractor import PDFDataExtractor, PDFExtractorConfig
from pathlib import Path

# Mock configuration
config = PDFExtractorConfig(
    api_endpoint="https://mock.example.com/upload",
    bearer_token="mock-token",
    openai_api_key="mock-key"
)

# Mock OpenAI response
mock_openai_response = Mock()
mock_openai_response.choices = [
    Mock(message=Mock(content='''
{
  "categories": [
    {
      "category_name_en": "Test Category",
      "subcategories": [
        {"subcategory_name_en": "Test Subcategory"}
      ]
    }
  ]
}
'''))
]

# Mock API response
mock_api_response = Mock()
mock_api_response.status_code = 200
mock_api_response.json.return_value = {"status": "success"}

with patch('openai.OpenAI') as mock_openai_class, \
     patch('requests.Session.post', return_value=mock_api_response):
    
    mock_openai_instance = Mock()
    mock_openai_instance.chat.completions.create.return_value = mock_openai_response
    mock_openai_class.return_value = mock_openai_instance
    
    extractor = PDFDataExtractor(config)
    # Test extraction logic without actual API calls
    print("Mock testing setup complete")
```

## Debugging Tips

### Enable Debug Logging

```python
import logging

# Set to DEBUG for maximum verbosity
logging.getLogger().setLevel(logging.DEBUG)

# Or enable for specific modules
logging.getLogger('pdf_extractor').setLevel(logging.DEBUG)
```

### Inspect Markdown Output

```python
from pdf_extractor import PDFDataExtractor, PDFExtractorConfig
import pymupdf4llm

# Extract markdown and save for inspection
markdown = pymupdf4llm.to_markdown("test.pdf")

with open("debug_markdown.md", "w", encoding="utf-8") as f:
    f.write(markdown)

print("Markdown saved to debug_markdown.md for inspection")
```

### Test LLM Prompts Manually

Copy the system prompt from `LLMExtractor.SYSTEM_PROMPT` and test it directly in ChatGPT or via API to verify the extraction logic.

### Check Processed Files Log

```python
import json

with open("processed_files.json") as f:
    processed = json.load(f)

for filename, details in processed.items():
    print(f"\nFile: {filename}")
    print(f"Success: {details['success']}")
    print(f"Timestamp: {details['timestamp']}")
    if not details['success']:
        print(f"Error: {details.get('details', {}).get('error', 'Unknown')}")
```

## Common Issues and Solutions

### Issue: "No module named 'pymupdf4llm'"

**Solution:**
```bash
pip install pymupdf4llm
```

### Issue: "JSONDecodeError: Expecting value"

**Cause:** LLM returned invalid JSON

**Debug:**
```python
# Enable debug logging to see raw LLM response
import logging
logging.getLogger('pdf_extractor').setLevel(logging.DEBUG)
```

### Issue: "API submission failed with 401 Unauthorized"

**Check:**
1. Bearer token is correct
2. Token hasn't expired
3. API endpoint is correct

### Issue: PDF has no text (blank output)

**Cause:** PDF might be scanned images (OCR not supported)

**Verify:**
```python
import pymupdf4llm
markdown = pymupdf4llm.to_markdown("test.pdf")
print(f"Length: {len(markdown)}")
print(markdown[:500])
```

If output is empty or gibberish, the PDF contains images, not text.

## Performance Testing

### Measure Processing Time

```python
import time
from pdf_extractor import PDFDataExtractor, PDFExtractorConfig
from pathlib import Path

config = PDFExtractorConfig(api_endpoint="https://test.example.com/upload")
extractor = PDFDataExtractor(config)

start_time = time.time()
result = extractor.process_pdf(Path("test.pdf"))
end_time = time.time()

print(f"Processing time: {end_time - start_time:.2f} seconds")
print(f"Success: {result['success']}")
```

### Batch Performance

```python
import time
from pathlib import Path

config = PDFExtractorConfig(api_endpoint="https://test.example.com/upload")
extractor = PDFDataExtractor(config)

pdf_dir = Path("./pdfs")
pdf_files = list(pdf_dir.glob("*.pdf"))

print(f"Testing with {len(pdf_files)} PDFs")

start_time = time.time()
results = extractor.process_directory(pdf_dir)
end_time = time.time()

total_time = end_time - start_time
avg_time = total_time / len(pdf_files) if pdf_files else 0

print(f"Total time: {total_time:.2f}s")
print(f"Average per file: {avg_time:.2f}s")
print(f"Success rate: {sum(1 for r in results if r['success'])}/{len(results)}")
```

## CI/CD Testing

For automated testing in CI/CD pipelines:

```bash
#!/bin/bash
# test.sh

# Set test environment variables
export OPENAI_API_KEY="test-key"
export API_BEARER_TOKEN="test-token"

# Run validation
python setup_check.py

# Run unit tests (if you create them)
# python -m pytest tests/

echo "All tests completed"
```

Make executable:
```bash
chmod +x test.sh
./test.sh
```