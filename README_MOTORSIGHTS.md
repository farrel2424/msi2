# Motorsights EPC PDF Automation - Setup Guide

## 🎯 Overview

Automated PDF catalog extraction and submission to Motorsights Electronic Product Catalog (EPC) using:
- **Maia Router**: Universal AI Gateway (GPT-4o, GPT-4.1, Claude-3-Sonnet)
- **Motorsights EPC API**: Development endpoint at `https://dev-epc.motorsights.com`

## 📋 **Workflow**

```
PDF Upload
   ↓
AI Extraction (via Maia Router)
   ↓
Review & Edit (optional)
   ↓
Approve
   ↓
Submit to EPC API
   ↓
Create Type Categories & Categories
```

## 🔧 **Installation**

### 1. Install Dependencies

```bash
pip install pymupdf4llm openai requests urllib3 Flask python-dotenv
```

### 2. Set Environment Variables

Create a `.env` file:

```bash
# Maia Router (AI Gateway)
MAIA_ROUTER_ENDPOINT=https://maia.motorsights.com/v1/chat/completions
MAIA_ROUTER_API_KEY=sk-your-maia-key-here
MAIA_ROUTER_MODEL=gpt-4o

# Motorsights EPC API
EPC_API_BASE_URL=https://dev-epc.motorsights.com
EPC_BEARER_TOKEN=your-epc-bearer-token-here

# Optional
DEFAULT_MASTER_CATEGORY_ID=1
MAX_RETRIES=3
ENABLE_REVIEW_MODE=true
LOG_LEVEL=INFO
```

## 🚀 **Quick Start**

### Option 1: Command Line

```bash
python epc_automation.py
```

### Option 2: Web Interface

```bash
python epc_web_ui.py
```

Then open: **http://localhost:5000**

## 📊 **API Mapping**

### PDF Structure → EPC API

| PDF Element | EPC Entity | API Endpoint |
|------------|-----------|--------------|
| **Bold text** (e.g., "**Electronics / 电子产品**") | Type Category | `POST /type_category/create` |
| Normal text (e.g., "Mobile Phones / 手机") | Category | `POST /categories/create` |

### Hierarchy

```
Master Category (pre-existing or manual)
  └── Type Category (from bold text)
      └── Category (from normal text)
```

### Example PDF Input

```
**Electronics / 电子产品**
Mobile Phones / 手机
Laptops / 笔记本电脑

**Clothing / 服装**
Shirts / 衬衫
Pants / 裤子
```

### Extracted JSON

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
    },
    {
      "category_name_en": "Clothing",
      "category_name_zh": "服装",
      "subcategories": [
        {
          "subcategory_name_en": "Shirts",
          "subcategory_name_zh": "衬衫"
        },
        {
          "subcategory_name_en": "Pants",
          "subcategory_name_zh": "裤子"
        }
      ]
    }
  ]
}
```

### EPC API Calls

```bash
# 1. Create Type Category "Electronics"
POST https://dev-epc.motorsights.com/type_category/create
{
  "name": "Electronics",
  "name_zh": "电子产品",
  "master_category_id": 1
}
# Response: { "id": 123 }

# 2. Create Category "Mobile Phones" under Type Category 123
POST https://dev-epc.motorsights.com/categories/create
{
  "name": "Mobile Phones",
  "name_zh": "手机",
  "type_category_id": 123
}

# 3. Create Category "Laptops" under Type Category 123
POST https://dev-epc.motorsights.com/categories/create
{
  "name": "Laptops",
  "name_zh": "笔记本电脑",
  "type_category_id": 123
}

# Repeat for "Clothing" and its categories...
```

## 🔑 **Configuration**

### Maia Router Settings

- **Endpoint**: `https://maia.motorsights.com/v1/chat/completions`
- **Auth**: Bearer token (sk-xxxx format)
- **Available Models**:
  - `gpt-4o` (recommended - fast, accurate)
  - `gpt-4.1` (more powerful)
  - `claude-3-sonnet` (alternative)

### EPC API Settings

- **Base URL**: `https://dev-epc.motorsights.com`
- **Auth**: Bearer token
- **Endpoints Used**:
  - `POST /type_category/create`
  - `POST /categories/create`
  - `POST /master_category/get` (optional)

### Review Mode

**Enabled** (default): Upload → Extract → **Review** → Approve → Submit
**Disabled**: Upload → Extract → **Auto-submit** to EPC

Set in `.env`:
```bash
ENABLE_REVIEW_MODE=true  # or false for auto-submit
```

## 📁 **File Structure**

```
motorsights-epc-automation/
├── epc_automation.py           # Main orchestrator
├── maia_router_client.py       # Maia Router AI client
├── motorsights_epc_client.py   # EPC API client
├── epc_web_ui.py              # Web interface
├── templates/
│   ├── epc_index.html         # Upload page
│   ├── epc_config.html        # Configuration
│   └── epc_history.html       # Job history
├── .env                       # Credentials (DO NOT COMMIT)
├── requirements.txt           # Dependencies
└── README_MOTORSIGHTS.md      # This file
```

## 🎨 **Web UI Features**

### Upload Page
- Drag & drop PDF upload
- Configure Maia Router and EPC settings
- Enable/disable review mode
- Set master category ID

### Review Page
- View extracted data
- Edit categories/subcategories
- Add/remove items
- Approve and submit to EPC

### History Page
- All processed jobs
- Status tracking
- Download extracted JSON
- Resubmit failed items

## 🧪 **Testing**

### 1. Test Maia Router Connection

```python
from maia_router_client import MaiaRouterClient

client = MaiaRouterClient(
    endpoint="https://maia.motorsights.com/v1/chat/completions",
    api_key="sk-your-key",
    model="gpt-4o"
)

# Test extraction
markdown = "**Electronics / 电子产品**\nMobile Phones / 手机"
result = client.extract_catalog_data(markdown)
print(result)
```

### 2. Test EPC API Connection

```python
from motorsights_epc_client import MotorsightsEPCClient

client = MotorsightsEPCClient(
    base_url="https://dev-epc.motorsights.com",
    bearer_token="your-token"
)

# Test getting master categories
success, data = client.get_master_categories()
print(f"Success: {success}")
print(f"Data: {data}")
```

### 3. Test End-to-End

```python
from epc_automation import EPCPDFAutomation, EPCAutomationConfig
from pathlib import Path

config = EPCAutomationConfig(
    maia_api_key="sk-your-maia-key",
    epc_bearer_token="your-epc-token",
    enable_review_mode=False  # Auto-submit for testing
)

automation = EPCPDFAutomation(config)
result = automation.process_pdf(Path("test_catalog.pdf"))

print(f"Success: {result['success']}")
print(f"EPC Results: {result.get('epc_submission')}")
```

## 🔍 **Troubleshooting**

### Issue: "Maia Router API key must be provided"
**Solution**: Set `MAIA_ROUTER_API_KEY` in .env or pass to config

### Issue: "EPC Bearer token must be provided"
**Solution**: Set `EPC_BEARER_TOKEN` in .env or pass to config

### Issue: "No ID returned for type category"
**Check**: EPC API response structure - may need to adjust ID extraction in `motorsights_epc_client.py`

### Issue: PDF extraction returns empty
**Cause**: PDF contains images (OCR not supported)
**Solution**: Ensure PDF has selectable text

### Issue: API returns 401 Unauthorized
**Check**:
1. Bearer token is correct
2. Token hasn't expired
3. Correct endpoint URL

### Issue: Categories not linking correctly
**Check**:
1. Master category ID exists
2. Type category ID is correctly returned from first API call
3. Review logs in `epc_automation.log`

## 📊 **Monitoring**

### Log Files

- `epc_automation.log` - Detailed processing logs
- `epc_processed_files.json` - Idempotency tracker
- `epc_processing_results.json` - Batch results summary

### Success Metrics

Check `epc_processing_results.json`:

```json
{
  "type_categories_created": 5,
  "categories_created": 23,
  "errors": 0
}
```

## 🚢 **Deployment**

### Production Considerations

1. **Use Environment Variables**
   - Never commit `.env` to git
   - Use secret management (AWS Secrets Manager, etc.)

2. **HTTPS**
   - Use nginx/Apache as reverse proxy
   - Enable SSL certificates

3. **Error Handling**
   - Set up monitoring (Sentry, CloudWatch)
   - Email alerts for failures

4. **Rate Limiting**
   - Maia Router: Check usage limits
   - EPC API: Implement backoff if rate limited

5. **Database**
   - Current: In-memory job storage
   - Production: Use Redis or PostgreSQL

6. **File Cleanup**
   - Implement scheduled cleanup of uploaded PDFs
   - Archive processed files

### Production Deployment

```bash
# Using gunicorn
pip install gunicorn

gunicorn -w 4 -b 0.0.0.0:5000 epc_web_ui:app

# With systemd
sudo systemctl enable epc-automation
sudo systemctl start epc-automation
```

## 📞 **Support**

For issues or questions:
1. Check logs: `epc_automation.log`
2. Review processed files: `epc_processed_files.json`
3. Enable debug logging: `LOG_LEVEL=DEBUG` in .env

## 🎯 **Next Steps**

1. ✅ Get Maia Router credentials from mentor
2. ✅ Get EPC Bearer token from mentor
3. ✅ Test connection to both APIs
4. ✅ Upload sample PDF catalog
5. ✅ Review extracted data
6. ✅ Approve and submit to dev EPC
7. ✅ Verify data in EPC UI at https://dev-epc.motorsights.com

## 📄 **License**

Internal use only - Motorsights EPC Automation