# Web UI User Guide

## 🌐 Overview

The PDF Data Extractor now includes a **modern web-based interface** with:
- 📤 **Drag & Drop Upload** - Simply drag PDF files into the browser
- 📊 **Real-time Status Updates** - Watch your PDFs being processed live
- 📜 **Processing History** - View all past jobs and their results
- ⚙️ **Easy Configuration** - Set API keys and options through forms
- 💾 **JSON Download** - Download extracted data with one click

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

This will install Flask and all other required packages.

### 2. Set Environment Variables (Recommended)

```bash
export OPENAI_API_KEY="sk-your-key-here"
export API_BEARER_TOKEN="your-token-here"
```

Or create a `.env` file:

```
OPENAI_API_KEY=sk-your-key-here
API_BEARER_TOKEN=your-token-here
```

### 3. Start the Web Server

```bash
python web_ui.py
```

You should see:

```
=======================================================
PDF Data Extractor - Web UI
=======================================================

Server starting...
Open your browser and navigate to: http://localhost:5000

Press Ctrl+C to stop the server
=======================================================
```

### 4. Open Your Browser

Navigate to: **http://localhost:5000**

## 📖 Using the Interface

### Main Upload Page

1. **Drag & Drop or Click to Upload**
   - Drag a PDF file into the upload area
   - Or click the area to browse and select a file
   - Only `.pdf` files are accepted

2. **Configure Settings** (if needed)
   - **API Endpoint**: Where to submit extracted data
   - **Bearer Token**: API authentication (or use env var)
   - **OpenAI API Key**: For GPT-4 access (or use env var)
   - **Model**: Choose GPT-4, GPT-4 Turbo, or GPT-4o
   - **Max Retries**: Number of retry attempts (default: 3)

3. **Upload**
   - The file will upload automatically after selection
   - Processing starts immediately

4. **Monitor Progress**
   - Status card appears showing:
     - File name
     - Current status (Queued → Processing → Completed/Failed)
     - Processing stage (PDF conversion → LLM extraction → API submission)
     - Progress bar

5. **View Results**
   - When completed, extracted JSON appears on screen
   - Click "💾 Download JSON" to save the file

### Configuration Page

Access at: **http://localhost:5000/config**

This page provides:
- Detailed setup instructions
- All configuration parameters explained
- Environment variable examples
- Security best practices

### History Page

Access at: **http://localhost:5000/history**

Features:
- **Statistics Dashboard**: Total, Completed, Processing, Failed jobs
- **Job List**: All processed PDFs with status
- **Auto-refresh**: Updates every 3 seconds while jobs are processing
- **Download**: One-click download for completed jobs
- **Error Details**: View error messages for failed jobs
- **Clear History**: Remove all job records

## 🎨 Features in Detail

### Drag & Drop Upload

- Visual feedback when dragging files
- Instant validation (PDF files only)
- Automatic processing after upload

### Real-time Status Updates

The interface polls the server every 2 seconds to update:
- Processing status
- Current stage
- Progress indicator
- Results when available

### Configuration Persistence

Settings are saved to browser localStorage:
- API Endpoint
- Model selection
- Max retries

These persist across browser sessions for convenience.

### Batch Processing

Upload multiple files by:
1. Upload first file
2. Wait for completion (or don't)
3. Upload next file
4. Check History page for all jobs

### Error Handling

The UI shows clear error messages for:
- Missing configuration (API keys)
- Invalid file types
- Upload failures
- Processing errors
- API submission failures

## 🔧 Configuration Options

### Required Settings

**OpenAI API Key**
- Required for GPT-4 access
- Get from: https://platform.openai.com/api-keys
- Format: `sk-proj-...`

**API Bearer Token**
- Required for API submission
- Get from your API provider
- Format: varies by provider

### Optional Settings

**API Endpoint**
- Default: `https://internal-api.example.com/upload`
- Change to your actual API URL

**Model**
- Options: `gpt-4`, `gpt-4-turbo-preview`, `gpt-4o`
- Default: `gpt-4`

**Max Retries**
- Range: 1-10
- Default: 3
- Includes self-correction attempts

## 🔒 Security

### Best Practices

1. **Use Environment Variables**
   ```bash
   export OPENAI_API_KEY="sk-..."
   export API_BEARER_TOKEN="token..."
   python web_ui.py
   ```

2. **Don't Commit Secrets**
   - Never add `.env` files to git
   - Don't hardcode keys in code

3. **Use HTTPS in Production**
   - The default server uses HTTP (localhost only)
   - For production, use a proper WSGI server with SSL

4. **Limit Access**
   - Default: accessible to anyone on your network (`0.0.0.0`)
   - For security, change to `127.0.0.1` (localhost only):
     ```python
     app.run(debug=True, host='127.0.0.1', port=5000)
     ```

### Password Fields

The web form uses password-type inputs for:
- Bearer Token
- OpenAI API Key

These mask the values from shoulder-surfers but don't encrypt transmission (use HTTPS for that).

## 📁 File Storage

### Uploads Directory

Files are temporarily stored in:
```
uploads/
  ├── {job_id}_filename.pdf
  └── ...
```

### Outputs Directory

Extracted JSON files are saved to:
```
outputs/
  ├── {job_id}_extracted.json
  └── ...
```

### Cleanup

Uploaded files and outputs are kept indefinitely. To clean up:

```bash
# Remove uploads
rm -rf uploads/*

# Remove outputs
rm -rf outputs/*

# Clear job history (via UI or API)
curl -X POST http://localhost:5000/api/clear-history
```

## 🛠️ Troubleshooting

### Server Won't Start

**Error: "Address already in use"**
- Another process is using port 5000
- Solution: Kill the other process or change the port:
  ```python
  app.run(debug=True, host='0.0.0.0', port=5001)
  ```

**Error: "No module named 'flask'"**
- Flask not installed
- Solution: `pip install Flask`

### Upload Fails

**Error: "Only PDF files are allowed"**
- File is not a PDF
- Solution: Ensure file has `.pdf` extension

**Error: "API Bearer Token is required"**
- Missing configuration
- Solution: Set via environment variable or web form

**Error: "OpenAI API Key is required"**
- Missing configuration
- Solution: Set via environment variable or web form

### Processing Fails

Check the terminal where `web_ui.py` is running for detailed error logs.

Common issues:
- Invalid API keys
- API rate limits exceeded
- PDF contains only images (OCR not supported)
- API endpoint unreachable

### Can't Download Results

**Error: "Output file not found"**
- Processing may have failed
- Check History page for job status
- Look at error details

## 🔄 API Endpoints

The Web UI exposes these REST endpoints:

### POST /api/upload
Upload and process a PDF file.

**Request:** `multipart/form-data`
- `file`: PDF file
- `api_endpoint`: API URL
- `bearer_token`: Auth token
- `openai_api_key`: OpenAI key
- `openai_model`: Model name
- `max_retries`: Retry count

**Response:** `200 OK`
```json
{
  "job_id": "uuid",
  "filename": "document.pdf",
  "message": "File uploaded successfully"
}
```

### GET /api/status/{job_id}
Get processing status for a job.

**Response:** `200 OK`
```json
{
  "id": "uuid",
  "filename": "document.pdf",
  "status": "processing",
  "stage": "llm_extraction",
  "uploaded_at": "2026-01-27T10:30:00",
  "result": {...}
}
```

### GET /api/jobs
Get all jobs.

**Response:** `200 OK`
```json
[
  {
    "id": "uuid",
    "filename": "document.pdf",
    "status": "completed",
    ...
  }
]
```

### GET /api/download/{job_id}
Download extracted JSON.

**Response:** `200 OK` (JSON file download)

### POST /api/clear-history
Clear all job history.

**Response:** `200 OK`
```json
{
  "message": "History cleared successfully"
}
```

## 🎯 Production Deployment

For production use:

1. **Use a Production Server**
   ```bash
   # Install gunicorn
   pip install gunicorn
   
   # Run with gunicorn
   gunicorn -w 4 -b 0.0.0.0:5000 web_ui:app
   ```

2. **Add HTTPS**
   - Use nginx as reverse proxy with SSL
   - Or use Cloudflare Tunnel

3. **Add Authentication**
   - Implement login system
   - Or use Basic Auth with nginx

4. **Database for Jobs**
   - Current implementation uses in-memory storage
   - Add Redis or PostgreSQL for persistence

5. **File Cleanup**
   - Add scheduled cleanup of old uploads
   - Implement file retention policies

## 💡 Tips & Tricks

1. **Keep the History Page Open**
   - Auto-refreshes while jobs are processing
   - No need to manually refresh

2. **Save Configuration**
   - Settings persist in browser localStorage
   - No need to re-enter each time

3. **Batch Upload**
   - Upload multiple files sequentially
   - Check History page to monitor all jobs

4. **Download from History**
   - Can download results anytime from History page
   - No need to stay on upload page

5. **Error Recovery**
   - Failed jobs show error details in History
   - Can identify issues and retry

## 🆘 Getting Help

If you encounter issues:

1. Check the browser console (F12) for JavaScript errors
2. Check the terminal running `web_ui.py` for server errors
3. Review `pdf_extractor.log` for processing details
4. Check `processed_files.json` for job history

## 🎉 Summary

The Web UI provides a user-friendly way to:
- ✅ Upload PDFs via drag & drop
- ✅ Configure settings easily
- ✅ Monitor processing in real-time
- ✅ View all job history
- ✅ Download extracted data
- ✅ Handle errors gracefully

No command-line knowledge required!