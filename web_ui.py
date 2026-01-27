"""
Web UI for PDF Data Extractor
Flask-based interface with drag-and-drop PDF upload
"""

from flask import Flask, render_template, request, jsonify, send_file
from werkzeug.utils import secure_filename
import os
import json
import threading
from pathlib import Path
from datetime import datetime
import uuid

from pdf_extractor import PDFDataExtractor, PDFExtractorConfig

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
app.config['ALLOWED_EXTENSIONS'] = {'pdf'}

# Create necessary directories
Path(app.config['UPLOAD_FOLDER']).mkdir(exist_ok=True)
Path('outputs').mkdir(exist_ok=True)

# Global storage for job status
job_status = {}
job_lock = threading.Lock()


def allowed_file(filename):
    """Check if file extension is allowed"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']


def process_pdf_async(job_id, pdf_path, config_params):
    """Process PDF in background thread"""
    try:
        with job_lock:
            job_status[job_id]['status'] = 'processing'
            job_status[job_id]['stage'] = 'initializing'
        
        # Create config
        config = PDFExtractorConfig(
            api_endpoint=config_params.get('api_endpoint', 'https://internal-api.example.com/upload'),
            bearer_token=config_params.get('bearer_token'),
            openai_api_key=config_params.get('openai_api_key'),
            openai_model=config_params.get('openai_model', 'gpt-4'),
            max_retries=int(config_params.get('max_retries', 3))
        )
        
        # Create extractor
        extractor = PDFDataExtractor(config)
        
        # Update status
        with job_lock:
            job_status[job_id]['stage'] = 'converting_pdf'
        
        # Process PDF
        result = extractor.process_pdf(Path(pdf_path))
        
        # Update final status
        with job_lock:
            job_status[job_id]['status'] = 'completed' if result['success'] else 'failed'
            job_status[job_id]['result'] = result
            job_status[job_id]['completed_at'] = datetime.now().isoformat()
            
            if result['success'] and result.get('data'):
                # Save extracted data
                output_file = f"outputs/{job_id}_extracted.json"
                with open(output_file, 'w') as f:
                    json.dump(result['data'], f, indent=2)
                job_status[job_id]['output_file'] = output_file
    
    except Exception as e:
        with job_lock:
            job_status[job_id]['status'] = 'error'
            job_status[job_id]['error'] = str(e)
            job_status[job_id]['completed_at'] = datetime.now().isoformat()


@app.route('/')
def index():
    """Render main page"""
    return render_template('index.html')


@app.route('/config')
def config_page():
    """Render configuration page"""
    return render_template('config.html')


@app.route('/history')
def history_page():
    """Render processing history page"""
    return render_template('history.html')


@app.route('/api/upload', methods=['POST'])
def upload_file():
    """Handle file upload"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not allowed_file(file.filename):
        return jsonify({'error': 'Only PDF files are allowed'}), 400
    
    # Get configuration from form
    config_params = {
        'api_endpoint': request.form.get('api_endpoint', 'https://internal-api.example.com/upload'),
        'bearer_token': request.form.get('bearer_token', os.getenv('API_BEARER_TOKEN')),
        'openai_api_key': request.form.get('openai_api_key', os.getenv('OPENAI_API_KEY')),
        'openai_model': request.form.get('openai_model', 'gpt-4'),
        'max_retries': request.form.get('max_retries', '3')
    }
    
    # Validate required fields
    if not config_params['bearer_token']:
        return jsonify({'error': 'API Bearer Token is required'}), 400
    
    if not config_params['openai_api_key']:
        return jsonify({'error': 'OpenAI API Key is required'}), 400
    
    # Save uploaded file
    filename = secure_filename(file.filename)
    job_id = str(uuid.uuid4())
    upload_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{job_id}_{filename}")
    file.save(upload_path)
    
    # Initialize job status
    with job_lock:
        job_status[job_id] = {
            'id': job_id,
            'filename': filename,
            'status': 'queued',
            'stage': 'uploaded',
            'uploaded_at': datetime.now().isoformat(),
            'result': None,
            'error': None
        }
    
    # Start background processing
    thread = threading.Thread(
        target=process_pdf_async,
        args=(job_id, upload_path, config_params)
    )
    thread.daemon = True
    thread.start()
    
    return jsonify({
        'job_id': job_id,
        'filename': filename,
        'message': 'File uploaded successfully. Processing started.'
    })


@app.route('/api/status/<job_id>')
def get_status(job_id):
    """Get processing status for a job"""
    with job_lock:
        if job_id not in job_status:
            return jsonify({'error': 'Job not found'}), 404
        
        return jsonify(job_status[job_id])


@app.route('/api/jobs')
def get_jobs():
    """Get all jobs"""
    with job_lock:
        jobs = list(job_status.values())
        # Sort by upload time, newest first
        jobs.sort(key=lambda x: x.get('uploaded_at', ''), reverse=True)
        return jsonify(jobs)


@app.route('/api/download/<job_id>')
def download_result(job_id):
    """Download extracted JSON data"""
    with job_lock:
        if job_id not in job_status:
            return jsonify({'error': 'Job not found'}), 404
        
        job = job_status[job_id]
        if job['status'] != 'completed':
            return jsonify({'error': 'Job not completed'}), 400
        
        output_file = job.get('output_file')
        if not output_file or not os.path.exists(output_file):
            return jsonify({'error': 'Output file not found'}), 404
        
        return send_file(
            output_file,
            as_attachment=True,
            download_name=f"{job['filename']}_extracted.json"
        )


@app.route('/api/clear-history', methods=['POST'])
def clear_history():
    """Clear all job history"""
    with job_lock:
        job_status.clear()
    
    return jsonify({'message': 'History cleared successfully'})


if __name__ == '__main__':
    print("=" * 60)
    print("PDF Data Extractor - Web UI")
    print("=" * 60)
    print("\nServer starting...")
    print("Open your browser and navigate to: http://localhost:5000")
    print("\nPress Ctrl+C to stop the server")
    print("=" * 60)
    
    app.run(debug=True, host='0.0.0.0', port=5000)