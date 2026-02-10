"""
Web UI for Motorsights EPC PDF Automation
Flask-based interface with Upload → Extract → Review → Approve → Submit workflow
"""

from flask import Flask, render_template, request, jsonify, send_file
from werkzeug.utils import secure_filename
import os
import json
import threading
from pathlib import Path
from datetime import datetime
import uuid

from epc_automation import EPCPDFAutomation, EPCAutomationConfig

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
        config = EPCAutomationConfig(
            sumopod_base_url=config_params.get('sumopod_base_url', 'https://ai.sumopod.com/v1'),
            sumopod_api_key=config_params.get('sumopod_api_key'),
            sumopod_model=config_params.get('sumopod_model', 'gpt4o'),
            sumopod_temperature=float(config_params.get('sumopod_temperature', 0.7)),
            sumopod_max_tokens=int(config_params.get('sumopod_max_tokens', 2000)),
            
            epc_base_url=config_params.get('epc_base_url', 'https://dev-epc.motorsights.com'),
            epc_bearer_token=config_params.get('epc_bearer_token'),
            
            max_retries=int(config_params.get('max_retries', 3)),
            enable_review_mode=config_params.get('enable_review_mode', True),
            master_category_id=config_params.get('master_category_id')  # UUID string
        )
        
        # Create automation
        automation = EPCPDFAutomation(config)
        
        # Update status
        with job_lock:
            job_status[job_id]['stage'] = 'converting_pdf'
        
        # Process PDF
        auto_submit = not config_params.get('enable_review_mode', True)
        result = automation.process_pdf(
            Path(pdf_path),
            auto_submit=auto_submit
        )
        
        # Update final status
        with job_lock:
            if result.get('review_required'):
                job_status[job_id]['status'] = 'pending_review'
                job_status[job_id]['extracted_data'] = result['extracted_data']
            elif result['success']:
                job_status[job_id]['status'] = 'completed'
            else:
                job_status[job_id]['status'] = 'failed'
            
            job_status[job_id]['result'] = result
            job_status[job_id]['completed_at'] = datetime.now().isoformat()
            
            # Save extracted data for review
            if result.get('extracted_data'):
                output_file = f"outputs/{job_id}_extracted.json"
                with open(output_file, 'w') as f:
                    json.dump(result['extracted_data'], f, indent=2)
                job_status[job_id]['output_file'] = output_file
    
    except Exception as e:
        with job_lock:
            job_status[job_id]['status'] = 'error'
            job_status[job_id]['error'] = str(e)
            job_status[job_id]['completed_at'] = datetime.now().isoformat()


@app.route('/')
def index():
    """Render main page"""
    return render_template('epc_index.html')


@app.route('/config')
def config_page():
    """Render configuration page"""
    return render_template('epc_config.html')


@app.route('/history')
def history_page():
    """Render processing history page"""
    return render_template('epc_history.html')


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
        'sumopod_base_url': request.form.get('sumopod_base_url', 'https://ai.sumopod.com/v1'),
        'sumopod_api_key': request.form.get('sumopod_api_key', os.getenv('SUMOPOD_API_KEY')),
        'sumopod_model': request.form.get('sumopod_model', 'gpt4o'),
        'sumopod_temperature': request.form.get('sumopod_temperature', '0.7'),
        'sumopod_max_tokens': request.form.get('sumopod_max_tokens', '2000'),
        
        'epc_base_url': request.form.get('epc_base_url', 'https://dev-epc.motorsights.com'),
        'epc_bearer_token': request.form.get('epc_bearer_token', os.getenv('EPC_BEARER_TOKEN')),
        
        'max_retries': request.form.get('max_retries', '3'),
        'enable_review_mode': request.form.get('enable_review_mode', 'true').lower() == 'true',
        'master_category_id': request.form.get('master_category_id', '')
    }
    
    # Validate required fields
    if not config_params['sumopod_api_key']:
        return jsonify({'error': 'Sumopod API Key is required'}), 400
    
    if not config_params['epc_bearer_token']:
        return jsonify({'error': 'EPC Bearer Token is required'}), 400
    
    if not config_params['master_category_id']:
        return jsonify({'error': 'Master Category ID is required'}), 400
    
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
            'error': None,
            'config': config_params
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


@app.route('/api/approve/<job_id>', methods=['POST'])
def approve_submission(job_id):
    """Approve and submit extracted data to EPC"""
    with job_lock:
        if job_id not in job_status:
            return jsonify({'error': 'Job not found'}), 404
        
        job = job_status[job_id]
        
        if job['status'] != 'pending_review':
            return jsonify({'error': 'Job is not pending review'}), 400
        
        # Get edited data if provided
        edited_data = request.get_json()
        extracted_data = edited_data if edited_data else job.get('extracted_data')
        
        if not extracted_data:
            return jsonify({'error': 'No data to submit'}), 400
        
        # Update status to submitting
        job_status[job_id]['status'] = 'submitting'
        job_status[job_id]['stage'] = 'epc_submission'
    
    try:
        # Create automation instance
        config = EPCAutomationConfig(
            sumopod_base_url=job['config']['sumopod_base_url'],
            sumopod_api_key=job['config']['sumopod_api_key'],
            sumopod_model=job['config']['sumopod_model'],
            sumopod_temperature=float(job['config'].get('sumopod_temperature', 0.7)),
            sumopod_max_tokens=int(job['config'].get('sumopod_max_tokens', 2000)),
            epc_base_url=job['config']['epc_base_url'],
            epc_bearer_token=job['config']['epc_bearer_token'],
            master_category_id=job['config'].get('master_category_id')  # UUID string
        )
        
        automation = EPCPDFAutomation(config)
        
        # Submit to EPC
        success, epc_results = automation.submit_to_epc(extracted_data)
        
        with job_lock:
            if success:
                job_status[job_id]['status'] = 'completed'
                job_status[job_id]['epc_submission'] = epc_results
            else:
                job_status[job_id]['status'] = 'failed'
                job_status[job_id]['error'] = f"EPC submission had {len(epc_results['errors'])} errors"
                job_status[job_id]['epc_submission'] = epc_results
            
            job_status[job_id]['completed_at'] = datetime.now().isoformat()
        
        return jsonify({
            'success': success,
            'results': epc_results
        })
    
    except Exception as e:
        with job_lock:
            job_status[job_id]['status'] = 'error'
            job_status[job_id]['error'] = str(e)
        
        return jsonify({'error': str(e)}), 500


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
        jobs.sort(key=lambda x: x.get('uploaded_at', ''), reverse=True)
        return jsonify(jobs)


@app.route('/api/download/<job_id>')
def download_result(job_id):
    """Download extracted JSON data"""
    with job_lock:
        if job_id not in job_status:
            return jsonify({'error': 'Job not found'}), 404
        
        job = job_status[job_id]
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
    print("Motorsights EPC PDF Automation - Web UI")
    print("=" * 60)
    print("\nServer starting...")
    print("Open your browser and navigate to: http://localhost:5000")
    print("\nPress Ctrl+C to stop the server")
    print("=" * 60)
    
    app.run(debug=True, host='0.0.0.0', port=5000)