"""
Web UI for Motorsights EPC PDF Automation
Updated to use SSO authentication for dynamic bearer token generation
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

# Master Categories Configuration
MASTER_CATEGORIES = {
    'transmission': {
        'id': os.getenv('MASTER_CATEGORY_TRANSMISSION_ID', ''),
        'name_en': 'Transmission',
        'name_cn': '传播'
    },
    'cabin_chassis': {
        'id': os.getenv('MASTER_CATEGORY_CABIN_CHASSIS_ID', ''),
        'name_en': 'Cabin & Chassis',
        'name_cn': '驾驶室和底盘'
    },
    'engine': {
        'id': os.getenv('MASTER_CATEGORY_ENGINE_ID', ''),
        'name_en': 'Engine',
        'name_cn': '引擎'
    },
    'axle': {
        'id': os.getenv('MASTER_CATEGORY_AXLE_ID', ''),
        'name_en': 'Axle',
        'name_cn': '轴'
    }
}


def _get_master_category_name(master_category_id: str) -> str:
    """
    FIX: Resolve the English name for a master category UUID.
    Returns the name_en (e.g. "Engine") or an empty string if not found.
    """
    for value in MASTER_CATEGORIES.values():
        if value['id'] and value['id'] == master_category_id:
            return value['name_en']
    return ""


# Global storage for job status
job_status = {}
job_lock = threading.Lock()


def allowed_file(filename):
    """Check if file extension is allowed"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']


def process_pdf_async(job_id, pdf_path, config_params):
    """Process PDF in background thread - EXTRACTION ONLY (no auto-submit)"""
    try:
        with job_lock:
            job_status[job_id]['status'] = 'processing'
            job_status[job_id]['stage'] = 'initializing'
        
        # Extract parameters
        custom_prompt = config_params.get('custom_prompt', '').strip()
        
        # Create config with SSO authentication
        config = EPCAutomationConfig(
            sumopod_base_url=config_params.get('sumopod_base_url', 'https://ai.sumopod.com/v1'),
            sumopod_api_key=config_params.get('sumopod_api_key'),
            sumopod_model=config_params.get('sumopod_model', 'gpt4o'),
            sumopod_temperature=float(config_params.get('sumopod_temperature', 0.7)),
            sumopod_max_tokens=int(config_params.get('sumopod_max_tokens', 2000)),
            sumopod_custom_prompt=custom_prompt if custom_prompt else None,
            
            # SSO Authentication
            sso_gateway_url=config_params.get('sso_gateway_url', 'https://dev-gateway.motorsights.com'),
            sso_email=config_params.get('sso_email'),
            sso_password=config_params.get('sso_password'),
            
            epc_base_url=config_params.get('epc_base_url', 'https://dev-gateway.motorsights.com/api/epc'),
            
            max_retries=3,
            enable_review_mode=True,  # ALWAYS True - mandatory review
            master_category_id=config_params.get('master_category_id'),
            master_category_name_en=config_params.get('master_category_name_en', '')  # FIX: pass name
        )
        
        # Create automation
        automation = EPCPDFAutomation(config)
        
        # Update status
        with job_lock:
            job_status[job_id]['stage'] = 'converting_pdf'
        
        # Process PDF - EXTRACTION ONLY
        result = automation.process_pdf(
            Path(pdf_path),
            auto_submit=False  # MANDATORY: Never auto-submit
        )
        
        # Update final status
        with job_lock:
            job_status[job_id]['status'] = 'pending_review'
            job_status[job_id]['extracted_data'] = result['extracted_data']
            job_status[job_id]['result'] = result
            job_status[job_id]['completed_at'] = datetime.now().isoformat()
            
            # Save extracted data for review
            if result.get('extracted_data'):
                output_file = f"outputs/{job_id}_extracted.json"
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(result['extracted_data'], f, indent=2, ensure_ascii=False)
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


@app.route('/history')
def history_page():
    """Render processing history page"""
    return render_template('epc_history.html')


@app.route('/api/master-categories')
def get_master_categories():
    """Get configured master categories"""
    categories = []
    for key, value in MASTER_CATEGORIES.items():
        if value['id']:  # Only include if UUID is configured
            categories.append({
                'key': key,
                'id': value['id'],
                'name_en': value['name_en'],
                'name_cn': value['name_cn'],
                'display': f"{value['name_en']} / {value['name_cn']}"
            })
    return jsonify(categories)


@app.route('/api/upload', methods=['POST'])
def upload_file():
    """Handle file upload and start extraction"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not allowed_file(file.filename):
        return jsonify({'error': 'Only PDF files are allowed'}), 400
    
    # Get configuration from form
    sumopod_api_key = request.form.get('sumopod_api_key', os.getenv('SUMOPOD_API_KEY'))
    
    # SSO credentials (required for bearer token generation)
    sso_email = request.form.get('sso_email', os.getenv('SSO_EMAIL'))
    sso_password = request.form.get('sso_password', os.getenv('SSO_PASSWORD'))
    
    master_category_id = request.form.get('master_category_id', '')
    ai_model = request.form.get('ai_model', 'gpt4o')
    custom_prompt = request.form.get('custom_prompt', '')
    
    # Validate required fields
    if not sumopod_api_key:
        return jsonify({'error': 'Sumopod API Key is required (set in .env or provide in form)'}), 400
    
    if not sso_email or not sso_password:
        return jsonify({'error': 'SSO Email and Password are required for authentication (set in .env or provide in form)'}), 400
    
    if not master_category_id:
        return jsonify({'error': 'Master Category is required'}), 400
    
    # FIX: resolve the human-readable name for this ID
    master_category_name_en = _get_master_category_name(master_category_id)
    
    config_params = {
        'sumopod_base_url': 'https://ai.sumopod.com/v1',
        'sumopod_api_key': sumopod_api_key,
        'sumopod_model': ai_model,
        'sumopod_temperature': 0.7,
        'sumopod_max_tokens': 2000,
        'sso_gateway_url': 'https://dev-gateway.motorsights.com',
        'sso_email': sso_email,
        'sso_password': sso_password,
        'epc_base_url': 'https://dev-gateway.motorsights.com/api/epc',
        'master_category_id': master_category_id,
        'master_category_name_en': master_category_name_en,  # FIX: store resolved name
        'custom_prompt': custom_prompt
    }
    
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
            'config': config_params,
            'upload_path': upload_path
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


@app.route('/api/re-extract/<job_id>', methods=['POST'])
def re_extract(job_id):
    """Re-extract with new prompt"""
    with job_lock:
        if job_id not in job_status:
            return jsonify({'error': 'Job not found'}), 404
        
        job = job_status[job_id]
        upload_path = job.get('upload_path')
        
        if not upload_path or not os.path.exists(upload_path):
            return jsonify({'error': 'Original PDF file not found'}), 404
    
    # Get new prompt
    data = request.get_json()
    new_prompt = data.get('prompt', '')
    
    # Update config with new prompt
    config_params = job['config'].copy()
    config_params['custom_prompt'] = new_prompt
    
    # Reset job status
    with job_lock:
        job_status[job_id]['status'] = 'queued'
        job_status[job_id]['stage'] = 'uploaded'
        job_status[job_id]['error'] = None
        job_status[job_id]['result'] = None
        job_status[job_id]['extracted_data'] = None
        job_status[job_id]['config'] = config_params
    
    # Re-process
    thread = threading.Thread(
        target=process_pdf_async,
        args=(job_id, upload_path, config_params)
    )
    thread.daemon = True
    thread.start()
    
    return jsonify({'message': 'Re-extraction started with new prompt'})


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
        request_data = request.get_json()
        edited_data = request_data.get('data') if request_data else None
        extracted_data = edited_data if edited_data else job.get('extracted_data')
        
        if not extracted_data:
            return jsonify({'error': 'No data to submit'}), 400
        
        # Update status to submitting
        job_status[job_id]['status'] = 'submitting'
        job_status[job_id]['stage'] = 'epc_submission'
    
    try:
        # FIX: retrieve the stored master category name alongside the ID
        master_category_id = job['config'].get('master_category_id')
        master_category_name_en = job['config'].get('master_category_name_en', '')

        # Create automation instance with SSO auth
        config = EPCAutomationConfig(
            sumopod_base_url=job['config']['sumopod_base_url'],
            sumopod_api_key=job['config']['sumopod_api_key'],
            sumopod_model=job['config']['sumopod_model'],
            sumopod_temperature=float(job['config'].get('sumopod_temperature', 0.7)),
            sumopod_max_tokens=int(job['config'].get('sumopod_max_tokens', 2000)),
            sso_gateway_url=job['config'].get('sso_gateway_url', 'https://dev-gateway.motorsights.com'),
            sso_email=job['config']['sso_email'],
            sso_password=job['config']['sso_password'],
            epc_base_url=job['config']['epc_base_url'],
            master_category_id=master_category_id,
            master_category_name_en=master_category_name_en  # FIX: pass name into config
        )
        
        automation = EPCPDFAutomation(config)
        
        # FIX: pass master_category_name_en explicitly to submit_to_epc
        success, epc_results = automation.submit_to_epc(
            extracted_data,
            master_category_id=master_category_id,
            master_category_name_en=master_category_name_en
        )
        
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
        # Remove sensitive data from response
        for job in jobs:
            job.pop('upload_path', None)
            # Don't expose SSO credentials in response
            if 'config' in job:
                config = job['config'].copy()
                config.pop('sso_password', None)
                config.pop('sumopod_api_key', None)
                job['config'] = config
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
    print("Motorsights EPC PDF Automation - Web UI with SSO Auth")
    print("=" * 60)
    print("\n🔐 SSO Authentication Enabled")
    print("Bearer tokens are generated dynamically via SSO login")
    print("\n⚠️  MANDATORY REVIEW MODE")
    print("All extractions require manual review before submission")
    print("\n📋 Master Categories Configured:")
    for key, value in MASTER_CATEGORIES.items():
        if value['id']:
            print(f"  ✓ {value['name_en']} / {value['name_cn']}")
        else:
            print(f"  ✗ {value['name_en']} / {value['name_cn']} (UUID not configured)")
    print("\n" + "=" * 60)
    print("\nServer starting...")
    print("Open your browser: http://localhost:5000")
    print("\nPress Ctrl+C to stop")
    print("=" * 60)
    
    app.run(debug=True, host='0.0.0.0', port=5000)