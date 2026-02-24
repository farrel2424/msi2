"""
Web UI for Motorsights EPC PDF Automation
Updated to support Engine, Transmission, and Cabin & Chassis partbook types.
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

Path(app.config['UPLOAD_FOLDER']).mkdir(exist_ok=True)
Path('outputs').mkdir(exist_ok=True)

# Master Categories — map internal key → env UUID + display names
MASTER_CATEGORIES = {
    'transmission': {
        'id': os.getenv('MASTER_CATEGORY_TRANSMISSION_ID', ''),
        'name_en': 'Transmission',
        'name_cn': '变速器',
        'partbook_type': 'transmission'   # drives extraction strategy
    },
    'cabin_chassis': {
        'id': os.getenv('MASTER_CATEGORY_CABIN_CHASSIS_ID', ''),
        'name_en': 'Cabin & Chassis',
        'name_cn': '驾驶室和底盘',
        'partbook_type': 'cabin_chassis'
    },
    'engine': {
        'id': os.getenv('MASTER_CATEGORY_ENGINE_ID', ''),
        'name_en': 'Engine',
        'name_cn': '发动机',
        'partbook_type': 'engine'
    },
    'axle': {
        'id': os.getenv('MASTER_CATEGORY_AXLE_ID', ''),
        'name_en': 'Axle',
        'name_cn': '车轴',
        'partbook_type': 'cabin_chassis'  # Axle uses same strategy as Cabin & Chassis
    }
}


def _get_master_category_info(master_category_id: str) -> dict:
    """Return the full info dict for a given master category UUID."""
    for value in MASTER_CATEGORIES.values():
        if value['id'] and value['id'] == master_category_id:
            return value
    return {'name_en': '', 'partbook_type': 'cabin_chassis'}


job_status = {}
job_lock = threading.Lock()


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']


def process_pdf_async(job_id, pdf_path, config_params):
    """Process PDF in background thread — EXTRACTION ONLY (mandatory review)."""
    try:
        with job_lock:
            job_status[job_id]['status'] = 'processing'
            job_status[job_id]['stage'] = 'initializing'

        custom_prompt = config_params.get('custom_prompt', '').strip()
        partbook_type = config_params.get('partbook_type', 'cabin_chassis')

        config = EPCAutomationConfig(
            sumopod_base_url=config_params.get('sumopod_base_url', 'https://ai.sumopod.com/v1'),
            sumopod_api_key=config_params.get('sumopod_api_key'),
            sumopod_model=config_params.get('sumopod_model', 'gpt4o'),
            sumopod_temperature=float(config_params.get('sumopod_temperature', 0.7)),
            sumopod_max_tokens=int(config_params.get('sumopod_max_tokens', 2000)),
            sumopod_custom_prompt=custom_prompt if custom_prompt else None,
            sso_gateway_url=config_params.get('sso_gateway_url', 'https://dev-gateway.motorsights.com'),
            sso_email=config_params.get('sso_email'),
            sso_password=config_params.get('sso_password'),
            epc_base_url=config_params.get('epc_base_url', 'https://dev-gateway.motorsights.com/api/epc'),
            max_retries=3,
            enable_review_mode=True,   # ALWAYS — mandatory review before submit
            master_category_id=config_params.get('master_category_id'),
            master_category_name_en=config_params.get('master_category_name_en', ''),
            partbook_type=partbook_type   # NEW: pass type to automation
        )

        with job_lock:
            job_status[job_id]['stage'] = 'extracting'

        automation = EPCPDFAutomation(config)
        result = automation.process_pdf(Path(pdf_path), auto_submit=False)

        with job_lock:
            job_status[job_id]['status'] = 'pending_review'
            job_status[job_id]['extracted_data'] = result['extracted_data']
            job_status[job_id]['result'] = result
            job_status[job_id]['completed_at'] = datetime.now().isoformat()

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
    return render_template('epc_index.html')


@app.route('/history')
def history_page():
    return render_template('epc_history.html')


@app.route('/api/master-categories')
def get_master_categories():
    """Return configured master categories with their partbook_type."""
    categories = []
    for key, value in MASTER_CATEGORIES.items():
        if value['id']:
            categories.append({
                'key': key,
                'id': value['id'],
                'name_en': value['name_en'],
                'name_cn': value['name_cn'],
                'partbook_type': value['partbook_type']
            })
    return jsonify({'categories': categories})


@app.route('/api/upload', methods=['POST'])
def upload_file():
    """Handle PDF upload and start background processing."""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['file']
    if not file or not allowed_file(file.filename):
        return jsonify({'error': 'Invalid file type — only PDF allowed'}), 400

    master_category_id = request.form.get('master_category_id', '').strip()
    ai_model = request.form.get('ai_model', 'gpt4o')
    custom_prompt = request.form.get('custom_prompt', '')
    sumopod_api_key = request.form.get('sumopod_api_key', '').strip() or os.getenv('SUMOPOD_API_KEY', '')
    sso_email = request.form.get('sso_email', '').strip() or os.getenv('SSO_EMAIL', '')
    sso_password = request.form.get('sso_password', '').strip() or os.getenv('SSO_PASSWORD', '')

    if not master_category_id:
        return jsonify({'error': 'master_category_id is required'}), 400

    # Resolve display name and partbook_type from the master category UUID
    cat_info = _get_master_category_info(master_category_id)
    master_category_name_en = cat_info.get('name_en', '')
    partbook_type = cat_info.get('partbook_type', 'cabin_chassis')

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
        'master_category_name_en': master_category_name_en,
        'partbook_type': partbook_type,   # NEW
        'custom_prompt': custom_prompt
    }

    filename = secure_filename(file.filename)
    job_id = str(uuid.uuid4())
    upload_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{job_id}_{filename}")
    file.save(upload_path)

    with job_lock:
        job_status[job_id] = {
            'id': job_id,
            'filename': filename,
            'partbook_type': partbook_type,
            'master_category_name_en': master_category_name_en,
            'status': 'queued',
            'stage': 'uploaded',
            'uploaded_at': datetime.now().isoformat(),
            'result': None,
            'error': None,
            'config': config_params,
            'upload_path': upload_path
        }

    thread = threading.Thread(target=process_pdf_async, args=(job_id, upload_path, config_params))
    thread.daemon = True
    thread.start()

    return jsonify({
        'job_id': job_id,
        'filename': filename,
        'partbook_type': partbook_type,
        'message': 'File uploaded successfully. Processing started.'
    })


@app.route('/api/status/<job_id>')
def get_status(job_id):
    with job_lock:
        if job_id not in job_status:
            return jsonify({'error': 'Job not found'}), 404
        return jsonify(job_status[job_id])


@app.route('/api/approve/<job_id>', methods=['POST'])
def approve_submission(job_id):
    """Approve and submit extracted data to EPC."""
    with job_lock:
        if job_id not in job_status:
            return jsonify({'error': 'Job not found'}), 404
        job = job_status[job_id]
        if job['status'] != 'pending_review':
            return jsonify({'error': 'Job is not pending review'}), 400
        request_data = request.get_json()
        edited_data = request_data.get('data') if request_data else None
        extracted_data = edited_data if edited_data else job.get('extracted_data')
        if not extracted_data:
            return jsonify({'error': 'No data to submit'}), 400
        job_status[job_id]['status'] = 'submitting'
        job_status[job_id]['stage'] = 'epc_submission'

    try:
        master_category_id = job['config'].get('master_category_id')
        master_category_name_en = job['config'].get('master_category_name_en', '')
        partbook_type = job['config'].get('partbook_type', 'cabin_chassis')

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
            master_category_name_en=master_category_name_en,
            partbook_type=partbook_type   # NEW
        )

        automation = EPCPDFAutomation(config)
        success, epc_results = automation.submit_to_epc(
            extracted_data,
            master_category_id=master_category_id,
            master_category_name_en=master_category_name_en
        )

        with job_lock:
            if success:
                job_status[job_id]['status'] = 'completed'
                job_status[job_id]['stage'] = 'completed'
            else:
                job_status[job_id]['status'] = 'error'
                job_status[job_id]['error'] = f"{len(epc_results.get('errors', []))} submission errors"
            job_status[job_id]['epc_results'] = epc_results
            job_status[job_id]['completed_at'] = datetime.now().isoformat()

        return jsonify({'success': success, 'epc_results': epc_results})

    except Exception as e:
        with job_lock:
            job_status[job_id]['status'] = 'error'
            job_status[job_id]['error'] = str(e)
        return jsonify({'error': str(e)}), 500


@app.route('/api/re-extract/<job_id>', methods=['POST'])
def re_extract(job_id):
    """Re-extract with a new custom prompt."""
    with job_lock:
        if job_id not in job_status:
            return jsonify({'error': 'Job not found'}), 404
        job = job_status[job_id]
        upload_path = job.get('upload_path')
        if not upload_path or not os.path.exists(upload_path):
            return jsonify({'error': 'Original PDF file not found'}), 404

    data = request.get_json()
    new_prompt = data.get('prompt', '')

    config_params = job['config'].copy()
    config_params['custom_prompt'] = new_prompt

    with job_lock:
        job_status[job_id]['status'] = 'queued'
        job_status[job_id]['stage'] = 'uploaded'
        job_status[job_id]['error'] = None
        job_status[job_id]['result'] = None
        job_status[job_id]['extracted_data'] = None
        job_status[job_id]['config'] = config_params

    thread = threading.Thread(target=process_pdf_async, args=(job_id, upload_path, config_params))
    thread.daemon = True
    thread.start()

    return jsonify({'message': 'Re-extraction started with new prompt'})


@app.route('/api/jobs')
def list_jobs():
    with job_lock:
        jobs = list(job_status.values())
    return jsonify({'jobs': jobs})


@app.route('/api/download/<job_id>')
def download_output(job_id):
    with job_lock:
        if job_id not in job_status:
            return jsonify({'error': 'Job not found'}), 404
        output_file = job_status[job_id].get('output_file')
    if not output_file or not os.path.exists(output_file):
        return jsonify({'error': 'Output file not found'}), 404
    return send_file(output_file, as_attachment=True)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)