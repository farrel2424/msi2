"""
Web UI for Motorsights EPC PDF Automation
Flask application: upload → extract → review → submit.
"""
import json
import os
import threading
import uuid
from datetime import datetime
from pathlib import Path

from flask import Flask, jsonify, render_template, request, send_file
from werkzeug.utils import secure_filename

from epc_automation import EPCAutomationConfig, EPCPDFAutomation

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = "uploads"
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16 MB
app.config["ALLOWED_EXTENSIONS"] = {"pdf"}

Path(app.config["UPLOAD_FOLDER"]).mkdir(exist_ok=True)
Path("outputs").mkdir(exist_ok=True)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_SSO_GATEWAY = "https://dev-gateway.motorsights.com"
_DEFAULT_EPC_BASE_URL = "https://dev-gateway.motorsights.com/api/epc"
_DEFAULT_SUMOPOD_URL = "https://ai.sumopod.com/v1"

MASTER_CATEGORIES = {
    "transmission": {
        "id": os.getenv("MASTER_CATEGORY_TRANSMISSION_ID", ""),
        "name_en": "Transmission",
        "name_cn": "变速器",
        "partbook_type": "transmission",
    },
    "cabin_chassis": {
        "id": os.getenv("MASTER_CATEGORY_CABIN_CHASSIS_ID", ""),
        "name_en": "Cabin & Chassis",
        "name_cn": "驾驶室和底盘",
        "partbook_type": "cabin_chassis",
    },
    "engine": {
        "id": os.getenv("MASTER_CATEGORY_ENGINE_ID", ""),
        "name_en": "Engine",
        "name_cn": "发动机",
        "partbook_type": "engine",
    },
    "axle": {
        "id": os.getenv("MASTER_CATEGORY_AXLE_ID", ""),
        "name_en": "Axle",
        "name_cn": "车轴",
        "partbook_type": "axle_drive",
    },
}

# ---------------------------------------------------------------------------
# In-memory job store
# ---------------------------------------------------------------------------

job_status: dict = {}
job_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in app.config["ALLOWED_EXTENSIONS"]


def _get_master_category_info(master_category_id: str) -> dict:
    """Return category metadata for the given UUID, or sensible defaults."""
    for value in MASTER_CATEGORIES.values():
        if value["id"] and value["id"] == master_category_id:
            return value
    return {"name_en": "", "partbook_type": "cabin_chassis"}


def _build_config(config_params: dict) -> EPCAutomationConfig:
    """Construct an EPCAutomationConfig from a flat config_params dict."""
    return EPCAutomationConfig(
        sumopod_base_url=config_params.get("sumopod_base_url", _DEFAULT_SUMOPOD_URL),
        sumopod_api_key=config_params.get("sumopod_api_key"),
        sumopod_model=config_params.get("sumopod_model", "gpt4o"),
        sumopod_temperature=float(config_params.get("sumopod_temperature", 0.7)),
        sumopod_max_tokens=int(config_params.get("sumopod_max_tokens", 2000)),
        sumopod_custom_prompt=config_params.get("custom_prompt") or None,
        sso_gateway_url=config_params.get("sso_gateway_url", _DEFAULT_SSO_GATEWAY),
        sso_email=config_params.get("sso_email"),
        sso_password=config_params.get("sso_password"),
        epc_base_url=config_params.get("epc_base_url", _DEFAULT_EPC_BASE_URL),
        master_category_id=config_params.get("master_category_id"),
        master_category_name_en=config_params.get("master_category_name_en", ""),
        partbook_type=config_params.get("partbook_type", "cabin_chassis"),
        max_retries=3,              
        enable_review_mode=True,
    )

# ---------------------------------------------------------------------------
# Background processing
# ---------------------------------------------------------------------------


def _process_pdf_async(job_id: str, pdf_path: str, config_params: dict) -> None:
    """Extract PDF data in a background thread. Submission requires manual approval."""
    try:
        with job_lock:
            job_status[job_id].update(status="processing", stage="initializing")

        config = _build_config(config_params)

        with job_lock:
            job_status[job_id]["stage"] = "extracting"

        result = EPCPDFAutomation(config).process_pdf(Path(pdf_path), auto_submit=False)

        with job_lock:
            job_status[job_id].update(
                status="pending_review",
                extracted_data=result["extracted_data"],
                result=result,
                completed_at=datetime.now().isoformat(),
            )
            if result.get("extracted_data"):
                output_file = f"outputs/{job_id}_extracted.json"
                Path(output_file).write_text(
                    json.dumps(result["extracted_data"], indent=2, ensure_ascii=False),
                    encoding="utf-8"
                )
                job_status[job_id]["output_file"] = output_file

    except Exception as e:
        with job_lock:
            job_status[job_id].update(
                status="error",
                error=str(e),
                completed_at=datetime.now().isoformat(),
            )

# ---------------------------------------------------------------------------
# Routes — pages
# ---------------------------------------------------------------------------


@app.route("/")
def index():
    return render_template("epc_index.html")


@app.route("/history")
def history_page():
    return render_template("epc_history.html")

# ---------------------------------------------------------------------------
# Routes — API
# ---------------------------------------------------------------------------


@app.route("/api/master-categories")
def get_master_categories():
    """Return configured master categories with partbook_type metadata."""
    categories = [
        {
            "key": key,
            "id": value["id"],
            "name_en": value["name_en"],
            "name_cn": value["name_cn"],
            "partbook_type": value["partbook_type"],
        }
        for key, value in MASTER_CATEGORIES.items()
        if value["id"]
    ]
    return jsonify({"categories": categories})


@app.route("/api/upload", methods=["POST"])
def upload_file():
    """Handle PDF upload and start background extraction."""
    if "file" not in request.files:
        return jsonify({"error": "No file provided."}), 400

    file = request.files["file"]
    if not file or not allowed_file(file.filename):
        return jsonify({"error": "Invalid file type — only PDF allowed."}), 400

    master_category_id = request.form.get("master_category_id", "").strip()
    if not master_category_id:
        return jsonify({"error": "master_category_id is required."}), 400

    cat_info = _get_master_category_info(master_category_id)

    config_params = {
        "sumopod_base_url": _DEFAULT_SUMOPOD_URL,
        "sumopod_api_key": request.form.get("sumopod_api_key", "").strip() or os.getenv("SUMOPOD_API_KEY", ""),
        "sumopod_model": request.form.get("ai_model", "gpt4o"),
        "sumopod_temperature": 0.7,
        "sumopod_max_tokens": 2000,
        "custom_prompt": request.form.get("custom_prompt", ""),
        "sso_gateway_url": _DEFAULT_SSO_GATEWAY,
        "sso_email": request.form.get("sso_email", "").strip() or os.getenv("SSO_EMAIL", ""),
        "sso_password": request.form.get("sso_password", "").strip() or os.getenv("SSO_PASSWORD", ""),
        "epc_base_url": _DEFAULT_EPC_BASE_URL,
        "master_category_id": master_category_id,
        "master_category_name_en": cat_info.get("name_en", ""),
        "partbook_type": cat_info.get("partbook_type", "cabin_chassis"),
    }

    filename = secure_filename(file.filename)
    job_id = str(uuid.uuid4())
    upload_path = os.path.join(app.config["UPLOAD_FOLDER"], f"{job_id}_{filename}")
    file.save(upload_path)

    with job_lock:
        job_status[job_id] = {
            "id": job_id,
            "filename": filename,
            "partbook_type": config_params["partbook_type"],
            "master_category_name_en": config_params["master_category_name_en"],
            "status": "queued",
            "stage": "uploaded",
            "uploaded_at": datetime.now().isoformat(),
            "result": None,
            "error": None,
            "config": config_params,
            "upload_path": upload_path,
        }

    thread = threading.Thread(target=_process_pdf_async, args=(job_id, upload_path, config_params))
    thread.daemon = True
    thread.start()

    return jsonify({
        "job_id": job_id,
        "filename": filename,
        "partbook_type": config_params["partbook_type"],
        "message": "File uploaded successfully. Processing started.",
    })


@app.route("/api/status/<job_id>")
def get_status(job_id: str):
    with job_lock:
        if job_id not in job_status:
            return jsonify({"error": "Job not found."}), 404
        return jsonify(job_status[job_id])


@app.route("/api/approve/<job_id>", methods=["POST"])
def approve_submission(job_id: str):
    """Approve the reviewed data and submit it to the EPC API."""
    with job_lock:
        if job_id not in job_status:
            return jsonify({"error": "Job not found."}), 404
        job = job_status[job_id]
        if job["status"] != "pending_review":
            return jsonify({"error": "Job is not pending review."}), 400

        request_data = request.get_json()
        extracted_data = (request_data or {}).get("data") or job.get("extracted_data")
        if not extracted_data:
            return jsonify({"error": "No data to submit."}), 400

        job_status[job_id].update(status="submitting", stage="epc_submission")

    try:
        config = _build_config(job["config"])
        automation = EPCPDFAutomation(config)
        success, epc_results = automation.submit_to_epc(
            extracted_data,
            master_category_id=job["config"].get("master_category_id"),
            master_category_name_en=job["config"].get("master_category_name_en", ""),
        )

        with job_lock:
            job_status[job_id].update(
                status="completed" if success else "error",
                stage="completed" if success else "error",
                epc_results=epc_results,
                completed_at=datetime.now().isoformat(),
                **({"error": f"{len(epc_results.get('errors', []))} submission error(s)."} if not success else {}),
            )

        return jsonify({"success": success, "epc_results": epc_results})

    except Exception as e:
        with job_lock:
            job_status[job_id].update(status="error", error=str(e))
        return jsonify({"error": str(e)}), 500


@app.route("/api/re-extract/<job_id>", methods=["POST"])
def re_extract(job_id: str):
    """Re-run extraction with a modified prompt."""
    with job_lock:
        if job_id not in job_status:
            return jsonify({"error": "Job not found."}), 404
        job = job_status[job_id]
        upload_path = job.get("upload_path")
        if not upload_path or not os.path.exists(upload_path):
            return jsonify({"error": "Original PDF file not found."}), 404

    new_prompt = (request.get_json() or {}).get("prompt", "")
    config_params = {**job["config"], "custom_prompt": new_prompt}

    with job_lock:
        job_status[job_id].update(
            status="queued", stage="uploaded",
            error=None, result=None, extracted_data=None,
            config=config_params,
        )

    thread = threading.Thread(target=_process_pdf_async, args=(job_id, upload_path, config_params))
    thread.daemon = True
    thread.start()

    return jsonify({"message": "Re-extraction started with new prompt."})


@app.route("/api/jobs")
def list_jobs():
    with job_lock:
        return jsonify(list(job_status.values()))


@app.route("/api/download/<job_id>")
def download_output(job_id: str):
    with job_lock:
        if job_id not in job_status:
            return jsonify({"error": "Job not found."}), 404
        output_file = job_status[job_id].get("output_file")

    if not output_file or not os.path.exists(output_file):
        return jsonify({"error": "Output file not found."}), 404
    return send_file(output_file, as_attachment=True)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5001)