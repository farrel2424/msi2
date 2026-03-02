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
        dokumen_name=config_params.get("dokumen_name"),
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

        parts_data = result.get("parts_data")
        parts_status = "ready" if parts_data and parts_data.get("subtypes") else "pending"

        with job_lock:
            job_status[job_id].update(
                status="pending_review",
                extracted_data=result["extracted_data"],
                parts_data=parts_data,
                parts_status=parts_status,
                result=result,
                completed_at=datetime.now().isoformat(),
            )
            if result.get("extracted_data"):
                output_file = f"outputs/{job_id}_extracted.json"
                Path(output_file).write_text(
                    json.dumps(result["extracted_data"], indent=2, ensure_ascii=False),
                    encoding="utf-8",
                )
                job_status[job_id]["output_file"] = output_file

            if parts_data and parts_data.get("subtypes"):
                parts_file = f"outputs/{job_id}_parts.json"
                Path(parts_file).write_text(
                    json.dumps(parts_data, indent=2, ensure_ascii=False),
                    encoding="utf-8",
                )
                job_status[job_id]["parts_file"] = parts_file

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
        "dokumen_name": request.form.get("dokumen_name", "").strip()
                        or os.getenv("CABIN_CHASSIS_DOKUMEN_NAME", "Cabin & Chassis Manual"),
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
            "parts_data": None,
            "parts_status": "pending",
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
    """Approve the reviewed categories data and submit it to the EPC API."""
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


@app.route("/api/approve-parts/<job_id>", methods=["POST"])
def approve_parts_submission(job_id: str):
    """
    Submit the parts management data (item_category + details) for a cabin_chassis job.
    Body: { "data": <parts_data>, "category_name_en": "Frame System" }
    """
    with job_lock:
        if job_id not in job_status:
            return jsonify({"error": "Job not found."}), 404
        job = job_status[job_id]
        if job.get("partbook_type") != "cabin_chassis":
            return jsonify({"error": "Parts submission is only available for Cabin & Chassis jobs."}), 400
        if job.get("parts_status") == "submitted":
            return jsonify({"error": "Parts already submitted for this job."}), 400

        job_status[job_id]["parts_status"] = "submitting"

    try:
        req = request.get_json() or {}
        parts_data       = req.get("data") or job.get("parts_data")
        category_name_en = (req.get("category_name_en") or "").strip()

        if not parts_data:
            return jsonify({"error": "No parts data available."}), 400
        if not category_name_en:
            return jsonify({"error": "category_name_en is required."}), 400

        config     = _build_config(job["config"])
        automation = EPCPDFAutomation(config)

        success, parts_results = automation.submit_parts_to_epc(
            parts_data=parts_data,
            category_name_en=category_name_en,
            master_category_id=job["config"].get("master_category_id"),
            dokumen_name=job["config"].get("dokumen_name"),
        )

        with job_lock:
            job_status[job_id].update(
                parts_status="submitted" if success else "error",
                parts_results=parts_results,
            )

        return jsonify({"success": success, "parts_results": parts_results})

    except Exception as e:
        with job_lock:
            job_status[job_id]["parts_status"] = "error"
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

        new_prompt = (request.get_json() or {}).get("prompt", "").strip()
        config_params = dict(job["config"])
        if new_prompt:
            config_params["custom_prompt"] = new_prompt

        job_status[job_id].update(
            status="queued",
            stage="re-extract queued",
            extracted_data=None,
            parts_data=None,
            parts_status="pending",
            result=None,
            error=None,
        )

    thread = threading.Thread(target=_process_pdf_async, args=(job_id, upload_path, config_params))
    thread.daemon = True
    thread.start()

    return jsonify({"success": True, "message": "Re-extraction started."})


@app.route("/api/download/<job_id>")
def download_json(job_id: str):
    """Download the extracted categories JSON for a given job."""
    with job_lock:
        if job_id not in job_status:
            return jsonify({"error": "Job not found."}), 404
        output_file = job_status[job_id].get("output_file")

    if not output_file or not os.path.exists(output_file):
        return jsonify({"error": "Output file not found."}), 404

    return send_file(output_file, as_attachment=True, download_name=f"extracted_{job_id}.json")


@app.route("/api/download-parts/<job_id>")
def download_parts_json(job_id: str):
    """Download the extracted parts JSON for a given job."""
    with job_lock:
        if job_id not in job_status:
            return jsonify({"error": "Job not found."}), 404
        parts_file = job_status[job_id].get("parts_file")

    if not parts_file or not os.path.exists(parts_file):
        return jsonify({"error": "Parts file not found."}), 404

    return send_file(parts_file, as_attachment=True, download_name=f"parts_{job_id}.json")


@app.route("/api/jobs")
def list_jobs():
    """Return all jobs (for history page)."""
    with job_lock:
        jobs = list(job_status.values())
    # Sort newest first; strip large blobs for the list view
    safe = []
    for j in jobs:
        safe.append({k: v for k, v in j.items() if k not in ("extracted_data", "parts_data", "result", "config")})
    safe.sort(key=lambda j: j.get("uploaded_at", ""), reverse=True)
    return jsonify({"jobs": safe})


@app.route("/api/jobs/<job_id>", methods=["DELETE"])
def delete_job(job_id: str):
    with job_lock:
        if job_id not in job_status:
            return jsonify({"error": "Job not found."}), 404
        del job_status[job_id]
    return jsonify({"success": True})


@app.route("/api/jobs", methods=["DELETE"])
def clear_jobs():
    with job_lock:
        job_status.clear()
    return jsonify({"success": True})


if __name__ == "__main__":
    app.run(debug=True, port=5000)