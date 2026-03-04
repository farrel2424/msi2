"""
epc_web_ui.py
─────────────────────────────────────────────────────────────────────────────
Web UI for Motorsights EPC PDF Automation.

Supports four partbook types: cabin_chassis, engine, transmission, axle_drive.

WORKFLOW (Cabin & Chassis — 3-level hierarchy)
───────────────────────────────────────────────
  1. Upload partbook PDF (ZIP-of-JPEGs format)
  2. Stage 1 — Extract category/type-category structure → review → submit
  3. Stage 2 — Extract parts rows per subtype          → review → submit
     (Parts Management: POST /item_category/create with data_items)
"""

from __future__ import annotations

import json
import os
import threading
import uuid
from datetime import datetime
from pathlib import Path

from flask import Flask, jsonify, render_template, request, send_file
from werkzeug.utils import secure_filename

from epc_automation import EPCPDFAutomation, EPCAutomationConfig

app = Flask(__name__)
app.config["UPLOAD_FOLDER"]        = "uploads"
app.config["MAX_CONTENT_LENGTH"]   = 16 * 1024 * 1024  # 16 MB
app.config["ALLOWED_EXTENSIONS"]   = {"pdf"}

Path(app.config["UPLOAD_FOLDER"]).mkdir(exist_ok=True)
Path("outputs").mkdir(exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# Master category registry
# ─────────────────────────────────────────────────────────────────────────────
MASTER_CATEGORIES = {
    "transmission": {
        "id":           os.getenv("MASTER_CATEGORY_TRANSMISSION_ID", ""),
        "name_en":      "Transmission",
        "name_cn":      "变速器",
        "partbook_type": "transmission",
    },
    "cabin_chassis": {
        "id":           os.getenv("MASTER_CATEGORY_CABIN_CHASSIS_ID", ""),
        "name_en":      "Cabin & Chassis",
        "name_cn":      "驾驶室和底盘",
        "partbook_type": "cabin_chassis",
    },
    "engine": {
        "id":           os.getenv("MASTER_CATEGORY_ENGINE_ID", ""),
        "name_en":      "Engine",
        "name_cn":      "发动机",
        "partbook_type": "engine",
    },
    "axle": {
        "id":           os.getenv("MASTER_CATEGORY_AXLE_ID", ""),
        "name_en":      "Axle",
        "name_cn":      "车轴",
        "partbook_type": "axle_drive",
    },
}

def _get_master_category_info(master_category_id: str) -> dict:
    for v in MASTER_CATEGORIES.values():
        if v["id"] and v["id"] == master_category_id:
            return v
    return {"name_en": "", "partbook_type": "cabin_chassis"}

# ─────────────────────────────────────────────────────────────────────────────
# In-memory job store
# ─────────────────────────────────────────────────────────────────────────────
job_status: dict = {}
job_lock = threading.Lock()

def allowed_file(filename: str) -> bool:
    return (
        "." in filename
        and filename.rsplit(".", 1)[1].lower() in app.config["ALLOWED_EXTENSIONS"]
    )

# ─────────────────────────────────────────────────────────────────────────────
# Background workers
# ─────────────────────────────────────────────────────────────────────────────

def _run_stage1(job_id: str, pdf_path: str, config_params: dict):
    with job_lock:
        job_status[job_id]["status"]  = "processing"
        job_status[job_id]["message"] = "Extracting category structure …"

    try:
        config = EPCAutomationConfig(**config_params)
        automation = EPCPDFAutomation(config)
        result = automation.process_pdf(
            pdf_path    = Path(pdf_path),
            auto_submit = False,
        )

        with job_lock:
            job_status[job_id]["status"]           = "review"
            job_status[job_id]["message"]          = "Structure extracted — awaiting review"
            job_status[job_id]["extracted_data"]   = result.get("extracted_data", {})
            job_status[job_id]["code_to_category"] = result.get("code_to_category", {})  # ← ADD
            job_status[job_id]["stage"]            = "structure"

    except Exception as e:
        with job_lock:
            job_status[job_id]["status"]  = "error"
            job_status[job_id]["message"] = str(e)


def _run_stage2(job_id: str, pdf_path: str, config_params: dict,
                master_category_id: str, dokumen_name: str, target_id_start: int):
    """Background thread: Stage 2 — parts extraction."""
    with job_lock:
        job_status[job_id]["status"]  = "processing_parts"
        job_status[job_id]["message"] = "Extracting parts rows from tables …"

    try:

        with job_lock:
            code_to_category = job_status[job_id].get("code_to_category", {})

        config = EPCAutomationConfig(**config_params)
        automation = EPCPDFAutomation(config)
        result = automation.process_parts(
            pdf_path           = Path(pdf_path),
            master_category_id = master_category_id,
            dokumen_name       = dokumen_name,
            target_id_start    = target_id_start,
            auto_submit        = False,   # pause for review
            code_to_category   = code_to_category,
        )

        with job_lock:
            job_status[job_id]["status"]     = "parts_review"
            job_status[job_id]["message"]    = "Parts extracted — awaiting review"
            job_status[job_id]["parts_data"] = result.get("parts_data", [])
            job_status[job_id]["stage"]      = "parts"

    except Exception as e:
        with job_lock:
            job_status[job_id]["status"]  = "error"
            job_status[job_id]["message"] = str(e)


# ─────────────────────────────────────────────────────────────────────────────
# Routes — UI
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("epc_index.html")


@app.route("/history")
def history():
    return render_template("epc_history.html")


# ─────────────────────────────────────────────────────────────────────────────
# Routes — API
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/master-categories")
def api_master_categories():
    cats = []
    for key, info in MASTER_CATEGORIES.items():
        if info["id"]:
            cats.append({
                "id":           info["id"],
                "name_en":      info["name_en"],
                "name_cn":      info["name_cn"],
                "partbook_type": info["partbook_type"],
                "display":      f"{info['name_en']} / {info['name_cn']}",
            })
    return jsonify({"categories": cats})


@app.route("/api/upload", methods=["POST"])
def api_upload():
    """Upload partbook PDF and start Stage 1 extraction."""
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    f = request.files["file"]
    if not f or not allowed_file(f.filename):
        return jsonify({"error": "Invalid file type. Only PDF files are accepted."}), 400

    master_category_id = request.form.get("master_category_id", "")
    if not master_category_id:
        return jsonify({"error": "master_category_id is required"}), 400

    cat_info       = _get_master_category_info(master_category_id)
    partbook_type  = cat_info.get("partbook_type", "cabin_chassis")

    filename   = secure_filename(f.filename)
    job_id     = str(uuid.uuid4())
    pdf_path   = Path(app.config["UPLOAD_FOLDER"]) / f"{job_id}_{filename}"
    f.save(str(pdf_path))

    config_params = {
        "sumopod_base_url":      os.getenv("SUMOPOD_BASE_URL", "https://ai.sumopod.com/v1"),
        "sumopod_api_key":       os.getenv("SUMOPOD_API_KEY", ""),
        "sumopod_model":         request.form.get("model", os.getenv("SUMOPOD_MODEL", "gpt4o")),
        "sso_email":             os.getenv("SSO_EMAIL", ""),
        "sso_password":          os.getenv("SSO_PASSWORD", ""),
        "sso_gateway_url":       os.getenv("SSO_GATEWAY_URL", "https://dev-gateway.motorsights.com"),
        "epc_base_url":          os.getenv("EPC_API_BASE_URL", "https://dev-gateway.motorsights.com/api/epc"),
        "master_category_id":    master_category_id,
        "master_category_name_en": cat_info.get("name_en", ""),
        "partbook_type":         partbook_type,
        "enable_review_mode":    True,
    }

    with job_lock:
        job_status[job_id] = {
            "status":           "queued",
            "message":          "Job queued",
            "filename":         filename,
            "pdf_path":         str(pdf_path),
            "master_category_id": master_category_id,
            "partbook_type":    partbook_type,
            "config_params":    config_params,
            "stage":            "structure",
            "created_at":       datetime.now().isoformat(),
        }

    threading.Thread(
        target=_run_stage1,
        args=(job_id, str(pdf_path), config_params),
        daemon=True
    ).start()

    return jsonify({"job_id": job_id})


@app.route("/api/status/<job_id>")
def api_status(job_id: str):
    with job_lock:
        job = job_status.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    return jsonify({
        "status":         job.get("status"),
        "message":        job.get("message"),
        "stage":          job.get("stage"),
        "partbook_type":  job.get("partbook_type"),
        "extracted_data": job.get("extracted_data"),
        "parts_data":     job.get("parts_data"),
        "submission_result": job.get("submission_result"),
    })


@app.route("/api/approve-structure/<job_id>", methods=["POST"])
def api_approve_structure(job_id: str):
    """
    User approves the category structure.  Submit Stage 1 to EPC.
    """
    with job_lock:
        job = job_status.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404

    body = request.get_json(force=True) or {}
    edited_data = body.get("extracted_data") or job.get("extracted_data", {})

    config_params = job["config_params"]
    config = EPCAutomationConfig(**config_params)
    automation = EPCPDFAutomation(config)

    success, epc_results = automation.submit_to_epc(
        extracted_data=edited_data,
        master_category_id=job["master_category_id"],
        master_category_name_en=config_params.get("master_category_name_en", ""),
    )

    with job_lock:
        job_status[job_id]["submission_result"] = epc_results
        job_status[job_id]["status"] = "structure_submitted" if success else "error"
        job_status[job_id]["message"] = (
            "Structure submitted — ready for Parts Management"
            if success else
            f"Submission errors: {len(epc_results.get('errors', []))}"
        )

    return jsonify({"success": success, "epc_results": epc_results})


@app.route("/api/start-parts/<job_id>", methods=["POST"])
def api_start_parts(job_id: str):
    
    with job_lock:
        job = job_status.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404

    body = request.get_json(force=True) or {}
    target_id_start = int(body.get("target_id_start", 1))
    dokumen_name    = body.get("dokumen_name", Path(job["pdf_path"]).stem)

    threading.Thread(
        target=_run_stage2,
        args=(
            job_id,
            job["pdf_path"],
            job["config_params"],
            job["master_category_id"],
            dokumen_name,
            target_id_start,
        ),
        daemon=True
    ).start()

    return jsonify({"status": "parts_extraction_started"})


@app.route("/api/approve-parts/<job_id>", methods=["POST"])
def api_approve_parts(job_id: str):
    """
    User approves the extracted parts data.  Submit Stage 2 to EPC.
    """
    try:
        with job_lock:
            job = job_status.get(job_id)
            
        if not job:
            return jsonify({"error": "Job not found"}), 404

        body       = request.get_json(force=True) or {}
        parts_data = (
            body.get("parts_data")
            or body.get("data")          # legacy key from old frontend
            or job.get("parts_data", [])
        )
    
        if isinstance(parts_data, dict):
            parts_data = parts_data.get("subtypes") or parts_data.get("parts_data") or []
    
        dokumen_name  = body.get("dokumen_name", Path(job["pdf_path"]).stem)
        config_params = job["config_params"]
        config = EPCAutomationConfig(**config_params)
        automation = EPCPDFAutomation(config)

        success, epc_results = automation.epc_client.batch_submit_parts(
            parts_data         = parts_data,
            master_category_id = job["master_category_id"],
            dokumen_name       = dokumen_name,
        )

        with job_lock:
            job_status[job_id]["parts_submission_result"] = epc_results
            job_status[job_id]["status"]  = "completed" if success else "parts_error"
            job_status[job_id]["message"] = (
                f"✓ Parts submitted — "
                f"{len(epc_results.get('created', []))} created, "
                f"{len(epc_results.get('updated', []))} updated"
                if success else
                f"Parts submission errors: {len(epc_results.get('errors', []))}"
            )

        return jsonify({"success": success, "epc_results": epc_results})

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/re-extract/<job_id>", methods=["POST"])
def api_re_extract(job_id: str):
    """Restart Stage 1 with a modified extraction prompt."""
    try:
        with job_lock:
            job = job_status.get(job_id)
            
        if not job:
            return jsonify({"error": "Job not found"}), 404

        body       = request.get_json(force=True) or {}
        new_prompt = body.get("prompt", "")

        config_params = dict(job["config_params"])
        config_params["sumopod_custom_prompt"] = new_prompt

        with job_lock:
            job_status[job_id]["status"]         = "queued"
            job_status[job_id]["message"]        = "Re-extraction queued"
            job_status[job_id]["extracted_data"] = None
            job_status[job_id]["config_params"]  = config_params

        threading.Thread(
            target=_run_stage1,
            args=(job_id, job["pdf_path"], config_params),
            daemon=True
        ).start()

        return jsonify({"status": "re_extraction_started"})
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/jobs")
def api_jobs():
    with job_lock:
        jobs = [
            {
                "job_id":       jid,
                "status":       j.get("status"),
                "filename":     j.get("filename"),
                "partbook_type": j.get("partbook_type"),
                "created_at":   j.get("created_at"),
                "message":      j.get("message"),
            }
            for jid, j in job_status.items()
        ]
    return jsonify({"jobs": sorted(jobs, key=lambda x: x["created_at"], reverse=True)})


@app.route("/api/clear-history", methods=["POST"])
def api_clear_history():
    with job_lock:
        job_status.clear()
    return jsonify({"success": True})


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)