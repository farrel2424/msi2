"""
cabin_chassis_parts_runner.py  —  FIXED
=========================================
Fix applied:
  FIX 4 (Bug 4): Print summary now uses "created" and "skipped" keys
                 (matching batch_submit_parts output). Removed phantom
                 "updated" key — no update path exists in the submit flow.

Usage examples:

  # Dry run — extract and preview without submitting
  python cabin_chassis_parts_runner.py \\
      --pdf partbook.pdf \\
      --category "Frame System" \\
      --dry-run

  # Full run — extract and submit to EPC
  python cabin_chassis_parts_runner.py \\
      --pdf partbook.pdf \\
      --category "Frame System"

  # Save extracted JSON for review, then submit from it later
  python cabin_chassis_parts_runner.py \\
      --pdf partbook.pdf \\
      --category "Frame System" \\
      --save-json extracted_parts.json \\
      --dry-run

  python cabin_chassis_parts_runner.py \\
      --from-json extracted_parts.json \\
      --category "Frame System"

Environment variables required (or via .env):
  MASTER_CATEGORY_CABIN_CHASSIS_ID  — UUID of the Cabin & Chassis master category
  SSO_EMAIL                         — Motorsights SSO email
  SSO_PASSWORD                      — Motorsights SSO password
  SUMOPOD_API_KEY                   — Sumopod AI API key
  SUMOPOD_MODEL                     — AI model (default: gpt-4o)
  CABIN_CHASSIS_DOKUMEN_NAME        — Document name (default: "Cabin & Chassis Manual")
"""

import argparse
import json
import os
import sys
from pathlib import Path

# Load .env if python-dotenv is available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from cabin_chassis_parts_submitter import CabinChassisPartsSubmitter, PartsSubmitterConfig


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cabin & Chassis Parts Management Automation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Input source (mutually exclusive)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--pdf", metavar="PATH", help="Path to the partbook PDF")
    source.add_argument(
        "--from-json",
        metavar="PATH",
        help="Path to previously extracted JSON (skip AI extraction)",
    )

    # Required
    parser.add_argument(
        "--category",
        required=True,
        metavar="NAME",
        help='Category English name, e.g. "Frame System"',
    )

    # Optional overrides
    parser.add_argument(
        "--dokumen",
        metavar="NAME",
        default=None,
        help="Document name (overrides CABIN_CHASSIS_DOKUMEN_NAME env var)",
    )
    parser.add_argument(
        "--master-category-id",
        metavar="UUID",
        default=None,
        help="Master category UUID (overrides MASTER_CATEGORY_CABIN_CHASSIS_ID env var)",
    )
    parser.add_argument(
        "--unit",
        default="pcs",
        help="Default unit for all parts (default: pcs)",
    )
    parser.add_argument(
        "--save-json",
        metavar="PATH",
        default=None,
        help="Save extracted parts JSON to this path before submitting",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Extract and preview only — do not submit to API",
    )
    parser.add_argument(
        "--model",
        default=None,
        help="AI model override (e.g. gpt-4o, claude-3-5-sonnet)",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    # Build config
    try:
        config = PartsSubmitterConfig(
            master_category_id=args.master_category_id,
            dokumen_name=args.dokumen,
            sumopod_model=args.model,
            default_unit=args.unit,
            dry_run=args.dry_run,
        )
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    submitter = CabinChassisPartsSubmitter(config)

    # Run
    if args.pdf:
        result = submitter.run(
            pdf_path=args.pdf,
            category_name_en=args.category,
            dokumen_name=args.dokumen,
            save_extracted_json=args.save_json,
        )
    else:
        result = submitter.run_from_extracted_json(
            json_path=args.from_json,
            category_name_en=args.category,
            dokumen_name=args.dokumen,
        )

    # Print final summary
    print("\n" + "=" * 60)
    print(f"Stage  : {result.get('stage', 'unknown')}")
    print(f"Success: {result.get('success', False)}")

    if result.get("error"):
        print(f"Error  : {result['error']}")

    sub = result.get("submission_results")
    if sub:
        # ✅ FIX 4: use "created" and "skipped" — the actual keys returned by
        #    batch_submit_parts. Removed phantom "updated" key (no update path).
        print(f"Created : {len(sub.get('created', []))} subtype(s)")
        print(f"Skipped : {len(sub.get('skipped', []))} subtype(s)")
        print(f"Errors  : {len(sub.get('errors', []))}")
        if sub.get("errors"):
            print("\nFailed subtypes:")
            for err in sub["errors"]:
                print(f"  - {err.get('subtype_name_en', '?')}: {err.get('error', '?')}")

    return 0 if result.get("success") else 1


if __name__ == "__main__":
    sys.exit(main())