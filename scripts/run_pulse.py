"""
run_pulse.py - Thin wrapper around PulseExtractionService.

Extracts a PDF via runpulse.com and prints the structured JSON that matches
wf_property_lossrun_extraction_schema.json.

Layout assumption:
    loss_run/
      scripts/run_pulse.py                            <-- this file
      wf_property_lossrun_extraction_schema.json      <-- schema lives here
      wf_lossrun_persistence/                         <-- package

The schema path is resolved by walking up from this script's directory until
the schema file is found, so the script works regardless of where it is run
from.

Usage:
    export PULSE_API_KEY='your-key-here'
    python scripts/run_pulse.py path/to/loss_run.pdf
    python scripts/run_pulse.py path/to/loss_run.pdf --out result.json
    python scripts/run_pulse.py path/to/loss_run.pdf --schema /custom/schema.json -v
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

# Make the wf_lossrun_persistence package importable when running this script
# directly (i.e., without `pip install -e`).
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
PACKAGE_PARENT = REPO_ROOT / "wf_lossrun_persistence"
if PACKAGE_PARENT.is_dir():
    sys.path.insert(0, str(PACKAGE_PARENT))

from wf_lossrun_persistence import PulseExtractionService  # noqa: E402
from wf_lossrun_persistence.models import WFPropertyLossRunExtraction  # noqa: E402
from pydantic import ValidationError as PydanticValidationError  # noqa: E402
import json  # noqa: E402

SCHEMA_FILENAME = "wf_property_lossrun_extraction_schema.json"


def locate_schema(explicit: str | None) -> Path:
    if explicit:
        path = Path(explicit).expanduser().resolve()
        if not path.is_file():
            sys.exit(f"ERROR: --schema path does not exist: {path}")
        return path

    for parent in [SCRIPT_DIR, *SCRIPT_DIR.parents]:
        candidate = parent / SCHEMA_FILENAME
        if candidate.is_file():
            return candidate

    sys.exit(
        f"ERROR: could not locate {SCHEMA_FILENAME} by walking up from "
        f"{SCRIPT_DIR}. Pass --schema explicitly."
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Run a PDF through the Pulse extract+schema pipeline.",
    )
    p.add_argument("pdf", help="Path to the PDF to extract")
    p.add_argument(
        "--schema",
        help=f"Path to the JSON Schema (default: auto-locate {SCHEMA_FILENAME})",
    )
    p.add_argument(
        "--out",
        help="Write output JSON to this file (default: stdout)",
    )
    p.add_argument(
        "--sync",
        action="store_true",
        help="Use synchronous Pulse calls instead of async polling",
    )
    p.add_argument(
        "--effort",
        action="store_true",
        help="Enable Pulse extended-reasoning mode (4 credits/page)",
    )
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    api_key = os.environ.get("PULSE_API_KEY")
    if not api_key:
        sys.exit("ERROR: set PULSE_API_KEY in the environment before running.")

    pdf_path = Path(args.pdf).expanduser().resolve()
    if not pdf_path.is_file():
        sys.exit(f"ERROR: PDF not found: {pdf_path}")

    schema_path = locate_schema(args.schema)
    logging.info("Using schema: %s", schema_path)

    svc = PulseExtractionService(
        api_key=api_key,
        schema_path=schema_path,
        effort=args.effort,
        use_async=not args.sync,
    )

    # Fetch raw Pulse output first so we never throw away the JSON on a
    # downstream validation failure.
    extraction_id, schema_id, raw_values, page_count, _citations = svc._fetch(pdf_path)

    raw_payload = json.dumps(raw_values, indent=2, default=str)

    try:
        validated = WFPropertyLossRunExtraction.model_validate(raw_values)
        payload = validated.model_dump_json(indent=2, by_alias=True, exclude_none=True)
        validation_ok = True
    except PydanticValidationError as exc:
        payload = raw_payload
        validation_ok = False
        print(
            "WARNING: Pulse output did NOT match the pydantic schema. "
            "Writing raw JSON anyway.\n"
            f"{exc}",
            file=sys.stderr,
        )

    if args.out:
        out_path = Path(args.out).expanduser().resolve()
        out_path.write_text(payload, encoding="utf-8")
        print(f"Wrote {out_path}", file=sys.stderr)
    else:
        print(payload)

    print(
        f"\nextraction_id={extraction_id}  schema_id={schema_id}  "
        f"pages={page_count}  validated={validation_ok}",
        file=sys.stderr,
    )
    return 0 if validation_ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
