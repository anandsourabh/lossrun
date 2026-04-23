"""
CLI entry point for the WF Loss Run Persistence Service.

Usage examples:

  # Persist a single extraction JSON file
  python -m wf_lossrun_persistence.cli persist extraction.json \
      --dsn "postgresql://user:pass@localhost/mydb"

  # Persist all JSON files in a directory
  python -m wf_lossrun_persistence.cli persist ./extractions/ \
      --dsn "postgresql://user:pass@localhost/mydb"

  # Create required unique indexes (run once after schema migration)
  python -m wf_lossrun_persistence.cli setup-constraints \
      --dsn "postgresql://user:pass@localhost/mydb"

  # DSN can also be set via environment variable
  export WF_LOSSRUN_DSN="postgresql://user:pass@localhost/mydb"
  python -m wf_lossrun_persistence.cli persist extraction.json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

from .models import WFPropertyLossRunExtraction
from .service import LossRunPersistenceService
from .exceptions import DuplicateReportError, PersistenceError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("wf_lossrun_persistence.cli")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="wf_lossrun_persistence",
        description="Persist WF Property Loss Run extraction JSON to PostgreSQL.",
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("WF_LOSSRUN_DSN"),
        help="PostgreSQL DSN. Defaults to $WF_LOSSRUN_DSN env var.",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    # ── persist ──────────────────────────────────────────────────────
    p_persist = sub.add_parser(
        "persist",
        help="Persist one or more extraction JSON files.",
    )
    p_persist.add_argument(
        "path",
        type=Path,
        help="Path to a single JSON file or a directory of JSON files.",
    )
    p_persist.add_argument(
        "--strict",
        action="store_true",
        default=False,
        help="Raise an error on duplicate reports instead of silently skipping.",
    )
    p_persist.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Validate and parse files without writing to the database.",
    )

    # ── setup-constraints ────────────────────────────────────────────
    sub.add_parser(
        "setup-constraints",
        help="Create required unique indexes on natural-key columns (idempotent).",
    )

    return parser


def _collect_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    if path.is_dir():
        files = sorted(path.glob("*.json"))
        if not files:
            logger.warning("No JSON files found in %s", path)
        return files
    raise FileNotFoundError(f"Path not found: {path}")


def _persist_file(
    svc: LossRunPersistenceService,
    json_path: Path,
    *,
    dry_run: bool,
) -> bool:
    """Returns True on success, False on handled error."""
    try:
        raw = json.loads(json_path.read_text(encoding="utf-8"))
        extraction = WFPropertyLossRunExtraction.model_validate(raw)
    except Exception as exc:
        logger.error("Parse error in %s: %s", json_path.name, exc)
        return False

    if dry_run:
        logger.info("[DRY RUN] %s — parsed OK, skipping DB write.", json_path.name)
        return True

    try:
        result = svc.persist(extraction, source_file=json_path.name)
        if result.skipped_duplicate_report:
            logger.info(
                "%s — duplicate report skipped (report_id=%s).",
                json_path.name, result.report_id,
            )
        else:
            logger.info(
                "%s — OK  report_id=%s  claims_new=%d  claims_updated=%d",
                json_path.name,
                result.report_id,
                len(result.claim_ids),
                len(result.updated_claim_ids),
            )
        return True

    except DuplicateReportError as exc:
        logger.error("%s — duplicate report: %s", json_path.name, exc)
        return False
    except PersistenceError as exc:
        logger.error("%s — persistence error: %s", json_path.name, exc)
        return False


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if not args.dsn and args.command != "persist":
        parser.error(
            "No DSN provided. Use --dsn or set $WF_LOSSRUN_DSN."
        )

    if args.command == "setup-constraints":
        if not args.dsn:
            parser.error("--dsn is required for setup-constraints.")
        svc = LossRunPersistenceService(dsn=args.dsn)
        svc.ensure_constraints()
        logger.info("Done.")
        return 0

    if args.command == "persist":
        if not args.dry_run and not args.dsn:
            parser.error("--dsn is required unless --dry-run is set.")

        svc = LossRunPersistenceService(
            dsn=args.dsn or "postgresql://",   # unused in dry-run
            idempotent=not args.strict,
        )

        files = _collect_files(args.path)
        logger.info("Processing %d file(s)…", len(files))

        successes = sum(
            _persist_file(svc, f, dry_run=args.dry_run)
            for f in files
        )
        failures = len(files) - successes

        logger.info("Finished: %d succeeded, %d failed.", successes, failures)
        return 0 if failures == 0 else 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
