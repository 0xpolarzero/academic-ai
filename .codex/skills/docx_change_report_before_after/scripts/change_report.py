#!/usr/bin/env python3
"""Generate before/after change report artifacts from DOCX patch/apply artifacts."""

from __future__ import annotations

import argparse
from pathlib import Path

from _report_lib import (
    DEFAULT_APPLY_LOG_PATH,
    DEFAULT_OUTPUT_JSON_PATH,
    DEFAULT_OUTPUT_MD_PATH,
    DEFAULT_PATCH_PATH,
    DEFAULT_REVIEW_UNITS_PATH,
    build_change_report_artifacts,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--review-units",
        type=Path,
        default=DEFAULT_REVIEW_UNITS_PATH,
        help="Path to review_units.json",
    )
    parser.add_argument(
        "--patch",
        type=Path,
        default=DEFAULT_PATCH_PATH,
        help="Path to merged_patch.json",
    )
    parser.add_argument(
        "--apply-log",
        type=Path,
        default=DEFAULT_APPLY_LOG_PATH,
        help="Path to apply_log.json",
    )
    parser.add_argument(
        "--output-md",
        type=Path,
        default=DEFAULT_OUTPUT_MD_PATH,
        help="Output markdown report path",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=DEFAULT_OUTPUT_JSON_PATH,
        help="Output JSON report path",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    if not args.review_units.exists():
        parser.error(f"review_units.json not found: {args.review_units}")
    if not args.patch.exists():
        parser.error(f"merged_patch.json not found: {args.patch}")
    if not args.apply_log.exists():
        parser.error(f"apply_log.json not found: {args.apply_log}")

    result = build_change_report_artifacts(
        review_units_path=args.review_units,
        patch_path=args.patch,
        apply_log_path=args.apply_log,
        output_md_path=args.output_md,
        output_json_path=args.output_json,
    )

    stats = result["stats"]
    print(f"Review units: {args.review_units}")
    print(f"Patch: {args.patch}")
    print(f"Apply log: {args.apply_log}")
    print(f"Wrote: {result['output_md']}")
    print(f"Wrote: {result['output_json']}")
    print(
        "Ops: "
        f"total={stats['op_count']} "
        f"applied={stats['applied']} "
        f"skipped={stats['skipped']} "
        f"unknown={stats['unknown']}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
