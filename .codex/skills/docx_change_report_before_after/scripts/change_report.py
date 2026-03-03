#!/usr/bin/env python3
"""Generate before/after change report artifacts from DOCX patch/apply artifacts."""

from __future__ import annotations

import argparse
from pathlib import Path

from _report_lib import (
    build_change_report_artifacts,
)

DEFAULT_REVIEW_UNITS_PATH = Path("artifacts/docx_extract/review_units.json")
DEFAULT_PATCH_PATH = Path("artifacts/patch/merged_patch.json")
DEFAULT_APPLY_LOG_PATH = Path("artifacts/apply/apply_log.json")
DEFAULT_OUTPUT_MD_PATH = Path("output/changes.md")
DEFAULT_OUTPUT_JSON_PATH = Path("output/changes.json")


def _resolve_project_path(project_dir: Path, raw_path: Path) -> Path:
    expanded = raw_path.expanduser()
    return expanded if expanded.is_absolute() else (project_dir / expanded)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-dir", type=Path, required=True, help="Project directory root")
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
    parser.add_argument(
        "--output-docx",
        type=Path,
        default=None,
        help="Optional output DOCX report path",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    project_dir = args.project_dir.expanduser().resolve()
    if not project_dir.exists() or not project_dir.is_dir():
        parser.error(f"--project-dir must be an existing directory: {project_dir}")

    review_units_path = _resolve_project_path(project_dir, args.review_units)
    patch_path = _resolve_project_path(project_dir, args.patch)
    apply_log_path = _resolve_project_path(project_dir, args.apply_log)
    output_md_path = _resolve_project_path(project_dir, args.output_md)
    output_json_path = _resolve_project_path(project_dir, args.output_json)
    output_docx_path = _resolve_project_path(project_dir, args.output_docx) if args.output_docx else None

    if not review_units_path.exists():
        parser.error(f"review_units.json not found: {review_units_path}")
    if not patch_path.exists():
        parser.error(f"merged_patch.json not found: {patch_path}")
    if not apply_log_path.exists():
        parser.error(f"apply_log.json not found: {apply_log_path}")

    result = build_change_report_artifacts(
        review_units_path=review_units_path,
        patch_path=patch_path,
        apply_log_path=apply_log_path,
        output_md_path=output_md_path,
        output_json_path=output_json_path,
        output_docx_path=output_docx_path,
    )

    stats = result["stats"]
    print(f"Project dir: {project_dir}")
    print(f"Review units: {review_units_path}")
    print(f"Patch: {patch_path}")
    print(f"Apply log: {apply_log_path}")
    print(f"Wrote: {result['output_md']}")
    print(f"Wrote: {result['output_json']}")
    if "output_docx" in result:
        print(f"Wrote: {result['output_docx']}")
    print(
        "Ops: "
        f"applied={stats.get('applied_op_count', stats.get('op_count', 'n/a'))} "
        f"failed={stats.get('failed_op_count', 0)} "
        f"patch_total={stats.get('input_patch_ops', 'n/a')}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
