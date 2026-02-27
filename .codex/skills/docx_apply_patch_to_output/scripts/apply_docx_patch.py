#!/usr/bin/env python3
"""Apply merged patch operations to a DOCX using tracked changes and comments."""

from __future__ import annotations

import argparse
from pathlib import Path

from _apply_lib import (
    DEFAULT_AUTHOR,
    apply_patch_to_output,
)

DEFAULT_INPUT_DOCX = Path("input/source.docx")
DEFAULT_PATCH_PATH = Path("artifacts/patch/merged_patch.json")
DEFAULT_REVIEW_UNITS_PATH = Path("artifacts/docx_extract/review_units.json")
DEFAULT_OUTPUT_DOCX = Path("output/annotated.docx")
DEFAULT_APPLY_LOG_PATH = Path("artifacts/apply/apply_log.json")


def _resolve_project_path(project_dir: Path, raw_path: Path) -> Path:
    expanded = raw_path.expanduser()
    return expanded if expanded.is_absolute() else (project_dir / expanded)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-dir", type=Path, required=True, help="Project directory root")
    parser.add_argument(
        "--input-docx",
        type=Path,
        default=DEFAULT_INPUT_DOCX,
        help="Source .docx file",
    )
    parser.add_argument(
        "--patch",
        type=Path,
        default=DEFAULT_PATCH_PATH,
        help="Path to merged_patch.json",
    )
    parser.add_argument(
        "--review-units",
        type=Path,
        default=DEFAULT_REVIEW_UNITS_PATH,
        help="Path to review_units.json",
    )
    parser.add_argument(
        "--output-docx",
        type=Path,
        default=DEFAULT_OUTPUT_DOCX,
        help="Output DOCX path",
    )
    parser.add_argument(
        "--apply-log",
        type=Path,
        default=DEFAULT_APPLY_LOG_PATH,
        help="Output apply_log.json path",
    )
    parser.add_argument(
        "--author",
        default=DEFAULT_AUTHOR,
        help=f'Author on generated revisions/comments (default: "{DEFAULT_AUTHOR}")',
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    project_dir = args.project_dir.expanduser().resolve()
    if not project_dir.exists() or not project_dir.is_dir():
        parser.error(f"--project-dir must be an existing directory: {project_dir}")

    input_docx = _resolve_project_path(project_dir, args.input_docx)
    patch_path = _resolve_project_path(project_dir, args.patch)
    review_units_path = _resolve_project_path(project_dir, args.review_units)
    output_docx = _resolve_project_path(project_dir, args.output_docx)
    apply_log = _resolve_project_path(project_dir, args.apply_log)

    if not input_docx.exists():
        parser.error(f"Input DOCX not found: {input_docx}")
    if input_docx.suffix.lower() != ".docx":
        parser.error(f"Input must be a .docx file: {input_docx}")
    if not patch_path.exists():
        parser.error(f"Patch file not found: {patch_path}")
    if not review_units_path.exists():
        parser.error(f"review_units.json not found: {review_units_path}")

    try:
        result = apply_patch_to_output(
            input_docx=input_docx,
            patch_path=patch_path,
            review_units_path=review_units_path,
            output_docx=output_docx,
            apply_log_path=apply_log,
            author=str(args.author),
        )
    except ValueError as exc:
        parser.error(str(exc))

    stats = result["stats"]
    print(f"Project dir: {project_dir}")
    print(f"Source DOCX: {input_docx}")
    print(f"Patch: {patch_path}")
    print(f"Review units: {review_units_path}")
    print(f"Wrote: {result['output_docx']}")
    print(f"Wrote: {result['apply_log']}")
    print(
        "Ops: "
        f"input={stats['input_ops']} "
        f"applied={stats['applied_ops']} "
        f"skipped={stats['skipped_ops']} "
        f"edits={stats['applied_edit_ops']} "
        f"comments={stats['applied_comment_ops']}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
