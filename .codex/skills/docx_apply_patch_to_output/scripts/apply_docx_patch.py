#!/usr/bin/env python3
"""Apply merged patch operations to a DOCX using tracked changes and comments."""

from __future__ import annotations

import argparse
from pathlib import Path

from _apply_lib import (
    DEFAULT_APPLY_LOG_PATH,
    DEFAULT_AUTHOR,
    DEFAULT_INPUT_DOCX,
    DEFAULT_OUTPUT_DOCX,
    DEFAULT_PATCH_PATH,
    DEFAULT_REVIEW_UNITS_PATH,
    apply_patch_to_output,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
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

    if not args.input_docx.exists():
        parser.error(f"Input DOCX not found: {args.input_docx}")
    if args.input_docx.suffix.lower() != ".docx":
        parser.error(f"Input must be a .docx file: {args.input_docx}")
    if not args.patch.exists():
        parser.error(f"Patch file not found: {args.patch}")
    if not args.review_units.exists():
        parser.error(f"review_units.json not found: {args.review_units}")

    try:
        result = apply_patch_to_output(
            input_docx=args.input_docx,
            patch_path=args.patch,
            review_units_path=args.review_units,
            output_docx=args.output_docx,
            apply_log_path=args.apply_log,
            author=str(args.author),
        )
    except ValueError as exc:
        parser.error(str(exc))

    stats = result["stats"]
    print(f"Source DOCX: {args.input_docx}")
    print(f"Patch: {args.patch}")
    print(f"Review units: {args.review_units}")
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
