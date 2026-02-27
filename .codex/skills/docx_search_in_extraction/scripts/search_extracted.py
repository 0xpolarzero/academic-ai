#!/usr/bin/env python3
"""Search extracted review units and write search_results.json."""

from __future__ import annotations

import argparse
from pathlib import Path

from _search_lib import (
    DEFAULT_SNIPPET_CHARS,
    search_extracted_to_artifacts,
)

DEFAULT_REVIEW_UNITS_PATH = Path("artifacts/docx_extract/review_units.json")
DEFAULT_OUTPUT_DIR = Path("artifacts/search")


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
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where search_results.json is written",
    )
    parser.add_argument("--query", required=True, help="Search query (literal by default)")
    parser.add_argument("--regex", action="store_true", help="Interpret --query as a regex pattern")
    parser.add_argument("--ignore-case", action="store_true", help="Use case-insensitive matching")
    parser.add_argument(
        "--snippet-chars",
        type=int,
        default=DEFAULT_SNIPPET_CHARS,
        help=f"Snippet window size around each hit (default: {DEFAULT_SNIPPET_CHARS})",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    project_dir = args.project_dir.expanduser().resolve()
    if not project_dir.exists() or not project_dir.is_dir():
        parser.error(f"--project-dir must be an existing directory: {project_dir}")

    review_units_path = _resolve_project_path(project_dir, args.review_units)
    output_dir = _resolve_project_path(project_dir, args.output_dir)

    if not review_units_path.exists():
        parser.error(f"review_units.json not found: {review_units_path}")
    if args.snippet_chars < 0:
        parser.error("--snippet-chars must be >= 0")

    try:
        result = search_extracted_to_artifacts(
            review_units_path=review_units_path,
            output_dir=output_dir,
            query=args.query,
            regex_mode=args.regex,
            case_sensitive=not args.ignore_case,
            snippet_chars=args.snippet_chars,
        )
    except ValueError as exc:
        parser.error(str(exc))

    mode = "regex" if args.regex else "literal"
    print(f"Project dir: {project_dir}")
    print(f"Review units: {review_units_path}")
    print(f"Query: {args.query} ({mode}, case_sensitive={not args.ignore_case})")
    print(f"Wrote: {result['output_path']}")
    print(f"Hits: {result['hit_count']} across {result['hit_unit_count']} units")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
