#!/usr/bin/env python3
"""Search extracted review units and write search_results.json."""

from __future__ import annotations

import argparse
from pathlib import Path

from _search_lib import (
    DEFAULT_OUTPUT_DIR,
    DEFAULT_REVIEW_UNITS_PATH,
    DEFAULT_SNIPPET_CHARS,
    search_extracted_to_artifacts,
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

    if not args.review_units.exists():
        parser.error(f"review_units.json not found: {args.review_units}")
    if args.snippet_chars < 0:
        parser.error("--snippet-chars must be >= 0")

    try:
        result = search_extracted_to_artifacts(
            review_units_path=args.review_units,
            output_dir=args.output_dir,
            query=args.query,
            regex_mode=args.regex,
            case_sensitive=not args.ignore_case,
            snippet_chars=args.snippet_chars,
        )
    except ValueError as exc:
        parser.error(str(exc))

    mode = "regex" if args.regex else "literal"
    print(f"Review units: {args.review_units}")
    print(f"Query: {args.query} ({mode}, case_sensitive={not args.ignore_case})")
    print(f"Wrote: {result['output_path']}")
    print(f"Hits: {result['hit_count']} across {result['hit_unit_count']} units")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
