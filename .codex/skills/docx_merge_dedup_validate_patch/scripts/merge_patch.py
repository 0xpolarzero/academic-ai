#!/usr/bin/env python3
"""Merge per-chunk patch suggestions into merged_patch.json and merge_report.json."""

from __future__ import annotations

import argparse
from pathlib import Path

from _merge_lib import (
    DEFAULT_AUTHOR,
    DEFAULT_CHUNK_RESULTS_DIR,
    DEFAULT_LINEAR_UNITS_PATH,
    DEFAULT_OUTPUT_DIR,
    merge_chunk_results_to_artifacts,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--chunk-results-dir",
        type=Path,
        default=DEFAULT_CHUNK_RESULTS_DIR,
        help="Directory containing chunk_XXXX_result.json files",
    )
    parser.add_argument(
        "--linear-units",
        type=Path,
        default=DEFAULT_LINEAR_UNITS_PATH,
        help="Optional linear_units.json path for document-order sorting",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Output directory for merged_patch.json and merge_report.json",
    )
    parser.add_argument(
        "--author",
        default=DEFAULT_AUTHOR,
        help=f'Author string to embed in merged_patch.json (default: "{DEFAULT_AUTHOR}")',
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    if not args.chunk_results_dir.exists():
        parser.error(f"chunk results directory not found: {args.chunk_results_dir}")

    linear_units_path = args.linear_units if args.linear_units.exists() else None

    result = merge_chunk_results_to_artifacts(
        chunk_results_dir=args.chunk_results_dir,
        output_dir=args.output_dir,
        linear_units_path=linear_units_path,
        author=str(args.author),
    )

    stats = result["stats"]
    print(f"Chunk results dir: {args.chunk_results_dir}")
    print(f"Chunk files read: {result['chunk_file_count']}")
    print(f"Wrote: {result['merged_patch_path']}")
    print(f"Wrote: {result['merge_report_path']}")
    print(
        "Ops: "
        f"input={stats['input_ops']} "
        f"valid={stats['valid_ops']} "
        f"dedup_removed={stats['duplicates_removed']} "
        f"conflict_downgrades={stats['conflict_downgrades']} "
        f"final={stats['final_ops']}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

