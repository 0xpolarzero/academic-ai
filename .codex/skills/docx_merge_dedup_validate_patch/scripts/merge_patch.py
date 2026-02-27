#!/usr/bin/env python3
"""Merge per-chunk patch suggestions into merged_patch.json and merge_report.json."""

from __future__ import annotations

import argparse
from pathlib import Path

from _merge_lib import (
    DEFAULT_AUTHOR,
    merge_chunk_results_to_artifacts,
)

DEFAULT_CHUNK_RESULTS_DIR = Path("artifacts/chunk_results")
DEFAULT_LINEAR_UNITS_PATH = Path("artifacts/docx_extract/linear_units.json")
DEFAULT_CHUNKS_MANIFEST_PATH = Path("artifacts/chunks/manifest.json")
DEFAULT_REVIEW_UNITS_PATH = Path("artifacts/docx_extract/review_units.json")
DEFAULT_OUTPUT_DIR = Path("artifacts/patch")


def _resolve_project_path(project_dir: Path, raw_path: Path) -> Path:
    expanded = raw_path.expanduser()
    return expanded if expanded.is_absolute() else (project_dir / expanded)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-dir", type=Path, required=True, help="Project directory root")
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
        "--chunks-manifest",
        type=Path,
        default=DEFAULT_CHUNKS_MANIFEST_PATH,
        help="chunks manifest path for primary-only target ownership enforcement",
    )
    parser.add_argument(
        "--review-units",
        type=Path,
        default=DEFAULT_REVIEW_UNITS_PATH,
        help="Optional review_units.json path for resolving ops with missing ranges",
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

    project_dir = args.project_dir.expanduser().resolve()
    if not project_dir.exists() or not project_dir.is_dir():
        parser.error(f"--project-dir must be an existing directory: {project_dir}")

    chunk_results_dir = _resolve_project_path(project_dir, args.chunk_results_dir)
    linear_units = _resolve_project_path(project_dir, args.linear_units)
    chunks_manifest = _resolve_project_path(project_dir, args.chunks_manifest)
    output_dir = _resolve_project_path(project_dir, args.output_dir)

    if not chunk_results_dir.exists():
        parser.error(f"chunk results directory not found: {chunk_results_dir}")
    if not chunks_manifest.exists():
        parser.error(f"chunks manifest not found: {chunks_manifest}")

    linear_units_path = linear_units if linear_units.exists() else None
    review_units_path = _resolve_project_path(project_dir, args.review_units)
    if not review_units_path.exists():
        review_units_path = None

    result = merge_chunk_results_to_artifacts(
        chunk_results_dir=chunk_results_dir,
        output_dir=output_dir,
        linear_units_path=linear_units_path,
        chunks_manifest_path=chunks_manifest,
        review_units_path=review_units_path,
        author=str(args.author),
    )

    stats = result["stats"]
    print(f"Project dir: {project_dir}")
    print(f"Chunk results dir: {chunk_results_dir}")
    print(f"Chunk files read: {result['chunk_file_count']}")
    print(f"Wrote: {result['merged_patch_path']}")
    print(f"Wrote: {result['merge_report_path']}")
    print(
        "Ops: "
        f"input={stats['input_ops']} "
        f"valid={stats['valid_ops']} "
        f"ownership_rejected={stats['ownership_rejected_ops']} "
        f"autofilled_uid={stats['ownership_autofilled_unit_uid_ops']} "
        f"dedup_removed={stats['duplicates_removed']} "
        f"conflict_downgrades={stats['conflict_downgrades']} "
        f"final={stats['final_ops']}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
