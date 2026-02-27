#!/usr/bin/env python3
"""Build chunk manifest and chunk files from extracted DOCX review units."""

from __future__ import annotations

import argparse
from pathlib import Path

from _chunk_lib import chunk_docx_to_artifacts, load_constants, resolve_chunk_paths

DEFAULT_CONSTANTS_PATH = Path("config/constants.json")
FALLBACK_REPO_CONSTANTS_PATH = Path(__file__).resolve().parents[4] / "config/constants.json"
DEFAULT_REVIEW_UNITS = Path("artifacts/docx_extract/review_units.json")
DEFAULT_LINEAR_UNITS = Path("artifacts/docx_extract/linear_units.json")
DEFAULT_DOCX_STRUCT = Path("artifacts/docx_extract/docx_struct.json")
DEFAULT_OUTPUT_DIR = Path("artifacts/chunks")


def _resolve_project_path(project_dir: Path, raw_path: Path | None) -> Path | None:
    if raw_path is None:
        return None
    expanded = raw_path.expanduser()
    return expanded if expanded.is_absolute() else (project_dir / expanded)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-dir", type=Path, required=True, help="Project directory root")
    parser.add_argument(
        "--constants",
        type=Path,
        default=DEFAULT_CONSTANTS_PATH,
        help="Path to constants JSON with chunking budget/path defaults (project-relative unless absolute)",
    )
    parser.add_argument("--review-units", type=Path, default=None, help="Path to review_units.json")
    parser.add_argument("--linear-units", type=Path, default=None, help="Path to linear_units.json")
    parser.add_argument("--docx-struct", type=Path, default=None, help="Optional path to docx_struct.json")
    parser.add_argument("--output-dir", type=Path, default=None, help="Output folder for chunk artifacts")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    project_dir = args.project_dir.expanduser().resolve()
    if not project_dir.exists() or not project_dir.is_dir():
        parser.error(f"--project-dir must be an existing directory: {project_dir}")

    constants_path = _resolve_project_path(project_dir, args.constants)
    assert constants_path is not None
    if not constants_path.exists():
        constants_path = FALLBACK_REPO_CONSTANTS_PATH
    constants = load_constants(constants_path)
    default_paths = resolve_chunk_paths(constants)

    review_units_raw = args.review_units or Path(str(default_paths.get("review_units", DEFAULT_REVIEW_UNITS)))
    linear_units_raw = args.linear_units or Path(str(default_paths.get("linear_units", DEFAULT_LINEAR_UNITS)))
    output_dir_raw = args.output_dir or Path(str(default_paths.get("output_dir", DEFAULT_OUTPUT_DIR)))

    review_units_path = _resolve_project_path(project_dir, review_units_raw)
    linear_units_path = _resolve_project_path(project_dir, linear_units_raw)
    output_dir = _resolve_project_path(project_dir, output_dir_raw)
    assert review_units_path is not None
    assert linear_units_path is not None
    assert output_dir is not None

    configured_docx_struct = Path(str(default_paths.get("docx_struct", DEFAULT_DOCX_STRUCT)))
    docx_struct_path = args.docx_struct if args.docx_struct is not None else configured_docx_struct
    docx_struct_path = _resolve_project_path(project_dir, docx_struct_path)
    if docx_struct_path is not None and not docx_struct_path.exists():
        docx_struct_path = None

    if not review_units_path.exists():
        parser.error(f"review_units.json not found: {review_units_path}")
    if not linear_units_path.exists():
        parser.error(f"linear_units.json not found: {linear_units_path}")

    result = chunk_docx_to_artifacts(
        review_units_path=review_units_path,
        linear_units_path=linear_units_path,
        output_dir=output_dir,
        constants_path=constants_path,
        docx_struct_path=docx_struct_path,
    )

    print(f"Project dir: {project_dir}")
    print(f"Review units: {review_units_path}")
    print(f"Linear units: {linear_units_path}")
    if docx_struct_path is not None:
        print(f"Docx struct: {docx_struct_path}")
    print(f"Wrote: {result['manifest_path']}")
    print(f"Chunk files: {result['chunk_count']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
