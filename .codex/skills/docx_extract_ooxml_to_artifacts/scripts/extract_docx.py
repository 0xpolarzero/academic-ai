#!/usr/bin/env python3
"""Extract paragraph review artifacts from DOCX OOXML parts."""

from __future__ import annotations

import argparse
from pathlib import Path

from _extract_lib import extract_docx_to_artifacts


DEFAULT_INPUT_DOCX = Path("input/source.docx")
DEFAULT_OUTPUT_DIR = Path("artifacts/docx_extract")


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
        help="Path to source .docx file (project-relative unless absolute)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where extraction artifacts are written (project-relative unless absolute)",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    project_dir = args.project_dir.expanduser().resolve()
    if not project_dir.exists() or not project_dir.is_dir():
        parser.error(f"--project-dir must be an existing directory: {project_dir}")

    input_docx = _resolve_project_path(project_dir, args.input_docx)
    output_dir = _resolve_project_path(project_dir, args.output_dir)

    if not input_docx.exists():
        parser.error(f"Input DOCX not found: {input_docx}")
    if input_docx.suffix.lower() != ".docx":
        parser.error(f"Input must be a .docx file: {input_docx}")

    written = extract_docx_to_artifacts(input_docx=input_docx, output_dir=output_dir)

    print(f"Project dir: {project_dir}")
    print(f"Source: {input_docx}")
    for file_name in ("review_units.json", "docx_struct.json", "linear_units.json"):
        print(f"Wrote: {written[file_name]}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
