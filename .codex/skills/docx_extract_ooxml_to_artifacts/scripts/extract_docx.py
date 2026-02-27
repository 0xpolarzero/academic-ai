#!/usr/bin/env python3
"""Extract paragraph review artifacts from DOCX OOXML parts."""

from __future__ import annotations

import argparse
from pathlib import Path

from _extract_lib import extract_docx_to_artifacts


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-docx", type=Path, required=True, help="Path to source .docx file")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("artifacts/docx_extract"),
        help="Directory where extraction artifacts are written",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    if not args.input_docx.exists():
        parser.error(f"Input DOCX not found: {args.input_docx}")
    if args.input_docx.suffix.lower() != ".docx":
        parser.error(f"Input must be a .docx file: {args.input_docx}")

    written = extract_docx_to_artifacts(input_docx=args.input_docx, output_dir=args.output_dir)

    print(f"Source: {args.input_docx}")
    for file_name in ("review_units.json", "docx_struct.json", "linear_units.json"):
        print(f"Wrote: {written[file_name]}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
