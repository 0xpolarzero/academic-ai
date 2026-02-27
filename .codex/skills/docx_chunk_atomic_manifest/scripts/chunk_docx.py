#!/usr/bin/env python3
"""Build chunk manifest and chunk files from extracted DOCX review units."""

from __future__ import annotations

import argparse
from pathlib import Path

from _chunk_lib import chunk_docx_to_artifacts, load_constants, resolve_chunk_paths


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--constants",
        type=Path,
        default=Path("config/constants.json"),
        help="Path to constants JSON with chunking budget/path defaults",
    )
    parser.add_argument("--review-units", type=Path, default=None, help="Path to review_units.json")
    parser.add_argument("--linear-units", type=Path, default=None, help="Path to linear_units.json")
    parser.add_argument("--docx-struct", type=Path, default=None, help="Optional path to docx_struct.json")
    parser.add_argument("--output-dir", type=Path, default=None, help="Output folder for chunk artifacts")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    constants = load_constants(args.constants)
    default_paths = resolve_chunk_paths(constants)

    review_units_path = args.review_units or default_paths["review_units"]
    linear_units_path = args.linear_units or default_paths["linear_units"]
    output_dir = args.output_dir or default_paths["output_dir"]

    configured_docx_struct = default_paths.get("docx_struct")
    docx_struct_path = args.docx_struct if args.docx_struct is not None else configured_docx_struct
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
        constants_path=args.constants,
        docx_struct_path=docx_struct_path,
    )

    print(f"Review units: {review_units_path}")
    print(f"Linear units: {linear_units_path}")
    if docx_struct_path is not None:
        print(f"Docx struct: {docx_struct_path}")
    print(f"Wrote: {result['manifest_path']}")
    print(f"Chunk files: {result['chunk_count']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
