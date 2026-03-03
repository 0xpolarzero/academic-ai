#!/usr/bin/env python3
"""Validate dry-run pipeline acceptance outputs for a project directory."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import xml.etree.ElementTree as ET
import zipfile

# Import report generation for on-demand DOCX generation
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / ".codex" / "skills" / "docx_change_report_before_after" / "scripts"))
from _report_lib import build_change_report_artifacts  # type: ignore[import-not-found]

W_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
CT_NS = "http://schemas.openxmlformats.org/package/2006/content-types"

COMMENTS_CONTENT_TYPE = "application/vnd.openxmlformats-officedocument.wordprocessingml.comments+xml"
COMMENTS_REL_TYPE = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/comments"


def qn(ns: str, local: str) -> str:
    return f"{{{ns}}}{local}"


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _require_file(path: Path) -> None:
    if not path.exists() or not path.is_file():
        raise RuntimeError(f"Missing required file: {path}")


def _require_dir(path: Path) -> None:
    if not path.exists() or not path.is_dir():
        raise RuntimeError(f"Missing required directory: {path}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-dir", type=Path, required=True, help="Project directory root")
    parser.add_argument(
        "--input-name",
        default=None,
        help=(
            "Input base filename without extension (e.g., 'chapter1'). "
            "Required when multiple input artifact sets exist."
        ),
    )
    return parser


def _discover_input_names(project_dir: Path) -> list[str]:
    """Discover input names from artifact subdirectories.
    
    Returns a list of input names (e.g., ['source', 'chapter1']) found in
    the artifacts/docx_extract/ directory.
    """
    extract_dir = project_dir / "artifacts" / "docx_extract"
    if not extract_dir.exists():
        return []
    
    input_names = []
    for item in extract_dir.iterdir():
        if item.is_dir() and (item / "review_units.json").exists():
            input_names.append(item.name)
    
    return sorted(input_names)


def _resolve_target_input_name(*, input_name_arg: str | None, input_names: list[str]) -> str | None:
    if input_name_arg:
        input_name = str(input_name_arg).strip()
        if not input_name:
            raise RuntimeError("--input-name cannot be empty")
        if input_names and input_name not in input_names:
            available = ", ".join(input_names)
            raise RuntimeError(
                f"--input-name '{input_name}' not found in extracted artifacts. "
                f"Available input names: {available}"
            )
        return input_name

    if not input_names:
        return None

    if len(input_names) == 1:
        return input_names[0]

    available = ", ".join(input_names)
    raise RuntimeError(
        "Multiple input artifact sets found; please select one with --input-name. "
        f"Available input names: {available}"
    )


def _resolve_chunk_results_dir(project_dir: Path, *, input_name: str) -> Path:
    candidates = [
        project_dir / "artifacts/judged/chunk_results" / input_name,
        project_dir / "artifacts/ralph_0/chunk_results" / input_name,
        project_dir / "artifacts/chunk_results" / input_name,
    ]

    for candidate in candidates:
        if candidate.exists() and candidate.is_dir():
            # Verify directory has actual chunk result files (not just empty dir)
            if list(candidate.glob("chunk_*_result.json")):
                return candidate

    attempted = ", ".join(str(path) for path in candidates)
    raise RuntimeError(
        f"Missing required chunk results directory for input '{input_name}'. Checked: {attempted}"
    )


def _validate_artifact_presence(project_dir: Path, *, input_name_arg: str | None) -> dict[str, Path]:
    # Discover available input names from artifact structure
    discovered_input_names = _discover_input_names(project_dir)
    input_name = _resolve_target_input_name(
        input_name_arg=input_name_arg,
        input_names=discovered_input_names,
    )

    if input_name is None:
        # Fall back to legacy structure for backward compatibility
        base_paths = {
            "review_units": project_dir / "artifacts/docx_extract/review_units.json",
            "linear_units": project_dir / "artifacts/docx_extract/linear_units.json",
            "docx_struct": project_dir / "artifacts/docx_extract/docx_struct.json",
            "manifest": project_dir / "artifacts/chunks/manifest.json",
            "final_patch": project_dir / "artifacts/patch/final_patch.json",
            "merge_report": project_dir / "artifacts/patch/merge_report.json",
            "apply_log": project_dir / "artifacts/apply/apply_log.json",
            "annotated_docx": project_dir / "output/annotated.docx",
            "changes_md": project_dir / "output/changes.md",
            "changes_json": project_dir / "output/changes.json",
        }
        # Optional: generated on-demand if missing
        optional_paths = {
            "changes_docx": project_dir / "output/changes.docx",
        }
        chunk_results_dir = project_dir / "artifacts/chunk_results"
    else:
        # Validate only the requested/current input artifact set.
        base_paths = {
            "review_units": project_dir / "artifacts/docx_extract" / input_name / "review_units.json",
            "linear_units": project_dir / "artifacts/docx_extract" / input_name / "linear_units.json",
            "docx_struct": project_dir / "artifacts/docx_extract" / input_name / "docx_struct.json",
            "manifest": project_dir / "artifacts/chunks" / input_name / "manifest.json",
            "final_patch": project_dir / "artifacts/patch" / input_name / "final_patch.json",
            "merge_report": project_dir / "artifacts/patch" / input_name / "merge_report.json",
            "apply_log": project_dir / "artifacts/apply" / input_name / "apply_log.json",
            "annotated_docx": project_dir / "output" / f"{input_name}_annotated.docx",
            "changes_md": project_dir / "output" / f"{input_name}_changes.md",
            "changes_json": project_dir / "output" / f"{input_name}_changes.json",
        }
        # Optional: generated on-demand if missing
        optional_paths = {
            "changes_docx": project_dir / "output" / f"{input_name}_changes.docx",
        }
        chunk_results_dir = _resolve_chunk_results_dir(project_dir, input_name=input_name)
    
    _require_dir(chunk_results_dir)

    for path in base_paths.values():
        _require_file(path)

    chunk_results = sorted(chunk_results_dir.glob("chunk_*_result.json"))
    if not chunk_results:
        raise RuntimeError("Expected at least one synthetic chunk result artifact")

    # Merge optional paths (not required to exist, but tracked for generation)
    return {**base_paths, **optional_paths}


def _validate_json_shapes(paths: dict[str, Path]) -> tuple[dict, dict]:
    manifest = _load_json(paths["manifest"])
    chunks = manifest.get("chunks")
    if not isinstance(chunks, list) or not chunks:
        raise RuntimeError("Chunk manifest must contain at least one chunk")

    patch = _load_json(paths["final_patch"])
    patch_ops = patch.get("ops")
    if not isinstance(patch_ops, list) or not patch_ops:
        raise RuntimeError("final_patch.json must contain at least one op")

    apply_log = _load_json(paths["apply_log"])
    apply_ops = apply_log.get("ops")
    if not isinstance(apply_ops, list):
        raise RuntimeError("apply_log.json must contain ops list")

    stats = apply_log.get("stats")
    if not isinstance(stats, dict):
        raise RuntimeError("apply_log.json must contain stats object")

    if stats.get("input_ops") != len(patch_ops):
        raise RuntimeError("apply_log stats.input_ops does not match final_patch op count")

    if stats.get("applied_ops", 0) <= 0:
        raise RuntimeError("Expected at least one applied operation in apply_log")

    return patch, apply_log


def _validate_annotated_docx(paths: dict[str, Path], patch: dict, apply_log: dict) -> None:
    patch_ops = patch.get("ops", [])
    has_comment_op = any(isinstance(op, dict) and op.get("type") == "add_comment" for op in patch_ops)

    with zipfile.ZipFile(paths["annotated_docx"], mode="r") as zf:
        if zf.testzip() is not None:
            raise RuntimeError("annotated.docx is not a valid zip package")

        names = set(zf.namelist())
        if "word/document.xml" not in names:
            raise RuntimeError("annotated.docx missing word/document.xml")

        document_root = ET.fromstring(zf.read("word/document.xml"))

        has_insertions = document_root.find(f".//{qn(W_NS, 'ins')}") is not None
        has_deletions = document_root.find(f".//{qn(W_NS, 'del')}") is not None

        edit_applied = int(apply_log.get("stats", {}).get("applied_edit_ops", 0) or 0) > 0
        if edit_applied and not (has_insertions or has_deletions):
            raise RuntimeError("Expected tracked changes (w:ins/w:del) for applied edit ops")

        if not has_comment_op:
            return

        if "word/comments.xml" not in names:
            raise RuntimeError("Expected word/comments.xml when add_comment ops exist")

        comments_root = ET.fromstring(zf.read("word/comments.xml"))
        if comments_root.tag != qn(W_NS, "comments"):
            raise RuntimeError("word/comments.xml must have w:comments root")
        if comments_root.find(f".//{qn(W_NS, 'comment')}") is None:
            raise RuntimeError("word/comments.xml must contain at least one w:comment")

        has_range_start = document_root.find(f".//{qn(W_NS, 'commentRangeStart')}") is not None
        has_range_end = document_root.find(f".//{qn(W_NS, 'commentRangeEnd')}") is not None
        has_reference = document_root.find(f".//{qn(W_NS, 'commentReference')}") is not None
        if not (has_range_start and has_range_end and has_reference):
            raise RuntimeError("Document missing comment range markers or comment reference")

        if "[Content_Types].xml" not in names:
            raise RuntimeError("annotated.docx missing [Content_Types].xml")
        content_types_root = ET.fromstring(zf.read("[Content_Types].xml"))
        has_comments_override = any(
            node.get("PartName") == "/word/comments.xml" and node.get("ContentType") == COMMENTS_CONTENT_TYPE
            for node in content_types_root.findall(f".//{{{CT_NS}}}Override")
        )
        if not has_comments_override:
            raise RuntimeError("[Content_Types].xml missing comments override")

        rels_path = "word/_rels/document.xml.rels"
        if rels_path not in names:
            raise RuntimeError("annotated.docx missing word/_rels/document.xml.rels")
        rels_root = ET.fromstring(zf.read(rels_path))
        has_comments_rel = any(
            node.get("Type") == COMMENTS_REL_TYPE and node.get("Target") == "comments.xml"
            for node in rels_root.findall(f".//{{{REL_NS}}}Relationship")
        )
        if not has_comments_rel:
            raise RuntimeError("document.xml.rels missing comments relationship")


def main() -> int:
    args = _build_parser().parse_args()
    project_dir = args.project_dir.expanduser().resolve()

    if not project_dir.exists() or not project_dir.is_dir():
        print(f"ERROR: --project-dir must be an existing directory: {project_dir}", file=sys.stderr)
        return 2

    try:
        paths = _validate_artifact_presence(project_dir, input_name_arg=args.input_name)
        patch, apply_log = _validate_json_shapes(paths)
        _validate_annotated_docx(paths, patch, apply_log)
    except Exception as exc:
        print(f"DRY-RUN ACCEPTANCE FAILED: {exc}", file=sys.stderr)
        return 1

    # Generate changes.docx if missing
    changes_docx_path = paths.get("changes_docx")
    if changes_docx_path and not changes_docx_path.exists():
        try:
            print(f"Generating missing changes DOCX: {changes_docx_path}")
            build_change_report_artifacts(
                review_units_path=paths["review_units"],
                patch_path=paths["final_patch"],
                apply_log_path=paths["apply_log"],
                output_md_path=paths["changes_md"],
                output_json_path=paths["changes_json"],
                output_docx_path=changes_docx_path,
            )
            print(f"Generated: {changes_docx_path}")
        except Exception as exc:
            print(f"Warning: Failed to generate changes.docx: {exc}", file=sys.stderr)

    print("Dry-run acceptance checks passed.")
    print(f"Project: {project_dir}")
    print(f"Annotated DOCX: {paths['annotated_docx']}")
    print(f"Patch: {paths['final_patch']}")
    print(f"Apply log: {paths['apply_log']}")
    if changes_docx_path:
        print(f"Changes DOCX: {changes_docx_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
