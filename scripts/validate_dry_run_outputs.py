#!/usr/bin/env python3
"""Validate dry-run pipeline acceptance outputs for a project directory."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import xml.etree.ElementTree as ET
import zipfile

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
    return parser


def _validate_artifact_presence(project_dir: Path) -> dict[str, Path]:
    required = {
        "review_units": project_dir / "artifacts/docx_extract/review_units.json",
        "linear_units": project_dir / "artifacts/docx_extract/linear_units.json",
        "docx_struct": project_dir / "artifacts/docx_extract/docx_struct.json",
        "manifest": project_dir / "artifacts/chunks/manifest.json",
        "merged_patch": project_dir / "artifacts/patch/merged_patch.json",
        "merge_report": project_dir / "artifacts/patch/merge_report.json",
        "apply_log": project_dir / "artifacts/apply/apply_log.json",
        "annotated_docx": project_dir / "output/annotated.docx",
        "changes_md": project_dir / "output/changes.md",
        "changes_json": project_dir / "output/changes.json",
    }

    _require_dir(project_dir / "artifacts/chunk_results")

    for path in required.values():
        _require_file(path)

    chunk_results = sorted((project_dir / "artifacts/chunk_results").glob("chunk_*_result.json"))
    if not chunk_results:
        raise RuntimeError("Expected at least one synthetic chunk result artifact")

    return required


def _validate_json_shapes(paths: dict[str, Path]) -> tuple[dict, dict]:
    manifest = _load_json(paths["manifest"])
    chunks = manifest.get("chunks")
    if not isinstance(chunks, list) or not chunks:
        raise RuntimeError("Chunk manifest must contain at least one chunk")

    patch = _load_json(paths["merged_patch"])
    patch_ops = patch.get("ops")
    if not isinstance(patch_ops, list) or not patch_ops:
        raise RuntimeError("merged_patch.json must contain at least one op")

    apply_log = _load_json(paths["apply_log"])
    apply_ops = apply_log.get("ops")
    if not isinstance(apply_ops, list):
        raise RuntimeError("apply_log.json must contain ops list")

    stats = apply_log.get("stats")
    if not isinstance(stats, dict):
        raise RuntimeError("apply_log.json must contain stats object")

    if stats.get("input_ops") != len(patch_ops):
        raise RuntimeError("apply_log stats.input_ops does not match merged_patch op count")

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
        paths = _validate_artifact_presence(project_dir)
        patch, apply_log = _validate_json_shapes(paths)
        _validate_annotated_docx(paths, patch, apply_log)
    except Exception as exc:
        print(f"DRY-RUN ACCEPTANCE FAILED: {exc}", file=sys.stderr)
        return 1

    print("Dry-run acceptance checks passed.")
    print(f"Project: {project_dir}")
    print(f"Annotated DOCX: {paths['annotated_docx']}")
    print(f"Patch: {paths['merged_patch']}")
    print(f"Apply log: {paths['apply_log']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
