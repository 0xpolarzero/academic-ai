from __future__ import annotations

import json
from pathlib import Path
import re
import subprocess
import sys
from typing import Any
import xml.etree.ElementTree as ET
import zipfile

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
REPORT_SCRIPT = (
    REPO_ROOT
    / ".codex/skills/docx_change_report_before_after/scripts/change_report.py"
)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _utf16_offsets(text: str) -> list[int]:
    offsets = [0]
    total = 0
    for char in text:
        total += len(char.encode("utf-16-le")) // 2
        offsets.append(total)
    return offsets


def _utf16_span_for_occurrence(text: str, snippet: str, occurrence: int = 1) -> tuple[int, int]:
    if occurrence < 1:
        raise ValueError("occurrence must be >= 1")

    remaining = occurrence
    cursor = 0
    while True:
        start_cp = text.find(snippet, cursor)
        if start_cp < 0:
            raise AssertionError(f"Could not find occurrence {occurrence} for snippet: {snippet!r}")
        remaining -= 1
        if remaining == 0:
            end_cp = start_cp + len(snippet)
            offsets = _utf16_offsets(text)
            return offsets[start_cp], offsets[end_cp]
        cursor = start_cp + 1


def _run_report(
    *,
    review_units_path: Path,
    patch_path: Path,
    apply_log_path: Path,
    output_md: Path,
    output_json: Path,
    output_docx: Path | None = None,
) -> tuple[dict[str, Any], str]:
    project_dir = output_json.parent.parent
    project_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            sys.executable,
            str(REPORT_SCRIPT),
            "--project-dir",
            str(project_dir),
            "--review-units",
            str(review_units_path),
            "--patch",
            str(patch_path),
            "--apply-log",
            str(apply_log_path),
            "--output-md",
            str(output_md),
            "--output-json",
            str(output_json),
        ]
        + (["--output-docx", str(output_docx)] if output_docx is not None else []),
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    assert output_json.exists(), "Missing output changes.json"
    assert output_md.exists(), "Missing output changes.md"
    if output_docx is not None:
        assert output_docx.exists(), "Missing output changes.docx"

    payload = json.loads(output_json.read_text(encoding="utf-8"))
    markdown = output_md.read_text(encoding="utf-8")
    return payload, markdown


def test_change_report_emits_stable_location_before_after_and_status(tmp_path: Path) -> None:
    accepted_text = "Alpha beta gamma."
    beta_start, beta_end = _utf16_span_for_occurrence(accepted_text, "beta")
    alpha_start, alpha_end = _utf16_span_for_occurrence(accepted_text, "Alpha")

    review_units_path = tmp_path / "artifacts/docx_extract/review_units.json"
    patch_path = tmp_path / "artifacts/patch/merged_patch.json"
    apply_log_path = tmp_path / "artifacts/apply/apply_log.json"
    output_md = tmp_path / "output/changes.md"
    output_json = tmp_path / "output/changes.json"

    _write_json(
        review_units_path,
        {
            "source_docx": "synthetic.docx",
            "part_count": 1,
            "unit_count": 1,
            "units": [
                {
                    "part": "word/document.xml",
                    "part_kind": "body",
                    "part_name": "document",
                    "para_id": "para_1",
                    "unit_uid": "unit_1",
                    "accepted_text": accepted_text,
                    "heading_path": ["Section 1"],
                    "order_index": 0,
                    "location": {
                        "global_order_index": 0,
                        "paragraph_index_in_part": 0,
                        "part_index": 0,
                        "in_table": False,
                        "path_hint": "word/document.xml::.//w:p[1]",
                    },
                }
            ],
        },
    )

    _write_json(
        patch_path,
        {
            "schema_version": "patch.v1",
            "created_at": "2026-02-27T00:00:00Z",
            "author": "test",
            "ops": [
                {
                    "type": "replace_range",
                    "target": {
                        "part": "word/document.xml",
                        "para_id": "para_1",
                        "unit_uid": "unit_1",
                    },
                    "range": {"start": beta_start, "end": beta_end},
                    "expected": {"snippet": "beta"},
                    "replacement": "BETA",
                },
                {
                    "type": "add_comment",
                    "target": {
                        "part": "word/document.xml",
                        "para_id": "para_1",
                        "unit_uid": "unit_1",
                    },
                    "range": {"start": alpha_start, "end": alpha_end},
                    "expected": {"snippet": "Alpha"},
                    "comment_text": "Clarify intro.",
                },
            ],
        },
    )

    _write_json(
        apply_log_path,
        {
            "schema_version": "apply_log.v1",
            "created_at": "2026-02-27T00:00:01Z",
            "ops": [
                {
                    "op_index": 0,
                    "type": "replace_range",
                    "target": {
                        "part": "word/document.xml",
                        "para_id": "para_1",
                        "unit_uid": "unit_1",
                    },
                    "resolved_target": {
                        "part": "word/document.xml",
                        "para_id": "para_1",
                        "unit_uid": "unit_1",
                        "paragraph_index_in_part": 0,
                    },
                    "range": {"start": beta_start, "end": beta_end},
                    "expected": {"snippet": "beta"},
                    "actual_snippet": "beta",
                    "status": "applied",
                    "reason": None,
                },
                {
                    "op_index": 1,
                    "type": "add_comment",
                    "target": {
                        "part": "word/document.xml",
                        "para_id": "para_1",
                        "unit_uid": "unit_1",
                    },
                    "range": {"start": alpha_start, "end": alpha_end},
                    "expected": {"snippet": "Alpha"},
                    "actual_snippet": "Alpha",
                    "status": "skipped",
                    "reason": "target_not_found",
                },
            ],
        },
    )

    payload, markdown = _run_report(
        review_units_path=review_units_path,
        patch_path=patch_path,
        apply_log_path=apply_log_path,
        output_md=output_md,
        output_json=output_json,
    )

    assert payload["schema_version"] == "change_report.v1"
    assert payload["stats"]["op_count"] == 1
    # Stats count only applied operations present in annotated DOCX.

    first_change = payload["changes"][0]
    assert first_change["location"]["heading_path"] == ["Section 1"]
    assert first_change["location"]["part"] == "word/document.xml"
    assert first_change["location"]["para_id"] == "para_1"
    assert first_change["location"]["unit_uid"] == "unit_1"
    assert "word/document.xml" in first_change["stable_location"]
    assert "para_1" in first_change["stable_location"]
    assert "unit_1" in first_change["stable_location"]
    # before_snippet now includes more context for Ctrl+F (up to ~60 chars)
    assert "beta" in first_change["before_snippet"]
    assert first_change["after_snippet"] == "BETA"
    # location_uncertain is False since there's no disambiguation
    assert first_change.get("location_uncertain") is False

    # Markdown now uses Review format with tables
    assert "# Review" in markdown
    assert "1 suggestions" in markdown
    assert "Section 1" in markdown
    assert "| # | At | Suggestion |" in markdown
    # Context in both columns, old text bold in At, new text bold in Suggestion
    assert "**beta**" in markdown  # Old text bold
    assert "**BETA**" in markdown  # New text bold


def test_change_report_includes_disambiguation_for_repeated_before_snippet(tmp_path: Path) -> None:
    accepted_text = "beta and beta again"
    second_beta_start, second_beta_end = _utf16_span_for_occurrence(accepted_text, "beta", occurrence=2)

    review_units_path = tmp_path / "artifacts/docx_extract/review_units.json"
    patch_path = tmp_path / "artifacts/patch/merged_patch.json"
    apply_log_path = tmp_path / "artifacts/apply/apply_log.json"
    output_md = tmp_path / "output/changes.md"
    output_json = tmp_path / "output/changes.json"

    _write_json(
        review_units_path,
        {
            "source_docx": "synthetic.docx",
            "part_count": 1,
            "unit_count": 1,
            "units": [
                {
                    "part": "word/document.xml",
                    "part_kind": "body",
                    "part_name": "document",
                    "para_id": "para_repeat",
                    "unit_uid": "unit_repeat",
                    "accepted_text": accepted_text,
                    "heading_path": [],
                    "order_index": 0,
                    "location": {
                        "global_order_index": 0,
                        "paragraph_index_in_part": 0,
                        "part_index": 0,
                        "in_table": False,
                        "path_hint": "word/document.xml::.//w:p[1]",
                    },
                }
            ],
        },
    )

    _write_json(
        patch_path,
        {
            "schema_version": "patch.v1",
            "created_at": "2026-02-27T00:00:00Z",
            "author": "test",
            "ops": [
                {
                    "type": "replace_range",
                    "target": {
                        "part": "word/document.xml",
                        "para_id": "para_repeat",
                        "unit_uid": "unit_repeat",
                    },
                    "range": {"start": second_beta_start, "end": second_beta_end},
                    "expected": {"snippet": "beta"},
                    "replacement": "BETA",
                }
            ],
        },
    )

    _write_json(
        apply_log_path,
        {
            "schema_version": "apply_log.v1",
            "created_at": "2026-02-27T00:00:01Z",
            "ops": [
                {
                    "op_index": 0,
                    "type": "replace_range",
                    "target": {
                        "part": "word/document.xml",
                        "para_id": "para_repeat",
                        "unit_uid": "unit_repeat",
                    },
                    "resolved_target": {
                        "part": "word/document.xml",
                        "para_id": "para_repeat",
                        "unit_uid": "unit_repeat",
                        "paragraph_index_in_part": 0,
                    },
                    "range": {
                        "start": second_beta_start,
                        "end": second_beta_end,
                    },
                    "expected": {"snippet": "beta"},
                    "actual_snippet": "beta",
                    "status": "applied",
                    "reason": None,
                }
            ],
        },
    )

    payload, _ = _run_report(
        review_units_path=review_units_path,
        patch_path=patch_path,
        apply_log_path=apply_log_path,
        output_md=output_md,
        output_json=output_json,
    )

    change = payload["changes"][0]
    assert "disambiguation" in change

    disambiguation = change["disambiguation"]
    assert disambiguation["kind"] == "repeated_before_snippet"
    assert disambiguation["occurrence_count"] == 2
    assert disambiguation["occurrence_index"] == 2
    assert disambiguation["range"] == {
        "start": second_beta_start,
        "end": second_beta_end,
    }
    assert len(disambiguation["match_start_offsets"]) == 2


def test_change_report_keeps_whitespace_changes_merges_adjacent_replacements_and_avoids_unsafe_bold(tmp_path: Path) -> None:
    accepted_text = " Dans ceci ; mais internaliste et externaliste. A*B pattern. géopolitique "

    dans_start, dans_end = _utf16_span_for_occurrence(accepted_text, " Dans")
    semicolon_start, semicolon_end = _utf16_span_for_occurrence(accepted_text, "; mais")
    internaliste_start, internaliste_end = _utf16_span_for_occurrence(accepted_text, "internaliste")
    externaliste_start, externaliste_end = _utf16_span_for_occurrence(accepted_text, "externaliste")
    star_start, star_end = _utf16_span_for_occurrence(accepted_text, "A*B")
    geo_start, geo_end = _utf16_span_for_occurrence(accepted_text, "géopolitique ")

    review_units_path = tmp_path / "artifacts/docx_extract/review_units.json"
    patch_path = tmp_path / "artifacts/patch/merged_patch.json"
    apply_log_path = tmp_path / "artifacts/apply/apply_log.json"
    output_md = tmp_path / "output/changes.md"
    output_json = tmp_path / "output/changes.json"

    _write_json(
        review_units_path,
        {
            "source_docx": "synthetic.docx",
            "part_count": 1,
            "unit_count": 1,
            "units": [
                {
                    "part": "word/document.xml",
                    "part_kind": "body",
                    "part_name": "document",
                    "para_id": "para_1",
                    "unit_uid": "unit_1",
                    "accepted_text": accepted_text,
                    "heading_path": ["Section 1"],
                    "order_index": 0,
                    "location": {
                        "global_order_index": 0,
                        "paragraph_index_in_part": 0,
                        "part_index": 0,
                        "in_table": False,
                        "path_hint": "word/document.xml::.//w:p[1]",
                    },
                }
            ],
        },
    )

    ops = [
        {
            "type": "replace_range",
            "target": {"part": "word/document.xml", "para_id": "para_1", "unit_uid": "unit_1"},
            "range": {"start": dans_start, "end": dans_end},
            "expected": {"snippet": " Dans"},
            "replacement": "Dans",
        },
        {
            "type": "replace_range",
            "target": {"part": "word/document.xml", "para_id": "para_1", "unit_uid": "unit_1"},
            "range": {"start": semicolon_start, "end": semicolon_end},
            "expected": {"snippet": "; mais"},
            "replacement": ", mais",
        },
        {
            "type": "replace_range",
            "target": {"part": "word/document.xml", "para_id": "para_1", "unit_uid": "unit_1"},
            "range": {"start": externaliste_start, "end": externaliste_end},
            "expected": {"snippet": "externaliste"},
            "replacement": "externalistes",
        },
        # Intentionally keep reverse textual order (externaliste before internaliste)
        # to validate order-agnostic merge behavior.
        {
            "type": "replace_range",
            "target": {"part": "word/document.xml", "para_id": "para_1", "unit_uid": "unit_1"},
            "range": {"start": internaliste_start, "end": internaliste_end},
            "expected": {"snippet": "internaliste"},
            "replacement": "internalistes",
        },
        {
            "type": "replace_range",
            "target": {"part": "word/document.xml", "para_id": "para_1", "unit_uid": "unit_1"},
            "range": {"start": star_start, "end": star_end},
            "expected": {"snippet": "A*B"},
            "replacement": "A*B*",
        },
        {
            "type": "replace_range",
            "target": {"part": "word/document.xml", "para_id": "para_1", "unit_uid": "unit_1"},
            "range": {"start": geo_start, "end": geo_end},
            "expected": {"snippet": "géopolitique "},
            "replacement": "géopolitique",
        },
    ]
    _write_json(
        patch_path,
        {
            "schema_version": "patch.v1",
            "created_at": "2026-03-01T00:00:00Z",
            "author": "test",
            "ops": ops,
        },
    )

    apply_ops = []
    for op_index, op in enumerate(ops):
        apply_ops.append(
            {
                "op_index": op_index,
                "type": op["type"],
                "target": op["target"],
                "resolved_target": {
                    "part": op["target"]["part"],
                    "para_id": op["target"]["para_id"],
                    "unit_uid": op["target"]["unit_uid"],
                    "paragraph_index_in_part": 0,
                },
                "range": op["range"],
                "expected": op["expected"],
                "actual_snippet": op["expected"]["snippet"],
                "status": "applied",
                "reason": None,
            }
        )

    _write_json(
        apply_log_path,
        {
            "schema_version": "apply_log.v1",
            "created_at": "2026-03-01T00:00:01Z",
            "ops": apply_ops,
        },
    )

    payload, markdown = _run_report(
        review_units_path=review_units_path,
        patch_path=patch_path,
        apply_log_path=apply_log_path,
        output_md=output_md,
        output_json=output_json,
    )

    # Whitespace-only edits should be visible in the markdown report.
    assert "␠" in markdown

    # Spacing before comma should be normalized in suggestion output.
    assert " , mais" not in markdown
    assert ", mais" in markdown
    assert re.search(r"[ \u00A0]\*\*,\*\*", markdown) is None

    # Adjacent sentence-level replacements should be merged into one suggestion.
    merged = [
        change for change in payload["changes"]
        if change.get("type") == "replace_range"
        and "internaliste" in str(change.get("exact_snippet", ""))
        and "externaliste" in str(change.get("exact_snippet", ""))
        and "internalistes" in str(change.get("after_snippet", ""))
        and "externalistes" in str(change.get("after_snippet", ""))
    ]
    assert len(merged) == 1
    merged_indices = set(merged[0].get("merged_op_indices", []))
    assert 2 in merged_indices
    assert 3 in merged_indices

    # Unsafe markdown-bold text should remain unwrapped.
    assert "**A*B**" not in markdown
    assert "A\\*B" in markdown


def test_change_report_emits_docx_table_and_bold_runs(tmp_path: Path) -> None:
    pytest.importorskip("docx")

    accepted_text = "Alpha beta gamma."
    beta_start, beta_end = _utf16_span_for_occurrence(accepted_text, "beta")

    review_units_path = tmp_path / "artifacts/docx_extract/review_units.json"
    patch_path = tmp_path / "artifacts/patch/merged_patch.json"
    apply_log_path = tmp_path / "artifacts/apply/apply_log.json"
    output_md = tmp_path / "output/changes.md"
    output_json = tmp_path / "output/changes.json"
    output_docx = tmp_path / "output/changes.docx"

    _write_json(
        review_units_path,
        {
            "source_docx": "synthetic.docx",
            "part_count": 1,
            "unit_count": 1,
            "units": [
                {
                    "part": "word/document.xml",
                    "part_kind": "body",
                    "part_name": "document",
                    "para_id": "para_1",
                    "unit_uid": "unit_1",
                    "accepted_text": accepted_text,
                    "heading_path": ["Section 1"],
                    "order_index": 0,
                    "location": {
                        "global_order_index": 0,
                        "paragraph_index_in_part": 0,
                        "part_index": 0,
                        "in_table": False,
                        "path_hint": "word/document.xml::.//w:p[1]",
                    },
                }
            ],
        },
    )

    _write_json(
        patch_path,
        {
            "schema_version": "patch.v1",
            "created_at": "2026-03-01T00:00:00Z",
            "author": "test",
            "ops": [
                {
                    "type": "replace_range",
                    "target": {
                        "part": "word/document.xml",
                        "para_id": "para_1",
                        "unit_uid": "unit_1",
                    },
                    "range": {"start": beta_start, "end": beta_end},
                    "expected": {"snippet": "beta"},
                    "replacement": "BETA",
                }
            ],
        },
    )

    _write_json(
        apply_log_path,
        {
            "schema_version": "apply_log.v1",
            "created_at": "2026-03-01T00:00:01Z",
            "ops": [
                {
                    "op_index": 0,
                    "type": "replace_range",
                    "target": {
                        "part": "word/document.xml",
                        "para_id": "para_1",
                        "unit_uid": "unit_1",
                    },
                    "resolved_target": {
                        "part": "word/document.xml",
                        "para_id": "para_1",
                        "unit_uid": "unit_1",
                        "paragraph_index_in_part": 0,
                    },
                    "range": {"start": beta_start, "end": beta_end},
                    "expected": {"snippet": "beta"},
                    "actual_snippet": "beta",
                    "status": "applied",
                    "reason": None,
                }
            ],
        },
    )

    _, _ = _run_report(
        review_units_path=review_units_path,
        patch_path=patch_path,
        apply_log_path=apply_log_path,
        output_md=output_md,
        output_json=output_json,
        output_docx=output_docx,
    )

    with zipfile.ZipFile(output_docx, mode="r") as zf:
        assert zf.testzip() is None, "Output DOCX is not a valid zip package"
        names = set(zf.namelist())
        assert "word/document.xml" in names
        document_root = ET.fromstring(zf.read("word/document.xml"))

    w_ns = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
    assert document_root.find(f".//{{{w_ns}}}tbl") is not None
    assert document_root.find(f".//{{{w_ns}}}b") is not None
    all_text = "".join(node.text or "" for node in document_root.findall(f".//{{{w_ns}}}t"))
    assert "BETA" in all_text
