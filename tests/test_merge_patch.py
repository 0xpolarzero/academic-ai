from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
MERGE_SCRIPT = (
    REPO_ROOT / ".codex/skills/docx_merge_dedup_validate_patch/scripts/merge_patch.py"
)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _run_merge(
    *,
    chunk_results_dir: Path,
    linear_units_path: Path,
    chunks_manifest_path: Path,
    output_dir: Path,
    author: str = "merge-test",
) -> tuple[dict[str, Any], dict[str, Any]]:
    subprocess.run(
        [
            sys.executable,
            str(MERGE_SCRIPT),
            "--chunk-results-dir",
            str(chunk_results_dir),
            "--linear-units",
            str(linear_units_path),
            "--chunks-manifest",
            str(chunks_manifest_path),
            "--output-dir",
            str(output_dir),
            "--author",
            author,
        ],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    merged_patch_path = output_dir / "merged_patch.json"
    merge_report_path = output_dir / "merge_report.json"
    assert merged_patch_path.exists(), "Missing merged_patch.json"
    assert merge_report_path.exists(), "Missing merge_report.json"

    merged_patch = json.loads(merged_patch_path.read_text(encoding="utf-8"))
    merge_report = json.loads(merge_report_path.read_text(encoding="utf-8"))
    return merged_patch, merge_report


def _target_key(op: dict[str, Any]) -> tuple[str, str, str]:
    target = op["target"]
    return (
        str(target.get("part", "")),
        str(target.get("para_id", "")),
        str(target.get("unit_uid", "")),
    )


def _unit_from_target(target: dict[str, str], *, role: str, editable: bool) -> dict[str, Any]:
    return {
        "part": target["part"],
        "para_id": target["para_id"],
        "unit_uid": target["unit_uid"],
        "accepted_text": "",
        "role": role,
        "editable": editable,
        "location": {},
    }


def _write_chunks_manifest(
    *,
    chunks_output_dir: Path,
    chunk_targets: dict[str, dict[str, list[dict[str, str]]]],
) -> Path:
    manifest_path = chunks_output_dir / "manifest.json"
    chunk_entries: list[dict[str, Any]] = []

    for index, chunk_id in enumerate(sorted(chunk_targets.keys()), start=1):
        definition = chunk_targets[chunk_id]
        primary = definition.get("primary", [])
        context_before = definition.get("context_before", [])
        context_after = definition.get("context_after", [])

        chunk_file_name = f"{chunk_id}.json"
        chunk_path = chunks_output_dir / chunk_file_name
        _write_json(
            chunk_path,
            {
                "schema_version": "chunk.v1",
                "chunk_id": chunk_id,
                "chunk_index": index - 1,
                "contract": "primary-only",
                "primary_units": [_unit_from_target(target, role="primary", editable=True) for target in primary],
                "context_units_before": [
                    _unit_from_target(target, role="context_before", editable=False) for target in context_before
                ],
                "context_units_after": [
                    _unit_from_target(target, role="context_after", editable=False) for target in context_after
                ],
                "metadata": {},
            },
        )

        chunk_entries.append(
            {
                "chunk_id": chunk_id,
                "path": chunk_file_name,
                "primary_targets": [
                    {"part": item["part"], "para_id": item["para_id"], "unit_uid": item["unit_uid"]}
                    for item in primary
                ],
                "context_targets_before": [
                    {"part": item["part"], "para_id": item["para_id"], "unit_uid": item["unit_uid"]}
                    for item in context_before
                ],
                "context_targets_after": [
                    {"part": item["part"], "para_id": item["para_id"], "unit_uid": item["unit_uid"]}
                    for item in context_after
                ],
            }
        )

    _write_json(
        manifest_path,
        {
            "schema_version": "chunk_manifest.v1",
            "generated_at": "2026-02-27T00:00:00Z",
            "contract": "primary-only",
            "source": {},
            "token_budget": {},
            "unit_count": 0,
            "chunk_count": len(chunk_entries),
            "chunks": chunk_entries,
        },
    )
    return manifest_path


def _write_linear_units(path: Path, *, targets: list[dict[str, str]]) -> None:
    _write_json(
        path,
        {
            "source_docx": "synthetic.docx",
            "part_count": 1,
            "unit_count": len(targets),
            "unit_uids": [target["unit_uid"] for target in targets],
            "units": [target["unit_uid"] for target in targets],
            "order": [
                {
                    "order_index": index,
                    "part": target["part"],
                    "part_kind": "body",
                    "part_name": "document",
                    "para_id": target["para_id"],
                    "unit_uid": target["unit_uid"],
                }
                for index, target in enumerate(targets)
            ],
        },
    )


def test_merge_patch_dedup_conflict_downgrade_and_ordering(tmp_path: Path) -> None:
    chunk_results_dir = tmp_path / "chunk_results"
    output_dir = tmp_path / "patch"
    linear_units_path = tmp_path / "docx_extract/linear_units.json"
    chunks_output_dir = tmp_path / "chunks"

    target_primary = {"part": "word/document.xml", "para_id": "para_z", "unit_uid": "unit_z"}
    target_secondary = {"part": "word/document.xml", "para_id": "para_a", "unit_uid": "unit_a"}
    chunks_manifest_path = _write_chunks_manifest(
        chunks_output_dir=chunks_output_dir,
        chunk_targets={
            "chunk_0001": {"primary": [target_primary, target_secondary]},
            "chunk_0002": {"primary": [target_primary]},
        },
    )

    _write_json(
        chunk_results_dir / "chunk_0001_result.json",
        {
            "schema_version": "chunk_result.v1",
            "chunk_id": "chunk_0001",
            "ops": [
                {
                    "type": "replace_range",
                    "target": target_primary,
                    "range": {"start": 10, "end": 14},
                    "expected": {"snippet": "beta"},
                    "replacement": "BETA",
                },
                {
                    "type": "insert_at",
                    "target": target_primary,
                    "range": {"start": 20, "end": 20},
                    "expected": {"snippet": ""},
                    "new_text": " !!!",
                },
                {
                    "type": "delete_range",
                    "target": target_secondary,
                    "range": {"start": 5, "end": 9},
                    "expected": {"snippet": "junk"},
                },
            ],
        },
    )

    _write_json(
        chunk_results_dir / "chunk_0002_result.json",
        {
            "schema_version": "chunk_result.v1",
            "chunk_id": "chunk_0002",
            "ops": [
                {
                    "type": "replace_range",
                    "target": target_primary,
                    "range": {"start": 10, "end": 14},
                    "expected": {"snippet": "beta"},
                    "replacement": "BETA",
                },
                {
                    "type": "replace_range",
                    "target": target_primary,
                    "range": {"start": 12, "end": 16},
                    "expected": {"snippet": "ta g"},
                    "replacement": "XXXX",
                },
                {
                    "type": "replace_range",
                    "target": target_primary,
                    "range": {"start": 10, "end": 14},
                    "expected": {"snippet": "beta"},
                    "replacement": "Beta2",
                },
                {
                    "type": "add_comment",
                    "target": target_primary,
                    "range": {"start": 0, "end": 5},
                    "expected": {"snippet": "Alpha"},
                    "comment_text": "Nit: tighten intro.",
                },
            ],
        },
    )

    _write_json(
        linear_units_path,
        {
            "source_docx": "synthetic.docx",
            "part_count": 1,
            "unit_count": 2,
            "unit_uids": ["unit_z", "unit_a"],
            "units": ["unit_z", "unit_a"],
            "order": [
                {
                    "order_index": 0,
                    "part": "word/document.xml",
                    "part_kind": "body",
                    "part_name": "document",
                    "para_id": "para_z",
                    "unit_uid": "unit_z",
                },
                {
                    "order_index": 1,
                    "part": "word/document.xml",
                    "part_kind": "body",
                    "part_name": "document",
                    "para_id": "para_a",
                    "unit_uid": "unit_a",
                },
            ],
        },
    )

    merged_patch, merge_report = _run_merge(
        chunk_results_dir=chunk_results_dir,
        linear_units_path=linear_units_path,
        chunks_manifest_path=chunks_manifest_path,
        output_dir=output_dir,
        author="merge-test",
    )

    assert merged_patch["schema_version"] == "patch.v1"
    assert merged_patch["author"] == "merge-test"

    stats = merge_report["stats"]
    assert stats["chunk_file_count"] == 2
    assert stats["input_ops"] == 7
    assert stats["valid_ops"] == 7
    assert stats["duplicates_removed"] == 1
    assert stats["ops_after_dedup"] == 6
    assert stats["conflict_downgrades"] == 2
    assert stats["final_ops"] == 6

    merged_ops = merged_patch["ops"]
    assert len(merged_ops) == 6

    kept_primary_replace = [
        op
        for op in merged_ops
        if op["type"] == "replace_range"
        and _target_key(op) == ("word/document.xml", "para_z", "unit_z")
        and op["range"] == {"start": 10, "end": 14}
        and op.get("replacement") == "BETA"
    ]
    assert len(kept_primary_replace) == 1

    assert not [
        op
        for op in merged_ops
        if op["type"] == "replace_range"
        and _target_key(op) == ("word/document.xml", "para_z", "unit_z")
        and op["range"] in ({"start": 12, "end": 16}, {"start": 10, "end": 14})
        and op.get("replacement") in {"XXXX", "Beta2"}
    ], "Conflicting replace_range edits should be downgraded to add_comment"

    downgraded_comments = [
        op
        for op in merged_ops
        if op["type"] == "add_comment" and "Conflict downgrade" in op.get("comment_text", "")
    ]
    assert len(downgraded_comments) == 2

    # linear_units order should dominate lexical para_id ordering (para_z before para_a)
    target_sequence = [_target_key(op) for op in merged_ops]
    first_secondary_index = next(
        index
        for index, key in enumerate(target_sequence)
        if key == ("word/document.xml", "para_a", "unit_a")
    )
    assert all(
        key == ("word/document.xml", "para_z", "unit_z")
        for key in target_sequence[:first_secondary_index]
    )

    starts_by_target: dict[tuple[str, str, str], list[int]] = {}
    for op in merged_ops:
        starts_by_target.setdefault(_target_key(op), []).append(op["range"]["start"])

    for starts in starts_by_target.values():
        assert starts == sorted(starts, reverse=True), "Ops must be descending by range.start per target"


def test_merge_patch_conflict_detected_when_unit_uid_missing(tmp_path: Path) -> None:
    chunk_results_dir = tmp_path / "chunk_results"
    output_dir = tmp_path / "patch"
    linear_units_path = tmp_path / "docx_extract/linear_units.json"
    chunks_output_dir = tmp_path / "chunks"

    target_with_uid = {"part": "word/document.xml", "para_id": "para_1", "unit_uid": "unit_1"}
    target_without_uid = {"part": "word/document.xml", "para_id": "para_1"}
    chunks_manifest_path = _write_chunks_manifest(
        chunks_output_dir=chunks_output_dir,
        chunk_targets={
            "chunk_0001": {"primary": [target_with_uid]},
            "chunk_0002": {"primary": [target_with_uid]},
        },
    )

    _write_json(
        chunk_results_dir / "chunk_0001_result.json",
        {
            "schema_version": "chunk_result.v1",
            "chunk_id": "chunk_0001",
            "ops": [
                {
                    "type": "replace_range",
                    "target": target_with_uid,
                    "range": {"start": 4, "end": 9},
                    "expected": {"snippet": "beta"},
                    "replacement": "BETA",
                }
            ],
        },
    )

    _write_json(
        chunk_results_dir / "chunk_0002_result.json",
        {
            "schema_version": "chunk_result.v1",
            "chunk_id": "chunk_0002",
            "ops": [
                {
                    "type": "delete_range",
                    "target": target_without_uid,
                    "range": {"start": 6, "end": 8},
                    "expected": {"snippet": "ta"},
                }
            ],
        },
    )

    _write_json(
        linear_units_path,
        {
            "source_docx": "synthetic.docx",
            "part_count": 1,
            "unit_count": 1,
            "unit_uids": ["unit_1"],
            "units": ["unit_1"],
            "order": [
                {
                    "order_index": 0,
                    "part": "word/document.xml",
                    "part_kind": "body",
                    "part_name": "document",
                    "para_id": "para_1",
                    "unit_uid": "unit_1",
                }
            ],
        },
    )

    merged_patch, merge_report = _run_merge(
        chunk_results_dir=chunk_results_dir,
        linear_units_path=linear_units_path,
        chunks_manifest_path=chunks_manifest_path,
        output_dir=output_dir,
        author="merge-test",
    )

    assert merge_report["stats"]["conflict_downgrades"] == 1

    merged_ops = merged_patch["ops"]
    assert len(merged_ops) == 2

    assert len([op for op in merged_ops if op["type"] == "replace_range"]) == 1
    assert not [op for op in merged_ops if op["type"] == "delete_range"]

    downgraded = next(op for op in merged_ops if op["type"] == "add_comment")
    assert "Conflict downgrade" in downgraded["comment_text"]
    assert "replacement" not in downgraded
    assert "new_text" not in downgraded


def test_merge_patch_orders_descending_start_per_para_across_unit_uid_variants(
    tmp_path: Path,
) -> None:
    chunk_results_dir = tmp_path / "chunk_results"
    output_dir = tmp_path / "patch"
    linear_units_path = tmp_path / "docx_extract/linear_units.json"
    chunks_output_dir = tmp_path / "chunks"

    target_with_uid = {"part": "word/document.xml", "para_id": "para_1", "unit_uid": "unit_1"}
    target_without_uid = {"part": "word/document.xml", "para_id": "para_1"}
    chunks_manifest_path = _write_chunks_manifest(
        chunks_output_dir=chunks_output_dir,
        chunk_targets={
            "chunk_0001": {"primary": [target_with_uid]},
            "chunk_0002": {"primary": [target_with_uid]},
        },
    )

    _write_json(
        chunk_results_dir / "chunk_0001_result.json",
        {
            "chunk_id": "chunk_0001",
            "ops": [
                {
                    "type": "replace_range",
                    "target": target_with_uid,
                    "range": {"start": 10, "end": 12},
                    "expected": {"snippet": "aa"},
                    "replacement": "AA",
                }
            ],
        },
    )

    _write_json(
        chunk_results_dir / "chunk_0002_result.json",
        {
            "chunk_id": "chunk_0002",
            "ops": [
                {
                    "type": "add_comment",
                    "target": target_without_uid,
                    "range": {"start": 40, "end": 40},
                    "expected": {"snippet": ""},
                    "comment_text": "High-offset para-level note.",
                }
            ],
        },
    )

    _write_json(
        linear_units_path,
        {
            "source_docx": "synthetic.docx",
            "part_count": 1,
            "unit_count": 1,
            "unit_uids": ["unit_1"],
            "units": ["unit_1"],
            "order": [
                {
                    "order_index": 0,
                    "part": "word/document.xml",
                    "part_kind": "body",
                    "part_name": "document",
                    "para_id": "para_1",
                    "unit_uid": "unit_1",
                }
            ],
        },
    )

    merged_patch, _ = _run_merge(
        chunk_results_dir=chunk_results_dir,
        linear_units_path=linear_units_path,
        chunks_manifest_path=chunks_manifest_path,
        output_dir=output_dir,
        author="merge-test",
    )

    starts = [
        op["range"]["start"]
        for op in merged_patch["ops"]
        if op["target"].get("part") == "word/document.xml" and op["target"].get("para_id") == "para_1"
    ]
    assert starts == sorted(starts, reverse=True)


def test_merge_patch_accepts_primary_target_from_manifest(tmp_path: Path) -> None:
    chunk_results_dir = tmp_path / "chunk_results"
    output_dir = tmp_path / "patch"
    linear_units_path = tmp_path / "docx_extract/linear_units.json"
    chunks_output_dir = tmp_path / "chunks"

    primary_target = {"part": "word/document.xml", "para_id": "para_ok", "unit_uid": "unit_ok"}
    chunks_manifest_path = _write_chunks_manifest(
        chunks_output_dir=chunks_output_dir,
        chunk_targets={"chunk_0001": {"primary": [primary_target]}},
    )

    _write_json(
        chunk_results_dir / "chunk_0001_result.json",
        {
            "chunk_id": "chunk_0001",
            "ops": [
                {
                    "type": "replace_range",
                    "target": primary_target,
                    "range": {"start": 0, "end": 4},
                    "expected": {"snippet": "test"},
                    "replacement": "TEST",
                }
            ],
        },
    )
    _write_linear_units(linear_units_path, targets=[primary_target])

    merged_patch, merge_report = _run_merge(
        chunk_results_dir=chunk_results_dir,
        linear_units_path=linear_units_path,
        chunks_manifest_path=chunks_manifest_path,
        output_dir=output_dir,
        author="merge-test",
    )

    assert merge_report["stats"]["ownership_rejected_ops"] == 0
    assert merge_report["stats"]["ownership_autofilled_unit_uid_ops"] == 0
    assert len(merged_patch["ops"]) == 1
    assert _target_key(merged_patch["ops"][0]) == ("word/document.xml", "para_ok", "unit_ok")


def test_merge_patch_rejects_context_and_unknown_targets(tmp_path: Path) -> None:
    chunk_results_dir = tmp_path / "chunk_results"
    output_dir = tmp_path / "patch"
    linear_units_path = tmp_path / "docx_extract/linear_units.json"
    chunks_output_dir = tmp_path / "chunks"

    primary_target = {"part": "word/document.xml", "para_id": "para_primary", "unit_uid": "unit_primary"}
    context_target = {"part": "word/document.xml", "para_id": "para_ctx", "unit_uid": "unit_ctx"}
    unknown_target = {"part": "word/document.xml", "para_id": "para_unknown", "unit_uid": "unit_unknown"}
    chunks_manifest_path = _write_chunks_manifest(
        chunks_output_dir=chunks_output_dir,
        chunk_targets={"chunk_0001": {"primary": [primary_target], "context_before": [context_target]}},
    )

    _write_json(
        chunk_results_dir / "chunk_0001_result.json",
        {
            "chunk_id": "chunk_0001",
            "ops": [
                {
                    "type": "delete_range",
                    "target": context_target,
                    "range": {"start": 1, "end": 3},
                    "expected": {"snippet": "xx"},
                },
                {
                    "type": "add_comment",
                    "target": unknown_target,
                    "range": {"start": 0, "end": 0},
                    "expected": {"snippet": ""},
                    "comment_text": "Unknown target should be rejected.",
                },
            ],
        },
    )
    _write_linear_units(linear_units_path, targets=[primary_target, context_target, unknown_target])

    merged_patch, merge_report = _run_merge(
        chunk_results_dir=chunk_results_dir,
        linear_units_path=linear_units_path,
        chunks_manifest_path=chunks_manifest_path,
        output_dir=output_dir,
        author="merge-test",
    )

    assert merged_patch["ops"] == []
    assert merge_report["stats"]["ownership_rejected_ops"] == 2
    assert merge_report["stats"]["ownership_rejected_context_ops"] == 1
    assert merge_report["stats"]["ownership_rejected_unknown_ops"] == 1
    rejection_reasons = {item["reason"] for item in merge_report["ownership"]["rejections"]}
    assert rejection_reasons == {"target_is_context_unit", "target_not_owned_by_chunk"}


def test_merge_patch_autofills_missing_unit_uid_for_unique_primary_match(tmp_path: Path) -> None:
    chunk_results_dir = tmp_path / "chunk_results"
    output_dir = tmp_path / "patch"
    linear_units_path = tmp_path / "docx_extract/linear_units.json"
    chunks_output_dir = tmp_path / "chunks"

    primary_target = {"part": "word/document.xml", "para_id": "para_auto", "unit_uid": "unit_auto"}
    chunks_manifest_path = _write_chunks_manifest(
        chunks_output_dir=chunks_output_dir,
        chunk_targets={"chunk_0001": {"primary": [primary_target]}},
    )

    _write_json(
        chunk_results_dir / "chunk_0001_result.json",
        {
            "chunk_id": "chunk_0001",
            "ops": [
                {
                    "type": "add_comment",
                    "target": {"part": "word/document.xml", "para_id": "para_auto"},
                    "range": {"start": 0, "end": 4},
                    "expected": {"snippet": "test"},
                    "comment_text": "Autofill expected.",
                }
            ],
        },
    )
    _write_linear_units(linear_units_path, targets=[primary_target])

    merged_patch, merge_report = _run_merge(
        chunk_results_dir=chunk_results_dir,
        linear_units_path=linear_units_path,
        chunks_manifest_path=chunks_manifest_path,
        output_dir=output_dir,
        author="merge-test",
    )

    assert merge_report["stats"]["ownership_autofilled_unit_uid_ops"] == 1
    assert merge_report["stats"]["ownership_rejected_ops"] == 0
    assert len(merged_patch["ops"]) == 1
    assert _target_key(merged_patch["ops"][0]) == ("word/document.xml", "para_auto", "unit_auto")


def test_merge_patch_rejects_missing_unit_uid_when_primary_match_is_ambiguous(tmp_path: Path) -> None:
    chunk_results_dir = tmp_path / "chunk_results"
    output_dir = tmp_path / "patch"
    linear_units_path = tmp_path / "docx_extract/linear_units.json"
    chunks_output_dir = tmp_path / "chunks"

    primary_target_one = {"part": "word/document.xml", "para_id": "para_amb", "unit_uid": "unit_amb_1"}
    primary_target_two = {"part": "word/document.xml", "para_id": "para_amb", "unit_uid": "unit_amb_2"}
    chunks_manifest_path = _write_chunks_manifest(
        chunks_output_dir=chunks_output_dir,
        chunk_targets={"chunk_0001": {"primary": [primary_target_one, primary_target_two]}},
    )

    _write_json(
        chunk_results_dir / "chunk_0001_result.json",
        {
            "chunk_id": "chunk_0001",
            "ops": [
                {
                    "type": "replace_range",
                    "target": {"part": "word/document.xml", "para_id": "para_amb"},
                    "range": {"start": 0, "end": 4},
                    "expected": {"snippet": "test"},
                    "replacement": "TEST",
                }
            ],
        },
    )
    _write_linear_units(linear_units_path, targets=[primary_target_one, primary_target_two])

    merged_patch, merge_report = _run_merge(
        chunk_results_dir=chunk_results_dir,
        linear_units_path=linear_units_path,
        chunks_manifest_path=chunks_manifest_path,
        output_dir=output_dir,
        author="merge-test",
    )

    assert merged_patch["ops"] == []
    assert merge_report["stats"]["ownership_rejected_ops"] == 1
    assert merge_report["stats"]["ownership_rejected_ambiguous_missing_unit_uid_ops"] == 1
    assert merge_report["ownership"]["rejections"][0]["reason"] == "missing_unit_uid_ambiguous_primary_match"
