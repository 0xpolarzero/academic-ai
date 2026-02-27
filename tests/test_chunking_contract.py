from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
CHUNK_SCRIPT = REPO_ROOT / ".codex/skills/docx_chunk_atomic_manifest/scripts/chunk_docx.py"
CHUNK_SCHEMA_PATH = REPO_ROOT / "schemas/chunk.v1.schema.json"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _target_tuple(raw: dict) -> tuple[str, str, str]:
    return (
        str(raw.get("part", "")),
        str(raw.get("para_id", "")),
        str(raw.get("unit_uid", "")),
    )


def _build_synthetic_extraction(project_dir: Path) -> tuple[Path, Path, Path, Path]:
    extract_dir = project_dir / "artifacts/docx_extract"

    large_unit = " ".join(["oversized"] * 420)
    texts = [
        "Chapter One",
        "This opening paragraph introduces the section and sets the context.",
        "It continues with connected detail so local flow should remain intact across chunk boundaries.",
        "A short transition paragraph follows.",
        large_unit,
        "A final paragraph closes the section with a concise summary.",
    ]

    units = []
    for index, text in enumerate(texts):
        unit_uid = f"unit_{index:012x}"
        unit = {
            "part": "word/document.xml",
            "part_kind": "body",
            "part_name": "document",
            "para_id": f"para_{index:016x}",
            "unit_uid": unit_uid,
            "accepted_text": text,
            "heading_path": ["Chapter One"] if index else ["Chapter One"],
            "order_index": index,
            "location": {
                "part_index": 0,
                "paragraph_index_in_part": index,
                "global_order_index": index,
                "path_hint": f"word/document.xml::.//w:p[{index + 1}]",
                "in_table": False,
            },
        }
        units.append(unit)

    review_units_path = extract_dir / "review_units.json"
    linear_units_path = extract_dir / "linear_units.json"
    docx_struct_path = extract_dir / "docx_struct.json"

    _write_json(
        review_units_path,
        {
            "source_docx": "synthetic.docx",
            "part_count": 1,
            "unit_count": len(units),
            "units": units,
        },
    )

    _write_json(
        linear_units_path,
        {
            "source_docx": "synthetic.docx",
            "part_count": 1,
            "unit_count": len(units),
            "unit_uids": [unit["unit_uid"] for unit in units],
            "units": [unit["unit_uid"] for unit in units],
            "order": [
                {
                    "order_index": unit["order_index"],
                    "part": unit["part"],
                    "part_kind": unit["part_kind"],
                    "part_name": unit["part_name"],
                    "para_id": unit["para_id"],
                    "unit_uid": unit["unit_uid"],
                }
                for unit in units
            ],
        },
    )

    _write_json(
        docx_struct_path,
        {
            "source_docx": "synthetic.docx",
            "part_count": 1,
            "unit_count": len(units),
            "parts": [],
        },
    )

    constants_path = project_dir / "config/constants.json"
    _write_json(
        constants_path,
        {
            "chunking": {
                "paths": {
                    "review_units": str(review_units_path),
                    "linear_units": str(linear_units_path),
                    "docx_struct": str(docx_struct_path),
                    "output_dir": "artifacts/chunks",
                },
                "token_budget": {
                    "model_context_window": 120,
                    "target_fraction": 0.25,
                    "hard_max_tokens": 40,
                    "overlap_before_units": 1,
                    "overlap_after_units": 1,
                    "tokenizer_model": "gpt-4o-mini",
                },
            }
        },
    )

    return review_units_path, linear_units_path, docx_struct_path, constants_path


def test_chunk_manifest_contract(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    project_dir.mkdir(parents=True, exist_ok=True)
    review_units_path, linear_units_path, docx_struct_path, constants_path = _build_synthetic_extraction(project_dir)
    output_dir = project_dir / "artifacts/chunks"

    subprocess.run(
        [
            sys.executable,
            str(CHUNK_SCRIPT),
            "--project-dir",
            str(project_dir),
            "--constants",
            str(constants_path),
            "--review-units",
            str(review_units_path),
            "--linear-units",
            str(linear_units_path),
            "--docx-struct",
            str(docx_struct_path),
            "--output-dir",
            str(output_dir),
        ],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    manifest_path = output_dir / "manifest.json"
    assert manifest_path.exists(), "Missing artifacts/chunks/manifest.json"

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    required_manifest_keys = {
        "schema_version",
        "generated_at",
        "contract",
        "source",
        "token_budget",
        "unit_count",
        "chunk_count",
        "chunks",
    }
    assert required_manifest_keys.issubset(manifest.keys()), "manifest.json missing required keys"

    schema = json.loads(CHUNK_SCHEMA_PATH.read_text(encoding="utf-8"))
    schema_required = set(schema.get("required", []))
    schema_properties = set(schema.get("properties", {}).keys())
    assert {"context_units_before", "context_units_after", "chunk_index", "contract", "metadata"}.issubset(
        schema_required
    )
    assert "accepted_text" not in schema_properties
    assert "context_units" not in schema_properties

    chunk_paths = sorted(output_dir.glob("chunk_*.json"))
    assert chunk_paths, "No chunk_XXXX.json files were generated"
    assert len(chunk_paths) == manifest["chunk_count"], "Manifest chunk_count mismatch"

    manifest_entries_by_path = {entry["path"]: entry for entry in manifest["chunks"]}
    required_manifest_entry_keys = {
        "chunk_id",
        "path",
        "source_span",
        "token_estimates",
        "heading_path",
        "primary_unit_uids",
        "context_before_unit_uids",
        "context_after_unit_uids",
        "primary_targets",
        "context_targets_before",
        "context_targets_after",
    }
    for path_name, entry in manifest_entries_by_path.items():
        assert required_manifest_entry_keys.issubset(entry.keys()), f"{path_name} missing manifest target metadata"

    expected_manifest_paths = set(manifest_entries_by_path.keys())
    actual_chunk_paths = {path.name for path in chunk_paths}
    assert expected_manifest_paths == actual_chunk_paths, "Manifest chunk file listing mismatch"

    saw_allowed_overflow = False
    for chunk_path in chunk_paths:
        chunk = json.loads(chunk_path.read_text(encoding="utf-8"))
        manifest_entry = manifest_entries_by_path[chunk_path.name]

        required_chunk_keys = {
            "schema_version",
            "chunk_id",
            "chunk_index",
            "contract",
            "primary_units",
            "context_units_before",
            "context_units_after",
            "metadata",
        }
        assert required_chunk_keys.issubset(chunk.keys()), f"{chunk_path.name} missing required keys"

        assert chunk["primary_units"], f"{chunk_path.name} has no primary_units"
        assert chunk["contract"]["primary_units_editable"] is True
        assert chunk["contract"]["context_units_editable"] is False
        assert chunk["contract"]["context_is_read_only"] is True

        primary_ids = {unit["unit_uid"] for unit in chunk["primary_units"]}
        context_ids = {
            unit["unit_uid"]
            for unit in chunk["context_units_before"] + chunk["context_units_after"]
        }
        assert primary_ids.isdisjoint(context_ids), f"{chunk_path.name} context overlaps primary units"

        for unit in chunk["primary_units"]:
            assert unit["role"] == "primary"
            assert unit["editable"] is True
            assert {"part", "para_id", "unit_uid", "token_estimate", "location"}.issubset(unit.keys())
        for unit in chunk["context_units_before"]:
            assert unit["role"] == "context_before"
            assert unit["editable"] is False
            assert {"part", "para_id", "unit_uid", "token_estimate", "location"}.issubset(unit.keys())
        for unit in chunk["context_units_after"]:
            assert unit["role"] == "context_after"
            assert unit["editable"] is False
            assert {"part", "para_id", "unit_uid", "token_estimate", "location"}.issubset(unit.keys())

        assert all(unit["editable"] for unit in chunk["primary_units"])
        assert all(not unit["editable"] for unit in chunk["context_units_before"])
        assert all(not unit["editable"] for unit in chunk["context_units_after"])

        source_span = chunk["metadata"]["source_span"]
        assert source_span["primary_unit_count"] == len(chunk["primary_units"])
        assert source_span["context_before_count"] == len(chunk["context_units_before"])
        assert source_span["context_after_count"] == len(chunk["context_units_after"])

        token_estimates = chunk["metadata"]["token_estimates"]
        total_tokens = token_estimates["total_tokens"]
        hard_max_tokens = token_estimates["hard_max_tokens"]
        allowed_overflow = token_estimates["allowed_overflow"]
        assert token_estimates["is_within_hard_max"] == (total_tokens <= hard_max_tokens)
        assert (
            token_estimates["primary_tokens"]
            + token_estimates["context_before_tokens"]
            + token_estimates["context_after_tokens"]
            == total_tokens
        )

        chunk_primary_targets = {_target_tuple(unit) for unit in chunk["primary_units"]}
        chunk_context_before_targets = {_target_tuple(unit) for unit in chunk["context_units_before"]}
        chunk_context_after_targets = {_target_tuple(unit) for unit in chunk["context_units_after"]}

        assert chunk_primary_targets == {_target_tuple(item) for item in manifest_entry["primary_targets"]}
        assert chunk_context_before_targets == {
            _target_tuple(item) for item in manifest_entry["context_targets_before"]
        }
        assert chunk_context_after_targets == {
            _target_tuple(item) for item in manifest_entry["context_targets_after"]
        }
        assert {target[2] for target in chunk_primary_targets} == set(manifest_entry["primary_unit_uids"])
        assert {target[2] for target in chunk_context_before_targets} == set(manifest_entry["context_before_unit_uids"])
        assert {target[2] for target in chunk_context_after_targets} == set(manifest_entry["context_after_unit_uids"])

        if total_tokens > hard_max_tokens:
            assert allowed_overflow, f"{chunk_path.name} exceeded hard max without controlled overflow"
            assert len(chunk["primary_units"]) == 1
            assert token_estimates["primary_tokens"] > hard_max_tokens
            assert chunk["context_units_before"] == []
            assert chunk["context_units_after"] == []
            assert token_estimates["context_before_tokens"] == 0
            assert token_estimates["context_after_tokens"] == 0
            saw_allowed_overflow = True
        else:
            assert not allowed_overflow, f"{chunk_path.name} incorrectly marked overflow"

    assert saw_allowed_overflow, "Expected at least one controlled overflow chunk for oversized unit"
