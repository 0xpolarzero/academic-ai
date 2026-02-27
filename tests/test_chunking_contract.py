from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
CHUNK_SCRIPT = REPO_ROOT / ".codex/skills/docx_chunk_atomic_manifest/scripts/chunk_docx.py"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _build_synthetic_extraction(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    extract_dir = tmp_path / "docx_extract"

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

    constants_path = tmp_path / "constants.json"
    _write_json(
        constants_path,
        {
            "chunking": {
                "paths": {
                    "review_units": str(review_units_path),
                    "linear_units": str(linear_units_path),
                    "docx_struct": str(docx_struct_path),
                    "output_dir": str(tmp_path / "chunks"),
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
    review_units_path, linear_units_path, docx_struct_path, constants_path = _build_synthetic_extraction(tmp_path)
    output_dir = tmp_path / "chunks"

    subprocess.run(
        [
            sys.executable,
            str(CHUNK_SCRIPT),
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
        "token_budget",
        "unit_count",
        "chunk_count",
        "chunks",
    }
    assert required_manifest_keys.issubset(manifest.keys()), "manifest.json missing required keys"

    chunk_paths = sorted(output_dir.glob("chunk_*.json"))
    assert chunk_paths, "No chunk_XXXX.json files were generated"
    assert len(chunk_paths) == manifest["chunk_count"], "Manifest chunk_count mismatch"

    expected_manifest_paths = {entry["path"] for entry in manifest["chunks"]}
    actual_chunk_paths = {path.name for path in chunk_paths}
    assert expected_manifest_paths == actual_chunk_paths, "Manifest chunk file listing mismatch"

    saw_allowed_overflow = False
    for chunk_path in chunk_paths:
        chunk = json.loads(chunk_path.read_text(encoding="utf-8"))

        required_chunk_keys = {
            "schema_version",
            "chunk_id",
            "contract",
            "primary_units",
            "context_units_before",
            "context_units_after",
            "metadata",
        }
        assert required_chunk_keys.issubset(chunk.keys()), f"{chunk_path.name} missing required keys"

        assert chunk["primary_units"], f"{chunk_path.name} has no primary_units"
        assert chunk["contract"]["context_units_editable"] is False
        assert chunk["contract"]["context_is_read_only"] is True

        primary_ids = {unit["unit_uid"] for unit in chunk["primary_units"]}
        context_ids = {
            unit["unit_uid"]
            for unit in chunk["context_units_before"] + chunk["context_units_after"]
        }
        assert primary_ids.isdisjoint(context_ids), f"{chunk_path.name} context overlaps primary units"

        assert all(unit["editable"] for unit in chunk["primary_units"])
        assert all(not unit["editable"] for unit in chunk["context_units_before"])
        assert all(not unit["editable"] for unit in chunk["context_units_after"])

        token_estimates = chunk["metadata"]["token_estimates"]
        total_tokens = token_estimates["total_tokens"]
        hard_max_tokens = token_estimates["hard_max_tokens"]
        allowed_overflow = token_estimates["allowed_overflow"]

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
