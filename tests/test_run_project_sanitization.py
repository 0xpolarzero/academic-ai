from __future__ import annotations

from pathlib import Path
import importlib.util
import sys
from types import SimpleNamespace


REPO_ROOT = Path(__file__).resolve().parents[1]
RUN_PROJECT_PATH = REPO_ROOT / "scripts" / "run_project.py"


def _load_run_project_module():
    spec = importlib.util.spec_from_file_location("run_project", RUN_PROJECT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_sanitizer_drops_invalid_ops_instead_of_placeholder_comments():
    run_project = _load_run_project_module()

    chunk_payload = {
        "primary_units": [
            {
                "part": "word/document.xml",
                "para_id": "para_1",
                "unit_uid": "unit_1",
            }
        ]
    }

    raw_payload = {
        "ops": [
            {
                "type": "replace_range",
                "target": {
                    "part": "word/document.xml",
                    "para_id": "para_1",
                    "unit_uid": "unit_1",
                },
                "range": {"start": 1, "end": 3},
                "expected": {"snippet": "abc"},
            },
            {
                "type": "add_comment",
                "target": {
                    "part": "word/document.xml",
                    "para_id": "para_1",
                    "unit_uid": "unit_1",
                },
                "range": {"start": 1, "end": 1},
                "expected": {"snippet": "abc"},
                "comment_text": "",
            },
        ]
    }

    sanitized, log = run_project._sanitize_chunk_result_ops(
        chunk_id="chunk_0001",
        raw_payload=raw_payload,
        chunk_payload=chunk_payload,
    )

    assert sanitized["ops"] == []
    assert log["converted_ops"] == 2
    assert [item["reason"] for item in log["conversions"]] == [
        "missing_replacement",
        "missing_comment_text",
    ]


def test_sanitizer_keeps_valid_comment_and_logs_metadata_for_dropped_ops():
    run_project = _load_run_project_module()

    chunk_payload = {
        "primary_units": [
            {
                "part": "word/document.xml",
                "para_id": "para_1",
                "unit_uid": "unit_1",
            }
        ]
    }

    raw_payload = {
        "ops": [
            {
                "type": "add_comment",
                "target": {
                    "part": "word/document.xml",
                    "para_id": "para_1",
                    "unit_uid": "unit_1",
                },
                "range": {"start": 0, "end": 0},
                "expected": {"snippet": "abc"},
                "comment_text": "Real reviewer comment.",
            },
            {
                "type": "unknown_type",
                "target": {
                    "part": "word/document.xml",
                    "para_id": "para_1",
                    "unit_uid": "unit_1",
                },
                "range": {"start": 0, "end": 0},
                "expected": {"snippet": "abc"},
            },
        ]
    }

    sanitized, log = run_project._sanitize_chunk_result_ops(
        chunk_id="chunk_0001",
        raw_payload=raw_payload,
        chunk_payload=chunk_payload,
    )

    assert len(sanitized["ops"]) == 1
    assert sanitized["ops"][0]["type"] == "add_comment"
    assert sanitized["ops"][0]["comment_text"] == "Real reviewer comment."

    assert log["converted_ops"] == 1
    conversion = log["conversions"][0]
    assert conversion["reason"] == "invalid_type"
    assert conversion["target"] == {
        "part": "word/document.xml",
        "para_id": "para_1",
        "unit_uid": "unit_1",
    }


def test_strict_gate_raises_when_any_chunk_ops_were_converted():
    run_project = _load_run_project_module()

    fake_paths = SimpleNamespace(
        chunk_result_sanitization_log=Path("/tmp/sanitization_report.json"),
    )
    review_summary = {
        "total_converted_ops": 4,
        "chunks": [
            {"chunk_id": "chunk_0002", "converted_ops": 3},
            {"chunk_id": "chunk_0003", "converted_ops": 1},
            {"chunk_id": "chunk_0004", "converted_ops": 0},
        ],
    }

    try:
        run_project._enforce_no_sanitized_chunk_ops(fake_paths, review_summary)
        assert False, "expected RuntimeError"
    except RuntimeError as exc:
        message = str(exc)
        assert "converted_ops=4" in message
        assert "chunk_0002 (3)" in message
        assert "chunk_0003 (1)" in message
        assert "/tmp/sanitization_report.json" in message


def test_strict_gate_allows_clean_review_summary():
    run_project = _load_run_project_module()

    fake_paths = SimpleNamespace(
        chunk_result_sanitization_log=Path("/tmp/sanitization_report.json"),
    )
    review_summary = {
        "total_converted_ops": 0,
        "chunks": [{"chunk_id": "chunk_0001", "converted_ops": 0}],
    }

    run_project._enforce_no_sanitized_chunk_ops(fake_paths, review_summary)


def test_sanitizer_converts_empty_replacement_to_delete_range_and_rejects_empty_new_text():
    run_project = _load_run_project_module()

    chunk_payload = {
        "primary_units": [
            {
                "part": "word/document.xml",
                "para_id": "para_1",
                "unit_uid": "unit_1",
            }
        ]
    }

    raw_payload = {
        "ops": [
            {
                "type": "replace_range",
                "target": {
                    "part": "word/document.xml",
                    "para_id": "para_1",
                    "unit_uid": "unit_1",
                },
                "range": {"start": 1, "end": 3},
                "expected": {"snippet": "abc"},
                "replacement": "",
            },
            {
                "type": "insert_at",
                "target": {
                    "part": "word/document.xml",
                    "para_id": "para_1",
                    "unit_uid": "unit_1",
                },
                "range": {"start": 3, "end": 3},
                "expected": {"snippet": ""},
                "new_text": "",
            },
        ]
    }

    sanitized, log = run_project._sanitize_chunk_result_ops(
        chunk_id="chunk_0001",
        raw_payload=raw_payload,
        chunk_payload=chunk_payload,
    )

    # Empty replacement is converted to delete_range (real deletion op)
    assert len(sanitized["ops"]) == 1
    assert sanitized["ops"][0]["type"] == "delete_range"
    assert "replacement" not in sanitized["ops"][0]
    assert [item["reason"] for item in log["conversions"]] == [
        "empty_replacement_to_delete_range",
        "empty_new_text",
    ]
