from __future__ import annotations

import json
from pathlib import Path
import re
import subprocess
import sys
from typing import Any

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
SEARCH_SCRIPT = REPO_ROOT / ".codex/skills/docx_search_in_extraction/scripts/search_extracted.py"
EXTRACT_SCRIPT = REPO_ROOT / ".codex/skills/docx_extract_ooxml_to_artifacts/scripts/extract_docx.py"
FIXTURES_DIR = REPO_ROOT / "fixtures"
DEFAULT_REVIEW_UNITS = REPO_ROOT / "artifacts/docx_extract/review_units.json"


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


def _utf16_to_cp_index(text: str, utf16_offset: int) -> int:
    offsets = _utf16_offsets(text)
    for index, value in enumerate(offsets):
        if value == utf16_offset:
            return index
    raise AssertionError(f"Offset {utf16_offset} is not a valid UTF-16 boundary for text: {text!r}")


def _utf16_slice(text: str, start: int, end: int) -> str:
    start_cp = _utf16_to_cp_index(text, start)
    end_cp = _utf16_to_cp_index(text, end)
    return text[start_cp:end_cp]


def _run_search(
    *,
    review_units_path: Path,
    output_dir: Path,
    query: str,
    regex_mode: bool = False,
    ignore_case: bool = False,
    snippet_chars: int = 12,
) -> dict[str, Any]:
    cmd = [
        sys.executable,
        str(SEARCH_SCRIPT),
        "--review-units",
        str(review_units_path),
        "--output-dir",
        str(output_dir),
        "--query",
        query,
        "--snippet-chars",
        str(snippet_chars),
    ]
    if regex_mode:
        cmd.append("--regex")
    if ignore_case:
        cmd.append("--ignore-case")

    subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    output_path = output_dir / "search_results.json"
    assert output_path.exists(), "Missing search_results.json"
    return json.loads(output_path.read_text(encoding="utf-8"))


def _assert_hit_consistency(
    results: dict[str, Any], units_by_key: dict[tuple[str, str, str], dict[str, Any]]
) -> None:
    hits = results.get("hits", [])
    assert results["hit_count"] == len(hits), "hit_count does not match hits length"
    assert results["hit_unit_count"] == len(
        {(hit["part"], hit["para_id"], hit["unit_uid"]) for hit in hits}
    ), "hit_unit_count mismatch"

    for hit in hits:
        assert {"part", "para_id", "unit_uid", "start", "end", "snippet_start", "snippet_end", "snippet"} <= set(
            hit.keys()
        ), "Hit payload missing required contract keys"

        key = (str(hit["part"]), str(hit["para_id"]), str(hit["unit_uid"]))
        assert key in units_by_key, f"Hit references unknown unit key: {key}"
        unit = units_by_key[key]
        assert hit["part"] == unit["part"], "Hit part does not match originating unit"
        accepted_text = str(unit.get("accepted_text", ""))

        start = int(hit["start"])
        end = int(hit["end"])
        snippet_start = int(hit["snippet_start"])
        snippet_end = int(hit["snippet_end"])

        text_len_utf16 = _utf16_offsets(accepted_text)[-1]
        assert 0 <= start <= end <= text_len_utf16, "Hit offsets out of range"
        assert 0 <= snippet_start <= start <= end <= snippet_end <= text_len_utf16, "Snippet offsets out of range"

        expected_match = _utf16_slice(accepted_text, start, end)
        expected_snippet = _utf16_slice(accepted_text, snippet_start, snippet_end)

        assert hit["match_text"] == expected_match, "match_text does not match accepted_text slice"
        assert hit["snippet"] == expected_snippet, "snippet does not match snippet offsets"
        assert expected_match in expected_snippet, "snippet does not contain matched text"


def _build_synthetic_review_units(tmp_path: Path) -> Path:
    units = [
        {
            "part": "word/document.xml",
            "part_kind": "body",
            "part_name": "document",
            "para_id": "para_0000000000000001",
            "unit_uid": "unit_000000000001",
            "accepted_text": "😀 Alpha beta ALPHA Beta alpha.",
            "heading_path": [],
            "order_index": 0,
            "location": {"global_order_index": 0},
        },
        {
            "part": "word/document.xml",
            "part_kind": "body",
            "part_name": "document",
            "para_id": "para_0000000000000002",
            "unit_uid": "unit_000000000002",
            "accepted_text": "gamma alpha beta.",
            "heading_path": [],
            "order_index": 1,
            "location": {"global_order_index": 1},
        },
    ]

    payload = {
        "source_docx": "synthetic.docx",
        "part_count": 1,
        "unit_count": len(units),
        "units": units,
    }
    path = tmp_path / "docx_extract/review_units.json"
    _write_json(path, payload)
    return path


def _literal_spans_utf16(text: str, query: str, *, ignore_case: bool) -> list[tuple[int, int]]:
    flags = re.IGNORECASE if ignore_case else 0
    offsets = _utf16_offsets(text)
    spans: list[tuple[int, int]] = []
    for match in re.finditer(re.escape(query), text, flags):
        if match.start() == match.end():
            continue
        spans.append((offsets[match.start()], offsets[match.end()]))
    return spans


def _regex_spans_utf16(text: str, query: str, *, ignore_case: bool) -> list[tuple[int, int]]:
    flags = re.IGNORECASE if ignore_case else 0
    pattern = re.compile(query, flags)
    offsets = _utf16_offsets(text)
    spans: list[tuple[int, int]] = []
    for match in pattern.finditer(text):
        if match.start() == match.end():
            continue
        spans.append((offsets[match.start()], offsets[match.end()]))
    return spans


def _load_or_generate_extraction(tmp_path: Path) -> Path | None:
    if DEFAULT_REVIEW_UNITS.exists():
        return DEFAULT_REVIEW_UNITS

    fixture_docx = next(iter(sorted(FIXTURES_DIR.glob("*.docx"))), None)
    if fixture_docx is None:
        return None

    output_dir = tmp_path / "docx_extract_generated"
    subprocess.run(
        [
            sys.executable,
            str(EXTRACT_SCRIPT),
            "--input-docx",
            str(fixture_docx),
            "--output-dir",
            str(output_dir),
        ],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    generated_review_units = output_dir / "review_units.json"
    if not generated_review_units.exists():
        return None
    return generated_review_units


def _pick_query_from_units(units: list[dict[str, Any]]) -> str | None:
    for unit in units:
        text = str(unit.get("accepted_text", ""))
        match = re.search(r"[A-Za-z]{4,}", text)
        if match:
            return match.group(0)
    return None


def test_search_literal_regex_and_case_sensitivity(tmp_path: Path) -> None:
    review_units_path = _build_synthetic_review_units(tmp_path)
    payload = json.loads(review_units_path.read_text(encoding="utf-8"))
    units = payload["units"]
    units_by_key = {(unit["part"], unit["para_id"], unit["unit_uid"]): unit for unit in units}

    literal_results = _run_search(
        review_units_path=review_units_path,
        output_dir=tmp_path / "search_literal",
        query="alpha",
        snippet_chars=8,
    )

    assert literal_results["query"]["mode"] == "literal"
    assert literal_results["query"]["case_sensitive"] is True
    assert literal_results["unit_count"] == len(units)

    expected_literal = []
    for unit in units:
        spans = _literal_spans_utf16(unit["accepted_text"], "alpha", ignore_case=False)
        expected_literal.extend((unit["unit_uid"], start, end) for start, end in spans)
    observed_literal = [(hit["unit_uid"], hit["start"], hit["end"]) for hit in literal_results["hits"]]
    assert observed_literal == expected_literal, "Literal search hits mismatch"
    _assert_hit_consistency(literal_results, units_by_key)

    literal_ignore_case_results = _run_search(
        review_units_path=review_units_path,
        output_dir=tmp_path / "search_literal_ignore_case",
        query="alpha",
        ignore_case=True,
        snippet_chars=8,
    )
    assert literal_ignore_case_results["query"]["case_sensitive"] is False
    assert literal_ignore_case_results["hit_count"] > literal_results["hit_count"]
    _assert_hit_consistency(literal_ignore_case_results, units_by_key)

    regex_results = _run_search(
        review_units_path=review_units_path,
        output_dir=tmp_path / "search_regex",
        query=r"alpha|gamma",
        regex_mode=True,
        ignore_case=True,
        snippet_chars=8,
    )
    assert regex_results["query"]["mode"] == "regex"
    assert regex_results["query"]["regex"] is True

    expected_regex = []
    for unit in units:
        spans = _regex_spans_utf16(unit["accepted_text"], r"alpha|gamma", ignore_case=True)
        expected_regex.extend((unit["unit_uid"], start, end) for start, end in spans)
    observed_regex = [(hit["unit_uid"], hit["start"], hit["end"]) for hit in regex_results["hits"]]
    assert observed_regex == expected_regex, "Regex search hits mismatch"
    _assert_hit_consistency(regex_results, units_by_key)


def test_search_ordering_and_no_hits(tmp_path: Path) -> None:
    units = [
        {
            "part": "word/header1.xml",
            "part_kind": "header",
            "part_name": "header1",
            "para_id": "para_0000000000000010",
            "unit_uid": "unit_000000000010",
            "accepted_text": "third token here",
            "heading_path": [],
            "order_index": 2,
            "location": {"global_order_index": 2},
        },
        {
            "part": "word/document.xml",
            "part_kind": "body",
            "part_name": "document",
            "para_id": "para_0000000000000008",
            "unit_uid": "unit_000000000008",
            "accepted_text": "first token here",
            "heading_path": [],
            "order_index": 0,
            "location": {"global_order_index": 0},
        },
        {
            "part": "word/document.xml",
            "part_kind": "body",
            "part_name": "document",
            "para_id": "para_0000000000000009",
            "unit_uid": "unit_000000000009",
            "accepted_text": "second token here",
            "heading_path": [],
            "order_index": 1,
            "location": {"global_order_index": 1},
        },
    ]
    review_units_path = tmp_path / "docx_extract/review_units.json"
    _write_json(
        review_units_path,
        {
            "source_docx": "synthetic.docx",
            "part_count": 2,
            "unit_count": len(units),
            "units": units,
        },
    )
    units_by_key = {(unit["part"], unit["para_id"], unit["unit_uid"]): unit for unit in units}

    token_results = _run_search(
        review_units_path=review_units_path,
        output_dir=tmp_path / "search_token",
        query="token",
        snippet_chars=6,
    )
    observed_unit_order = [hit["unit_uid"] for hit in token_results["hits"]]
    assert observed_unit_order == [
        "unit_000000000008",
        "unit_000000000009",
        "unit_000000000010",
    ], "Hits are not emitted in deterministic unit order"
    _assert_hit_consistency(token_results, units_by_key)

    no_hit_results = _run_search(
        review_units_path=review_units_path,
        output_dir=tmp_path / "search_no_hits",
        query="definitely_not_present",
        snippet_chars=6,
    )
    assert no_hit_results["unit_count"] == len(units)
    assert no_hit_results["hit_count"] == 0
    assert no_hit_results["hit_unit_count"] == 0
    assert no_hit_results["hits"] == []


def test_search_finds_patterns_in_generated_or_fixture_extraction(tmp_path: Path) -> None:
    review_units_path = _load_or_generate_extraction(tmp_path)
    if review_units_path is None:
        pytest.skip("No extraction artifact found and no fixture DOCX available to generate one.")

    payload = json.loads(review_units_path.read_text(encoding="utf-8"))
    units = payload.get("units")
    assert isinstance(units, list), "review_units.json must contain a list at key 'units'"
    assert units, "review_units.json exists but contains no units"

    query = _pick_query_from_units(units)
    assert query is not None, "review_units.json exists but has no searchable accepted_text pattern"

    results = _run_search(
        review_units_path=review_units_path,
        output_dir=tmp_path / "search_integration",
        query=query,
        snippet_chars=10,
    )

    assert results["query"]["value"] == query
    assert results["query"]["mode"] == "literal"
    assert results["hit_count"] >= 1, "Expected at least one hit for selected extraction query"

    units_by_key = {(unit["part"], unit["para_id"], unit["unit_uid"]): unit for unit in units}
    _assert_hit_consistency(results, units_by_key)
