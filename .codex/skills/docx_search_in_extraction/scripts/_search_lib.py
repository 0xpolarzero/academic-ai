#!/usr/bin/env python3
"""Search helpers for extracted DOCX review-unit artifacts."""

from __future__ import annotations

import json
from pathlib import Path
import re
from typing import Any

DEFAULT_REVIEW_UNITS_PATH = Path("artifacts/docx_extract/review_units.json")
DEFAULT_OUTPUT_DIR = Path("artifacts/search")
DEFAULT_SNIPPET_CHARS = 40
SCHEMA_VERSION = "search_results.v1"


def _to_int(value: Any, fallback: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def dump_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _compile_pattern(*, query: str, regex_mode: bool, case_sensitive: bool) -> re.Pattern[str]:
    if not query:
        raise ValueError("Query must be non-empty.")

    flags = 0 if case_sensitive else re.IGNORECASE
    expression = query if regex_mode else re.escape(query)

    try:
        pattern = re.compile(expression, flags)
    except re.error as exc:
        raise ValueError(f"Invalid regex query: {exc}") from exc

    if regex_mode and pattern.match("") is not None:
        raise ValueError("Regex query must not match empty strings.")

    return pattern


def _utf16_offsets(text: str) -> list[int]:
    offsets = [0]
    total = 0
    for char in text:
        total += len(char.encode("utf-16-le")) // 2
        offsets.append(total)
    return offsets


def _unit_order_key(item: tuple[int, dict[str, Any]]) -> tuple[int, int, str, str, str, int]:
    index, unit = item
    location = unit.get("location")
    if not isinstance(location, dict):
        location = {}

    order_index = _to_int(unit.get("order_index"), 10**9)
    global_order_index = _to_int(location.get("global_order_index"), order_index)

    return (
        order_index,
        global_order_index,
        str(unit.get("part", "")),
        str(unit.get("para_id", "")),
        str(unit.get("unit_uid", "")),
        index,
    )


def _ordered_units(review_units_payload: dict[str, Any]) -> list[dict[str, Any]]:
    units = review_units_payload.get("units", [])
    if not isinstance(units, list):
        raise ValueError("review_units.json must include a list at key 'units'.")

    indexed_units: list[tuple[int, dict[str, Any]]] = []
    for index, unit in enumerate(units):
        if isinstance(unit, dict):
            indexed_units.append((index, unit))
        else:
            indexed_units.append((index, {}))

    indexed_units.sort(key=_unit_order_key)
    return [unit for _, unit in indexed_units]


def search_review_units_payload(
    *,
    review_units_payload: dict[str, Any],
    source_path: Path,
    query: str,
    regex_mode: bool = False,
    case_sensitive: bool = True,
    snippet_chars: int = DEFAULT_SNIPPET_CHARS,
) -> dict[str, Any]:
    if snippet_chars < 0:
        raise ValueError("snippet_chars must be >= 0.")

    pattern = _compile_pattern(query=query, regex_mode=regex_mode, case_sensitive=case_sensitive)
    ordered_units = _ordered_units(review_units_payload)

    hits: list[dict[str, Any]] = []
    for unit in ordered_units:
        accepted_text = str(unit.get("accepted_text", ""))
        offsets = _utf16_offsets(accepted_text)

        for match in pattern.finditer(accepted_text):
            start_cp, end_cp = match.span()
            if start_cp == end_cp:
                continue

            snippet_start_cp = max(0, start_cp - snippet_chars)
            snippet_end_cp = min(len(accepted_text), end_cp + snippet_chars)

            hit = {
                "part": unit.get("part"),
                "para_id": unit.get("para_id"),
                "unit_uid": unit.get("unit_uid"),
                "order_index": _to_int(unit.get("order_index"), 10**9),
                "start": offsets[start_cp],
                "end": offsets[end_cp],
                "match_text": accepted_text[start_cp:end_cp],
                "snippet_start": offsets[snippet_start_cp],
                "snippet_end": offsets[snippet_end_cp],
                "snippet": accepted_text[snippet_start_cp:snippet_end_cp],
            }
            hits.append(hit)

    # Count unique hit-bearing units using the complete unit identity.
    hit_unit_count = len({(hit["part"], hit["para_id"], hit["unit_uid"]) for hit in hits})

    return {
        "schema_version": SCHEMA_VERSION,
        "source_review_units": str(source_path),
        "query": {
            "value": query,
            "mode": "regex" if regex_mode else "literal",
            "regex": regex_mode,
            "case_sensitive": case_sensitive,
            "snippet_chars": snippet_chars,
        },
        "unit_count": len(ordered_units),
        "hit_count": len(hits),
        "hit_unit_count": hit_unit_count,
        "hits": hits,
    }


def search_extracted_to_artifacts(
    *,
    review_units_path: Path,
    output_dir: Path,
    query: str,
    regex_mode: bool = False,
    case_sensitive: bool = True,
    snippet_chars: int = DEFAULT_SNIPPET_CHARS,
) -> dict[str, Any]:
    review_units_payload = load_json(review_units_path)
    results_payload = search_review_units_payload(
        review_units_payload=review_units_payload,
        source_path=review_units_path,
        query=query,
        regex_mode=regex_mode,
        case_sensitive=case_sensitive,
        snippet_chars=snippet_chars,
    )

    output_path = output_dir / "search_results.json"
    dump_json(output_path, results_payload)

    return {
        "output_path": output_path,
        "hit_count": results_payload["hit_count"],
        "hit_unit_count": results_payload["hit_unit_count"],
        "unit_count": results_payload["unit_count"],
    }
