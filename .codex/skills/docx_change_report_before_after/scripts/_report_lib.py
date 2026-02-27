#!/usr/bin/env python3
"""Build before/after change reports from patch + apply artifacts."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

DEFAULT_REVIEW_UNITS_PATH = Path("artifacts/docx_extract/review_units.json")
DEFAULT_PATCH_PATH = Path("artifacts/patch/merged_patch.json")
DEFAULT_APPLY_LOG_PATH = Path("artifacts/apply/apply_log.json")
DEFAULT_OUTPUT_MD_PATH = Path("output/changes.md")
DEFAULT_OUTPUT_JSON_PATH = Path("output/changes.json")

CHANGE_REPORT_SCHEMA_VERSION = "change_report.v1"


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _to_int(value: Any, fallback: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def dump_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def dump_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _normalize_target(raw_target: Any) -> dict[str, str]:
    if not isinstance(raw_target, dict):
        return {"part": "", "para_id": "", "unit_uid": ""}

    return {
        "part": str(raw_target.get("part", "")).strip(),
        "para_id": str(raw_target.get("para_id", "")).strip(),
        "unit_uid": str(raw_target.get("unit_uid", "")).strip(),
    }


def _unit_order_key(indexed_unit: tuple[int, dict[str, Any]]) -> tuple[int, int, str, str, str, int]:
    index, unit = indexed_unit
    location = unit.get("location") if isinstance(unit.get("location"), dict) else {}
    order_index = _to_int(unit.get("order_index"), 10**9)
    global_index = _to_int(location.get("global_order_index"), order_index)
    return (
        order_index,
        global_index,
        str(unit.get("part", "")),
        str(unit.get("para_id", "")),
        str(unit.get("unit_uid", "")),
        index,
    )


def _build_unit_maps(
    review_units_payload: dict[str, Any],
) -> tuple[dict[tuple[str, str, str], dict[str, Any]], dict[tuple[str, str], list[dict[str, Any]]]]:
    raw_units = review_units_payload.get("units", [])
    if not isinstance(raw_units, list):
        raise ValueError("review_units.json must include a list at key 'units'.")

    indexed_units: list[tuple[int, dict[str, Any]]] = []
    for index, unit in enumerate(raw_units):
        if isinstance(unit, dict):
            indexed_units.append((index, unit))

    indexed_units.sort(key=_unit_order_key)
    ordered_units = [unit for _, unit in indexed_units]

    exact_map: dict[tuple[str, str, str], dict[str, Any]] = {}
    by_para: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)

    for unit in ordered_units:
        part = str(unit.get("part", "")).strip()
        para_id = str(unit.get("para_id", "")).strip()
        unit_uid = str(unit.get("unit_uid", "")).strip()
        if not part or not para_id:
            continue

        by_para[(part, para_id)].append(unit)
        if unit_uid:
            exact_map[(part, para_id, unit_uid)] = unit

    return exact_map, by_para


def _apply_log_index_map(apply_log_payload: dict[str, Any]) -> dict[int, dict[str, Any]]:
    by_index: dict[int, dict[str, Any]] = {}
    raw_entries = apply_log_payload.get("ops", [])
    if not isinstance(raw_entries, list):
        return by_index

    for entry in raw_entries:
        if not isinstance(entry, dict):
            continue
        op_index = _to_int(entry.get("op_index"), -1)
        if op_index < 0:
            continue
        if op_index not in by_index:
            by_index[op_index] = entry

    return by_index


def _first_non_empty(*values: str) -> str:
    for value in values:
        if value:
            return value
    return ""


def _resolve_unit(
    *,
    op_target: dict[str, str],
    resolved_target: dict[str, str],
    exact_map: dict[tuple[str, str, str], dict[str, Any]],
    by_para: dict[tuple[str, str], list[dict[str, Any]]],
) -> dict[str, Any] | None:
    candidates: list[tuple[str, str, str]] = [
        (
            resolved_target.get("part", ""),
            resolved_target.get("para_id", ""),
            resolved_target.get("unit_uid", ""),
        ),
        (
            op_target.get("part", ""),
            op_target.get("para_id", ""),
            op_target.get("unit_uid", ""),
        ),
    ]

    for part, para_id, unit_uid in candidates:
        if part and para_id and unit_uid:
            unit = exact_map.get((part, para_id, unit_uid))
            if unit is not None:
                return unit

    fallback_pairs = [
        (resolved_target.get("part", ""), resolved_target.get("para_id", "")),
        (op_target.get("part", ""), op_target.get("para_id", "")),
    ]
    for part, para_id in fallback_pairs:
        if part and para_id:
            units = by_para.get((part, para_id), [])
            if units:
                return units[0]

    return None


def _comment_text_from_op(op: dict[str, Any]) -> str:
    if "comment_text" in op and op.get("comment_text") is not None:
        return str(op.get("comment_text"))

    comment = op.get("comment")
    if isinstance(comment, dict) and comment.get("text") is not None:
        return str(comment.get("text"))

    return ""


def _expected_snippet(op: dict[str, Any]) -> str:
    expected = op.get("expected")
    if isinstance(expected, dict):
        return str(expected.get("snippet", ""))
    return ""


def _before_snippet(op: dict[str, Any], apply_entry: dict[str, Any]) -> str:
    actual = apply_entry.get("actual_snippet")
    if actual is not None:
        return str(actual)
    return _expected_snippet(op)


def _after_snippet(op: dict[str, Any], op_type: str) -> str:
    if op_type == "replace_range":
        return str(op.get("replacement", ""))
    if op_type == "insert_at":
        if op.get("new_text") is not None:
            return str(op.get("new_text"))
        if op.get("text") is not None:
            return str(op.get("text"))
        return ""
    if op_type == "delete_range":
        return ""
    if op_type == "add_comment":
        return _comment_text_from_op(op)
    return ""


def _utf16_offsets(text: str) -> list[int]:
    offsets = [0]
    total = 0
    for char in text:
        total += len(char.encode("utf-16-le")) // 2
        offsets.append(total)
    return offsets


def _find_occurrences(text: str, needle: str) -> list[tuple[int, int]]:
    if not needle:
        return []

    spans: list[tuple[int, int]] = []
    cursor = 0
    while True:
        start = text.find(needle, cursor)
        if start < 0:
            break
        end = start + len(needle)
        spans.append((start, end))
        cursor = start + 1
    return spans


def _disambiguation(
    *,
    before_snippet: str,
    accepted_text: str,
    range_start_u16: int,
    range_end_u16: int,
) -> dict[str, Any] | None:
    if not before_snippet:
        return None

    spans_cp = _find_occurrences(accepted_text, before_snippet)
    if len(spans_cp) <= 1:
        return None

    offsets = _utf16_offsets(accepted_text)
    cp_by_u16 = {offset: cp_index for cp_index, offset in enumerate(offsets)}

    selected_index: int | None = None
    if range_start_u16 in cp_by_u16 and range_end_u16 in cp_by_u16:
        start_cp = cp_by_u16[range_start_u16]
        end_cp = cp_by_u16[range_end_u16]
        for index, (span_start_cp, span_end_cp) in enumerate(spans_cp, start=1):
            if span_start_cp == start_cp and span_end_cp == end_cp:
                selected_index = index
                break

    return {
        "kind": "repeated_before_snippet",
        "occurrence_count": len(spans_cp),
        "occurrence_index": selected_index,
        "match_start_offsets": [offsets[start_cp] for start_cp, _ in spans_cp],
        "range": {
            "start": range_start_u16,
            "end": range_end_u16,
        },
    }


def _markdown_status(change: dict[str, Any]) -> str:
    status = str(change.get("apply", {}).get("status", "unknown"))
    reason = change.get("apply", {}).get("reason")
    if reason:
        return f"{status} ({reason})"
    return status


def _stable_location_string(location: dict[str, Any]) -> str:
    heading_path = location.get("heading_path", [])
    heading_label = " > ".join(str(item) for item in heading_path if str(item).strip()) or "(no heading)"
    unit_uid = str(location.get("unit_uid", "")).strip() or "unknown_unit_uid"
    return " | ".join(
        [
            heading_label,
            str(location.get("part", "")).strip(),
            str(location.get("para_id", "")).strip(),
            unit_uid,
        ]
    )


def build_change_report_payload(
    *,
    review_units_payload: dict[str, Any],
    patch_payload: dict[str, Any],
    apply_log_payload: dict[str, Any],
    source_review_units: Path,
    source_patch: Path,
    source_apply_log: Path,
) -> dict[str, Any]:
    exact_map, by_para = _build_unit_maps(review_units_payload)
    apply_by_index = _apply_log_index_map(apply_log_payload)

    raw_ops = patch_payload.get("ops", [])
    if not isinstance(raw_ops, list):
        raise ValueError("merged_patch.json must include a list at key 'ops'.")

    changes: list[dict[str, Any]] = []

    for op_index, raw_op in enumerate(raw_ops):
        op = raw_op if isinstance(raw_op, dict) else {}
        op_type = str(op.get("type", "")).strip()

        op_target = _normalize_target(op.get("target"))
        apply_entry = apply_by_index.get(op_index, {})
        resolved_target = _normalize_target(apply_entry.get("resolved_target"))
        unit = _resolve_unit(
            op_target=op_target,
            resolved_target=resolved_target,
            exact_map=exact_map,
            by_para=by_para,
        )

        unit_part = str(unit.get("part", "")).strip() if unit else ""
        unit_para = str(unit.get("para_id", "")).strip() if unit else ""
        unit_uid = str(unit.get("unit_uid", "")).strip() if unit else ""

        location = {
            "heading_path": list(unit.get("heading_path", [])) if isinstance(unit, dict) else [],
            "part": _first_non_empty(resolved_target.get("part", ""), op_target.get("part", ""), unit_part),
            "para_id": _first_non_empty(resolved_target.get("para_id", ""), op_target.get("para_id", ""), unit_para),
            "unit_uid": _first_non_empty(resolved_target.get("unit_uid", ""), op_target.get("unit_uid", ""), unit_uid),
        }

        range_raw = op.get("range") if isinstance(op.get("range"), dict) else {}
        range_start_u16 = _to_int(range_raw.get("start"), 0)
        range_end_u16 = _to_int(range_raw.get("end"), range_start_u16)

        before = _before_snippet(op, apply_entry)
        after = _after_snippet(op, op_type)
        accepted_text = str(unit.get("accepted_text", "")) if unit else ""
        disambiguation = _disambiguation(
            before_snippet=before,
            accepted_text=accepted_text,
            range_start_u16=range_start_u16,
            range_end_u16=range_end_u16,
        )

        status = str(apply_entry.get("status", "unknown")) if isinstance(apply_entry, dict) else "unknown"
        reason = apply_entry.get("reason") if isinstance(apply_entry, dict) else None

        annotation = _comment_text_from_op(op) if op_type == "add_comment" else None

        change: dict[str, Any] = {
            "op_index": op_index,
            "type": op_type,
            "location": location,
            "stable_location": _stable_location_string(location),
            "range": {
                "start": range_start_u16,
                "end": range_end_u16,
            },
            "before_snippet": before,
            "after_snippet": after,
            "annotation": annotation,
            "apply": {
                "status": status,
                "reason": reason,
            },
        }
        if disambiguation is not None:
            change["disambiguation"] = disambiguation

        changes.append(change)

    stats = {
        "op_count": len(changes),
        "applied": sum(1 for change in changes if change.get("apply", {}).get("status") == "applied"),
        "skipped": sum(1 for change in changes if change.get("apply", {}).get("status") == "skipped"),
        "unknown": sum(1 for change in changes if change.get("apply", {}).get("status") == "unknown"),
    }

    return {
        "schema_version": CHANGE_REPORT_SCHEMA_VERSION,
        "created_at": _now_iso(),
        "source_review_units": str(source_review_units),
        "source_patch": str(source_patch),
        "source_apply_log": str(source_apply_log),
        "stats": stats,
        "changes": changes,
    }


def render_changes_markdown(payload: dict[str, Any]) -> str:
    changes = payload.get("changes", [])
    if not isinstance(changes, list):
        changes = []

    lines = [
        "# Change Report",
        "",
        f"- source_review_units: `{payload.get('source_review_units', '')}`",
        f"- source_patch: `{payload.get('source_patch', '')}`",
        f"- source_apply_log: `{payload.get('source_apply_log', '')}`",
        f"- op_count: {payload.get('stats', {}).get('op_count', len(changes))}",
        "",
    ]

    for change in changes:
        op_index = _to_int(change.get("op_index"), 0)
        op_type = str(change.get("type", ""))
        location = str(change.get("stable_location", ""))

        lines.append(f"## Op {op_index}: {op_type}")
        lines.append(f"- location: `{location}`")
        lines.append(f"- apply: `{_markdown_status(change)}`")

        before = str(change.get("before_snippet", ""))
        after = str(change.get("after_snippet", ""))
        lines.append("- before:")
        lines.append("```text")
        lines.append(before)
        lines.append("```")
        lines.append("- after:")
        lines.append("```text")
        lines.append(after)
        lines.append("```")

        annotation = change.get("annotation")
        if annotation:
            lines.append("- annotation:")
            lines.append("```text")
            lines.append(str(annotation))
            lines.append("```")

        disambiguation = change.get("disambiguation")
        if isinstance(disambiguation, dict):
            occurrence_index = disambiguation.get("occurrence_index")
            occurrence_count = disambiguation.get("occurrence_count")
            range_info = disambiguation.get("range", {})
            lines.append(
                "- disambiguation: "
                f"repeated before snippet ({occurrence_index}/{occurrence_count}) "
                f"at UTF-16 range [{range_info.get('start')}, {range_info.get('end')}]"
            )

        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def build_change_report_artifacts(
    *,
    review_units_path: Path = DEFAULT_REVIEW_UNITS_PATH,
    patch_path: Path = DEFAULT_PATCH_PATH,
    apply_log_path: Path = DEFAULT_APPLY_LOG_PATH,
    output_md_path: Path = DEFAULT_OUTPUT_MD_PATH,
    output_json_path: Path = DEFAULT_OUTPUT_JSON_PATH,
) -> dict[str, Any]:
    review_units_payload = load_json(review_units_path)
    patch_payload = load_json(patch_path)
    apply_log_payload = load_json(apply_log_path)

    payload = build_change_report_payload(
        review_units_payload=review_units_payload,
        patch_payload=patch_payload,
        apply_log_payload=apply_log_payload,
        source_review_units=review_units_path,
        source_patch=patch_path,
        source_apply_log=apply_log_path,
    )
    markdown = render_changes_markdown(payload)

    dump_json(output_json_path, payload)
    dump_text(output_md_path, markdown)

    return {
        "output_json": output_json_path,
        "output_md": output_md_path,
        "stats": payload["stats"],
    }
