#!/usr/bin/env python3
"""Merge helpers for deduplicating and conflict-resolving chunk patch ops."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from typing import Any
import unicodedata

DEFAULT_CHUNK_RESULTS_DIR = Path("artifacts/chunk_results")
DEFAULT_OUTPUT_DIR = Path("artifacts/patch")
DEFAULT_LINEAR_UNITS_PATH = Path("artifacts/docx_extract/linear_units.json")
DEFAULT_AUTHOR = "docx_merge_dedup_validate_patch"

PATCH_SCHEMA_VERSION = "patch.v1"
MERGE_REPORT_SCHEMA_VERSION = "merge_report.v1"

VALID_OP_TYPES = {"add_comment", "replace_range", "insert_at", "delete_range"}
EDIT_OP_TYPES = {"replace_range", "insert_at", "delete_range"}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def dump_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _normalize_text(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or ""))
    return re.sub(r"\s+", " ", text).strip()


def _to_int(value: Any, *, field: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be an integer") from exc


def _normalize_target(raw_target: Any) -> dict[str, str]:
    if not isinstance(raw_target, dict):
        raise ValueError("target must be an object")

    part = str(raw_target.get("part", "")).strip()
    para_id = str(raw_target.get("para_id", "")).strip()
    unit_uid_raw = raw_target.get("unit_uid")
    unit_uid = str(unit_uid_raw).strip() if unit_uid_raw is not None else ""

    if not part:
        raise ValueError("target.part is required")
    if not para_id:
        raise ValueError("target.para_id is required")

    target = {"part": part, "para_id": para_id}
    if unit_uid:
        target["unit_uid"] = unit_uid
    return target


def _normalize_range(raw_range: Any, *, op_type: str) -> dict[str, int]:
    if not isinstance(raw_range, dict):
        raise ValueError("range must be an object")

    start = _to_int(raw_range.get("start"), field="range.start")
    end = _to_int(raw_range.get("end"), field="range.end")

    if start < 0 or end < 0:
        raise ValueError("range.start and range.end must be >= 0")
    if start > end:
        raise ValueError("range.start must be <= range.end")
    if op_type == "insert_at" and start != end:
        raise ValueError("insert_at requires range.start == range.end")

    return {"start": start, "end": end}


def _normalize_expected(raw_expected: Any) -> dict[str, str]:
    if not isinstance(raw_expected, dict):
        raw_expected = {}
    return {"snippet": str(raw_expected.get("snippet", ""))}


def _dedup_key(op: dict[str, Any]) -> str:
    payload = {
        "type": op["type"],
        "target": op["target"],
        "range": op["range"],
        "old": _normalize_text(op["expected"].get("snippet")),
        "new": _normalize_text(op.get("replacement") or op.get("new_text")),
        "comment": _normalize_text(op.get("comment_text")),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _target_key(op: dict[str, Any]) -> tuple[str, str, str]:
    target = op["target"]
    return (
        str(target.get("part", "")),
        str(target.get("para_id", "")),
        str(target.get("unit_uid", "")),
    )


def _target_para_key(op: dict[str, Any]) -> tuple[str, str]:
    target = op["target"]
    return (
        str(target.get("part", "")),
        str(target.get("para_id", "")),
    )


def _same_range(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return left["range"]["start"] == right["range"]["start"] and left["range"]["end"] == right["range"]["end"]


def _ranges_overlap_for_edits(left: dict[str, Any], right: dict[str, Any]) -> bool:
    left_start = left["range"]["start"]
    left_end = left["range"]["end"]
    right_start = right["range"]["start"]
    right_end = right["range"]["end"]

    left_point = left_start == left_end
    right_point = right_start == right_end

    if left_point and right_point:
        return left_start == right_start
    if left_point:
        return right_start <= left_start <= right_end
    if right_point:
        return left_start <= right_start <= left_end

    return max(left_start, right_start) < min(left_end, right_end)


def _conflict_reason(current: dict[str, Any], existing: dict[str, Any]) -> str | None:
    current_type = current["type"]
    existing_type = existing["type"]
    if current_type not in EDIT_OP_TYPES or existing_type not in EDIT_OP_TYPES:
        return None

    if current_type == "replace_range" and existing_type == "replace_range" and _same_range(current, existing):
        current_replacement = _normalize_text(current.get("replacement"))
        existing_replacement = _normalize_text(existing.get("replacement"))
        if current_replacement != existing_replacement:
            return "contradictory_replacement"

    if current_type == "insert_at" and existing_type == "insert_at" and _same_range(current, existing):
        current_insert = _normalize_text(current.get("new_text"))
        existing_insert = _normalize_text(existing.get("new_text"))
        if current_insert != existing_insert:
            return "contradictory_insertion"

    if _ranges_overlap_for_edits(current, existing):
        return "overlapping_edit"

    return None


def _source_ref(op: dict[str, Any]) -> dict[str, Any]:
    source = op.get("_source", {})
    return {
        "chunk_id": source.get("chunk_id"),
        "source_file": source.get("source_file"),
        "op_index": source.get("op_index"),
    }


def _downgrade_to_comment(
    *,
    op: dict[str, Any],
    reason: str,
    conflict_with: dict[str, Any],
) -> dict[str, Any]:
    proposal = ""
    if op["type"] == "replace_range":
        proposal = f' proposed replacement="{op.get("replacement", "")}".'
    elif op["type"] == "insert_at":
        proposal = f' proposed insertion="{op.get("new_text", "")}".'

    conflict_source = _source_ref(conflict_with)
    conflict_label = f"{conflict_source.get('chunk_id')}#{conflict_source.get('op_index')}"
    comment_text = (
        f"Conflict downgrade ({reason}) from {op['type']} against {conflict_label};"
        f"{proposal}".rstrip()
    )
    comment_text = comment_text.rstrip(".; ") + "."

    downgraded = {
        "type": "add_comment",
        "target": op["target"],
        "range": op["range"],
        "expected": op["expected"],
        "comment_text": comment_text,
        "_source": op["_source"],
        "_sequence": op["_sequence"],
    }
    downgraded["dedup_key"] = _dedup_key(downgraded)
    return downgraded


def _normalize_raw_op(
    *,
    raw_op: Any,
    chunk_id: str,
    source_file: str,
    op_index: int,
    sequence: int,
) -> dict[str, Any]:
    if not isinstance(raw_op, dict):
        raise ValueError("op must be an object")

    op_type = str(raw_op.get("type") or raw_op.get("op") or "").strip()
    if op_type not in VALID_OP_TYPES:
        raise ValueError(f"unsupported op type: {op_type!r}")

    normalized: dict[str, Any] = {
        "type": op_type,
        "target": _normalize_target(raw_op.get("target")),
        "range": _normalize_range(raw_op.get("range"), op_type=op_type),
        "expected": _normalize_expected(raw_op.get("expected")),
        "_source": {
            "chunk_id": chunk_id,
            "source_file": source_file,
            "op_index": op_index,
        },
        "_sequence": sequence,
    }

    if op_type == "replace_range":
        if "replacement" not in raw_op:
            raise ValueError("replace_range requires replacement")
        normalized["replacement"] = str(raw_op["replacement"])
    elif op_type == "insert_at":
        text_value = raw_op.get("new_text", raw_op.get("text"))
        if text_value is None:
            raise ValueError("insert_at requires new_text")
        normalized["new_text"] = str(text_value)
    elif op_type == "add_comment":
        comment_text = raw_op.get("comment_text")
        if comment_text is None and isinstance(raw_op.get("comment"), dict):
            comment_text = raw_op["comment"].get("text")
        if comment_text is None or not str(comment_text).strip():
            raise ValueError("add_comment requires non-empty comment_text")
        normalized["comment_text"] = str(comment_text)
        if "category" in raw_op and raw_op["category"] is not None:
            normalized["category"] = str(raw_op["category"])

    normalized["dedup_key"] = _dedup_key(normalized)
    return normalized


def _load_linear_order(
    linear_units_path: Path | None,
) -> tuple[dict[tuple[str, str, str], int], dict[tuple[str, str], int], bool]:
    if linear_units_path is None or not linear_units_path.exists():
        return {}, {}, False

    payload = load_json(linear_units_path)
    raw_order = payload.get("order", [])
    if not isinstance(raw_order, list):
        return {}, {}, False

    by_unit: dict[tuple[str, str, str], int] = {}
    by_para: dict[tuple[str, str], int] = {}
    for index, item in enumerate(raw_order):
        if not isinstance(item, dict):
            continue
        part = str(item.get("part", "")).strip()
        para_id = str(item.get("para_id", "")).strip()
        unit_uid = str(item.get("unit_uid", "")).strip()
        if not part or not para_id:
            continue
        by_para.setdefault((part, para_id), index)
        if unit_uid:
            by_unit[(part, para_id, unit_uid)] = index

    return by_unit, by_para, bool(by_para)


def _doc_order_index(
    op: dict[str, Any],
    by_unit: dict[tuple[str, str, str], int],
    by_para: dict[tuple[str, str], int],
) -> int:
    part, para_id, unit_uid = _target_key(op)
    if unit_uid and (part, para_id, unit_uid) in by_unit:
        return by_unit[(part, para_id, unit_uid)]
    if (part, para_id) in by_para:
        return by_para[(part, para_id)]
    return 10**12


def _to_output_op(op: dict[str, Any]) -> dict[str, Any]:
    output = {
        "type": op["type"],
        "target": op["target"],
        "range": op["range"],
        "expected": op["expected"],
        "dedup_key": op["dedup_key"],
    }
    if "replacement" in op:
        output["replacement"] = op["replacement"]
    if "new_text" in op:
        output["new_text"] = op["new_text"]
    if "comment_text" in op:
        output["comment_text"] = op["comment_text"]
    if "category" in op:
        output["category"] = op["category"]
    return output


def _count_ops_by_type(ops: list[dict[str, Any]]) -> dict[str, int]:
    counts = {op_type: 0 for op_type in sorted(VALID_OP_TYPES)}
    for op in ops:
        op_type = op.get("type")
        if op_type in counts:
            counts[op_type] += 1
    return counts


def merge_chunk_results_to_artifacts(
    *,
    chunk_results_dir: Path,
    output_dir: Path,
    linear_units_path: Path | None = DEFAULT_LINEAR_UNITS_PATH,
    author: str = DEFAULT_AUTHOR,
) -> dict[str, Any]:
    chunk_files = sorted(chunk_results_dir.glob("chunk_*_result.json"))

    normalized_ops: list[dict[str, Any]] = []
    invalid_ops: list[dict[str, Any]] = []
    raw_input_op_count = 0
    sequence = 0

    for chunk_file in chunk_files:
        try:
            payload = load_json(chunk_file)
        except Exception as exc:
            invalid_ops.append(
                {
                    "source_file": str(chunk_file),
                    "chunk_id": chunk_file.stem,
                    "op_index": None,
                    "reason": f"failed to parse json: {exc}",
                }
            )
            continue

        if not isinstance(payload, dict):
            invalid_ops.append(
                {
                    "source_file": str(chunk_file),
                    "chunk_id": chunk_file.stem,
                    "op_index": None,
                    "reason": "chunk result payload must be an object",
                }
            )
            continue

        chunk_id = str(payload.get("chunk_id") or chunk_file.stem)
        raw_ops = payload.get("ops")
        if raw_ops is None:
            raw_ops = payload.get("patches", [])

        if not isinstance(raw_ops, list):
            invalid_ops.append(
                {
                    "source_file": str(chunk_file),
                    "chunk_id": chunk_id,
                    "op_index": None,
                    "reason": "chunk result must include a list at ops",
                }
            )
            continue

        for op_index, raw_op in enumerate(raw_ops):
            raw_input_op_count += 1
            try:
                normalized = _normalize_raw_op(
                    raw_op=raw_op,
                    chunk_id=chunk_id,
                    source_file=str(chunk_file),
                    op_index=op_index,
                    sequence=sequence,
                )
            except ValueError as exc:
                invalid_ops.append(
                    {
                        "source_file": str(chunk_file),
                        "chunk_id": chunk_id,
                        "op_index": op_index,
                        "reason": str(exc),
                    }
                )
                continue

            normalized_ops.append(normalized)
            sequence += 1

    deduped_ops: list[dict[str, Any]] = []
    seen_keys: dict[str, dict[str, Any]] = {}
    duplicate_ops: list[dict[str, Any]] = []

    for op in normalized_ops:
        key = op["dedup_key"]
        existing = seen_keys.get(key)
        if existing is None:
            seen_keys[key] = op
            deduped_ops.append(op)
            continue

        duplicate_ops.append(
            {
                "dedup_key": key,
                "kept": _source_ref(existing),
                "dropped": _source_ref(op),
            }
        )

    resolved_ops: list[dict[str, Any]] = []
    # Group conflicts by paragraph identity so mixed presence/absence of unit_uid
    # cannot allow overlapping edits to pass silently.
    accepted_by_para: dict[tuple[str, str], list[dict[str, Any]]] = {}
    conflicts: list[dict[str, Any]] = []

    for op in deduped_ops:
        para_key = _target_para_key(op)
        prior_ops = accepted_by_para.setdefault(para_key, [])

        if op["type"] not in EDIT_OP_TYPES:
            prior_ops.append(op)
            resolved_ops.append(op)
            continue

        conflict_reason: str | None = None
        conflict_with: dict[str, Any] | None = None
        for existing in prior_ops:
            reason = _conflict_reason(op, existing)
            if reason is not None:
                conflict_reason = reason
                conflict_with = existing
                break

        if conflict_reason is None or conflict_with is None:
            prior_ops.append(op)
            resolved_ops.append(op)
            continue

        downgraded = _downgrade_to_comment(op=op, reason=conflict_reason, conflict_with=conflict_with)
        prior_ops.append(downgraded)
        resolved_ops.append(downgraded)

        conflicts.append(
            {
                "reason": conflict_reason,
                "target": op["target"],
                "incoming": {
                    "source": _source_ref(op),
                    "type": op["type"],
                    "range": op["range"],
                },
                "existing": {
                    "source": _source_ref(conflict_with),
                    "type": conflict_with["type"],
                    "range": conflict_with["range"],
                },
            }
        )

    order_by_unit, order_by_para, used_linear_order = _load_linear_order(linear_units_path)
    resolved_ops.sort(
        key=lambda op: (
            _doc_order_index(op, order_by_unit, order_by_para),
            _target_para_key(op),
            -op["range"]["start"],
            -op["range"]["end"],
            _target_key(op),
            op["_sequence"],
        )
    )

    merged_patch_payload = {
        "schema_version": PATCH_SCHEMA_VERSION,
        "created_at": _now_iso(),
        "author": author,
        "ops": [_to_output_op(op) for op in resolved_ops],
    }

    merge_report_payload = {
        "schema_version": MERGE_REPORT_SCHEMA_VERSION,
        "created_at": _now_iso(),
        "author": author,
        "inputs": {
            "chunk_results_dir": str(chunk_results_dir),
            "chunk_files": [path.name for path in chunk_files],
            "linear_units_path": str(linear_units_path) if linear_units_path is not None else None,
            "used_linear_order": used_linear_order,
        },
        "stats": {
            "chunk_file_count": len(chunk_files),
            "input_ops": raw_input_op_count,
            "valid_ops": len(normalized_ops),
            "invalid_ops": len(invalid_ops),
            "duplicates_removed": len(duplicate_ops),
            "ops_after_dedup": len(deduped_ops),
            "conflict_downgrades": len(conflicts),
            "final_ops": len(resolved_ops),
            "final_ops_by_type": _count_ops_by_type(resolved_ops),
        },
        "duplicates": duplicate_ops,
        "conflicts": conflicts,
        "invalid_ops": invalid_ops,
    }

    merged_patch_path = output_dir / "merged_patch.json"
    merge_report_path = output_dir / "merge_report.json"
    dump_json(merged_patch_path, merged_patch_payload)
    dump_json(merge_report_path, merge_report_payload)

    return {
        "merged_patch_path": merged_patch_path,
        "merge_report_path": merge_report_path,
        "chunk_file_count": len(chunk_files),
        "stats": merge_report_payload["stats"],
    }
