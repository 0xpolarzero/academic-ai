#!/usr/bin/env python3
"""Chunk DOCX extraction units into atomic, budgeted review manifests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import re
from typing import Any

DEFAULT_CHUNK_PATHS: dict[str, str] = {
    "review_units": "artifacts/docx_extract/review_units.json",
    "linear_units": "artifacts/docx_extract/linear_units.json",
    "docx_struct": "artifacts/docx_extract/docx_struct.json",
    "output_dir": "artifacts/chunks",
}

DEFAULT_MODEL_CONTEXT_WINDOW = 128_000
DEFAULT_TARGET_FRACTION = 0.35
DEFAULT_HARD_MAX_TOKENS = 56_000
DEFAULT_OVERLAP_UNITS = 1
DEFAULT_TOKENIZER_MODEL = "gpt-4o-mini"

CONTEXT_EDIT_CONTRACT: dict[str, Any] = {
    "primary_units_editable": True,
    "context_units_editable": False,
    "context_is_read_only": True,
    "enforcement": "All edits/comments must target primary_units only.",
}


@dataclass(frozen=True)
class ChunkBudget:
    model_context_window: int
    target_fraction: float
    target_tokens: int
    hard_max_tokens: int
    overlap_before_units: int
    overlap_after_units: int
    tokenizer_model: str


class TokenEstimator:
    """Estimate token counts using tiktoken when available, with deterministic fallback."""

    def __init__(self, model_name: str) -> None:
        self._encoding = None
        self.mode = "heuristic"

        try:
            import tiktoken  # type: ignore

            try:
                self._encoding = tiktoken.encoding_for_model(model_name)
            except Exception:
                self._encoding = tiktoken.get_encoding("cl100k_base")
            self.mode = "tiktoken"
        except Exception:
            self._encoding = None
            self.mode = "heuristic"

    def estimate(self, text: str) -> int:
        if self._encoding is not None:
            return max(1, len(self._encoding.encode(text or "")))
        return _estimate_tokens_heuristic(text)


def _estimate_tokens_heuristic(text: str) -> int:
    if not text:
        return 1

    char_based = math.ceil(len(text) / 4)
    lexical_units = re.findall(r"[A-Za-z0-9_]+|[^\sA-Za-z0-9_]", text)
    lexical_based = math.ceil(len(lexical_units) * 0.75)
    return max(1, char_based, lexical_based)


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


def load_constants(constants_path: Path) -> dict[str, Any]:
    if not constants_path.exists():
        return {}
    return load_json(constants_path)


def resolve_chunk_paths(constants: dict[str, Any]) -> dict[str, Path]:
    raw_paths = constants.get("chunking", {}).get("paths", {})
    return {
        key: Path(raw_paths.get(key, fallback))
        for key, fallback in DEFAULT_CHUNK_PATHS.items()
    }


def load_chunk_budget(constants: dict[str, Any]) -> ChunkBudget:
    token_budget = constants.get("chunking", {}).get("token_budget", {})

    model_context_window = max(
        1,
        _to_int(token_budget.get("model_context_window"), DEFAULT_MODEL_CONTEXT_WINDOW),
    )

    try:
        target_fraction = float(token_budget.get("target_fraction", DEFAULT_TARGET_FRACTION))
    except (TypeError, ValueError):
        target_fraction = DEFAULT_TARGET_FRACTION

    if target_fraction <= 0:
        target_fraction = DEFAULT_TARGET_FRACTION

    configured_hard_max = max(
        1,
        _to_int(token_budget.get("hard_max_tokens"), DEFAULT_HARD_MAX_TOKENS),
    )
    hard_max_tokens = min(configured_hard_max, model_context_window)

    target_tokens = max(1, int(model_context_window * target_fraction))
    target_tokens = min(target_tokens, hard_max_tokens)

    overlap_before_units = max(
        0,
        _to_int(token_budget.get("overlap_before_units"), DEFAULT_OVERLAP_UNITS),
    )
    overlap_after_units = max(
        0,
        _to_int(token_budget.get("overlap_after_units"), DEFAULT_OVERLAP_UNITS),
    )

    tokenizer_model = str(token_budget.get("tokenizer_model", DEFAULT_TOKENIZER_MODEL))

    return ChunkBudget(
        model_context_window=model_context_window,
        target_fraction=target_fraction,
        target_tokens=target_tokens,
        hard_max_tokens=hard_max_tokens,
        overlap_before_units=overlap_before_units,
        overlap_after_units=overlap_after_units,
        tokenizer_model=tokenizer_model,
    )


def _ordered_units(
    review_units_payload: dict[str, Any],
    linear_units_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    review_units = review_units_payload.get("units", [])
    if not isinstance(review_units, list):
        raise ValueError("review_units.json must contain a list at key 'units'.")

    unit_map: dict[str, dict[str, Any]] = {}
    for unit in review_units:
        unit_uid = unit.get("unit_uid")
        if not unit_uid:
            raise ValueError("Each review unit must include unit_uid.")
        if unit_uid in unit_map:
            raise ValueError(f"Duplicate unit_uid in review units: {unit_uid}")
        unit_map[unit_uid] = unit

    linear_uids = linear_units_payload.get("unit_uids")
    if not linear_uids:
        linear_uids = linear_units_payload.get("units")
    if not linear_uids:
        linear_order = linear_units_payload.get("order", [])
        linear_uids = [item["unit_uid"] for item in linear_order if "unit_uid" in item]

    if not linear_uids:
        linear_uids = [
            unit.get("unit_uid")
            for unit in sorted(review_units, key=lambda candidate: candidate.get("order_index", 0))
        ]

    ordered: list[dict[str, Any]] = []
    seen: set[str] = set()

    for unit_uid in linear_uids:
        if not unit_uid or unit_uid in seen:
            continue
        unit = unit_map.get(unit_uid)
        if unit is None:
            raise ValueError(f"linear_units.json references missing unit_uid: {unit_uid}")
        ordered.append(unit)
        seen.add(unit_uid)

    for unit in sorted(review_units, key=lambda candidate: candidate.get("order_index", 0)):
        unit_uid = unit.get("unit_uid")
        if unit_uid and unit_uid not in seen:
            ordered.append(unit)
            seen.add(unit_uid)

    return ordered


def _looks_like_local_continuation(previous_text: str, next_text: str) -> bool:
    left = (previous_text or "").rstrip()
    right = (next_text or "").lstrip()

    if not left or not right:
        return False

    if left[-1] in {".", "?", "!", ";", ":"}:
        return False

    first = right[0]
    return first.islower() or first in {",", ")", "]"}


def _build_primary_spans(
    ordered_units: list[dict[str, Any]],
    unit_tokens: list[int],
    budget: ChunkBudget,
) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    cursor = 0
    total_units = len(ordered_units)

    while cursor < total_units:
        start = cursor
        cursor += 1
        chunk_tokens = unit_tokens[start]

        while cursor < total_units:
            next_tokens = unit_tokens[cursor]
            prospective_tokens = chunk_tokens + next_tokens

            if prospective_tokens <= budget.target_tokens:
                chunk_tokens = prospective_tokens
                cursor += 1
                continue

            prev_text = ordered_units[cursor - 1].get("accepted_text", "")
            next_text = ordered_units[cursor].get("accepted_text", "")
            keep_for_coherence = _looks_like_local_continuation(prev_text, next_text)

            if keep_for_coherence and prospective_tokens <= budget.hard_max_tokens:
                chunk_tokens = prospective_tokens
                cursor += 1
                continue

            break

        spans.append((start, cursor))

    return spans


def _longest_common_heading_path(units: list[dict[str, Any]]) -> list[str]:
    if not units:
        return []

    heading_paths: list[list[str]] = []
    for unit in units:
        path = unit.get("heading_path")
        if isinstance(path, list):
            heading_paths.append([str(segment) for segment in path])
        else:
            heading_paths.append([])

    common = heading_paths[0]
    for path in heading_paths[1:]:
        max_prefix = min(len(common), len(path))
        pivot = 0
        while pivot < max_prefix and common[pivot] == path[pivot]:
            pivot += 1
        common = common[:pivot]
        if not common:
            break

    return common


def _to_chunk_unit(unit: dict[str, Any], *, role: str, editable: bool, token_estimate: int) -> dict[str, Any]:
    return {
        "part": unit.get("part"),
        "part_kind": unit.get("part_kind"),
        "part_name": unit.get("part_name"),
        "para_id": unit.get("para_id"),
        "unit_uid": unit.get("unit_uid"),
        "accepted_text": unit.get("accepted_text", ""),
        "heading_path": unit.get("heading_path", []),
        "order_index": unit.get("order_index"),
        "location": unit.get("location", {}),
        "role": role,
        "editable": editable,
        "token_estimate": token_estimate,
    }


def _build_chunk_payload(
    *,
    chunk_id: str,
    chunk_index: int,
    ordered_units: list[dict[str, Any]],
    unit_tokens: list[int],
    start: int,
    end: int,
    budget: ChunkBudget,
) -> dict[str, Any]:
    before_start = max(0, start - budget.overlap_before_units)
    before_indices = list(range(before_start, start))

    after_end = min(len(ordered_units), end + budget.overlap_after_units)
    after_indices = list(range(end, after_end))

    primary_indices = list(range(start, end))

    primary_tokens = sum(unit_tokens[index] for index in primary_indices)

    # Keep overflow controlled: when a single primary unit is already oversized,
    # remove overlap context so non-primary units never contribute to overflow.
    context_trimmed_for_budget = False
    if primary_tokens > budget.hard_max_tokens and (before_indices or after_indices):
        before_indices = []
        after_indices = []
        context_trimmed_for_budget = True

    # Keep nearest overlap units by default, but trim farthest context units if needed
    # so regular chunks remain within hard max budget.
    while True:
        context_before_tokens = sum(unit_tokens[index] for index in before_indices)
        context_after_tokens = sum(unit_tokens[index] for index in after_indices)
        total_tokens = primary_tokens + context_before_tokens + context_after_tokens

        if total_tokens <= budget.hard_max_tokens:
            break
        if primary_tokens > budget.hard_max_tokens:
            break
        if not before_indices and not after_indices:
            break

        context_trimmed_for_budget = True
        if after_indices and (len(after_indices) >= len(before_indices) or not before_indices):
            after_indices.pop()  # remove farthest trailing context first
            continue
        if before_indices:
            before_indices.pop(0)  # remove farthest leading context first
            continue

    primary_units_raw = [ordered_units[index] for index in primary_indices]
    context_before_raw = [ordered_units[index] for index in before_indices]
    context_after_raw = [ordered_units[index] for index in after_indices]

    primary_units = [
        _to_chunk_unit(unit, role="primary", editable=True, token_estimate=unit_tokens[index])
        for index, unit in zip(primary_indices, primary_units_raw, strict=False)
    ]
    context_before = [
        _to_chunk_unit(unit, role="context_before", editable=False, token_estimate=unit_tokens[index])
        for index, unit in zip(before_indices, context_before_raw, strict=False)
    ]
    context_after = [
        _to_chunk_unit(unit, role="context_after", editable=False, token_estimate=unit_tokens[index])
        for index, unit in zip(after_indices, context_after_raw, strict=False)
    ]

    context_before_tokens = sum(unit_tokens[index] for index in before_indices)
    context_after_tokens = sum(unit_tokens[index] for index in after_indices)
    total_tokens = primary_tokens + context_before_tokens + context_after_tokens

    allowed_overflow = (
        total_tokens > budget.hard_max_tokens
        and len(primary_indices) == 1
        and primary_tokens > budget.hard_max_tokens
    )

    token_estimates: dict[str, Any] = {
        "primary_tokens": primary_tokens,
        "context_before_tokens": context_before_tokens,
        "context_after_tokens": context_after_tokens,
        "total_tokens": total_tokens,
        "target_tokens": budget.target_tokens,
        "hard_max_tokens": budget.hard_max_tokens,
        "is_within_hard_max": total_tokens <= budget.hard_max_tokens,
        "allowed_overflow": allowed_overflow,
    }
    if allowed_overflow:
        token_estimates["overflow_reason"] = "single_primary_unit_exceeds_hard_max"

    heading_path = _longest_common_heading_path(primary_units_raw)

    return {
        "schema_version": "chunk.v1",
        "chunk_id": chunk_id,
        "chunk_index": chunk_index,
        "contract": CONTEXT_EDIT_CONTRACT,
        "primary_units": primary_units,
        "context_units_before": context_before,
        "context_units_after": context_after,
        "metadata": {
            "heading_path": heading_path,
            "source_span": {
                "start_linear_index": start,
                "end_linear_index": end - 1,
                "start_order_index": ordered_units[start].get("order_index", start),
                "end_order_index": ordered_units[end - 1].get("order_index", end - 1),
                "primary_unit_count": len(primary_units),
                "context_before_count": len(context_before),
                "context_after_count": len(context_after),
                "requested_overlap_before": budget.overlap_before_units,
                "requested_overlap_after": budget.overlap_after_units,
                "context_trimmed_for_budget": context_trimmed_for_budget,
            },
            "token_estimates": token_estimates,
        },
    }


def chunk_docx_to_artifacts(
    *,
    review_units_path: Path,
    linear_units_path: Path,
    output_dir: Path,
    constants_path: Path,
    docx_struct_path: Path | None = None,
) -> dict[str, Any]:
    constants = load_constants(constants_path)
    budget = load_chunk_budget(constants)

    review_units_payload = load_json(review_units_path)
    linear_units_payload = load_json(linear_units_path)

    ordered_units = _ordered_units(review_units_payload, linear_units_payload)
    estimator = TokenEstimator(model_name=budget.tokenizer_model)
    unit_tokens = [estimator.estimate(unit.get("accepted_text", "")) for unit in ordered_units]

    primary_spans = _build_primary_spans(ordered_units=ordered_units, unit_tokens=unit_tokens, budget=budget)

    output_dir.mkdir(parents=True, exist_ok=True)
    for stale_chunk in output_dir.glob("chunk_*.json"):
        stale_chunk.unlink()

    chunks_manifest_entries: list[dict[str, Any]] = []
    chunk_paths: list[Path] = []

    for chunk_index, (start, end) in enumerate(primary_spans, start=1):
        chunk_id = f"chunk_{chunk_index:04d}"
        chunk_payload = _build_chunk_payload(
            chunk_id=chunk_id,
            chunk_index=chunk_index - 1,
            ordered_units=ordered_units,
            unit_tokens=unit_tokens,
            start=start,
            end=end,
            budget=budget,
        )

        chunk_file_name = f"{chunk_id}.json"
        chunk_path = output_dir / chunk_file_name
        dump_json(chunk_path, chunk_payload)
        chunk_paths.append(chunk_path)

        chunks_manifest_entries.append(
            {
                "chunk_id": chunk_id,
                "path": chunk_file_name,
                "source_span": chunk_payload["metadata"]["source_span"],
                "token_estimates": chunk_payload["metadata"]["token_estimates"],
                "heading_path": chunk_payload["metadata"]["heading_path"],
                "primary_unit_uids": [unit["unit_uid"] for unit in chunk_payload["primary_units"]],
                "context_before_unit_uids": [unit["unit_uid"] for unit in chunk_payload["context_units_before"]],
                "context_after_unit_uids": [unit["unit_uid"] for unit in chunk_payload["context_units_after"]],
                "primary_targets": [
                    {
                        "part": unit.get("part"),
                        "para_id": unit.get("para_id"),
                        "unit_uid": unit.get("unit_uid"),
                    }
                    for unit in chunk_payload["primary_units"]
                ],
                "context_targets_before": [
                    {
                        "part": unit.get("part"),
                        "para_id": unit.get("para_id"),
                        "unit_uid": unit.get("unit_uid"),
                    }
                    for unit in chunk_payload["context_units_before"]
                ],
                "context_targets_after": [
                    {
                        "part": unit.get("part"),
                        "para_id": unit.get("para_id"),
                        "unit_uid": unit.get("unit_uid"),
                    }
                    for unit in chunk_payload["context_units_after"]
                ],
            }
        )

    manifest_payload: dict[str, Any] = {
        "schema_version": "chunk_manifest.v1",
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "contract": CONTEXT_EDIT_CONTRACT,
        "source": {
            "review_units": str(review_units_path),
            "linear_units": str(linear_units_path),
            "docx_struct": str(docx_struct_path) if docx_struct_path else None,
        },
        "token_budget": {
            "model_context_window": budget.model_context_window,
            "target_fraction": budget.target_fraction,
            "target_tokens": budget.target_tokens,
            "hard_max_tokens": budget.hard_max_tokens,
            "overlap_before_units": budget.overlap_before_units,
            "overlap_after_units": budget.overlap_after_units,
            "tokenizer": estimator.mode,
            "tokenizer_model": budget.tokenizer_model,
        },
        "unit_count": len(ordered_units),
        "chunk_count": len(chunks_manifest_entries),
        "chunks": chunks_manifest_entries,
    }

    manifest_path = output_dir / "manifest.json"
    dump_json(manifest_path, manifest_payload)

    return {
        "manifest_path": manifest_path,
        "chunk_paths": chunk_paths,
        "chunk_count": len(chunks_manifest_entries),
        "unit_count": len(ordered_units),
    }
