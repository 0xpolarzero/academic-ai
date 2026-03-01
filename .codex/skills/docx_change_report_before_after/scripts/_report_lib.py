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


def _location_uncertain(change: dict[str, Any]) -> bool:
    """Determine if the location is uncertain based on disambiguation data."""
    disambiguation = change.get("disambiguation")
    if isinstance(disambiguation, dict):
        return disambiguation.get("occurrence_count", 1) > 1
    return False


def _find_word_boundary_start(text: str, pos: int) -> int:
    """Find the start of the word containing position, ensuring we don't cut words."""
    if pos <= 0:
        return 0
    if pos >= len(text):
        return len(text)
    
    # If we're at a word boundary (space or start), return current position
    if text[pos].isspace():
        # Move forward to find non-space
        while pos < len(text) and text[pos].isspace():
            pos += 1
        return pos
    
    # If previous char is space, we're at word start
    if text[pos - 1].isspace():
        return pos
    
    # We're in the middle of a word - find the word start
    while pos > 0 and not text[pos - 1].isspace():
        pos -= 1
    return pos


def _find_word_boundary_end(text: str, pos: int) -> int:
    """Find the end of the word containing position, ensuring we don't cut words."""
    if pos <= 0:
        return 0
    if pos >= len(text):
        return len(text)
    
    # If we're at a word boundary (space or end), return current position
    if text[pos - 1].isspace():
        # Move backward to find non-space
        while pos > 0 and text[pos - 1].isspace():
            pos -= 1
        return pos
    
    # If current char is space, we're at word end
    if pos < len(text) and text[pos].isspace():
        return pos
    
    # We're in the middle of a word - find the word end
    while pos < len(text) and not text[pos].isspace():
        pos += 1
    return pos


def find_diff_bounds(old: str, new: str) -> tuple[int, int, int, int, int]:
    """
    Find the minimal diff between old and new text.
    
    Returns:
        (prefix_len, old_changed_start, old_changed_end, new_changed_start, new_changed_end)
        where prefix_len is the length of common prefix,
        old_changed_start/end are the bounds of changed portion in old,
        new_changed_start/end are the bounds of changed portion in new.
    """
    if old == new:
        return (len(old), 0, 0, 0, 0)
    
    if not old:
        # Pure insertion - bold entire new text
        return (0, 0, 0, 0, len(new))
    
    if not new:
        # Pure deletion - bold entire old text
        return (0, 0, len(old), 0, 0)
    
    # Find common prefix
    prefix_len = 0
    max_prefix = min(len(old), len(new))
    while prefix_len < max_prefix and old[prefix_len] == new[prefix_len]:
        prefix_len += 1
    
    # Find common suffix (only from remaining portions after prefix)
    suffix_len = 0
    max_suffix = min(len(old) - prefix_len, len(new) - prefix_len)
    while suffix_len < max_suffix:
        old_idx = len(old) - 1 - suffix_len
        new_idx = len(new) - 1 - suffix_len
        # Make sure we don't overlap with prefix
        if old_idx < prefix_len or new_idx < prefix_len:
            break
        if old[old_idx] != new[new_idx]:
            break
        suffix_len += 1
    
    # Calculate changed bounds
    old_changed_start = prefix_len
    old_changed_end = len(old) - suffix_len
    new_changed_start = prefix_len
    new_changed_end = len(new) - suffix_len
    
    # Check if this is a pure insertion or pure deletion within the diff region
    old_is_empty = old_changed_start >= old_changed_end
    new_is_empty = new_changed_start >= new_changed_end
    
    # Only apply word boundary expansion for replacements (both sides have content)
    # For pure insertions/deletions, keep the minimal diff
    if not old_is_empty and not new_is_empty:
        # Replacement: expand to word boundaries so we don't cut words in half
        old_changed_start = _find_word_boundary_start(old, old_changed_start)
        old_changed_end = _find_word_boundary_end(old, old_changed_end)
        new_changed_start = _find_word_boundary_start(new, new_changed_start)
        new_changed_end = _find_word_boundary_end(new, new_changed_end)
    elif old_is_empty and not new_is_empty:
        # Pure insertion at the position - bold the entire new changed portion
        # Keep as-is, don't expand
        pass
    elif new_is_empty and not old_is_empty:
        # Pure deletion - bold the entire old changed portion
        # Keep as-is, don't expand
        pass
    
    return (prefix_len, old_changed_start, old_changed_end, new_changed_start, new_changed_end)


def _strip_boundary_spaces(text: str, start: int, end: int) -> tuple[int, int]:
    """
    Adjust bounds to exclude leading/trailing spaces from the highlighted region.
    
    This prevents bold markers from including spaces like '** de**' or '**être **'.
    Spaces should remain outside the bold markers for clean formatting.
    
    Returns:
        (adjusted_start, adjusted_end) where spaces are trimmed from boundaries
    """
    # Trim leading spaces from the changed region
    while start < end and start < len(text) and text[start].isspace():
        start += 1
    # Trim trailing spaces from the changed region
    while end > start and end > 0 and text[end - 1].isspace():
        end -= 1
    return start, end


def _is_word_boundary(text: str, pos: int) -> bool:
    """Check if position is at a word boundary (start/end of string, whitespace, or punctuation)."""
    if pos <= 0 or pos >= len(text):
        return True
    # Whitespace is always a boundary
    if text[pos].isspace() or text[pos - 1].isspace():
        return True
    # Punctuation characters act as word boundaries
    punctuation = '.,;:!?"''()[]{}«»'
    if text[pos] in punctuation or text[pos - 1] in punctuation:
        return True
    return False


def _find_with_word_boundary(text: str, substring: str) -> int:
    """Find substring in text, but only at word boundaries.
    
    This prevents matching partial words (e.g., finding "ce" inside "ces",
    "les" inside "agricoles", or "particulière" inside "particulièrement").
    
    Word boundaries include:
    - Start/end of string
    - Whitespace characters
    - Punctuation characters (.,;:!?"'()[]{}«» etc.)
    
    Returns:
        Index of the match, or -1 if not found at a word boundary.
    """
    if not substring:
        return -1
    idx = text.find(substring)
    while idx >= 0:
        # Check if the match is at a word boundary
        start_boundary = (idx == 0) or _is_word_boundary(text, idx)
        end_pos = idx + len(substring)
        end_boundary = (end_pos >= len(text)) or _is_word_boundary(text, end_pos)
        
        if start_boundary and end_boundary:
            return idx
        
        # Look for next occurrence
        idx = text.find(substring, idx + 1)
    return -1


def format_replacement(context: str, old_text: str, new_text: str) -> tuple[str, str]:
    """
    Format a replacement operation with context in both columns.
    
    For pure insertions at word boundaries (e.g., "Hongrie" -> "de la Hongrie"):
    - At column: bold the entire old_text (the word being modified)
    - Suggestion: context with new_text replacing old_text
    
    For pure deletions:
    - At column: bold the deleted portion
    - Suggestion: context without the deleted portion
    
    For replacements:
    - At column: context with only the changed portion of old_text bold
    - Suggestion: context with only the changed portion of new_text bold
    
    Returns:
        (at_formatted, suggestion_formatted)
    """
    context = context.strip()
    original_old_text = old_text
    original_new_text = new_text
    
    # Check for whitespace-only changes BEFORE stripping
    # This handles cases like "word ." -> "word." or "word " -> "word"
    # Compare by removing ALL whitespace - if equal after removal, only whitespace changed
    import re
    old_no_ws = re.sub(r'\s+', '', original_old_text)
    new_no_ws = re.sub(r'\s+', '', original_new_text)
    
    if old_no_ws == new_no_ws and original_old_text != original_new_text:
        # Only whitespace differs - find what changed
        import difflib
        diff = list(difflib.ndiff(original_old_text, original_new_text))
        old_only = ''.join(d[2:] for d in diff if d.startswith('- '))
        new_only = ''.join(d[2:] for d in diff if d.startswith('+ '))
        
        # Build display with markers - use text with normalized single spaces
        at_result = re.sub(r'\s+', ' ', original_old_text).strip()
        suggestion_result = re.sub(r'\s+', ' ', original_new_text).strip()
        
        if old_only and not new_only:
            # Whitespace removed from old
            ws_marker = "** **" if old_only == " " else f"**{old_only}**"
            # Find position by counting characters before the first '- ' in diff
            ws_pos = 0
            for d in diff:
                if d.startswith('- '):
                    break
                ws_pos += 1
            before_ws = original_old_text[:ws_pos]
            after_ws = original_old_text[ws_pos + len(old_only):]
            # At shows original with marker, Suggestion shows new (without the space)
            return f"{before_ws}{ws_marker}{after_ws}", suggestion_result
        elif new_only and not old_only:
            # Whitespace added to new
            ws_marker = "** **" if new_only == " " else f"**{new_only}**"
            ws_pos = 0
            for d in diff:
                if d.startswith('+ '):
                    break
                ws_pos += 1
            before_ws = original_new_text[:ws_pos]
            after_ws = original_new_text[ws_pos + len(new_only):]
            # At shows original, Suggestion shows new with marker
            return at_result, f"{before_ws}{ws_marker}{after_ws}"
    
    old_text = old_text.strip()
    new_text = new_text.strip()
    
    # Handle whitespace-only deletions (e.g., space removal) after stripping
    old_is_whitespace_only = not old_text or (original_old_text and original_old_text.strip() == "")
    new_is_whitespace_only = not new_text or (original_new_text and original_new_text.strip() == "")
    
    if old_is_whitespace_only and new_is_whitespace_only:
        # Both sides are whitespace-only, show appropriate marker
        if original_old_text == " ":
            return "*(space removed)*", "*(deleted)*"
        elif original_old_text:
            return "*(whitespace removed)*", "*(deleted)*"
        elif original_new_text == " ":
            return "*(space added)*", "**(space)**"
        else:
            return "*(whitespace added)*", "*(added)*"
    
    if not context:
        # No context, just show the minimal diff
        prefix_len, old_start, old_end, new_start, new_end = find_diff_bounds(old_text, new_text)
        
        # Strip boundary spaces to prevent '** de**' or '**être **'
        old_start, old_end = _strip_boundary_spaces(old_text, old_start, old_end)
        new_start, new_end = _strip_boundary_spaces(new_text, new_start, new_end)
        
        old_changed = old_text[old_start:old_end] if old_text else ""
        new_changed = new_text[new_start:new_end] if new_text else ""
        
        at_result = old_text[:old_start] + f"**{old_changed}**" + old_text[old_end:]
        suggestion_result = new_text[:new_start] + f"**{new_changed}**" + new_text[new_end:]
        return at_result.strip(), suggestion_result.strip()
    
    if not old_text:
        return f"**{old_text}**", f"**{new_text}**" if new_text else "*(deleted)*"
    
    # Find old_text at a word boundary to avoid partial word matches
    # (e.g., finding "ce" inside "ces", "les" inside "agricoles")
    idx = _find_with_word_boundary(context, old_text)
    if idx < 0:
        # Old text not found as a complete word in context, fall back to showing both
        return f"**{old_text}**", f"**{new_text}**" if new_text else "*(deleted)*"
    
    # Split context into before, target, after
    before = context[:idx]
    after = context[idx + len(old_text):]
    
    # Find the minimal diff
    prefix_len, old_start, old_end, new_start, new_end = find_diff_bounds(old_text, new_text)
    
    # Check what type of change this is
    old_is_empty = old_start >= old_end
    new_is_empty = new_start >= new_end or not new_text
    
    # Handle whitespace-only changes (e.g., removing space before period)
    old_changed = old_text[old_start:old_end] if not old_is_empty else ""
    new_changed = new_text[new_start:new_end] if not new_is_empty else ""
    
    # If the only change is whitespace, show a clear marker
    if old_changed and not new_changed and old_changed.strip() == "":
        # Whitespace was removed (e.g., "word ." -> "word.")
        at_formatted = before + old_text[:old_start] + f"**{old_changed}**" + old_text[old_end:] + after
        suggestion_formatted = before + old_text[:old_start] + old_text[old_end:] + after
        return at_formatted.strip(), suggestion_formatted.strip()
    
    if not old_changed and new_changed and new_changed.strip() == "":
        # Whitespace was added
        at_formatted = before + old_text + after
        suggestion_formatted = before + new_text[:new_start] + f"**{new_changed}**" + new_text[new_end:] + after
        return at_formatted.strip(), suggestion_formatted.strip()
    
    if old_is_empty and not new_is_empty:
        # Pure insertion (e.g., "Hongrie" -> "de la Hongrie")
        # The old_text is entirely contained in new_text as a suffix
        # Bold the entire old_text in At column
        # Show the full new_text with the insertion bolded in Suggestion
        at_formatted = before + f"**{old_text}**" + after
        suggestion_formatted = before + f"**{new_text}**" + after
    elif new_is_empty and not old_is_empty:
        # Pure deletion within the text
        # Strip boundary spaces to prevent '** de**' or '**être **'
        old_start, old_end = _strip_boundary_spaces(old_text, old_start, old_end)
        old_changed = old_text[old_start:old_end]
        at_formatted = before + old_text[:old_start] + f"**{old_changed}**" + old_text[old_end:] + after
        # Remove the deleted portion for suggestion
        suggestion_formatted = before + old_text[:old_start] + old_text[old_end:] + after
    elif old_is_empty and new_is_empty:
        # No change at all (shouldn't happen)
        at_formatted = context
        suggestion_formatted = context
    else:
        # Replacement - both have changed content
        # Strip boundary spaces to prevent '** de**' or '**être **'
        old_start, old_end = _strip_boundary_spaces(old_text, old_start, old_end)
        new_start, new_end = _strip_boundary_spaces(new_text, new_start, new_end)
        
        old_changed = old_text[old_start:old_end]
        new_changed = new_text[new_start:new_end]
        
        # Build the At column
        at_formatted = before + old_text[:old_start] + f"**{old_changed}**" + old_text[old_end:] + after
        
        # Build the Suggestion column using same structure
        # We need to construct new_text within the context position
        suggestion_formatted = before + new_text[:new_start] + f"**{new_changed}**" + new_text[new_end:] + after
    
    return at_formatted.strip(), suggestion_formatted.strip()


def format_deletion(context: str, deleted_text: str) -> tuple[str, str]:
    """
    Format a deletion operation.
    
    Returns:
        (at_formatted, suggestion_formatted) where:
        - at_formatted: context with deleted text in bold
        - suggestion_formatted: context with *(deleted)* marker
    """
    context = context.strip()
    original_deleted_text = deleted_text
    deleted_text = deleted_text.strip()
    
    # Handle whitespace-only deletions (e.g., space removal)
    if not deleted_text and original_deleted_text and original_deleted_text.strip() == "":
        return "*(space removed)*" if original_deleted_text == " " else "*(whitespace removed)*", "*(deleted)*"
    
    if not context or not deleted_text:
        return f"**{deleted_text}**" if deleted_text else "", "*(deleted)*"
    
    if deleted_text in context:
        at_formatted = context.replace(deleted_text, f"**{deleted_text}**", 1)
        suggestion_formatted = context.replace(deleted_text, "*(deleted)*", 1)
    else:
        at_formatted = f"**{deleted_text}**"
        suggestion_formatted = "*(deleted)*"
    
    return at_formatted.strip(), suggestion_formatted.strip()


def format_comment(context: str, target: str, comment_text: str) -> tuple[str, str]:
    """
    Format a comment operation.
    
    Returns:
        (at_formatted, suggestion_formatted) where:
        - at_formatted: context with target text in bold
        - suggestion_formatted: the comment text (no bold)
    """
    context = context.strip()
    target = target.strip()
    
    if not context:
        return f"**{target}**" if target else "", comment_text.strip()
    
    if target and target in context:
        at_formatted = context.replace(target, f"**{target}**", 1)
    else:
        at_formatted = context
    
    return at_formatted.strip(), comment_text.strip()


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


def _extract_document_name(source_path: str) -> str:
    """Extract a readable document name from the source path."""
    path = Path(source_path)
    name = path.stem
    if name == "review_units":
        parent = path.parent
        if parent.name and parent.name != "artifacts":
            return parent.name.replace("_", " ").replace("-", " ").title()
    return name.replace("_", " ").replace("-", " ").title()


def _group_changes_by_section(changes: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Group changes by their section heading."""
    sections: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for change in changes:
        location = change.get("location", {})
        heading_path = location.get("heading_path", [])
        section = str(heading_path[-1]) if heading_path else "(no heading)"
        sections[section].append(change)
    return sections


def _is_edit_op(op_type: str) -> bool:
    """Check if operation is an edit (replacement, insertion, deletion)."""
    return op_type in {"replace_range", "insert_at", "delete_range"}


def _is_comment_op(op_type: str) -> bool:
    """Check if operation is a comment."""
    return op_type == "add_comment"


def _escape_table_cell(text: str) -> str:
    """Escape pipe characters in table cells."""
    return text.replace("|", "\\|")


def _render_changes_table(changes: list[dict[str, Any]], lines: list[str]) -> None:
    """Render a table of changes."""
    if not changes:
        return
    
    lines.append("| # | At | Suggestion |")
    lines.append("|---|----|------------|")
    
    for change in changes:
        op_index = _to_int(change.get("op_index"), 0)
        before = str(change.get("before_snippet", ""))  # Extended context with surrounding words
        exact_snippet = str(change.get("exact_snippet", ""))  # Exact text targeted (the old text)
        after = str(change.get("after_snippet", ""))
        op_type = change.get("type", "")
        uncertain = change.get("location_uncertain", False)
        annotation = change.get("annotation")
        
        # Build the At column and suggestion based on operation type
        if op_type == "replace_range":
            at_snippet, suggestion = format_replacement(before, exact_snippet, after)
        elif op_type == "delete_range":
            at_snippet, suggestion = format_deletion(before, exact_snippet)
        elif op_type == "insert_at":
            at_snippet = before
            suggestion = f"**{after}**" if after else "*(deleted)*"
        elif op_type == "add_comment":
            at_snippet, suggestion = format_comment(before, exact_snippet, str(annotation) if annotation else "")
        else:
            at_snippet = before
            suggestion = after
        
        # Add uncertainty flag if needed
        if uncertain:
            at_snippet += " *(approx)*"
        
        # Escape pipe characters
        at_column = _escape_table_cell(at_snippet)
        suggestion = _escape_table_cell(suggestion)
        
        lines.append(f"| {op_index} | {at_column} | {suggestion} |")
    
    lines.append("")


def render_changes_markdown(payload: dict[str, Any]) -> str:
    """
    Render the change report as markdown.
    
    Format:
    ## Text Changes
    ### Section Name
    | # | At | Suggestion |
    |---|----|------------|
    | 1 | ... | ... |
    
    ## Comments
    ### Section Name
    | # | At | Suggestion |
    |---|----|------------|
    | 5 | ... | Comment text |
    """
    changes = payload.get("changes", [])
    if not isinstance(changes, list):
        changes = []

    doc_name = _extract_document_name(payload.get("source_review_units", ""))
    suggestion_count = len(changes)
    
    # Separate edits (replacements/deletions/insertions) from comments
    edits = [c for c in changes if _is_edit_op(c.get("type", ""))]
    comments = [c for c in changes if _is_comment_op(c.get("type", ""))]
    others = [c for c in changes if c not in edits and c not in comments]

    lines: list[str] = [
        f"# Review — {doc_name} ({suggestion_count} suggestions)",
        "",
    ]
    
    # Section 1: Text Changes (replacements, deletions, insertions)
    if edits:
        lines.append("## Text Changes")
        lines.append("")
        
        edit_sections = _group_changes_by_section(edits)
        for section_name, section_changes in edit_sections.items():
            lines.append(f"### {section_name}")
            lines.append("")
            _render_changes_table(section_changes, lines)
    
    # Section 2: Comments
    if comments:
        lines.append("## Comments")
        lines.append("")
        
        comment_sections = _group_changes_by_section(comments)
        for section_name, section_changes in comment_sections.items():
            lines.append(f"### {section_name}")
            lines.append("")
            _render_changes_table(section_changes, lines)
    
    # Section 3: Others (if any)
    if others:
        lines.append("## Other")
        lines.append("")
        
        other_sections = _group_changes_by_section(others)
        for section_name, section_changes in other_sections.items():
            lines.append(f"### {section_name}")
            lines.append("")
            _render_changes_table(section_changes, lines)

    return "\n".join(lines).rstrip() + "\n"


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
        
        # Skip no-op operations (where old_text == new_text)
        # These are typically data quality issues in the upstream patch
        # Apply to all edit operations: replace_range, insert_at, delete_range
        if op_type in ("replace_range", "insert_at", "delete_range"):
            # For insert_at, 'before' is typically empty and 'after' has the new text
            # For delete_range, 'after' is empty and 'before' has the deleted text
            # For replace_range, both have content
            # Skip if they appear identical (ignoring all whitespace variations)
            import re
            before_no_ws = re.sub(r'\s+', '', before)
            after_no_ws = re.sub(r'\s+', '', after)
            if before_no_ws == after_no_ws:
                continue
        
        accepted_text = str(unit.get("accepted_text", "")) if unit else ""
        disambiguation = _disambiguation(
            before_snippet=before,
            accepted_text=accepted_text,
            range_start_u16=range_start_u16,
            range_end_u16=range_end_u16,
        )

        annotation = _comment_text_from_op(op) if op_type == "add_comment" else None

        # Keep the original exact snippet for replacements
        original_before = before
        
        # Build extended before_snippet with LOTS of context (80 chars each side)
        extended_before = before
        if unit and accepted_text and before:
            before_idx = accepted_text.find(before)
            if before_idx >= 0:
                # Start with 80 chars before, then expand to word boundary
                context_start = max(0, before_idx - 80)
                # Expand to start of word
                while context_start > 0 and not accepted_text[context_start - 1].isspace():
                    context_start -= 1
                
                # End with 80 chars after, then expand to word boundary
                context_end = min(len(accepted_text), before_idx + len(before) + 80)
                # Expand to end of word
                while context_end < len(accepted_text) and not accepted_text[context_end].isspace():
                    context_end += 1
                
                extended_before = accepted_text[context_start:context_end]

        change: dict[str, Any] = {
            "op_index": op_index,
            "type": op_type,
            "location": location,
            "stable_location": _stable_location_string(location),
            "range": {
                "start": range_start_u16,
                "end": range_end_u16,
            },
            "before_snippet": extended_before,
            "exact_snippet": original_before,
            "after_snippet": after,
            "annotation": annotation,
            "location_uncertain": disambiguation is not None and disambiguation.get("occurrence_count", 1) > 1,
        }

        if disambiguation is not None:
            change["disambiguation"] = disambiguation

        changes.append(change)

    stats = {
        "op_count": len(changes),
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
