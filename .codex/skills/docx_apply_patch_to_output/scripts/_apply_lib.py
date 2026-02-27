#!/usr/bin/env python3
"""Apply merged patch operations to a DOCX using tracked changes and Word comments."""

from __future__ import annotations

from collections import OrderedDict, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
import io
import json
from pathlib import PurePosixPath, Path
import re
from typing import Any
import copy
import xml.etree.ElementTree as ET
import zipfile

DEFAULT_INPUT_DOCX = Path("input/source.docx")
DEFAULT_PATCH_PATH = Path("artifacts/patch/merged_patch.json")
DEFAULT_REVIEW_UNITS_PATH = Path("artifacts/docx_extract/review_units.json")
DEFAULT_OUTPUT_DOCX = Path("output/annotated.docx")
DEFAULT_APPLY_LOG_PATH = Path("artifacts/apply/apply_log.json")
DEFAULT_AUTHOR = "docx_apply_patch_to_output"

PATCH_SCHEMA_VERSION = "patch.v1"
APPLY_LOG_SCHEMA_VERSION = "apply_log.v1"

VALID_OP_TYPES = {"add_comment", "replace_range", "insert_at", "delete_range"}
EDIT_OP_TYPES = {"replace_range", "insert_at", "delete_range"}

W_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
XML_NS = "http://www.w3.org/XML/1998/namespace"
REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
CT_NS = "http://schemas.openxmlformats.org/package/2006/content-types"
NS = {"w": W_NS, "rels": REL_NS, "ct": CT_NS}

COMMENTS_CONTENT_TYPE = "application/vnd.openxmlformats-officedocument.wordprocessingml.comments+xml"
COMMENTS_REL_TYPE = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/comments"
COMMENTS_PART = PurePosixPath("word/comments.xml")


ET.register_namespace("w", W_NS)
ET.register_namespace("r", "http://schemas.openxmlformats.org/officeDocument/2006/relationships")
ET.register_namespace("rels", REL_NS)
ET.register_namespace("ct", CT_NS)


def qn(ns: str, local: str) -> str:
    return f"{{{ns}}}{local}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def dump_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _xml_bytes(root: ET.Element) -> bytes:
    buffer = io.BytesIO()
    ET.ElementTree(root).write(buffer, encoding="utf-8", xml_declaration=True)
    return buffer.getvalue()


def _to_int(value: Any, *, field: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be an integer") from exc


def _utf16_offsets(text: str) -> list[int]:
    offsets = [0]
    total = 0
    for char in text:
        total += len(char.encode("utf-16-le")) // 2
        offsets.append(total)
    return offsets


def _utf16_map(text: str) -> tuple[list[int], dict[int, int]]:
    offsets = _utf16_offsets(text)
    mapping = {value: index for index, value in enumerate(offsets)}
    return offsets, mapping


def _utf16_slice(text: str, *, start: int, end: int) -> str:
    _, mapping = _utf16_map(text)
    if start not in mapping or end not in mapping:
        raise ValueError("range boundaries must align to UTF-16 code unit boundaries")
    return text[mapping[start] : mapping[end]]


def _leading_or_trailing_space(text: str) -> bool:
    return bool(text) and (text[0].isspace() or text[-1].isspace() or "  " in text)


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


def _build_parent_map(root: ET.Element) -> dict[ET.Element, ET.Element]:
    return {child: parent for parent in root.iter() for child in parent}


def _closest_ancestor_with_local_name(
    node: ET.Element,
    parent_map: dict[ET.Element, ET.Element],
    local_name: str,
) -> ET.Element | None:
    cursor = node
    while cursor in parent_map:
        cursor = parent_map[cursor]
        if _local_name(cursor.tag) == local_name:
            return cursor
    return None


@dataclass(frozen=True)
class UnitLocator:
    part: str
    para_id: str
    unit_uid: str
    order_index: int
    paragraph_index_in_part: int
    accepted_text: str


@dataclass(frozen=True)
class CharToken:
    char: str
    kind: str
    style_key: str


@dataclass(frozen=True)
class InsertEvent:
    pos_cp: int
    text: str
    style_key: str
    rev_id: int
    sequence: int
    after_delete: bool


@dataclass(frozen=True)
class DeleteEvent:
    start_cp: int
    end_cp: int
    text: str
    style_key: str
    rev_id: int


@dataclass(frozen=True)
class CommentEvent:
    start_cp: int
    end_cp: int
    comment_id: int
    sequence: int


class ParagraphBuilder:
    def __init__(self, paragraph: ET.Element, style_pool: dict[str, ET.Element]) -> None:
        self._paragraph = paragraph
        self._style_pool = style_pool
        self._pending_text = ""
        self._pending_style_key = ""

    def _clone_rpr(self, style_key: str) -> ET.Element | None:
        base = self._style_pool.get(style_key)
        if base is None:
            return None
        return copy.deepcopy(base)

    def _flush_text(self) -> None:
        if not self._pending_text:
            return

        run = ET.Element(qn(W_NS, "r"))
        rpr = self._clone_rpr(self._pending_style_key)
        if rpr is not None:
            run.append(rpr)

        text_node = ET.SubElement(run, qn(W_NS, "t"))
        if _leading_or_trailing_space(self._pending_text):
            text_node.set(qn(XML_NS, "space"), "preserve")
        text_node.text = self._pending_text

        self._paragraph.append(run)
        self._pending_text = ""
        self._pending_style_key = ""

    def append_normal_token(self, token: CharToken) -> None:
        if token.kind == "text":
            if self._pending_text and self._pending_style_key == token.style_key:
                self._pending_text += token.char
                return

            self._flush_text()
            self._pending_text = token.char
            self._pending_style_key = token.style_key
            return

        self._flush_text()
        run = ET.Element(qn(W_NS, "r"))
        rpr = self._clone_rpr(token.style_key)
        if rpr is not None:
            run.append(rpr)

        if token.kind == "tab":
            ET.SubElement(run, qn(W_NS, "tab"))
        else:
            ET.SubElement(run, qn(W_NS, "br"))

        self._paragraph.append(run)

    def append_element(self, element: ET.Element) -> None:
        self._flush_text()
        self._paragraph.append(element)

    def finalize(self) -> None:
        self._flush_text()


def _serialize_rpr(rpr: ET.Element | None) -> str:
    if rpr is None:
        return ""
    return ET.tostring(rpr, encoding="unicode")


def _paragraph_tokens(paragraph: ET.Element) -> tuple[list[CharToken], dict[str, ET.Element]]:
    parent_map = _build_parent_map(paragraph)
    style_pool: dict[str, ET.Element] = {}
    run_style_cache: dict[int, str] = {}

    def style_key_for_node(node: ET.Element) -> str:
        run = _closest_ancestor_with_local_name(node, parent_map, "r")
        if run is None:
            return ""

        run_id = id(run)
        if run_id in run_style_cache:
            return run_style_cache[run_id]

        rpr = run.find("w:rPr", NS)
        key = _serialize_rpr(rpr)
        if rpr is not None and key not in style_pool:
            style_pool[key] = copy.deepcopy(rpr)
        run_style_cache[run_id] = key
        return key

    tokens: list[CharToken] = []
    for node in paragraph.iter():
        if node.tag == qn(W_NS, "t") and node.text:
            style_key = style_key_for_node(node)
            for char in node.text:
                tokens.append(CharToken(char=char, kind="text", style_key=style_key))
            continue

        if node.tag == qn(W_NS, "tab"):
            style_key = style_key_for_node(node)
            tokens.append(CharToken(char="\t", kind="tab", style_key=style_key))
            continue

        if node.tag in {qn(W_NS, "br"), qn(W_NS, "cr")}:
            style_key = style_key_for_node(node)
            tokens.append(CharToken(char="\n", kind="br", style_key=style_key))

    return tokens, style_pool


def _paragraph_text(paragraph: ET.Element) -> str:
    chunks: list[str] = []
    for node in paragraph.iter():
        if node.tag == qn(W_NS, "t") and node.text:
            chunks.append(node.text)
            continue
        if node.tag == qn(W_NS, "tab"):
            chunks.append("\t")
            continue
        if node.tag in {qn(W_NS, "br"), qn(W_NS, "cr")}:
            chunks.append("\n")
    return "".join(chunks)


def _clone_rpr_from_pool(style_pool: dict[str, ET.Element], style_key: str) -> ET.Element | None:
    if not style_key:
        return None
    base = style_pool.get(style_key)
    if base is None:
        return None
    return copy.deepcopy(base)


def _make_text_run(*, text: str, style_pool: dict[str, ET.Element], style_key: str, deleted: bool) -> ET.Element:
    run = ET.Element(qn(W_NS, "r"))
    rpr = _clone_rpr_from_pool(style_pool, style_key)
    if rpr is not None:
        run.append(rpr)

    tag = qn(W_NS, "delText") if deleted else qn(W_NS, "t")
    text_node = ET.SubElement(run, tag)
    if _leading_or_trailing_space(text):
        text_node.set(qn(XML_NS, "space"), "preserve")
    text_node.text = text
    return run


def _make_ins_element(
    *,
    event: InsertEvent,
    style_pool: dict[str, ET.Element],
    author: str,
    timestamp: str,
) -> ET.Element:
    ins = ET.Element(
        qn(W_NS, "ins"),
        {
            qn(W_NS, "id"): str(event.rev_id),
            qn(W_NS, "author"): author,
            qn(W_NS, "date"): timestamp,
        },
    )

    if event.text:
        ins.append(_make_text_run(text=event.text, style_pool=style_pool, style_key=event.style_key, deleted=False))
    return ins


def _make_del_element(
    *,
    event: DeleteEvent,
    style_pool: dict[str, ET.Element],
    author: str,
    timestamp: str,
) -> ET.Element:
    deleted = ET.Element(
        qn(W_NS, "del"),
        {
            qn(W_NS, "id"): str(event.rev_id),
            qn(W_NS, "author"): author,
            qn(W_NS, "date"): timestamp,
        },
    )

    deleted.append(_make_text_run(text=event.text, style_pool=style_pool, style_key=event.style_key, deleted=True))
    return deleted


def _make_comment_start(comment_id: int) -> ET.Element:
    return ET.Element(qn(W_NS, "commentRangeStart"), {qn(W_NS, "id"): str(comment_id)})


def _make_comment_end(comment_id: int) -> ET.Element:
    return ET.Element(qn(W_NS, "commentRangeEnd"), {qn(W_NS, "id"): str(comment_id)})


def _make_comment_reference_run(comment_id: int) -> ET.Element:
    run = ET.Element(qn(W_NS, "r"))
    ET.SubElement(run, qn(W_NS, "commentReference"), {qn(W_NS, "id"): str(comment_id)})
    return run


def _style_key_for_offset(tokens: list[CharToken], cp_offset: int) -> str:
    if 0 <= cp_offset < len(tokens):
        return tokens[cp_offset].style_key
    if cp_offset > 0 and (cp_offset - 1) < len(tokens):
        return tokens[cp_offset - 1].style_key
    for token in tokens:
        if token.style_key:
            return token.style_key
    return ""


def _ranges_overlap(left: tuple[int, int], right: tuple[int, int]) -> bool:
    return max(left[0], right[0]) < min(left[1], right[1])


def _parse_paragraph_index(unit: dict[str, Any]) -> int | None:
    location = unit.get("location")
    if isinstance(location, dict):
        paragraph_index = location.get("paragraph_index_in_part")
        try:
            if paragraph_index is not None:
                idx = int(paragraph_index)
                if idx >= 0:
                    return idx
        except (TypeError, ValueError):
            pass

        path_hint = location.get("path_hint")
        if isinstance(path_hint, str):
            match = re.search(r"\\.//w:p\[(\d+)\]", path_hint)
            if match:
                parsed = int(match.group(1)) - 1
                if parsed >= 0:
                    return parsed

    return None


def _build_locator_maps(review_units_payload: dict[str, Any]) -> tuple[
    dict[tuple[str, str, str], UnitLocator],
    dict[tuple[str, str], list[UnitLocator]],
]:
    units_raw = review_units_payload.get("units", [])
    if not isinstance(units_raw, list):
        raise ValueError("review_units.json must contain a list at key 'units'.")

    exact: dict[tuple[str, str, str], UnitLocator] = {}
    by_para: dict[tuple[str, str], list[UnitLocator]] = defaultdict(list)

    for unit in units_raw:
        if not isinstance(unit, dict):
            continue

        part = str(unit.get("part", "")).strip()
        para_id = str(unit.get("para_id", "")).strip()
        unit_uid = str(unit.get("unit_uid", "")).strip()
        if not part or not para_id:
            continue

        paragraph_index_in_part = _parse_paragraph_index(unit)
        if paragraph_index_in_part is None:
            continue

        order_index_raw = unit.get("order_index", 10**9)
        try:
            order_index = int(order_index_raw)
        except (TypeError, ValueError):
            order_index = 10**9

        locator = UnitLocator(
            part=part,
            para_id=para_id,
            unit_uid=unit_uid,
            order_index=order_index,
            paragraph_index_in_part=paragraph_index_in_part,
            accepted_text=str(unit.get("accepted_text", "")),
        )

        exact[(part, para_id, unit_uid)] = locator
        by_para[(part, para_id)].append(locator)

    for key in by_para:
        by_para[key].sort(key=lambda item: (item.order_index, item.unit_uid))

    return exact, by_para


def _ensure_comments_part(zip_entries: dict[str, bytes]) -> ET.Element:
    comments_name = str(COMMENTS_PART)
    if comments_name in zip_entries:
        return ET.fromstring(zip_entries[comments_name])

    return ET.Element(qn(W_NS, "comments"))


def _max_comment_id(comments_root: ET.Element) -> int:
    max_id = -1
    for node in comments_root.findall("w:comment", NS):
        raw = node.get(qn(W_NS, "id"))
        try:
            if raw is not None:
                max_id = max(max_id, int(raw))
        except ValueError:
            continue
    return max_id


def _max_revision_id(part_roots: dict[str, ET.Element]) -> int:
    max_id = -1
    for root in part_roots.values():
        for node in root.findall(".//w:ins", NS) + root.findall(".//w:del", NS):
            raw = node.get(qn(W_NS, "id"))
            try:
                if raw is not None:
                    max_id = max(max_id, int(raw))
            except ValueError:
                continue
    return max_id


def _ensure_content_types_override(content_types_root: ET.Element) -> None:
    for node in content_types_root.findall("ct:Override", NS):
        if node.get("PartName") == f"/{COMMENTS_PART.as_posix()}":
            return

    ET.SubElement(
        content_types_root,
        qn(CT_NS, "Override"),
        {
            "PartName": f"/{COMMENTS_PART.as_posix()}",
            "ContentType": COMMENTS_CONTENT_TYPE,
        },
    )


def _part_rels_path(part: str) -> str:
    part_path = PurePosixPath(part)
    if not part_path.name:
        raise ValueError(f"Invalid part path: {part}")
    rels_name = f"{part_path.name}.rels"
    return str(part_path.parent / "_rels" / rels_name)


def _relative_target(from_part: str, to_part: PurePosixPath) -> str:
    from_dir = PurePosixPath(from_part).parent
    parts = [p for p in PurePosixPath(*to_part.parts).parts]
    from_parts = [p for p in from_dir.parts]

    common = 0
    while common < len(parts) and common < len(from_parts) and parts[common] == from_parts[common]:
        common += 1

    up = [".."] * (len(from_parts) - common)
    down = parts[common:]
    target_parts = up + down
    if not target_parts:
        return "."
    return "/".join(target_parts)


def _ensure_comments_relationship(zip_entries: dict[str, bytes], part: str) -> None:
    rels_path = _part_rels_path(part)
    if rels_path in zip_entries:
        rels_root = ET.fromstring(zip_entries[rels_path])
    else:
        rels_root = ET.Element(qn(REL_NS, "Relationships"))

    for rel in rels_root.findall("rels:Relationship", NS):
        if rel.get("Type") == COMMENTS_REL_TYPE:
            return

    existing_ids = {rel.get("Id", "") for rel in rels_root.findall("rels:Relationship", NS)}
    next_id = 1
    while f"rId{next_id}" in existing_ids:
        next_id += 1

    target = _relative_target(from_part=part, to_part=COMMENTS_PART)
    ET.SubElement(
        rels_root,
        qn(REL_NS, "Relationship"),
        {
            "Id": f"rId{next_id}",
            "Type": COMMENTS_REL_TYPE,
            "Target": target,
        },
    )

    zip_entries[rels_path] = _xml_bytes(rels_root)


def _append_comment_body(
    comments_root: ET.Element,
    *,
    comment_id: int,
    comment_text: str,
    author: str,
    timestamp: str,
) -> None:
    comment = ET.SubElement(
        comments_root,
        qn(W_NS, "comment"),
        {
            qn(W_NS, "id"): str(comment_id),
            qn(W_NS, "author"): author,
            qn(W_NS, "date"): timestamp,
        },
    )

    paragraph = ET.SubElement(comment, qn(W_NS, "p"))
    run = ET.SubElement(paragraph, qn(W_NS, "r"))
    text = ET.SubElement(run, qn(W_NS, "t"))
    if _leading_or_trailing_space(comment_text):
        text.set(qn(XML_NS, "space"), "preserve")
    text.text = comment_text


def _normalize_op(raw_op: Any, op_index: int) -> tuple[dict[str, Any] | None, str | None]:
    if not isinstance(raw_op, dict):
        return None, "invalid_op_payload"

    op_type = str(raw_op.get("type", "")).strip()
    if op_type not in VALID_OP_TYPES:
        return None, "unsupported_op_type"

    target_raw = raw_op.get("target")
    if not isinstance(target_raw, dict):
        return None, "invalid_target"

    part = str(target_raw.get("part", "")).strip()
    para_id = str(target_raw.get("para_id", "")).strip()
    if not part or not para_id:
        return None, "missing_target_key"

    unit_uid = target_raw.get("unit_uid")
    unit_uid_str = str(unit_uid).strip() if unit_uid is not None else ""

    range_raw = raw_op.get("range")
    if not isinstance(range_raw, dict):
        return None, "invalid_range"

    try:
        start = _to_int(range_raw.get("start"), field="range.start")
        end = _to_int(range_raw.get("end"), field="range.end")
    except ValueError:
        return None, "invalid_range"

    if start < 0 or end < 0 or start > end:
        return None, "invalid_range"

    expected_raw = raw_op.get("expected")
    snippet = ""
    if isinstance(expected_raw, dict):
        snippet = str(expected_raw.get("snippet", ""))

    normalized: dict[str, Any] = {
        "op_index": op_index,
        "type": op_type,
        "target": {
            "part": part,
            "para_id": para_id,
        },
        "range": {
            "start": start,
            "end": end,
        },
        "expected": {
            "snippet": snippet,
        },
    }
    if unit_uid_str:
        normalized["target"]["unit_uid"] = unit_uid_str

    if op_type == "replace_range":
        if "replacement" not in raw_op:
            return None, "missing_replacement"
        normalized["replacement"] = str(raw_op.get("replacement", ""))
    elif op_type == "insert_at":
        text_value = raw_op.get("new_text", raw_op.get("text"))
        if text_value is None:
            return None, "missing_new_text"
        normalized["new_text"] = str(text_value)
    elif op_type == "add_comment":
        comment_text = raw_op.get("comment_text")
        if comment_text is None and isinstance(raw_op.get("comment"), dict):
            comment_text = raw_op["comment"].get("text")
        comment_text = "" if comment_text is None else str(comment_text)
        if not comment_text.strip():
            return None, "missing_comment_text"
        normalized["comment_text"] = comment_text

    return normalized, None


def _resolve_locator(
    *,
    target: dict[str, Any],
    exact_map: dict[tuple[str, str, str], UnitLocator],
    by_para_map: dict[tuple[str, str], list[UnitLocator]],
) -> tuple[UnitLocator | None, bool]:
    part = str(target.get("part", ""))
    para_id = str(target.get("para_id", ""))
    unit_uid = str(target.get("unit_uid", ""))

    if unit_uid:
        return exact_map.get((part, para_id, unit_uid)), False

    candidates = by_para_map.get((part, para_id), [])
    if not candidates:
        return None, False

    ambiguous = len(candidates) > 1
    return candidates[0], ambiguous


def _sort_ops_descending(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        records,
        key=lambda record: (
            int(record["normalized"]["range"]["start"]),
            int(record["normalized"]["range"]["end"]),
            -int(record["normalized"]["op_index"]),
        ),
        reverse=True,
    )


def _rebuild_paragraph(
    *,
    paragraph: ET.Element,
    tokens: list[CharToken],
    style_pool: dict[str, ET.Element],
    delete_by_start: dict[int, DeleteEvent],
    insert_pre: dict[int, list[InsertEvent]],
    insert_post: dict[int, list[InsertEvent]],
    comment_starts: dict[int, list[CommentEvent]],
    comment_ends: dict[int, list[CommentEvent]],
    author: str,
    timestamp: str,
) -> None:
    ppr = paragraph.find("w:pPr", NS)
    ppr_copy = copy.deepcopy(ppr) if ppr is not None else None

    for child in list(paragraph):
        paragraph.remove(child)

    if ppr_copy is not None:
        paragraph.append(ppr_copy)

    builder = ParagraphBuilder(paragraph, style_pool)
    token_count = len(tokens)

    pos = 0
    while True:
        for event in sorted(comment_starts.get(pos, []), key=lambda item: item.sequence):
            builder.append_element(_make_comment_start(event.comment_id))

        for event in sorted(insert_pre.get(pos, []), key=lambda item: item.sequence):
            builder.append_element(
                _make_ins_element(event=event, style_pool=style_pool, author=author, timestamp=timestamp)
            )

        delete_event = delete_by_start.get(pos)
        if delete_event is not None:
            builder.append_element(
                _make_del_element(event=delete_event, style_pool=style_pool, author=author, timestamp=timestamp)
            )

            for event in sorted(insert_post.get(pos, []), key=lambda item: item.sequence):
                builder.append_element(
                    _make_ins_element(event=event, style_pool=style_pool, author=author, timestamp=timestamp)
                )

            pos = delete_event.end_cp
            continue

        for event in sorted(comment_ends.get(pos, []), key=lambda item: item.sequence):
            builder.append_element(_make_comment_end(event.comment_id))
            builder.append_element(_make_comment_reference_run(event.comment_id))

        for event in sorted(insert_post.get(pos, []), key=lambda item: item.sequence):
            builder.append_element(
                _make_ins_element(event=event, style_pool=style_pool, author=author, timestamp=timestamp)
            )

        if pos >= token_count:
            break

        builder.append_normal_token(tokens[pos])
        pos += 1

    builder.finalize()


def apply_patch_to_output(
    *,
    input_docx: Path = DEFAULT_INPUT_DOCX,
    patch_path: Path = DEFAULT_PATCH_PATH,
    review_units_path: Path = DEFAULT_REVIEW_UNITS_PATH,
    output_docx: Path = DEFAULT_OUTPUT_DOCX,
    apply_log_path: Path = DEFAULT_APPLY_LOG_PATH,
    author: str = DEFAULT_AUTHOR,
) -> dict[str, Any]:
    patch_payload = load_json(patch_path)
    review_units_payload = load_json(review_units_path)

    if patch_payload.get("schema_version") != PATCH_SCHEMA_VERSION:
        raise ValueError(f"Unsupported patch schema_version: {patch_payload.get('schema_version')!r}")

    raw_ops = patch_payload.get("ops", [])
    if not isinstance(raw_ops, list):
        raise ValueError("merged_patch.json must contain a list at key 'ops'.")

    exact_map, by_para_map = _build_locator_maps(review_units_payload)

    with zipfile.ZipFile(input_docx, mode="r") as zf:
        zip_entries = {name: zf.read(name) for name in zf.namelist()}

    grouped: OrderedDict[tuple[str, str, str], list[dict[str, Any]]] = OrderedDict()
    log_entries: list[dict[str, Any]] = []

    for op_index, raw_op in enumerate(raw_ops):
        entry: dict[str, Any] = {
            "op_index": op_index,
            "status": "skipped",
            "reason": None,
        }

        normalized, normalize_error = _normalize_op(raw_op, op_index)
        if normalized is None:
            entry["type"] = raw_op.get("type") if isinstance(raw_op, dict) else None
            entry["reason"] = normalize_error
            log_entries.append(entry)
            continue

        entry["type"] = normalized["type"]
        entry["target"] = normalized["target"]
        entry["range"] = normalized["range"]
        entry["expected"] = normalized["expected"]

        locator, ambiguous = _resolve_locator(
            target=normalized["target"],
            exact_map=exact_map,
            by_para_map=by_para_map,
        )
        if locator is None:
            entry["reason"] = "target_not_found"
            log_entries.append(entry)
            continue

        if locator.part not in zip_entries:
            entry["reason"] = "target_part_missing_in_docx"
            log_entries.append(entry)
            continue

        entry["resolved_target"] = {
            "part": locator.part,
            "para_id": locator.para_id,
            "unit_uid": locator.unit_uid,
            "paragraph_index_in_part": locator.paragraph_index_in_part,
        }
        if ambiguous and "unit_uid" not in normalized["target"]:
            entry["resolution_note"] = "multiple review units share target para_id; selected first by order_index"

        record = {
            "normalized": normalized,
            "locator": locator,
            "log_entry": entry,
        }

        group_key = (locator.part, locator.para_id, locator.unit_uid)
        grouped.setdefault(group_key, []).append(record)
        log_entries.append(entry)

    part_roots: dict[str, ET.Element] = {}
    part_paragraphs: dict[str, list[ET.Element]] = {}
    for part, _, _ in grouped.keys():
        root = ET.fromstring(zip_entries[part])
        part_roots[part] = root
        part_paragraphs[part] = root.findall(".//w:p", NS)

    comments_root = _ensure_comments_part(zip_entries)
    next_comment_id = _max_comment_id(comments_root) + 1
    next_revision_id = _max_revision_id(part_roots) + 1

    sequence_counter = 0
    parts_modified: set[str] = set()
    parts_with_comments: set[str] = set()

    for group_records in grouped.values():
        locator: UnitLocator = group_records[0]["locator"]
        paragraphs = part_paragraphs.get(locator.part, [])
        if locator.paragraph_index_in_part < 0 or locator.paragraph_index_in_part >= len(paragraphs):
            for record in group_records:
                entry = record["log_entry"]
                entry["status"] = "skipped"
                entry["reason"] = "target_paragraph_not_found"
            continue

        paragraph = paragraphs[locator.paragraph_index_in_part]
        tokens, style_pool = _paragraph_tokens(paragraph)
        paragraph_text = "".join(token.char for token in tokens)
        _, utf16_to_cp = _utf16_map(paragraph_text)

        delete_by_start: dict[int, DeleteEvent] = {}
        insert_pre: dict[int, list[InsertEvent]] = defaultdict(list)
        insert_post: dict[int, list[InsertEvent]] = defaultdict(list)
        comment_starts: dict[int, list[CommentEvent]] = defaultdict(list)
        comment_ends: dict[int, list[CommentEvent]] = defaultdict(list)
        occupied_delete_ranges: list[tuple[int, int]] = []

        any_applied = False
        sorted_records = _sort_ops_descending(group_records)

        for record in sorted_records:
            normalized = record["normalized"]
            entry = record["log_entry"]
            op_type = normalized["type"]
            start_u16 = int(normalized["range"]["start"])
            end_u16 = int(normalized["range"]["end"])

            if start_u16 not in utf16_to_cp or end_u16 not in utf16_to_cp:
                entry["status"] = "skipped"
                entry["reason"] = "invalid_utf16_boundary"
                continue

            start_cp = utf16_to_cp[start_u16]
            end_cp = utf16_to_cp[end_u16]
            if start_cp < 0 or end_cp < start_cp or end_cp > len(tokens):
                entry["status"] = "skipped"
                entry["reason"] = "range_out_of_bounds"
                continue

            actual_snippet = paragraph_text[start_cp:end_cp]
            expected_snippet = str(normalized["expected"].get("snippet", ""))
            if actual_snippet != expected_snippet:
                entry["status"] = "skipped"
                entry["reason"] = "snippet_mismatch"
                entry["actual_snippet"] = actual_snippet
                continue

            if op_type == "insert_at" and start_cp != end_cp:
                entry["status"] = "skipped"
                entry["reason"] = "insert_requires_collapsed_range"
                continue

            if op_type in {"replace_range", "delete_range"} and start_cp == end_cp:
                entry["status"] = "skipped"
                entry["reason"] = "empty_edit_range"
                continue

            if op_type in {"replace_range", "delete_range"}:
                current_range = (start_cp, end_cp)
                if any(_ranges_overlap(current_range, used_range) for used_range in occupied_delete_ranges):
                    entry["status"] = "skipped"
                    entry["reason"] = "overlapping_edit_in_group"
                    continue

                if start_cp in delete_by_start:
                    entry["status"] = "skipped"
                    entry["reason"] = "duplicate_delete_start"
                    continue

                style_key = _style_key_for_offset(tokens, start_cp)
                deletion_text = paragraph_text[start_cp:end_cp]
                delete_by_start[start_cp] = DeleteEvent(
                    start_cp=start_cp,
                    end_cp=end_cp,
                    text=deletion_text,
                    style_key=style_key,
                    rev_id=next_revision_id,
                )
                next_revision_id += 1
                occupied_delete_ranges.append(current_range)
                entry.setdefault("revision_ids", []).append(delete_by_start[start_cp].rev_id)

            if op_type in {"replace_range", "insert_at"}:
                insert_text = str(normalized.get("replacement") if op_type == "replace_range" else normalized.get("new_text"))
                if insert_text:
                    style_key = _style_key_for_offset(tokens, start_cp)
                    insert_event = InsertEvent(
                        pos_cp=start_cp,
                        text=insert_text,
                        style_key=style_key,
                        rev_id=next_revision_id,
                        sequence=sequence_counter,
                        after_delete=(op_type == "replace_range"),
                    )
                    sequence_counter += 1
                    next_revision_id += 1

                    bucket = insert_post if insert_event.after_delete else insert_pre
                    bucket[start_cp].append(insert_event)
                    entry.setdefault("revision_ids", []).append(insert_event.rev_id)

            if op_type == "add_comment":
                comment_text = str(normalized.get("comment_text", "")).strip()
                if not comment_text:
                    entry["status"] = "skipped"
                    entry["reason"] = "missing_comment_text"
                    continue

                comment_event = CommentEvent(
                    start_cp=start_cp,
                    end_cp=end_cp,
                    comment_id=next_comment_id,
                    sequence=sequence_counter,
                )
                sequence_counter += 1
                next_comment_id += 1

                comment_starts[start_cp].append(comment_event)
                comment_ends[end_cp].append(comment_event)
                _append_comment_body(
                    comments_root,
                    comment_id=comment_event.comment_id,
                    comment_text=comment_text,
                    author=author,
                    timestamp=_now_iso(),
                )
                parts_with_comments.add(locator.part)
                entry["comment_id"] = comment_event.comment_id

            entry["status"] = "applied"
            entry["reason"] = None
            entry["actual_snippet"] = actual_snippet
            any_applied = True

        if not any_applied:
            continue

        _rebuild_paragraph(
            paragraph=paragraph,
            tokens=tokens,
            style_pool=style_pool,
            delete_by_start=delete_by_start,
            insert_pre=insert_pre,
            insert_post=insert_post,
            comment_starts=comment_starts,
            comment_ends=comment_ends,
            author=author,
            timestamp=_now_iso(),
        )
        parts_modified.add(locator.part)

    for part in sorted(parts_modified):
        zip_entries[part] = _xml_bytes(part_roots[part])

    if parts_with_comments:
        zip_entries[COMMENTS_PART.as_posix()] = _xml_bytes(comments_root)

        content_types_name = "[Content_Types].xml"
        if content_types_name not in zip_entries:
            raise ValueError("Missing [Content_Types].xml in source DOCX")
        content_types_root = ET.fromstring(zip_entries[content_types_name])
        _ensure_content_types_override(content_types_root)
        zip_entries[content_types_name] = _xml_bytes(content_types_root)

        for part in sorted(parts_with_comments):
            _ensure_comments_relationship(zip_entries, part)

    output_docx.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output_docx, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name in sorted(zip_entries.keys()):
            zf.writestr(name, zip_entries[name])

    applied_ops = sum(1 for item in log_entries if item.get("status") == "applied")
    skipped_ops = len(log_entries) - applied_ops
    applied_edit_ops = sum(
        1 for item in log_entries if item.get("status") == "applied" and item.get("type") in EDIT_OP_TYPES
    )
    applied_comment_ops = sum(
        1 for item in log_entries if item.get("status") == "applied" and item.get("type") == "add_comment"
    )

    apply_log_payload: dict[str, Any] = {
        "schema_version": APPLY_LOG_SCHEMA_VERSION,
        "created_at": _now_iso(),
        "source_docx": str(input_docx),
        "source_patch": str(patch_path),
        "source_review_units": str(review_units_path),
        "output_docx": str(output_docx),
        "stats": {
            "input_ops": len(raw_ops),
            "applied_ops": applied_ops,
            "skipped_ops": skipped_ops,
            "applied_edit_ops": applied_edit_ops,
            "applied_comment_ops": applied_comment_ops,
            "parts_modified": sorted(parts_modified),
            "parts_with_comments": sorted(parts_with_comments),
        },
        "ops": sorted(log_entries, key=lambda item: int(item.get("op_index", 10**9))),
    }

    dump_json(apply_log_path, apply_log_payload)

    return {
        "output_docx": output_docx,
        "apply_log": apply_log_path,
        "stats": apply_log_payload["stats"],
    }
