#!/usr/bin/env python3
"""DOCX OOXML extraction helpers for review artifact generation."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
from typing import Any
import xml.etree.ElementTree as ET
import zipfile

W_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
W14_NS = "http://schemas.microsoft.com/office/word/2010/wordml"
NS = {"w": W_NS, "w14": W14_NS}


def qn(ns: str, local: str) -> str:
    return f"{{{ns}}}{local}"


@dataclass(frozen=True)
class PartSpec:
    part: str
    part_kind: str
    part_name: str


def _stable_hash(text: str, length: int) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:length]


def _discover_story_parts(zf: zipfile.ZipFile) -> list[PartSpec]:
    names = set(zf.namelist())
    parts: list[PartSpec] = []

    def numbered_part_sort_key(path: str, prefix: str) -> tuple[int, str]:
        match = re.fullmatch(fr"word/{prefix}(\d*)\.xml", path)
        if not match:
            return (10_000, path)
        suffix = match.group(1)
        if not suffix:
            return (0, path)
        return (int(suffix), path)

    if "word/document.xml" in names:
        parts.append(PartSpec(part="word/document.xml", part_kind="body", part_name="document"))

    header_paths = sorted(
        [name for name in names if re.fullmatch(r"word/header\d*\.xml", name)],
        key=lambda value: numbered_part_sort_key(value, "header"),
    )
    for part in header_paths:
        parts.append(PartSpec(part=part, part_kind="header", part_name=Path(part).stem))

    footer_paths = sorted(
        [name for name in names if re.fullmatch(r"word/footer\d*\.xml", name)],
        key=lambda value: numbered_part_sort_key(value, "footer"),
    )
    for part in footer_paths:
        parts.append(PartSpec(part=part, part_kind="footer", part_name=Path(part).stem))

    if "word/footnotes.xml" in names:
        parts.append(PartSpec(part="word/footnotes.xml", part_kind="footnotes", part_name="footnotes"))

    if "word/endnotes.xml" in names:
        parts.append(PartSpec(part="word/endnotes.xml", part_kind="endnotes", part_name="endnotes"))

    return parts


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


def _paragraph_style(paragraph: ET.Element) -> str | None:
    style_node = paragraph.find("w:pPr/w:pStyle", NS)
    if style_node is None:
        return None
    return style_node.get(qn(W_NS, "val"))


def _heading_level(style_id: str | None) -> int | None:
    if not style_id:
        return None
    compact = re.sub(r"[^a-z0-9]", "", style_id.strip().lower())
    match = re.match(r"heading([1-9][0-9]*)", compact)
    if not match:
        return None
    return int(match.group(1))


def _native_para_id(paragraph: ET.Element) -> str | None:
    native = paragraph.get(qn(W14_NS, "paraId"))
    if native:
        return native
    for key, value in paragraph.attrib.items():
        if key.endswith("}paraId") and value:
            return value
    return None


def _build_parent_map(root: ET.Element) -> dict[ET.Element, ET.Element]:
    return {child: parent for parent in root.iter() for child in parent}


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


def _has_ancestor(node: ET.Element, parent_map: dict[ET.Element, ET.Element], local_name: str) -> bool:
    cursor = node
    while cursor in parent_map:
        cursor = parent_map[cursor]
        if _local_name(cursor.tag) == local_name:
            return True
    return False


def _closest_note_ancestor(node: ET.Element, parent_map: dict[ET.Element, ET.Element]) -> tuple[str | None, str | None]:
    cursor = node
    while cursor in parent_map:
        cursor = parent_map[cursor]
        local = _local_name(cursor.tag)
        if local == "footnote":
            return "footnote", cursor.get(qn(W_NS, "id"))
        if local == "endnote":
            return "endnote", cursor.get(qn(W_NS, "id"))
    return None, None


def _json_dump(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def extract_docx_payloads(input_docx: Path) -> dict[str, dict[str, Any]]:
    with zipfile.ZipFile(input_docx, mode="r") as zf:
        parts = _discover_story_parts(zf)

        review_units: list[dict[str, Any]] = []
        docx_parts: list[dict[str, Any]] = []
        order_index = 0

        for part_index, part in enumerate(parts):
            xml_root = ET.fromstring(zf.read(part.part))
            parent_map = _build_parent_map(xml_root)
            paragraphs = xml_root.findall(".//w:p", NS)

            part_heading_stack: list[str] = []
            struct_paragraphs: list[dict[str, Any]] = []
            seen_para_keys: dict[str, int] = {}

            for paragraph_index, paragraph in enumerate(paragraphs):
                accepted_text = _paragraph_text(paragraph)
                style_id = _paragraph_style(paragraph)
                heading_level = _heading_level(style_id)

                stripped_text = accepted_text.strip()
                if heading_level is not None and stripped_text:
                    keep = max(heading_level - 1, 0)
                    part_heading_stack = part_heading_stack[:keep]
                    part_heading_stack.append(stripped_text)

                heading_path = list(part_heading_stack)
                native_para_id = _native_para_id(paragraph)
                para_key = (
                    f"{part.part}|native:{native_para_id.lower()}"
                    if native_para_id
                    else f"{part.part}|index:{paragraph_index}"
                )
                para_occurrence = seen_para_keys.get(para_key, 0)
                seen_para_keys[para_key] = para_occurrence + 1
                para_seed = para_key if para_occurrence == 0 else f"{para_key}|dup:{para_occurrence}"
                para_id = f"para_{_stable_hash(para_seed, 16)}"
                unit_uid = f"unit_{_stable_hash(f'{para_id}|unit:0', 12)}"

                note_kind, note_id = _closest_note_ancestor(paragraph, parent_map)

                location: dict[str, Any] = {
                    "part_index": part_index,
                    "paragraph_index_in_part": paragraph_index,
                    "global_order_index": order_index,
                    "path_hint": f"{part.part}::.//w:p[{paragraph_index + 1}]",
                    "in_table": _has_ancestor(paragraph, parent_map, "tbl"),
                }
                if native_para_id:
                    location["native_para_id"] = native_para_id
                if note_kind and note_id is not None:
                    location["note_kind"] = note_kind
                    location["note_id"] = note_id

                unit = {
                    "part": part.part,
                    "part_kind": part.part_kind,
                    "part_name": part.part_name,
                    "para_id": para_id,
                    "unit_uid": unit_uid,
                    "accepted_text": accepted_text,
                    "heading_path": heading_path,
                    "order_index": order_index,
                    "location": location,
                }
                review_units.append(unit)
                struct_paragraphs.append(
                    {
                        "para_id": para_id,
                        "unit_uid": unit_uid,
                        "accepted_text": accepted_text,
                        "style_id": style_id,
                        "heading_level": heading_level,
                        "heading_path": heading_path,
                        "order_index": order_index,
                        "location": location,
                    }
                )
                order_index += 1

            docx_parts.append(
                {
                    "part": part.part,
                    "part_kind": part.part_kind,
                    "part_name": part.part_name,
                    "part_order_index": part_index,
                    "paragraph_count": len(struct_paragraphs),
                    "paragraphs": struct_paragraphs,
                }
            )

    payload_common = {
        "source_docx": str(input_docx),
        "part_count": len(docx_parts),
        "unit_count": len(review_units),
    }

    review_units_payload: dict[str, Any] = {**payload_common, "units": review_units}
    ordered_unit_uids = [unit["unit_uid"] for unit in review_units]
    linear_order = [
        {
            "order_index": unit["order_index"],
            "part": unit["part"],
            "part_kind": unit["part_kind"],
            "part_name": unit["part_name"],
            "para_id": unit["para_id"],
            "unit_uid": unit["unit_uid"],
        }
        for unit in review_units
    ]
    linear_units_payload: dict[str, Any] = {
        **payload_common,
        "unit_uids": ordered_unit_uids,
        "units": ordered_unit_uids,
        "order": linear_order,
    }
    docx_struct_payload: dict[str, Any] = {**payload_common, "parts": docx_parts}

    return {
        "review_units.json": review_units_payload,
        "linear_units.json": linear_units_payload,
        "docx_struct.json": docx_struct_payload,
    }


def extract_docx_to_artifacts(input_docx: Path, output_dir: Path) -> dict[str, Path]:
    payloads = extract_docx_payloads(input_docx=input_docx)
    written: dict[str, Path] = {}
    for file_name, payload in payloads.items():
        output_path = output_dir / file_name
        _json_dump(output_path, payload)
        written[file_name] = output_path
    return written
