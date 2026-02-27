#!/usr/bin/env python3
"""Extract structured proofreading units from a DOCX file.

Outputs:
- docx_struct.json
- review_units.json
- chunks.json
- chunks/chunk_XXX.json
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import posixpath
import re
import zipfile
from pathlib import Path
from typing import Iterable
import xml.etree.ElementTree as ET

W_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
NS = {"w": W_NS}


def qn(tag: str) -> str:
    prefix, local = tag.split(":", 1)
    if prefix == "w":
        return f"{{{W_NS}}}{local}"
    raise ValueError(f"Unsupported namespace prefix: {prefix}")


def local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def discover_parts(zf: zipfile.ZipFile) -> list[str]:
    names = set(zf.namelist())
    parts: list[str] = []

    def add_part(name: str) -> None:
        if name in names and name not in parts:
            parts.append(name)

    add_part("word/document.xml")

    rels_path = "word/_rels/document.xml.rels"
    if rels_path in names:
        rel_root = ET.fromstring(zf.read(rels_path))
        for rel in rel_root.findall(f"{{{REL_NS}}}Relationship"):
            r_type = rel.attrib.get("Type", "")
            target = rel.attrib.get("Target", "")
            if not target or "externalLink" in r_type:
                continue
            if not any(
                key in r_type
                for key in ("/header", "/footer", "/footnotes", "/endnotes", "/comments")
            ):
                continue
            if target.startswith("/"):
                part_name = target.lstrip("/")
            else:
                part_name = posixpath.normpath(posixpath.join("word", target))
            add_part(part_name)

    for extra in sorted(names):
        if re.fullmatch(r"word/header\d+\.xml", extra):
            add_part(extra)
        elif re.fullmatch(r"word/footer\d+\.xml", extra):
            add_part(extra)
        elif extra in ("word/footnotes.xml", "word/endnotes.xml", "word/comments.xml"):
            add_part(extra)

    return parts


def walk_paragraphs(node: ET.Element, in_table: bool = False) -> Iterable[tuple[ET.Element, bool]]:
    here_is_table = in_table or (local_name(node.tag) == "tbl")
    if local_name(node.tag) == "p":
        yield node, here_is_table
    for child in list(node):
        yield from walk_paragraphs(child, here_is_table)


def extract_text(node: ET.Element, in_deleted: bool = False) -> str:
    tag = local_name(node.tag)
    deleted_here = in_deleted or tag in {"del", "moveFrom"}
    pieces: list[str] = []

    if tag == "t" and node.text and not deleted_here:
        pieces.append(node.text)
    elif tag in {"tab"} and not deleted_here:
        pieces.append("\t")
    elif tag in {"br", "cr"} and not deleted_here:
        pieces.append("\n")

    for child in list(node):
        pieces.append(extract_text(child, deleted_here))
    return "".join(pieces)


def normalize_text(text: str) -> str:
    text = text.replace("\u00a0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()


def get_style_name(p: ET.Element) -> str | None:
    style_el = p.find("./w:pPr/w:pStyle", NS)
    if style_el is None:
        return None
    return style_el.attrib.get(qn("w:val"))


def get_unit_type(p: ET.Element, in_table: bool) -> str:
    style = (get_style_name(p) or "").lower()
    has_num = p.find("./w:pPr/w:numPr", NS) is not None
    if in_table:
        return "table_cell"
    if has_num:
        return "list_item"
    if style.startswith("heading") or style.startswith("titre"):
        return "heading"
    return "paragraph"


def complexity_flags(text: str) -> list[str]:
    flags: list[str] = []
    word_count = len(re.findall(r"\w+", text))
    if word_count > 120:
        flags.append("long_paragraph")
    if text.count("(") >= 4:
        flags.append("many_parentheses")
    if len(re.findall(r"[;:]", text)) >= 6:
        flags.append("dense_punctuation")
    return flags


def build_chunks(units: list[dict], max_words: int = 420, max_units: int = 18) -> list[dict]:
    chunks: list[dict] = []
    current_units: list[dict] = []
    current_words = 0
    current_chars = 0

    def flush() -> None:
        nonlocal current_units, current_words, current_chars
        if not current_units:
            return
        cid = f"chunk_{len(chunks) + 1:03d}"
        chunk_units = [
            {
                "uid": u["uid"],
                "order": u["order"],
                "part": u["part"],
                "para_id": u["para_id"],
                "accepted_text": u["accepted_text"],
                "word_count": u["word_count"],
            }
            for u in current_units
        ]
        chunks.append(
            {
                "chunk_id": cid,
                "start_order": current_units[0]["order"],
                "end_order": current_units[-1]["order"],
                "unit_uids": [u["uid"] for u in current_units],
                "word_count": current_words,
                "char_count": current_chars,
                "units": chunk_units,
            }
        )
        current_units = []
        current_words = 0
        current_chars = 0

    for unit in units:
        u_words = unit["word_count"]
        u_chars = len(unit["accepted_text"])
        if current_units and (
            len(current_units) >= max_units or current_words + u_words > max_words
        ):
            flush()
        current_units.append(unit)
        current_words += u_words
        current_chars += u_chars

    flush()
    return chunks


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract DOCX proofreading units")
    parser.add_argument("input_docx", type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()

    in_path = args.input_docx
    out_dir = args.out
    out_dir.mkdir(parents=True, exist_ok=True)
    chunks_dir = out_dir / "chunks"
    chunks_dir.mkdir(parents=True, exist_ok=True)

    review_units: list[dict] = []
    doc_parts_info: list[dict] = []
    order = 0

    with zipfile.ZipFile(in_path, "r") as zf:
        parts = discover_parts(zf)
        for part in parts:
            xml_bytes = zf.read(part)
            root = ET.fromstring(xml_bytes)
            paragraphs = list(walk_paragraphs(root))
            reviewable_count = 0
            for idx, (p, in_table) in enumerate(paragraphs):
                text = normalize_text(extract_text(p))
                style = get_style_name(p)
                if not text:
                    continue
                reviewable_count += 1
                uid = f"{part}::p{idx}"
                unit = {
                    "uid": uid,
                    "order": order,
                    "part": part,
                    "para_id": f"p{idx}",
                    "unit_type": get_unit_type(p, in_table),
                    "style": style,
                    "accepted_text": text,
                    "word_count": len(re.findall(r"\w+", text)),
                    "char_count": len(text),
                    "complexity_flags": complexity_flags(text),
                }
                review_units.append(unit)
                order += 1
            doc_parts_info.append(
                {
                    "part": part,
                    "paragraph_count": len(paragraphs),
                    "reviewable_units": reviewable_count,
                }
            )

    chunks = build_chunks(review_units)

    docx_struct = {
        "source_file": str(in_path),
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "total_units": len(review_units),
        "parts": doc_parts_info,
    }

    (out_dir / "docx_struct.json").write_text(
        json.dumps(docx_struct, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (out_dir / "review_units.json").write_text(
        json.dumps(review_units, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (out_dir / "chunks.json").write_text(
        json.dumps(chunks, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    for chunk in chunks:
        chunk_path = chunks_dir / f"{chunk['chunk_id']}.json"
        chunk_path.write_text(
            json.dumps(chunk, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    print(
        f"Extraction complete: {len(review_units)} units, {len(chunks)} chunks -> {out_dir}"
    )


if __name__ == "__main__":
    main()
