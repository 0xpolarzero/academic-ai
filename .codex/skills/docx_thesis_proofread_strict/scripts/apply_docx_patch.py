#!/usr/bin/env python3
"""Apply proofreading patch operations to DOCX.

Supported ops:
- add_comment (implemented)
- replace_range / insert_at / delete_range (ignored with warning)
"""

from __future__ import annotations

import argparse
import copy
import datetime as dt
import json
import posixpath
import re
import zipfile
from pathlib import Path
import xml.etree.ElementTree as ET

W_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
P_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
O_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
CT_NS = "http://schemas.openxmlformats.org/package/2006/content-types"


ET.register_namespace("w", W_NS)
ET.register_namespace("r", O_REL_NS)


def qn(ns: str, local: str) -> str:
    if ns == "w":
        return f"{{{W_NS}}}{local}"
    if ns == "pr":
        return f"{{{P_REL_NS}}}{local}"
    if ns == "ct":
        return f"{{{CT_NS}}}{local}"
    raise ValueError(f"Unsupported ns key: {ns}")


def local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def norm_text(text: str) -> str:
    text = text.replace("\u00a0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()


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


def ensure_part_tree(trees: dict[str, ET.ElementTree], files: dict[str, bytes], part: str) -> ET.ElementTree:
    if part in trees:
        return trees[part]
    if part not in files:
        raise KeyError(f"Part not found in DOCX: {part}")
    tree = ET.ElementTree(ET.fromstring(files[part]))
    trees[part] = tree
    return tree


def get_paragraph(root: ET.Element, para_id: str) -> ET.Element:
    m = re.fullmatch(r"p(\d+)", para_id)
    if not m:
        raise ValueError(f"Invalid para_id: {para_id}")
    index = int(m.group(1))
    paragraphs = list(root.iter(qn("w", "p")))
    if index < 0 or index >= len(paragraphs):
        raise IndexError(f"Paragraph index out of range: {para_id}")
    return paragraphs[index]


def next_comment_id(comments_root: ET.Element) -> int:
    ids: list[int] = []
    for comment in comments_root.findall(qn("w", "comment")):
        cid = comment.attrib.get(qn("w", "id"))
        if cid is not None and cid.isdigit():
            ids.append(int(cid))
    return (max(ids) + 1) if ids else 0


def ensure_comments_in_content_types(ct_tree: ET.ElementTree) -> None:
    root = ct_tree.getroot()
    for ov in root.findall(qn("ct", "Override")):
        if ov.attrib.get("PartName") == "/word/comments.xml":
            return
    ET.SubElement(
        root,
        qn("ct", "Override"),
        {
            "PartName": "/word/comments.xml",
            "ContentType": "application/vnd.openxmlformats-officedocument.wordprocessingml.comments+xml",
        },
    )


def ensure_comments_rel(rels_tree: ET.ElementTree) -> None:
    root = rels_tree.getroot()
    for rel in root.findall(qn("pr", "Relationship")):
        if rel.attrib.get("Type") == f"{O_REL_NS}/comments":
            return
    ids = [
        int(m.group(1))
        for rel in root.findall(qn("pr", "Relationship"))
        for m in [re.fullmatch(r"rId(\d+)", rel.attrib.get("Id", ""))]
        if m
    ]
    next_id = (max(ids) + 1) if ids else 1
    ET.SubElement(
        root,
        qn("pr", "Relationship"),
        {
            "Id": f"rId{next_id}",
            "Type": f"{O_REL_NS}/comments",
            "Target": "comments.xml",
        },
    )


def ensure_comments_part(trees: dict[str, ET.ElementTree], files: dict[str, bytes]) -> ET.ElementTree:
    if "word/comments.xml" in files:
        return ensure_part_tree(trees, files, "word/comments.xml")

    comments_root = ET.Element(qn("w", "comments"))
    comments_tree = ET.ElementTree(comments_root)
    trees["word/comments.xml"] = comments_tree
    files["word/comments.xml"] = ET.tostring(comments_root, encoding="utf-8", xml_declaration=True)

    rels_path = "word/_rels/document.xml.rels"
    if rels_path in files:
        rels_tree = ensure_part_tree(trees, files, rels_path)
    else:
        rels_root = ET.Element(qn("pr", "Relationships"))
        rels_tree = ET.ElementTree(rels_root)
        trees[rels_path] = rels_tree
        files[rels_path] = ET.tostring(rels_root, encoding="utf-8", xml_declaration=True)
    ensure_comments_rel(rels_tree)

    ct_path = "[Content_Types].xml"
    if ct_path in files:
        ct_tree = ensure_part_tree(trees, files, ct_path)
        ensure_comments_in_content_types(ct_tree)

    return comments_tree


def comment_paragraph(
    part_tree: ET.ElementTree,
    comments_tree: ET.ElementTree,
    para_id: str,
    comment_text: str,
    author: str,
    expected_snippet: str | None,
) -> str | None:
    root = part_tree.getroot()
    try:
        p = get_paragraph(root, para_id)
    except (ValueError, IndexError) as exc:
        return f"target_not_found:{exc}"

    p_text = norm_text(extract_text(p))
    if expected_snippet:
        if norm_text(expected_snippet) not in p_text:
            return "expected_snippet_not_found"

    comments_root = comments_tree.getroot()
    cid = next_comment_id(comments_root)

    comment_el = ET.SubElement(
        comments_root,
        qn("w", "comment"),
        {
            qn("w", "id"): str(cid),
            qn("w", "author"): author,
            qn("w", "date"): dt.datetime.now(dt.timezone.utc).isoformat(),
        },
    )
    cp = ET.SubElement(comment_el, qn("w", "p"))
    cr = ET.SubElement(cp, qn("w", "r"))
    ct = ET.SubElement(cr, qn("w", "t"))
    ct.text = comment_text

    start = ET.Element(qn("w", "commentRangeStart"), {qn("w", "id"): str(cid)})
    end = ET.Element(qn("w", "commentRangeEnd"), {qn("w", "id"): str(cid)})
    ref_r = ET.Element(qn("w", "r"))
    ref_rpr = ET.SubElement(ref_r, qn("w", "rPr"))
    ET.SubElement(ref_rpr, qn("w", "rStyle"), {qn("w", "val"): "CommentReference"})
    ET.SubElement(ref_r, qn("w", "commentReference"), {qn("w", "id"): str(cid)})

    insert_idx = 1 if len(p) > 0 and local_name(p[0].tag) == "pPr" else 0
    p.insert(insert_idx, start)
    p.append(end)
    p.append(ref_r)

    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply DOCX proofreading patch")
    parser.add_argument("input_docx", type=Path)
    parser.add_argument("patch_json", type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()

    patch = json.loads(args.patch_json.read_text(encoding="utf-8"))
    if patch.get("schema_version") != 1:
        raise ValueError("Unsupported patch schema_version, expected 1")

    author = patch.get("author") or "Codex"
    ops = patch.get("ops") or []

    files: dict[str, bytes] = {}
    trees: dict[str, ET.ElementTree] = {}

    with zipfile.ZipFile(args.input_docx, "r") as zin:
        for name in zin.namelist():
            files[name] = zin.read(name)

    comments_tree = ensure_comments_part(trees, files)

    warnings: list[str] = []
    applied = 0

    for op in ops:
        op_id = op.get("op_id", "unknown_op")
        op_type = op.get("type")

        if op_type != "add_comment":
            warnings.append(f"{op_id}:unsupported_op_type:{op_type}")
            continue

        target = op.get("target") or {}
        part = target.get("part")
        para_id = target.get("para_id")
        if not part or not para_id:
            warnings.append(f"{op_id}:missing_target")
            continue

        if part not in files:
            warnings.append(f"{op_id}:part_not_found:{part}")
            continue

        part_tree = ensure_part_tree(trees, files, part)
        expected = (op.get("expected") or {}).get("snippet")
        comment_text = op.get("comment_text") or "Point de relecture"

        err = comment_paragraph(
            part_tree=part_tree,
            comments_tree=comments_tree,
            para_id=para_id,
            comment_text=comment_text,
            author=author,
            expected_snippet=expected,
        )
        if err:
            warnings.append(f"{op_id}:{err}")
            continue

        applied += 1

    for part_name, tree in trees.items():
        root = tree.getroot()
        files[part_name] = ET.tostring(root, encoding="utf-8", xml_declaration=True)

    with zipfile.ZipFile(args.out, "w", compression=zipfile.ZIP_DEFLATED) as zout:
        for name, payload in files.items():
            zout.writestr(name, payload)

    print(f"Patch applied: {applied} operations")
    if warnings:
        print("Warnings:")
        for w in warnings:
            print(f"- {w}")


if __name__ == "__main__":
    main()
