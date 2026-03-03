from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import xml.etree.ElementTree as ET
import zipfile


REPO_ROOT = Path(__file__).resolve().parents[1]
APPLY_SCRIPT = REPO_ROOT / ".codex/skills/docx_apply_patch_to_output/scripts/apply_docx_patch.py"

W_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
CT_NS = "http://schemas.openxmlformats.org/package/2006/content-types"

COMMENTS_CONTENT_TYPE = "application/vnd.openxmlformats-officedocument.wordprocessingml.comments+xml"
COMMENTS_REL_TYPE = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/comments"

NS = {"w": W_NS, "rels": REL_NS, "ct": CT_NS}


def qn(ns: str, local: str) -> str:
    return f"{{{ns}}}{local}"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _build_minimal_docx(path: Path, text: str) -> None:
    content_types = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
</Types>
"""

    root_rels = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>
</Relationships>
"""

    document_rels = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"/>
"""

    left, right = text.split(" ", 1)
    document_xml = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"
            xmlns:w14="http://schemas.microsoft.com/office/word/2010/wordml">
  <w:body>
    <w:p w14:paraId="00A1B2C3">
      <w:r>
        <w:rPr><w:b/></w:rPr>
        <w:t xml:space="preserve">{left} </w:t>
      </w:r>
      <w:r><w:t>{right}</w:t></w:r>
    </w:p>
    <w:sectPr/>
  </w:body>
</w:document>
"""

    parts = {
        "[Content_Types].xml": content_types,
        "_rels/.rels": root_rels,
        "word/document.xml": document_xml,
        "word/_rels/document.xml.rels": document_rels,
    }

    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for part_name, payload in parts.items():
            zf.writestr(part_name, payload)


def _build_structured_docx_with_fld_simple(path: Path, text: str) -> None:
    content_types = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
</Types>
"""

    root_rels = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>
</Relationships>
"""

    document_rels = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"/>
"""

    beta_start = text.index("beta")
    beta_end = beta_start + len("beta")
    left = text[:beta_start]
    right = text[beta_end:]

    left_space = ' xml:space="preserve"' if left and (left[0].isspace() or left[-1].isspace()) else ""
    right_space = ' xml:space="preserve"' if right and (right[0].isspace() or right[-1].isspace()) else ""

    document_xml = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"
            xmlns:w14="http://schemas.microsoft.com/office/word/2010/wordml">
  <w:body>
    <w:p w14:paraId="00A1B2C3">
      <w:r><w:t{left_space}>{left}</w:t></w:r>
      <w:fldSimple w:instr=' HYPERLINK "https://example.com" '>
        <w:r><w:t>beta</w:t></w:r>
      </w:fldSimple>
      <w:r><w:t{right_space}>{right}</w:t></w:r>
    </w:p>
    <w:sectPr/>
  </w:body>
</w:document>
"""

    parts = {
        "[Content_Types].xml": content_types,
        "_rels/.rels": root_rels,
        "word/document.xml": document_xml,
        "word/_rels/document.xml.rels": document_rels,
    }

    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for part_name, payload in parts.items():
            zf.writestr(part_name, payload)


def _build_docx_with_footnote_reference(path: Path, text: str) -> None:
    if not text.endswith("."):
        raise ValueError("text must end with a period")

    content_types = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
  <Override PartName="/word/footnotes.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.footnotes+xml"/>
</Types>
"""

    root_rels = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>
</Relationships>
"""

    document_rels = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/footnotes" Target="footnotes.xml"/>
</Relationships>
"""

    left = text[:-1]
    document_xml = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"
            xmlns:w14="http://schemas.microsoft.com/office/word/2010/wordml">
  <w:body>
    <w:p w14:paraId="00A1B2C3">
      <w:r><w:t>{left}</w:t></w:r>
      <w:r>
        <w:rPr><w:rStyle w:val="FootnoteReference"/></w:rPr>
        <w:footnoteReference w:id="1"/>
      </w:r>
      <w:r><w:t>.</w:t></w:r>
    </w:p>
    <w:sectPr/>
  </w:body>
</w:document>
"""

    footnotes_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:footnotes xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:footnote w:type="separator" w:id="-1">
    <w:p><w:r><w:separator/></w:r></w:p>
  </w:footnote>
  <w:footnote w:type="continuationSeparator" w:id="0">
    <w:p><w:r><w:continuationSeparator/></w:r></w:p>
  </w:footnote>
  <w:footnote w:id="1">
    <w:p><w:r><w:t>Footnote body.</w:t></w:r></w:p>
  </w:footnote>
</w:footnotes>
"""

    parts = {
        "[Content_Types].xml": content_types,
        "_rels/.rels": root_rels,
        "word/document.xml": document_xml,
        "word/_rels/document.xml.rels": document_rels,
        "word/footnotes.xml": footnotes_xml,
    }

    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for part_name, payload in parts.items():
            zf.writestr(part_name, payload)


def _build_review_units(path: Path, text: str, *, include_paragraph_index: bool = True) -> None:
    location = {
        "part_index": 0,
        "global_order_index": 0,
        "path_hint": "word/document.xml::.//w:p[1]",
        "in_table": False,
    }
    if include_paragraph_index:
        location["paragraph_index_in_part"] = 0

    _write_json(
        path,
        {
            "source_docx": "synthetic.docx",
            "part_count": 1,
            "unit_count": 1,
            "units": [
                {
                    "part": "word/document.xml",
                    "part_kind": "body",
                    "part_name": "document",
                    "para_id": "para_test",
                    "unit_uid": "unit_test",
                    "accepted_text": text,
                    "heading_path": [],
                    "order_index": 0,
                    "location": location,
                }
            ],
        },
    )


def _build_replace_only_patch(path: Path, text: str) -> None:
    replace_start = text.index("beta")
    replace_end = replace_start + len("beta")

    _write_json(
        path,
        {
            "schema_version": "patch.v1",
            "created_at": "2026-02-27T00:00:00Z",
            "author": "test",
            "ops": [
                {
                    "type": "replace_range",
                    "target": {
                        "part": "word/document.xml",
                        "para_id": "para_test",
                        "unit_uid": "unit_test",
                    },
                    "range": {"start": replace_start, "end": replace_end},
                    "expected": {"snippet": "beta"},
                    "replacement": "BETA",
                }
            ],
        },
    )


def _build_patch(path: Path, text: str) -> None:
    replace_start = text.index("beta")
    replace_end = replace_start + len("beta")

    insert_at = text.index("gamma") + len("gamma")

    delete_start = text.index("delta")
    delete_end = delete_start + len("delta")

    patch = {
        "schema_version": "patch.v1",
        "created_at": "2026-02-27T00:00:00Z",
        "author": "test",
        "ops": [
            {
                "type": "replace_range",
                "target": {
                    "part": "word/document.xml",
                    "para_id": "para_test",
                    "unit_uid": "unit_test",
                },
                "range": {"start": replace_start, "end": replace_end},
                "expected": {"snippet": "beta"},
                "replacement": "BETA",
            },
            {
                "type": "insert_at",
                "target": {
                    "part": "word/document.xml",
                    "para_id": "para_test",
                    "unit_uid": "unit_test",
                },
                "range": {"start": insert_at, "end": insert_at},
                "expected": {"snippet": ""},
                "new_text": "++",
            },
            {
                "type": "delete_range",
                "target": {
                    "part": "word/document.xml",
                    "para_id": "para_test",
                },
                "range": {"start": delete_start, "end": delete_end},
                "expected": {"snippet": "delta"},
            },
            {
                "type": "add_comment",
                "target": {
                    "part": "word/document.xml",
                    "para_id": "para_test",
                },
                "range": {"start": 0, "end": len("Alpha")},
                "expected": {"snippet": "Alpha"},
                "comment_text": "Check intro term.",
            },
        ],
    }

    _write_json(path, patch)


def _build_mismatch_patch(path: Path, text: str) -> None:
    replace_start = text.index("beta")
    replace_end = replace_start + len("beta")

    _write_json(
        path,
        {
            "schema_version": "patch.v1",
            "created_at": "2026-02-27T00:00:00Z",
            "author": "test",
            "ops": [
                {
                    "type": "replace_range",
                    "target": {
                        "part": "word/document.xml",
                        "para_id": "para_test",
                        "unit_uid": "unit_test",
                    },
                    "range": {"start": replace_start, "end": replace_end},
                    "expected": {"snippet": "WRONG"},
                    "replacement": "BETA",
                }
            ],
        },
    )


def _build_add_comment_only_patch(path: Path, text: str) -> None:
    comment_start = text.index("beta")
    comment_end = comment_start + len("beta")

    _write_json(
        path,
        {
            "schema_version": "patch.v1",
            "created_at": "2026-02-27T00:00:00Z",
            "author": "test",
            "ops": [
                {
                    "type": "add_comment",
                    "target": {
                        "part": "word/document.xml",
                        "para_id": "para_test",
                        "unit_uid": "unit_test",
                    },
                    "range": {"start": comment_start, "end": comment_end},
                    "expected": {"snippet": "beta"},
                    "comment_text": "Structured paragraph should skip comments.",
                }
            ],
        },
    )


def _run_apply(*, source_docx: Path, patch_path: Path, review_units_path: Path, output_docx: Path, apply_log: Path) -> None:
    project_dir = output_docx.parent.parent
    project_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            sys.executable,
            str(APPLY_SCRIPT),
            "--project-dir",
            str(project_dir),
            "--input-docx",
            str(source_docx),
            "--patch",
            str(patch_path),
            "--review-units",
            str(review_units_path),
            "--output-docx",
            str(output_docx),
            "--apply-log",
            str(apply_log),
            "--author",
            "apply-test",
        ],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )


def _count_note_references(document_root: ET.Element) -> int:
    return len(document_root.findall(".//w:footnoteReference", NS)) + len(document_root.findall(".//w:endnoteReference", NS))


def test_apply_patch_emits_track_changes_and_comments(tmp_path: Path) -> None:
    base_text = "Alpha beta gamma delta."

    source_docx = tmp_path / "source.docx"
    patch_path = tmp_path / "artifacts/patch/merged_patch.json"
    review_units_path = tmp_path / "artifacts/docx_extract/review_units.json"
    output_docx = tmp_path / "output/annotated.docx"
    apply_log = tmp_path / "artifacts/apply/apply_log.json"

    _build_minimal_docx(source_docx, base_text)
    _build_patch(patch_path, base_text)
    _build_review_units(review_units_path, base_text)

    _run_apply(
        source_docx=source_docx,
        patch_path=patch_path,
        review_units_path=review_units_path,
        output_docx=output_docx,
        apply_log=apply_log,
    )

    assert output_docx.exists(), "Missing output DOCX"
    assert apply_log.exists(), "Missing apply_log.json"

    with zipfile.ZipFile(output_docx, mode="r") as zf:
        assert zf.testzip() is None, "Output DOCX is not a valid zip package"

        names = set(zf.namelist())
        assert "word/comments.xml" in names, "Comment part must exist when add_comment is applied"

        document_root = ET.fromstring(zf.read("word/document.xml"))
        comments_root = ET.fromstring(zf.read("word/comments.xml"))
        content_types_root = ET.fromstring(zf.read("[Content_Types].xml"))
        document_rels_root = ET.fromstring(zf.read("word/_rels/document.xml.rels"))

    assert document_root.find(".//w:ins", NS) is not None, "Expected tracked insertion w:ins"
    assert document_root.find(".//w:del", NS) is not None, "Expected tracked deletion w:del"
    assert document_root.find(".//w:delText", NS) is not None, "Expected deleted text node w:delText"

    start = document_root.find(".//w:commentRangeStart", NS)
    end = document_root.find(".//w:commentRangeEnd", NS)
    ref = document_root.find(".//w:commentReference", NS)
    assert start is not None and end is not None and ref is not None, "Missing comment anchor markers"
    assert start.get(qn(W_NS, "id")) == end.get(qn(W_NS, "id")) == ref.get(
        qn(W_NS, "id")
    ), "comment marker IDs must match"

    comment = comments_root.find(".//w:comment", NS)
    assert comments_root.tag == qn(W_NS, "comments")
    assert comment is not None, "comments.xml must contain at least one comment"

    has_comments_override = any(
        node.get("PartName") == "/word/comments.xml" and node.get("ContentType") == COMMENTS_CONTENT_TYPE
        for node in content_types_root.findall("ct:Override", NS)
    )
    assert has_comments_override, "[Content_Types].xml must include comments override"

    has_comments_rel = any(
        node.get("Type") == COMMENTS_REL_TYPE and node.get("Target") == "comments.xml"
        for node in document_rels_root.findall("rels:Relationship", NS)
    )
    assert has_comments_rel, "document.xml.rels must include comments relationship"

    log_payload = json.loads(apply_log.read_text(encoding="utf-8"))
    assert log_payload["stats"]["input_ops"] == 4
    assert log_payload["stats"]["applied_ops"] == 4
    assert log_payload["stats"]["skipped_ops"] == 0


def test_apply_patch_skips_on_expected_snippet_mismatch(tmp_path: Path) -> None:
    base_text = "Alpha beta gamma delta."

    source_docx = tmp_path / "source.docx"
    patch_path = tmp_path / "artifacts/patch/merged_patch.json"
    review_units_path = tmp_path / "artifacts/docx_extract/review_units.json"
    output_docx = tmp_path / "output/annotated.docx"
    apply_log = tmp_path / "artifacts/apply/apply_log.json"

    _build_minimal_docx(source_docx, base_text)
    _build_mismatch_patch(patch_path, base_text)
    _build_review_units(review_units_path, base_text)

    _run_apply(
        source_docx=source_docx,
        patch_path=patch_path,
        review_units_path=review_units_path,
        output_docx=output_docx,
        apply_log=apply_log,
    )

    with zipfile.ZipFile(output_docx, mode="r") as zf:
        assert zf.testzip() is None, "Output DOCX is not a valid zip package"
        document_root = ET.fromstring(zf.read("word/document.xml"))

    assert document_root.find(".//w:ins", NS) is None
    assert document_root.find(".//w:del", NS) is None

    log_payload = json.loads(apply_log.read_text(encoding="utf-8"))
    assert log_payload["stats"]["input_ops"] == 1
    assert log_payload["stats"]["applied_ops"] == 0
    assert log_payload["stats"]["skipped_ops"] == 1

    op_entry = log_payload["ops"][0]
    assert op_entry["status"] == "skipped"
    assert op_entry["reason"] == "snippet_mismatch"
    assert op_entry["actual_snippet"] == "beta"


def test_apply_patch_uses_path_hint_when_paragraph_index_is_missing(tmp_path: Path) -> None:
    base_text = "Alpha beta gamma delta."

    source_docx = tmp_path / "source.docx"
    patch_path = tmp_path / "artifacts/patch/merged_patch.json"
    review_units_path = tmp_path / "artifacts/docx_extract/review_units.json"
    output_docx = tmp_path / "output/annotated.docx"
    apply_log = tmp_path / "artifacts/apply/apply_log.json"

    _build_minimal_docx(source_docx, base_text)
    _build_replace_only_patch(patch_path, base_text)
    _build_review_units(review_units_path, base_text, include_paragraph_index=False)

    _run_apply(
        source_docx=source_docx,
        patch_path=patch_path,
        review_units_path=review_units_path,
        output_docx=output_docx,
        apply_log=apply_log,
    )

    with zipfile.ZipFile(output_docx, mode="r") as zf:
        assert zf.testzip() is None, "Output DOCX is not a valid zip package"
        document_root = ET.fromstring(zf.read("word/document.xml"))

    assert document_root.find(".//w:ins", NS) is not None
    assert document_root.find(".//w:del", NS) is not None

    log_payload = json.loads(apply_log.read_text(encoding="utf-8"))
    assert log_payload["stats"]["input_ops"] == 1
    assert log_payload["stats"]["applied_ops"] == 1
    assert log_payload["stats"]["skipped_ops"] == 0

    op_entry = log_payload["ops"][0]
    assert op_entry["status"] == "applied"
    assert op_entry["resolved_target"]["paragraph_index_in_part"] == 0


def test_add_comment_preserves_footnote_reference(tmp_path: Path) -> None:
    base_text = "Alpha beta gamma delta."

    source_docx = tmp_path / "source.docx"
    patch_path = tmp_path / "artifacts/patch/merged_patch.json"
    review_units_path = tmp_path / "artifacts/docx_extract/review_units.json"
    output_docx = tmp_path / "output/annotated.docx"
    apply_log = tmp_path / "artifacts/apply/apply_log.json"

    _build_docx_with_footnote_reference(source_docx, base_text)
    _build_add_comment_only_patch(patch_path, base_text)
    _build_review_units(review_units_path, base_text)

    with zipfile.ZipFile(source_docx, mode="r") as zf:
        source_document_xml = zf.read("word/document.xml")
        source_footnotes_xml = zf.read("word/footnotes.xml")
        source_document_root = ET.fromstring(source_document_xml)

    _run_apply(
        source_docx=source_docx,
        patch_path=patch_path,
        review_units_path=review_units_path,
        output_docx=output_docx,
        apply_log=apply_log,
    )

    with zipfile.ZipFile(output_docx, mode="r") as zf:
        assert zf.testzip() is None, "Output DOCX is not a valid zip package"
        output_document_xml = zf.read("word/document.xml")
        output_footnotes_xml = zf.read("word/footnotes.xml")
        output_document_root = ET.fromstring(output_document_xml)

    assert _count_note_references(source_document_root) == 1
    assert _count_note_references(output_document_root) == 1
    assert output_footnotes_xml == source_footnotes_xml
    assert output_document_root.find(".//w:commentRangeStart", NS) is not None
    assert output_document_root.find(".//w:commentRangeEnd", NS) is not None
    assert output_document_root.find(".//w:commentReference", NS) is not None

    log_payload = json.loads(apply_log.read_text(encoding="utf-8"))
    assert log_payload["stats"]["input_ops"] == 1
    assert log_payload["stats"]["applied_ops"] == 1
    assert log_payload["stats"]["skipped_ops"] == 0
    assert log_payload["stats"]["skipped_inline_element_integrity_mismatch"] == 0
    assert log_payload["ops"][0]["status"] == "applied"


def test_replace_range_crossing_footnote_reference_is_skipped_without_xml_mutation(tmp_path: Path) -> None:
    base_text = "Alpha beta gamma delta."
    replace_start = base_text.index("delta")
    replace_end = replace_start + len("delta.")

    source_docx = tmp_path / "source.docx"
    patch_path = tmp_path / "artifacts/patch/merged_patch.json"
    review_units_path = tmp_path / "artifacts/docx_extract/review_units.json"
    output_docx = tmp_path / "output/annotated.docx"
    apply_log = tmp_path / "artifacts/apply/apply_log.json"

    _build_docx_with_footnote_reference(source_docx, base_text)
    _build_review_units(review_units_path, base_text)
    _write_json(
        patch_path,
        {
            "schema_version": "patch.v1",
            "created_at": "2026-02-27T00:00:00Z",
            "author": "test",
            "ops": [
                {
                    "type": "replace_range",
                    "target": {
                        "part": "word/document.xml",
                        "para_id": "para_test",
                        "unit_uid": "unit_test",
                    },
                    "range": {"start": replace_start, "end": replace_end},
                    "expected": {"snippet": "delta."},
                    "replacement": "DELTA.",
                }
            ],
        },
    )

    with zipfile.ZipFile(source_docx, mode="r") as zf:
        source_document_xml = zf.read("word/document.xml")

    _run_apply(
        source_docx=source_docx,
        patch_path=patch_path,
        review_units_path=review_units_path,
        output_docx=output_docx,
        apply_log=apply_log,
    )

    with zipfile.ZipFile(output_docx, mode="r") as zf:
        assert zf.testzip() is None, "Output DOCX is not a valid zip package"
        output_document_xml = zf.read("word/document.xml")
        output_document_root = ET.fromstring(output_document_xml)

    assert output_document_xml == source_document_xml
    assert _count_note_references(output_document_root) == 1

    log_payload = json.loads(apply_log.read_text(encoding="utf-8"))
    assert log_payload["stats"]["input_ops"] == 1
    assert log_payload["stats"]["applied_ops"] == 0
    assert log_payload["stats"]["skipped_ops"] == 1
    assert log_payload["stats"]["skipped_range_intersects_preserved_inline_element"] == 1
    assert log_payload["ops"][0]["status"] == "skipped"
    assert log_payload["ops"][0]["reason"] == "range_intersects_preserved_inline_element"


def test_structured_paragraph_replace_range_is_skipped_without_xml_mutation(tmp_path: Path) -> None:
    base_text = "Alpha beta gamma delta."

    source_docx = tmp_path / "source.docx"
    patch_path = tmp_path / "artifacts/patch/merged_patch.json"
    review_units_path = tmp_path / "artifacts/docx_extract/review_units.json"
    output_docx = tmp_path / "output/annotated.docx"
    apply_log = tmp_path / "artifacts/apply/apply_log.json"

    _build_structured_docx_with_fld_simple(source_docx, base_text)
    _build_replace_only_patch(patch_path, base_text)
    _build_review_units(review_units_path, base_text)

    with zipfile.ZipFile(source_docx, mode="r") as zf:
        source_document_xml = zf.read("word/document.xml")

    _run_apply(
        source_docx=source_docx,
        patch_path=patch_path,
        review_units_path=review_units_path,
        output_docx=output_docx,
        apply_log=apply_log,
    )

    with zipfile.ZipFile(output_docx, mode="r") as zf:
        assert zf.testzip() is None, "Output DOCX is not a valid zip package"
        output_document_xml = zf.read("word/document.xml")
        document_root = ET.fromstring(output_document_xml)

    assert document_root.find(".//w:ins", NS) is None
    assert document_root.find(".//w:del", NS) is None
    assert document_root.find(".//w:delText", NS) is None
    assert document_root.find(".//w:fldSimple", NS) is not None, "Structured element must remain intact"
    assert output_document_xml == source_document_xml

    log_payload = json.loads(apply_log.read_text(encoding="utf-8"))
    assert log_payload["stats"]["input_ops"] == 1
    assert log_payload["stats"]["applied_ops"] == 0
    assert log_payload["stats"]["skipped_ops"] == 1
    assert log_payload["stats"]["skipped_unsupported_paragraph_structure"] == 1

    op_entry = log_payload["ops"][0]
    assert op_entry["status"] == "skipped"
    assert op_entry["reason"] == "unsupported_paragraph_structure"


def test_structured_paragraph_add_comment_is_skipped_without_creating_comments(tmp_path: Path) -> None:
    base_text = "Alpha beta gamma delta."

    source_docx = tmp_path / "source.docx"
    patch_path = tmp_path / "artifacts/patch/merged_patch.json"
    review_units_path = tmp_path / "artifacts/docx_extract/review_units.json"
    output_docx = tmp_path / "output/annotated.docx"
    apply_log = tmp_path / "artifacts/apply/apply_log.json"

    _build_structured_docx_with_fld_simple(source_docx, base_text)
    _build_add_comment_only_patch(patch_path, base_text)
    _build_review_units(review_units_path, base_text)

    with zipfile.ZipFile(source_docx, mode="r") as zf:
        source_document_xml = zf.read("word/document.xml")

    _run_apply(
        source_docx=source_docx,
        patch_path=patch_path,
        review_units_path=review_units_path,
        output_docx=output_docx,
        apply_log=apply_log,
    )

    with zipfile.ZipFile(output_docx, mode="r") as zf:
        assert zf.testzip() is None, "Output DOCX is not a valid zip package"
        assert "word/comments.xml" not in set(zf.namelist())
        output_document_xml = zf.read("word/document.xml")
        document_root = ET.fromstring(output_document_xml)

    assert document_root.find(".//w:commentRangeStart", NS) is None
    assert document_root.find(".//w:commentRangeEnd", NS) is None
    assert document_root.find(".//w:commentReference", NS) is None
    assert document_root.find(".//w:ins", NS) is None
    assert document_root.find(".//w:del", NS) is None
    assert document_root.find(".//w:fldSimple", NS) is not None, "Structured element must remain intact"
    assert output_document_xml == source_document_xml

    log_payload = json.loads(apply_log.read_text(encoding="utf-8"))
    assert log_payload["stats"]["input_ops"] == 1
    assert log_payload["stats"]["applied_ops"] == 0
    assert log_payload["stats"]["applied_comment_ops"] == 0
    assert log_payload["stats"]["skipped_ops"] == 1
    assert log_payload["stats"]["skipped_unsupported_paragraph_structure"] == 1

    op_entry = log_payload["ops"][0]
    assert op_entry["status"] == "skipped"
    assert op_entry["reason"] == "unsupported_paragraph_structure"
