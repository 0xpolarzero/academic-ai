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


def _run_apply(*, source_docx: Path, patch_path: Path, review_units_path: Path, output_docx: Path, apply_log: Path) -> None:
    subprocess.run(
        [
            sys.executable,
            str(APPLY_SCRIPT),
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
