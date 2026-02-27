#!/usr/bin/env python3
"""Build and validate a minimal DOCX with tracked insertion + comment anchors."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import xml.etree.ElementTree as ET
import zipfile

W_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
CT_NS = "http://schemas.openxmlformats.org/package/2006/content-types"

COMMENTS_CONTENT_TYPE = "application/vnd.openxmlformats-officedocument.wordprocessingml.comments+xml"
COMMENTS_REL_TYPE = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/comments"

REQUIRED_FILES = {
    "[Content_Types].xml",
    "_rels/.rels",
    "word/document.xml",
    "word/comments.xml",
    "word/_rels/document.xml.rels",
}


@dataclass(frozen=True)
class Check:
    label: str
    ok: bool


def qn(ns: str, local: str) -> str:
    return f"{{{ns}}}{local}"


def utc_now_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_parts(timestamp: str) -> dict[str, str]:
    content_types = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
  <Override PartName="/word/comments.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.comments+xml"/>
</Types>
"""

    root_rels = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>
</Relationships>
"""

    document_rels = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rIdComments" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/comments" Target="comments.xml"/>
</Relationships>
"""

    document_xml = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"
            xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <w:body>
    <w:p>
      <w:r><w:t xml:space="preserve">Base </w:t></w:r>
      <w:ins w:id="1" w:author="OOXML Probe" w:date="{timestamp}">
        <w:r><w:t>inserted</w:t></w:r>
      </w:ins>
      <w:r><w:t xml:space="preserve"> text with </w:t></w:r>
      <w:commentRangeStart w:id="0"/>
      <w:r><w:t>comment anchor</w:t></w:r>
      <w:commentRangeEnd w:id="0"/>
      <w:r><w:commentReference w:id="0"/></w:r>
    </w:p>
    <w:sectPr/>
  </w:body>
</w:document>
"""

    comments_xml = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:comments xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:comment w:id="0" w:author="OOXML Probe" w:date="{timestamp}">
    <w:p>
      <w:r><w:t>Probe comment</w:t></w:r>
    </w:p>
  </w:comment>
</w:comments>
"""

    return {
        "[Content_Types].xml": content_types,
        "_rels/.rels": root_rels,
        "word/document.xml": document_xml,
        "word/comments.xml": comments_xml,
        "word/_rels/document.xml.rels": document_rels,
    }


def write_docx(output_path: Path) -> None:
    parts = build_parts(timestamp=utc_now_z())
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for part_name, xml_text in parts.items():
            zf.writestr(part_name, xml_text)


def validate_docx(output_path: Path) -> list[Check]:
    checks: list[Check] = []

    with zipfile.ZipFile(output_path, mode="r") as zf:
        names = set(zf.namelist())
        checks.append(Check("ZIP contains required OOXML parts", REQUIRED_FILES.issubset(names)))

        document = ET.fromstring(zf.read("word/document.xml"))
        comments = ET.fromstring(zf.read("word/comments.xml"))
        content_types = ET.fromstring(zf.read("[Content_Types].xml"))
        document_rels = ET.fromstring(zf.read("word/_rels/document.xml.rels"))

    ins = document.find(f".//{qn(W_NS, 'ins')}")
    ins_has_attrs = (
        ins is not None
        and ins.get(qn(W_NS, "id")) is not None
        and ins.get(qn(W_NS, "author")) is not None
        and ins.get(qn(W_NS, "date")) is not None
    )
    checks.append(Check("word/document.xml has tracked insertion (w:ins + w:id/w:author/w:date)", ins_has_attrs))

    start = document.find(f".//{qn(W_NS, 'commentRangeStart')}")
    end = document.find(f".//{qn(W_NS, 'commentRangeEnd')}")
    ref = document.find(f".//{qn(W_NS, 'commentReference')}")
    comment_anchor_ok = (
        start is not None
        and end is not None
        and ref is not None
        and start.get(qn(W_NS, "id")) == end.get(qn(W_NS, "id")) == ref.get(qn(W_NS, "id"))
    )
    checks.append(Check("word/document.xml has matching commentRangeStart/commentRangeEnd/commentReference ids", comment_anchor_ok))

    comment = comments.find(f".//{qn(W_NS, 'comment')}")
    comment_part_ok = comments.tag == qn(W_NS, "comments") and comment is not None
    checks.append(Check("word/comments.xml has w:comments root and at least one w:comment", comment_part_ok))

    comments_override_ok = any(
        elem.get("PartName") == "/word/comments.xml"
        and elem.get("ContentType") == COMMENTS_CONTENT_TYPE
        for elem in content_types.findall(f".//{qn(CT_NS, 'Override')}")
    )
    checks.append(Check("[Content_Types].xml contains comments override", comments_override_ok))

    comments_rel_ok = any(
        elem.get("Type") == COMMENTS_REL_TYPE and elem.get("Target") == "comments.xml"
        for elem in document_rels.findall(f".//{qn(PKG_REL_NS, 'Relationship')}")
    )
    checks.append(Check("word/_rels/document.xml.rels contains comments relationship", comments_rel_ok))

    return checks


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/probes/ooxml_comment_trackchange_probe.docx"),
        help="Output .docx path",
    )
    args = parser.parse_args()

    write_docx(args.output)
    checks = validate_docx(args.output)

    print(f"Created: {args.output}")
    for check in checks:
        status = "OK" if check.ok else "FAIL"
        print(f"[{status}] {check.label}")

    failed = [c for c in checks if not c.ok]
    if failed:
        print(f"Validation failed: {len(failed)} check(s) failed")
        return 1

    print("Validation passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
