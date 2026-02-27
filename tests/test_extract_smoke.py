from __future__ import annotations

import json
from pathlib import Path
import re
import subprocess
import sys

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
EXTRACT_SCRIPT = REPO_ROOT / ".codex/skills/docx_extract_ooxml_to_artifacts/scripts/extract_docx.py"
FIXTURES_DIR = REPO_ROOT / "fixtures"


def _find_fixture_docx() -> Path | None:
    fixtures = sorted(FIXTURES_DIR.glob("*.docx"))
    if fixtures:
        return fixtures[0]
    return None


def test_extract_docx_smoke(tmp_path: Path) -> None:
    fixture_docx = _find_fixture_docx()
    if fixture_docx is None:
        pytest.skip("No fixture DOCX found in fixtures/*.docx; skipping extraction smoke test.")

    output_dir = tmp_path / "docx_extract"

    subprocess.run(
        [
            sys.executable,
            str(EXTRACT_SCRIPT),
            "--input-docx",
            str(fixture_docx),
            "--output-dir",
            str(output_dir),
        ],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    review_units_path = output_dir / "review_units.json"
    docx_struct_path = output_dir / "docx_struct.json"
    linear_units_path = output_dir / "linear_units.json"

    assert review_units_path.exists(), "Missing review_units.json"
    assert docx_struct_path.exists(), "Missing docx_struct.json"
    assert linear_units_path.exists(), "Missing linear_units.json"

    review_payload = json.loads(review_units_path.read_text(encoding="utf-8"))
    linear_payload = json.loads(linear_units_path.read_text(encoding="utf-8"))
    struct_payload = json.loads(docx_struct_path.read_text(encoding="utf-8"))

    review_units = review_payload.get("units", [])
    linear_unit_uids = linear_payload.get("unit_uids", [])
    linear_units_alias = linear_payload.get("units", [])
    linear_order = linear_payload.get("order", [])
    struct_parts = struct_payload.get("parts", [])

    assert isinstance(review_units, list) and review_units, "review_units.json has no extracted units"
    assert isinstance(linear_unit_uids, list) and linear_unit_uids, "linear_units.json has no ordered unit_uids"
    assert isinstance(struct_parts, list), "docx_struct.json has invalid parts payload"

    allowed_part_kinds = {"body", "header", "footer", "footnotes", "endnotes"}
    assert all(unit["part_kind"] in allowed_part_kinds for unit in review_units), "unexpected review unit part_kind"
    assert all(part["part_kind"] in allowed_part_kinds for part in struct_parts), "unexpected docx part_kind"

    required_fields = {
        "part",
        "part_kind",
        "part_name",
        "para_id",
        "unit_uid",
        "accepted_text",
        "heading_path",
        "order_index",
        "location",
    }
    assert required_fields.issubset(review_units[0].keys()), "review unit missing required fields"

    assert [unit["order_index"] for unit in review_units] == list(range(len(review_units))), "review unit order mismatch"
    assert [unit["unit_uid"] for unit in review_units] == linear_unit_uids, "linear unit_uids are not ordered by review units"
    assert linear_units_alias == linear_unit_uids, "linear units alias diverges from ordered unit_uids"

    assert isinstance(linear_order, list) and len(linear_order) == len(
        linear_unit_uids
    ), "linear order metadata length mismatch"
    assert [item["unit_uid"] for item in linear_order] == linear_unit_uids, "linear order unit_uid mismatch"
    assert [item["order_index"] for item in linear_order] == list(range(len(linear_order))), "linear order index mismatch"

    assert all(re.fullmatch(r"para_[0-9a-f]{16}", unit["para_id"]) for unit in review_units), "invalid para_id shape"
    assert all(re.fullmatch(r"unit_[0-9a-f]{12}", unit["unit_uid"]) for unit in review_units), "invalid unit_uid shape"

    composite_ids = {(unit["para_id"], unit["unit_uid"]) for unit in review_units}
    assert len(composite_ids) == len(review_units), "non-unique para_id/unit_uid composite keys"
