from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
VALIDATE_SCRIPT_PATH = REPO_ROOT / "scripts" / "validate_dry_run_outputs.py"


def _load_validate_module():
    spec = importlib.util.spec_from_file_location("validate_dry_run_outputs", VALIDATE_SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_validate_requires_input_name_when_multiple_artifact_sets_present(tmp_path: Path):
    validate = _load_validate_module()

    project_dir = tmp_path / "project"
    (project_dir / "artifacts/docx_extract/Archives_ML").mkdir(parents=True, exist_ok=True)
    (project_dir / "artifacts/docx_extract/INTRODUCTION").mkdir(parents=True, exist_ok=True)
    (project_dir / "artifacts/docx_extract/Archives_ML/review_units.json").write_text("{}", encoding="utf-8")
    (project_dir / "artifacts/docx_extract/INTRODUCTION/review_units.json").write_text("{}", encoding="utf-8")

    try:
        validate._validate_artifact_presence(project_dir, input_name_arg=None)
        assert False, "expected RuntimeError"
    except RuntimeError as exc:
        message = str(exc)
        assert "Multiple input artifact sets found" in message
        assert "Archives_ML" in message
        assert "INTRODUCTION" in message


def test_validate_artifact_presence_targets_requested_input_name(tmp_path: Path):
    validate = _load_validate_module()

    project_dir = tmp_path / "project"

    # Multiple extracted inputs exist, but only INTRODUCTION is complete.
    (project_dir / "artifacts/docx_extract/Archives_ML").mkdir(parents=True, exist_ok=True)
    (project_dir / "artifacts/docx_extract/Archives_ML/review_units.json").write_text("{}", encoding="utf-8")

    intro_extract = project_dir / "artifacts/docx_extract/INTRODUCTION"
    intro_extract.mkdir(parents=True, exist_ok=True)
    (intro_extract / "review_units.json").write_text("{}", encoding="utf-8")
    (intro_extract / "linear_units.json").write_text("{}", encoding="utf-8")
    (intro_extract / "docx_struct.json").write_text("{}", encoding="utf-8")

    intro_chunks = project_dir / "artifacts/chunks/INTRODUCTION"
    intro_chunks.mkdir(parents=True, exist_ok=True)
    (intro_chunks / "manifest.json").write_text("{}", encoding="utf-8")

    intro_patch = project_dir / "artifacts/patch/INTRODUCTION"
    intro_patch.mkdir(parents=True, exist_ok=True)
    (intro_patch / "final_patch.json").write_text("{}", encoding="utf-8")
    (intro_patch / "merge_report.json").write_text("{}", encoding="utf-8")

    intro_apply = project_dir / "artifacts/apply/INTRODUCTION"
    intro_apply.mkdir(parents=True, exist_ok=True)
    (intro_apply / "apply_log.json").write_text("{}", encoding="utf-8")

    output_dir = project_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "INTRODUCTION_annotated.docx").write_text("placeholder", encoding="utf-8")
    (output_dir / "INTRODUCTION_changes.md").write_text("placeholder", encoding="utf-8")
    (output_dir / "INTRODUCTION_changes.json").write_text("{}", encoding="utf-8")

    intro_chunk_results = project_dir / "artifacts/ralph_0/chunk_results/INTRODUCTION"
    intro_chunk_results.mkdir(parents=True, exist_ok=True)
    (intro_chunk_results / "chunk_0001_result.json").write_text("{}", encoding="utf-8")

    paths = validate._validate_artifact_presence(project_dir, input_name_arg="INTRODUCTION")

    assert str(paths["final_patch"]).endswith("artifacts/patch/INTRODUCTION/final_patch.json")

