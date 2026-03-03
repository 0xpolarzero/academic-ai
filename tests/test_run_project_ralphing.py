from __future__ import annotations

import importlib.util
import json
import sys
from argparse import Namespace
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
RUN_PROJECT_PATH = REPO_ROOT / "scripts" / "run_project.py"


def _load_run_project_module():
    spec = importlib.util.spec_from_file_location("run_project_ralph", RUN_PROJECT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _setup_project_tree(tmp_path: Path) -> tuple[Path, Path, Path]:
    project_dir = tmp_path / "projects" / "demo"
    workflow_xml = project_dir / "workflows" / "edit.xml"
    input_docx = project_dir / "input" / "chapter1.docx"
    constants = tmp_path / "config" / "constants.json"

    workflow_xml.parent.mkdir(parents=True, exist_ok=True)
    input_docx.parent.mkdir(parents=True, exist_ok=True)
    constants.parent.mkdir(parents=True, exist_ok=True)

    workflow_xml.write_text('<workflow name="edit"></workflow>', encoding="utf-8")
    input_docx.write_bytes(b"docx-bytes-placeholder")
    constants.write_text("{}", encoding="utf-8")

    return project_dir, input_docx, constants


def _make_paths(run_project, tmp_path: Path, *, ralph_count: int, use_judge: bool):
    project_dir, input_docx, constants = _setup_project_tree(tmp_path)
    input_name = "chapter1"

    extract_output_dir = project_dir / "artifacts" / "docx_extract" / input_name
    chunks_output_dir = project_dir / "artifacts" / "chunks" / input_name
    ralph_dirs = [
        project_dir / "artifacts" / f"ralph_{index}" / "chunk_results" / input_name
        for index in range(ralph_count)
    ]
    judged_dir = project_dir / "artifacts" / "judged" / "chunk_results" / input_name
    chunk_results_dir = judged_dir if use_judge else ralph_dirs[0]
    patch_output_dir = project_dir / "artifacts" / "patch" / input_name
    apply_dir = project_dir / "artifacts" / "apply" / input_name
    output_dir = project_dir / "output"

    for path in [extract_output_dir, chunks_output_dir, judged_dir, patch_output_dir, apply_dir, output_dir, *ralph_dirs]:
        path.mkdir(parents=True, exist_ok=True)

    return run_project.ProjectPaths(
        project_dir=project_dir,
        workflow_xml=project_dir / "workflows" / "edit.xml",
        source_docx=input_docx,
        input_name=input_name,
        constants=constants,
        extract_output_dir=extract_output_dir,
        chunks_output_dir=chunks_output_dir,
        ralph_count=ralph_count,
        use_judge=use_judge,
        ralph_chunk_results_dirs=ralph_dirs,
        judged_chunk_results_dir=judged_dir,
        chunk_results_dir=chunk_results_dir,
        patch_output_dir=patch_output_dir,
        merged_patch=patch_output_dir / "merged_patch.json",
        merge_report=patch_output_dir / "merge_report.json",
        final_patch=patch_output_dir / "final_patch.json",
        chunk_qa_report=chunks_output_dir / "chunk_qa_report.json",
        merge_qa_report=patch_output_dir / "merge_qa_report.json",
        final_patch_overrides=patch_output_dir / "final_patch_overrides.json",
        chunk_result_sanitization_log=chunk_results_dir / "sanitization_report.json",
        apply_log=apply_dir / "apply_log.json",
        annotated_docx=output_dir / f"{input_name}_annotated.docx",
        changes_docx=output_dir / f"{input_name}_changes.docx",
        changes_md=output_dir / f"{input_name}_changes.md",
        changes_json=output_dir / f"{input_name}_changes.json",
    )


def _extract_merge_chunk_results_arg(commands: list[list[str]]) -> str:
    for cmd in commands:
        if "--chunk-results-dir" in cmd:
            index = cmd.index("--chunk-results-dir")
            return cmd[index + 1]
    raise AssertionError("merge command with --chunk-results-dir was not called")


def test_resolve_paths_ralph_one_uses_ralph0_no_judge(tmp_path: Path):
    run_project = _load_run_project_module()
    _, _, constants = _setup_project_tree(tmp_path)
    run_project.REPO_ROOT = tmp_path

    args = Namespace(
        project="demo",
        workflow="edit",
        input="chapter1.docx",
        constants=constants,
        ralph=1,
        skip_judge=False,
    )
    paths = run_project._resolve_paths(args)

    assert paths.ralph_count == 1
    assert paths.use_judge is False
    assert len(paths.ralph_chunk_results_dirs) == 1
    assert paths.chunk_results_dir == paths.ralph_chunk_results_dirs[0]
    assert "artifacts/ralph_0/chunk_results/chapter1" in str(paths.chunk_results_dir)


def test_resolve_paths_ralph_three_skip_judge_uses_ralph0(tmp_path: Path):
    run_project = _load_run_project_module()
    _, _, constants = _setup_project_tree(tmp_path)
    run_project.REPO_ROOT = tmp_path

    args = Namespace(
        project="demo",
        workflow="edit",
        input="chapter1.docx",
        constants=constants,
        ralph=3,
        skip_judge=True,
    )
    paths = run_project._resolve_paths(args)

    assert paths.ralph_count == 3
    assert paths.use_judge is False
    assert len(paths.ralph_chunk_results_dirs) == 3
    assert len(set(paths.ralph_chunk_results_dirs)) == 3
    assert paths.chunk_results_dir == paths.ralph_chunk_results_dirs[0]


def test_run_pipeline_ralph_three_runs_judge_and_merges_judged(tmp_path: Path, monkeypatch):
    run_project = _load_run_project_module()
    paths = _make_paths(run_project, tmp_path, ralph_count=3, use_judge=True)

    calls: dict[str, list] = {"commands": [], "ralph": []}
    judge_calls = {"count": 0}

    def fake_run(cmd: list[str]) -> None:
        calls["commands"].append(cmd)

    def fake_chunk_qa(*args, **kwargs):
        return {"status": "ok", "passes": 1, "applied_fixes": []}

    def fake_single_ralph(*args, **kwargs):
        calls["ralph"].append(kwargs["ralph_index"])
        return {"chunk_count": 1, "total_input_ops": 1, "total_output_ops": 1, "chunks": []}

    def fake_judge(*args, **kwargs):
        judge_calls["count"] += 1
        return {"chunk_count": 1, "total_input_ops": 1, "total_output_ops": 1, "chunks": []}

    monkeypatch.setattr(run_project, "_run", fake_run)
    monkeypatch.setattr(run_project, "_run_chunk_qa_with_optional_fix", fake_chunk_qa)
    monkeypatch.setattr(run_project, "_run_single_ralph_review", fake_single_ralph)
    monkeypatch.setattr(run_project, "_run_judge_phase", fake_judge)
    monkeypatch.setattr(
        run_project,
        "_apply_merge_qa_overrides",
        lambda *args, **kwargs: {"actions_in": 0, "actions_applied": 0, "actions_ignored": 0},
    )
    monkeypatch.setattr(run_project, "_assert_outputs", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_project, "_enforce_no_sanitized_chunk_ops", lambda *args, **kwargs: None)

    run_project.run_pipeline(
        paths,
        author="test",
        dry_run=False,
        validate=False,
        max_concurrency=2,
        cli="codex",
        model=None,
    )

    assert calls["ralph"] == [0, 1, 2]
    assert judge_calls["count"] == 1
    assert _extract_merge_chunk_results_arg(calls["commands"]) == "artifacts/judged/chunk_results/chapter1"


def test_run_pipeline_ralph_three_skip_judge_merges_ralph0(tmp_path: Path, monkeypatch):
    run_project = _load_run_project_module()
    paths = _make_paths(run_project, tmp_path, ralph_count=3, use_judge=False)

    calls: dict[str, list] = {"commands": [], "ralph": []}
    judge_calls = {"count": 0}

    def fake_run(cmd: list[str]) -> None:
        calls["commands"].append(cmd)

    def fake_chunk_qa(*args, **kwargs):
        return {"status": "ok", "passes": 1, "applied_fixes": []}

    def fake_single_ralph(*args, **kwargs):
        calls["ralph"].append(kwargs["ralph_index"])
        return {"chunk_count": 1, "total_input_ops": 1, "total_output_ops": 1, "chunks": []}

    def fake_judge(*args, **kwargs):
        judge_calls["count"] += 1
        return {"chunk_count": 1, "total_input_ops": 1, "total_output_ops": 1, "chunks": []}

    monkeypatch.setattr(run_project, "_run", fake_run)
    monkeypatch.setattr(run_project, "_run_chunk_qa_with_optional_fix", fake_chunk_qa)
    monkeypatch.setattr(run_project, "_run_single_ralph_review", fake_single_ralph)
    monkeypatch.setattr(run_project, "_run_judge_phase", fake_judge)
    monkeypatch.setattr(
        run_project,
        "_apply_merge_qa_overrides",
        lambda *args, **kwargs: {"actions_in": 0, "actions_applied": 0, "actions_ignored": 0},
    )
    monkeypatch.setattr(run_project, "_assert_outputs", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_project, "_enforce_no_sanitized_chunk_ops", lambda *args, **kwargs: None)

    run_project.run_pipeline(
        paths,
        author="test",
        dry_run=False,
        validate=False,
        max_concurrency=2,
        cli="codex",
        model=None,
    )

    assert calls["ralph"] == [0, 1, 2]
    assert judge_calls["count"] == 0
    assert _extract_merge_chunk_results_arg(calls["commands"]) == "artifacts/ralph_0/chunk_results/chapter1"


def test_run_pipeline_validation_targets_current_input_name(tmp_path: Path, monkeypatch):
    run_project = _load_run_project_module()
    paths = _make_paths(run_project, tmp_path, ralph_count=1, use_judge=False)

    calls: dict[str, list] = {"commands": []}

    def fake_run(cmd: list[str]) -> None:
        calls["commands"].append(cmd)

    monkeypatch.setattr(run_project, "_run", fake_run)
    monkeypatch.setattr(run_project, "_run_chunk_qa_with_optional_fix", lambda *args, **kwargs: {"status": "ok", "passes": 1, "applied_fixes": []})
    monkeypatch.setattr(
        run_project,
        "_run_single_ralph_review",
        lambda *args, **kwargs: {"chunk_count": 1, "total_input_ops": 1, "total_output_ops": 1, "chunks": []},
    )
    monkeypatch.setattr(
        run_project,
        "_apply_merge_qa_overrides",
        lambda *args, **kwargs: {"actions_in": 0, "actions_applied": 0, "actions_ignored": 0},
    )
    monkeypatch.setattr(run_project, "_assert_outputs", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_project, "_enforce_no_sanitized_chunk_ops", lambda *args, **kwargs: None)

    run_project.run_pipeline(
        paths,
        author="test",
        dry_run=False,
        validate=True,
        max_concurrency=2,
        cli="codex",
        model=None,
    )

    validate_cmds = [cmd for cmd in calls["commands"] if str(run_project.VALIDATE_SCRIPT) in cmd]
    assert len(validate_cmds) == 1
    validate_cmd = validate_cmds[0]

    assert "--input-name" in validate_cmd
    assert validate_cmd[validate_cmd.index("--input-name") + 1] == paths.input_name


def test_run_judge_phase_writes_schema_valid_raw_and_sanitized_results(tmp_path: Path, monkeypatch):
    run_project = _load_run_project_module()
    paths = _make_paths(run_project, tmp_path, ralph_count=2, use_judge=True)

    chunk_id = "chunk_0001"
    target = {"part": "word/document.xml", "para_id": "para_1", "unit_uid": "unit_1"}
    chunk_payload = {
        "chunk_id": chunk_id,
        "primary_units": [
            {
                **target,
                "accepted_text": "This sentence is for testing the judge reconciliation logic in practice.",
            }
        ],
        "context_units_before": [],
        "context_units_after": [],
    }
    _write_json(paths.chunks_output_dir / "chunk_0001.json", chunk_payload)
    _write_json(
        paths.chunks_output_dir / "manifest.json",
        {"chunks": [{"chunk_id": chunk_id, "path": "chunk_0001.json"}]},
    )

    proposal = {
        "chunk_id": chunk_id,
        "suggestions": ["comment"],
        "ops": [
            {
                "type": "add_comment",
                "target": target,
                "quoted_text": "This sentence is for [[testing]] the judge reconciliation logic.",
                "expected": {"snippet": "testing"},
                "replacement": "",
                "new_text": "",
                "comment_text": "Clarify this term.",
            }
        ],
    }
    for directory in paths.ralph_chunk_results_dirs:
        _write_json(directory / f"{chunk_id}_result.json", proposal)

    def fake_run_cli_exec(*, output_path: Path, **kwargs):
        _write_json(output_path, proposal)

    monkeypatch.setattr(run_project, "_run_cli_exec", fake_run_cli_exec)

    summary = run_project._run_judge_phase(paths, cli="codex", model=None)
    assert summary["chunk_count"] == 1

    raw = run_project._load_json(paths.judged_chunk_results_dir / f"{chunk_id}_result.raw.json")
    schema = run_project._load_json(run_project.SCHEMA_CHUNK_REVIEW)
    errors = run_project._validate_against_schema(raw, schema)
    assert errors == []

    sanitized = run_project._load_json(paths.judged_chunk_results_dir / f"{chunk_id}_result.json")
    assert sanitized["chunk_id"] == chunk_id
    assert len(sanitized["ops"]) == 1
    assert sanitized["ops"][0]["type"] == "add_comment"
