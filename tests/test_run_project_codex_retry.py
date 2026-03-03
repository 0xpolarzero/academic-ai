from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
RUN_PROJECT_PATH = REPO_ROOT / "scripts" / "run_project.py"


def _load_run_project_module():
    spec = importlib.util.spec_from_file_location("run_project_retry", RUN_PROJECT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class _FakeStdin:
    def __init__(self) -> None:
        self.buffer = ""
        self.closed = False

    def write(self, value: str) -> None:
        self.buffer += value

    def close(self) -> None:
        self.closed = True


class _FakeStdout:
    def __init__(self, lines: list[str]) -> None:
        self._lines = [line if line.endswith("\n") else f"{line}\n" for line in lines]
        self._index = 0

    def has_pending(self) -> bool:
        return self._index < len(self._lines)

    def readline(self) -> str:
        if self._index < len(self._lines):
            line = self._lines[self._index]
            self._index += 1
            return line
        return ""

    def read(self) -> str:
        return ""


class _FakeProcess:
    def __init__(
        self,
        *,
        lines: list[str],
        returncode: int,
        output_path: Path | None = None,
        output_payload: str = "{}",
    ) -> None:
        self.stdin = _FakeStdin()
        self.stdout = _FakeStdout(lines)
        self._returncode = returncode
        self._killed = False
        self._output_path = output_path
        self._output_payload = output_payload

    def poll(self) -> int | None:
        if self._killed:
            return -9
        if self.stdout.has_pending():
            return None
        return self._returncode

    def wait(self) -> int:
        if self._killed:
            return -9
        if self._returncode == 0 and self._output_path is not None:
            self._output_path.write_text(self._output_payload, encoding="utf-8")
        return self._returncode

    def kill(self) -> None:
        self._killed = True


def _fake_select(readers: list[_FakeStdout], _writers, _errors, _timeout):
    if readers and readers[0].has_pending():
        return readers, [], []
    return [], [], []


def test_run_codex_retries_when_heredoc_tempfile_fails(tmp_path: Path, monkeypatch):
    run_project = _load_run_project_module()

    schema_path = tmp_path / "schema.json"
    schema_path.write_text("{}", encoding="utf-8")
    output_path = tmp_path / "chunk_0007_result.json"

    first = _FakeProcess(
        lines=[
            "thinking",
            "zsh:1: can't create temp file for here document: operation not permitted",
        ],
        returncode=1,
    )
    second = _FakeProcess(
        lines=["ok"],
        returncode=0,
        output_path=output_path,
        output_payload='{"chunk_id":"chunk_0007","suggestions":[],"ops":[]}',
    )
    launches = [first, second]

    def fake_popen(*args, **kwargs):
        assert launches
        return launches.pop(0)

    monkeypatch.setattr(run_project.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(run_project.select, "select", _fake_select)

    run_project._run_codex(
        prompt="<prompt>judge</prompt>",
        schema_path=schema_path,
        output_path=output_path,
        phase="ralph judge chunk_0007",
        model=None,
    )

    assert first.stdin.buffer == "<prompt>judge</prompt>"
    assert run_project.CODEX_NO_SHELL_CONSTRAINTS in second.stdin.buffer
    assert second.stdin.buffer.startswith("<prompt>judge</prompt>")
    assert output_path.exists()


def test_run_codex_raises_after_second_heredoc_tempfile_failure(tmp_path: Path, monkeypatch):
    run_project = _load_run_project_module()

    schema_path = tmp_path / "schema.json"
    schema_path.write_text("{}", encoding="utf-8")
    output_path = tmp_path / "chunk_0007_result.json"

    launches = [
        _FakeProcess(
            lines=["zsh:1: can't create temp file for here document: operation not permitted"],
            returncode=1,
        ),
        _FakeProcess(
            lines=["zsh:1: can't create temp file for here document: operation not permitted"],
            returncode=1,
        ),
    ]

    def fake_popen(*args, **kwargs):
        assert launches
        return launches.pop(0)

    monkeypatch.setattr(run_project.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(run_project.select, "select", _fake_select)

    try:
        run_project._run_codex(
            prompt="<prompt>judge</prompt>",
            schema_path=schema_path,
            output_path=output_path,
            phase="ralph judge chunk_0007",
            model=None,
        )
        assert False, "expected RuntimeError"
    except RuntimeError as exc:
        message = str(exc)
        assert "retried with no-shell constraints" in message
