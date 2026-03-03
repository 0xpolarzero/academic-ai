#!/usr/bin/env python3
"""Run project workflow pipeline with dry-run and Codex-integrated review support."""

from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import itertools
import copy
import os
from pathlib import Path
import json
import re
import select
import subprocess
import sys
import time
import unicodedata
from difflib import SequenceMatcher
import xml.etree.ElementTree as ET
from typing import Any
from typing import TextIO
from typing import Literal

REPO_ROOT = Path(__file__).resolve().parents[1]

# Import unified CLI runner for claude/kimi support
sys.path.insert(0, str(REPO_ROOT / "scripts"))
from unified_cli_runner import run_cli_exec, detect_available_cli, validate_claude_kimi_setup

CLI_EXEC_TIMEOUT_SECONDS = 600

EXTRACT_SCRIPT = REPO_ROOT / ".codex/skills/docx_extract_ooxml_to_artifacts/scripts/extract_docx.py"
CHUNK_SCRIPT = REPO_ROOT / ".codex/skills/docx_chunk_atomic_manifest/scripts/chunk_docx.py"
MERGE_SCRIPT = REPO_ROOT / ".codex/skills/docx_merge_dedup_validate_patch/scripts/merge_patch.py"
APPLY_SCRIPT = REPO_ROOT / ".codex/skills/docx_apply_patch_to_output/scripts/apply_docx_patch.py"
REPORT_SCRIPT = REPO_ROOT / ".codex/skills/docx_change_report_before_after/scripts/change_report.py"
VALIDATE_SCRIPT = REPO_ROOT / "scripts/validate_dry_run_outputs.py"

TEMPLATE_CHUNK_QA = REPO_ROOT / "templates/chunk_qa.xml"
TEMPLATE_CHUNK_REVIEW = REPO_ROOT / "templates/chunk_review.xml"
TEMPLATE_MERGE_QA = REPO_ROOT / "templates/merge_qa.xml"
TEMPLATE_RALPH_JUDGE = REPO_ROOT / "templates/ralph_judge.xml"

SCHEMA_CHUNK_QA = REPO_ROOT / "schemas/chunk_qa.schema.json"
SCHEMA_CHUNK_REVIEW = REPO_ROOT / "schemas/chunk_result.schema.json"
SCHEMA_MERGE_QA = REPO_ROOT / "schemas/merge_qa.schema.json"


def _load_schema_content(schema_path: Path) -> str:
    """Load JSON schema content for inclusion in prompts."""
    try:
        return schema_path.read_text(encoding="utf-8")
    except Exception:
        return "{}"

WORD_RE = re.compile(r"[A-Za-z][A-Za-z'-]{3,}")
VALID_OP_TYPES = {"add_comment", "replace_range", "insert_at"}
EDIT_OP_TYPES = {"replace_range", "insert_at"}

ANSI_RESET = "\033[0m"
ANSI_DIM = "\033[2m"
ANSI_CYAN = "\033[36m"
ANSI_BLUE = "\033[34m"
ANSI_YELLOW = "\033[33m"
ANSI_RED = "\033[31m"
ANSI_MAGENTA = "\033[35m"
ANSI_GREEN = "\033[32m"

_RUN_LOG_FH: TextIO | None = None


@dataclass(frozen=True)
class ProjectPaths:
    project_dir: Path
    workflow_xml: Path
    source_docx: Path
    input_name: str  # Base name of input file without extension
    constants: Path
    extract_output_dir: Path
    chunks_output_dir: Path
    ralph_count: int
    use_judge: bool
    ralph_chunk_results_dirs: list[Path]
    judged_chunk_results_dir: Path
    chunk_results_dir: Path
    patch_output_dir: Path
    merged_patch: Path
    merge_report: Path
    final_patch: Path
    chunk_qa_report: Path
    merge_qa_report: Path
    final_patch_overrides: Path
    chunk_result_sanitization_log: Path
    apply_log: Path
    annotated_docx: Path
    changes_md: Path
    changes_json: Path
    changes_docx: Path


@dataclass(frozen=True)
class SyntheticChunkResult:
    chunk_id: str
    output_path: Path
    op_count: int


def _init_run_log(path: Path) -> None:
    global _RUN_LOG_FH
    path.parent.mkdir(parents=True, exist_ok=True)
    _RUN_LOG_FH = path.open("w", encoding="utf-8", buffering=1)


def _close_run_log() -> None:
    global _RUN_LOG_FH
    if _RUN_LOG_FH is not None:
        _RUN_LOG_FH.close()
        _RUN_LOG_FH = None


def _log_line(message: str, *, stderr: bool = False) -> None:
    stream = sys.stderr if stderr else sys.stdout
    print(message, file=stream, flush=True)
    if _RUN_LOG_FH is not None:
        _RUN_LOG_FH.write(message + "\n")
        _RUN_LOG_FH.flush()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True, help="Project slug under projects/")
    parser.add_argument("--workflow", required=True, help="Workflow name in projects/<project>/workflows/<name>.xml")
    parser.add_argument("--input", default=None, help="Input DOCX filename (e.g., 'chapter1.docx'). If omitted and only one file exists in input/, uses that file. If omitted and multiple files exist, errors.")
    parser.add_argument("--constants", type=Path, default=Path("config/constants.json"), help="Path to constants JSON")
    parser.add_argument("--author", default="phase4-runner", help="Author value used in merge/apply artifacts")
    parser.add_argument("--max-concurrency", type=int, default=4, help="Max concurrent chunk reviewers")
    parser.add_argument("--dry-run", action="store_true", help="Generate synthetic chunk results instead of model outputs")
    parser.add_argument("--skip-validation", action="store_true", help="Skip QA acceptance checks at the end")
    parser.add_argument("--ralph", type=int, default=1, help="Number of sequential review ensemble runs (default: 1)")
    parser.add_argument("--skip-judge", action="store_true", help="When --ralph > 1, skip judge and use ralph_0 results directly")
    parser.add_argument("--cli", default="codex", choices=("codex", "claude", "kimi"), help="CLI provider to use for AI calls: codex, claude (Claude Code), or kimi (default: codex)")
    parser.add_argument("--model", default=None, help="Model to use for AI calls (e.g., 'gpt-5.3-codex', 'kimi-k2', 'sonnet')")
    resume_group = parser.add_mutually_exclusive_group()
    resume_group.add_argument(
        "--from-step",
        choices=("judge", "merge", "apply", "report"),
        default=None,
        help="Resume from a pipeline step using existing artifacts: judge|merge|apply|report",
    )
    resume_group.add_argument(
        "--from-ralph",
        type=int,
        default=None,
        help="Resume from Ralph run index N (0-based), then continue to judge/merge/apply/report",
    )
    return parser


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _dump_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _project_rel_path(project_dir: Path, path: Path) -> str:
    return path.resolve().relative_to(project_dir.resolve()).as_posix()


def _clear_chunk_result_artifacts(chunk_results_dir: Path) -> None:
    chunk_results_dir.mkdir(parents=True, exist_ok=True)
    for pattern in (
        "chunk_*_result.json",
        "chunk_*_sanitization.json",
        "chunk_*_result.raw.json",
        "sanitization_report.json",
    ):
        for stale in chunk_results_dir.glob(pattern):
            stale.unlink()


def _run(cmd: list[str]) -> None:
    _log_line("$ " + " ".join(cmd))
    proc = subprocess.Popen(
        cmd,
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        _log_line(line.rstrip())
    returncode = proc.wait()
    if returncode != 0:
        raise subprocess.CalledProcessError(returncode=returncode, cmd=cmd)


def _colors_enabled() -> bool:
    if os.environ.get("NO_COLOR"):
        return False
    return bool(sys.stdout.isatty())


def _colorize_codemsg(line: str, cli: str = "codex") -> str:
    if not _colors_enabled():
        return line

    lower = line.strip().lower()
    if not lower:
        return line
    if lower.startswith("error") or "invalid schema" in lower or "failed" in lower:
        return f"{ANSI_RED}{line}{ANSI_RESET}"
    if lower.startswith("warning") or lower.startswith("deprecated"):
        return f"{ANSI_YELLOW}{line}{ANSI_RESET}"
    if lower == "thinking" or lower.startswith("**"):
        return f"{ANSI_MAGENTA}{line}{ANSI_RESET}"
    if lower.startswith(cli.lower()):
        return f"{ANSI_CYAN}{line}{ANSI_RESET}"
    if lower.startswith("user"):
        return f"{ANSI_BLUE}{line}{ANSI_RESET}"
    if lower.startswith("exec"):
        return f"{ANSI_GREEN}{line}{ANSI_RESET}"
    if lower.startswith("tokens used") or lower.startswith("openai codex"):
        return f"{ANSI_DIM}{line}{ANSI_RESET}"
    return line


def _phase_prefix(phase: str) -> str:
    base = f"[{phase}]"
    if not _colors_enabled():
        return base
    return f"{ANSI_DIM}{base}{ANSI_RESET}"


def _build_cli_command(cli: str, *, prompt: str, schema_path: Path, output_path: Path) -> list[str]:
    """Build CLI command based on the selected provider."""
    if cli == "kimi":
        # For Kimi, we include the schema in the prompt since it doesn't have
        # built-in schema enforcement like Codex
        schema_content = _load_schema_content(schema_path)
        enhanced_prompt = f"""{prompt}

<output_schema>
{schema_content}
</output_schema>

CRITICAL: You must output ONLY valid JSON matching the schema above. Do not output any explanatory text, markdown formatting, or conversational content. Output raw JSON only."""
        return [
            "kimi",
            "--work-dir", str(REPO_ROOT),
            "--yolo",
            "--print",
            "--output-format", "stream-json",
            "--prompt", enhanced_prompt,
        ]
    # Default to codex
    return [
        "codex",
        "exec",
        "--cd",
        str(REPO_ROOT),
        "--sandbox",
        "read-only",
        "--output-schema",
        str(schema_path),
        "--output-last-message",
        str(output_path),
        "-",
    ]


def _extract_kimi_raw_text(lines: list[str]) -> str:
    """Extract raw text content from Kimi stream-json output.
    
    Collects all text content from assistant messages to pass to codex spark
    for structured JSON extraction.
    
    Returns:
        Concatenated text from all assistant messages
    """
    text_parts: list[str] = []
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            parsed = json.loads(line)
            if isinstance(parsed, dict) and parsed.get("role") == "assistant":
                content = parsed.get("content", [])
                if isinstance(content, list):
                    for item in content:
                        if isinstance(item, dict) and item.get("type") == "text":
                            text = item.get("text", "")
                            if text:
                                text_parts.append(text)
                elif isinstance(content, str):
                    text_parts.append(content)
        except json.JSONDecodeError:
            continue
    
    return "".join(text_parts)


def _extract_json_via_spark(*, raw_text: str, schema_path: Path, phase: str) -> dict[str, Any] | None:
    """Use Codex Spark to convert conversational text into valid JSON.
    
    All kimi output is passed through codex spark with structured output
    to ensure consistent JSON parsing. Codex enforces the output schema.
    """
    schema_content = _load_schema_content(schema_path)
    
    spark_prompt = f"""You are a JSON extraction specialist. Your task is to convert the following text into valid JSON that conforms exactly to the provided schema.

<instructions>
1. Read the text content below which describes some operations/changes.
2. Extract the relevant information and format it as valid JSON matching the schema.
3. Ensure all required fields from the schema are present.
4. If the text describes operations (like edits or comments), put them in the "ops" array.
5. Use empty strings "" for optional fields that aren't specified.
</instructions>

<output_schema>
{schema_content}
</output_schema>

<text_content>
{raw_text}
</text_content>

CRITICAL: Output ONLY valid JSON matching the schema."""

    output_path = REPO_ROOT / f"artifacts/.spark_extract_{phase.replace(' ', '_')}.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    cmd = [
        "codex",
        "exec",
        "--model", "gpt-5.3-codex-spark",
        "--cd", str(REPO_ROOT),
        "--sandbox", "read-only",
        "--output-schema", str(schema_path),
        "--output-last-message", str(output_path),
        "-",
    ]
    
    _log_line(f"[{phase}] Extracting JSON via Codex Spark...")
    
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=REPO_ROOT,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
    except FileNotFoundError:
        _log_line(f"[{phase}] ERROR: codex CLI not found for spark extraction")
        return None

    assert proc.stdin is not None
    assert proc.stdout is not None

    try:
        proc.stdin.write(spark_prompt)
        proc.stdin.close()

        # Read output with timeout
        deadline = time.monotonic() + 120  # 2 minutes
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                proc.kill()
                _log_line(f"[{phase}] Codex Spark extraction timed out")
                return None

            ready, _, _ = select.select([proc.stdout], [], [], min(1.0, remaining))
            if ready:
                line = proc.stdout.readline()
                if line:
                    continue

            if proc.poll() is not None:
                break

        # Wait for process to complete
        returncode = proc.wait()
        
        # Read any remaining output
        tail = proc.stdout.read()

        if returncode != 0:
            _log_line(f"[{phase}] Codex Spark extraction failed with exit code {returncode}")
            return None

        # Read the JSON output from file
        if output_path.exists():
            content = _load_json(output_path)
            _log_line(f"[{phase}] Successfully extracted JSON via Codex Spark")
            # Clean up temp file
            output_path.unlink(missing_ok=True)
            return content
        else:
            _log_line(f"[{phase}] Codex Spark output file not found")
            return None
        
    except Exception as exc:
        _log_line(f"[{phase}] Codex Spark extraction failed: {exc}")
        if proc.poll() is None:
            proc.kill()
        return None
    finally:
        # Clean up temp file if it exists
        output_path.unlink(missing_ok=True)


def _normalize_kimi_ops(result: dict[str, Any], chunk_payload: dict[str, Any]) -> None:
    """Normalize Kimi's output format to match the expected schema.
    
    - 'patch_ops' -> 'ops'
    - 'target_unit_uid' -> 'target' with full unit info (part, para_id, unit_uid)
    """
    if not isinstance(result, dict):
        return
    
    # Kimi may output 'patch_ops' but we expect 'ops'
    if "patch_ops" in result and "ops" not in result:
        result["ops"] = result.pop("patch_ops")
    
    # Build a map of unit_uid -> target for lookups
    unit_uid_to_target: dict[str, dict[str, str]] = {}
    primary_units = chunk_payload.get("primary_units", [])
    if isinstance(primary_units, list):
        for unit in primary_units:
            if isinstance(unit, dict):
                unit_uid = str(unit.get("unit_uid", "")).strip()
                if unit_uid:
                    unit_uid_to_target[unit_uid] = {
                        "part": str(unit.get("part", "")).strip(),
                        "para_id": str(unit.get("para_id", "")).strip(),
                        "unit_uid": unit_uid,
                    }
    
    # Normalize ops: Kimi uses 'target_unit_uid' but we expect 'target' object
    ops = result.get("ops", [])
    if isinstance(ops, list):
        for op in ops:
            if isinstance(op, dict) and "target_unit_uid" in op and "target" not in op:
                unit_uid = str(op.pop("target_unit_uid", "")).strip()
                if unit_uid in unit_uid_to_target:
                    op["target"] = unit_uid_to_target[unit_uid]


def _run_cli_exec(*, cli: str, prompt: str, schema_path: Path, output_path: Path, phase: str, model: str | None = None) -> None:
    """Run CLI command using appropriate backend.
    
    - codex: Uses native --output-schema (most reliable)
    - claude: Uses --json-schema for Anthropic models
    - kimi: Uses validation and retry logic (no external dependency)
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    if cli == "kimi":
        # Kimi uses validation/retry without Codex Spark dependency
        _run_kimi_with_retry(
            prompt=prompt,
            schema_path=schema_path,
            output_path=output_path,
            phase=phase,
            model=model,
        )
    elif cli == "claude":
        # Claude Code uses unified runner
        def log_callback(msg: str, stderr: bool = False):
            _log_line(msg, stderr=stderr)
        from unified_cli_runner import run_cli_exec as _run_claude
        _run_claude(
            cli="claude",
            prompt=prompt,
            schema_path=schema_path,
            output_path=output_path,
            work_dir=REPO_ROOT,
            phase=phase,
            log_callback=log_callback,
            model=model,
        )
    else:
        # Default codex (original behavior)
        _run_codex(
            prompt=prompt,
            schema_path=schema_path,
            output_path=output_path,
            phase=phase,
            model=model,
        )


def _run_codex(*, prompt: str, schema_path: Path, output_path: Path, phase: str, model: str | None = None) -> None:
    """Run Codex CLI with native structured output."""
    cmd = [
        "codex",
        "exec",
        "--cd", str(REPO_ROOT),
        "--sandbox", "read-only",
        "--output-schema", str(schema_path),
        "--output-last-message", str(output_path),
        "-",
    ]
    if model:
        cmd.extend(["-m", model])
    _log_line("$ " + " ".join(cmd))
    
    proc = subprocess.Popen(
        cmd,
        cwd=REPO_ROOT,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    
    try:
        assert proc.stdin is not None
        assert proc.stdout is not None
        
        proc.stdin.write(prompt)
        proc.stdin.close()
        
        deadline = time.monotonic() + CLI_EXEC_TIMEOUT_SECONDS
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                proc.kill()
                raise RuntimeError(f"Codex timed out after {CLI_EXEC_TIMEOUT_SECONDS}s")
            
            ready, _, _ = select.select([proc.stdout], [], [], min(1.0, remaining))
            if ready:
                line = proc.stdout.readline()
                if line:
                    _log_line(f"{_phase_prefix(phase)} {_colorize_codemsg(line.rstrip(), cli='codex')}")
                    continue
            
            if proc.poll() is not None:
                break
        
        tail = proc.stdout.read()
        if tail:
            for line in tail.splitlines():
                _log_line(f"{_phase_prefix(phase)} {_colorize_codemsg(line, cli='codex')}")
        
        returncode = proc.wait()
        if returncode != 0:
            raise subprocess.CalledProcessError(returncode, cmd)
        
        if not output_path.exists():
            raise RuntimeError(f"Codex did not create output file: {output_path}")
            
    finally:
        if proc.poll() is None:
            proc.kill()


def _run_kimi_with_retry(*, prompt: str, schema_path: Path, output_path: Path, phase: str, max_retries: int = 2, model: str | None = None) -> None:
    """Run Kimi CLI with validation and retry logic.
    
    Unlike the old implementation that used Codex Spark for extraction,
    this version validates locally and retries with error feedback.
    """
    schema_content = _load_schema_content(schema_path)
    schema = json.loads(schema_content) if schema_content else {}
    
    # Build initial prompt with strong JSON instructions
    base_prompt = _build_kimi_structured_prompt(prompt, schema_content)
    
    current_prompt = base_prompt
    last_errors: list[str] = []
    last_raw_text: str = ""
    
    for attempt in range(max_retries + 1):
        if attempt > 0:
            _log_line(f"[{phase}] Kimi retry attempt {attempt}/{max_retries} due to validation errors")
            # Build retry prompt with error feedback
            error_feedback = "\n".join(f"- {e}" for e in last_errors[:5])
            current_prompt = f"""Your previous JSON output had validation errors. Fix them.

Validation Errors:
{error_feedback}

Previous Output:
```
{last_raw_text[:4000]}
```

Original Task:
{base_prompt}

CRITICAL: Output ONLY valid JSON. Fix ALL validation errors above."""
        
        # Run Kimi
        cmd = [
            "kimi",
            "--work-dir", str(REPO_ROOT),
            "--yolo",
            "--print",
            "--output-format", "stream-json",
            "--prompt", current_prompt,
        ]
        if model:
            cmd.extend(["--model", model])
        
        _log_line(f"[{phase}] Running Kimi (attempt {attempt + 1}/{max_retries + 1})")
        
        try:
            proc = subprocess.Popen(
                cmd,
                cwd=REPO_ROOT,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
        except FileNotFoundError as exc:
            raise RuntimeError("kimi CLI was not found on PATH") from exc
        
        json_lines: list[str] = []
        
        try:
            assert proc.stdout is not None
            
            deadline = time.monotonic() + CLI_EXEC_TIMEOUT_SECONDS
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    proc.kill()
                    raise RuntimeError(f"Kimi timed out after {CLI_EXEC_TIMEOUT_SECONDS}s")
                
                ready, _, _ = select.select([proc.stdout], [], [], min(1.0, remaining))
                if ready:
                    line = proc.stdout.readline()
                    if line:
                        stripped = line.strip()
                        if stripped:
                            json_lines.append(stripped)
                        _log_line(f"{_phase_prefix(phase)} {_colorize_codemsg(line.rstrip(), cli='kimi')}")
                        continue
                
                if proc.poll() is not None:
                    break
            
            tail = proc.stdout.read()
            if tail:
                for line in tail.splitlines():
                    stripped = line.strip()
                    if stripped:
                        json_lines.append(stripped)
                    _log_line(f"{_phase_prefix(phase)} {_colorize_codemsg(line, cli='kimi')}")
            
            returncode = proc.wait()
            
            # Extract text from kimi output
            raw_text = _extract_kimi_text(json_lines)
            last_raw_text = raw_text
            
            if not raw_text.strip():
                last_errors = ["No text output from Kimi"]
                if attempt < max_retries:
                    continue
                raise RuntimeError(f"No text content found in kimi output during {phase}")
            
            # Try to extract and validate JSON
            parsed = _extract_json_from_text(raw_text)
            if parsed is None:
                last_errors = ["Could not parse valid JSON from output"]
                if attempt < max_retries:
                    continue
                raise RuntimeError(f"Failed to parse JSON from kimi output during {phase}")
            
            # Validate against schema
            validation_errors = _validate_against_schema(parsed, schema)
            if validation_errors:
                last_errors = validation_errors
                if attempt < max_retries:
                    continue
                # Final attempt failed - save what we have but warn
                _log_line(f"[{phase}] Warning: JSON validation failed after {max_retries + 1} attempts", stderr=True)
                for err in validation_errors[:5]:
                    _log_line(f"[{phase}]   - {err}", stderr=True)
            
            # Save the result (even if validation failed, we have best effort)
            output_path.write_text(json.dumps(parsed, indent=2), encoding="utf-8")
            _log_line(f"[{phase}] Kimi completed (validation: {'passed' if not validation_errors else 'failed'})")
            return
            
        finally:
            if proc.poll() is None:
                proc.kill()
    
    # Should not reach here, but just in case
    raise RuntimeError(f"Kimi failed after {max_retries + 1} attempts")


def _build_kimi_structured_prompt(base_prompt: str, schema_content: str) -> str:
    """Build a prompt that strongly encourages valid JSON output."""
    return f"""You are a JSON-only API. Return ONLY valid JSON matching the schema below.

=== TASK ===
{base_prompt}

=== SCHEMA ===
{schema_content}

=== RULES ===
1. Output MUST be valid, parseable JSON
2. Do NOT wrap in ```json code blocks - output raw JSON only
3. Do NOT include explanations or conversational text
4. Every required field must be present
5. Use empty strings "" for optional fields you don't populate
6. Use empty arrays [] for array fields with no items
7. Property names must match the schema exactly (case-sensitive)
8. All values must be the correct type (string, number, boolean, array, object)

Output ONLY the JSON object, nothing else."""


def _extract_kimi_text(lines: list[str]) -> str:
    """Extract text content from Kimi stream-json output."""
    text_parts: list[str] = []
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            parsed = json.loads(line)
            if isinstance(parsed, dict) and parsed.get("role") == "assistant":
                content = parsed.get("content", [])
                if isinstance(content, list):
                    for item in content:
                        if isinstance(item, dict) and item.get("type") == "text":
                            text = item.get("text", "")
                            if text:
                                text_parts.append(text)
                elif isinstance(content, str):
                    text_parts.append(content)
        except json.JSONDecodeError:
            continue
    
    return "".join(text_parts)


def _extract_json_from_text(text: str) -> dict[str, Any] | None:
    """Try to extract JSON from text that might have markdown or extra content."""
    text = text.strip()
    
    # Try direct parse first
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    
    # Try removing markdown code blocks
    import re
    
    # Pattern 1: ```json ... ```
    match = re.search(r'```(?:json)?\s*\n?(.*?)\n?```', text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1).strip())
        except json.JSONDecodeError:
            pass
    
    # Pattern 2: Find JSON object/array at start
    for pattern in [r'^(\{[\s\S]*\})\s*$', r'^(\[[\s\S]*\])\s*$']:
        match = re.search(pattern, text)
        if match:
            try:
                return json.loads(match.group(1).strip())
            except json.JSONDecodeError:
                continue
    
    # Pattern 3: Find JSON object/array anywhere
    for pattern in [r'(\{[\s\S]*\})', r'(\[[\s\S]*\])']:
        matches = list(re.finditer(pattern, text))
        # Try the largest match first (most likely to be complete)
        for match in sorted(matches, key=lambda m: len(m.group(1)), reverse=True):
            try:
                return json.loads(match.group(1).strip())
            except json.JSONDecodeError:
                continue
    
    return None


def _validate_against_schema(data: Any, schema: dict, path: str = "root") -> list[str]:
    """Basic schema validation, returns list of error messages."""
    errors: list[str] = []
    
    if not isinstance(schema, dict):
        return errors
    
    schema_type = schema.get("type")
    
    if schema_type == "object":
        if not isinstance(data, dict):
            errors.append(f"Expected object at {path}, got {type(data).__name__}")
            return errors
        
        # Check required fields
        required = schema.get("required", [])
        for field in required:
            if field not in data:
                errors.append(f"Missing required field: {path}.{field}")
        
        # Validate properties
        properties = schema.get("properties", {})
        for key, prop_schema in properties.items():
            if key in data:
                sub_errors = _validate_against_schema(data[key], prop_schema, f"{path}.{key}")
                errors.extend(sub_errors)
    
    elif schema_type == "array":
        if not isinstance(data, list):
            errors.append(f"Expected array at {path}, got {type(data).__name__}")
            return errors
        
        items_schema = schema.get("items")
        if items_schema:
            for i, item in enumerate(data):
                sub_errors = _validate_against_schema(item, items_schema, f"{path}[{i}]")
                errors.extend(sub_errors)
    
    elif schema_type == "string":
        if not isinstance(data, str):
            errors.append(f"Expected string at {path}, got {type(data).__name__}")
    
    elif schema_type == "integer":
        if not isinstance(data, int) or isinstance(data, bool):
            errors.append(f"Expected integer at {path}, got {type(data).__name__}")
    
    elif schema_type == "number":
        if not isinstance(data, (int, float)) or isinstance(data, bool):
            errors.append(f"Expected number at {path}, got {type(data).__name__}")
    
    elif schema_type == "boolean":
        if not isinstance(data, bool):
            errors.append(f"Expected boolean at {path}, got {type(data).__name__}")
    
    return errors


@dataclass
class MatchResult:
    start: int  # UTF-16 code unit offset
    end: int    # UTF-16 code unit offset
    score: float


def _derive_range_from_quoted_text(
    quoted_text: str, 
    unit_text: str, 
    op_index: int,
    chunk_id: str = ""
) -> tuple[int, int]:
    """
    Derive UTF-16 character range from quoted text with inline markers.
    
    Args:
        quoted_text: Text with [[target]] markers
        unit_text: Full text of the target unit (Unicode string)
        op_index: Index for error reporting
        chunk_id: Chunk ID for error reporting
        
    Returns:
        (start, end) UTF-16 code unit positions (not Python string indices)
        
    Raises:
        RuntimeError: If quoted_text is invalid or cannot be found
    """
    # Validate brackets exist and are properly paired
    if quoted_text.count("[[") != 1 or quoted_text.count("]]") != 1:
        raise RuntimeError(
            f"Chunk {chunk_id} op {op_index}: quoted_text must contain exactly one [[ and one ]]. "
            f"Got: {quoted_text[:50]}..."
        )
    
    # Use regex to extract components (handles nested brackets gracefully)
    # Pattern: everything up to [[, then target, then everything after ]]
    match = re.match(r'^(.*?)\[\[(.*?)\]\](.*)$', quoted_text, re.DOTALL)
    if not match:
        raise RuntimeError(
            f"Chunk {chunk_id} op {op_index}: Cannot parse [[markers]] in quoted_text. "
            f"Got: {quoted_text[:50]}..."
        )
    
    pre, target, post = match.groups()
    
    # Handle insert-at case: collapsed markers [[ ]] or [[]]
    is_insert_at = target.strip() == ""
    
    # Build search text (quote without brackets)
    # Normalize Unicode to NFC for matching
    search_text = unicodedata.normalize('NFC', pre + target + post)
    unit_text_nfc = unicodedata.normalize('NFC', unit_text)
    
    # Fuzzy find
    match_result = _fuzzy_find(search_text, unit_text_nfc, threshold=0.80)
    if not match_result:
        # Try with whitespace normalized
        search_normalized = re.sub(r'\s+', ' ', search_text).strip()
        unit_normalized = re.sub(r'\s+', ' ', unit_text_nfc)
        match_result = _fuzzy_find(search_normalized, unit_normalized, threshold=0.85)
        
    if not match_result:
        raise RuntimeError(
            f"Chunk {chunk_id} op {op_index}: Cannot find quoted_text in unit.\n"
            f"  Quoted: {quoted_text[:80]}...\n"
            f"  Target: {target[:40] if target else '(insert)'}")
    
    # Calculate positions in Unicode codepoints first
    match_start_cp = match_result.start
    
    # Adjust for pre-context to get actual target start
    # Normalize pre the same way we normalized search_text
    pre_normalized = unicodedata.normalize('NFC', pre)
    if pre_normalized:
        # Find pre_normalized within the matched text
        # Since we fuzzy-matched, we need to find where pre ends in the match
        matched_text = unit_text_nfc[match_start_cp:match_start_cp + len(search_text)]
        # Find where target starts within the matched text
        target_idx_in_match = matched_text.find(target) if target else len(pre_normalized)
        if target_idx_in_match < 0:
            target_idx_in_match = len(pre_normalized)  # Fallback
    else:
        target_idx_in_match = 0
    
    target_start_cp = match_start_cp + target_idx_in_match
    target_end_cp = target_start_cp + len(target) if target else target_start_cp
    
    # Convert to UTF-16 code units for DOCX compatibility
    text_before_start = unit_text_nfc[:target_start_cp]
    text_before_end = unit_text_nfc[:target_end_cp]
    
    start_utf16 = len(text_before_start.encode('utf-16-le')) // 2
    end_utf16 = len(text_before_end.encode('utf-16-le')) // 2
    
    return start_utf16, end_utf16


def _fuzzy_find(needle: str, haystack: str, threshold: float = 0.85) -> MatchResult | None:
    """
    Find needle in haystack with fuzzy matching.
    
    Uses SequenceMatcher for similarity. Optimized with early-exit for exact matches.
    Returns MatchResult with UTF-16 positions, or None if no match above threshold.
    """
    # Early exit: exact match
    idx = haystack.find(needle)
    if idx >= 0:
        # Calculate UTF-16 position
        text_before = haystack[:idx]
        start_utf16 = len(text_before.encode('utf-16-le')) // 2
        end_utf16 = start_utf16 + len(needle.encode('utf-16-le')) // 2
        return MatchResult(start=start_utf16, end=end_utf16, score=1.0)
    
    # For short needles, sliding window is fast enough
    # For long needles, use larger step or Boyer-Moore pre-filter
    needle_len = len(needle)
    haystack_len = len(haystack)
    
    if needle_len > haystack_len:
        return None
    
    # Optimization: if needle is long (>200 chars), check fewer windows
    step = 1 if needle_len < 100 else 5
    
    best_score = 0.0
    best_start = -1
    
    for i in range(0, haystack_len - needle_len + 1, step):
        window = haystack[i:i + needle_len]
        score = SequenceMatcher(None, needle, window).ratio()
        if score > best_score:
            best_score = score
            best_start = i
            if score == 1.0:  # Exact match found
                break
    
    if best_score >= threshold:
        # Fine-tune around best match (in case we skipped with step > 1)
        if step > 1:
            start_fine = max(0, best_start - step)
            end_fine = min(haystack_len - needle_len + 1, best_start + step + 1)
            for i in range(start_fine, end_fine):
                window = haystack[i:i + needle_len]
                score = SequenceMatcher(None, needle, window).ratio()
                if score > best_score:
                    best_score = score
                    best_start = i
        
        text_before = haystack[:best_start]
        start_utf16 = len(text_before.encode('utf-16-le')) // 2
        end_utf16 = start_utf16 + len(needle.encode('utf-16-le')) // 2
        return MatchResult(start=start_utf16, end=end_utf16, score=best_score)
    
    return None


def _render_template(path: Path, replacements: dict[str, str]) -> str:
    text = path.read_text(encoding="utf-8")
    for key, value in replacements.items():
        text = text.replace(f"{{{{{key}}}}}", value)
    return text


def _target_key(raw_target: Any) -> tuple[str, str, str] | None:
    if not isinstance(raw_target, dict):
        return None
    part = str(raw_target.get("part", "")).strip()
    para_id = str(raw_target.get("para_id", "")).strip()
    unit_uid = str(raw_target.get("unit_uid", "")).strip()
    if not part or not para_id or not unit_uid:
        return None
    return (part, para_id, unit_uid)


def _targets_from_units(units: list[dict[str, Any]]) -> list[dict[str, str]]:
    targets: list[dict[str, str]] = []
    for unit in units:
        key = _target_key(unit)
        if key is None:
            continue
        part, para_id, unit_uid = key
        targets.append({"part": part, "para_id": para_id, "unit_uid": unit_uid})
    return targets


def _manifest_entry_map(manifest: dict[str, Any]) -> dict[str, int]:
    chunks = manifest.get("chunks", [])
    out: dict[str, int] = {}
    if not isinstance(chunks, list):
        return out
    for idx, entry in enumerate(chunks):
        if not isinstance(entry, dict):
            continue
        chunk_id = str(entry.get("chunk_id", "")).strip()
        if chunk_id:
            out[chunk_id] = idx
    return out


def _load_manifest_chunk_payload(paths: ProjectPaths, entry: dict[str, Any]) -> dict[str, Any]:
    rel_path = str(entry.get("path", "")).strip()
    if not rel_path:
        raise RuntimeError(f"Chunk path missing for entry: {entry.get('chunk_id')}")
    chunk_path = paths.chunks_output_dir / rel_path
    if not chunk_path.exists():
        raise FileNotFoundError(f"Chunk file not found: {chunk_path}")
    payload = _load_json(chunk_path)
    if not isinstance(payload, dict):
        raise RuntimeError(f"Chunk payload must be an object: {chunk_path}")
    return payload


def _write_manifest_and_chunks(paths: ProjectPaths, manifest: dict[str, Any], chunk_payloads: dict[str, dict[str, Any]]) -> None:
    chunks = manifest.get("chunks", [])
    if not isinstance(chunks, list):
        raise RuntimeError("Manifest chunks must be a list")

    for idx, entry in enumerate(chunks):
        if not isinstance(entry, dict):
            continue
        chunk_id = str(entry.get("chunk_id", "")).strip()
        rel_path = str(entry.get("path", "")).strip()
        if not chunk_id or not rel_path:
            continue
        payload = chunk_payloads.get(chunk_id)
        if payload is None:
            continue

        payload["chunk_id"] = chunk_id
        payload["chunk_index"] = idx

        primary_units = payload.get("primary_units", [])
        context_before = payload.get("context_units_before", [])
        context_after = payload.get("context_units_after", [])
        if not isinstance(primary_units, list):
            primary_units = []
            payload["primary_units"] = primary_units
        if not isinstance(context_before, list):
            context_before = []
            payload["context_units_before"] = context_before
        if not isinstance(context_after, list):
            context_after = []
            payload["context_units_after"] = context_after

        for unit in primary_units:
            if isinstance(unit, dict):
                unit["role"] = "primary"
                unit["editable"] = True
        for unit in context_before:
            if isinstance(unit, dict):
                unit["role"] = "context_before"
                unit["editable"] = False
        for unit in context_after:
            if isinstance(unit, dict):
                unit["role"] = "context_after"
                unit["editable"] = False

        entry["chunk_id"] = chunk_id
        entry["primary_targets"] = _targets_from_units(primary_units)
        entry["context_targets_before"] = _targets_from_units(context_before)
        entry["context_targets_after"] = _targets_from_units(context_after)
        entry["context_before_unit_uids"] = [t["unit_uid"] for t in entry["context_targets_before"]]
        entry["context_after_unit_uids"] = [t["unit_uid"] for t in entry["context_targets_after"]]

        metadata = payload.get("metadata")
        if isinstance(metadata, dict):
            source_span = metadata.get("source_span")
            if isinstance(source_span, dict):
                source_span["primary_unit_count"] = len(primary_units)
                source_span["context_before_count"] = len(context_before)
                source_span["context_after_count"] = len(context_after)

        _dump_json(paths.chunks_output_dir / rel_path, payload)

    manifest["chunk_count"] = len(chunks)
    _dump_json(paths.chunks_output_dir / "manifest.json", manifest)


def _apply_shift_boundary(
    *,
    left_payload: dict[str, Any],
    right_payload: dict[str, Any],
    move_primary_units: int,
    left_chunk_id: str,
    right_chunk_id: str,
) -> None:
    left_primary = left_payload.get("primary_units", [])
    right_primary = right_payload.get("primary_units", [])

    if not isinstance(left_primary, list) or not isinstance(right_primary, list):
        raise RuntimeError("Chunk payload primary_units must be lists")

    if move_primary_units == 0:
        return

    if move_primary_units > 0:
        if move_primary_units > len(right_primary):
            raise RuntimeError(f"shift_boundary move exceeds right chunk size: {right_chunk_id}")
        moved = right_primary[:move_primary_units]
        left_payload["primary_units"] = left_primary + moved
        right_payload["primary_units"] = right_primary[move_primary_units:]
    else:
        count = -move_primary_units
        if count > len(left_primary):
            raise RuntimeError(f"shift_boundary move exceeds left chunk size: {left_chunk_id}")
        moved = left_primary[-count:]
        left_payload["primary_units"] = left_primary[:-count]
        right_payload["primary_units"] = moved + right_primary

    left_after = right_payload["primary_units"][0:1] if right_payload.get("primary_units") else []
    right_before = left_payload["primary_units"][-1:] if left_payload.get("primary_units") else []
    left_payload["context_units_after"] = left_after
    right_payload["context_units_before"] = right_before


def _apply_merge_adjacent(
    *,
    left_payload: dict[str, Any],
    right_payload: dict[str, Any],
) -> None:
    left_primary = left_payload.get("primary_units", [])
    right_primary = right_payload.get("primary_units", [])
    left_before = left_payload.get("context_units_before", [])
    right_after = right_payload.get("context_units_after", [])

    if not isinstance(left_primary, list) or not isinstance(right_primary, list):
        raise RuntimeError("Chunk payload primary_units must be lists")

    left_payload["primary_units"] = left_primary + right_primary
    left_payload["context_units_before"] = left_before if isinstance(left_before, list) else []
    left_payload["context_units_after"] = right_after if isinstance(right_after, list) else []


def _apply_chunk_boundary_fixes(paths: ProjectPaths, fixes: list[dict[str, Any]]) -> dict[str, Any]:
    manifest_path = paths.chunks_output_dir / "manifest.json"
    manifest = _load_json(manifest_path)
    if not isinstance(manifest, dict):
        raise RuntimeError("Chunk manifest payload must be an object")

    chunks = manifest.get("chunks", [])
    if not isinstance(chunks, list):
        raise RuntimeError("Chunk manifest must include a list at chunks")

    applied: list[dict[str, Any]] = []
    chunk_payloads: dict[str, dict[str, Any]] = {}

    for fix in fixes:
        if not isinstance(fix, dict):
            continue
        fix_type = str(fix.get("type", "")).strip()
        left_chunk_id = str(fix.get("left_chunk_id", "")).strip()
        right_chunk_id = str(fix.get("right_chunk_id", "")).strip()

        index_map = _manifest_entry_map(manifest)
        if left_chunk_id not in index_map or right_chunk_id not in index_map:
            raise RuntimeError(f"Chunk QA fix references unknown chunk ids: {left_chunk_id}, {right_chunk_id}")

        left_idx = index_map[left_chunk_id]
        right_idx = index_map[right_chunk_id]
        if right_idx != left_idx + 1:
            raise RuntimeError(f"Chunk QA fix must reference adjacent chunks in manifest order: {left_chunk_id}, {right_chunk_id}")

        left_entry = chunks[left_idx]
        right_entry = chunks[right_idx]
        if not isinstance(left_entry, dict) or not isinstance(right_entry, dict):
            raise RuntimeError("Chunk manifest entries must be objects")

        left_payload = chunk_payloads.get(left_chunk_id) or _load_manifest_chunk_payload(paths, left_entry)
        right_payload = chunk_payloads.get(right_chunk_id) or _load_manifest_chunk_payload(paths, right_entry)

        if fix_type == "shift_boundary":
            move_primary_units = int(fix.get("move_primary_units", 0))
            _apply_shift_boundary(
                left_payload=left_payload,
                right_payload=right_payload,
                move_primary_units=move_primary_units,
                left_chunk_id=left_chunk_id,
                right_chunk_id=right_chunk_id,
            )
            chunk_payloads[left_chunk_id] = left_payload
            chunk_payloads[right_chunk_id] = right_payload
            applied.append(
                {
                    "type": "shift_boundary",
                    "left_chunk_id": left_chunk_id,
                    "right_chunk_id": right_chunk_id,
                    "move_primary_units": move_primary_units,
                }
            )
            continue

        if fix_type == "merge_adjacent":
            _apply_merge_adjacent(left_payload=left_payload, right_payload=right_payload)
            chunk_payloads[left_chunk_id] = left_payload

            right_rel_path = str(right_entry.get("path", "")).strip()
            if right_rel_path:
                right_file = paths.chunks_output_dir / right_rel_path
                if right_file.exists():
                    right_file.unlink()

            chunks.pop(right_idx)
            chunk_payloads.pop(right_chunk_id, None)

            applied.append(
                {
                    "type": "merge_adjacent",
                    "left_chunk_id": left_chunk_id,
                    "right_chunk_id": right_chunk_id,
                }
            )
            continue

        raise RuntimeError(f"Unsupported chunk QA fix type: {fix_type}")

    _write_manifest_and_chunks(paths, manifest, chunk_payloads)
    return {"applied_fixes": applied, "chunk_count": len(chunks)}


def _run_chunk_qa_with_optional_fix(paths: ProjectPaths, *, cli: str, model: str | None = None) -> dict[str, Any]:
    manifest_path = paths.chunks_output_dir / "manifest.json"
    manifest = _load_json(manifest_path)
    chunks = manifest.get("chunks", []) if isinstance(manifest, dict) else []
    if not isinstance(chunks, list) or not chunks:
        raise RuntimeError("Chunk manifest has no chunks for QA")

    sample_paths = []
    for entry in chunks[:3]:
        if not isinstance(entry, dict):
            continue
        rel = str(entry.get("path", "")).strip()
        if rel:
            sample_paths.append(str((paths.chunks_output_dir / rel).resolve()))

    prompt = _render_template(
        TEMPLATE_CHUNK_QA,
        {
            "MANIFEST_PATH": str(manifest_path.resolve()),
            "SAMPLE_CHUNK_PATHS": "\n".join(sample_paths),
        },
    )
    _run_cli_exec(
        cli=cli,
        prompt=prompt,
        schema_path=SCHEMA_CHUNK_QA,
        output_path=paths.chunk_qa_report,
        phase="chunk QA pass 1",
        model=model,
    )
    first_report = _load_json(paths.chunk_qa_report)

    status = str(first_report.get("status", "")).strip()
    if status == "ok":
        return {"status": "ok", "passes": 1, "applied_fixes": []}

    if status != "needs_fix":
        raise RuntimeError(f"Unexpected chunk QA status: {status!r}")

    fixes = first_report.get("fixes", [])
    if not isinstance(fixes, list) or not fixes:
        raise RuntimeError("Chunk QA requested fixes but returned no fixes")

    apply_result = _apply_chunk_boundary_fixes(paths, fixes)

    manifest = _load_json(manifest_path)
    chunks = manifest.get("chunks", []) if isinstance(manifest, dict) else []
    sample_paths = []
    if isinstance(chunks, list):
        for entry in chunks[:3]:
            if not isinstance(entry, dict):
                continue
            rel = str(entry.get("path", "")).strip()
            if rel:
                sample_paths.append(str((paths.chunks_output_dir / rel).resolve()))

    second_prompt = _render_template(
        TEMPLATE_CHUNK_QA,
        {
            "MANIFEST_PATH": str(manifest_path.resolve()),
            "SAMPLE_CHUNK_PATHS": "\n".join(sample_paths),
        },
    )
    _run_cli_exec(
        cli=cli,
        prompt=second_prompt,
        schema_path=SCHEMA_CHUNK_QA,
        output_path=paths.chunk_qa_report,
        phase="chunk QA pass 2",
        model=model,
    )
    second_report = _load_json(paths.chunk_qa_report)
    second_status = str(second_report.get("status", "")).strip()
    if second_status != "ok":
        raise RuntimeError(f"Chunk QA still failing after deterministic fixes. See {paths.chunk_qa_report}")

    return {"status": "ok", "passes": 2, "applied_fixes": apply_result.get("applied_fixes", [])}


def _utf16_offsets(text: str) -> list[int]:
    offsets = [0]
    total = 0
    for char in text:
        total += len(char.encode("utf-16-le")) // 2
        offsets.append(total)
    return offsets


def _ranges_overlap(left_start: int, left_end: int, right_start: int, right_end: int) -> bool:
    left_point = left_start == left_end
    right_point = right_start == right_end

    if left_point and right_point:
        return left_start == right_start
    if left_point:
        return right_start <= left_start <= right_end
    if right_point:
        return left_start <= right_start <= left_end

    return max(left_start, right_start) < min(left_end, right_end)


def _build_quoted_text(text: str, target_start: int, target_end: int, min_context: int = 15, max_context: int = 40) -> str:
    """Build quoted_text with [[target]] markers from character positions.
    
    Args:
        text: The full unit text
        target_start: Start position in the text (Python string index)
        target_end: End position in the text (Python string index)
        min_context: Minimum context chars before and after target
        max_context: Maximum context chars before and after target
    
    Returns:
        quoted_text with [[target]] markers
    """
    # Get target text
    target = text[target_start:target_end]
    
    # Calculate context bounds
    context_before_start = max(0, target_start - max_context)
    context_before_end = max(0, target_start - min_context)
    context_after_start = min(len(text), target_end + min_context)
    context_after_end = min(len(text), target_end + max_context)
    
    # Get context with at least min_context chars if available
    if context_before_end > context_before_start:
        before = text[context_before_end:target_start]
    else:
        # Try to get more context if available
        before = text[context_before_start:target_start]
    
    if context_after_end > context_after_start:
        after = text[target_end:context_after_start]
    else:
        # Try to get more context if available
        after = text[target_end:context_after_end]
    
    # Handle edge cases at start/end
    if target_start == 0:
        quoted = f"[[{target}]]{after}"
    elif target_end >= len(text):
        quoted = f"{before}[[{target}]]"
    else:
        quoted = f"{before}[[{target}]]{after}"
    
    return quoted


def _build_ops_for_unit(unit: dict[str, Any]) -> list[dict[str, Any]] | None:
    accepted_text = str(unit.get("accepted_text", ""))
    part = str(unit.get("part", "")).strip()
    para_id = str(unit.get("para_id", "")).strip()
    unit_uid = str(unit.get("unit_uid", "")).strip()

    if not accepted_text or not part or not para_id or not unit_uid:
        return None

    matches = list(WORD_RE.finditer(accepted_text))
    if len(matches) < 4:
        return None

    counts = Counter(match.group(0) for match in matches)
    unique_matches = [match for match in matches if counts[match.group(0)] == 1]
    if len(unique_matches) < 4:
        return None

    for indices in itertools.combinations(range(len(unique_matches)), 4):
        comment_match = unique_matches[indices[0]]
        replace_match = unique_matches[indices[1]]
        insert_match = unique_matches[indices[2]]
        delete_match = unique_matches[indices[3]]

        # Get Python string positions
        comment_start, comment_end = comment_match.start(), comment_match.end()
        replace_start, replace_end = replace_match.start(), replace_match.end()
        delete_start, delete_end = delete_match.start(), delete_match.end()
        insert_pos = insert_match.end()

        # Build quoted_text for each operation
        replace_quoted = _build_quoted_text(accepted_text, replace_start, replace_end)
        delete_quoted = _build_quoted_text(accepted_text, delete_start, delete_end)
        comment_quoted = _build_quoted_text(accepted_text, comment_start, comment_end)
        
        # For insert_at: use [[ ]] at insertion point
        insert_before = max(0, insert_pos - 20)
        insert_after = min(len(accepted_text), insert_pos + 20)
        before_text = accepted_text[insert_before:insert_pos]
        after_text = accepted_text[insert_pos:insert_after]
        insert_quoted = f"{before_text}[[ ]]{after_text}"

        # Check for overlaps in Python positions (not UTF-16 for dry-run check)
        if _ranges_overlap(replace_start, replace_end, delete_start, delete_end):
            continue
        if _ranges_overlap(insert_pos, insert_pos, replace_start, replace_end):
            continue
        if _ranges_overlap(insert_pos, insert_pos, delete_start, delete_end):
            continue

        target = {
            "part": part,
            "para_id": para_id,
            "unit_uid": unit_uid,
        }

        replace_before = replace_match.group(0)
        replacement = replace_before.upper()
        if replacement == replace_before:
            replacement = f"{replace_before}_DRYRUN"

        return [
            {
                "type": "replace_range",
                "target": target,
                "quoted_text": replace_quoted,
                "expected": {"snippet": replace_before},
                "replacement": replacement,
                "new_text": "",
                "comment_text": "",
            },
            {
                "type": "insert_at",
                "target": target,
                "quoted_text": insert_quoted,
                "expected": {"snippet": ""},
                "new_text": " [DRY-RUN]",
                "replacement": "",
                "comment_text": "",
            },
            {
                "type": "replace_range",
                "target": target,
                "quoted_text": delete_quoted,
                "expected": {"snippet": delete_match.group(0)},
                "replacement": "",
                "new_text": "",
                "comment_text": "",
            },
            {
                "type": "add_comment",
                "target": target,
                "quoted_text": comment_quoted,
                "expected": {"snippet": comment_match.group(0)},
                "comment_text": "Dry-run QA marker comment.",
                "replacement": "",
                "new_text": "",
            },
        ]

    return None


def _discover_synthetic_chunk_result(paths: ProjectPaths) -> SyntheticChunkResult:
    manifest_path = paths.chunks_output_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Chunk manifest missing: {manifest_path}")

    manifest = _load_json(manifest_path)
    chunk_entries = manifest.get("chunks", [])
    if not isinstance(chunk_entries, list) or not chunk_entries:
        raise RuntimeError("Chunk manifest has no chunks")

    for stale in paths.chunk_results_dir.glob("chunk_*_result.json"):
        stale.unlink()

    for entry in chunk_entries:
        if not isinstance(entry, dict):
            continue
        chunk_file = entry.get("path")
        chunk_id = str(entry.get("chunk_id", "")).strip()
        if not chunk_file or not chunk_id:
            continue

        chunk_path = paths.chunks_output_dir / str(chunk_file)
        if not chunk_path.exists():
            continue

        chunk_payload = _load_json(chunk_path)
        primary_units = chunk_payload.get("primary_units", [])
        if not isinstance(primary_units, list):
            continue

        for unit in primary_units:
            if not isinstance(unit, dict):
                continue
            ops = _build_ops_for_unit(unit)
            if ops is None:
                continue

            output_path = paths.chunk_results_dir / f"{chunk_id}_result.json"
            _dump_json(
                output_path,
                {
                    "schema_version": "chunk_result.v1",
                    "chunk_id": chunk_id,
                    "status": "ok",
                    "summary": "Synthetic dry-run operations generated from primary_units text.",
                    "ops": ops,
                    "suggestions": ["dry_run_synthetic_ops"],
                },
            )
            return SyntheticChunkResult(chunk_id=chunk_id, output_path=output_path, op_count=len(ops))

    raise RuntimeError("Unable to generate synthetic chunk results from chunk primary_units")


def _normalize_range(raw_range: Any) -> dict[str, int] | None:
    if not isinstance(raw_range, dict):
        return None
    start = raw_range.get("start")
    end = raw_range.get("end")
    if not isinstance(start, int) or not isinstance(end, int):
        return None
    if start < 0 or end < 0 or start > end:
        return None
    return {"start": start, "end": end}


def _sanitize_chunk_result_ops(
    *,
    chunk_id: str,
    raw_payload: dict[str, Any],
    chunk_payload: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    primary_units = chunk_payload.get("primary_units", [])
    if not isinstance(primary_units, list) or not primary_units:
        raise RuntimeError(f"Chunk {chunk_id} has no primary_units")

    primary_targets: set[tuple[str, str, str]] = set()
    primary_by_para: dict[tuple[str, str], list[str]] = {}
    for unit in primary_units:
        key = _target_key(unit)
        if key is None:
            continue
        primary_targets.add(key)
        part, para_id, unit_uid = key
        primary_by_para.setdefault((part, para_id), []).append(unit_uid)

    first_primary = next(iter(primary_targets), None)
    if first_primary is None:
        raise RuntimeError(f"Chunk {chunk_id} has no valid primary target identities")

    # Build lookup map: unit_uid -> (part, para_id, unit_uid) from chunk's primary_units
    # We ALWAYS use this for target resolution since unit_uid is the unique identifier
    # This is deterministic and ignores any AI hallucinations in part/para_id fields
    unit_uid_to_target: dict[str, tuple[str, str, str]] = {}
    for unit in primary_units:
        if isinstance(unit, dict):
            uid = str(unit.get("unit_uid", "")).strip()
            part = str(unit.get("part", "")).strip()
            para_id = str(unit.get("para_id", "")).strip()
            if uid and part and para_id:
                unit_uid_to_target[uid] = (part, para_id, uid)

    raw_ops = raw_payload.get("ops", [])
    if not isinstance(raw_ops, list):
        raw_ops = []

    sanitized_ops: list[dict[str, Any]] = []
    kept = 0
    conversions: list[dict[str, Any]] = []

    for idx, raw_op in enumerate(raw_ops):
        if not isinstance(raw_op, dict):
            conversions.append({"index": idx, "reason": "not_a_dict", "raw_type": str(type(raw_op))})
            continue

        op_type = str(raw_op.get("type", "")).strip()
        if op_type not in VALID_OP_TYPES:
            conversions.append({"index": idx, "reason": "invalid_type", "type": op_type})
            continue

        # ALWAYS resolve target using unit_uid from the AI's output
        # but use the chunk's ground truth for part/para_id/unit_uid
        target = raw_op.get("target")
        key = None
        if isinstance(target, dict):
            unit_uid = str(target.get("unit_uid", "")).strip()
            if unit_uid in unit_uid_to_target:
                key = unit_uid_to_target[unit_uid]  # Use chunk's ground truth

        if key is None:
            conversions.append({"index": idx, "reason": "non_primary_target", "target": target})
            continue

        normalized_target = {"part": key[0], "para_id": key[1], "unit_uid": key[2]}
        expected = raw_op.get("expected")
        snippet = ""
        if isinstance(expected, dict):
            snippet = str(expected.get("snippet", ""))

        # Get unit text from primary_units for range derivation
        unit_text = ""
        for unit in primary_units:
            if isinstance(unit, dict) and str(unit.get("unit_uid", "")).strip() == key[2]:
                unit_text = str(unit.get("accepted_text", ""))
                break
        
        # Derive range from quoted_text instead of using LLM-provided range
        quoted_text = raw_op.get("quoted_text", "")
        normalized_range = None
        
        if quoted_text:
            try:
                start_utf16, end_utf16 = _derive_range_from_quoted_text(
                    quoted_text, unit_text, idx, chunk_id
                )
                normalized_range = {"start": start_utf16, "end": end_utf16}
            except RuntimeError as e:
                # Fall back to provided range if derivation fails
                pass
        
        # Fallback to provided range if quoted_text derivation failed or was missing
        if normalized_range is None:
            normalized_range = _normalize_range(raw_op.get("range"))
        
        if normalized_range is None:
            conversions.append({"index": idx, "reason": "missing_range", "type": op_type})
            continue

        sanitized: dict[str, Any] = {
            "type": op_type,
            "target": normalized_target,
            "expected": {"snippet": snippet},
            "range": normalized_range,
        }

        if op_type == "replace_range":
            if "replacement" not in raw_op:
                conversions.append({"index": idx, "reason": "missing_replacement", "target": normalized_target})
                continue
            replacement = str(raw_op.get("replacement", ""))
            # Empty replacement is valid - means delete the range
            sanitized["replacement"] = replacement
        elif op_type == "insert_at":
            if normalized_range["start"] != normalized_range["end"]:
                conversions.append({"index": idx, "reason": "non_collapsed_range", "range": normalized_range})
                continue
            if "new_text" not in raw_op:
                conversions.append({"index": idx, "reason": "missing_new_text", "target": normalized_target})
                continue
            new_text = str(raw_op.get("new_text", ""))
            if new_text == "":
                conversions.append({"index": idx, "reason": "empty_new_text", "target": normalized_target})
                continue
            sanitized["new_text"] = new_text
        elif op_type == "add_comment":
            comment_text = str(raw_op.get("comment_text", "")).strip()
            if not comment_text:
                conversions.append({"index": idx, "reason": "missing_comment_text", "target": normalized_target})
                continue
            sanitized["comment_text"] = comment_text

        sanitized_ops.append(sanitized)
        kept += 1

    # Do not propagate free-text status/progress chatter into saved results.
    # Keep runner-owned suggestion output deterministic and empty.
    suggestions: list[str] = []

    sanitized_payload = {
        "schema_version": "chunk_result.v1",
        "chunk_id": chunk_id,
        "status": "ok",
        "summary": "Chunk result sanitized by runner ownership and shape checks.",
        "ops": sanitized_ops,
        "suggestions": suggestions,
    }

    log_payload = {
        "chunk_id": chunk_id,
        "input_op_count": len(raw_ops),
        "output_op_count": len(sanitized_ops),
        "kept_ops": kept,
        "converted_ops": len(conversions),
        "conversions": conversions,
    }
    return sanitized_payload, log_payload


def _run_chunk_reviews(
    paths: ProjectPaths,
    *,
    chunk_results_dir: Path,
    sanitization_log_path: Path,
    max_concurrency: int,
    cli: str,
    model: str | None = None,
    phase_label: str = "chunk review",
) -> dict[str, Any]:
    manifest_path = paths.chunks_output_dir / "manifest.json"
    manifest = _load_json(manifest_path)
    chunk_entries = manifest.get("chunks", []) if isinstance(manifest, dict) else []
    if not isinstance(chunk_entries, list) or not chunk_entries:
        raise RuntimeError("Chunk manifest has no chunks for review")

    _clear_chunk_result_artifacts(chunk_results_dir)

    def process_chunk(entry: dict[str, Any]) -> dict[str, Any]:
        chunk_id = str(entry.get("chunk_id", "")).strip()
        rel_path = str(entry.get("path", "")).strip()
        if not chunk_id or not rel_path:
            raise RuntimeError("Manifest chunk entry is missing chunk_id/path")

        chunk_path = paths.chunks_output_dir / rel_path
        if not chunk_path.exists():
            raise FileNotFoundError(f"Chunk file missing: {chunk_path}")

        output_path = chunk_results_dir / f"{chunk_id}_result.json"
        workflow_xml = paths.workflow_xml.read_text(encoding="utf-8")
        prompt = _render_template(
            TEMPLATE_CHUNK_REVIEW,
            {
                "WORKFLOW_XML": workflow_xml,
                "CHUNK_PATH": str(chunk_path.resolve()),
            },
        )
        _log_line(f"Chunk review start: {chunk_id}")
        _run_cli_exec(
            cli=cli,
            prompt=prompt,
            schema_path=SCHEMA_CHUNK_REVIEW,
            output_path=output_path,
            phase=f"{phase_label} {chunk_id}",
            model=model,
        )

        raw_payload = _load_json(output_path)
        _dump_json(chunk_results_dir / f"{chunk_id}_result.raw.json", raw_payload)
        chunk_payload = _load_json(chunk_path)

        # Normalize Kimi's output format if needed
        if cli == "kimi":
            _normalize_kimi_ops(raw_payload, chunk_payload)
        sanitized_payload, log_payload = _sanitize_chunk_result_ops(
            chunk_id=chunk_id,
            raw_payload=raw_payload,
            chunk_payload=chunk_payload,
        )
        _dump_json(output_path, sanitized_payload)
        _dump_json(chunk_results_dir / f"{chunk_id}_sanitization.json", log_payload)

        return {
            "chunk_id": chunk_id,
            "result_path": str(output_path),
            "input_ops": log_payload["input_op_count"],
            "output_ops": log_payload["output_op_count"],
        }

    worker_count = max(1, int(max_concurrency))
    summaries: list[dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [executor.submit(process_chunk, entry) for entry in chunk_entries if isinstance(entry, dict)]
        completed = 0
        total = len(futures)
        for future in as_completed(futures):
            summary = future.result()
            summaries.append(summary)
            completed += 1
            _log_line(
                "Chunk review done: "
                f"{summary['chunk_id']} "
                f"({completed}/{total}) "
                f"input_ops={summary['input_ops']} output_ops={summary['output_ops']}"
            )

    summaries.sort(key=lambda item: str(item.get("chunk_id", "")))
    aggregate = {
        "chunk_count": len(summaries),
        "total_input_ops": sum(int(item.get("input_ops", 0)) for item in summaries),
        "total_output_ops": sum(int(item.get("output_ops", 0)) for item in summaries),
        "chunks": summaries,
    }
    _dump_json(sanitization_log_path, aggregate)
    return aggregate


def _run_single_ralph_review(
    paths: ProjectPaths,
    *,
    ralph_index: int,
    max_concurrency: int,
    cli: str,
    model: str | None = None,
) -> dict[str, Any]:
    if ralph_index < 0 or ralph_index >= len(paths.ralph_chunk_results_dirs):
        raise RuntimeError(f"Invalid ralph index: {ralph_index}")

    output_dir = paths.ralph_chunk_results_dirs[ralph_index]
    log_path = output_dir / "sanitization_report.json"

    _log_line(
        f"Ralph run {ralph_index + 1}/{paths.ralph_count} "
        f"output_dir={output_dir}"
    )
    return _run_chunk_reviews(
        paths,
        chunk_results_dir=output_dir,
        sanitization_log_path=log_path,
        max_concurrency=max_concurrency,
        cli=cli,
        model=model,
        phase_label=f"ralph {ralph_index} chunk review",
    )


def _run_judge_phase(paths: ProjectPaths, *, cli: str, model: str | None = None) -> dict[str, Any]:
    manifest_path = paths.chunks_output_dir / "manifest.json"
    manifest = _load_json(manifest_path)
    chunk_entries = manifest.get("chunks", []) if isinstance(manifest, dict) else []
    if not isinstance(chunk_entries, list) or not chunk_entries:
        raise RuntimeError("Chunk manifest has no chunks for judge phase")

    _clear_chunk_result_artifacts(paths.judged_chunk_results_dir)
    workflow_xml = paths.workflow_xml.read_text(encoding="utf-8")

    summaries: list[dict[str, Any]] = []
    for entry in chunk_entries:
        if not isinstance(entry, dict):
            continue

        chunk_id = str(entry.get("chunk_id", "")).strip()
        rel_path = str(entry.get("path", "")).strip()
        if not chunk_id or not rel_path:
            raise RuntimeError("Manifest chunk entry is missing chunk_id/path")

        chunk_path = paths.chunks_output_dir / rel_path
        if not chunk_path.exists():
            raise FileNotFoundError(f"Chunk file missing: {chunk_path}")

        proposal_paths: list[Path] = []
        for ralph_dir in paths.ralph_chunk_results_dirs:
            proposal_path = ralph_dir / f"{chunk_id}_result.json"
            if not proposal_path.exists():
                raise FileNotFoundError(
                    "Judge phase missing ralph proposal: "
                    f"chunk={chunk_id} path={proposal_path}"
                )
            proposal_paths.append(proposal_path)

        output_path = paths.judged_chunk_results_dir / f"{chunk_id}_result.json"
        prompt = _render_template(
            TEMPLATE_RALPH_JUDGE,
            {
                "WORKFLOW_XML": workflow_xml,
                "CHUNK_PATH": str(chunk_path.resolve()),
                "RALPH_COUNT": str(paths.ralph_count),
                "PROPOSAL_PATHS": "\n".join(str(path.resolve()) for path in proposal_paths),
            },
        )

        _log_line(f"Judge start: {chunk_id}")
        _run_cli_exec(
            cli=cli,
            prompt=prompt,
            schema_path=SCHEMA_CHUNK_REVIEW,
            output_path=output_path,
            phase=f"ralph judge {chunk_id}",
            model=model,
        )

        raw_payload = _load_json(output_path)
        _dump_json(paths.judged_chunk_results_dir / f"{chunk_id}_result.raw.json", raw_payload)
        chunk_payload = _load_json(chunk_path)

        if cli == "kimi":
            _normalize_kimi_ops(raw_payload, chunk_payload)
        sanitized_payload, log_payload = _sanitize_chunk_result_ops(
            chunk_id=chunk_id,
            raw_payload=raw_payload,
            chunk_payload=chunk_payload,
        )
        _dump_json(output_path, sanitized_payload)
        _dump_json(paths.judged_chunk_results_dir / f"{chunk_id}_sanitization.json", log_payload)

        summaries.append(
            {
                "chunk_id": chunk_id,
                "result_path": str(output_path),
                "input_ops": log_payload["input_op_count"],
                "output_ops": log_payload["output_op_count"],
            }
        )
        _log_line(
            "Judge done: "
            f"{chunk_id} "
            f"input_ops={log_payload['input_op_count']} "
            f"output_ops={log_payload['output_op_count']}"
        )

    summaries.sort(key=lambda item: str(item.get("chunk_id", "")))
    aggregate = {
        "chunk_count": len(summaries),
        "total_input_ops": sum(int(item.get("input_ops", 0)) for item in summaries),
        "total_output_ops": sum(int(item.get("output_ops", 0)) for item in summaries),
        "chunks": summaries,
        "ralph_count": paths.ralph_count,
        "source_dirs": [str(path) for path in paths.ralph_chunk_results_dirs],
    }
    _dump_json(paths.judged_chunk_results_dir / "sanitization_report.json", aggregate)
    return aggregate


def _enforce_no_sanitized_chunk_ops(paths: ProjectPaths, review_summary: dict[str, Any]) -> None:
    # No longer needed - validation happens immediately during sanitization
    pass


def _resolve_input_file(project_dir: Path, input_arg: str | None) -> Path:
    """Resolve the input DOCX file based on --input arg or auto-detect.
    
    Rules:
    1. If --input is provided, use that file
    2. If --input is not provided and only one .docx exists, use that
    3. If --input is not provided and multiple/no .docx exist, error
    """
    input_dir = project_dir / "input"
    
    if input_arg:
        # User specified a file
        input_path = input_dir / input_arg
        if not input_path.exists():
            raise FileNotFoundError(f"Specified input file not found: {input_path}")
        if input_path.suffix.lower() != ".docx":
            raise ValueError(f"Input file must be a .docx file: {input_path}")
        return input_path.resolve()
    
    # Auto-detect: find all .docx files in input directory
    docx_files = [f for f in input_dir.iterdir() if f.is_file() and f.suffix.lower() == ".docx"]
    
    if len(docx_files) == 0:
        raise FileNotFoundError(f"No .docx files found in {input_dir}. Please add an input file or specify one with --input.")
    
    if len(docx_files) > 1:
        available = ", ".join(sorted(f.name for f in docx_files))
        raise RuntimeError(
            f"Multiple input files found in {input_dir}: {available}. "
            f"Please specify which one to process with --input <filename>"
        )
    
    return docx_files[0].resolve()


def _resolve_paths(args: argparse.Namespace) -> ProjectPaths:
    project_dir = (REPO_ROOT / "projects" / str(args.project)).resolve()
    workflow_xml = (project_dir / "workflows" / f"{args.workflow}.xml").resolve()

    # Resolve input file
    source_docx = _resolve_input_file(project_dir, args.input)
    input_name = source_docx.stem  # filename without extension

    ralph_count = int(getattr(args, "ralph", 1))
    if ralph_count < 1:
        raise ValueError("--ralph must be >= 1")
    use_judge = ralph_count > 1 and not bool(getattr(args, "skip_judge", False))

    # Build output paths based on input filename
    # Intermediate artifacts go in subdirectories named after the input file
    extract_output_dir = (project_dir / "artifacts" / "docx_extract" / input_name).resolve()
    chunks_output_dir = (project_dir / "artifacts" / "chunks" / input_name).resolve()
    ralph_chunk_results_dirs = [
        (project_dir / "artifacts" / f"ralph_{index}" / "chunk_results" / input_name).resolve()
        for index in range(ralph_count)
    ]
    judged_chunk_results_dir = (project_dir / "artifacts" / "judged" / "chunk_results" / input_name).resolve()
    chunk_results_dir = judged_chunk_results_dir if use_judge else ralph_chunk_results_dirs[0]
    patch_output_dir = (project_dir / "artifacts" / "patch" / input_name).resolve()
    apply_log_dir = (project_dir / "artifacts" / "apply" / input_name).resolve()

    return ProjectPaths(
        project_dir=project_dir,
        workflow_xml=workflow_xml,
        source_docx=source_docx,
        input_name=input_name,
        constants=(Path(args.constants).expanduser().resolve() if Path(args.constants).is_absolute() else (REPO_ROOT / Path(args.constants)).resolve()),
        extract_output_dir=extract_output_dir,
        chunks_output_dir=chunks_output_dir,
        ralph_count=ralph_count,
        use_judge=use_judge,
        ralph_chunk_results_dirs=ralph_chunk_results_dirs,
        judged_chunk_results_dir=judged_chunk_results_dir,
        chunk_results_dir=chunk_results_dir,
        patch_output_dir=patch_output_dir,
        merged_patch=(patch_output_dir / "merged_patch.json").resolve(),
        merge_report=(patch_output_dir / "merge_report.json").resolve(),
        final_patch=(patch_output_dir / "final_patch.json").resolve(),
        chunk_qa_report=(chunks_output_dir / "chunk_qa_report.json").resolve(),
        merge_qa_report=(patch_output_dir / "merge_qa_report.json").resolve(),
        final_patch_overrides=(patch_output_dir / "final_patch_overrides.json").resolve(),
        chunk_result_sanitization_log=(chunk_results_dir / "sanitization_report.json").resolve(),
        apply_log=(apply_log_dir / "apply_log.json").resolve(),
        annotated_docx=(project_dir / "output" / f"{input_name}_annotated.docx").resolve(),
        changes_md=(project_dir / "output" / f"{input_name}_changes.md").resolve(),
        changes_json=(project_dir / "output" / f"{input_name}_changes.json").resolve(),
        changes_docx=(project_dir / "output" / f"{input_name}_changes.docx").resolve(),
    )


def _resolve_resume_start(
    args: argparse.Namespace,
    paths: ProjectPaths,
    *,
    dry_run: bool,
) -> tuple[Literal["extract", "judge", "merge", "apply", "report"], int | None]:
    from_step = getattr(args, "from_step", None)
    from_ralph = getattr(args, "from_ralph", None)

    if from_step is None and from_ralph is None:
        return "extract", None
    if dry_run:
        raise RuntimeError("--from-step/--from-ralph cannot be used with --dry-run")

    if from_step is not None:
        return str(from_step), None

    start_index = int(from_ralph)
    if start_index < 0:
        raise RuntimeError("--from-ralph must be >= 0")
    if start_index >= paths.ralph_count:
        raise RuntimeError(
            f"--from-ralph {start_index} is out of range for --ralph {paths.ralph_count}"
        )
    return "judge", start_index


def _ensure_project_prereqs(paths: ProjectPaths) -> None:
    if not paths.project_dir.exists() or not paths.project_dir.is_dir():
        raise FileNotFoundError(f"Project directory not found: {paths.project_dir}")
    if not paths.workflow_xml.exists():
        raise FileNotFoundError(f"Workflow XML not found: {paths.workflow_xml}")
    try:
        workflow_root = ET.fromstring(paths.workflow_xml.read_text(encoding="utf-8"))
    except ET.ParseError as exc:
        raise RuntimeError(f"Invalid workflow XML: {paths.workflow_xml} ({exc})") from exc
    if workflow_root.tag != "workflow":
        raise RuntimeError(f"Workflow root must be <workflow>: {paths.workflow_xml}")
    if str(workflow_root.attrib.get("name", "")).strip() != paths.workflow_xml.stem:
        raise RuntimeError(
            f"Workflow name mismatch in {paths.workflow_xml}: expected name='{paths.workflow_xml.stem}'"
        )
    if not paths.source_docx.exists():
        raise FileNotFoundError(f"Source DOCX not found: {paths.source_docx}")
    if not paths.constants.exists():
        raise FileNotFoundError(f"constants.json not found: {paths.constants}")

    required_dirs = [
        paths.extract_output_dir,
        paths.chunks_output_dir,
        paths.patch_output_dir,
        paths.apply_log.parent,
        paths.annotated_docx.parent,
        paths.changes_md.parent,
        paths.judged_chunk_results_dir,
        paths.chunk_results_dir,
    ]
    required_dirs.extend(paths.ralph_chunk_results_dirs)

    for path in required_dirs:
        path.mkdir(parents=True, exist_ok=True)


def _assert_outputs(paths: ProjectPaths) -> None:
    required = [
        paths.merged_patch,
        paths.merge_report,
        paths.final_patch,
        paths.apply_log,
        paths.annotated_docx,
        paths.changes_md,
        paths.changes_json,
        paths.changes_docx,
    ]
    missing = [path for path in required if not path.exists()]
    if missing:
        formatted = "\n".join(f"- {path}" for path in missing)
        raise RuntimeError(f"Pipeline finished with missing outputs:\n{formatted}")


def _resolve_action_index(action: dict[str, Any], base_ops: list[dict[str, Any]]) -> int | None:
    if "op_index" in action and isinstance(action.get("op_index"), int):
        index = int(action["op_index"])
        return index if 0 <= index < len(base_ops) else None

    op_id = str(action.get("op_id", "")).strip()
    if not op_id:
        return None
    for idx, op in enumerate(base_ops):
        if str(op.get("op_id", "")).strip() == op_id or str(op.get("dedup_key", "")).strip() == op_id:
            return idx
    return None


def _apply_merge_qa_overrides(paths: ProjectPaths, *, author: str, cli: str, model: str | None = None) -> dict[str, Any]:
    prompt = _render_template(
        TEMPLATE_MERGE_QA,
        {
            "MERGED_PATCH_PATH": str(paths.merged_patch),
            "MERGE_REPORT_PATH": str(paths.merge_report),
        },
    )
    _run_cli_exec(
        cli=cli,
        prompt=prompt,
        schema_path=SCHEMA_MERGE_QA,
        output_path=paths.merge_qa_report,
        phase="merge QA",
        model=model,
    )

    merge_qa = _load_json(paths.merge_qa_report)
    actions = merge_qa.get("actions", [])
    if not isinstance(actions, list):
        actions = []

    merged_patch = _load_json(paths.merged_patch)
    base_ops = merged_patch.get("ops", [])
    if not isinstance(base_ops, list):
        raise RuntimeError("merged_patch.json must contain ops list")

    dropped_indices: set[int] = set()
    downgraded: dict[int, str] = {}
    applied_actions: list[dict[str, Any]] = []
    ignored_actions: list[dict[str, Any]] = []

    for action in actions:
        if not isinstance(action, dict):
            ignored_actions.append({"reason": "action_not_object", "action": action})
            continue

        action_type = str(action.get("type", "")).strip()
        index = _resolve_action_index(action, base_ops)
        if index is None:
            ignored_actions.append({"reason": "unresolved_op_reference", "action": action})
            continue

        if action_type == "drop_op":
            dropped_indices.add(index)
            applied_actions.append({"type": "drop_op", "op_index": index})
            continue

        if action_type == "downgrade_to_comment":
            comment_text = str(action.get("comment_text", "")).strip()
            if not comment_text:
                ignored_actions.append({"reason": "missing_comment_text", "action": action})
                continue
            downgraded[index] = comment_text
            applied_actions.append({"type": "downgrade_to_comment", "op_index": index})
            continue

        ignored_actions.append({"reason": "unsupported_action_type", "action": action})

    final_ops: list[dict[str, Any]] = []
    for idx, op in enumerate(base_ops):
        if idx in dropped_indices:
            continue

        rewritten = copy.deepcopy(op)
        if idx in downgraded:
            rewritten = {
                "type": "add_comment",
                "target": rewritten.get("target", {}),
                "range": rewritten.get("range", {"start": 0, "end": 0}),
                "expected": rewritten.get("expected", {"snippet": ""}),
                "comment_text": downgraded[idx],
            }
        final_ops.append(rewritten)

    final_patch = {
        "schema_version": str(merged_patch.get("schema_version", "patch.v1")),
        "created_at": str(merged_patch.get("created_at", "")),
        "author": author,
        "ops": final_ops,
    }
    _dump_json(paths.final_patch, final_patch)

    override_report = {
        "actions_in": len(actions),
        "actions_applied": len(applied_actions),
        "actions_ignored": len(ignored_actions),
        "applied": applied_actions,
        "ignored": ignored_actions,
        "merged_ops": len(base_ops),
        "final_ops": len(final_ops),
    }
    _dump_json(paths.final_patch_overrides, override_report)
    return override_report


def run_pipeline(
    paths: ProjectPaths,
    *,
    author: str,
    dry_run: bool,
    validate: bool,
    max_concurrency: int,
    cli: str,
    start_stage: Literal["extract", "judge", "merge", "apply", "report"] = "extract",
    from_ralph: int | None = None,
    model: str | None = None,
) -> SyntheticChunkResult | None:
    input_rel_path = f"input/{paths.source_docx.name}"
    extract_rel_dir = _project_rel_path(paths.project_dir, paths.extract_output_dir)
    chunks_rel_dir = _project_rel_path(paths.project_dir, paths.chunks_output_dir)
    chunk_results_rel_dir = _project_rel_path(paths.project_dir, paths.chunk_results_dir)
    patch_rel_dir = _project_rel_path(paths.project_dir, paths.patch_output_dir)
    apply_rel_dir = _project_rel_path(paths.project_dir, paths.apply_log.parent)
    output_docx_rel = _project_rel_path(paths.project_dir, paths.annotated_docx)
    stage_order = {"extract": 0, "judge": 1, "merge": 2, "apply": 3, "report": 4}
    start_index = stage_order[start_stage]

    if start_index <= stage_order["extract"]:
        _run(
            [
                sys.executable,
                str(EXTRACT_SCRIPT),
                "--project-dir",
                str(paths.project_dir),
                "--input-docx",
                input_rel_path,
                "--output-dir",
                extract_rel_dir,
            ]
        )

        _run(
            [
                sys.executable,
                str(CHUNK_SCRIPT),
                "--project-dir",
                str(paths.project_dir),
                "--constants",
                str(paths.constants),
                "--review-units",
                f"{extract_rel_dir}/review_units.json",
                "--linear-units",
                f"{extract_rel_dir}/linear_units.json",
                "--docx-struct",
                f"{extract_rel_dir}/docx_struct.json",
                "--output-dir",
                chunks_rel_dir,
            ]
        )

    synthetic: SyntheticChunkResult | None = None
    if start_index <= stage_order["judge"]:
        if dry_run:
            if paths.ralph_count > 1:
                _log_line(
                    "Dry-run mode ignores additional ralph runs and writes a single synthetic result set "
                    f"to {paths.chunk_results_dir}"
                )
            _clear_chunk_result_artifacts(paths.chunk_results_dir)
            synthetic = _discover_synthetic_chunk_result(paths)
            _log_line(f"Synthetic chunk result: {synthetic.output_path} (ops={synthetic.op_count})")
        else:
            if from_ralph is None and start_stage == "extract":
                qa = _run_chunk_qa_with_optional_fix(paths, cli=cli, model=model)
                _log_line(f"Chunk QA status={qa['status']} passes={qa['passes']} applied_fixes={len(qa.get('applied_fixes', []))}")

            ralph_summaries: list[dict[str, Any]] = []
            if from_ralph is not None:
                ralph_indices = range(from_ralph, paths.ralph_count)
            elif start_stage == "extract":
                ralph_indices = range(paths.ralph_count)
            else:
                ralph_indices = range(0)

            for ralph_index in ralph_indices:
                review_summary = _run_single_ralph_review(
                    paths,
                    ralph_index=ralph_index,
                    max_concurrency=max_concurrency,
                    cli=cli,
                    model=model,
                )
                ralph_summaries.append(review_summary)
                _log_line(
                    f"Ralph run {ralph_index + 1}/{paths.ralph_count} complete: "
                    f"chunks={review_summary['chunk_count']} "
                    f"input_ops={review_summary['total_input_ops']} "
                    f"output_ops={review_summary['total_output_ops']}"
                )

            if paths.use_judge:
                judge_summary = _run_judge_phase(paths, cli=cli, model=model)
                _log_line(
                    "Judge phase complete: "
                    f"chunks={judge_summary['chunk_count']} "
                    f"input_ops={judge_summary['total_input_ops']} "
                    f"output_ops={judge_summary['total_output_ops']}"
                )
                _enforce_no_sanitized_chunk_ops(paths, judge_summary)
            else:
                _log_line("Judge phase skipped; merge will use ralph_0 results.")
                if ralph_summaries:
                    _enforce_no_sanitized_chunk_ops(paths, ralph_summaries[0])

    if start_index <= stage_order["merge"]:
        _run(
            [
                sys.executable,
                str(MERGE_SCRIPT),
                "--project-dir",
                str(paths.project_dir),
                "--chunk-results-dir",
                chunk_results_rel_dir,
                "--linear-units",
                f"{extract_rel_dir}/linear_units.json",
                "--chunks-manifest",
                f"{chunks_rel_dir}/manifest.json",
                "--review-units",
                f"{extract_rel_dir}/review_units.json",
                "--output-dir",
                patch_rel_dir,
                "--author",
                author,
            ]
        )

        if dry_run:
            _dump_json(paths.final_patch, _load_json(paths.merged_patch))
            _dump_json(
                paths.final_patch_overrides,
                {
                    "actions_in": 0,
                    "actions_applied": 0,
                    "actions_ignored": 0,
                    "applied": [],
                    "ignored": [],
                    "merged_ops": len(_load_json(paths.merged_patch).get("ops", [])),
                    "final_ops": len(_load_json(paths.final_patch).get("ops", [])),
                },
            )
        else:
            override_report = _apply_merge_qa_overrides(paths, author=author, cli=cli, model=model)
            _log_line(
                "Merge QA overrides: "
                f"actions_in={override_report['actions_in']} "
                f"applied={override_report['actions_applied']} "
                f"ignored={override_report['actions_ignored']}"
            )

    if start_index <= stage_order["apply"]:
        _run(
            [
                sys.executable,
                str(APPLY_SCRIPT),
                "--project-dir",
                str(paths.project_dir),
                "--input-docx",
                input_rel_path,
                "--patch",
                f"{patch_rel_dir}/final_patch.json",
                "--review-units",
                f"{extract_rel_dir}/review_units.json",
                "--output-docx",
                output_docx_rel,
                "--apply-log",
                f"{apply_rel_dir}/apply_log.json",
                "--author",
                author,
            ]
        )

    if start_index <= stage_order["report"]:
        _run(
            [
                sys.executable,
                str(REPORT_SCRIPT),
                "--project-dir",
                str(paths.project_dir),
                "--review-units",
                f"{extract_rel_dir}/review_units.json",
                "--patch",
                f"{patch_rel_dir}/final_patch.json",
                "--apply-log",
                f"{apply_rel_dir}/apply_log.json",
                "--output-md",
                f"output/{paths.input_name}_changes.md",
                "--output-json",
                f"output/{paths.input_name}_changes.json",
                "--output-docx",
                f"output/{paths.input_name}_changes.docx",
            ]
        )

    _assert_outputs(paths)

    if validate:
        _run([sys.executable, str(VALIDATE_SCRIPT), "--project-dir", str(paths.project_dir), "--input-name", paths.input_name])

    return synthetic


def main() -> int:
    args = _build_parser().parse_args()
    log_started = False

    try:
        paths = _resolve_paths(args)
        resume_start_stage, resume_from_ralph = _resolve_resume_start(
            args,
            paths,
            dry_run=bool(args.dry_run),
        )
        _init_run_log(paths.project_dir / "artifacts" / f"last_run_{paths.input_name}.txt")
        log_started = True
        _ensure_project_prereqs(paths)

        is_resume = resume_start_stage != "extract" or resume_from_ralph is not None
        if not is_resume:
            for stale in [
                paths.chunk_qa_report,
                paths.merge_qa_report,
                paths.final_patch,
                paths.final_patch_overrides,
            ]:
                if stale.exists():
                    stale.unlink()

            for chunk_results_dir in [*paths.ralph_chunk_results_dirs, paths.judged_chunk_results_dir]:
                _clear_chunk_result_artifacts(chunk_results_dir)
        else:
            if resume_from_ralph is not None:
                _log_line(f"Resume mode: from Ralph run {resume_from_ralph}/{paths.ralph_count - 1}")
            else:
                _log_line(f"Resume mode: from step '{resume_start_stage}'")

        synthetic = run_pipeline(
            paths,
            author=str(args.author),
            dry_run=bool(args.dry_run),
            validate=not bool(args.skip_validation),
            max_concurrency=int(args.max_concurrency),
            cli=str(args.cli),
            start_stage=resume_start_stage,
            from_ralph=resume_from_ralph,
            model=args.model,
        )

        _log_line("Project run completed successfully.")
        _log_line(f"Project: {paths.project_dir}")
        _log_line(f"Input file: {paths.source_docx.name}")
        _log_line(f"Workflow: {paths.workflow_xml.name}")
        _log_line(f"Ralph runs: {paths.ralph_count}")
        _log_line(f"Judge enabled: {paths.use_judge}")
        _log_line(f"Chunk results used for merge: {paths.chunk_results_dir}")
        _log_line(f"Final patch: {paths.final_patch}")
        _log_line(f"Annotated DOCX: {paths.annotated_docx}")
        _log_line(f"Change report: {paths.changes_md}")
        _log_line(f"Change report DOCX: {paths.changes_docx}")
        if synthetic is not None:
            _log_line(f"Dry-run synthetic chunk result: {synthetic.output_path}")

        return 0
    except KeyboardInterrupt:
        if log_started:
            _log_line("Run interrupted by user (SIGINT).", stderr=True)
        return 130
    except subprocess.CalledProcessError as exc:
        _log_line(f"Command failed with exit code {exc.returncode}: {exc.cmd}", stderr=True)
        return exc.returncode
    except Exception as exc:
        _log_line(f"Project run failed: {exc}", stderr=True)
        return 1
    finally:
        _close_run_log()


if __name__ == "__main__":
    raise SystemExit(main())
