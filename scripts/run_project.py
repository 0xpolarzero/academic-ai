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
import xml.etree.ElementTree as ET
from typing import Any
from typing import TextIO

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
EDIT_OP_TYPES = {"replace_range", "insert_at", "delete_range"}

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
    constants: Path
    extract_output_dir: Path
    chunks_output_dir: Path
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
    parser.add_argument("--constants", type=Path, default=Path("config/constants.json"), help="Path to constants JSON")
    parser.add_argument("--author", default="phase4-runner", help="Author value used in merge/apply artifacts")
    parser.add_argument("--max-concurrency", type=int, default=4, help="Max concurrent chunk reviewers")
    parser.add_argument("--dry-run", action="store_true", help="Generate synthetic chunk results instead of model outputs")
    parser.add_argument("--skip-validation", action="store_true", help="Skip QA acceptance checks at the end")
    parser.add_argument("--cli", default="codex", choices=("codex", "claude", "kimi"), help="CLI provider to use for AI calls: codex, claude (Claude Code), or kimi (default: codex)")
    return parser


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _dump_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


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


def _run_cli_exec(*, cli: str, prompt: str, schema_path: Path, output_path: Path, phase: str) -> None:
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
        )
    else:
        # Default codex (original behavior)
        _run_codex(
            prompt=prompt,
            schema_path=schema_path,
            output_path=output_path,
            phase=phase,
        )


def _run_codex(*, prompt: str, schema_path: Path, output_path: Path, phase: str) -> None:
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


def _run_kimi_with_retry(*, prompt: str, schema_path: Path, output_path: Path, phase: str, max_retries: int = 2) -> None:
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


def _run_chunk_qa_with_optional_fix(paths: ProjectPaths, *, cli: str) -> dict[str, Any]:
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

    offsets = _utf16_offsets(accepted_text)

    for indices in itertools.combinations(range(len(unique_matches)), 4):
        comment_match = unique_matches[indices[0]]
        replace_match = unique_matches[indices[1]]
        insert_match = unique_matches[indices[2]]
        delete_match = unique_matches[indices[3]]

        comment_span = (offsets[comment_match.start()], offsets[comment_match.end()])
        replace_span = (offsets[replace_match.start()], offsets[replace_match.end()])
        delete_span = (offsets[delete_match.start()], offsets[delete_match.end()])
        insert_pos = offsets[insert_match.end()]

        if _ranges_overlap(replace_span[0], replace_span[1], delete_span[0], delete_span[1]):
            continue
        if _ranges_overlap(insert_pos, insert_pos, replace_span[0], replace_span[1]):
            continue
        if _ranges_overlap(insert_pos, insert_pos, delete_span[0], delete_span[1]):
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
                "range": {"start": replace_span[0], "end": replace_span[1]},
                "expected": {"snippet": replace_before},
                "replacement": replacement,
                "new_text": "",
                "comment_text": "",
            },
            {
                "type": "insert_at",
                "target": target,
                "range": {"start": insert_pos, "end": insert_pos},
                "expected": {"snippet": ""},
                "new_text": " [DRY-RUN]",
                "replacement": "",
                "comment_text": "",
            },
            {
                "type": "replace_range",
                "target": target,
                "range": {"start": delete_span[0], "end": delete_span[1]},
                "expected": {"snippet": delete_match.group(0)},
                "replacement": "",
                "new_text": "",
                "comment_text": "",
            },
            {
                "type": "add_comment",
                "target": target,
                "range": {"start": comment_span[0], "end": comment_span[1]},
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

    for idx, raw_op in enumerate(raw_ops):
        if not isinstance(raw_op, dict):
            raise RuntimeError(f"Operation at index {idx} is not a dict: {type(raw_op)}")

        op_type = str(raw_op.get("type", "")).strip()
        if op_type not in VALID_OP_TYPES:
            raise RuntimeError(f"Invalid op type '{op_type}' at index {idx}. Valid types: {VALID_OP_TYPES}")

        # ALWAYS resolve target using unit_uid from the AI's output
        # but use the chunk's ground truth for part/para_id/unit_uid
        target = raw_op.get("target")
        key = None
        if isinstance(target, dict):
            unit_uid = str(target.get("unit_uid", "")).strip()
            if unit_uid in unit_uid_to_target:
                key = unit_uid_to_target[unit_uid]  # Use chunk's ground truth

        if key is None:
            raise RuntimeError(f"Operation at index {idx} targets non-primary unit: {target}")

        normalized_target = {"part": key[0], "para_id": key[1], "unit_uid": key[2]}
        expected = raw_op.get("expected")
        snippet = ""
        if isinstance(expected, dict):
            snippet = str(expected.get("snippet", ""))

        normalized_range = _normalize_range(raw_op.get("range"))
        if normalized_range is None and op_type == "add_comment":
            normalized_range = {"start": 0, "end": 0}

        sanitized: dict[str, Any] = {
            "type": op_type,
            "target": normalized_target,
            "expected": {"snippet": snippet},
        }
        if normalized_range is not None:
            sanitized["range"] = normalized_range

        if op_type == "replace_range":
            if "replacement" not in raw_op:
                raise RuntimeError(f"replace_range op missing 'replacement' field at index {idx}")
            replacement = str(raw_op.get("replacement", ""))
            # Empty replacement is valid - means delete the range
            sanitized["replacement"] = replacement
        elif op_type == "insert_at":
            if normalized_range is not None and normalized_range["start"] != normalized_range["end"]:
                raise RuntimeError(f"insert_at op at index {idx} has non-collapsed range: {normalized_range}")
            if "new_text" not in raw_op:
                raise RuntimeError(f"insert_at op at index {idx} missing 'new_text' field")
            new_text = str(raw_op.get("new_text", ""))
            if new_text == "":
                raise RuntimeError(f"insert_at op at index {idx} has empty 'new_text'")
            sanitized["new_text"] = new_text
        elif op_type == "add_comment":
            comment_text = str(raw_op.get("comment_text", "")).strip()
            if not comment_text:
                raise RuntimeError(f"add_comment op missing 'comment_text' at index {idx}")
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
    }
    return sanitized_payload, log_payload


def _run_chunk_reviews(paths: ProjectPaths, *, max_concurrency: int, cli: str) -> dict[str, Any]:
    manifest_path = paths.chunks_output_dir / "manifest.json"
    manifest = _load_json(manifest_path)
    chunk_entries = manifest.get("chunks", []) if isinstance(manifest, dict) else []
    if not isinstance(chunk_entries, list) or not chunk_entries:
        raise RuntimeError("Chunk manifest has no chunks for review")

    for stale in paths.chunk_results_dir.glob("chunk_*_result.json"):
        stale.unlink()
    for stale in paths.chunk_results_dir.glob("chunk_*_sanitization.json"):
        stale.unlink()
    for stale in paths.chunk_results_dir.glob("chunk_*_result.raw.json"):
        stale.unlink()

    def process_chunk(entry: dict[str, Any]) -> dict[str, Any]:
        chunk_id = str(entry.get("chunk_id", "")).strip()
        rel_path = str(entry.get("path", "")).strip()
        if not chunk_id or not rel_path:
            raise RuntimeError("Manifest chunk entry is missing chunk_id/path")

        chunk_path = paths.chunks_output_dir / rel_path
        if not chunk_path.exists():
            raise FileNotFoundError(f"Chunk file missing: {chunk_path}")

        output_path = paths.chunk_results_dir / f"{chunk_id}_result.json"
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
            phase=f"chunk review {chunk_id}",
        )

        raw_payload = _load_json(output_path)
        _dump_json(paths.chunk_results_dir / f"{chunk_id}_result.raw.json", raw_payload)
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
        _dump_json(paths.chunk_results_dir / f"{chunk_id}_sanitization.json", log_payload)

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
    _dump_json(paths.chunk_result_sanitization_log, aggregate)
    return aggregate


def _enforce_no_sanitized_chunk_ops(paths: ProjectPaths, review_summary: dict[str, Any]) -> None:
    # No longer needed - validation happens immediately during sanitization
    pass


def _resolve_paths(args: argparse.Namespace) -> ProjectPaths:
    project_dir = (REPO_ROOT / "projects" / str(args.project)).resolve()
    workflow_xml = (project_dir / "workflows" / f"{args.workflow}.xml").resolve()

    return ProjectPaths(
        project_dir=project_dir,
        workflow_xml=workflow_xml,
        source_docx=(project_dir / "input" / "source.docx").resolve(),
        constants=(Path(args.constants).expanduser().resolve() if Path(args.constants).is_absolute() else (REPO_ROOT / Path(args.constants)).resolve()),
        extract_output_dir=(project_dir / "artifacts" / "docx_extract").resolve(),
        chunks_output_dir=(project_dir / "artifacts" / "chunks").resolve(),
        chunk_results_dir=(project_dir / "artifacts" / "chunk_results").resolve(),
        patch_output_dir=(project_dir / "artifacts" / "patch").resolve(),
        merged_patch=(project_dir / "artifacts" / "patch" / "merged_patch.json").resolve(),
        merge_report=(project_dir / "artifacts" / "patch" / "merge_report.json").resolve(),
        final_patch=(project_dir / "artifacts" / "patch" / "final_patch.json").resolve(),
        chunk_qa_report=(project_dir / "artifacts" / "chunks" / "chunk_qa_report.json").resolve(),
        merge_qa_report=(project_dir / "artifacts" / "patch" / "merge_qa_report.json").resolve(),
        final_patch_overrides=(project_dir / "artifacts" / "patch" / "final_patch_overrides.json").resolve(),
        chunk_result_sanitization_log=(project_dir / "artifacts" / "chunk_results" / "sanitization_report.json").resolve(),
        apply_log=(project_dir / "artifacts" / "apply" / "apply_log.json").resolve(),
        annotated_docx=(project_dir / "output" / "annotated.docx").resolve(),
        changes_md=(project_dir / "output" / "changes.md").resolve(),
        changes_json=(project_dir / "output" / "changes.json").resolve(),
    )


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

    for path in [
        paths.extract_output_dir,
        paths.chunks_output_dir,
        paths.chunk_results_dir,
        paths.patch_output_dir,
        paths.apply_log.parent,
        paths.annotated_docx.parent,
        paths.changes_md.parent,
    ]:
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


def _apply_merge_qa_overrides(paths: ProjectPaths, *, author: str, cli: str) -> dict[str, Any]:
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
) -> SyntheticChunkResult | None:
    _run(
        [
            sys.executable,
            str(EXTRACT_SCRIPT),
            "--project-dir",
            str(paths.project_dir),
            "--input-docx",
            "input/source.docx",
            "--output-dir",
            "artifacts/docx_extract",
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
            "artifacts/docx_extract/review_units.json",
            "--linear-units",
            "artifacts/docx_extract/linear_units.json",
            "--docx-struct",
            "artifacts/docx_extract/docx_struct.json",
            "--output-dir",
            "artifacts/chunks",
        ]
    )

    synthetic: SyntheticChunkResult | None = None
    if dry_run:
        synthetic = _discover_synthetic_chunk_result(paths)
        _log_line(f"Synthetic chunk result: {synthetic.output_path} (ops={synthetic.op_count})")
    else:
        qa = _run_chunk_qa_with_optional_fix(paths, cli=cli)
        _log_line(f"Chunk QA status={qa['status']} passes={qa['passes']} applied_fixes={len(qa.get('applied_fixes', []))}")
        review_summary = _run_chunk_reviews(paths, max_concurrency=max_concurrency, cli=cli)
        _log_line(
            "Chunk reviews complete: "
            f"chunks={review_summary['chunk_count']} "
            f"input_ops={review_summary['total_input_ops']} "
            f"output_ops={review_summary['total_output_ops']}"
        )
        _enforce_no_sanitized_chunk_ops(paths, review_summary)

    _run(
        [
            sys.executable,
            str(MERGE_SCRIPT),
            "--project-dir",
            str(paths.project_dir),
            "--chunk-results-dir",
            "artifacts/chunk_results",
            "--linear-units",
            "artifacts/docx_extract/linear_units.json",
            "--chunks-manifest",
            "artifacts/chunks/manifest.json",
            "--review-units",
            "artifacts/docx_extract/review_units.json",
            "--output-dir",
            "artifacts/patch",
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
        override_report = _apply_merge_qa_overrides(paths, author=author, cli=cli)
        _log_line(
            "Merge QA overrides: "
            f"actions_in={override_report['actions_in']} "
            f"applied={override_report['actions_applied']} "
            f"ignored={override_report['actions_ignored']}"
        )

    _run(
        [
            sys.executable,
            str(APPLY_SCRIPT),
            "--project-dir",
            str(paths.project_dir),
            "--input-docx",
            "input/source.docx",
            "--patch",
            "artifacts/patch/final_patch.json",
            "--review-units",
            "artifacts/docx_extract/review_units.json",
            "--output-docx",
            "output/annotated.docx",
            "--apply-log",
            "artifacts/apply/apply_log.json",
            "--author",
            author,
        ]
    )

    _run(
        [
            sys.executable,
            str(REPORT_SCRIPT),
            "--project-dir",
            str(paths.project_dir),
            "--review-units",
            "artifacts/docx_extract/review_units.json",
            "--patch",
            "artifacts/patch/final_patch.json",
            "--apply-log",
            "artifacts/apply/apply_log.json",
            "--output-md",
            "output/changes.md",
            "--output-json",
            "output/changes.json",
        ]
    )

    _assert_outputs(paths)

    if validate:
        _run([sys.executable, str(VALIDATE_SCRIPT), "--project-dir", str(paths.project_dir)])

    return synthetic


def main() -> int:
    args = _build_parser().parse_args()
    log_started = False

    try:
        paths = _resolve_paths(args)
        _init_run_log(paths.project_dir / "artifacts" / "last_run.txt")
        log_started = True
        _ensure_project_prereqs(paths)

        for stale in [
            paths.chunk_qa_report,
            paths.merge_qa_report,
            paths.final_patch,
            paths.final_patch_overrides,
            paths.chunk_result_sanitization_log,
        ]:
            if stale.exists():
                stale.unlink()

        if args.dry_run:
            for stale in paths.chunk_results_dir.glob("chunk_*_result.json"):
                stale.unlink()

        synthetic = run_pipeline(
            paths,
            author=str(args.author),
            dry_run=bool(args.dry_run),
            validate=not bool(args.skip_validation),
            max_concurrency=int(args.max_concurrency),
            cli=str(args.cli),
        )

        _log_line("Project run completed successfully.")
        _log_line(f"Project: {paths.project_dir}")
        _log_line(f"Workflow: {paths.workflow_xml.name}")
        _log_line(f"Final patch: {paths.final_patch}")
        _log_line(f"Annotated DOCX: {paths.annotated_docx}")
        _log_line(f"Change report: {paths.changes_md}")
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
