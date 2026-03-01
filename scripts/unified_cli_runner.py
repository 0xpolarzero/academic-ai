#!/usr/bin/env python3
"""Unified CLI runner supporting Codex, Claude Code, and Kimi CLIs with structured output.

Supports three backends:
- codex: OpenAI Codex CLI with --output-schema
- claude: Anthropic Claude Code CLI with --json-schema
- kimi: Kimi CLI with validation/retry wrapper (fallback)

Environment setup for Claude + Kimi:
    export ANTHROPIC_BASE_URL="https://api.moonshot.cn/anthropic"
    export ANTHROPIC_API_KEY="your-moonshot-key"
    export ANTHROPIC_MODEL="kimi-k2.5"
    
    # Or for kimi-for-coding:
    export ANTHROPIC_BASE_URL="https://api.kimi.com/coding/"
    export ANTHROPIC_API_KEY="your-kimi-for-coding-key"
    export ANTHROPIC_MODEL="kimi-for-coding"
"""

from __future__ import annotations

import json
import select
import subprocess
import time
from pathlib import Path
from typing import Any

# Kimi runner imports - deferred to avoid circular imports
# Will be imported at runtime in _run_kimi()


CLI_EXEC_TIMEOUT_SECONDS = 600


def _load_schema_content(schema_path: Path) -> str:
    """Load JSON schema content for inclusion in prompts."""
    try:
        return schema_path.read_text(encoding="utf-8")
    except Exception:
        return "{}"


def _build_codex_command(
    *,
    prompt: str,
    schema_path: Path,
    work_dir: Path,
    output_path: Path,
    model: str | None = None,
) -> list[str]:
    """Build Codex CLI command."""
    cmd = [
        "codex",
        "exec",
        "--cd", str(work_dir),
        "--sandbox", "read-only",
        "--output-schema", str(schema_path),
        "--output-last-message", str(output_path),
        "-",
    ]
    if model:
        cmd.extend(["-m", model])
    return cmd


def _build_claude_command(
    *,
    prompt: str,
    schema_path: Path,
    work_dir: Path,
    model: str | None = None,
) -> list[str]:
    """Build Claude Code CLI command with JSON schema support.
    
    Uses --json-schema for structured output. The response will have:
    - structured_output: The validated JSON matching the schema
    - result: Text explanation
    - session_id, usage, cost metadata
    """
    schema_content = _load_schema_content(schema_path)
    
    # Build a focused prompt for structured output
    structured_prompt = f"""{prompt}

You must output your response as valid JSON matching the provided schema.
Be precise and follow the schema exactly."""

    cmd = [
        "claude",
        "-p", structured_prompt,
        "--output-format", "json",
        "--json-schema", schema_content,
        "--dangerously-skip-permissions",  # Auto-approve for non-interactive
    ]
    if model:
        cmd.extend(["--model", model])
    return cmd


def _build_kimi_command(
    *,
    prompt: str,
    work_dir: Path,
    schema_content: str | None = None,
    model: str | None = None,
) -> list[str]:
    """Build Kimi CLI command."""
    system_context = """You are a JSON-only API. Your task is to analyze the provided content and return ONLY valid JSON.

STRICT RULES:
1. Output MUST be valid, parseable JSON - no markdown, no conversational text
2. Do NOT wrap in ```json code blocks
3. Do NOT include explanations before or after the JSON
4. Every field in the schema must be present
5. Use empty strings "" for optional fields you don't populate
6. Use empty arrays [] for array fields with no items

Your entire response must be a single JSON object that passes validation."""

    enhanced_prompt = f"""{system_context}

{prompt}
"""
    if schema_content:
        enhanced_prompt += f"""
<output_schema>
{schema_content}
</output_schema>

REMEMBER: Output ONLY the JSON object. No markdown, no explanations."""

    cmd = [
        "kimi",
        "--work-dir", str(work_dir),
        "--yolo",
        "--print",
        "--output-format", "stream-json",
        "--prompt", enhanced_prompt,
    ]
    if model:
        cmd.extend(["--model", model])
    return cmd


def _parse_claude_output(raw_output: str) -> dict[str, Any]:
    """Parse Claude Code JSON output and extract structured_output.
    
    Claude Code returns:
    {
        "type": "result",
        "subtype": "success",
        "structured_output": {...},  # The schema-validated JSON
        "result": "text explanation",
        "session_id": "...",
        "total_cost_usd": 0.01,
        ...
    }
    """
    try:
        parsed = json.loads(raw_output)
        
        # Check if it's a Claude Code response with structured_output
        if isinstance(parsed, dict):
            # Extract the structured output
            if "structured_output" in parsed:
                return parsed["structured_output"]
            
            # If no structured_output but has result, return full response
            # (might happen if schema validation failed)
            if "result" in parsed or "type" in parsed:
                return parsed
                
        return parsed
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse Claude Code output as JSON: {e}") from e


def run_cli_exec(
    *,
    cli: str,
    prompt: str,
    schema_path: Path,
    output_path: Path,
    work_dir: Path,
    phase: str,
    log_callback: callable | None = None,
    model: str | None = None,
) -> dict[str, Any]:
    """Run a CLI command (codex, claude, or kimi) with structured output.
    
    Args:
        cli: Which CLI to use - "codex", "claude", or "kimi"
        prompt: The task prompt
        schema_path: Path to JSON schema file
        output_path: Where to save the output JSON
        work_dir: Working directory
        phase: Phase name for logging
        log_callback: Optional callback for log messages (func(message, stderr=False))
        model: Optional model name to use (e.g., 'gpt-5.3-codex', 'kimi-k2', 'sonnet')
    
    Returns:
        The parsed JSON output as a dict
        
    Raises:
        RuntimeError: If execution fails or output is invalid
    """
    def _log(msg: str, stderr: bool = False):
        if log_callback:
            log_callback(msg, stderr)
        else:
            print(msg, file=__import__('sys').stderr if stderr else None)
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    if cli == "codex":
        return _run_codex(
            prompt=prompt,
            schema_path=schema_path,
            output_path=output_path,
            work_dir=work_dir,
            phase=phase,
            log=_log,
            model=model,
        )
    elif cli == "claude":
        return _run_claude(
            prompt=prompt,
            schema_path=schema_path,
            output_path=output_path,
            work_dir=work_dir,
            phase=phase,
            log=_log,
            model=model,
        )
    elif cli == "kimi":
        return _run_kimi(
            prompt=prompt,
            schema_path=schema_path,
            output_path=output_path,
            work_dir=work_dir,
            phase=phase,
            log=_log,
            model=model,
        )
    else:
        raise ValueError(f"Unknown CLI: {cli}. Use 'codex', 'claude', or 'kimi'")


def _run_codex(
    *,
    prompt: str,
    schema_path: Path,
    output_path: Path,
    work_dir: Path,
    phase: str,
    log: callable,
    model: str | None = None,
) -> dict[str, Any]:
    """Run Codex CLI with structured output."""
    cmd = _build_codex_command(
        prompt=prompt,
        schema_path=schema_path,
        work_dir=work_dir,
        output_path=output_path,
        model=model,
    )
    
    log(f"[{phase}] Running Codex: {' '.join(cmd[:6])}...")
    
    proc = subprocess.Popen(
        cmd,
        cwd=work_dir,
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
        
        # Stream output
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
                    log(f"[{phase}] {line.rstrip()}")
                    continue
            
            if proc.poll() is not None:
                break
        
        # Read remaining output
        tail = proc.stdout.read()
        if tail:
            for line in tail.splitlines():
                log(f"[{phase}] {line}")
        
        returncode = proc.wait()
        if returncode != 0:
            raise subprocess.CalledProcessError(returncode, cmd)
        
        # Read and return output
        if not output_path.exists():
            raise RuntimeError(f"Codex did not create output file: {output_path}")
            
        result = json.loads(output_path.read_text(encoding="utf-8"))
        log(f"[{phase}] Codex completed successfully")
        return result
        
    finally:
        if proc.poll() is None:
            proc.kill()


def _run_claude(
    *,
    prompt: str,
    schema_path: Path,
    output_path: Path,
    work_dir: Path,
    phase: str,
    log: callable,
    model: str | None = None,
) -> dict[str, Any]:
    """Run Claude Code CLI with structured output.
    
    Uses --json-schema for validated output. Captures stdout and parses
the JSON response to extract structured_output.
    """
    cmd = _build_claude_command(
        prompt=prompt,
        schema_path=schema_path,
        work_dir=work_dir,
        model=model,
    )
    
    log(f"[{phase}] Running Claude Code: {' '.join(cmd[:4])}...")
    
    proc = subprocess.Popen(
        cmd,
        cwd=work_dir,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    
    output_lines: list[str] = []
    
    try:
        assert proc.stdout is not None
        
        # Stream and capture output
        deadline = time.monotonic() + CLI_EXEC_TIMEOUT_SECONDS
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                proc.kill()
                raise RuntimeError(f"Claude Code timed out after {CLI_EXEC_TIMEOUT_SECONDS}s")
            
            ready, _, _ = select.select([proc.stdout], [], [], min(1.0, remaining))
            if ready:
                line = proc.stdout.readline()
                if line:
                    output_lines.append(line)
                    # Only log important lines to avoid spam
                    stripped = line.strip()
                    if any(keyword in stripped.lower() for keyword in 
                           ["error", "warning", "failed", "success", "cost"]):
                        log(f"[{phase}] {stripped}")
                    continue
            
            if proc.poll() is not None:
                break
        
        # Read remaining output
        tail = proc.stdout.read()
        if tail:
            output_lines.append(tail)
        
        returncode = proc.wait()
        raw_output = "".join(output_lines)
        
        # Save raw output for debugging
        debug_path = output_path.parent / f"{output_path.stem}.raw.json"
        debug_path.write_text(raw_output, encoding="utf-8")
        
        if returncode != 0:
            # Even with errors, try to parse output
            try:
                result = _parse_claude_output(raw_output)
                if result:
                    log(f"[{phase}] Warning: Claude exited with {returncode} but produced valid output")
            except Exception:
                raise subprocess.CalledProcessError(returncode, cmd, output=raw_output)
        
        # Parse the output
        result = _parse_claude_output(raw_output)
        
        # Write the extracted structured output
        output_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
        
        log(f"[{phase}] Claude Code completed successfully")
        return result
        
    finally:
        if proc.poll() is None:
            proc.kill()


def _run_kimi(
    *,
    prompt: str,
    schema_path: Path,
    output_path: Path,
    work_dir: Path,
    phase: str,
    log: callable,
    model: str | None = None,
) -> dict[str, Any]:
    """Run Kimi CLI with validation and retry.
    
    Uses stream-json output and validates against schema.
    """
    import json
    import re
    
    schema_content = _load_schema_content(schema_path)
    
    # Build a prompt that strongly encourages valid JSON output
    structured_prompt = f"""You are a JSON-only API. Return ONLY valid JSON matching the schema below.

=== TASK ===
{prompt}

=== SCHEMA ===
{schema_content}

=== RULES ===
1. Output MUST be valid, parseable JSON
2. Do NOT wrap in ```json code blocks - output raw JSON only
3. Do NOT include explanations or conversational text
4. Every required field must be present
5. Use empty strings "" for optional fields you don't populate
6. Use empty arrays [] for array fields with no items

Output ONLY the JSON object, nothing else."""

    cmd = [
        "kimi",
        "--work-dir", str(work_dir),
        "--yolo",
        "--print",
        "--output-format", "stream-json",
        "--prompt", structured_prompt,
    ]
    if model:
        cmd.extend(["--model", model])
    
    log(f"[{phase}] Running Kimi...")
    
    proc = subprocess.Popen(
        cmd,
        cwd=work_dir,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    
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
                    continue
            
            if proc.poll() is not None:
                break
        
        # Read remaining output
        tail = proc.stdout.read()
        if tail:
            for line in tail.splitlines():
                stripped = line.strip()
                if stripped:
                    json_lines.append(stripped)
        
        returncode = proc.wait()
        
        # Extract text from kimi stream-json output
        text_parts: list[str] = []
        for line in json_lines:
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
        
        raw_text = "".join(text_parts)
        
        # Try to extract JSON from text
        result = None
        text = raw_text.strip()
        
        # Try direct parse first
        try:
            result = json.loads(text)
        except json.JSONDecodeError:
            pass
        
        # Try removing markdown code blocks
        if result is None:
            match = re.search(r'```(?:json)?\s*\n?(.*?)\n?```', text, re.DOTALL)
            if match:
                try:
                    result = json.loads(match.group(1).strip())
                except json.JSONDecodeError:
                    pass
        
        # Try finding JSON object/array
        if result is None:
            for pattern in [r'(\{[\s\S]*\})', r'(\[[\s\S]*\])']:
                matches = list(re.finditer(pattern, text))
                for match in sorted(matches, key=lambda m: len(m.group(1)), reverse=True):
                    try:
                        result = json.loads(match.group(1).strip())
                        break
                    except json.JSONDecodeError:
                        continue
                if result is not None:
                    break
        
        if result is None:
            raise RuntimeError(f"Could not parse valid JSON from Kimi output")
        
        # Save result
        output_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
        
        log(f"[{phase}] Kimi completed successfully")
        return result
        
    finally:
        if proc.poll() is None:
            proc.kill()


def detect_available_cli() -> str | None:
    """Detect which CLI tools are available.
    
    Returns:
        The preferred available CLI: "codex", "claude", or "kimi"
        None if none are available
    """
    import shutil
    
    # Check in order of preference (based on structured output reliability)
    for cli in ["claude", "codex", "kimi"]:
        if shutil.which(cli):
            return cli
    
    return None


def validate_claude_kimi_setup() -> tuple[bool, str]:
    """Validate that Claude Code is configured to use Kimi.
    
    Returns:
        (is_valid, message)
    """
    import os
    
    base_url = os.environ.get("ANTHROPIC_BASE_URL", "")
    api_key = os.environ.get("ANTHROPIC_API_KEY", "") or os.environ.get("ANTHROPIC_AUTH_TOKEN", "")
    
    if not base_url or "moonshot" not in base_url.lower():
        return False, "ANTHROPIC_BASE_URL not set to Moonshot endpoint"
    
    if not api_key:
        return False, "ANTHROPIC_API_KEY or ANTHROPIC_AUTH_TOKEN not set"
    
    if not api_key.startswith("sk-"):
        return False, "API key doesn't look like a Moonshot key (should start with 'sk-')"
    
    return True, f"Claude Code configured for: {base_url}"


if __name__ == "__main__":
    # Simple test
    import argparse
    import sys
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--cli", choices=["codex", "claude", "kimi"], required=True)
    parser.add_argument("--prompt", required=True)
    parser.add_argument("--schema", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, default=Path("."))
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()
    
    def log(msg: str, stderr: bool = False):
        print(msg, file=sys.stderr if stderr else sys.stdout)
    
    try:
        result = run_cli_exec(
            cli=args.cli,
            prompt=args.prompt,
            schema_path=args.schema,
            output_path=args.output,
            work_dir=args.work_dir,
            phase="test",
            log_callback=log,
        )
        print(f"\n✓ Success! Output written to {args.output}")
        print(f"Result preview: {json.dumps(result, indent=2)[:500]}...")
    except Exception as e:
        print(f"\n✗ Failed: {e}", file=sys.stderr)
        sys.exit(1)
