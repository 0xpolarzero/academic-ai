# CLI Migration Guide: Unified Codex/Claude/Kimi Support

## Overview

The workflow runner supports three CLI backends:

1. **codex** (default) - OpenAI Codex CLI with native structured output
2. **claude** - Anthropic Claude Code CLI for Anthropic models
3. **kimi** - Kimi CLI with validation and retry logic

## How It Works

### Codex (Default)
```bash
python3 scripts/run_project.py --project <name> --workflow <name>
# or explicitly:
python3 scripts/run_project.py --project <name> --workflow <name> --cli codex
```
- Uses OpenAI models (GPT-4o, etc.)
- Native `--output-schema` support (most reliable)
- Requires `OPENAI_API_KEY`

### Claude Code
```bash
python3 scripts/run_project.py --project <name> --workflow <name> --cli claude
```
- Uses Anthropic models (Claude 3.5/4 Sonnet, etc.)
- Native `--json-schema` support for structured output
- Can work with Anthropic-compatible APIs
- Requires `ANTHROPIC_API_KEY`

### Kimi CLI
```bash
python3 scripts/run_project.py --project <name> --workflow <name> --cli kimi
```
- Uses Kimi models (kimi-k2.5, kimi-for-coding, etc.)
- **Validation and strict error handling** - fails fast on invalid ops
- Runs kimi-cli directly with schema validation
- No external LLM dependencies for fixing JSON

## Operation Types

The system uses **three operation types**:

| Type | Purpose | Required Fields |
|------|---------|-----------------|
| `replace_range` | Replace text (or delete if replacement="") | `replacement`, `new_text=""`, `comment_text=""` |
| `insert_at` | Insert text at position | `new_text`, `replacement=""`, `comment_text=""` |
| `add_comment` | Add comment without changing text | `comment_text`, `replacement=""`, `new_text=""` |

### To Delete Text
Use `replace_range` with `replacement=""` (empty string).

Example:
```json
{
  "type": "replace_range",
  "target": {...},
  "range": {"start": 10, "end": 20},
  "expected": {"snippet": "text to delete"},
  "replacement": "",
  "new_text": "",
  "comment_text": ""
}
```

## Configuration

### Codex (Default)
```bash
export OPENAI_API_KEY="sk-your-openai-key"
```

### Claude Code
```bash
# For Anthropic models
export ANTHROPIC_API_KEY="sk-ant-your-key"

# Or configure for compatible APIs in ~/.claude/settings.json:
{
  "env": {
    "ANTHROPIC_BASE_URL": "https://api.example.com/anthropic",
    "ANTHROPIC_API_KEY": "your-key"
  }
}
```

### Kimi CLI
```bash
# Login once
kimi login

# Or set API key directly
export KIMI_API_KEY="your-key"
```

## Checking Your Setup

```bash
# Check all CLIs and environment
python3 scripts/setup_cli_env.py --check

# Get setup instructions
python3 scripts/setup_cli_env.py --setup-kimi
python3 scripts/setup_cli_env.py --setup-claude
python3 scripts/setup_cli_env.py --setup-codex
```

## Comparison

| Feature | Codex | Claude | Kimi |
|---------|-------|--------|------|
| **Flag** | `--cli codex` (default) | `--cli claude` | `--cli kimi` |
| **Models** | OpenAI (GPT-4o) | Anthropic (Claude) | Moonshot (Kimi K2.5) |
| **Structured Output** | Native `--output-schema` | Native `--json-schema` | Validation + strict errors |
| **External Dependencies** | None | None | None |
| **Error Handling** | Schema validation | Schema validation | Fail-fast on invalid ops |
| **Best For** | Reliability | Anthropic users | Kimi users |

## Error Handling

### Kimi Mode
- Validates JSON immediately
- Fails fast with clear error messages
- No automatic conversion of invalid operations
- Strict schema enforcement

Example errors:
```
RuntimeError: replace_range op missing 'replacement' field at index 2
RuntimeError: insert_at op has empty 'new_text' at index 0
RuntimeError: Invalid op type 'delete_range' at index 1
```

## Migration from Old Implementation

### If you were using `--cli kimi` before

The new kimi implementation:
- ✅ **No longer uses Codex Spark** for JSON fixing
- ✅ **Local validation** is faster and cheaper
- ✅ **Fail-fast behavior** - errors are caught immediately
- ✅ **Stricter validation** - invalid ops cause immediate failure
- ✅ Same CLI: `--cli kimi`

### Key Changes

| Aspect | Old | New |
|--------|-----|-----|
| Deletions | `delete_range` type | `replace_range` with `replacement=""` |
| Empty replacement | Auto-converted to delete_range | Valid for deletions |
| Invalid ops | Converted with warning | Immediate error |
| Error handling | Lenient | Strict/fail-fast |

### Updated Prompt

The chunk review prompt now clearly states:
```
If type=replace_range: replacement is the text to insert. Set to empty string "" to delete the range.
To delete text: use type=replace_range with replacement="" (empty string).
```

## Summary

| Approach | CLI Flag | Pros | Cons |
|----------|----------|------|------|
| **Claude + Kimi** | `--cli claude` | Best structured output, 1 call, schema validation | Requires Claude Code install |
| **Kimi direct** | `--cli kimi` | Native Kimi, no extra tools, strict validation | Fails on invalid ops |
| **OpenAI Codex** | `--cli codex` (default) | Original, well-tested | Uses OpenAI models, not Kimi |

**Recommendation:** Use `--cli kimi` for strict validation and immediate feedback on errors. The system will fail fast if the AI generates invalid operations, helping you catch issues early.
