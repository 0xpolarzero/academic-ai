# academic-ai

AI-assisted DOCX review pipeline for long documents.

It takes a `.docx`, extracts deterministic text units, runs chunk-based review agents, merges suggestions into a single patch, applies tracked changes/comments to DOCX, and produces writer-facing change reports.

## Experimental Status

This repository is experimental. Do not trust generated `*_annotated.docx` files blindly.
Always review the output manually in Word before accepting changes. Bugs can still produce
unintended document mutations beyond strict comment/suggestion behavior.

## What You Get

For each input file `<input_name>.docx`:

- `projects/<project>/output/<input_name>_annotated.docx`
- `projects/<project>/output/<input_name>_changes.md`
- `projects/<project>/output/<input_name>_changes.json`
- `projects/<project>/output/<input_name>_changes.docx`

Intermediates/logs are written under `projects/<project>/artifacts/`.

## Quick Start

1. Create environment.

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip pytest python-docx
```

Already included in this repo:

- `projects/thesis/`
- `projects/thesis/workflows/fr_copyedit_conservative.xml`
- `projects/thesis/workflows/fr_copyedit_micro.xml`

You can run immediately by adding a DOCX to `projects/thesis/input/`.

2. (Optional) Scaffold a new project (only if you want another project besides `thesis`).

```bash
make project PROJECT=my_project
```

3. Put one or more `.docx` files in:

```text
projects/thesis/input/
```

Built-in starter workflows:

- `fr_copyedit_conservative`
- `fr_copyedit_micro`

4. Run a first dry run (no AI calls):

```bash
make run PROJECT=thesis WORKFLOW=fr_copyedit_conservative DRY_RUN=1
```

5. Configure AI CLI, then run real review:

```bash
python scripts/setup_cli_env.py --check
make run PROJECT=thesis WORKFLOW=fr_copyedit_conservative INPUT=chapter1.docx CLI=codex
```

## What A Workflow Run Does

Each `make run` execution:

1. Extracts the DOCX into structured review units.
2. Splits units into chunk files (`primary_units` editable, context read-only).
3. Runs AI review per chunk using your selected workflow XML policy.
4. Merges and safety-checks all suggested operations.
5. Applies operations to DOCX as tracked changes/comments.
6. Generates markdown/json/docx change reports.

## Run Modes

Use one of these:

```bash
# 1) Safety smoke run (no AI calls)
make run PROJECT=thesis WORKFLOW=fr_copyedit_conservative DRY_RUN=1

# 2) Normal real run (recommended default)
make run PROJECT=thesis WORKFLOW=fr_copyedit_conservative INPUT=chapter1.docx

# 3) Higher-confidence run (ensemble + judge)
make run PROJECT=thesis WORKFLOW=fr_copyedit_conservative INPUT=chapter1.docx RALPH=3
```

Resume if a run was interrupted:

```bash
make resume PROJECT=thesis WORKFLOW=fr_copyedit_conservative FROM=merge INPUT=chapter1.docx
make resume PROJECT=thesis WORKFLOW=fr_copyedit_conservative FROM_RALPH=1 RALPH=3 INPUT=chapter1.docx
```

Other useful commands:

```bash
make fixtures   # download public fixture
make e2e        # offline smoke workflow
make test       # unit/integration tests
```

## Runner CLI

Equivalent direct command:

```bash
.venv/bin/python scripts/run_project.py \
  --project thesis \
  --workflow fr_copyedit_conservative \
  --input chapter1.docx \
  --cli codex
```

Useful flags:

- `--dry-run`: generates synthetic chunk results (no AI calls)
- `--ralph N`: run `N` sequential review passes
- `--from-step judge|merge|apply|report`: resume from stage
- `--from-ralph N`: resume ensemble from Ralph run index `N`
- `--model <name>`: pass model to selected CLI backend
- `--skip-judge`: advanced/debug flag for `--ralph > 1` runs

## CLI Backends

`run_project.py` supports:

- `--cli codex` (default)
- `--cli claude`
- `--cli kimi`

Use `scripts/setup_cli_env.py` for setup checks and instructions:

```bash
python scripts/setup_cli_env.py --check
python scripts/setup_cli_env.py --setup-codex
python scripts/setup_cli_env.py --setup-claude
python scripts/setup_cli_env.py --setup-kimi
```

## Project Layout

```text
projects/<project>/
  input/*.docx
  workflows/*.xml
  artifacts/
    docx_extract/<input_name>/
    chunks/<input_name>/
    ralph_0/chunk_results/<input_name>/
    ...
    judged/chunk_results/<input_name>/
    patch/<input_name>/
    apply/<input_name>/
  output/
    <input_name>_annotated.docx
    <input_name>_changes.md
    <input_name>_changes.json
    <input_name>_changes.docx
```

## Troubleshooting

- Multiple files in `input/` with no `INPUT=...` or `--input`: choose one explicitly.
- Workflow file must exist at `projects/<project>/workflows/<workflow>.xml` and `<workflow name="...">` must match filename.
- Missing CLI binary/API setup: run `python scripts/setup_cli_env.py --check`.
- Missing expected outputs: inspect `projects/<project>/artifacts/patch/<input_name>/merge_report.json` and `projects/<project>/artifacts/apply/<input_name>/apply_log.json`.
