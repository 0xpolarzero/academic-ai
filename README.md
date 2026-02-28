# academic-ai

DOCX review pipeline with deterministic artifacts and writer-facing outputs.

## Project Contract (Phase 0)

All pipeline skills are being standardized to a single required CLI flag:

- `--project-dir projects/<project_slug>`

Path resolution contract:

- `input/source.docx` is resolved under `--project-dir`
- all intermediate files are resolved under `<project-dir>/artifacts/...`
- final files are resolved under `<project-dir>/output/...`
- workflow files are resolved under `<project-dir>/workflows/...`

Planned default structure:

```text
projects/<project_slug>/
  input/source.docx
  workflows/<workflow_name>.xml
  artifacts/
  output/
```

Agent contracts (used by `scripts/run_project.py`):

- prompt templates in `templates/`:
  - `chunk_qa.xml`
  - `chunk_review.xml`
  - `merge_qa.xml`
- JSON output schemas in `schemas/`:
  - `chunk_qa.schema.json`
  - `chunk_result.schema.json`
  - `merge_qa.schema.json`

Deterministic chunk-boundary fixes (runner behavior):

- source of truth is `artifacts/chunks/manifest.json` chunk order
- `merge_adjacent(left,right)` merges adjacent chunks into left, removes right
- `shift_boundary(left,right,move_primary_units)` shifts primary ownership:
  - positive: move right -> left
  - negative: move left -> right
- fixes are applied in chunk index order, then chunks are regenerated and reindexed

## Path Rules

- Final writer-facing outputs live in `projects/<project>/output/` only:
  - `annotated.docx`
  - `changes.md`
  - `changes.json`
- Intermediate/runtime artifacts live in `projects/<project>/artifacts/` only.
- Source input DOCX is `projects/<project>/input/source.docx`.
- Workflow policy is `projects/<project>/workflows/<workflow>.xml`.

## Prerequisites

- Python 3.10+ (`.venv/bin/python` is used automatically when present, otherwise `python3`).
- `pytest` installed in the selected Python environment for `make test`.
- `curl` or `wget` for `make fixtures` (manual fallback is documented in `fixtures/README.md`).

## Setup

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip pytest
```

## Runbook

1. Create or refresh a project scaffold:

```bash
make project PROJECT=thesis
```

2. Put your source DOCX at:

```text
projects/thesis/input/source.docx
```

3. Run one command:

```bash
make run PROJECT=thesis WORKFLOW=fr_copyedit_conservative
```

4. Read final outputs:

```text
projects/thesis/output/annotated.docx
projects/thesis/output/changes.md
projects/thesis/output/changes.json
```

5. Offline CI/e2e smoke (no Codex calls):

```bash
make e2e
```

6. Unit tests:

```bash
make test
```

## Runner Command

```bash
.venv/bin/python scripts/run_project.py \
  --project thesis \
  --workflow fr_copyedit_conservative
```

Use `--dry-run` to avoid CLI calls and generate synthetic chunk review outputs:

```bash
.venv/bin/python scripts/run_project.py \
  --project thesis \
  --workflow fr_copyedit_conservative \
  --dry-run
```

Use `--cli` to select the CLI provider (`codex` or `kimi`, default is `codex`):

```bash
.venv/bin/python scripts/run_project.py \
  --project thesis \
  --workflow fr_copyedit_conservative \
  --cli kimi
```

Or via make:

```bash
make run PROJECT=thesis WORKFLOW=fr_copyedit_conservative CLI=kimi
```

## Troubleshooting

- `Workflow XML not found`: create or verify `projects/<project>/workflows/<workflow>.xml`.
- `Workflow name mismatch`: ensure workflow root has `<workflow name="<workflow>">`.
- `Source DOCX not found`: place file at `projects/<project>/input/source.docx`.
- `codex CLI was not found on PATH`: install/configure Codex CLI or run with `--dry-run`.
- `kimi CLI was not found on PATH`: install Kimi CLI (https://moonshotai.github.io/kimi-cli/) or run with `--dry-run`.
- `Chunk QA still failing after deterministic fixes`: inspect `projects/<project>/artifacts/chunks/chunk_qa_report.json`.
- `Missing output files`: inspect `projects/<project>/artifacts/patch/merge_report.json` and `projects/<project>/artifacts/apply/apply_log.json`.

## Notes

- `make clean` removes generated files under root `artifacts/` and `output/` (legacy targets).
- Project runs write intermediates under `projects/<project>/artifacts/`.
