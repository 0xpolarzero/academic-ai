# AGENTS.md

Technical onboarding for agents working in this repo.

## Mission

`academic-ai` is a project-oriented DOCX review pipeline:

1. Extract OOXML paragraphs into deterministic JSON units.
2. Chunk units into editable `primary_units` + read-only context.
3. Run AI chunk review(s) under a workflow policy XML.
4. Merge/dedup/conflict-resolve ops into a final patch.
5. Apply patch to DOCX as tracked changes/comments.
6. Generate writer-facing change reports.

## Ground Truth Entry Points

- Main orchestrator: `scripts/run_project.py`
- Make targets: `Makefile`
- CLI backend wrapper: `scripts/unified_cli_runner.py`
- Validation gate: `scripts/validate_dry_run_outputs.py`

Pipeline skills (real processing logic):

- Extract: `.codex/skills/docx_extract_ooxml_to_artifacts/scripts/extract_docx.py`
- Chunk: `.codex/skills/docx_chunk_atomic_manifest/scripts/chunk_docx.py`
- Merge: `.codex/skills/docx_merge_dedup_validate_patch/scripts/merge_patch.py`
- Apply: `.codex/skills/docx_apply_patch_to_output/scripts/apply_docx_patch.py`
- Report: `.codex/skills/docx_change_report_before_after/scripts/change_report.py`
- Search helper: `.codex/skills/docx_search_in_extraction/scripts/search_extracted.py`

Prompt templates + schemas used by runner AI phases:

- Templates: `templates/chunk_qa.xml`, `templates/chunk_review.xml`, `templates/ralph_judge.xml`, `templates/merge_qa.xml`
- Schemas: `schemas/chunk_qa.schema.json`, `schemas/chunk_result.schema.json`, `schemas/merge_qa.schema.json`

## Project Layout Contract

All runtime artifacts are project-scoped:

- Inputs: `projects/<project>/input/*.docx`
- Workflow policy: `projects/<project>/workflows/<workflow>.xml`
- Intermediates/logs: `projects/<project>/artifacts/...`
- Final outputs: `projects/<project>/output/...`

`run_project.py` resolves one `input_name` per run (file stem) and writes per-input subtrees:

- `artifacts/docx_extract/<input_name>/`
- `artifacts/chunks/<input_name>/`
- `artifacts/ralph_i/chunk_results/<input_name>/`
- `artifacts/judged/chunk_results/<input_name>/`
- `artifacts/patch/<input_name>/`
- `artifacts/apply/<input_name>/`
- `output/<input_name>_annotated.docx`
- `output/<input_name>_changes.{md,json,docx}`

## Runner Behavior (`scripts/run_project.py`)

Execution order:

1. Extract
2. Chunk
3. Chunk QA (with deterministic boundary fixes + second QA pass)
4. Ralph review runs (`--ralph N`) and optional judge
5. Merge (`merged_patch.json` + `merge_report.json`)
6. Merge QA overrides -> `final_patch.json`
7. Apply patch -> annotated DOCX + apply log
8. Report generation -> md/json/docx change reports
9. Optional validation (`scripts/validate_dry_run_outputs.py`)

Resume modes:

- `--from-step judge|merge|apply|report`
- `--from-ralph N` (0-based)

Ensemble semantics:

- `--ralph 1`: merge from `ralph_0` only (no judge)
- `--ralph > 1`: judge enabled unless `--skip-judge`

## Critical Data Contracts

Identity and offsets:

- Target identity uses `part + para_id + unit_uid`.
- Offsets are UTF-16 code units.
- Range semantics: `start` inclusive, `end` exclusive.

Chunk contract:

- Only `primary_units` are editable.
- `context_units_before/after` are read-only and ownership-enforced later.

Runner chunk result sanitization:

- Allowed op types in runner output: `add_comment`, `replace_range`, `insert_at`.
- Targets are forced to chunk `primary_units` using `unit_uid` lookup.
- Runner derives ranges from `quoted_text` `[[target]]` when present; falls back to provided `range`.
- Invalid/unsafe ops are dropped and logged in `*_sanitization.json`.

Merge contract (`_merge_lib.py`):

- Accepts op types: `add_comment`, `replace_range`, `insert_at`, `delete_range`.
- Enforces chunk target ownership via `chunks/manifest.json`.
- Dedups by normalized semantic key.
- Detects edit conflicts at paragraph level and downgrades to comments.
- Resolves missing ranges from `expected.snippet` + `review_units.json`.
  - ambiguous snippet -> downgrade to comment
  - snippet not found -> op rejected
- Final op ordering is deterministic and safe for application.

Apply contract (`_apply_lib.py`):

- Applies edits as Word tracked changes (`w:ins`, `w:del`, `w:delText`).
- Applies comments via real OOXML comment parts/relationships.
- Edit ops skip on snippet mismatch.
- Comment ops can still apply on mismatch with `location_uncertain` warning.
- Paragraphs with unsupported structures (e.g. complex fields) are skipped, never force-mutated.

Report contract (`_report_lib.py`):

- Emits only ops that were `applied` per apply log.
- Includes stable location (`heading_path`, `part`, `para_id`, `unit_uid`).
- Adds disambiguation metadata when before-snippet appears multiple times.
- Merges adjacent replacement changes for cleaner writer output.

## CLI/Command Surface

Primary commands:

- `make project PROJECT=<slug>`
- `make run PROJECT=<slug> WORKFLOW=<name> [INPUT=file.docx] [DRY_RUN=1] [RALPH=N] [SKIP_JUDGE=1] [CLI=codex|claude|kimi] [MODEL=...]`
- `make resume PROJECT=<slug> WORKFLOW=<name> FROM=<step>|FROM_RALPH=N [INPUT=...] [RALPH=N] [CLI=...]`
- `make e2e`
- `make test`

Direct runner:

- `python scripts/run_project.py --project <slug> --workflow <name> [--input file.docx ...]`

## Commit Rules

- Use Conventional Commits for every commit message.
- Format: `<type>(<optional-scope>): <short summary>`
- Common types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`.
- Keep subject line imperative and concise.
- Do not use emojis in commit subjects or bodies.

## What To Edit For Common Tasks

- Change orchestration/resume/phase rules: `scripts/run_project.py`
- Change CLI backend behavior: `scripts/unified_cli_runner.py`
- Change extraction/chunk/merge/apply/report core logic: corresponding `._lib.py` under `.codex/skills/.../scripts/`
- Change AI instructions: `templates/*.xml`
- Change structured outputs: `schemas/*.json`
- Change workflow editorial policy: `projects/<project>/workflows/*.xml`

## Testing Notes

- Tests live in `tests/` and cover extraction, chunking, merge, apply, report, search, and runner behaviors.
- Run with `.venv/bin/python -m pytest` (or `make test` when `pytest` is installed in selected Python).
- `make e2e` runs an offline dry-run flow through the project runner.
- If runner tests fail, first inspect `tests/test_run_project_codex_retry.py` and `tests/test_run_project_sanitization.py` for expectation drift against current `run_project.py` behavior.
