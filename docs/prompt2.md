# IMPLEMENTATION SPEC (finish-line, opinionated, minimal, academic-grade)

You are an AI software engineer working inside an existing repository that already implements the **generic DOCX (OOXML) pipeline** skills:

- docx_extract_ooxml_to_artifacts
- docx_chunk_atomic_manifest
- docx_search_in_extraction
- docx_merge_dedup_validate_patch
- docx_apply_patch_to_output
- docx_change_report_before_after

Your job now is to take the repo from “generic pipeline exists” to “opinionated, project-based, one-command workflow”, with a **script orchestrator** that runs code steps directly and calls Codex subagents itself.

You MUST:
- split work into phases
- spawn subagents per phase (Architect, Implementer, Reviewer, QA)
- implement incrementally, with tests and explicit acceptance criteria between phases
- keep it SIMPLE: no new config bloat (no extra per-language/glossary config, no throttles)
- commit after each phase (clean commits, runnable repo at each phase)

---

## DESIGN GOAL (user experience)

1. Put a `.docx` into a project folder
2. Choose a workflow (XML prompt)
3. Run one command
4. Get:
   - `projects/<project>/output/annotated.docx` (ONLY track changes suggestions + real Word comments; no silent edits)
   - `projects/<project>/output/changes.md`
   - `projects/<project>/output/changes.json`

Everything else must live under `projects/<project>/artifacts/` (gitignored).

---

## PROJECTS DIRECTORY (opinionated stuff lives here)

Add:

```

projects/
<project_slug>/
input/
source.docx              # gitignored
workflows/
<workflow_name>.xml      # committed (opinionated policy)
artifacts/                 # gitignored (extraction, chunks, results, patch, logs)
output/                    # gitignored recommended (annotated.docx + changes.* only)
PROJECT.md                 # optional notes (committed)

```

### Gitignore
Add:
- `projects/**/input/`
- `projects/**/artifacts/`
- `projects/**/output/`

---

## DELETE OLD PATH ROUTING (IMPORTANT REFACTOR)

The existing skills likely assume repo-root paths like:
- `input/`
- `artifacts/`
- `output/`

These must be removed from the workflow.

**Rewrite every skill script (and SKILL.md instructions) to support a project-root flag** so all paths are computed under:

- input:     `projects/<project>/input/source.docx`
- artifacts: `projects/<project>/artifacts/...`
- output:    `projects/<project>/output/...`

### New CLI convention (apply consistently across all skill scripts)
Each skill script MUST accept:

- `--project-dir <path>` (required)
  - Example: `--project-dir projects/thesis`

No shared python package imports between skills: keep helpers inside each skill’s `scripts/` directory.

---

## ONE FILE = ONE WORKFLOW (no extra layers)

A **workflow** is a single XML file, authored by the user, selected at runtime.

Create the first project called `thesis` with two workflow files:

1) `projects/thesis/workflows/fr_copyedit_conservative.xml` (copyedit + strict meaning preservation; no rephrasing unless truly obscure)

2) `projects/thesis/workflows/fr_copyedit_micro.xml` (micro-improvements only; intended as a second pass; may rephrase but only micro-clarity improvements; MUST NOT redo copyedit tasks)

(Use the exact XML content already defined in this repo/spec.)

---

## RUNTIME ORCHESTRATION (SCRIPT-LED, AGENTS CALLED BY SCRIPT)

We no longer use a “main AI orchestrator” to run the pipeline.
Instead, implement a deterministic **runner script** that:

- runs all code steps directly (python scripts)
- calls Codex subagents only where needed:
  1) chunk QA/verification (and optional auto-fix)
  2) chunk review (one agent per chunk, with concurrency)
  3) final merge/QA agent after deterministic merge to sanity-check conflicts/resolution (optional but required by this spec)

### Runner interface
Implement:

- `python scripts/run_project.py --project <project_slug> --workflow <workflow_name>`
  - Example: `python scripts/run_project.py --project thesis --workflow fr_copyedit_conservative`

Keep it minimal. It should infer all paths from `projects/<project>/...`.

Recommended optional flags (minimal + practical):
- `--max-concurrency N` (default 4)
- `--dry-run` (no Codex calls; generate synthetic chunk_results so CI can run e2e without a Codex subscription)

### Runner behavior (exact step order)

1) Validate project structure exists:
   - `projects/<project>/input/source.docx` must exist
   - workflow must exist at `projects/<project>/workflows/<workflow>.xml`

2) Extraction (code):
   - run the existing extraction skill script with `--project-dir`

3) Chunking (code):
   - run the existing chunk skill script with `--project-dir`

4) Chunk QA agent (Codex exec):
   - call a Codex exec run that reads the chunk manifest + a sample of chunk files
   - agent outputs structured JSON:
     - status: `ok` | `needs_fix`
     - if `needs_fix`: a minimal set of boundary edits (see “Chunk QA contract” below)

   If `needs_fix`:
   - apply the fixes deterministically (code), regenerate chunks/manifest, then re-run QA once.
   - if still failing, stop and write a QA report to `projects/<project>/artifacts/chunks/chunk_qa_report.json`

5) Chunk review agents (Codex exec, concurrent):
   - for each `chunk_XXXX.json`, call Codex exec with a prompt that:
     - points to the chunk file path
     - points to the workflow xml path
     - enforces: only edit primary_units
   - save each result to:
     - `projects/<project>/artifacts/chunk_results/chunk_XXXX_result.json`

   After each chunk result:
   - validate JSON shape
   - validate all ops target only the chunk’s `primary_units`
     - if violations exist: remove/convert illegal ops to comments targeting allowed primary units, and log this sanitization.

6) Merge (code + final agent):
   - run deterministic merge skill script to produce:
     - `projects/<project>/artifacts/patch/merged_patch.json`
     - `projects/<project>/artifacts/patch/merge_report.json`
   - then run a “final merge QA” Codex agent that reads merged_patch + merge_report and outputs structured JSON of safe override actions (optional downgrades only).
   - apply overrides deterministically, producing final patch:
     - `projects/<project>/artifacts/patch/final_patch.json`

7) Apply patch (code):
   - apply final patch to produce:
     - `projects/<project>/output/annotated.docx`
     - `projects/<project>/artifacts/apply/apply_log.json`

8) Report (code):
   - create:
     - `projects/<project>/output/changes.md`
     - `projects/<project>/output/changes.json`

9) Final checks (code):
   - annotated.docx is a valid zip
   - if edits exist: document.xml contains `w:ins` and/or `w:del`
   - if comments exist: `word/comments.xml` exists and range markers exist
   - output files exist

---

## RELIABILITY UPGRADE (KEEP THIS)

Chunk agents MUST NOT be required to compute character offsets.

Update merge skill to allow chunk_results ops where `range` may be missing.
Merge must resolve offsets deterministically using:
- `expected.snippet`
- the unit’s `accepted_text` from extraction

Rules:
- if snippet not found: skip edit op (log), optionally convert to add_comment (“target not found”)
- if snippet ambiguous (multiple matches): downgrade to comment (log); never guess
- once resolved, apply per-unit ops end→start (descending start offsets) to avoid shifting bugs

Patch schema v1 stays unchanged in the final patch artifacts; merge fills missing ranges.

---

## AGENT PROMPTS & SCHEMAS (USED BY THE RUNNER)

Add these repo-level files:

```

templates/
chunk_qa.xml
chunk_review.xml
merge_qa.xml
schemas/
chunk_qa.schema.json
chunk_result.schema.json
merge_qa.schema.json

````

### Chunk QA contract (must be minimal + machine-actionable)
Agent output JSON:
- `status`: "ok" | "needs_fix"
- `issues`: list of strings
- `fixes`: list of boundary edits (empty if ok)

Boundary edit types (support exactly these, keep it simple):
- `merge_adjacent`: { "left_chunk_id": "...", "right_chunk_id": "..." }
- `shift_boundary`: { "left_chunk_id": "...", "right_chunk_id": "...", "move_primary_units": <int> }
  - positive moves units from right→left; negative moves left→right

The runner applies fixes deterministically and regenerates chunk files + manifest.

### Chunk review contract
Agent output JSON:
- `chunk_id`
- `ops`: list of ops (same patch-op types as your patch schema)
  - ops may omit `range`
  - MUST include `expected.snippet` for edits/comments
  - MUST target only primary_units
- `suggestions`: optional free-text notes

### Merge QA contract
Agent output JSON:
- `actions`: list of safe overrides (only downgrades/removals)
  - `drop_op`: { op_id or op_index }
  - `downgrade_to_comment`: { op_id or op_index, comment_text }
- `notes`: optional

Runner applies overrides deterministically.

---

## CODEX EXEC INVOCATION (runner must use this pattern)

The runner must call Codex via `codex exec` in non-interactive mode, using:
- `--cd/-C` to set workspace root
- `--sandbox/-s` (read-only is enough; do not grant more)
- `--output-schema <schema.json>`
- `--output-last-message/-o <output.json>`
- prompt from stdin by passing `-` as PROMPT

Example (single run):

```bash
PROMPT_FILE=/tmp/prompt.txt
OUT_JSON=projects/thesis/artifacts/chunk_results/chunk_0001_result.json
SCHEMA=schemas/chunk_result.schema.json

cat "$PROMPT_FILE" | codex exec \
  --cd . \
  --sandbox read-only \
  --output-schema "$SCHEMA" \
  --output-last-message "$OUT_JSON" \
  -
````

The runner should generate prompt text in-memory (no need for temp files) and pipe it to stdin.

---

## MAKEFILE TARGETS (updated for projects)

Update Makefile so everything is runnable with:

* `make project PROJECT=thesis`

  * creates: `projects/thesis/{input,workflows,artifacts,output}`
  * (workflows are committed; do not overwrite if they exist)

* `make run PROJECT=thesis WORKFLOW=fr_copyedit_conservative`

  * calls runner script

* `make test`

  * runs unit tests (no Codex calls)

* `make e2e`

  * runs runner in `--dry-run` mode so CI can validate end-to-end outputs exist without Codex

Also keep existing fixture targets, but adapt scripts/tests to use `--project-dir`.

---

## PHASED IMPLEMENTATION PLAN (MUST USE SUBAGENTS)

### Phase 0 — Architecture & contracts (Subagents: Architect + Reviewer)

Deliverables:

* Decide exact `--project-dir` behavior across skills
* Define schemas for chunk_qa, chunk_result, merge_qa
* Define how chunk boundary fixes are applied deterministically
  Acceptance criteria:
* `schemas/` and `templates/` planned and stubbed
* doc in README about new project-based paths
  Commit: `phase0: project-dir refactor plan + agent contracts`

### Phase 1 — Repo structure + thesis project (Subagents: Repo Implementer + Repo Reviewer)

Deliverables:

* `projects/` skeleton
* `projects/thesis/workflows/*.xml` (both workflows)
* `.gitignore` updated
* Make targets `project`, `run` stubbed
  Acceptance criteria:
* `make project PROJECT=thesis` works
* workflow files exist at expected paths
  Commit: `phase1: add projects/ structure + thesis workflows`

### Phase 2 — Refactor all skills to `--project-dir` (Subagents: Refactor Implementer + Skill Reviewer + QA)

Deliverables:

* update every skill script to accept `--project-dir` and write under project artifacts/output
* update each SKILL.md I/O paths accordingly
* update tests to create temp project dirs and pass `--project-dir`
  Acceptance criteria:
* `make test` passes (offline)
* extraction/chunk/apply/report smoke tests work against a temp project dir
  Commit: `phase2: refactor skills for project-dir`

### Phase 3 — Runner script + dry-run mode (Subagents: Runner Implementer + Runner Reviewer + QA)

Deliverables:

* `scripts/run_project.py` implementing the full step sequence
* `--dry-run` mode that generates synthetic chunk_results (so merge/apply/report can run)
  Acceptance criteria:
* `python scripts/run_project.py --project thesis --workflow fr_copyedit_conservative --dry-run` produces annotated.docx + changes.*
* `make e2e` passes offline
  Commit: `phase3: add script runner (dry-run supported)`

### Phase 4 — Codex integration (Subagents: Integration Implementer + Prompt Reviewer + QA)

Deliverables:

* implement Codex exec calls for chunk QA, chunk reviews (concurrency), merge QA
* implement deterministic application of chunk boundary fixes
* implement strict validation/sanitization of chunk_results ops
  Acceptance criteria:
* with Codex available, a real run creates chunk_results and completes pipeline
* runner logs failures clearly to `projects/<project>/artifacts/...`
  Commit: `phase4: codex exec subagents for qa/review/merge`

### Phase 5 — Final docs + acceptance (Subagents: Documentation + Integration QA)

Deliverables:

* README runbook with exact commands
* troubleshooting section
  Acceptance criteria (final):
* one-command run works
* annotated.docx contains tracked changes + comments only
* no silent edits
* stable change reports exist
  Commit: `phase5: runbook + final QA`

---

## FINAL ACCEPTANCE CRITERIA

* Works per-project under `projects/<project>/...`
* No repo-root input/artifacts/output are used
* Runner is deterministic for code steps; only review/QA uses Codex agents
* Chunk QA agent exists and can fix simple boundary issues deterministically
* Chunk review agents run per chunk with concurrency and strict output validation
* Merge resolves missing ranges from expected.snippet against accepted_text (no guessing)
* Apply produces valid OOXML with track changes and real comments
* Report outputs stable locations (no page numbers)
* `make test` and `make e2e` succeed offline (`--dry-run`)

```

References for Codex CLI command/flags and stdin prompting (verified against current docs and latest CLI release notes):
- Codex CLI command line options (flags incl. `--cd/-C`, `--sandbox/-s`, `--output-schema`, `--output-last-message/-o`, `--skip-git-repo-check`, and `PROMPT` accepting `-` to read stdin). :contentReference[oaicite:0]{index=0}  
- Non-interactive mode semantics for `codex exec` (final message to stdout, `-o/--output-last-message`, JSON/schema usage). :contentReference[oaicite:1]{index=1}  
- Latest Codex CLI release shown as 0.106.0 (Feb 26, 2026). :contentReference[oaicite:2]{index=2}
