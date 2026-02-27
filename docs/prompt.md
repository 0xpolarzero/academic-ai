You are an AI software engineer building a repository for reviewing very long .docx files with Codex CLI multi-agent runs.

You MUST:
- split the work into phases
- spawn subagents per phase (research, implementation, review, QA)
- research and verify anything you’re not sure about (Codex skill format, OOXML track-changes markup, Word comments OOXML, relationships/content types)
- write high-quality code with thorough tests and end-to-end smoke runs

If you are uncertain about a detail, do NOT guess: either (1) research it, (2) prototype it, or (3) ask the user a clarifying question.

──────────────────────────────────────────────────────────────────────────────
PROJECT GOAL
Build a high-fidelity DOCX (OOXML) pipeline:

extract → chunk → review via subagents → merge → apply patch → produce outputs

Outputs for the WRITER (only these go in output/):
1) output/annotated.docx
   - edits must appear ONLY as track-changes suggestions (Word revisions w:ins/w:del)
   - comments must be real Word comments (comments.xml + comment range markers)
   - NEVER silently apply edits (no direct edits)
2) output/changes.md + output/changes.json
   - “before → after” report with stable locations (NOT page numbers)

Intermediates (everything else) go in artifacts/ and are gitignored.

The actual input doc the writer wants reviewed will be placed in input/ (gitignored).

──────────────────────────────────────────────────────────────────────────────
IMPORTANT CONSTRAINTS
- Never apply silent edits. Any text change must be represented as track changes.
- Comments must anchor to the smallest relevant span possible (avoid whole paragraphs when not necessary).
- Generic library: do NOT bake in “what to look for” (no proofreading rubric). The chunk agents’ review policy is out-of-scope here.
- Codex CLI supports subagents. Use them during implementation and also assume they’ll be used at runtime for chunk reviews.
- No shared top-level Python module/package. Keep scripts inside each skill. If a skill needs helpers, keep them inside that skill’s scripts/ directory (e.g., scripts/_lib.py).
- Use a config/constants.json (or .yaml) that scripts read (NOT a shared Python import) for token budgets, paths, etc.

──────────────────────────────────────────────────────────────────────────────
REPO DIRECTORY REQUIREMENTS
- .codex/skills/                       # skills live here
- input/                               # user places their source docx here (gitignored)
- output/                              # final writer-facing outputs only (optionally gitignored)
- artifacts/                            # all intermediate files (MUST be gitignored)
- fixtures/                            # test fixtures (see below)
- Makefile (or justfile) + scripts for testing

Add clear .gitignore entries at least for:
- artifacts/
- input/
- output/ (recommended)
(Do not ignore fixtures/ if you want fixtures committed; if fixture is large, prefer download script + ignore the actual .docx.)

──────────────────────────────────────────────────────────────────────────────
PHASED IMPLEMENTATION PLAN (YOU MUST USE SUBAGENTS)
You must implement progressively; do not attempt everything in one pass.

Phase 0 — Repo scaffold + decisions (Subagent: “Architect”)
- Create repo skeleton: folders, .gitignore, README outline, Makefile outline.
- Decide and document:
  - patch schema v1
  - chunk schema v1
  - stable paragraph/unit identification scheme (para_id, unit_uid)
  - how offsets map to “accepted_text”
- Output:
  - initial README with runbook placeholders
  - config/constants.json
  - schemas/ (optional but recommended) OR runtime validators (must exist somewhere)

Phase 1 — Research: Codex skill format + OOXML (Subagent: “OOXML Researcher”)
- Verify Codex skill format and where skills are discovered from (project-local).
- Research and summarize:
  - how track changes are represented in OOXML (w:ins / w:del / w:delText, author/date attrs, IDs)
  - how Word comments are stored (comments.xml, relationships, commentRangeStart/End, commentReference)
  - any required content-type/relationship updates when adding comments part
- Output:
  - docs/NOTES_OOXML.md (short, actionable, with references/links)
  - a tiny prototype script in a scratch area (or within a skill) that inserts one w:ins and one comment into a tiny docx and verifies Word opens it

Phase 2 — Implement extraction skill (Subagent: “Extractor Implementer”, plus “Extractor Reviewer”)
- Implement .codex/skills/docx_extract_ooxml_to_artifacts/
  - SKILL.md
  - scripts/extract_docx.py (+ helpers inside scripts/)
- Requirements:
  - Parse DOCX as zip; extract and enumerate:
    - main body (word/document.xml)
    - headers/footers if present (word/header*.xml, word/footer*.xml)
    - footnotes/endnotes if present (word/footnotes.xml, word/endnotes.xml)
    - paragraphs inside tables too
  - Produce artifacts/docx_extract/review_units.json with one “unit” per paragraph-like entity:
    - part (body/header/footer/footnotes/endnotes + specific part name)
    - para_id (stable, deterministic)
    - unit_uid (stable)
    - accepted_text (linearized visible text)
    - enough structural metadata for location reporting (heading_path if feasible)
  - Also produce artifacts/docx_extract/docx_struct.json (outline / headings / part inventory)
  - Also produce artifacts/docx_extract/linear_units.json (ordered list of unit_uids for chunking)
- Must include a deterministic para_id scheme that apply will use too.
- Tests:
  - A smoke test that runs extraction on fixtures and checks output JSON validity + non-empty units.

Phase 3 — Implement chunking skill (Subagent: “Chunker Implementer”, plus “Chunker Reviewer”)
- Implement .codex/skills/docx_chunk_atomic_manifest/
  - SKILL.md
  - scripts/chunk_docx.py (+ helpers)
- Requirements:
  - Input: artifacts/docx_extract/review_units.json and linear_units.json/docx_struct.json
  - Output:
    - artifacts/chunks/manifest.json
    - artifacts/chunks/chunk_XXXX.json
  - Each chunk must include:
    - primary_units: editable units
    - context_units_before/after: read-only context
    - metadata: heading_path, source_span indices, token_estimates
  - Coherence rule:
    - do not split in a way that breaks local coherence (e.g., sentence-to-sentence issues)
    - include minimal neighbor context so boundary issues remain visible
  - Enforce “context-only is not editable” by contract in chunk file.
  - Token budgeting:
    - read config/constants.json for model context, target fraction, hard max, overlap counts
    - estimate tokens (use tiktoken if available; otherwise a robust heuristic)
- Tests:
  - Validate chunk manifests
  - Ensure primary-only + context-only separation is correct
  - Ensure chunk sizes respect budgets (with controlled overflow for coherence)

Phase 4 — Implement search skill (Subagent: “Search Implementer”)
- Implement .codex/skills/docx_search_in_extraction/
  - SKILL.md
  - scripts/search_extracted.py
- Requirements:
  - Search accepted_text across units
  - Return hits with:
    - part + para_id + unit_uid
    - offsets into accepted_text
    - small snippet windows
  - Output artifacts/search/search_results.json
- Tests:
  - Search finds expected patterns in fixtures
  - Returned offsets/snippets are consistent

Phase 5 — Define chunk agent result contract + merge/dedup skill (Subagent: “Merger Implementer”, plus “Merger Reviewer”)
- Implement .codex/skills/docx_merge_dedup_validate_patch/
  - SKILL.md
  - scripts/merge_patch.py
- Requirements:
  - Read artifacts/chunk_results/chunk_XXXX_result.json files
  - Deduplicate repeated suggestions/ops using a computed dedup key:
    - recommended: hash of (type + target + range + normalized old/new/comment)
  - Detect conflicts:
    - overlapping edits in the same target paragraph/unit
    - contradictory replacements
  - Resolve safely:
    - prefer downgrading one/both to comments over risking bad edits
  - Ordering:
    - overall doc order
    - within same target, prepare ops so apply can run end→start (descending offsets) to avoid offset shifting bugs
  - Output:
    - artifacts/patch/merged_patch.json
    - artifacts/patch/merge_report.json
- Tests:
  - Feed synthetic chunk_results with duplicates/conflicts and verify dedup/conflict resolution

Phase 6 — Apply patch skill (MOST IMPORTANT) (Subagent: “Applier Implementer”, plus “Applier Reviewer”, plus “Word-Openability QA”)
- Implement .codex/skills/docx_apply_patch_to_output/
  - SKILL.md
  - scripts/apply_docx_patch.py (+ helpers)
- Inputs:
  - input/source.docx (or configurable input path under input/)
  - artifacts/patch/merged_patch.json
  - artifacts/docx_extract/review_units.json (baseline extraction; required for stable IDs + baseline accepted_text if you choose)
- Outputs:
  - artifacts/apply/apply_log.json
  - output/annotated.docx
- Non-negotiable behavior:
  - All text edits must be applied as track changes (w:ins/w:del). NEVER direct edits.
  - Comments must be inserted as true Word comments (comments.xml etc).
  - Verify expected.snippet at target range before applying; if mismatch:
    - do not force
    - log it in apply_log.json
    - optionally add a “target not found / snippet mismatch” comment if feasible.
- Offset-shift handling (must be implemented):
  - Group ops by target paragraph/unit; apply them in descending order of range.start so earlier offsets are not affected by later edits.
- Formatting preservation:
  - Preserve existing run formatting as much as feasible by cloning nearby w:rPr into inserted/deleted runs.
- OOXML packaging:
  - Update relationships + [Content_Types].xml if creating comments.xml.
- Tests:
  - After applying:
    - output/annotated.docx must be a valid zip
    - must contain w:ins and/or w:del for test edits
    - if comments inserted: must contain word/comments.xml and commentRangeStart/End markers
  - Add a test that applies:
    - one replacement edit
    - one insertion
    - one deletion
    - one comment
  - Verify results by parsing the output XML (do not rely on Word UI).

Phase 7 — Change report skill (Subagent: “Reporter Implementer”)
- Implement .codex/skills/docx_change_report_before_after/
  - SKILL.md
  - scripts/change_report.py
- Inputs:
  - artifacts/docx_extract/review_units.json
  - artifacts/patch/merged_patch.json
  - artifacts/apply/apply_log.json
- Outputs (writer-facing):
  - output/changes.md
  - output/changes.json
- Requirements:
  - for each op, emit:
    - stable location: heading_path (if available) + part + para_id/unit_uid
    - before snippet + after snippet
    - disambiguation when “before” text repeats
  - No page numbers.

Phase 8 — End-to-end runbook + Makefile + QA (Subagent: “Integration QA”)
- Provide Makefile targets (or justfile):
  - make fixtures         (download fixture docx if not present)
  - make extract
  - make chunk
  - make merge            (with synthetic chunk_results for pipeline test)
  - make apply
  - make report
  - make test             (runs unit + integration checks)
  - make e2e              (extract→chunk→(synthetic results)→merge→apply→report)
- Ensure README contains exact commands.

──────────────────────────────────────────────────────────────────────────────
SKILLS FORMAT (MUST BE VALID CODEX SKILLS)
Create skills under:
  .codex/skills/<skill_name>/SKILL.md
with optional:
  .codex/skills/<skill_name>/scripts/*

Each SKILL.md must start with YAML frontmatter:
---
name: <skill_name>
description: "<one-line triggerable description>"
---

and then concise operational instructions and exact I/O paths.

Do NOT put skills as single markdown files; each must be a directory.

──────────────────────────────────────────────────────────────────────────────
REQUIRED SKILLS (EACH ONE RESPONSIBILITY)
1) docx_extract_ooxml_to_artifacts
2) docx_chunk_atomic_manifest
3) docx_search_in_extraction
4) docx_merge_dedup_validate_patch
5) docx_apply_patch_to_output
6) docx_change_report_before_after

──────────────────────────────────────────────────────────────────────────────
PATCH FORMAT (YOU DESIGN, v1)
Design a JSON patch schema v1 supporting:
- schema_version, created_at, author
- ops: ordered list
op fields must include:
- type: add_comment | replace_range | insert_at | delete_range
- target: { part, para_id, unit_uid? }
- expected: { snippet }
- range: { start, end } offsets into accepted_text
For edits:
- MUST be track_changes only (no “direct”)
For comments:
- comment_text
Optional:
- category string (generic; no enforced proofreading taxonomy)

Important: for safe apply, apply grouped ops per paragraph end→start.

──────────────────────────────────────────────────────────────────────────────
CHUNK AGENT OUTPUT CONTRACT (FILE ONLY)
Each review subagent will output:
  artifacts/chunk_results/chunk_XXXX_result.json
Must include:
- chunk_id
- ops: patch ops targeting ONLY primary_units
- suggestions: optional notes (including “needs_neighbor_unit” handoffs)
- optional dedup_keys

Enforce:
- agents may read context_units but must not edit/comment on them

──────────────────────────────────────────────────────────────────────────────
FIXTURES (BIG DOCX, OPEN LICENSE, MANY PAGES)
You must include a fixtures download strategy.

Primary fixture suggestion (download into fixtures/):
- fixtures/NPPF_December_2023.docx
  Download from:
  https://data.parliament.uk/DepositedPapers/Files/DEP2023-1029/NPPF_December_2023.docx

This file is large and includes footnotes and an Open Government Licence statement in its text (verify in your notes).

Implement:
- fixtures/README.md explaining how to download
- a Makefile target “make fixtures” that downloads it (curl/wget) if missing
- optional: record sha256 in fixtures/README.md after download so tests are reproducible

If the environment cannot download during CI, at minimum:
- keep the fixture URL in fixtures/README.md and instruct users to download manually

Optionally add a second fixture if needed for tables/images complexity (only if you find a clearly licensed docx).

──────────────────────────────────────────────────────────────────────────────
TESTING REQUIREMENTS
You must add tests that:
- run extraction on the fixture and validate JSON outputs
- run chunking and validate manifest + chunk invariants
- generate a SMALL synthetic patch automatically (do NOT hardcode offsets blindly):
  - use extraction output to find a safe, unique occurrence of a target string
  - create patch ops for:
      * replace_range (small word replacement)
      * insert_at
      * delete_range
      * add_comment
- run apply and verify:
  - output/annotated.docx contains track-change tags in XML
  - output/annotated.docx contains comments.xml if comments requested
- run report and verify output files exist and include stable locations

Use a dedicated test script (python) and wire it into Makefile (make test, make e2e).

──────────────────────────────────────────────────────────────────────────────
ACCEPTANCE CRITERIA
- output/annotated.docx opens in Word and shows tracked changes (no silent edits).
- Comments are real Word comments anchored to tight spans.
- apply step is robust: expected.snippet mismatch causes skip + logging (never forced).
- Chunking yields coherent chunks with explicit primary vs context-only separation.
- All intermediates go to artifacts/; writer-facing final outputs go to output/.
- Skills are valid Codex skills under .codex/skills/ and are each single-responsibility.
- Everything is runnable via Makefile targets.

Now build the repository accordingly. Use subagents per phase, implement incrementally, and run tests after each phase before moving on.