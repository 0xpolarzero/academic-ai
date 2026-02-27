# DOCX Review Pipeline Design Doc

## Purpose

This repository provides a **repeatable workflow** to review very long `.docx` files using multiple agents, while producing outputs that are **actionable for writers**:

* An **annotated DOCX** where all edits appear as **Track Changes** (no silent edits)
* **Word comments** anchored to small, precise spans
* A **before → after report** with stable locations so the writer can search/replace if they prefer

The system is **generic**: it does not embed a “proofreading rubric”. The “what to suggest” policy is supplied separately to chunk-review agents.

---

## High-level workflow

1. **Put the original file in `input/`**
   Example: `input/source.docx`

2. **Extract the document into a structured representation (`artifacts/`)**
   The extractor reads the DOCX (OOXML) and outputs:

   * ordered “units” (paragraph-like blocks)
   * each unit’s `accepted_text` (plain text)
   * stable IDs (`part`, `para_id`, `unit_uid`)
   * enough mapping info to later place edits/comments back into OOXML

3. **Split into coherent chunks (`artifacts/chunks/`)**
   Chunking produces small, self-contained JSON files, each with:

   * `primary_units`: what the chunk agent is allowed to edit/comment on
   * `context_units_before/after`: nearby context for understanding (read-only)

4. **Spawn one review agent per chunk**
   Each agent reads its chunk file and produces a **chunk result JSON** containing:

   * patch operations (track-changes edits and/or comments) targeting only `primary_units`
   * optional “handoff” notes if a change would require touching context-only text

5. **Merge + deduplicate + validate all chunk results**
   The merger:

   * removes duplicates (common with chunk overlap)
   * detects conflicts (overlapping edits on the same target)
   * produces one ordered patch file

6. **Apply the patch once to the original DOCX**
   The applier writes:

   * `output/annotated.docx` with track changes + comments
   * an apply log for anything that could not be applied safely

7. **Generate a before → after report**
   Produces:

   * `output/changes.md`
   * `output/changes.json`
     Each entry includes stable location info (heading path + IDs) to disambiguate duplicates.

---

## Directory layout

* `input/`
  User-provided source DOCX (gitignored)

* `artifacts/`
  Intermediates (gitignored). Includes extracted JSON, chunks, chunk results, merged patch, logs.

* `output/`
  Final writer-facing files:

  * `output/annotated.docx`
  * `output/changes.md`
  * `output/changes.json`

* `fixtures/`
  Large public test DOCX and/or a download script

* `.codex/skills/`
  One directory per Codex skill (each has `SKILL.md` + its `scripts/`)

---

## Why “extract → patch → apply” (instead of editing DOCX directly)

DOCX files are **zip archives containing XML**. Inserting Track Changes and comments requires editing multiple parts correctly (document XML, comments XML, relationships, content types).
This pipeline avoids brittle, ad-hoc edits by:

* extracting stable “units” and plain text with IDs
* making agents produce **structured patch operations**
* applying changes centrally in a controlled, testable step

---

## Chunking model: primary vs context-only

Chunk coherence is the core requirement for long documents.

Each chunk file explicitly separates:

* **Primary units**: the content the chunk agent may propose edits/comments on
* **Context-only units**: included only so the agent can understand boundaries (e.g., the next sentence), but the agent must not propose changes to them

This prevents two common failure modes:

* agents missing issues that span the boundary between chunks
* duplicate/conflicting edits because overlap is “editable”

If an agent sees a necessary change in context-only text, it emits a **handoff suggestion**, and the orchestrator can create a follow-up chunk spanning both units.

---

## Patch model (conceptually)

Agents don’t output edited DOCX files. They output a **patch**: a list of operations such as:

* `replace_range` (track changes)
* `insert_at` (track changes)
* `delete_range` (track changes)
* `add_comment`

Every operation includes:

* a **target location** (`part`, `para_id`, optionally `unit_uid`)
* a **character range** into the unit’s `accepted_text`
* an `expected.snippet` safety check (must match baseline)

This makes patching safer and makes merging multi-agent output possible.

---

## Why merging patches matters (simple explanation)

Character offsets can shift when earlier edits are applied. Also, merging two independently edited DOCX files is complex and error-prone.

So instead:

* merge **operations** (patch ops), not files
* apply ops in a stable order:

  * **by document order**
  * within each paragraph/unit, apply edits **from end → start** so earlier edits don’t shift later ranges

---

## Safety and failure handling

The applier is conservative:

* If `expected.snippet` doesn’t match at apply-time:

  * it **does not force the change**
  * it logs the failure in `artifacts/apply/apply_log.json`
  * optionally it can add a “target not found” comment (if implemented)

This prevents “wrong place” edits.

---

## Codex skills: responsibilities

Each skill does exactly one job:

1. **Extract DOCX → artifacts**
2. **Chunk extracted units → artifacts/chunks**
3. **Search extracted units (location-aware)**
4. **Merge/dedup/validate chunk results → merged patch**
5. **Apply patch → output/annotated.docx**
6. **Generate before/after report → output/**

All scripts live inside their skill folder.

---

## Testing strategy

Testing is part of the workflow, not an afterthought:

* A real, multi-page fixture DOCX lives (or downloads) into `fixtures/`
* `make test` / `make e2e` runs:

  1. extract on fixture (validate JSON outputs)
  2. chunk (validate chunk invariants + size budgets)
  3. generate a small synthetic patch from extracted text (so offsets are real)
  4. apply patch (verify resulting XML contains `w:ins`/`w:del` and comments parts)
  5. produce report (verify it references stable locations)

---

## What this repo does not do

* It does not define what “good writing” is.
* It does not decide which corrections to make.
* It does not rely on page numbers (Word pagination is layout-dependent).

It provides a reliable **mechanism** for extracting, distributing work, collecting suggestions, and producing a track-changes-ready DOCX.
