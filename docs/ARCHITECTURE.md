# Architecture (Phase 0)

## Goal

Define stable contracts and repository scaffolding for a document-proofreading pipeline, without implementing runtime logic yet.

## Phase 0 Boundaries

- Included: directory scaffold, schema contracts, ID and range semantics.
- Excluded: chunking logic, orchestration, model execution, patch application code.

## Stable Identity

All text units are addressed by a composite identity:

- `para_id`: stable paragraph-level identifier.
- `unit_uid`: stable unit-level identifier within that paragraph.
- Composite form: `{para_id}::{unit_uid}`.

This pair is the canonical target key in all contracts.

## Canonical Text And Offsets

Every chunk defines `accepted_text`, which is the only baseline for offsets and snippet checks.

Offset semantics:

- Indexing: UTF-16 code units.
- `range.start`: inclusive.
- `range.end`: exclusive.
- Validity rule: `0 <= start <= end <= accepted_text.length`.

Operational implications:

- `replace_range` and `delete_range` operate on `accepted_text.slice(start, end)`.
- `insert_at` uses a collapsed range (`start == end`) as the insertion point.
- `add_comment` anchors to the provided range.
- `expected.snippet` is matched against `accepted_text.slice(start, end)` before applying any operation.

## Chunk Contract

`chunk.v1` separates units into:

- `primary_units`: units that the chunk is responsible for editing/reviewing.
- `context_units`: neighboring units provided only for context.

Both unit types carry `para_id`, `unit_uid`, `range`, and `text`, but role is explicit and enforced by schema.

## Patch Contract

`patch.v1` supports four operations:

- `add_comment`
- `replace_range`
- `insert_at`
- `delete_range`

Each operation includes:

- `target` (`para_id`, `unit_uid`)
- `range` (against `accepted_text`)
- `expected.snippet` (optimistic text match guard)

## Chunk Result Contract

`chunk_result.v1` packages per-chunk outcomes:

- status (`ok`, `needs_review`, `error`)
- patch operations
- optional findings/notes/errors metadata

## Directory Scaffold

Expected top-level directories:

- `.codex/skills`
- `input/`
- `output/`
- `artifacts/`
- `fixtures/`
- `tests/`
- `scripts/`
- `docs/`
- `config/`
- `schemas/`

`input/`, `output/`, and `artifacts/` are intentionally gitignored.
