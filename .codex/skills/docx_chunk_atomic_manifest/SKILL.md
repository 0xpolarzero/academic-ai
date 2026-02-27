---
name: docx_chunk_atomic_manifest
description: Build atomic DOCX review chunks and a manifest with editable primary units, read-only context overlap, and token-budget metadata.
---

# DOCX Chunking (Atomic Manifest)

Input paths:
- Project root: `--project-dir projects/<project_slug>`
- `artifacts/docx_extract/review_units.json`
- `artifacts/docx_extract/linear_units.json`
- `artifacts/docx_extract/docx_struct.json` (optional)

Output paths:
- `projects/<project_slug>/artifacts/chunks/manifest.json`
- `projects/<project_slug>/artifacts/chunks/chunk_XXXX.json`

Run:

```bash
python .codex/skills/docx_chunk_atomic_manifest/scripts/chunk_docx.py \
  --project-dir projects/thesis \
  --review-units artifacts/docx_extract/review_units.json \
  --linear-units artifacts/docx_extract/linear_units.json \
  --output-dir artifacts/chunks
```

Chunk contract:
- `primary_units`: editable units owned by the chunk.
- `context_units_before` and `context_units_after`: read-only neighboring units.
- `contract`: explicit non-editable rule for all context units.
- `metadata`: heading path, source span indices, and token estimates.

Budgeting and overlap:
- Reads `config/constants.json` (`chunking.token_budget`, `chunking.paths`).
- Uses `tiktoken` token counts when available.
- Falls back to deterministic heuristic estimation when `tiktoken` is unavailable.
- Allows controlled overflow only when a single unit itself exceeds the hard max.
