---
name: docx_merge_dedup_validate_patch
description: Merge chunk result ops into a single validated patch with deterministic deduplication, conflict downgrades, and stable ordering.
---

# DOCX Merge + Dedup + Validate Patch

Input paths:
- Project root: `--project-dir projects/<project_slug>`
- `artifacts/chunk_results/chunk_XXXX_result.json`
- `artifacts/docx_extract/linear_units.json` (optional, used for document-order sorting)
- `artifacts/chunks/manifest.json`

Output paths:
- `projects/<project_slug>/artifacts/patch/merged_patch.json`
- `projects/<project_slug>/artifacts/patch/merge_report.json`

Run:

```bash
python .codex/skills/docx_merge_dedup_validate_patch/scripts/merge_patch.py \
  --project-dir projects/thesis \
  --chunk-results-dir artifacts/chunk_results \
  --linear-units artifacts/docx_extract/linear_units.json \
  --chunks-manifest artifacts/chunks/manifest.json \
  --output-dir artifacts/patch \
  --author "docx_merge_dedup_validate_patch"
```

Behavior:
- Reads `chunk_XXXX_result.json` files in deterministic filename order.
- Normalizes patch ops to the `patch.v1` operation contract (`type`, `target`, `range`, `expected`).
- Deduplicates ops using a deterministic key hashed from:
  - `type`
  - `target`
  - `range`
  - normalized `expected.snippet` (old text)
  - normalized `replacement` / `new_text` / `comment_text`
- Detects conflicts within the same paragraph target (`part + para_id`, regardless of `unit_uid` presence):
  - overlapping edits
  - contradictory replacements or insertions at the same range
- Resolves conflicts safely by downgrading conflicting edit ops to `add_comment`.
- Orders merged ops:
  - document order from `linear_units.json` when available
  - descending `range.start` within each target (safe for end-to-start application).
