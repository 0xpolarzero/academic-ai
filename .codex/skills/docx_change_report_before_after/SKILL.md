---
name: docx_change_report_before_after
description: Build writer-facing before/after change reports from patch and apply artifacts with stable locations and apply status.
---

# DOCX Change Report (Before/After)

Input paths:
- `artifacts/docx_extract/review_units.json`
- `artifacts/patch/merged_patch.json`
- `artifacts/apply/apply_log.json`

Output paths:
- `output/changes.md`
- `output/changes.json`

Run:

```bash
python .codex/skills/docx_change_report_before_after/scripts/change_report.py \
  --review-units artifacts/docx_extract/review_units.json \
  --patch artifacts/patch/merged_patch.json \
  --apply-log artifacts/apply/apply_log.json \
  --output-md output/changes.md \
  --output-json output/changes.json
```

Behavior:
- Emits one report entry per patch op in original op order.
- Includes stable location for each op:
  - `heading_path` (if available)
  - `part`
  - `para_id`
  - `unit_uid`
- Includes `before_snippet` and `after_snippet` for every op.
- For `add_comment`, includes comment text in both `after_snippet` and `annotation`.
- Adds repeat disambiguation metadata when the `before_snippet` appears multiple times in the unit `accepted_text`.
- Includes apply status from `apply_log.json` (`applied`/`skipped`) and skip reason when present.
- Does not use page numbers.
