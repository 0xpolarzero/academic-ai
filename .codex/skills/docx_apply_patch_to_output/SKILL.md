---
name: docx_apply_patch_to_output
description: Apply merged patch operations to DOCX output using track changes and real Word comments, with snippet guards and apply logging.
---

# DOCX Apply Patch To Output

Inputs:
- Project root: `--project-dir projects/<project_slug>`
- `input/source.docx`
- `artifacts/patch/merged_patch.json`
- `artifacts/docx_extract/review_units.json`

Outputs:
- `projects/<project_slug>/output/annotated.docx`
- `projects/<project_slug>/artifacts/apply/apply_log.json`

Run:

```bash
python .codex/skills/docx_apply_patch_to_output/scripts/apply_docx_patch.py \
  --project-dir projects/thesis \
  --input-docx input/source.docx \
  --patch artifacts/patch/merged_patch.json \
  --review-units artifacts/docx_extract/review_units.json \
  --output-docx output/annotated.docx \
  --apply-log artifacts/apply/apply_log.json \
  --author "docx_apply_patch_to_output"
```

Behavior:
- Resolves targets by `target.part + target.para_id` (and optional `target.unit_uid`) using `review_units.json` location metadata.
- Validates every op by matching `expected.snippet` against the target paragraph visible text at `range` offsets.
- Skips and logs mismatches (never force-applies).
- Groups ops per target paragraph and applies in descending `range.start`.
- Applies text edits only as tracked revisions (`w:ins`, `w:del`, `w:delText`).
- Inserts real Word comments (`word/comments.xml` + `commentRangeStart/End` + `commentReference`).
- Preserves formatting as feasible by cloning nearby `w:rPr` into generated runs.
- Updates OOXML packaging when comments are created (`[Content_Types].xml` + part `.rels`).
