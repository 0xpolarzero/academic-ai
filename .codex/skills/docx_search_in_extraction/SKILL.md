---
name: docx_search_in_extraction
description: Search extracted DOCX accepted_text across review units and emit deterministic hit artifacts with offsets and snippets.
---

# DOCX Search In Extraction

Input path:
- `artifacts/docx_extract/review_units.json`

Output path:
- `artifacts/search/search_results.json`

Run:

```bash
python .codex/skills/docx_search_in_extraction/scripts/search_extracted.py \
  --review-units artifacts/docx_extract/review_units.json \
  --output-dir artifacts/search \
  --query "comment anchor"
```

Options:
- `--regex`: treat `--query` as a regex pattern.
- `--ignore-case`: case-insensitive matching.
- `--snippet-chars N`: snippet window size around each hit (default: `40`).

Search contract:
- Search `accepted_text` in deterministic unit order.
- Each hit includes:
  - `part`, `para_id`, `unit_uid`
  - `start`, `end` (UTF-16 code-unit offsets; start inclusive, end exclusive)
  - `match_text`, `snippet`, `snippet_start`, `snippet_end`
- Root metadata includes `query`, `unit_count`, `hit_count`, and `hit_unit_count`.
