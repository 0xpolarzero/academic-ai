---
name: docx_extract_ooxml_to_artifacts
description: Extract DOCX OOXML paragraph units from core document parts into deterministic JSON artifacts for downstream review.
---

# DOCX Extract (OOXML -> Artifacts)

Input path:
- Project root: `--project-dir projects/<project_slug>`
- DOCX file passed by CLI argument: `--input-docx input/source.docx` (project-relative unless absolute)

Output paths:
- `projects/<project_slug>/artifacts/docx_extract/review_units.json`
- `projects/<project_slug>/artifacts/docx_extract/docx_struct.json`
- `projects/<project_slug>/artifacts/docx_extract/linear_units.json`

Run:

```bash
python .codex/skills/docx_extract_ooxml_to_artifacts/scripts/extract_docx.py \
  --project-dir projects/thesis \
  --input-docx input/source.docx \
  --output-dir artifacts/docx_extract
```

Extraction scope:
- `word/document.xml`
- `word/header*.xml`
- `word/footer*.xml`
- `word/footnotes.xml`
- `word/endnotes.xml`

Paragraph traversal rule:
- Use `.//w:p` so table paragraphs are included.
