---
name: docx_extract_ooxml_to_artifacts
description: Extract DOCX OOXML paragraph units from core document parts into deterministic JSON artifacts for downstream review.
---

# DOCX Extract (OOXML -> Artifacts)

Input path:
- DOCX file passed by CLI argument: `--input-docx /absolute/or/relative/path/to/file.docx`

Output paths:
- `artifacts/docx_extract/review_units.json`
- `artifacts/docx_extract/docx_struct.json`
- `artifacts/docx_extract/linear_units.json`

Run:

```bash
python .codex/skills/docx_extract_ooxml_to_artifacts/scripts/extract_docx.py \
  --input-docx fixtures/example.docx \
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
