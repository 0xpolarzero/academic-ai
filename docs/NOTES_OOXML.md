# OOXML Notes (Phase 1)

## 1) Codex project-local skill folder format (verified)

- Project-local skills live under `./.codex/skills/<skill-name>/` (repo scaffold includes `.codex/skills`).
- Each skill folder must include `SKILL.md` with YAML frontmatter containing at least:
  - `name`
  - `description`
- `name` constraints (from validator): lowercase/hyphen-case (`[a-z0-9-]+`), no leading/trailing/consecutive `-`, max 64 chars.
- Optional but recommended:
  - `agents/openai.yaml`
  - `scripts/`, `references/`, `assets/`
- Fast validation command:
  - `python /Users/polarzero/.codex/skills/.system/skill-creator/scripts/quick_validate.py <skill-dir>`

## 2) Track changes elements/attrs (insert/delete)

- Inserted content is represented by `w:ins`.
- Deleted run content is represented by `w:del` and deleted text by `w:delText` inside the deleted run.
- Revision metadata is attached via `w:id`, `w:author`, `w:date`.
- Practical rule: always emit `w:id`, `w:author`, and an ISO-8601 UTC `w:date` on `w:ins`/`w:del` when generating tracked changes.

Example shapes:

```xml
<w:ins w:id="1" w:author="Reviewer" w:date="2026-02-27T12:00:00Z">
  <w:r><w:t>inserted text</w:t></w:r>
</w:ins>

<w:del w:id="2" w:author="Reviewer" w:date="2026-02-27T12:00:00Z">
  <w:r><w:delText>deleted text</w:delText></w:r>
</w:del>
```

## 3) Comment storage and anchors

- Comment bodies are stored in `word/comments.xml` under root `w:comments` with `w:comment` entries keyed by `w:id`.
- In the document story (`word/document.xml`), anchors use:
  - `w:commentRangeStart w:id="..."`
  - `w:commentRangeEnd w:id="..."`
  - `w:commentReference w:id="..."`
- Practical rule: use one consistent `w:id` value across `commentRangeStart`, `commentRangeEnd`, `commentReference`, and the matching `w:comment` in `comments.xml`.

## 4) Packaging updates when adding comments

When adding comments to a DOCX package, update all of the following:

1. Add the comments part payload at `word/comments.xml`.
2. Add `[Content_Types].xml` `Override` for `/word/comments.xml` with content type:
   - `application/vnd.openxmlformats-officedocument.wordprocessingml.comments+xml`
3. Add relationship in `word/_rels/document.xml.rels` from main document to comments part using type:
   - `http://schemas.openxmlformats.org/officeDocument/2006/relationships/comments`
   - target typically `comments.xml` (relative to `word/`)

The probe script in `scripts/prototypes/ooxml_comment_trackchange_probe.py` enforces these checks.

## References

### Codex skills (local)

- `/Users/polarzero/.codex/skills/.system/skill-creator/SKILL.md`
- `/Users/polarzero/.codex/skills/.system/skill-creator/scripts/quick_validate.py`
- `/Users/polarzero/code/projects/academic-ai/docs/ARCHITECTURE.md`

### OOXML / Open XML references

- InsertedRun (`w:ins`): https://learn.microsoft.com/en-us/dotnet/api/documentformat.openxml.wordprocessing.insertedrun?view=openxml-3.0.1
- DeletedRun (`w:del`): https://learn.microsoft.com/en-us/dotnet/api/documentformat.openxml.wordprocessing.deletedrun?view=openxml-3.0.1
- DeletedText (`w:delText`): https://learn.microsoft.com/en-us/dotnet/api/documentformat.openxml.wordprocessing.deletedtext?view=openxml-3.0.1
- RunTrackChangeType (`w:id`, `w:author`, `w:date` attrs): https://learn.microsoft.com/en-us/dotnet/api/documentformat.openxml.wordprocessing.runtrackchangetype?view=openxml-3.0.1
- Comments root (`w:comments`): https://learn.microsoft.com/en-us/dotnet/api/documentformat.openxml.wordprocessing.comments?view=openxml-3.0.1
- CommentRangeStart: https://learn.microsoft.com/en-us/dotnet/api/documentformat.openxml.wordprocessing.commentrangestart?view=openxml-3.0.1
- CommentRangeEnd: https://learn.microsoft.com/en-us/dotnet/api/documentformat.openxml.wordprocessing.commentrangeend?view=openxml-3.0.1
- CommentReference: https://learn.microsoft.com/en-us/dotnet/api/documentformat.openxml.wordprocessing.commentreference?view=openxml-3.0.1
- WordprocessingCommentsPart (official SDK source, constants):
  - https://github.com/dotnet/Open-XML-SDK/blob/main/generated/DocumentFormat.OpenXml/DocumentFormat.OpenXml.Generator/DocumentFormat.OpenXml.Generator.OpenXmlGenerator/Part_WordprocessingCommentsPart.g.cs
  - https://github.com/dotnet/Open-XML-SDK/blob/main/generated/DocumentFormat.OpenXml/DocumentFormat.OpenXml.Generator/DocumentFormat.OpenXml.Generator.OpenXmlGenerator/Part_MainDocumentPart.g.cs
