# academic-ai

DOCX review pipeline with deterministic artifacts and writer-facing outputs.

## Path Rules

- Final writer-facing outputs are in `output/` only:
  - `output/annotated.docx`
  - `output/changes.md`
  - `output/changes.json`
- Intermediate/runtime artifacts are in `artifacts/` only.

## Prerequisites

- Python 3.10+ (`.venv/bin/python` is used automatically when present, otherwise `python3`).
- `pytest` installed in the selected Python environment for `make test`.
- `curl` or `wget` for `make fixtures` (manual fallback is documented in `fixtures/README.md`).

## Setup

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip pytest
```

## Runbook (Make Targets)

1. Download fixture (if missing):

```bash
make fixtures
```

2. Extract OOXML paragraphs to artifacts:

```bash
make extract
```

3. Build chunk manifest + chunk files:

```bash
make chunk
```

4. Merge patch from synthetic chunk results (pipeline smoke):

```bash
make merge
```

5. Apply merged patch with tracked changes/comments:

```bash
make apply
```

6. Build writer-facing before/after report:

```bash
make report
```

7. Run tests (unit + integration checks):

```bash
make test
```

8. Run full e2e smoke pipeline:

```bash
make e2e
```

## Direct Command Equivalents

```bash
# fixture
make fixtures

# extract
python .codex/skills/docx_extract_ooxml_to_artifacts/scripts/extract_docx.py \
  --input-docx fixtures/NPPF_December_2023.docx \
  --output-dir artifacts/docx_extract

# chunk
python .codex/skills/docx_chunk_atomic_manifest/scripts/chunk_docx.py \
  --constants config/constants.json \
  --review-units artifacts/docx_extract/review_units.json \
  --linear-units artifacts/docx_extract/linear_units.json \
  --docx-struct artifacts/docx_extract/docx_struct.json \
  --output-dir artifacts/chunks

# synthetic chunk_result + merge
python scripts/run_e2e.py --constants config/constants.json --only-generate-synthetic
python .codex/skills/docx_merge_dedup_validate_patch/scripts/merge_patch.py \
  --chunk-results-dir artifacts/chunk_results \
  --linear-units artifacts/docx_extract/linear_units.json \
  --output-dir artifacts/patch \
  --author phase8-merge

# apply
python .codex/skills/docx_apply_patch_to_output/scripts/apply_docx_patch.py \
  --input-docx fixtures/NPPF_December_2023.docx \
  --patch artifacts/patch/merged_patch.json \
  --review-units artifacts/docx_extract/review_units.json \
  --output-docx output/annotated.docx \
  --apply-log artifacts/apply/apply_log.json \
  --author phase8-apply

# report
python .codex/skills/docx_change_report_before_after/scripts/change_report.py \
  --review-units artifacts/docx_extract/review_units.json \
  --patch artifacts/patch/merged_patch.json \
  --apply-log artifacts/apply/apply_log.json \
  --output-md output/changes.md \
  --output-json output/changes.json

# tests
python scripts/run_tests.py

# e2e
python scripts/run_e2e.py --constants config/constants.json
```

## Notes

- `scripts/run_e2e.py` generates a small synthetic patch by discovering unique safe text spans from extracted primary chunk units (no hardcoded offsets).
- `make clean` removes generated files under `artifacts/` and `output/`.
