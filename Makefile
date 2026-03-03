SHELL := /bin/sh

PYTHON := $(shell if [ -x .venv/bin/python ]; then printf '%s' .venv/bin/python; elif command -v python3 >/dev/null 2>&1; then printf '%s' python3; else printf '%s' python; fi)

CONSTANTS := config/constants.json
FIXTURE_DOCX := fixtures/NPPF_December_2023.docx
FIXTURE_URL := https://data.parliament.uk/DepositedPapers/Files/DEP2023-1029/NPPF_December_2023.docx
PROJECT ?= thesis
WORKFLOW ?= fr_copyedit_conservative
RALPH ?= 1

EXTRACT_SCRIPT := .codex/skills/docx_extract_ooxml_to_artifacts/scripts/extract_docx.py
CHUNK_SCRIPT := .codex/skills/docx_chunk_atomic_manifest/scripts/chunk_docx.py
MERGE_SCRIPT := .codex/skills/docx_merge_dedup_validate_patch/scripts/merge_patch.py
APPLY_SCRIPT := .codex/skills/docx_apply_patch_to_output/scripts/apply_docx_patch.py
REPORT_SCRIPT := .codex/skills/docx_change_report_before_after/scripts/change_report.py

.PHONY: help fixtures extract chunk merge apply report test e2e clean project run resume

help:
	@echo "Pipeline targets"
	@echo "  make fixtures  # download fixture DOCX if missing"
	@echo "  make extract   # DOCX -> artifacts/docx_extract"
	@echo "  make chunk     # extraction -> artifacts/chunks"
	@echo "  make merge     # synthetic chunk_results -> artifacts/patch"
	@echo "  make apply     # patch -> output/annotated.docx + artifacts/apply"
	@echo "  make report    # patch/apply -> output/changes.{md,json,docx}"
	@echo "  make project PROJECT=<slug>                 # scaffold projects/<slug> layout"
	@echo "  make run PROJECT=<slug> WORKFLOW=<name> [INPUT=<file.docx>] [DRY_RUN=1] [RALPH=N] [SKIP_JUDGE=1] [CLI=codex|kimi|claude] [MODEL=<model>]  # run project workflow"
	@echo "  make resume PROJECT=<slug> WORKFLOW=<name> FROM=<step> [INPUT=<file.docx>] [RALPH=N] [CLI=codex|kimi|claude] [MODEL=<model>]  # resume from step: judge|merge|apply|report"
	@echo "  make resume PROJECT=<slug> WORKFLOW=<name> FROM_RALPH=N [INPUT=<file.docx>] [RALPH=N] [CLI=codex|kimi|claude] [MODEL=<model>]  # resume from Ralph run N"
	@echo "  make test      # pytest unit + integration checks"
	@echo "  make e2e       # offline thesis dry-run + acceptance checks"

project:
	@test -n "$(PROJECT)" || (echo "Usage: make project PROJECT=<slug>" && exit 2)
	@mkdir -p "projects/$(PROJECT)/input" "projects/$(PROJECT)/workflows" "projects/$(PROJECT)/artifacts" "projects/$(PROJECT)/output"
	@touch "projects/$(PROJECT)/input/.gitkeep" "projects/$(PROJECT)/artifacts/.gitkeep" "projects/$(PROJECT)/output/.gitkeep"
	@if [ -f "projects/thesis/workflows/fr_copyedit_conservative.xml" ] && [ ! -f "projects/$(PROJECT)/workflows/fr_copyedit_conservative.xml" ]; then \
		cp "projects/thesis/workflows/fr_copyedit_conservative.xml" "projects/$(PROJECT)/workflows/fr_copyedit_conservative.xml"; \
	fi
	@if [ -f "projects/thesis/workflows/fr_copyedit_micro.xml" ] && [ ! -f "projects/$(PROJECT)/workflows/fr_copyedit_micro.xml" ]; then \
		cp "projects/thesis/workflows/fr_copyedit_micro.xml" "projects/$(PROJECT)/workflows/fr_copyedit_micro.xml"; \
	fi
	@echo "Scaffolded projects/$(PROJECT)"

run:
	@test -n "$(PROJECT)" || (echo "Usage: make run PROJECT=<slug> WORKFLOW=<name> [INPUT=<file.docx>]" && exit 2)
	@test -n "$(WORKFLOW)" || (echo "Usage: make run PROJECT=<slug> WORKFLOW=<name> [INPUT=<file.docx>]" && exit 2)
	@test -d "projects/$(PROJECT)" || (echo "Missing project directory: projects/$(PROJECT)" && exit 2)
	@test -f "projects/$(PROJECT)/workflows/$(WORKFLOW).xml" || (echo "Missing workflow file: projects/$(PROJECT)/workflows/$(WORKFLOW).xml" && exit 2)
	@test -f "scripts/run_project.py" || (echo "Missing runner: scripts/run_project.py" && exit 2)
	@mkdir -p "projects/$(PROJECT)/input"
	@DOCX_COUNT=$$(find "projects/$(PROJECT)/input" -maxdepth 1 -name "*.docx" | wc -l); \
	if [ "$(INPUT)" = "" ] && [ "$$DOCX_COUNT" -eq 0 ] && [ -f "$(FIXTURE_DOCX)" ]; then \
		cp "$(FIXTURE_DOCX)" "projects/$(PROJECT)/input/source.docx"; \
		echo "Seeded projects/$(PROJECT)/input/source.docx from fixture"; \
	elif [ "$(INPUT)" != "" ] && [ ! -f "projects/$(PROJECT)/input/$(INPUT)" ]; then \
		echo "Error: Specified input file not found: projects/$(PROJECT)/input/$(INPUT)"; \
		exit 2; \
	fi
	@DRY_FLAG=""; \
	if [ -n "$(DRY_RUN)" ] && [ "$(DRY_RUN)" != "0" ] && [ "$(DRY_RUN)" != "false" ] && [ "$(DRY_RUN)" != "no" ]; then \
		DRY_FLAG="--dry-run"; \
	fi; \
	RALPH_FLAG="--ralph $(RALPH)"; \
	SKIP_JUDGE_FLAG=""; \
	if [ -n "$(SKIP_JUDGE)" ] && [ "$(SKIP_JUDGE)" != "0" ] && [ "$(SKIP_JUDGE)" != "false" ] && [ "$(SKIP_JUDGE)" != "no" ]; then \
		SKIP_JUDGE_FLAG="--skip-judge"; \
	fi; \
	CLI_FLAG=""; \
	if [ -n "$(CLI)" ]; then \
		CLI_FLAG="--cli $(CLI)"; \
	fi; \
	MODEL_FLAG=""; \
	if [ -n "$(MODEL)" ]; then \
		MODEL_FLAG="--model $(MODEL)"; \
	fi; \
	INPUT_FLAG=""; \
	if [ -n "$(INPUT)" ]; then \
		INPUT_FLAG="--input $(INPUT)"; \
	fi; \
	$(PYTHON) scripts/run_project.py --project "$(PROJECT)" --workflow "$(WORKFLOW)" --constants "$(CONSTANTS)" $$INPUT_FLAG $$DRY_FLAG $$RALPH_FLAG $$SKIP_JUDGE_FLAG $$CLI_FLAG $$MODEL_FLAG

resume:
	@test -n "$(PROJECT)" || (echo "Usage: make resume PROJECT=<slug> WORKFLOW=<name> FROM=<step>|FROM_RALPH=N [INPUT=<file.docx>]" && exit 2)
	@test -n "$(WORKFLOW)" || (echo "Usage: make resume PROJECT=<slug> WORKFLOW=<name> FROM=<step>|FROM_RALPH=N [INPUT=<file.docx>]" && exit 2)
	@test -n "$(FROM)$(FROM_RALPH)" || (echo "Usage: make resume PROJECT=<slug> WORKFLOW=<name> FROM=<step>|FROM_RALPH=N [INPUT=<file.docx>]" && exit 2)
	@if [ -n "$(FROM)" ] && [ -n "$(FROM_RALPH)" ]; then \
		echo "Error: specify only one of FROM=<step> or FROM_RALPH=N"; \
		exit 2; \
	fi
	@test -d "projects/$(PROJECT)" || (echo "Missing project directory: projects/$(PROJECT)" && exit 2)
	@test -f "projects/$(PROJECT)/workflows/$(WORKFLOW).xml" || (echo "Missing workflow file: projects/$(PROJECT)/workflows/$(WORKFLOW).xml" && exit 2)
	@test -f "scripts/run_project.py" || (echo "Missing runner: scripts/run_project.py" && exit 2)
	@mkdir -p "projects/$(PROJECT)/input"
	@if [ "$(INPUT)" != "" ] && [ ! -f "projects/$(PROJECT)/input/$(INPUT)" ]; then \
		echo "Error: Specified input file not found: projects/$(PROJECT)/input/$(INPUT)"; \
		exit 2; \
	fi
	@RALPH_FLAG="--ralph $(RALPH)"; \
	CLI_FLAG=""; \
	if [ -n "$(CLI)" ]; then \
		CLI_FLAG="--cli $(CLI)"; \
	fi; \
	MODEL_FLAG=""; \
	if [ -n "$(MODEL)" ]; then \
		MODEL_FLAG="--model $(MODEL)"; \
	fi; \
	INPUT_FLAG=""; \
	if [ -n "$(INPUT)" ]; then \
		INPUT_FLAG="--input $(INPUT)"; \
	fi; \
	FROM_STEP_FLAG=""; \
	if [ -n "$(FROM)" ]; then \
		FROM_STEP_FLAG="--from-step $(FROM)"; \
	fi; \
	FROM_RALPH_FLAG=""; \
	if [ -n "$(FROM_RALPH)" ]; then \
		FROM_RALPH_FLAG="--from-ralph $(FROM_RALPH)"; \
	fi; \
	$(PYTHON) scripts/run_project.py --project "$(PROJECT)" --workflow "$(WORKFLOW)" --constants "$(CONSTANTS)" $$FROM_STEP_FLAG $$FROM_RALPH_FLAG $$INPUT_FLAG $$RALPH_FLAG $$CLI_FLAG $$MODEL_FLAG

fixtures:
	@mkdir -p fixtures
	@if [ -f "$(FIXTURE_DOCX)" ]; then \
		echo "Fixture already exists: $(FIXTURE_DOCX)"; \
	elif command -v curl >/dev/null 2>&1; then \
		echo "Downloading fixture with curl..."; \
		curl -fL --retry 3 --retry-delay 2 --output "$(FIXTURE_DOCX)" "$(FIXTURE_URL)"; \
	elif command -v wget >/dev/null 2>&1; then \
		echo "Downloading fixture with wget..."; \
		wget -O "$(FIXTURE_DOCX)" "$(FIXTURE_URL)"; \
	else \
		echo "No curl/wget available. Download manually:"; \
		echo "  URL:  $(FIXTURE_URL)"; \
		echo "  Save: $(FIXTURE_DOCX)"; \
		exit 1; \
	fi
	@echo "Fixture ready: $(FIXTURE_DOCX)"

extract: fixtures
	@mkdir -p artifacts/docx_extract
	$(PYTHON) $(EXTRACT_SCRIPT) \
		--input-docx $(FIXTURE_DOCX) \
		--output-dir artifacts/docx_extract

chunk: extract
	$(PYTHON) $(CHUNK_SCRIPT) \
		--constants $(CONSTANTS) \
		--review-units artifacts/docx_extract/review_units.json \
		--linear-units artifacts/docx_extract/linear_units.json \
		--docx-struct artifacts/docx_extract/docx_struct.json \
		--output-dir artifacts/chunks

merge: chunk
	$(PYTHON) scripts/run_e2e.py --constants $(CONSTANTS) --only-generate-synthetic
	$(PYTHON) $(MERGE_SCRIPT) \
		--chunk-results-dir artifacts/chunk_results \
		--linear-units artifacts/docx_extract/linear_units.json \
		--output-dir artifacts/patch \
		--author phase8-merge

apply: merge
	$(PYTHON) $(APPLY_SCRIPT) \
		--input-docx $(FIXTURE_DOCX) \
		--patch artifacts/patch/merged_patch.json \
		--review-units artifacts/docx_extract/review_units.json \
		--output-docx output/annotated.docx \
		--apply-log artifacts/apply/apply_log.json \
		--author phase8-apply

report: apply
	$(PYTHON) $(REPORT_SCRIPT) \
		--review-units artifacts/docx_extract/review_units.json \
		--patch artifacts/patch/merged_patch.json \
		--apply-log artifacts/apply/apply_log.json \
		--output-md output/changes.md \
		--output-json output/changes.json

test:
	$(PYTHON) scripts/run_tests.py

e2e: fixtures
	$(MAKE) run PROJECT=thesis WORKFLOW=fr_copyedit_conservative DRY_RUN=1

clean:
	@rm -rf artifacts/* output/*
	@echo "Cleaned artifacts/ and output/."
