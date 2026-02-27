#!/usr/bin/env python3
"""Run end-to-end DOCX pipeline smoke flow with synthetic chunk results."""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass
import itertools
import json
from pathlib import Path
import re
import subprocess
import sys
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]

EXTRACT_SCRIPT = REPO_ROOT / ".codex/skills/docx_extract_ooxml_to_artifacts/scripts/extract_docx.py"
CHUNK_SCRIPT = REPO_ROOT / ".codex/skills/docx_chunk_atomic_manifest/scripts/chunk_docx.py"
MERGE_SCRIPT = REPO_ROOT / ".codex/skills/docx_merge_dedup_validate_patch/scripts/merge_patch.py"
APPLY_SCRIPT = REPO_ROOT / ".codex/skills/docx_apply_patch_to_output/scripts/apply_docx_patch.py"
REPORT_SCRIPT = REPO_ROOT / ".codex/skills/docx_change_report_before_after/scripts/change_report.py"

WORD_RE = re.compile(r"[A-Za-z][A-Za-z'-]{3,}")
DEFAULT_FIXTURE_NAME = "NPPF_December_2023.docx"

DEFAULT_PIPELINE_PATHS = {
    "fixtures_dir": "fixtures",
    "default_fixture_docx": f"fixtures/{DEFAULT_FIXTURE_NAME}",
    "extract_output_dir": "artifacts/docx_extract",
    "chunks_output_dir": "artifacts/chunks",
    "chunk_results_dir": "artifacts/chunk_results",
    "patch_output_dir": "artifacts/patch",
    "merged_patch": "artifacts/patch/merged_patch.json",
    "merge_report": "artifacts/patch/merge_report.json",
    "apply_log": "artifacts/apply/apply_log.json",
    "annotated_docx": "output/annotated.docx",
    "changes_md": "output/changes.md",
    "changes_json": "output/changes.json",
}


@dataclass(frozen=True)
class PipelinePaths:
    constants: Path
    fixtures_dir: Path
    fixture_docx: Path
    extract_output_dir: Path
    chunks_output_dir: Path
    chunk_results_dir: Path
    patch_output_dir: Path
    merged_patch: Path
    merge_report: Path
    apply_log: Path
    annotated_docx: Path
    changes_md: Path
    changes_json: Path


@dataclass(frozen=True)
class SyntheticChunkResult:
    chunk_id: str
    output_path: Path
    op_count: int


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--constants",
        type=Path,
        default=Path("config/constants.json"),
        help="Path to config constants JSON",
    )
    parser.add_argument(
        "--author",
        default="phase8-e2e",
        help="Author value used for merge/apply metadata",
    )
    parser.add_argument(
        "--only-generate-synthetic",
        action="store_true",
        help="Only write artifacts/chunk_results/chunk_*_result.json from existing chunk artifacts",
    )
    return parser


def _repo_path(raw_path: Any) -> Path:
    path = Path(str(raw_path))
    if path.is_absolute():
        return path
    return REPO_ROOT / path


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _dump_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _load_constants(constants_path: Path) -> dict[str, Any]:
    resolved = _repo_path(constants_path)
    if not resolved.exists():
        raise FileNotFoundError(f"constants.json not found: {resolved}")
    return _load_json(resolved)


def _resolve_paths(constants_path: Path, constants: dict[str, Any]) -> PipelinePaths:
    pipeline = constants.get("pipeline", {}) if isinstance(constants.get("pipeline"), dict) else {}
    paths_cfg = pipeline.get("paths", {}) if isinstance(pipeline.get("paths"), dict) else {}
    fixture_cfg = pipeline.get("fixture", {}) if isinstance(pipeline.get("fixture"), dict) else {}

    fixtures_dir = _repo_path(paths_cfg.get("fixtures_dir", DEFAULT_PIPELINE_PATHS["fixtures_dir"]))
    configured_fixture = paths_cfg.get("default_fixture_docx")
    if configured_fixture:
        fixture_docx = _repo_path(configured_fixture)
    else:
        fixture_name = str(fixture_cfg.get("nppf_filename", DEFAULT_FIXTURE_NAME))
        fixture_docx = fixtures_dir / fixture_name

    return PipelinePaths(
        constants=_repo_path(constants_path),
        fixtures_dir=fixtures_dir,
        fixture_docx=fixture_docx,
        extract_output_dir=_repo_path(paths_cfg.get("extract_output_dir", DEFAULT_PIPELINE_PATHS["extract_output_dir"])),
        chunks_output_dir=_repo_path(paths_cfg.get("chunks_output_dir", DEFAULT_PIPELINE_PATHS["chunks_output_dir"])),
        chunk_results_dir=_repo_path(paths_cfg.get("chunk_results_dir", DEFAULT_PIPELINE_PATHS["chunk_results_dir"])),
        patch_output_dir=_repo_path(paths_cfg.get("patch_output_dir", DEFAULT_PIPELINE_PATHS["patch_output_dir"])),
        merged_patch=_repo_path(paths_cfg.get("merged_patch", DEFAULT_PIPELINE_PATHS["merged_patch"])),
        merge_report=_repo_path(paths_cfg.get("merge_report", DEFAULT_PIPELINE_PATHS["merge_report"])),
        apply_log=_repo_path(paths_cfg.get("apply_log", DEFAULT_PIPELINE_PATHS["apply_log"])),
        annotated_docx=_repo_path(paths_cfg.get("annotated_docx", DEFAULT_PIPELINE_PATHS["annotated_docx"])),
        changes_md=_repo_path(paths_cfg.get("changes_md", DEFAULT_PIPELINE_PATHS["changes_md"])),
        changes_json=_repo_path(paths_cfg.get("changes_json", DEFAULT_PIPELINE_PATHS["changes_json"])),
    )


def _resolve_fixture_docx(paths: PipelinePaths) -> Path:
    if paths.fixture_docx.exists():
        return paths.fixture_docx

    for candidate in sorted(paths.fixtures_dir.glob("*.docx")):
        return candidate

    raise FileNotFoundError(
        "No fixture DOCX found. Run `make fixtures` or download one manually into fixtures/."
    )


def _ensure_parent_dirs(paths: PipelinePaths) -> None:
    for folder in [
        paths.fixtures_dir,
        paths.extract_output_dir,
        paths.chunks_output_dir,
        paths.chunk_results_dir,
        paths.patch_output_dir,
        paths.apply_log.parent,
        paths.annotated_docx.parent,
        paths.changes_md.parent,
        paths.changes_json.parent,
    ]:
        folder.mkdir(parents=True, exist_ok=True)


def _run(cmd: list[str]) -> None:
    print("$", " ".join(cmd))
    subprocess.run(cmd, cwd=REPO_ROOT, check=True)


def _utf16_offsets(text: str) -> list[int]:
    offsets = [0]
    total = 0
    for char in text:
        total += len(char.encode("utf-16-le")) // 2
        offsets.append(total)
    return offsets


def _ranges_overlap(left_start: int, left_end: int, right_start: int, right_end: int) -> bool:
    left_point = left_start == left_end
    right_point = right_start == right_end

    if left_point and right_point:
        return left_start == right_start
    if left_point:
        return right_start <= left_start <= right_end
    if right_point:
        return left_start <= right_start <= left_end

    return max(left_start, right_start) < min(left_end, right_end)


def _build_ops_for_unit(unit: dict[str, Any]) -> list[dict[str, Any]] | None:
    accepted_text = str(unit.get("accepted_text", ""))
    part = str(unit.get("part", "")).strip()
    para_id = str(unit.get("para_id", "")).strip()
    unit_uid = str(unit.get("unit_uid", "")).strip()

    if not accepted_text or not part or not para_id or not unit_uid:
        return None

    matches = list(WORD_RE.finditer(accepted_text))
    if len(matches) < 4:
        return None

    counts = Counter(match.group(0) for match in matches)
    unique_matches = [match for match in matches if counts[match.group(0)] == 1]
    if len(unique_matches) < 4:
        return None

    offsets = _utf16_offsets(accepted_text)

    for indices in itertools.combinations(range(len(unique_matches)), 4):
        comment_match = unique_matches[indices[0]]
        replace_match = unique_matches[indices[1]]
        insert_match = unique_matches[indices[2]]
        delete_match = unique_matches[indices[3]]

        comment_span = (offsets[comment_match.start()], offsets[comment_match.end()])
        replace_span = (offsets[replace_match.start()], offsets[replace_match.end()])
        delete_span = (offsets[delete_match.start()], offsets[delete_match.end()])
        insert_pos = offsets[insert_match.end()]

        if _ranges_overlap(replace_span[0], replace_span[1], delete_span[0], delete_span[1]):
            continue
        if _ranges_overlap(insert_pos, insert_pos, replace_span[0], replace_span[1]):
            continue
        if _ranges_overlap(insert_pos, insert_pos, delete_span[0], delete_span[1]):
            continue

        target = {
            "part": part,
            "para_id": para_id,
            "unit_uid": unit_uid,
        }

        replace_before = replace_match.group(0)
        replacement = replace_before.upper()
        if replacement == replace_before:
            replacement = f"{replace_before}_E2E"

        ops: list[dict[str, Any]] = [
            {
                "type": "replace_range",
                "target": target,
                "range": {"start": replace_span[0], "end": replace_span[1]},
                "expected": {"snippet": replace_before},
                "replacement": replacement,
            },
            {
                "type": "insert_at",
                "target": target,
                "range": {"start": insert_pos, "end": insert_pos},
                "expected": {"snippet": ""},
                "new_text": " [E2E]",
            },
            {
                "type": "delete_range",
                "target": target,
                "range": {"start": delete_span[0], "end": delete_span[1]},
                "expected": {"snippet": delete_match.group(0)},
            },
            {
                "type": "add_comment",
                "target": target,
                "range": {"start": comment_span[0], "end": comment_span[1]},
                "expected": {"snippet": comment_match.group(0)},
                "comment_text": "E2E smoke: verify wording at this location.",
            },
        ]

        return ops

    return None


def _discover_synthetic_chunk_result(chunks_output_dir: Path, chunk_results_dir: Path) -> SyntheticChunkResult:
    manifest_path = chunks_output_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Chunk manifest missing: {manifest_path}")

    manifest = _load_json(manifest_path)
    chunk_entries = manifest.get("chunks", [])
    if not isinstance(chunk_entries, list) or not chunk_entries:
        raise ValueError("Chunk manifest has no chunks to target")

    for stale in chunk_results_dir.glob("chunk_*_result.json"):
        stale.unlink()

    for entry in chunk_entries:
        if not isinstance(entry, dict):
            continue
        chunk_file = entry.get("path")
        chunk_id = str(entry.get("chunk_id", "")).strip()
        if not chunk_file or not chunk_id:
            continue

        chunk_path = chunks_output_dir / str(chunk_file)
        if not chunk_path.exists():
            continue

        chunk_payload = _load_json(chunk_path)
        primary_units = chunk_payload.get("primary_units", [])
        if not isinstance(primary_units, list):
            continue

        for unit in primary_units:
            if not isinstance(unit, dict):
                continue
            ops = _build_ops_for_unit(unit)
            if ops is None:
                continue

            output_path = chunk_results_dir / f"{chunk_id}_result.json"
            payload = {
                "schema_version": "chunk_result.v1",
                "chunk_id": chunk_id,
                "status": "ok",
                "summary": "Synthetic e2e smoke operations generated from primary_units text.",
                "ops": ops,
                "suggestions": [
                    "synthetic_ops_generated_from_unique_text",
                ],
            }
            _dump_json(output_path, payload)
            return SyntheticChunkResult(chunk_id=chunk_id, output_path=output_path, op_count=len(ops))

    raise RuntimeError(
        "Could not build synthetic chunk_result ops from chunk primary_units. "
        "Try a different fixture DOCX with longer paragraph text."
    )


def _assert_outputs(paths: PipelinePaths) -> None:
    required = [
        paths.merged_patch,
        paths.merge_report,
        paths.apply_log,
        paths.annotated_docx,
        paths.changes_md,
        paths.changes_json,
    ]
    missing = [path for path in required if not path.exists()]
    if missing:
        missing_lines = "\n".join(f"- {path}" for path in missing)
        raise RuntimeError(f"E2E finished with missing artifacts:\n{missing_lines}")


def run_full_pipeline(paths: PipelinePaths, fixture_docx: Path, author: str) -> SyntheticChunkResult:
    extract_review_units = paths.extract_output_dir / "review_units.json"
    extract_linear_units = paths.extract_output_dir / "linear_units.json"
    extract_docx_struct = paths.extract_output_dir / "docx_struct.json"

    _run(
        [
            sys.executable,
            str(EXTRACT_SCRIPT),
            "--input-docx",
            str(fixture_docx),
            "--output-dir",
            str(paths.extract_output_dir),
        ]
    )

    _run(
        [
            sys.executable,
            str(CHUNK_SCRIPT),
            "--constants",
            str(paths.constants),
            "--review-units",
            str(extract_review_units),
            "--linear-units",
            str(extract_linear_units),
            "--docx-struct",
            str(extract_docx_struct),
            "--output-dir",
            str(paths.chunks_output_dir),
        ]
    )

    synthetic = _discover_synthetic_chunk_result(paths.chunks_output_dir, paths.chunk_results_dir)

    _run(
        [
            sys.executable,
            str(MERGE_SCRIPT),
            "--chunk-results-dir",
            str(paths.chunk_results_dir),
            "--linear-units",
            str(extract_linear_units),
            "--output-dir",
            str(paths.patch_output_dir),
            "--author",
            author,
        ]
    )

    _run(
        [
            sys.executable,
            str(APPLY_SCRIPT),
            "--input-docx",
            str(fixture_docx),
            "--patch",
            str(paths.merged_patch),
            "--review-units",
            str(extract_review_units),
            "--output-docx",
            str(paths.annotated_docx),
            "--apply-log",
            str(paths.apply_log),
            "--author",
            author,
        ]
    )

    _run(
        [
            sys.executable,
            str(REPORT_SCRIPT),
            "--review-units",
            str(extract_review_units),
            "--patch",
            str(paths.merged_patch),
            "--apply-log",
            str(paths.apply_log),
            "--output-md",
            str(paths.changes_md),
            "--output-json",
            str(paths.changes_json),
        ]
    )

    _assert_outputs(paths)
    return synthetic


def main() -> int:
    args = _build_parser().parse_args()

    try:
        constants = _load_constants(args.constants)
        paths = _resolve_paths(args.constants, constants)
        _ensure_parent_dirs(paths)

        if args.only_generate_synthetic:
            result = _discover_synthetic_chunk_result(paths.chunks_output_dir, paths.chunk_results_dir)
            print(f"Synthetic chunk result: {result.output_path} (ops={result.op_count})")
            return 0

        fixture_docx = _resolve_fixture_docx(paths)
        result = run_full_pipeline(paths=paths, fixture_docx=fixture_docx, author=str(args.author))

        apply_log = _load_json(paths.apply_log)
        stats = apply_log.get("stats", {}) if isinstance(apply_log.get("stats"), dict) else {}
        print("E2E completed successfully.")
        print(f"Fixture: {fixture_docx}")
        print(f"Synthetic chunk result: {result.output_path} (ops={result.op_count})")
        print(f"Annotated DOCX: {paths.annotated_docx}")
        print(f"Change report: {paths.changes_md}")
        print(
            "Apply stats: "
            f"input={stats.get('input_ops', 'n/a')} "
            f"applied={stats.get('applied_ops', 'n/a')} "
            f"skipped={stats.get('skipped_ops', 'n/a')}"
        )
        return 0
    except subprocess.CalledProcessError as exc:
        print(f"Command failed with exit code {exc.returncode}: {exc.cmd}", file=sys.stderr)
        return exc.returncode
    except Exception as exc:  # pragma: no cover - top-level guard
        print(f"E2E failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
