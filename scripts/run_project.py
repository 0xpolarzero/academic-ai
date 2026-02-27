#!/usr/bin/env python3
"""Run project workflow pipeline with dry-run synthetic chunk results support."""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass
import itertools
from pathlib import Path
import json
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
VALIDATE_SCRIPT = REPO_ROOT / "scripts/validate_dry_run_outputs.py"

WORD_RE = re.compile(r"[A-Za-z][A-Za-z'-]{3,}")


@dataclass(frozen=True)
class ProjectPaths:
    project_dir: Path
    workflow_xml: Path
    source_docx: Path
    constants: Path
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
    parser.add_argument("--project", required=True, help="Project slug under projects/")
    parser.add_argument("--workflow", required=True, help="Workflow name in projects/<project>/workflows/<name>.xml")
    parser.add_argument("--constants", type=Path, default=Path("config/constants.json"), help="Path to constants JSON")
    parser.add_argument("--author", default="phase3-runner", help="Author value used in merge/apply artifacts")
    parser.add_argument("--dry-run", action="store_true", help="Generate synthetic chunk results instead of model outputs")
    parser.add_argument("--skip-validation", action="store_true", help="Skip QA acceptance checks at the end")
    return parser


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _dump_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


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
            replacement = f"{replace_before}_DRYRUN"

        return [
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
                "new_text": " [DRY-RUN]",
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
                "comment_text": "Dry-run QA marker comment.",
            },
        ]

    return None


def _discover_synthetic_chunk_result(paths: ProjectPaths) -> SyntheticChunkResult:
    manifest_path = paths.chunks_output_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Chunk manifest missing: {manifest_path}")

    manifest = _load_json(manifest_path)
    chunk_entries = manifest.get("chunks", [])
    if not isinstance(chunk_entries, list) or not chunk_entries:
        raise RuntimeError("Chunk manifest has no chunks")

    for stale in paths.chunk_results_dir.glob("chunk_*_result.json"):
        stale.unlink()

    for entry in chunk_entries:
        if not isinstance(entry, dict):
            continue
        chunk_file = entry.get("path")
        chunk_id = str(entry.get("chunk_id", "")).strip()
        if not chunk_file or not chunk_id:
            continue

        chunk_path = paths.chunks_output_dir / str(chunk_file)
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

            output_path = paths.chunk_results_dir / f"{chunk_id}_result.json"
            _dump_json(
                output_path,
                {
                    "schema_version": "chunk_result.v1",
                    "chunk_id": chunk_id,
                    "status": "ok",
                    "summary": "Synthetic dry-run operations generated from primary_units text.",
                    "ops": ops,
                    "suggestions": ["dry_run_synthetic_ops"],
                },
            )
            return SyntheticChunkResult(chunk_id=chunk_id, output_path=output_path, op_count=len(ops))

    raise RuntimeError("Unable to generate synthetic chunk results from chunk primary_units")


def _resolve_paths(args: argparse.Namespace) -> ProjectPaths:
    project_dir = (REPO_ROOT / "projects" / str(args.project)).resolve()
    workflow_xml = (project_dir / "workflows" / f"{args.workflow}.xml").resolve()

    return ProjectPaths(
        project_dir=project_dir,
        workflow_xml=workflow_xml,
        source_docx=(project_dir / "input" / "source.docx").resolve(),
        constants=(Path(args.constants).expanduser().resolve() if Path(args.constants).is_absolute() else (REPO_ROOT / Path(args.constants)).resolve()),
        extract_output_dir=(project_dir / "artifacts" / "docx_extract").resolve(),
        chunks_output_dir=(project_dir / "artifacts" / "chunks").resolve(),
        chunk_results_dir=(project_dir / "artifacts" / "chunk_results").resolve(),
        patch_output_dir=(project_dir / "artifacts" / "patch").resolve(),
        merged_patch=(project_dir / "artifacts" / "patch" / "merged_patch.json").resolve(),
        merge_report=(project_dir / "artifacts" / "patch" / "merge_report.json").resolve(),
        apply_log=(project_dir / "artifacts" / "apply" / "apply_log.json").resolve(),
        annotated_docx=(project_dir / "output" / "annotated.docx").resolve(),
        changes_md=(project_dir / "output" / "changes.md").resolve(),
        changes_json=(project_dir / "output" / "changes.json").resolve(),
    )


def _ensure_project_prereqs(paths: ProjectPaths) -> None:
    if not paths.project_dir.exists() or not paths.project_dir.is_dir():
        raise FileNotFoundError(f"Project directory not found: {paths.project_dir}")
    if not paths.workflow_xml.exists():
        raise FileNotFoundError(f"Workflow XML not found: {paths.workflow_xml}")
    if not paths.source_docx.exists():
        raise FileNotFoundError(f"Source DOCX not found: {paths.source_docx}")
    if not paths.constants.exists():
        raise FileNotFoundError(f"constants.json not found: {paths.constants}")

    for path in [
        paths.extract_output_dir,
        paths.chunks_output_dir,
        paths.chunk_results_dir,
        paths.patch_output_dir,
        paths.apply_log.parent,
        paths.annotated_docx.parent,
        paths.changes_md.parent,
    ]:
        path.mkdir(parents=True, exist_ok=True)


def _assert_outputs(paths: ProjectPaths) -> None:
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
        formatted = "\n".join(f"- {path}" for path in missing)
        raise RuntimeError(f"Pipeline finished with missing outputs:\n{formatted}")


def run_pipeline(paths: ProjectPaths, *, author: str, dry_run: bool, validate: bool) -> SyntheticChunkResult | None:
    _run(
        [
            sys.executable,
            str(EXTRACT_SCRIPT),
            "--project-dir",
            str(paths.project_dir),
            "--input-docx",
            "input/source.docx",
            "--output-dir",
            "artifacts/docx_extract",
        ]
    )

    _run(
        [
            sys.executable,
            str(CHUNK_SCRIPT),
            "--project-dir",
            str(paths.project_dir),
            "--constants",
            str(paths.constants),
            "--review-units",
            "artifacts/docx_extract/review_units.json",
            "--linear-units",
            "artifacts/docx_extract/linear_units.json",
            "--docx-struct",
            "artifacts/docx_extract/docx_struct.json",
            "--output-dir",
            "artifacts/chunks",
        ]
    )

    synthetic: SyntheticChunkResult | None = None
    if dry_run:
        synthetic = _discover_synthetic_chunk_result(paths)
        print(f"Synthetic chunk result: {synthetic.output_path} (ops={synthetic.op_count})")

    _run(
        [
            sys.executable,
            str(MERGE_SCRIPT),
            "--project-dir",
            str(paths.project_dir),
            "--chunk-results-dir",
            "artifacts/chunk_results",
            "--linear-units",
            "artifacts/docx_extract/linear_units.json",
            "--chunks-manifest",
            "artifacts/chunks/manifest.json",
            "--output-dir",
            "artifacts/patch",
            "--author",
            author,
        ]
    )

    _run(
        [
            sys.executable,
            str(APPLY_SCRIPT),
            "--project-dir",
            str(paths.project_dir),
            "--input-docx",
            "input/source.docx",
            "--patch",
            "artifacts/patch/merged_patch.json",
            "--review-units",
            "artifacts/docx_extract/review_units.json",
            "--output-docx",
            "output/annotated.docx",
            "--apply-log",
            "artifacts/apply/apply_log.json",
            "--author",
            author,
        ]
    )

    _run(
        [
            sys.executable,
            str(REPORT_SCRIPT),
            "--project-dir",
            str(paths.project_dir),
            "--review-units",
            "artifacts/docx_extract/review_units.json",
            "--patch",
            "artifacts/patch/merged_patch.json",
            "--apply-log",
            "artifacts/apply/apply_log.json",
            "--output-md",
            "output/changes.md",
            "--output-json",
            "output/changes.json",
        ]
    )

    _assert_outputs(paths)

    if validate:
        _run([sys.executable, str(VALIDATE_SCRIPT), "--project-dir", str(paths.project_dir)])

    return synthetic


def main() -> int:
    args = _build_parser().parse_args()

    try:
        paths = _resolve_paths(args)
        _ensure_project_prereqs(paths)

        if not args.dry_run:
            if not any(paths.chunk_results_dir.glob("chunk_*_result.json")):
                raise RuntimeError(
                    "No chunk results found for non-dry run. Use --dry-run or provide chunk reviewer outputs in artifacts/chunk_results/."
                )
        else:
            for stale in paths.chunk_results_dir.glob("chunk_*_result.json"):
                stale.unlink()

        synthetic = run_pipeline(
            paths,
            author=str(args.author),
            dry_run=bool(args.dry_run),
            validate=not bool(args.skip_validation),
        )

        print("Project run completed successfully.")
        print(f"Project: {paths.project_dir}")
        print(f"Workflow: {paths.workflow_xml.name}")
        print(f"Annotated DOCX: {paths.annotated_docx}")
        print(f"Change report: {paths.changes_md}")
        if synthetic is not None:
            print(f"Dry-run synthetic chunk result: {synthetic.output_path}")

        return 0
    except subprocess.CalledProcessError as exc:
        print(f"Command failed with exit code {exc.returncode}: {exc.cmd}", file=sys.stderr)
        return exc.returncode
    except Exception as exc:
        print(f"Project run failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
