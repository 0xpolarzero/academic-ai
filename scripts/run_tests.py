#!/usr/bin/env python3
"""Run project unit + integration tests."""

from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--tests-path",
        default="tests",
        help="Path to pass to pytest (default: tests)",
    )
    parser.add_argument(
        "pytest_args",
        nargs=argparse.REMAINDER,
        help="Additional args forwarded to pytest. Prefix with --, e.g. -- -k merge",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()

    extra_args = list(args.pytest_args)
    if extra_args and extra_args[0] == "--":
        extra_args = extra_args[1:]

    cmd = [sys.executable, "-m", "pytest", args.tests_path, *extra_args]
    print("$", " ".join(cmd))

    completed = subprocess.run(cmd, cwd=REPO_ROOT)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
