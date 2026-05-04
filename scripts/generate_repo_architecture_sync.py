#!/usr/bin/env python
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.context_framework import render_checked_in_artifacts, write_artifacts


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate optional context artifacts under context/."
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if current context artifacts differ from the renderer.",
    )
    args = parser.parse_args()

    artifacts = render_checked_in_artifacts()
    if args.check:
        for relative_path, contents in artifacts.items():
            full_path = REPO_ROOT / relative_path
            if not full_path.exists() or full_path.read_text(encoding="utf-8") != contents:
                print("Out of date: %s" % relative_path)
                return 1
        return 0

    write_artifacts(artifacts)
    return 0


if __name__ == "__main__":
    sys.exit(main())
