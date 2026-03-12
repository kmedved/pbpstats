#!/usr/bin/env python
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.context_framework import (
    BUNDLE_CONTRACT,
    render_bundle,
    render_checked_in_artifacts,
    render_context_budget,
)


def main() -> int:
    context_dir = REPO_ROOT / "context"
    context_dir.mkdir(parents=True, exist_ok=True)

    checked_in = render_checked_in_artifacts()
    architecture_text = checked_in["context/REPO_ARCHITECTURE.md"]

    bundle_texts = {}
    for bundle in BUNDLE_CONTRACT["bundles"]:
        bundle_text = render_bundle(bundle["name"], bundle["purpose"])
        bundle_texts[bundle["name"]] = bundle_text
        (context_dir / bundle["name"]).write_text(bundle_text, encoding="utf-8")

    budget_text = render_context_budget(architecture_text, bundle_texts, include_src=False)
    (context_dir / "CONTEXT_BUDGET.md").write_text(budget_text, encoding="utf-8")

    stale_index = context_dir / "CONTEXT_INDEX.md"
    if stale_index.exists():
        stale_index.unlink()

    return 0


if __name__ == "__main__":
    sys.exit(main())
