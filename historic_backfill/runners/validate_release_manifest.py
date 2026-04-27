"""Validate committed historic backfill release manifests."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]


def _load_json(path: Path) -> dict[str, Any]:
    with path.open() as f:
        return json.load(f)


def validate_manifest(manifest_path: Path) -> list[str]:
    manifest = _load_json(manifest_path)
    release_dir = manifest_path.parent
    errors: list[str] = []

    expected_tag = "historic-backfill-v4-1997-2020-20260424"
    if manifest.get("git", {}).get("release_tag") != expected_tag:
        errors.append("manifest git.release_tag does not match expected v4 tag")
    if "integrated_repo_commit" in manifest.get("git", {}):
        errors.append("manifest must not embed a self-referential integrated repo commit SHA")

    seasons = manifest.get("seasons")
    if seasons != list(range(1997, 2021)):
        errors.append("manifest seasons must be 1997 through 2020")

    validation = manifest.get("validation", {})
    full_history = _load_json(
        release_dir / "summaries" / "original" / "full_history_summary.original.json"
    )
    reviewed = _load_json(
        release_dir / "summaries" / "original" / "reviewed_residuals_summary.original.json"
    )
    raw = _load_json(
        release_dir / "summaries" / "original" / "raw_residuals_summary.original.json"
    )
    sidecar = _load_json(release_dir / "sidecar" / "summary.json")

    checks = {
        "failed_games": full_history.get("failed_games"),
        "event_stats_errors": full_history.get("event_stats_errors"),
        "raw_open_games": raw.get("release_blocking_game_count"),
        "reviewed_release_blocking_game_count": reviewed.get(
            "release_blocking_game_count"
        ),
        "reviewed_research_open_game_count": reviewed.get("research_open_game_count"),
        "tier1_release_ready": reviewed.get("tier1_release_ready"),
        "tier2_frontier_closed": reviewed.get("tier2_frontier_closed"),
        "reviewed_override_game_count": sidecar.get("reviewed_override_game_count"),
    }
    for key, observed in checks.items():
        expected = validation.get(key)
        if observed != expected:
            errors.append(
                f"manifest validation.{key}={expected!r} does not match observed {observed!r}"
            )

    for label, rel_path in manifest.get("authoritative_files", {}).items():
        artifact_path = REPO_ROOT / rel_path
        if not artifact_path.exists():
            errors.append(f"authoritative file {label} is missing: {rel_path}")

    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    args = parser.parse_args(argv)

    errors = validate_manifest(args.manifest)
    if errors:
        for error in errors:
            print(error)
        return 1
    print(f"manifest ok: {args.manifest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
