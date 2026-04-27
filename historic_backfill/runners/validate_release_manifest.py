"""Validate committed historic backfill release manifests."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]


def _load_json(path: Path) -> dict[str, Any]:
    with path.open() as f:
        return json.load(f)


def _load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _as_bool(value: object) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes"}


def validate_checksums(release_dir: Path) -> list[str]:
    checksum_path = release_dir / "checksums.sha256"
    errors: list[str] = []
    if not checksum_path.exists():
        return [f"checksum file is missing: {checksum_path}"]

    for line_number, raw_line in enumerate(checksum_path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            expected_hash, raw_rel_path = line.split(maxsplit=1)
        except ValueError:
            errors.append(f"checksum line {line_number} is malformed")
            continue
        rel_path = raw_rel_path.strip()
        if rel_path in {"checksums.sha256", "./checksums.sha256"}:
            errors.append("checksums.sha256 must not include its own checksum")
            continue
        artifact_path = (release_dir / rel_path).resolve()
        try:
            artifact_path.relative_to(release_dir.resolve())
        except ValueError:
            errors.append(f"checksum line {line_number} points outside release dir: {rel_path}")
            continue
        if not artifact_path.exists():
            errors.append(f"checksum artifact missing: {rel_path}")
            continue
        observed_hash = hashlib.sha256(artifact_path.read_bytes()).hexdigest()
        if observed_hash != expected_hash:
            errors.append(f"checksum mismatch for {rel_path}: expected {expected_hash}, observed {observed_hash}")
    return errors


def _validate_sidecar_contract(release_dir: Path) -> list[str]:
    errors: list[str] = []
    sidecar_dir = release_dir / "sidecar"
    summary = _load_json(sidecar_dir / "summary.json")
    join_contract = _load_json(sidecar_dir / "join_contract.json")
    if summary.get("default_absent_row_values") != join_contract.get("default_absent_row_values"):
        errors.append("sidecar summary default_absent_row_values do not match join contract")

    rows = _load_csv(sidecar_dir / "game_quality_sparse.csv")
    game_ids = [row.get("game_id", "") for row in rows]
    if len(game_ids) != len(set(game_ids)):
        errors.append("sidecar game_quality_sparse.csv contains duplicate game_id rows")
    blocking_ids = [row["game_id"] for row in rows if _as_bool(row.get("blocks_release"))]
    if blocking_ids:
        errors.append(f"sidecar contains release-blocking game rows: {blocking_ids}")
    research_open_ids = [row["game_id"] for row in rows if _as_bool(row.get("research_open"))]
    if research_open_ids:
        errors.append(f"sidecar contains research-open game rows: {research_open_ids}")
    documented_hold_ids = [row["game_id"] for row in rows if row.get("execution_lane") == "documented_hold"]
    if documented_hold_ids:
        errors.append(f"sidecar contains documented_hold execution lanes: {documented_hold_ids}")

    overlay_rows = _load_csv(
        release_dir
        / "policy"
        / "reviewed_frontier_policy_overlay_20260424_mechanics_fullrun_v4.csv"
    )
    if len(overlay_rows) != 13:
        errors.append(f"reviewed policy overlay row count is {len(overlay_rows)}, expected 13")
    reviewed_ids = {row["game_id"] for row in overlay_rows}
    sidecar_reviewed_ids = {
        row["game_id"] for row in rows if row.get("policy_source") == "reviewed_override"
    }
    missing_reviewed_ids = sorted(reviewed_ids - sidecar_reviewed_ids)
    if missing_reviewed_ids:
        errors.append(f"reviewed policy games missing from sidecar: {missing_reviewed_ids}")
    if summary.get("reviewed_override_game_count") != len(sidecar_reviewed_ids):
        errors.append("sidecar reviewed_override_game_count does not match reviewed sidecar rows")
    return errors


def _validate_path_policy(manifest: dict[str, Any], release_dir: Path) -> list[str]:
    errors: list[str] = []
    path_policy = manifest.get("path_policy", {})
    original_root = path_policy.get("original_workspace_root")
    if not original_root:
        return errors

    overlay_path = (
        release_dir
        / "policy"
        / "reviewed_frontier_policy_overlay_20260424_mechanics_fullrun_v4.csv"
    )
    overlay_has_original_paths = any(
        original_root in row.get("evidence_artifact", "")
        for row in _load_csv(overlay_path)
    )
    if overlay_has_original_paths and path_policy.get("policy_overlay_evidence_artifact") is None:
        errors.append("path_policy does not document policy overlay evidence_artifact absolute paths")

    sidecar_summary = _load_json(release_dir / "sidecar" / "summary.json")
    sidecar_has_original_paths = any(
        original_root in str(path)
        for path in sidecar_summary.get("residual_dirs", [])
    )
    if sidecar_has_original_paths and path_policy.get("sidecar_summary_residual_dirs") is None:
        errors.append("path_policy does not document sidecar summary residual_dirs absolute paths")
    return errors


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

    raw_open_games = (
        raw.get("quality_status_counts", {}).get("open")
        or raw.get("raw_quality_status_counts", {}).get("open")
    )
    checks = {
        "failed_games": full_history.get("failed_games"),
        "event_stats_errors": full_history.get("event_stats_errors"),
        "raw_open_games": raw_open_games,
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

    errors.extend(validate_checksums(release_dir))
    errors.extend(_validate_sidecar_contract(release_dir))
    errors.extend(_validate_path_policy(manifest, release_dir))

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
