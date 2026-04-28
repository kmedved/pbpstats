"""Validate committed historic backfill release manifests."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any

from historic_backfill.catalogs.boxscore_source_overrides import (
    validate_boxscore_source_overrides,
)
from historic_backfill.catalogs.lineup_correction_manifest import (
    validate_compiled_runtime_views,
    validate_manifest_schema,
)
from historic_backfill.catalogs.loader import validate_historic_pbp_row_override_catalog
from historic_backfill.catalogs.pbp_stat_overrides import load_pbp_stat_overrides
from historic_backfill.catalogs.validation_overrides import validate_validation_overrides
from pbpstats.offline.row_overrides import normalize_game_id


REPO_ROOT = Path(__file__).resolve().parents[2]
SIDECAR_REQUIRED_COLUMNS = {
    "game_id",
    "blocks_release",
    "research_open",
    "execution_lane",
    "policy_source",
    "primary_quality_status",
    "release_gate_status",
    "release_reason_code",
}
ALLOWED_RELEASE_GATE_STATUSES = {
    "accepted_boundary_difference",
    "accepted_unresolvable_contradiction",
    "exact",
    "override_corrected",
    "source_limited_upstream_error",
}
ALLOWED_EXECUTION_LANES = {
    "exact",
    "local_override_chosen",
    "override_corrected",
    "policy_frontier_non_local",
    "policy_overlay_chosen",
    "source_limited",
    "status_quo_chosen",
    "synthetic_sub_chosen",
}
ALLOWED_POLICY_SOURCES = {"auto_default", "reviewed_override"}
ALLOWED_PRIMARY_QUALITY_STATUSES = {
    "boundary_difference",
    "exact",
    "open",
    "override_corrected",
    "source_limited",
}
CATALOG_SNAPSHOT_FILES = {
    "catalog_snapshot/pbp_row_overrides.csv": (
        "historic_backfill/catalogs/pbp_row_overrides.csv"
    ),
    "catalog_snapshot/pbp_stat_overrides.csv": (
        "historic_backfill/catalogs/pbp_stat_overrides.csv"
    ),
    "catalog_snapshot/validation_overrides.csv": (
        "historic_backfill/catalogs/validation_overrides.csv"
    ),
    "catalog_snapshot/boxscore_source_overrides.csv": (
        "historic_backfill/catalogs/boxscore_source_overrides.csv"
    ),
    "catalog_snapshot/overrides/correction_manifest.json": (
        "historic_backfill/catalogs/overrides/correction_manifest.json"
    ),
    "catalog_snapshot/overrides/period_starters_overrides.json": (
        "historic_backfill/catalogs/overrides/period_starters_overrides.json"
    ),
    "catalog_snapshot/overrides/lineup_window_overrides.json": (
        "historic_backfill/catalogs/overrides/lineup_window_overrides.json"
    ),
}


def _load_json(path: Path) -> dict[str, Any]:
    with path.open() as f:
        return json.load(f)


def _load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _load_csv_with_header(path: Path) -> tuple[list[dict[str, str]], set[str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        return rows, set(reader.fieldnames or [])


def _as_bool(value: object, *, allow_blank: bool = False) -> bool:
    text = str(value if value is not None else "").strip().lower()
    if not text and not allow_blank:
        raise ValueError("blank boolean value")
    if text in {"1", "true", "yes"}:
        return True
    if text in {"0", "false", "no"} or (allow_blank and text == ""):
        return False
    raise ValueError(f"invalid boolean value: {value!r}")


def _true_game_ids(
    rows: list[dict[str, str]], field: str
) -> tuple[list[str], list[str]]:
    game_ids: list[str] = []
    errors: list[str] = []
    for row_number, row in enumerate(rows, start=2):
        try:
            is_true = _as_bool(row.get(field))
        except ValueError as exc:
            errors.append(f"sidecar row {row_number} field {field}: {exc}")
            continue
        if is_true:
            game_ids.append(row.get("game_id", ""))
    return game_ids, errors


def _validate_sidecar_game_ids(rows: list[dict[str, str]]) -> list[str]:
    errors: list[str] = []
    normalized_ids: list[str] = []
    for row_number, row in enumerate(rows, start=2):
        raw = str(row.get("game_id", ""))
        try:
            normalized = normalize_game_id(raw)
        except ValueError as exc:
            errors.append(f"sidecar row {row_number} invalid game_id={raw!r}: {exc}")
            continue
        if raw != normalized:
            errors.append(
                f"sidecar row {row_number} non-canonical game_id={raw!r}; "
                f"expected {normalized!r}"
            )
        normalized_ids.append(normalized)

    duplicate_normalized = sorted(
        game_id for game_id, count in Counter(normalized_ids).items() if count > 1
    )
    if duplicate_normalized:
        errors.append(
            f"sidecar duplicate normalized game_id rows: {duplicate_normalized}"
        )
    return errors


def _validate_csv_game_ids(rows: list[dict[str, str]], *, label: str) -> list[str]:
    errors: list[str] = []
    normalized_ids: list[str] = []
    for row_number, row in enumerate(rows, start=2):
        raw = str(row.get("game_id", ""))
        try:
            normalized = normalize_game_id(raw)
        except ValueError as exc:
            errors.append(f"{label} row {row_number} invalid game_id={raw!r}: {exc}")
            continue
        if raw != normalized:
            errors.append(
                f"{label} row {row_number} non-canonical game_id={raw!r}; "
                f"expected {normalized!r}"
            )
        normalized_ids.append(normalized)

    duplicate_normalized = sorted(
        game_id for game_id, count in Counter(normalized_ids).items() if count > 1
    )
    if duplicate_normalized:
        errors.append(f"{label} duplicate normalized game_id rows: {duplicate_normalized}")
    return errors


def _validate_enum_values(
    rows: list[dict[str, str]],
    *,
    label: str,
    field: str,
    allowed_values: set[str],
) -> list[str]:
    errors: list[str] = []
    for row_number, row in enumerate(rows, start=2):
        value = str(row.get(field, ""))
        if value not in allowed_values:
            errors.append(
                f"{label} row {row_number} has invalid {field}: {value!r}"
            )
    return errors


def _raw_open_game_count(raw_summary: dict[str, Any]) -> Any:
    value = raw_summary.get("quality_status_counts", {}).get("open")
    if value is None:
        value = raw_summary.get("raw_quality_status_counts", {}).get("open")
    return value


def _normalize_checksum_rel_path(raw_rel_path: str) -> str:
    return raw_rel_path.strip().removeprefix("./")


def _checksum_paths(release_dir: Path) -> set[str]:
    checksum_path = release_dir / "checksums.sha256"
    if not checksum_path.exists():
        return set()

    paths: set[str] = set()
    for raw_line in checksum_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            _expected_hash, raw_rel_path = line.split(maxsplit=1)
        except ValueError:
            continue
        paths.add(_normalize_checksum_rel_path(raw_rel_path))
    return paths


def validate_checksums(release_dir: Path) -> list[str]:
    checksum_path = release_dir / "checksums.sha256"
    errors: list[str] = []
    if not checksum_path.exists():
        return [f"checksum file is missing: {checksum_path}"]

    for line_number, raw_line in enumerate(
        checksum_path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            expected_hash, raw_rel_path = line.split(maxsplit=1)
        except ValueError:
            errors.append(f"checksum line {line_number} is malformed")
            continue
        rel_path = _normalize_checksum_rel_path(raw_rel_path)
        if rel_path == "checksums.sha256":
            errors.append("checksums.sha256 must not include its own checksum")
            continue
        artifact_path = (release_dir / rel_path).resolve()
        try:
            artifact_path.relative_to(release_dir.resolve())
        except ValueError:
            errors.append(
                f"checksum line {line_number} points outside release dir: {rel_path}"
            )
            continue
        if not artifact_path.exists():
            errors.append(f"checksum artifact missing: {rel_path}")
            continue
        observed_hash = hashlib.sha256(artifact_path.read_bytes()).hexdigest()
        if observed_hash != expected_hash:
            errors.append(
                f"checksum mismatch for {rel_path}: expected {expected_hash}, observed {observed_hash}"
            )
    return errors


def _validate_sidecar_contract(release_dir: Path) -> list[str]:
    errors: list[str] = []
    sidecar_dir = release_dir / "sidecar"
    summary = _load_json(sidecar_dir / "summary.json")
    join_contract = _load_json(sidecar_dir / "join_contract.json")
    if summary.get("default_absent_row_values") != join_contract.get(
        "default_absent_row_values"
    ):
        errors.append(
            "sidecar summary default_absent_row_values do not match join contract"
        )

    rows, sidecar_columns = _load_csv_with_header(
        sidecar_dir / "game_quality_sparse.csv"
    )
    missing_columns = SIDECAR_REQUIRED_COLUMNS - sidecar_columns
    if missing_columns:
        errors.append(
            f"sidecar game_quality_sparse.csv missing columns: {sorted(missing_columns)}"
        )
    game_ids = [row.get("game_id", "") for row in rows]
    errors.extend(_validate_sidecar_game_ids(rows))
    if len(game_ids) != len(set(game_ids)):
        errors.append("sidecar game_quality_sparse.csv contains duplicate game_id rows")
    errors.extend(
        _validate_enum_values(
            rows,
            label="sidecar",
            field="release_gate_status",
            allowed_values=ALLOWED_RELEASE_GATE_STATUSES,
        )
    )
    errors.extend(
        _validate_enum_values(
            rows,
            label="sidecar",
            field="execution_lane",
            allowed_values=ALLOWED_EXECUTION_LANES,
        )
    )
    errors.extend(
        _validate_enum_values(
            rows,
            label="sidecar",
            field="policy_source",
            allowed_values=ALLOWED_POLICY_SOURCES,
        )
    )
    errors.extend(
        _validate_enum_values(
            rows,
            label="sidecar",
            field="primary_quality_status",
            allowed_values=ALLOWED_PRIMARY_QUALITY_STATUSES,
        )
    )
    blocking_ids, bool_errors = _true_game_ids(rows, "blocks_release")
    errors.extend(bool_errors)
    if blocking_ids:
        errors.append(f"sidecar contains release-blocking game rows: {blocking_ids}")
    research_open_ids, bool_errors = _true_game_ids(rows, "research_open")
    errors.extend(bool_errors)
    if research_open_ids:
        errors.append(f"sidecar contains research-open game rows: {research_open_ids}")
    documented_hold_ids = [
        row["game_id"] for row in rows if row.get("execution_lane") == "documented_hold"
    ]
    if documented_hold_ids:
        errors.append(
            f"sidecar contains documented_hold execution lanes: {documented_hold_ids}"
        )
    if summary.get("row_count") != len(rows):
        errors.append("sidecar row_count does not match CSV row count")
    if summary.get("unique_game_count") != len(set(game_ids)):
        errors.append("sidecar unique_game_count does not match unique game_id count")
    if summary.get("release_blocking_game_count") != len(blocking_ids):
        errors.append("sidecar release_blocking_game_count does not match rows")
    if summary.get("research_open_game_count") != len(research_open_ids):
        errors.append("sidecar research_open_game_count does not match rows")
    if sorted(summary.get("release_blocking_game_ids", [])) != sorted(blocking_ids):
        errors.append("sidecar release_blocking_game_ids do not match CSV rows")
    if sorted(summary.get("research_open_game_ids", [])) != sorted(research_open_ids):
        errors.append("sidecar research_open_game_ids do not match CSV rows")
    for field in (
        "execution_lane",
        "policy_source",
        "release_gate_status",
    ):
        expected_counts = dict(Counter(row.get(field, "") for row in rows))
        summary_key = f"{field}_counts"
        if summary.get(summary_key) != expected_counts:
            errors.append(f"sidecar {summary_key} do not match CSV rows")

    overlay_rows = _load_csv(
        release_dir
        / "policy"
        / "reviewed_frontier_policy_overlay_20260424_mechanics_fullrun_v4.csv"
    )
    errors.extend(_validate_csv_game_ids(overlay_rows, label="reviewed policy overlay"))
    errors.extend(
        _validate_enum_values(
            overlay_rows,
            label="reviewed policy overlay",
            field="release_gate_status",
            allowed_values=ALLOWED_RELEASE_GATE_STATUSES,
        )
    )
    errors.extend(
        _validate_enum_values(
            overlay_rows,
            label="reviewed policy overlay",
            field="execution_lane",
            allowed_values=ALLOWED_EXECUTION_LANES,
        )
    )
    errors.extend(
        _validate_enum_values(
            overlay_rows,
            label="reviewed policy overlay",
            field="policy_source",
            allowed_values=ALLOWED_POLICY_SOURCES,
        )
    )
    if len(overlay_rows) != 13:
        errors.append(
            f"reviewed policy overlay row count is {len(overlay_rows)}, expected 13"
        )
    reviewed_ids = {row["game_id"] for row in overlay_rows}
    sidecar_reviewed_ids = {
        row["game_id"]
        for row in rows
        if row.get("policy_source") == "reviewed_override"
    }
    missing_reviewed_ids = sorted(reviewed_ids - sidecar_reviewed_ids)
    if missing_reviewed_ids:
        errors.append(
            f"reviewed policy games missing from sidecar: {missing_reviewed_ids}"
        )
    extra_reviewed_ids = sorted(sidecar_reviewed_ids - reviewed_ids)
    if extra_reviewed_ids:
        errors.append(
            f"sidecar has reviewed_override games not in overlay: {extra_reviewed_ids}"
        )
    if sidecar_reviewed_ids != reviewed_ids:
        errors.append(
            "sidecar reviewed_override game set does not match reviewed policy overlay"
        )
    if summary.get("reviewed_override_game_count") != len(reviewed_ids):
        errors.append(
            "sidecar reviewed_override_game_count does not match reviewed policy overlay"
        )
    sidecar_by_game_id = {row.get("game_id", ""): row for row in rows}
    overlay_compare_fields = (
        "release_gate_status",
        "release_reason_code",
        "execution_lane",
        "blocks_release",
        "research_open",
        "policy_source",
    )
    for overlay_row in overlay_rows:
        game_id = overlay_row.get("game_id", "")
        sidecar_row = sidecar_by_game_id.get(game_id)
        if sidecar_row is None:
            continue
        for field in overlay_compare_fields:
            if str(sidecar_row.get(field, "")) != str(overlay_row.get(field, "")):
                errors.append(
                    f"reviewed overlay field mismatch for {game_id} {field}: "
                    f"sidecar={sidecar_row.get(field, '')!r}, "
                    f"overlay={overlay_row.get(field, '')!r}"
                )
        sidecar_primary = str(sidecar_row.get("primary_quality_status", ""))
        overlay_primary = str(overlay_row.get("expected_primary_quality_status", ""))
        if sidecar_primary != overlay_primary:
            errors.append(
                f"reviewed overlay field mismatch for {game_id} primary_quality_status: "
                f"sidecar={sidecar_primary!r}, "
                f"overlay expected={overlay_primary!r}"
            )
    return errors


def _validate_authoritative_checksum_coverage(
    manifest: dict[str, Any],
    release_dir: Path,
) -> list[str]:
    errors: list[str] = []
    covered_paths = _checksum_paths(release_dir)
    for label, rel_path in manifest.get("authoritative_files", {}).items():
        artifact_path = (REPO_ROOT / rel_path).resolve()
        try:
            release_rel_path = artifact_path.relative_to(
                release_dir.resolve()
            ).as_posix()
        except ValueError:
            errors.append(
                f"authoritative file {label} is outside release dir: {rel_path}"
            )
            continue
        if release_rel_path not in covered_paths:
            errors.append(
                f"authoritative file {label} is not covered by checksums.sha256: {rel_path}"
            )
    return errors


def _validate_reviewed_game_sets(
    release_dir: Path,
    raw_summary: dict[str, Any],
) -> list[str]:
    errors: list[str] = []
    raw_blocking_ids = set(raw_summary.get("release_blocking_game_ids", []))
    overlay_rows = _load_csv(
        release_dir
        / "policy"
        / "reviewed_frontier_policy_overlay_20260424_mechanics_fullrun_v4.csv"
    )
    overlay_ids = {row.get("game_id", "") for row in overlay_rows}
    errors.extend(_validate_csv_game_ids(overlay_rows, label="reviewed policy overlay"))
    sidecar_rows = _load_csv(release_dir / "sidecar" / "game_quality_sparse.csv")
    sidecar_reviewed_ids = {
        row.get("game_id", "")
        for row in sidecar_rows
        if row.get("policy_source") == "reviewed_override"
    }
    open_inventory_ids = {
        row.get("game_id", "")
        for row in _load_csv(
            release_dir
            / "inventories"
            / "phase7_open_blocker_inventory_20260424_mechanics_fullrun_v4.csv"
        )
    }
    errors.extend(
        _validate_csv_game_ids(
            _load_csv(
                release_dir
                / "inventories"
                / "phase7_open_blocker_inventory_20260424_mechanics_fullrun_v4.csv"
            ),
            label="open blocker inventory",
        )
    )

    comparisons = {
        "reviewed policy overlay": overlay_ids,
        "sidecar reviewed_override": sidecar_reviewed_ids,
        "open blocker inventory": open_inventory_ids,
    }
    for label, observed_ids in comparisons.items():
        if observed_ids != raw_blocking_ids:
            errors.append(
                f"{label} game set does not match raw release-blocking set: "
                f"missing={sorted(raw_blocking_ids - observed_ids)}, "
                f"extra={sorted(observed_ids - raw_blocking_ids)}"
            )
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
    if (
        overlay_has_original_paths
        and path_policy.get("policy_overlay_evidence_artifact") is None
    ):
        errors.append(
            "path_policy does not document policy overlay evidence_artifact absolute paths"
        )

    sidecar_summary = _load_json(release_dir / "sidecar" / "summary.json")
    sidecar_has_original_paths = any(
        original_root in str(path) for path in sidecar_summary.get("residual_dirs", [])
    )
    if (
        sidecar_has_original_paths
        and path_policy.get("sidecar_summary_residual_dirs") is None
    ):
        errors.append(
            "path_policy does not document sidecar summary residual_dirs absolute paths"
        )
    return errors


def _validate_catalog_snapshots(release_dir: Path) -> list[str]:
    errors: list[str] = []
    snapshot_root = release_dir / "catalog_snapshot"
    row_snapshot = snapshot_root / "pbp_row_overrides.csv"
    try:
        validate_historic_pbp_row_override_catalog(row_snapshot)
    except Exception as exc:  # noqa: BLE001 - report plainly in CLI output.
        errors.append(f"catalog_snapshot/pbp_row_overrides.csv invalid: {exc}")

    stat_snapshot = snapshot_root / "pbp_stat_overrides.csv"
    try:
        load_pbp_stat_overrides(stat_snapshot, strict=True)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"catalog_snapshot/pbp_stat_overrides.csv invalid: {exc}")

    validation_snapshot = snapshot_root / "validation_overrides.csv"
    try:
        validate_validation_overrides(validation_snapshot)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"catalog_snapshot/validation_overrides.csv invalid: {exc}")

    boxscore_snapshot = snapshot_root / "boxscore_source_overrides.csv"
    try:
        validate_boxscore_source_overrides(boxscore_snapshot)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"catalog_snapshot/boxscore_source_overrides.csv invalid: {exc}")

    overrides_snapshot = snapshot_root / "overrides"
    manifest_snapshot = overrides_snapshot / "correction_manifest.json"
    try:
        validate_manifest_schema(manifest_snapshot)
        validate_compiled_runtime_views(manifest_snapshot, overrides_snapshot)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"catalog_snapshot/overrides invalid: {exc}")

    for snapshot_rel_path, active_rel_path in CATALOG_SNAPSHOT_FILES.items():
        snapshot_path = release_dir / snapshot_rel_path
        active_path = REPO_ROOT / active_rel_path
        if not snapshot_path.exists():
            errors.append(f"release catalog snapshot missing: {snapshot_rel_path}")
            continue
        if not active_path.exists():
            errors.append(f"active catalog missing: {active_rel_path}")
            continue
        if snapshot_path.suffix == ".json":
            try:
                active_payload = json.loads(active_path.read_text(encoding="utf-8"))
                snapshot_payload = json.loads(
                    snapshot_path.read_text(encoding="utf-8")
                )
            except json.JSONDecodeError as exc:
                errors.append(f"{snapshot_rel_path} has invalid JSON: {exc}")
                continue
            if active_payload != snapshot_payload:
                errors.append(
                    f"active {active_rel_path} does not match release {snapshot_rel_path}"
                )
            continue
        if active_path.read_bytes() != snapshot_path.read_bytes():
            errors.append(
                f"active {active_rel_path} does not match release {snapshot_rel_path}"
            )
    return errors


def validate_manifest(manifest_path: Path) -> list[str]:
    manifest = _load_json(manifest_path)
    release_dir = manifest_path.parent
    errors: list[str] = []

    expected_tag = "historic-backfill-v4-1997-2020-20260424"
    if manifest.get("git", {}).get("release_tag") != expected_tag:
        errors.append("manifest git.release_tag does not match expected v4 tag")
    if "integrated_repo_commit" in manifest.get("git", {}):
        errors.append(
            "manifest must not embed a self-referential integrated repo commit SHA"
        )

    seasons = manifest.get("seasons")
    if seasons != list(range(1997, 2021)):
        errors.append("manifest seasons must be 1997 through 2020")

    validation = manifest.get("validation", {})
    full_history = _load_json(
        release_dir / "summaries" / "original" / "full_history_summary.original.json"
    )
    reviewed = _load_json(
        release_dir
        / "summaries"
        / "original"
        / "reviewed_residuals_summary.original.json"
    )
    raw = _load_json(
        release_dir / "summaries" / "original" / "raw_residuals_summary.original.json"
    )
    sidecar = _load_json(release_dir / "sidecar" / "summary.json")

    raw_open_games = _raw_open_game_count(raw)
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
    errors.extend(_validate_authoritative_checksum_coverage(manifest, release_dir))
    errors.extend(_validate_sidecar_contract(release_dir))
    errors.extend(_validate_reviewed_game_sets(release_dir, raw))
    errors.extend(_validate_path_policy(manifest, release_dir))
    errors.extend(_validate_catalog_snapshots(release_dir))

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
