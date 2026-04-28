import csv
import shutil
from pathlib import Path

from historic_backfill.runners.validate_release_manifest import (
    _validate_authoritative_checksum_coverage,
    _validate_catalog_snapshots,
    _validate_sidecar_contract,
    _raw_open_game_count,
    validate_checksums,
    validate_manifest,
)


RELEASE_DIR = (
    Path(__file__).resolve().parents[1]
    / "releases"
    / "v4_1997_2020_20260424_mechanics_fullrun"
)


def test_v4_release_manifest_matches_committed_summaries():
    errors = validate_manifest(RELEASE_DIR / "release_manifest.json")

    assert errors == []


def test_v4_release_checksums_match_committed_files():
    errors = validate_checksums(RELEASE_DIR)

    assert errors == []


def test_v4_release_checksum_validation_catches_mutated_artifact(tmp_path):
    release_copy = tmp_path / "release"
    shutil.copytree(RELEASE_DIR, release_copy)
    (release_copy / "closure_note.md").write_text("mutated\n", encoding="utf-8")

    errors = validate_checksums(release_copy)

    assert any("closure_note.md" in error for error in errors)


def test_raw_open_game_count_preserves_zero():
    assert (
        _raw_open_game_count(
            {
                "quality_status_counts": {"open": 0},
                "raw_quality_status_counts": {"open": 13},
            }
        )
        == 0
    )


def test_v4_release_manifest_rejects_authoritative_files_without_checksum(monkeypatch):
    manifest = {
        "authoritative_files": {
            "covered": (
                "historic_backfill/releases/v4_1997_2020_20260424_mechanics_fullrun/"
                "closure_note.md"
            ),
            "uncovered": (
                "historic_backfill/releases/v4_1997_2020_20260424_mechanics_fullrun/"
                "commands.md"
            ),
        }
    }
    monkeypatch.setattr(
        "historic_backfill.runners.validate_release_manifest._checksum_paths",
        lambda _release_dir: {"closure_note.md"},
    )

    errors = _validate_authoritative_checksum_coverage(manifest, RELEASE_DIR)

    assert any("not covered by checksums.sha256" in error for error in errors)


def test_v4_release_sidecar_validation_rejects_extra_reviewed_rows(tmp_path):
    release_copy = tmp_path / "release"
    shutil.copytree(RELEASE_DIR, release_copy)
    sidecar_path = release_copy / "sidecar" / "game_quality_sparse.csv"
    with sidecar_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
        fieldnames = list(rows[0])
    extra_row = dict(rows[0])
    extra_row["game_id"] = "0099999999"
    extra_row["policy_source"] = "reviewed_override"
    rows.append(extra_row)
    with sidecar_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    errors = validate_manifest(release_copy / "release_manifest.json")

    assert any("reviewed_override games not in overlay" in error for error in errors)


def test_v4_release_validation_rejects_overlay_game_set_drift(tmp_path):
    release_copy = tmp_path / "release"
    shutil.copytree(RELEASE_DIR, release_copy)
    overlay_path = (
        release_copy
        / "policy"
        / "reviewed_frontier_policy_overlay_20260424_mechanics_fullrun_v4.csv"
    )
    with overlay_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
        fieldnames = list(rows[0])
    rows[0]["game_id"] = "0099999999"
    with overlay_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    errors = validate_manifest(release_copy / "release_manifest.json")

    assert any(
        "reviewed policy overlay game set does not match" in error for error in errors
    )


def test_v4_release_sidecar_validation_rejects_bad_summary_counts(tmp_path):
    release_copy = tmp_path / "release"
    shutil.copytree(RELEASE_DIR, release_copy)
    summary_path = release_copy / "sidecar" / "summary.json"
    summary_text = summary_path.read_text(encoding="utf-8")
    summary_path.write_text(
        summary_text.replace('"row_count": 753', '"row_count": 752'),
        encoding="utf-8",
    )

    errors = validate_manifest(release_copy / "release_manifest.json")

    assert any("row_count does not match" in error for error in errors)


def test_v4_release_sidecar_validation_rejects_summary_id_list_drift(tmp_path):
    release_copy = tmp_path / "release"
    shutil.copytree(RELEASE_DIR, release_copy)
    summary_path = release_copy / "sidecar" / "summary.json"
    summary_text = summary_path.read_text(encoding="utf-8")
    summary_path.write_text(
        summary_text.replace(
            '"research_open_game_ids": []',
            '"research_open_game_ids": ["0029700001"]',
        ),
        encoding="utf-8",
    )

    errors = validate_manifest(release_copy / "release_manifest.json")

    assert any("research_open_game_ids do not match" in error for error in errors)


def test_v4_release_sidecar_validation_recomputes_summary_count_dictionaries(tmp_path):
    release_copy = tmp_path / "release"
    shutil.copytree(RELEASE_DIR, release_copy)
    summary_path = release_copy / "sidecar" / "summary.json"
    summary_text = summary_path.read_text(encoding="utf-8")
    summary_path.write_text(
        summary_text.replace('"reviewed_override": 13', '"reviewed_override": 12'),
        encoding="utf-8",
    )

    errors = validate_manifest(release_copy / "release_manifest.json")

    assert any("policy_source_counts do not match" in error for error in errors)


def test_v4_release_sidecar_validation_rejects_invalid_booleans(tmp_path):
    release_copy = tmp_path / "release"
    shutil.copytree(RELEASE_DIR, release_copy)
    sidecar_path = release_copy / "sidecar" / "game_quality_sparse.csv"
    with sidecar_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
        fieldnames = list(rows[0])
    rows[0]["blocks_release"] = "maybe"
    with sidecar_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    errors = validate_manifest(release_copy / "release_manifest.json")

    assert any("invalid boolean value" in error for error in errors)


def test_v4_release_sidecar_validation_rejects_blank_booleans(tmp_path):
    release_copy = tmp_path / "release"
    shutil.copytree(RELEASE_DIR, release_copy)
    sidecar_path = release_copy / "sidecar" / "game_quality_sparse.csv"
    with sidecar_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
        fieldnames = list(rows[0])
    rows[0]["blocks_release"] = ""
    with sidecar_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    errors = _validate_sidecar_contract(release_copy)

    assert any("blank boolean value" in error for error in errors)


def test_v4_release_sidecar_validation_rejects_noncanonical_game_ids(tmp_path):
    release_copy = tmp_path / "release"
    shutil.copytree(RELEASE_DIR, release_copy)
    sidecar_path = release_copy / "sidecar" / "game_quality_sparse.csv"
    with sidecar_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
        fieldnames = list(rows[0])
    rows[0]["game_id"] = rows[0]["game_id"].lstrip("0")
    with sidecar_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    errors = _validate_sidecar_contract(release_copy)

    assert any("non-canonical game_id" in error for error in errors)


def test_v4_release_sidecar_validation_rejects_missing_required_columns(tmp_path):
    release_copy = tmp_path / "release"
    shutil.copytree(RELEASE_DIR, release_copy)
    sidecar_path = release_copy / "sidecar" / "game_quality_sparse.csv"
    with sidecar_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
        fieldnames = [field for field in rows[0] if field != "research_open"]
    with sidecar_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row[field] for field in fieldnames})

    errors = validate_manifest(release_copy / "release_manifest.json")

    assert any(
        "missing columns" in error and "research_open" in error for error in errors
    )


def test_v4_release_sidecar_validation_rejects_invalid_terminal_status(tmp_path):
    release_copy = tmp_path / "release"
    shutil.copytree(RELEASE_DIR, release_copy)
    sidecar_path = release_copy / "sidecar" / "game_quality_sparse.csv"
    with sidecar_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
        fieldnames = list(rows[0])
    rows[0]["release_gate_status"] = "open_actionable"
    with sidecar_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    errors = _validate_sidecar_contract(release_copy)

    assert any("invalid release_gate_status" in error for error in errors)


def test_v4_release_sidecar_validation_rejects_overlay_decision_drift(tmp_path):
    release_copy = tmp_path / "release"
    shutil.copytree(RELEASE_DIR, release_copy)
    overlay_path = (
        release_copy
        / "policy"
        / "reviewed_frontier_policy_overlay_20260424_mechanics_fullrun_v4.csv"
    )
    with overlay_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
        fieldnames = list(rows[0])
    rows[0]["execution_lane"] = "exact"
    with overlay_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    errors = _validate_sidecar_contract(release_copy)

    assert any("reviewed overlay field mismatch" in error for error in errors)


def test_v4_release_sidecar_validation_rejects_primary_quality_drift(tmp_path):
    release_copy = tmp_path / "release"
    shutil.copytree(RELEASE_DIR, release_copy)
    sidecar_path = release_copy / "sidecar" / "game_quality_sparse.csv"
    with sidecar_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
        fieldnames = list(rows[0])
    for row in rows:
        if row["policy_source"] == "reviewed_override":
            row["primary_quality_status"] = "exact"
            break
    with sidecar_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    errors = _validate_sidecar_contract(release_copy)

    assert any("primary_quality_status" in error for error in errors)


def test_v4_release_sidecar_validation_checks_empty_csv_header(tmp_path):
    release_copy = tmp_path / "release"
    shutil.copytree(RELEASE_DIR, release_copy)
    sidecar_path = release_copy / "sidecar" / "game_quality_sparse.csv"
    sidecar_path.write_text("game_id\n", encoding="utf-8")

    errors = validate_manifest(release_copy / "release_manifest.json")

    assert any(
        "missing columns" in error and "blocks_release" in error for error in errors
    )


def test_v4_release_validation_rejects_invalid_catalog_snapshot(tmp_path):
    release_copy = tmp_path / "release"
    shutil.copytree(RELEASE_DIR, release_copy)
    snapshot_path = release_copy / "catalog_snapshot" / "pbp_row_overrides.csv"
    snapshot_path.write_text(
        "game_id,action,event_num,anchor_event_num,notes\n"
        "0021900261,drop,367,,drop stranded row\n"
        "0021900261,move_before,367,368,stale move\n",
        encoding="utf-8",
    )

    errors = _validate_catalog_snapshots(release_copy)

    assert any(
        "catalog_snapshot/pbp_row_overrides.csv invalid" in error for error in errors
    )


def test_v4_release_validation_rejects_invalid_stat_catalog_snapshot(tmp_path):
    release_copy = tmp_path / "release"
    shutil.copytree(RELEASE_DIR, release_copy)
    snapshot_path = release_copy / "catalog_snapshot" / "pbp_stat_overrides.csv"
    snapshot_path.write_text(
        "game_id,team_id,player_id,stat_key,stat_value,notes\n"
        "0029700001,1610612740,123,PTS,nan,bad\n",
        encoding="utf-8",
    )

    errors = _validate_catalog_snapshots(release_copy)

    assert any(
        "catalog_snapshot/pbp_stat_overrides.csv invalid" in error
        for error in errors
    )


def test_v4_release_validation_rejects_missing_catalog_snapshot(tmp_path):
    release_copy = tmp_path / "release"
    shutil.copytree(RELEASE_DIR, release_copy)
    (
        release_copy
        / "catalog_snapshot"
        / "overrides"
        / "lineup_window_overrides.json"
    ).unlink()

    errors = _validate_catalog_snapshots(release_copy)

    assert any(
        "catalog_snapshot/overrides/lineup_window_overrides.json" in error
        for error in errors
    )


def test_v4_release_validation_rejects_active_catalog_snapshot_drift(tmp_path):
    release_copy = tmp_path / "release"
    shutil.copytree(RELEASE_DIR, release_copy)
    snapshot_path = release_copy / "catalog_snapshot" / "pbp_row_overrides.csv"
    snapshot_path.write_text(
        snapshot_path.read_text(encoding="utf-8") + "\n",
        encoding="utf-8",
    )

    errors = _validate_catalog_snapshots(release_copy)

    assert any(
        "active historic_backfill/catalogs/pbp_row_overrides.csv" in error
        for error in errors
    )
