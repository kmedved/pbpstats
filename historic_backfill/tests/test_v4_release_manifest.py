import csv
import shutil
from pathlib import Path

from historic_backfill.runners.validate_release_manifest import (
    _validate_authoritative_checksum_coverage,
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
    assert _raw_open_game_count(
        {
            "quality_status_counts": {"open": 0},
            "raw_quality_status_counts": {"open": 13},
        }
    ) == 0


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
