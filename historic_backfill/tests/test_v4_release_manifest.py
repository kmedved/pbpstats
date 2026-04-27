import shutil
from pathlib import Path

from historic_backfill.runners.validate_release_manifest import validate_checksums, validate_manifest


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
