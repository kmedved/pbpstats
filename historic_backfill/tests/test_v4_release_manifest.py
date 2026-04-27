from pathlib import Path

from historic_backfill.runners.validate_release_manifest import validate_manifest


RELEASE_DIR = (
    Path(__file__).resolve().parents[1]
    / "releases"
    / "v4_1997_2020_20260424_mechanics_fullrun"
)


def test_v4_release_manifest_matches_committed_summaries():
    errors = validate_manifest(RELEASE_DIR / "release_manifest.json")

    assert errors == []
