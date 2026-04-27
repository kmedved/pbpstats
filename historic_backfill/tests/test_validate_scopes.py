from pathlib import Path

from historic_backfill.runners.validate import validate_scope


def _touch(root: Path, rel_path: str) -> None:
    path = root / rel_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("fixture\n", encoding="utf-8")


def test_core_validation_requires_nba_inputs_without_checking_cross_source(tmp_path):
    for rel_path in (
        "data/nba_raw.db",
        "data/playbyplayv2.parq",
        "data/playbyplayv3.parq",
    ):
        _touch(tmp_path, rel_path)

    result = validate_scope("core", root=tmp_path)

    assert result.ok is True
    assert result.missing_required == []
    assert result.skipped_optional == []


def test_cross_source_validation_skips_missing_optional_inputs(tmp_path):
    result = validate_scope("cross-source", root=tmp_path)

    assert result.ok is True
    assert "data/bbr/bbref_boxscores.db" in result.skipped_optional
    assert "data/tpdev/full_pbp_new.parq" in result.skipped_optional


def test_provenance_validation_fails_when_evidence_inputs_are_missing(tmp_path):
    result = validate_scope("provenance", root=tmp_path)

    assert result.ok is False
    assert "data/bbr/bbref_boxscores.db" in result.missing_required
    assert "data/tpdev/tpdev_box.parq" in result.missing_required
