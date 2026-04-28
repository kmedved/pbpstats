from __future__ import annotations

import json
from pathlib import Path

import pytest

from historic_backfill.catalogs.lineup_correction_manifest import (
    DEFAULT_DB_PATH,
    DEFAULT_FILE_DIRECTORY,
    DEFAULT_OVERRIDES_DIR,
    DEFAULT_PARQUET_PATH,
    LINEUP_WINDOWS_JSON,
    LINEUP_WINDOWS_NOTES_CSV,
    PERIOD_STARTERS_JSON,
    PERIOD_STARTERS_NOTES_CSV,
    ManifestValidationError,
    build_explicit_runtime_views,
    compile_runtime_views,
    seed_manifest_from_runtime,
)


def test_seed_manifest_captures_all_active_runtime_corrections():
    manifest = seed_manifest_from_runtime(overrides_dir=DEFAULT_OVERRIDES_DIR)

    corrections = manifest["corrections"]
    assert len(corrections) == 54
    assert sum(1 for row in corrections if row["scope_type"] == "period_start") == 48
    assert sum(1 for row in corrections if row["scope_type"] in {"window", "event"}) == 6


def test_seeded_manifest_round_trips_runtime_views_exactly(tmp_path: Path, monkeypatch):
    manifest = seed_manifest_from_runtime(overrides_dir=DEFAULT_OVERRIDES_DIR)
    roster_by_game = {}
    for correction in manifest["corrections"]:
        if correction["status"] != "active":
            continue
        roster_by_game.setdefault(str(correction["game_id"]).zfill(10), {}).setdefault(
            correction["team_id"],
            set(),
        ).update(correction["lineup_player_ids"])
    monkeypatch.setattr(
        "historic_backfill.catalogs.lineup_correction_manifest._load_game_rosters",
        lambda game_id, *_args, **_kwargs: roster_by_game[str(game_id).zfill(10)],
    )
    compile_runtime_views(
        manifest,
        output_dir=tmp_path,
        db_path=DEFAULT_DB_PATH,
        parquet_path=DEFAULT_PARQUET_PATH,
        file_directory=DEFAULT_FILE_DIRECTORY,
    )

    for filename in [
        PERIOD_STARTERS_JSON,
        PERIOD_STARTERS_NOTES_CSV,
        LINEUP_WINDOWS_JSON,
        LINEUP_WINDOWS_NOTES_CSV,
    ]:
        original = (DEFAULT_OVERRIDES_DIR / filename).read_text(encoding="utf-8")
        compiled = (tmp_path / filename).read_text(encoding="utf-8")
        assert compiled == original


def test_compiler_rejects_non_lineup_active_domains(tmp_path: Path):
    manifest = seed_manifest_from_runtime(overrides_dir=DEFAULT_OVERRIDES_DIR)
    manifest["corrections"].append(
        {
            "correction_id": "bad_domain",
            "episode_id": "bad_domain",
            "status": "active",
            "domain": "pbp_row",
            "scope_type": "event",
            "authoring_mode": "explicit",
            "game_id": "0029700367",
            "period": 4,
            "team_id": 1610612761,
            "start_event_num": 464,
            "end_event_num": 464,
            "lineup_player_ids": [757, 948, 57, 961, 932],
            "reason_code": "bad_domain",
            "evidence_summary": "bad domain should fail",
            "source_primary": "manual_trace",
            "source_secondary": "unknown",
            "preferred_source": "manual_trace",
            "confidence": "low",
        }
    )

    with pytest.raises(ManifestValidationError, match="only supports active lineup corrections"):
        compile_runtime_views(
            manifest,
            output_dir=tmp_path,
            db_path=DEFAULT_DB_PATH,
            parquet_path=DEFAULT_PARQUET_PATH,
            file_directory=DEFAULT_FILE_DIRECTORY,
        )


def test_compiler_resolves_delta_windows(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    manifest = {
        "manifest_version": "test",
        "corrections": [
            {
                "correction_id": "delta_window",
                "episode_id": "delta_window",
                "status": "active",
                "domain": "lineup",
                "scope_type": "event",
                "authoring_mode": "delta",
                "game_id": "0029700367",
                "period": 4,
                "team_id": 1610612761,
                "start_event_num": 464,
                "end_event_num": 464,
                "swap_out_player_id": 932,
                "swap_in_player_id": 757,
                "reason_code": "delta_swap",
                "evidence_summary": "delta test",
                "source_primary": "manual_trace",
                "source_secondary": "unknown",
                "preferred_source": "manual_trace",
                "confidence": "medium",
            }
        ],
        "residual_annotations": [],
    }

    monkeypatch.setattr(
        "historic_backfill.catalogs.lineup_correction_manifest._resolve_base_lineup",
        lambda *args, **kwargs: [932, 948, 57, 961, 111],
    )
    monkeypatch.setattr(
        "historic_backfill.catalogs.lineup_correction_manifest._load_game_rosters",
        lambda *args, **kwargs: {1610612761: {757, 948, 57, 961, 111, 932}},
    )

    compile_runtime_views(
        manifest,
        output_dir=tmp_path,
        db_path=DEFAULT_DB_PATH,
        parquet_path=DEFAULT_PARQUET_PATH,
        file_directory=DEFAULT_FILE_DIRECTORY,
    )

    compiled = json.loads((tmp_path / LINEUP_WINDOWS_JSON).read_text(encoding="utf-8"))
    assert compiled["0029700367"][0]["lineup_player_ids"] == [757, 948, 57, 961, 111]


def test_delta_compiler_forwards_runtime_snapshot_paths(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    manifest = {
        "manifest_version": "test",
        "corrections": [
            {
                "correction_id": "delta_window",
                "episode_id": "delta_window",
                "status": "active",
                "domain": "lineup",
                "scope_type": "event",
                "authoring_mode": "delta",
                "game_id": "0029700367",
                "period": 4,
                "team_id": 1610612761,
                "start_event_num": 464,
                "end_event_num": 464,
                "swap_out_player_id": 932,
                "swap_in_player_id": 757,
                "reason_code": "delta_swap",
                "evidence_summary": "delta test",
                "source_primary": "manual_trace",
                "source_secondary": "unknown",
                "preferred_source": "manual_trace",
                "confidence": "medium",
            }
        ],
        "residual_annotations": [],
    }
    snapshot_paths = {
        "pbp_row_overrides_path": tmp_path / "row.csv",
        "pbp_stat_overrides_path": tmp_path / "stat.csv",
        "boxscore_source_overrides_path": tmp_path / "boxscore.csv",
        "period_starter_parquet_paths": [tmp_path / "period.parquet"],
    }
    observed: dict[str, object] = {}

    def fake_load_game_context(*_args, **kwargs):
        observed.update(kwargs)
        return None, object(), {}

    monkeypatch.setattr(
        "historic_backfill.catalogs.lineup_correction_manifest._load_game_context",
        fake_load_game_context,
    )
    monkeypatch.setattr(
        "historic_backfill.catalogs.lineup_correction_manifest._collect_game_events",
        lambda _possessions: [object()],
    )
    monkeypatch.setattr(
        "historic_backfill.catalogs.lineup_correction_manifest._event_index_lookup",
        lambda _events: {(4, 464): [0]},
    )
    monkeypatch.setattr(
        "historic_backfill.catalogs.lineup_correction_manifest._lineup_at_event",
        lambda *_args, **_kwargs: [932, 948, 57, 961, 111],
    )
    monkeypatch.setattr(
        "historic_backfill.catalogs.lineup_correction_manifest._load_game_rosters",
        lambda *args, **kwargs: {1610612761: {757, 948, 57, 961, 111, 932}},
    )

    compile_runtime_views(
        manifest,
        output_dir=tmp_path,
        db_path=DEFAULT_DB_PATH,
        parquet_path=DEFAULT_PARQUET_PATH,
        file_directory=DEFAULT_FILE_DIRECTORY,
        **snapshot_paths,
    )

    for key, value in snapshot_paths.items():
        assert observed[key] == value


def test_explicit_runtime_views_normalize_float_like_game_ids():
    manifest = {
        "manifest_version": "test",
        "corrections": [
            {
                "correction_id": "float_like_game_id",
                "episode_id": "float_like_game_id",
                "status": "active",
                "domain": "lineup",
                "scope_type": "period_start",
                "authoring_mode": "explicit",
                "game_id": "29700367.0",
                "period": 4,
                "team_id": 1610612761,
                "lineup_player_ids": [757, 948, 57, 961, 932],
                "reason_code": "float_like_game_id",
                "evidence_summary": "normalize game id",
                "source_primary": "manual_trace",
                "source_secondary": "unknown",
                "preferred_source": "manual_trace",
                "confidence": "low",
            }
        ],
        "residual_annotations": [],
    }

    period_overrides, lineup_windows = build_explicit_runtime_views(manifest)

    assert set(period_overrides) == {"0029700367"}
    assert lineup_windows == {}
