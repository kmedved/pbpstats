from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

TESTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = TESTS_DIR.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from lineup_correction_manifest import (
    DEFAULT_DB_PATH,
    DEFAULT_FILE_DIRECTORY,
    DEFAULT_OVERRIDES_DIR,
    DEFAULT_PARQUET_PATH,
    LINEUP_WINDOWS_JSON,
    LINEUP_WINDOWS_NOTES_CSV,
    PERIOD_STARTERS_JSON,
    PERIOD_STARTERS_NOTES_CSV,
    ManifestValidationError,
    compile_runtime_views,
    seed_manifest_from_runtime,
)


def test_seed_manifest_captures_all_active_runtime_corrections():
    manifest = seed_manifest_from_runtime(overrides_dir=DEFAULT_OVERRIDES_DIR)

    corrections = manifest["corrections"]
    assert len(corrections) == 54
    assert sum(1 for row in corrections if row["scope_type"] == "period_start") == 48
    assert sum(1 for row in corrections if row["scope_type"] in {"window", "event"}) == 6


def test_seeded_manifest_round_trips_runtime_views_exactly(tmp_path: Path):
    manifest = seed_manifest_from_runtime(overrides_dir=DEFAULT_OVERRIDES_DIR)
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
        "lineup_correction_manifest._resolve_base_lineup",
        lambda *args, **kwargs: [932, 948, 57, 961, 111],
    )
    monkeypatch.setattr(
        "lineup_correction_manifest._load_game_rosters",
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
