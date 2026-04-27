import argparse
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

import historic_backfill.runners.rerun_selected_games as rerun
from historic_backfill.runners.rerun_selected_games import _expand_game_id_tokens, _load_game_ids


def test_expand_game_id_tokens_supports_comma_and_space_separated_values() -> None:
    assert _expand_game_id_tokens(["0021700236,0021700917", "0021900333"]) == [
        "0021700236",
        "0021700917",
        "0021900333",
    ]


def test_load_game_ids_normalizes_and_dedupes_comma_separated_args(tmp_path) -> None:
    game_ids_file = tmp_path / "game_ids.txt"
    game_ids_file.write_text("21700917,0021900333\n0021700236\n", encoding="utf-8")
    args = argparse.Namespace(
        game_ids=["0021700236,21700917"],
        game_ids_file=game_ids_file,
    )

    assert _load_game_ids(args) == ["0021700236", "0021700917", "0021900333"]


def test_main_writes_runtime_provenance_and_uses_runtime_file_directory(
    tmp_path: Path,
    monkeypatch,
) -> None:
    runtime_file_directory = tmp_path / "runtime_file_directory"
    runtime_file_directory.mkdir()
    prepared_args = {}
    install_calls = []
    audit_calls = []

    def fake_prepare_local_runtime_inputs(cache_dir, **kwargs):
        prepared_args["cache_dir"] = cache_dir
        prepared_args["kwargs"] = kwargs
        notebook_dump_path = tmp_path / "dump.py"
        notebook_dump_path.write_text("pass\n", encoding="utf-8")
        return {
            "db_path": tmp_path / "nba_raw.db",
            "parquet_path": tmp_path / "playbyplayv2.parq",
            "notebook_dump_path": notebook_dump_path,
            "preload_module_paths": {},
            "overrides_path": tmp_path / "validation_overrides.csv",
            "boxscore_source_overrides_path": tmp_path / "boxscore_source_overrides.csv",
            "period_starter_parquet_paths": [tmp_path / "period_starters_v6.parquet"],
            "file_directory": runtime_file_directory,
            "runtime_input_provenance": {"runtime_input_cache_mode": "fresh-copy"},
        }

    def fake_load_v9b_namespace(*, notebook_dump_path, preload_module_paths):
        assert notebook_dump_path.exists()
        assert preload_module_paths == {}
        return {
            "load_validation_overrides": lambda _path: [],
            "clear_event_stats_errors": lambda: None,
            "clear_rebound_fallback_deletions": lambda: None,
            "load_pbp_from_parquet": lambda _path, season: pd.DataFrame({"GAME_ID": [season]}),
            "process_games_parallel": lambda season_game_ids, *_args, **_kwargs: (
                pd.DataFrame({"Game_SingleGame": season_game_ids, "dummy": [1] * len(season_game_ids)}),
                pd.DataFrame(),
                pd.DataFrame(),
                pd.DataFrame(),
                pd.DataFrame(),
            ),
            "export_rebound_fallback_deletions": lambda _path: None,
            "write_boxscore_audit_outputs": lambda **_kwargs: {
                "audit_failures": 0,
                "player_rows_with_mismatch": 0,
                "team_rows_with_mismatch": 0,
            },
        }

    monkeypatch.setattr(rerun, "prepare_local_runtime_inputs", fake_prepare_local_runtime_inputs)
    monkeypatch.setattr(rerun, "load_v9b_namespace", fake_load_v9b_namespace)
    monkeypatch.setattr(
        rerun,
        "install_local_boxscore_wrapper",
        lambda *args, **kwargs: install_calls.append(kwargs),
    )
    monkeypatch.setattr(
        rerun,
        "run_lineup_audits",
        lambda **kwargs: audit_calls.append(kwargs) or {
            "minutes_plus_minus": {
                "rows": 1,
                "minutes_mismatches": 0,
                "minutes_outliers": 0,
                "plus_minus_mismatches": 0,
                "minutes_outlier_threshold": 0.5,
            },
            "problem_games": 0,
            "event_on_court": {"games": 0, "issue_rows": 0, "issue_games": 0, "status_counts": {}},
        },
    )

    exit_code = rerun.main(
        [
            "--game-ids",
            "0021700236",
            "0029900342",
            "--output-dir",
            str(tmp_path / "rerun"),
            "--run-boxscore-audit",
            "--runtime-input-cache-mode",
            "fresh-copy",
        ]
    )

    assert exit_code == 0
    assert prepared_args["kwargs"]["runtime_input_cache_mode"] == "fresh-copy"
    assert install_calls[0]["file_directory"] == runtime_file_directory
    assert audit_calls[0]["file_directory"] == runtime_file_directory
    summary = json.loads((tmp_path / "rerun" / "summary.json").read_text(encoding="utf-8"))
    assert summary["runtime_input_cache_mode"] == "fresh-copy"
    assert Path(summary["runtime_input_provenance_path"]).exists()
    assert Path(summary["runtime_file_directory"]) == runtime_file_directory


def test_main_counting_only_profile_skips_lineup_audits(tmp_path: Path, monkeypatch) -> None:
    runtime_file_directory = tmp_path / "runtime_file_directory"
    runtime_file_directory.mkdir()
    audit_calls = []

    def fake_prepare_local_runtime_inputs(_cache_dir, **_kwargs):
        notebook_dump_path = tmp_path / "dump.py"
        notebook_dump_path.write_text("pass\n", encoding="utf-8")
        return {
            "db_path": tmp_path / "nba_raw.db",
            "parquet_path": tmp_path / "playbyplayv2.parq",
            "notebook_dump_path": notebook_dump_path,
            "preload_module_paths": {},
            "overrides_path": tmp_path / "validation_overrides.csv",
            "boxscore_source_overrides_path": tmp_path / "boxscore_source_overrides.csv",
            "period_starter_parquet_paths": [tmp_path / "period_starters_v6.parquet"],
            "file_directory": runtime_file_directory,
            "runtime_input_provenance": {"runtime_input_cache_mode": "reuse-validated-cache"},
        }

    def fake_load_v9b_namespace(*, notebook_dump_path, preload_module_paths):
        assert notebook_dump_path.exists()
        assert preload_module_paths == {}
        return {
            "load_validation_overrides": lambda _path: [],
            "clear_event_stats_errors": lambda: None,
            "clear_rebound_fallback_deletions": lambda: None,
            "load_pbp_from_parquet": lambda _path, season: pd.DataFrame({"GAME_ID": [season]}),
            "process_games_parallel": lambda season_game_ids, *_args, **_kwargs: (
                pd.DataFrame({"Game_SingleGame": season_game_ids, "dummy": [1] * len(season_game_ids)}),
                pd.DataFrame(),
                pd.DataFrame(),
                pd.DataFrame(),
                pd.DataFrame(),
            ),
            "export_rebound_fallback_deletions": lambda _path: None,
            "write_boxscore_audit_outputs": lambda **_kwargs: {
                "audit_failures": 0,
                "player_rows_with_mismatch": 0,
                "team_rows_with_mismatch": 0,
            },
        }

    monkeypatch.setattr(rerun, "prepare_local_runtime_inputs", fake_prepare_local_runtime_inputs)
    monkeypatch.setattr(rerun, "load_v9b_namespace", fake_load_v9b_namespace)
    monkeypatch.setattr(rerun, "install_local_boxscore_wrapper", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        rerun,
        "run_lineup_audits",
        lambda **kwargs: audit_calls.append(kwargs) or {},
    )

    exit_code = rerun.main(
        [
            "--game-ids",
            "0021700236",
            "--output-dir",
            str(tmp_path / "rerun"),
            "--audit-profile",
            "counting_only",
            "--run-boxscore-audit",
        ]
    )

    assert exit_code == 0
    assert audit_calls == []
    summary = json.loads((tmp_path / "rerun" / "summary.json").read_text(encoding="utf-8"))
    assert summary["audit_profile"] == "counting_only"
