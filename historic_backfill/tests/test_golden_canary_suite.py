import argparse
import json
from pathlib import Path

import historic_backfill.runners.run_golden_canary_suite as golden
from historic_backfill.runners.run_golden_canary_suite import _case_expectations, _case_fail_reasons


def test_positive_canary_defaults_to_clean_expectations() -> None:
    case = {"category": "positive_canaries", "game_id": "0021200444"}
    expectations = _case_expectations(case, 0.5)

    assert expectations["max_minute_outlier_rows"] == 0
    assert expectations["max_plus_minus_mismatch_rows"] == 0
    assert expectations["max_event_issue_rows"] == 0
    assert expectations["max_game_max_minutes_abs_diff"] == 0.5


def test_manifest_expectations_allow_dirty_fixed_game_envelope() -> None:
    case = {
        "category": "fixed_dirty_games",
        "game_id": "0029700159",
        "max_minutes_mismatch_rows": 5,
        "max_minute_outlier_rows": 3,
        "max_plus_minus_mismatch_rows": 4,
        "max_game_max_minutes_abs_diff": 12.0,
        "max_event_issue_rows": 1,
    }
    expectations = _case_expectations(case, 0.5)
    row = {
        "minutes_mismatch_rows": 5,
        "minute_outlier_rows": 3,
        "plus_minus_mismatch_rows": 4,
        "game_max_minutes_abs_diff": 11.866666666666667,
        "event_issue_rows": 1,
        "boxscore_audit_failures": 0,
    }

    assert _case_fail_reasons(row, expectations) == []


def test_case_fail_reasons_reports_unexpected_regression() -> None:
    case = {
        "category": "failed_patch_anti_canaries",
        "game_id": "0021700236",
        "max_plus_minus_mismatch_rows": 2,
    }
    expectations = _case_expectations(case, 0.5)
    row = {
        "minutes_mismatch_rows": 0,
        "minute_outlier_rows": 0,
        "plus_minus_mismatch_rows": 3,
        "game_max_minutes_abs_diff": 0.01,
        "event_issue_rows": 2,
        "boxscore_audit_failures": 0,
    }

    assert _case_fail_reasons(row, expectations) == ["plus_minus_mismatch_rows 3 > 2"]


def test_main_forwards_runtime_cache_mode_and_summarizes_cases(tmp_path, monkeypatch) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "positive_canaries": [
                    {"game_id": "0021200444", "period": 4},
                ],
                "fixed_dirty_games": [
                    {
                        "game_id": "0029700159",
                        "period": 3,
                        "max_minutes_mismatch_rows": 5,
                        "max_minute_outlier_rows": 3,
                        "max_plus_minus_mismatch_rows": 4,
                        "max_game_max_minutes_abs_diff": 12.0,
                        "max_event_issue_rows": 1,
                    }
                ],
                "failed_patch_anti_canaries": [],
                "source_limited_negative_controls": [],
                "pm_only_boundary_controls": [],
            }
        ),
        encoding="utf-8",
    )

    command_calls = []

    def fake_run_command(args, *, log_path, env_updates=None):
        command_calls.append((args, env_updates))
        rerun_dir = log_path.parent
        rerun_dir.mkdir(parents=True, exist_ok=True)
        (rerun_dir / "summary.json").write_text(
            json.dumps({"failed_games": 0, "event_stats_errors": 0}),
            encoding="utf-8",
        )
        log_path.write_text("", encoding="utf-8")

    def fake_game_summary_from_run(_run_dir, game_id):
        if game_id == "0021200444":
            return {
                "minutes_plus_minus": {
                    "minutes_mismatch_rows": 0,
                    "minute_outlier_rows": 0,
                    "plus_minus_mismatch_rows": 0,
                    "game_max_minutes_abs_diff": 0.01,
                },
                "event_on_court": {"issue_rows": 0},
                "boxscore_audit": {"audit_failures": 0, "player_rows_with_mismatch": 0, "team_rows_with_mismatch": 0},
            }
        return {
            "minutes_plus_minus": {
                "minutes_mismatch_rows": 5,
                "minute_outlier_rows": 3,
                "plus_minus_mismatch_rows": 4,
                "game_max_minutes_abs_diff": 11.0,
            },
            "event_on_court": {"issue_rows": 1},
            "boxscore_audit": {"audit_failures": 0, "player_rows_with_mismatch": 0, "team_rows_with_mismatch": 0},
        }

    monkeypatch.setattr(golden, "_run_command", fake_run_command)
    monkeypatch.setattr(golden, "_game_summary_from_run", fake_game_summary_from_run)

    monkeypatch.setattr(
        golden,
        "parse_args",
        lambda: argparse.Namespace(
            output_dir=tmp_path / "suite",
            manifest_path=manifest_path,
            db_path=tmp_path / "nba_raw.db",
            parquet_path=tmp_path / "playbyplayv2.parq",
            overrides_path=tmp_path / "validation_overrides.csv",
            file_directory=tmp_path,
            max_workers=8,
            severe_minute_threshold=0.5,
            pbpstats_repo=tmp_path / "pbpstats",
            runtime_input_cache_mode="fresh-copy",
            audit_profile="counting_only",
        ),
    )

    assert golden.main() == 0
    args, env_updates = command_calls[0]
    assert "--runtime-input-cache-mode" in args
    assert "fresh-copy" in args
    assert "--audit-profile" in args
    assert "counting_only" in args
    assert "--run-boxscore-audit" in args
    assert env_updates == {"PBPSTATS_REPO": str((tmp_path / "pbpstats").resolve())}
    summary = json.loads((tmp_path / "suite" / "summary.json").read_text(encoding="utf-8"))
    assert summary["suite_pass"] is True
    assert summary["suite_pass_all_cases"] is True
    assert summary["suite_pass_stable_cases_only"] is True
    assert summary["unstable_control_case_count"] == 0
    assert summary["unstable_control_failures"] == 0
    assert summary["categories"]["fixed_dirty_games"] == 1
    assert summary["categories"]["positive_canaries"] == 1


def test_unstable_control_failure_does_not_fail_stable_only_gate(tmp_path, monkeypatch) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "positive_canaries": [
                    {"game_id": "0021200444", "period": 4},
                ],
                "fixed_dirty_games": [],
                "failed_patch_anti_canaries": [],
                "source_limited_negative_controls": [
                    {
                        "game_id": "0029800606",
                        "period": 5,
                        "stability_class": "unstable_control",
                        "max_plus_minus_mismatch_rows": 0,
                    }
                ],
                "pm_only_boundary_controls": [],
            }
        ),
        encoding="utf-8",
    )

    def fake_run_command(_args, *, log_path, env_updates=None):
        rerun_dir = log_path.parent
        rerun_dir.mkdir(parents=True, exist_ok=True)
        (rerun_dir / "summary.json").write_text(
            json.dumps({"failed_games": 0, "event_stats_errors": 0}),
            encoding="utf-8",
        )
        log_path.write_text("", encoding="utf-8")

    def fake_game_summary_from_run(_run_dir, game_id):
        if game_id == "0021200444":
            return {
                "minutes_plus_minus": {
                    "minutes_mismatch_rows": 0,
                    "minute_outlier_rows": 0,
                    "plus_minus_mismatch_rows": 0,
                    "game_max_minutes_abs_diff": 0.0,
                },
                "event_on_court": {"issue_rows": 0},
                "boxscore_audit": {"audit_failures": 0, "player_rows_with_mismatch": 0, "team_rows_with_mismatch": 0},
            }
        return {
            "minutes_plus_minus": {
                "minutes_mismatch_rows": 0,
                "minute_outlier_rows": 0,
                "plus_minus_mismatch_rows": 1,
                "game_max_minutes_abs_diff": 0.0,
            },
            "event_on_court": {"issue_rows": 0},
            "boxscore_audit": {"audit_failures": 0, "player_rows_with_mismatch": 0, "team_rows_with_mismatch": 0},
        }

    monkeypatch.setattr(golden, "_run_command", fake_run_command)
    monkeypatch.setattr(golden, "_game_summary_from_run", fake_game_summary_from_run)
    monkeypatch.setattr(
        golden,
        "parse_args",
        lambda: argparse.Namespace(
            output_dir=tmp_path / "suite",
            manifest_path=manifest_path,
            db_path=tmp_path / "nba_raw.db",
            parquet_path=tmp_path / "playbyplayv2.parq",
            overrides_path=tmp_path / "validation_overrides.csv",
            file_directory=tmp_path,
            max_workers=2,
            severe_minute_threshold=0.5,
            pbpstats_repo=None,
            runtime_input_cache_mode="fresh-copy",
            audit_profile="full",
        ),
    )

    assert golden.main() == 0
    summary = json.loads((tmp_path / "suite" / "summary.json").read_text(encoding="utf-8"))
    assert summary["suite_pass"] is False
    assert summary["suite_pass_all_cases"] is False
    assert summary["suite_pass_stable_cases_only"] is True
    assert summary["unstable_control_case_count"] == 1
    assert summary["unstable_control_failures"] == 1
