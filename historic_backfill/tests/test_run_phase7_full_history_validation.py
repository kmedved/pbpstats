from __future__ import annotations

import csv
import json
import subprocess
from pathlib import Path

import pandas as pd

import historic_backfill.runners.run_phase7_full_history_validation as phase7


TEST_COMPILE_SUMMARY = {
    "active_corrections": 54,
    "active_period_start_corrections": 48,
    "active_window_corrections": 6,
}


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _base_stat_row(game_id: int, team_id: int, player_id: int, **overrides: int) -> dict[str, object]:
    row: dict[str, object] = {
        "Game_SingleGame": game_id,
        "Team_SingleGame": team_id,
        "NbaDotComID": player_id,
    }
    for column in phase7.COUNTING_STAT_COLUMNS:
        row[column] = 0
    row.update(overrides)
    return row


def _write_parquet(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


def _write_baseline_dir(path: Path, rows: list[dict[str, object]]) -> None:
    _write_parquet(path / "darko_1997_2020.parquet", rows)
    _write_json(path / "summary_1997.json", {"failed_games": 0, "event_stats_errors": 0, "player_rows": 1})
    _write_json(path / "summary_2020.json", {"failed_games": 0, "event_stats_errors": 0, "player_rows": 1})


def _write_candidate_run_bundle(candidate_run_dir: Path, season_rows: dict[int, list[dict[str, object]]]) -> None:
    candidate_run_dir.mkdir(parents=True, exist_ok=True)
    for season, rows in season_rows.items():
        _write_parquet(candidate_run_dir / f"darko_{season}.parquet", rows)
        _write_json(
            candidate_run_dir / f"summary_{season}.json",
            {
                "season": season,
                "player_rows": len(rows),
                "failed_games": 0,
                "event_stats_errors": 0,
            },
        )


def _inventory_rows() -> list[dict[str, object]]:
    return [
        {
            "game_id": "0029700159",
            "block_key": "A",
            "season": 1998,
            "lane": "special_holdout_material_minute",
            "recommended_next_action": "keep_open",
            "has_event_on_court_issue": True,
            "has_material_minute_issue": True,
            "has_severe_minute_issue": True,
            "n_actionable_event_rows": 1,
            "max_abs_minute_diff": 1.85,
            "n_pm_reference_delta_rows": 3,
            "notes": "",
        },
        {
            "game_id": "0029701075",
            "block_key": "A",
            "season": 1999,
            "lane": "candidate_systematic_defect",
            "recommended_next_action": "keep_open",
            "has_event_on_court_issue": True,
            "has_material_minute_issue": True,
            "has_severe_minute_issue": True,
            "n_actionable_event_rows": 2,
            "max_abs_minute_diff": 2.15,
            "n_pm_reference_delta_rows": 4,
            "notes": "",
        },
    ]


def _shortlist_rows() -> list[dict[str, object]]:
    return [
        {
            "game_id": "0029700159",
            "current_blocker_status": "open",
            "next_step": "keep_open",
            "evidence_basis": "test",
        },
        {
            "game_id": "0029701075",
            "current_blocker_status": "open",
            "next_step": "keep_open",
            "evidence_basis": "test",
        },
    ]


def _overlay_rows() -> list[dict[str, object]]:
    return [
        {
            "policy_decision_id": "reviewed_release_policy_20260322_v1",
            "game_id": "0029700159",
            "release_gate_status": "documented_hold",
            "release_reason_code": "source_limited_tradeoff_hold",
            "execution_lane": "documented_hold",
            "blocks_release": False,
            "research_open": True,
            "policy_source": "reviewed_override",
            "expected_primary_quality_status": "open",
            "evidence_artifact": "artifact",
            "reviewed_at": "2026-03-22",
            "notes": "",
        },
        {
            "policy_decision_id": "reviewed_release_policy_20260322_v1",
            "game_id": "0029701075",
            "release_gate_status": "documented_hold",
            "release_reason_code": "scrambled_pbp_missing_subs_blockA",
            "execution_lane": "documented_hold",
            "blocks_release": False,
            "research_open": True,
            "policy_source": "reviewed_override",
            "expected_primary_quality_status": "open",
            "evidence_artifact": "artifact",
            "reviewed_at": "2026-03-22",
            "notes": "",
        },
    ]


def _raw_game_quality_rows(exact_match: bool) -> list[dict[str, object]]:
    rows = [
        {
            "game_id": "0029700159",
            "primary_quality_status": "open",
            "has_event_on_court_issue": True,
            "has_material_minute_issue": True,
            "has_severe_minute_issue": True,
            "n_actionable_event_rows": 1,
            "max_abs_minute_diff": 1.85,
            "n_pm_reference_delta_rows": 3,
        }
    ]
    if exact_match:
        rows.append(
            {
                "game_id": "0029701075",
                "primary_quality_status": "open",
                "has_event_on_court_issue": True,
                "has_material_minute_issue": True,
                "has_severe_minute_issue": True,
                "n_actionable_event_rows": 2,
                "max_abs_minute_diff": 2.15,
                "n_pm_reference_delta_rows": 4,
            }
        )
    return rows


def test_build_raw_frontier_diff_reports_drift_and_metric_changes(tmp_path: Path) -> None:
    inventory_csv = tmp_path / "phase6_open_blocker_inventory_20260322_v1.csv"
    fresh_game_quality_csv = tmp_path / "game_quality.csv"
    output_dir = tmp_path / "frontier_diff"

    _write_csv(inventory_csv, list(_inventory_rows()[0].keys()), _inventory_rows())
    _write_csv(
        fresh_game_quality_csv,
        [
            "game_id",
            "primary_quality_status",
            "has_event_on_court_issue",
            "has_material_minute_issue",
            "has_severe_minute_issue",
            "n_actionable_event_rows",
            "max_abs_minute_diff",
            "n_pm_reference_delta_rows",
        ],
        [
            {
                "game_id": "0029701075",
                "primary_quality_status": "open",
                "has_event_on_court_issue": True,
                "has_material_minute_issue": True,
                "has_severe_minute_issue": True,
                "n_actionable_event_rows": 3,
                "max_abs_minute_diff": 2.65,
                "n_pm_reference_delta_rows": 5,
            },
            {
                "game_id": "0029702001",
                "primary_quality_status": "open",
                "has_event_on_court_issue": True,
                "has_material_minute_issue": True,
                "has_severe_minute_issue": False,
                "n_actionable_event_rows": 1,
                "max_abs_minute_diff": 0.5,
                "n_pm_reference_delta_rows": 1,
            },
        ],
    )

    summary = phase7.build_raw_frontier_diff(
        current_inventory_csv=inventory_csv,
        fresh_game_quality_csv=fresh_game_quality_csv,
        output_dir=output_dir,
    )

    assert summary["exact_match"] is False
    assert summary["left_open_set_game_ids"] == ["0029700159"]
    assert summary["joined_open_set_game_ids"] == ["0029702001"]
    assert summary["unchanged_open_set_game_ids"] == ["0029701075"]
    assert summary["unchanged_metric_shift_game_ids"] == ["0029701075"]


def test_parse_args_defaults_resume_and_workers() -> None:
    args = phase7.parse_args([])

    assert args.resume_from == "start"
    assert args.max_workers == 8
    assert args.fallback_max_workers == 0
    assert args.runtime_input_cache_mode == "reuse-validated-cache"
    assert args.audit_profile == "full"


def test_stitch_consolidated_candidate_parquet_writes_expected_summary(tmp_path: Path, monkeypatch) -> None:
    candidate_run_dir = tmp_path / "candidate_run"
    monkeypatch.setattr(phase7, "PHASE7_SEASONS", [1997, 1998])

    _write_candidate_run_bundle(
        candidate_run_dir,
        {
            1997: [
                _base_stat_row(29700159, 1, 101, PTS=10),
                _base_stat_row(29700159, 1, 102, AST=3),
            ],
            1998: [_base_stat_row(29701075, 2, 201, REB=4)],
        },
    )

    summary = phase7.stitch_consolidated_candidate_parquet(
        candidate_run_dir=candidate_run_dir,
        expected_total_rows=3,
    )

    assert summary["total_rows"] == 3
    assert summary["season_input_count"] == 2
    assert (candidate_run_dir / "darko_1997_2020.parquet").exists()
    df = pd.read_parquet(candidate_run_dir / "darko_1997_2020.parquet", columns=["Game_SingleGame"])
    assert len(df) == 3


def test_compare_consolidated_parquets_allows_row_count_delta_when_stats_clean(tmp_path: Path) -> None:
    compare_dir = tmp_path / "compare"
    baseline_parquet = tmp_path / "baseline.parquet"
    candidate_parquet = tmp_path / "candidate.parquet"

    _write_parquet(
        baseline_parquet,
        [
            _base_stat_row(1, 10, 100, PTS=8),
            _base_stat_row(2, 20, 200, AST=2),
        ],
    )
    _write_parquet(
        candidate_parquet,
        [
            _base_stat_row(1, 10, 100, PTS=8),
            _base_stat_row(2, 20, 200, AST=2),
            _base_stat_row(3, 30, 300, REB=5),
        ],
    )

    summary = phase7.compare_consolidated_parquets(
        baseline_parquet=baseline_parquet,
        candidate_parquet=candidate_parquet,
        output_dir=compare_dir,
        expected_baseline_total_rows=2,
    )

    assert summary["passed"] is True
    assert summary["row_count_delta"] == 1
    assert summary["added_key_count"] == 1
    assert summary["counting_stat_diff_row_count"] == 0


def test_compare_consolidated_parquets_derives_reb_when_column_missing(tmp_path: Path) -> None:
    compare_dir = tmp_path / "compare"
    baseline_parquet = tmp_path / "baseline.parquet"
    candidate_parquet = tmp_path / "candidate.parquet"

    baseline_df = pd.DataFrame(
        [
            {
                "Game_SingleGame": 1,
                "Team_SingleGame": 10,
                "NbaDotComID": 100,
                "PTS": 8,
                "AST": 0,
                "STL": 0,
                "BLK": 0,
                "TOV": 0,
                "PF": 0,
                "FGM": 0,
                "FGA": 0,
                "3PM": 0,
                "3PA": 0,
                "FTM": 0,
                "FTA": 0,
                "OREB": 2,
                "DRB": 3,
            }
        ]
    )
    candidate_df = baseline_df.copy()
    baseline_df.to_parquet(baseline_parquet, index=False)
    candidate_df.to_parquet(candidate_parquet, index=False)

    summary = phase7.compare_consolidated_parquets(
        baseline_parquet=baseline_parquet,
        candidate_parquet=candidate_parquet,
        output_dir=compare_dir,
        expected_baseline_total_rows=1,
    )

    assert summary["passed"] is True
    assert summary["counting_stat_diff_counts"]["REB"]["diff_count"] == 0


def test_compare_consolidated_parquets_fails_on_counting_stat_diff(tmp_path: Path) -> None:
    compare_dir = tmp_path / "compare"
    baseline_parquet = tmp_path / "baseline.parquet"
    candidate_parquet = tmp_path / "candidate.parquet"

    _write_parquet(baseline_parquet, [_base_stat_row(1, 10, 100, PTS=8)])
    _write_parquet(candidate_parquet, [_base_stat_row(1, 10, 100, PTS=9)])

    summary = phase7.compare_consolidated_parquets(
        baseline_parquet=baseline_parquet,
        candidate_parquet=candidate_parquet,
        output_dir=compare_dir,
        expected_baseline_total_rows=1,
    )

    assert summary["passed"] is False
    assert "counting_stat_differences" in summary["gate_failures"]


def test_phase7_main_resume_post_rerun_allows_report_only_season_compare_regressions(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(phase7, "PHASE7_SEASONS", [1997, 1998])

    output_dir = tmp_path / "phase7_resume"
    baseline_dir = tmp_path / "baseline"
    candidate_run_dir = tmp_path / "candidate_run"
    raw_residual_dir = tmp_path / "raw_residual"
    reviewed_residual_dir = tmp_path / "reviewed_residual"
    frontier_diff_dir = tmp_path / "frontier_diff"
    reviewed_frontier_dir = tmp_path / "reviewed_frontier"
    reviewed_pm_dir = tmp_path / "reviewed_pm"
    reviewed_sidecar_dir = tmp_path / "sidecar"
    smoke_dir = tmp_path / "smoke"
    staged_baseline_dir = tmp_path / "staged" / "full_history_1997_20260322_v1"
    compile_summary_json = tmp_path / "compile_summary.json"
    canary_summary_json = tmp_path / "canary_summary.json"
    overlay_csv = tmp_path / "overlay.csv"
    inventory_csv = tmp_path / "inventory.csv"
    shortlist_csv = tmp_path / "shortlist.csv"

    _write_json(compile_summary_json, TEST_COMPILE_SUMMARY)
    _write_json(
        canary_summary_json,
        {"suite_pass": True, "suite_pass_all_cases": True, "suite_pass_stable_cases_only": True},
    )
    _write_baseline_dir(
        baseline_dir,
        [
            _base_stat_row(29700159, 1, 101, PTS=10),
            _base_stat_row(29701075, 2, 201, REB=4),
        ],
    )
    _write_candidate_run_bundle(
        candidate_run_dir,
        {
            1997: [_base_stat_row(29700159, 1, 101, PTS=10)],
            1998: [
                _base_stat_row(29701075, 2, 201, REB=4),
                _base_stat_row(29701075, 2, 202, AST=1),
            ],
        },
    )
    _write_csv(inventory_csv, list(_inventory_rows()[0].keys()), _inventory_rows())
    _write_csv(shortlist_csv, list(_shortlist_rows()[0].keys()), _shortlist_rows())
    _write_csv(overlay_csv, list(_overlay_rows()[0].keys()), _overlay_rows())
    monkeypatch.setattr(phase7, "_resolve_pbpstats_repo", lambda: tmp_path / "pbpstats_repo")

    def fake_run_command(args: list[str], *, log_path: Path, allow_exit_codes: set[int] | None = None):
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text("", encoding="utf-8")
        script_name = Path(args[1]).name
        if script_name == "build_override_runtime_views.py":
            return subprocess.CompletedProcess(args, 0, "", "")
        if script_name == "compare_run_outputs.py":
            payload = {
                "baseline_dir": str(baseline_dir),
                "candidate_dir": str(candidate_run_dir),
                "seasons": [
                    {
                        "season": 1997,
                        "regressions": [
                            "rebound_fallback_deletions regressed: 1 vs 0 (+1)",
                            "games_with_team_mismatch regressed: 1 vs 0 (+1)",
                        ],
                        "improvements": [],
                        "notes": [],
                    },
                    {"season": 1998, "regressions": [], "improvements": [], "notes": []},
                ],
            }
            return subprocess.CompletedProcess(args, 1, json.dumps(payload), "")
        if script_name == "build_lineup_residual_outputs.py":
            if str(raw_residual_dir) in args:
                _write_csv(
                    raw_residual_dir / "game_quality.csv",
                    list(_raw_game_quality_rows(exact_match=False)[0].keys()),
                    _raw_game_quality_rows(exact_match=False),
                )
                _write_json(raw_residual_dir / "summary.json", {"raw_counts": {"failed_games": 0, "event_stats_errors": 0}})
                return subprocess.CompletedProcess(args, 0, "", "")
            raise AssertionError("Reviewed rebuild should not run on frontier drift")
        raise AssertionError(f"Unexpected command: {args}")

    monkeypatch.setattr(phase7, "_run_command", fake_run_command)

    result = phase7.main(
        [
            "--resume-from",
            "post_rerun",
            "--output-dir",
            str(output_dir),
            "--baseline-dir",
            str(baseline_dir),
            "--candidate-run-dir",
            str(candidate_run_dir),
            "--raw-residual-dir",
            str(raw_residual_dir),
            "--frontier-diff-dir",
            str(frontier_diff_dir),
            "--reviewed-residual-dir",
            str(reviewed_residual_dir),
            "--reviewed-frontier-dir",
            str(reviewed_frontier_dir),
            "--reviewed-pm-dir",
            str(reviewed_pm_dir),
            "--reviewed-sidecar-dir",
            str(reviewed_sidecar_dir),
            "--sidecar-smoke-dir",
            str(smoke_dir),
            "--staged-baseline-dir",
            str(staged_baseline_dir),
            "--compile-summary-json",
            str(compile_summary_json),
            "--golden-canary-summary-json",
            str(canary_summary_json),
            "--reviewed-policy-overlay-csv",
            str(overlay_csv),
            "--frontier-inventory-csv",
            str(inventory_csv),
            "--shortlist-csv",
            str(shortlist_csv),
            "--expected-candidate-total-rows",
            "3",
            "--expected-baseline-total-rows",
            "2",
        ]
    )

    assert result == 2
    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["compare"]["passed"] is True
    assert summary["compare"]["raw_returncode"] == 1
    assert summary["compare"]["blocking_regression_count"] == 0
    assert summary["compare"]["report_only_regression_count"] == 1
    assert summary["stop_reason"] == "raw_open_frontier_drift"


def test_phase7_main_resume_post_rerun_stops_on_frontier_drift(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(phase7, "PHASE7_SEASONS", [1997, 1998])

    output_dir = tmp_path / "phase7_resume"
    baseline_dir = tmp_path / "baseline"
    candidate_run_dir = tmp_path / "candidate_run"
    raw_residual_dir = tmp_path / "raw_residual"
    reviewed_residual_dir = tmp_path / "reviewed_residual"
    frontier_diff_dir = tmp_path / "frontier_diff"
    reviewed_frontier_dir = tmp_path / "reviewed_frontier"
    reviewed_pm_dir = tmp_path / "reviewed_pm"
    reviewed_sidecar_dir = tmp_path / "sidecar"
    smoke_dir = tmp_path / "smoke"
    staged_baseline_dir = tmp_path / "staged" / "full_history_1997_2020_20260322_v1"
    compile_summary_json = tmp_path / "compile_summary.json"
    canary_summary_json = tmp_path / "canary_summary.json"
    overlay_csv = tmp_path / "overlay.csv"
    inventory_csv = tmp_path / "inventory.csv"
    shortlist_csv = tmp_path / "shortlist.csv"

    _write_json(compile_summary_json, TEST_COMPILE_SUMMARY)
    _write_json(
        canary_summary_json,
        {"suite_pass": True, "suite_pass_all_cases": True, "suite_pass_stable_cases_only": True},
    )
    _write_baseline_dir(
        baseline_dir,
        [
            _base_stat_row(29700159, 1, 101, PTS=10),
            _base_stat_row(29701075, 2, 201, REB=4),
        ],
    )
    _write_candidate_run_bundle(
        candidate_run_dir,
        {
            1997: [_base_stat_row(29700159, 1, 101, PTS=10)],
            1998: [
                _base_stat_row(29701075, 2, 201, REB=4),
                _base_stat_row(29701075, 2, 202, AST=1),
            ],
        },
    )
    _write_csv(inventory_csv, list(_inventory_rows()[0].keys()), _inventory_rows())
    _write_csv(shortlist_csv, list(_shortlist_rows()[0].keys()), _shortlist_rows())
    _write_csv(overlay_csv, list(_overlay_rows()[0].keys()), _overlay_rows())
    monkeypatch.setattr(phase7, "_resolve_pbpstats_repo", lambda: tmp_path / "pbpstats_repo")

    def fake_run_command(args: list[str], *, log_path: Path, allow_exit_codes: set[int] | None = None):
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text("", encoding="utf-8")
        script_name = Path(args[1]).name
        if script_name == "build_override_runtime_views.py":
            return subprocess.CompletedProcess(args, 0, "", "")
        if script_name == "compare_run_outputs.py":
            payload = {
                "baseline_dir": str(baseline_dir),
                "candidate_dir": str(candidate_run_dir),
                "seasons": [
                    {"season": 1997, "regressions": [], "improvements": [], "notes": []},
                    {"season": 1998, "regressions": [], "improvements": [], "notes": []},
                ],
            }
            return subprocess.CompletedProcess(args, 0, json.dumps(payload), "")
        if script_name == "build_lineup_residual_outputs.py":
            if str(raw_residual_dir) in args:
                _write_csv(
                    raw_residual_dir / "game_quality.csv",
                    list(_raw_game_quality_rows(exact_match=False)[0].keys()),
                    _raw_game_quality_rows(exact_match=False),
                )
                _write_json(raw_residual_dir / "summary.json", {"raw_counts": {"failed_games": 0, "event_stats_errors": 0}})
                return subprocess.CompletedProcess(args, 0, "", "")
            raise AssertionError("Reviewed rebuild should not run on frontier drift")
        raise AssertionError(f"Unexpected command: {args}")

    monkeypatch.setattr(phase7, "_run_command", fake_run_command)

    result = phase7.main(
        [
            "--resume-from",
            "post_rerun",
            "--output-dir",
            str(output_dir),
            "--baseline-dir",
            str(baseline_dir),
            "--candidate-run-dir",
            str(candidate_run_dir),
            "--raw-residual-dir",
            str(raw_residual_dir),
            "--frontier-diff-dir",
            str(frontier_diff_dir),
            "--reviewed-residual-dir",
            str(reviewed_residual_dir),
            "--reviewed-frontier-dir",
            str(reviewed_frontier_dir),
            "--reviewed-pm-dir",
            str(reviewed_pm_dir),
            "--reviewed-sidecar-dir",
            str(reviewed_sidecar_dir),
            "--sidecar-smoke-dir",
            str(smoke_dir),
            "--staged-baseline-dir",
            str(staged_baseline_dir),
            "--compile-summary-json",
            str(compile_summary_json),
            "--golden-canary-summary-json",
            str(canary_summary_json),
            "--reviewed-policy-overlay-csv",
            str(overlay_csv),
            "--frontier-inventory-csv",
            str(inventory_csv),
            "--shortlist-csv",
            str(shortlist_csv),
            "--expected-candidate-total-rows",
            "3",
            "--expected-baseline-total-rows",
            "2",
        ]
    )

    assert result == 2
    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["stop_reason"] == "raw_open_frontier_drift"
    assert summary["rerun"]["skipped"] is True
    assert summary["consolidation"]["total_rows"] == 3
    assert summary["consolidated_compare"]["passed"] is True
    assert summary["reviewed_rebuild"]["attempted"] is False


def test_phase7_main_resume_post_rerun_rebuilds_and_stages_on_exact_match(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(phase7, "PHASE7_SEASONS", [1997, 1998])

    output_dir = tmp_path / "phase7_resume"
    baseline_dir = tmp_path / "baseline"
    candidate_run_dir = tmp_path / "candidate_run"
    raw_residual_dir = tmp_path / "raw_residual"
    reviewed_residual_dir = tmp_path / "reviewed_residual"
    frontier_diff_dir = tmp_path / "frontier_diff"
    reviewed_frontier_dir = tmp_path / "reviewed_frontier"
    reviewed_pm_dir = tmp_path / "reviewed_pm"
    reviewed_sidecar_dir = tmp_path / "sidecar"
    smoke_dir = tmp_path / "smoke"
    staged_baseline_dir = tmp_path / "staged" / "full_history_1997_2020_20260322_v1"
    compile_summary_json = tmp_path / "compile_summary.json"
    canary_summary_json = tmp_path / "canary_summary.json"
    overlay_csv = tmp_path / "overlay.csv"
    inventory_csv = tmp_path / "inventory.csv"
    shortlist_csv = tmp_path / "shortlist.csv"

    _write_json(compile_summary_json, TEST_COMPILE_SUMMARY)
    _write_json(
        canary_summary_json,
        {"suite_pass": True, "suite_pass_all_cases": True, "suite_pass_stable_cases_only": True},
    )
    _write_baseline_dir(
        baseline_dir,
        [
            _base_stat_row(29700159, 1, 101, PTS=10),
            _base_stat_row(29701075, 2, 201, REB=4),
        ],
    )
    _write_candidate_run_bundle(
        candidate_run_dir,
        {
            1997: [_base_stat_row(29700159, 1, 101, PTS=10)],
            1998: [
                _base_stat_row(29701075, 2, 201, REB=4),
                _base_stat_row(29701075, 2, 202, AST=1),
            ],
        },
    )
    _write_csv(inventory_csv, list(_inventory_rows()[0].keys()), _inventory_rows())
    _write_csv(shortlist_csv, list(_shortlist_rows()[0].keys()), _shortlist_rows())
    _write_csv(overlay_csv, list(_overlay_rows()[0].keys()), _overlay_rows())
    monkeypatch.setattr(phase7, "_resolve_pbpstats_repo", lambda: tmp_path / "pbpstats_repo")

    def fake_run_command(args: list[str], *, log_path: Path, allow_exit_codes: set[int] | None = None):
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text("", encoding="utf-8")
        script_name = Path(args[1]).name
        if script_name == "build_override_runtime_views.py":
            return subprocess.CompletedProcess(args, 0, "", "")
        if script_name == "compare_run_outputs.py":
            payload = {
                "baseline_dir": str(baseline_dir),
                "candidate_dir": str(candidate_run_dir),
                "seasons": [
                    {"season": 1997, "regressions": [], "improvements": [], "notes": []},
                    {"season": 1998, "regressions": [], "improvements": [], "notes": []},
                ],
            }
            return subprocess.CompletedProcess(args, 0, json.dumps(payload), "")
        if script_name == "build_lineup_residual_outputs.py":
            if str(raw_residual_dir) in args:
                _write_csv(
                    raw_residual_dir / "game_quality.csv",
                    list(_raw_game_quality_rows(exact_match=True)[0].keys()),
                    _raw_game_quality_rows(exact_match=True),
                )
                _write_json(raw_residual_dir / "summary.json", {"raw_counts": {"failed_games": 0, "event_stats_errors": 0}})
                return subprocess.CompletedProcess(args, 0, "", "")
            if str(reviewed_residual_dir) in args:
                _write_csv(
                    reviewed_residual_dir / "game_quality.csv",
                    [
                        "game_id",
                        "primary_quality_status",
                        "release_gate_status",
                        "release_reason_code",
                        "execution_lane",
                        "blocks_release",
                        "research_open",
                        "policy_source",
                        "has_event_on_court_issue",
                        "has_material_minute_issue",
                        "has_severe_minute_issue",
                        "n_actionable_event_rows",
                        "max_abs_minute_diff",
                        "n_pm_reference_delta_rows",
                    ],
                    [
                        {
                            "game_id": "0029700159",
                            "primary_quality_status": "open",
                            "release_gate_status": "documented_hold",
                            "release_reason_code": "source_limited_tradeoff_hold",
                            "execution_lane": "documented_hold",
                            "blocks_release": False,
                            "research_open": True,
                            "policy_source": "reviewed_override",
                            "has_event_on_court_issue": True,
                            "has_material_minute_issue": True,
                            "has_severe_minute_issue": True,
                            "n_actionable_event_rows": 1,
                            "max_abs_minute_diff": 1.85,
                            "n_pm_reference_delta_rows": 3,
                        },
                        {
                            "game_id": "0029701075",
                            "primary_quality_status": "open",
                            "release_gate_status": "documented_hold",
                            "release_reason_code": "scrambled_pbp_missing_subs_blockA",
                            "execution_lane": "documented_hold",
                            "blocks_release": False,
                            "research_open": True,
                            "policy_source": "reviewed_override",
                            "has_event_on_court_issue": True,
                            "has_material_minute_issue": True,
                            "has_severe_minute_issue": True,
                            "n_actionable_event_rows": 2,
                            "max_abs_minute_diff": 2.15,
                            "n_pm_reference_delta_rows": 4,
                        },
                        {
                            "game_id": "0029800606",
                            "primary_quality_status": "boundary_difference",
                            "release_gate_status": "accepted_boundary_difference",
                            "release_reason_code": "exact",
                            "execution_lane": "policy_frontier_non_local",
                            "blocks_release": False,
                            "research_open": False,
                            "policy_source": "auto_default",
                            "has_event_on_court_issue": False,
                            "has_material_minute_issue": False,
                            "has_severe_minute_issue": False,
                            "n_actionable_event_rows": 0,
                            "max_abs_minute_diff": 0.0,
                            "n_pm_reference_delta_rows": 0,
                        },
                    ],
                )
                _write_json(
                    reviewed_residual_dir / "summary.json",
                    {
                        "release_blocking_game_count": 0,
                        "tier1_release_ready": True,
                        "research_open_game_count": 2,
                    },
                )
                return subprocess.CompletedProcess(args, 0, "", "")
        if script_name == "build_reviewed_frontier_inventory.py":
            _write_json(
                reviewed_frontier_dir / "summary.json",
                {
                    "release_blocking_game_count": 0,
                    "research_open_game_count": 2,
                    "research_open_game_ids": ["0029700159", "0029701075"],
                    "tier2_frontier_closed": False,
                    "reviewed_policy_overlay_version": "reviewed_release_policy_20260322_v1",
                    "frontier_inventory_snapshot_id": "inventory",
                },
            )
            return subprocess.CompletedProcess(args, 0, "", "")
        if script_name == "build_plus_minus_reference_report.py":
            _write_json(
                reviewed_pm_dir / "summary.json",
                {
                    "release_blocker_game_count": 0,
                    "reviewed_policy_overlay_version": "reviewed_release_policy_20260322_v1",
                    "frontier_inventory_snapshot_id": "inventory",
                },
            )
            return subprocess.CompletedProcess(args, 0, "", "")
        if script_name == "build_reviewed_release_quality_sidecar.py":
            _write_json(
                reviewed_sidecar_dir / "summary.json",
                {
                    "release_blocking_game_count": 0,
                    "research_open_game_ids": ["0029700159", "0029701075"],
                    "reviewed_policy_overlay_version": "reviewed_release_policy_20260322_v1",
                    "frontier_inventory_snapshot_id": "inventory",
                },
            )
            return subprocess.CompletedProcess(args, 0, "", "")
        if script_name == "smoke_test_reviewed_release_quality_sidecar_join.py":
            _write_json(
                smoke_dir / "summary.json",
                {
                    "join_passed": True,
                    "reviewed_policy_overlay_version": "reviewed_release_policy_20260322_v1",
                    "frontier_inventory_snapshot_id": "inventory",
                },
            )
            return subprocess.CompletedProcess(args, 0, "", "")
        raise AssertionError(f"Unexpected command: {args}")

    monkeypatch.setattr(phase7, "_run_command", fake_run_command)

    result = phase7.main(
        [
            "--resume-from",
            "post_rerun",
            "--output-dir",
            str(output_dir),
            "--baseline-dir",
            str(baseline_dir),
            "--candidate-run-dir",
            str(candidate_run_dir),
            "--raw-residual-dir",
            str(raw_residual_dir),
            "--frontier-diff-dir",
            str(frontier_diff_dir),
            "--reviewed-residual-dir",
            str(reviewed_residual_dir),
            "--reviewed-frontier-dir",
            str(reviewed_frontier_dir),
            "--reviewed-pm-dir",
            str(reviewed_pm_dir),
            "--reviewed-sidecar-dir",
            str(reviewed_sidecar_dir),
            "--sidecar-smoke-dir",
            str(smoke_dir),
            "--staged-baseline-dir",
            str(staged_baseline_dir),
            "--compile-summary-json",
            str(compile_summary_json),
            "--golden-canary-summary-json",
            str(canary_summary_json),
            "--reviewed-policy-overlay-csv",
            str(overlay_csv),
            "--frontier-inventory-csv",
            str(inventory_csv),
            "--shortlist-csv",
            str(shortlist_csv),
            "--expected-candidate-total-rows",
            "3",
            "--expected-baseline-total-rows",
            "2",
        ]
    )

    assert result == 0
    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["phase7_passed"] is True
    assert summary["stop_reason"] == ""
    assert summary["rerun"]["skipped"] is True
    assert summary["consolidation"]["total_rows"] == 3
    assert summary["consolidated_compare"]["passed"] is True
    assert summary["raw_frontier_diff"]["exact_match"] is True
    assert summary["reviewed_rebuild"]["passed"] is True
    assert summary["promotion"]["promoted"] is True
    assert (staged_baseline_dir / "darko_1997_2020.parquet").exists()
