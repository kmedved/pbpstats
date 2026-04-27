from __future__ import annotations

import json

import pandas as pd

from historic_backfill.runners.compare_run_outputs import (
    INVALID_TEAM_TECH_NORMALIZATION,
    compare_runs,
    summarize_output_dir,
)


def test_compare_runs_flags_regressions_and_improvements(tmp_path) -> None:
    baseline_dir = tmp_path / "baseline"
    candidate_dir = tmp_path / "candidate"
    baseline_dir.mkdir()
    candidate_dir.mkdir()

    season = 1998

    baseline_summary = {
        "season": season,
        "player_rows": 100,
        "failed_games": 0,
        "event_stats_errors": 2,
    }
    candidate_summary = {
        "season": season,
        "player_rows": 102,
        "failed_games": 1,
        "event_stats_errors": 1,
    }

    (baseline_dir / f"summary_{season}.json").write_text(json.dumps(baseline_summary), encoding="utf-8")
    (candidate_dir / f"summary_{season}.json").write_text(json.dumps(candidate_summary), encoding="utf-8")

    pd.DataFrame({"Game_SingleGame": [1, 1, 2]}).to_parquet(baseline_dir / f"darko_{season}.parquet", index=False)
    pd.DataFrame({"Game_SingleGame": [1, 1, 2, 3]}).to_parquet(candidate_dir / f"darko_{season}.parquet", index=False)

    pd.DataFrame([{"game_id": 1, "error": "x"}, {"game_id": 2, "error": "y"}]).to_csv(
        baseline_dir / f"event_stats_errors_{season}.csv",
        index=False,
    )
    pd.DataFrame([{"game_id": 1, "error": "x"}]).to_csv(
        candidate_dir / f"event_stats_errors_{season}.csv",
        index=False,
    )

    pd.DataFrame([{"deleted_EVENTNUM": 1}, {"deleted_EVENTNUM": 2}]).to_csv(
        baseline_dir / f"rebound_fallback_deletions_{season}.csv",
        index=False,
    )
    pd.DataFrame([{"deleted_EVENTNUM": 1}]).to_csv(
        candidate_dir / f"rebound_fallback_deletions_{season}.csv",
        index=False,
    )

    baseline_metrics = summarize_output_dir(baseline_dir, season)
    candidate_metrics = summarize_output_dir(candidate_dir, season)
    regressions, improvements, notes = compare_runs(baseline_metrics, candidate_metrics)

    assert any("failed_games regressed" in item for item in regressions)
    assert any("player_rows improved" in item for item in improvements)
    assert any("event_stats_errors improved" in item for item in improvements)
    assert any("rebound_fallback_deletions improved" in item for item in improvements)
    assert any("darko_games changed" in item for item in notes)


def test_invalid_team_tech_normalization_filters_only_bogus_team_rows(tmp_path) -> None:
    baseline_dir = tmp_path / "baseline"
    candidate_dir = tmp_path / "candidate"
    baseline_dir.mkdir()
    candidate_dir.mkdir()

    season = 2020
    baseline_summary = {"season": season, "player_rows": 3, "failed_games": 0, "event_stats_errors": 0}
    candidate_summary = {"season": season, "player_rows": 2, "failed_games": 0, "event_stats_errors": 0}
    (baseline_dir / f"summary_{season}.json").write_text(json.dumps(baseline_summary), encoding="utf-8")
    (candidate_dir / f"summary_{season}.json").write_text(json.dumps(candidate_summary), encoding="utf-8")

    baseline_rows = pd.DataFrame(
        [
            {
                "Game_SingleGame": 21900339,
                "NbaDotComID": 0,
                "Team_SingleGame": 2623,
                "FullName": "Team Stats (2623)",
                "h_tm_id": 1610612764,
                "v_tm_id": 1610612746,
                "TECH": 1,
                "FLAGRANT": 0,
                "PTS": 0,
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
                "OREB": 0,
                "DRB": 0,
                "REB": 0,
            },
            {
                "Game_SingleGame": 21900339,
                "NbaDotComID": 0,
                "Team_SingleGame": 1610612764,
                "FullName": "Team Stats (1610612764)",
                "h_tm_id": 1610612764,
                "v_tm_id": 1610612746,
                "TECH": 1,
                "FLAGRANT": 0,
                "PTS": 0,
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
                "OREB": 0,
                "DRB": 0,
                "REB": 0,
            },
            {
                "Game_SingleGame": 21900339,
                "NbaDotComID": 202397,
                "Team_SingleGame": 1610612764,
                "FullName": "Ish Smith",
                "h_tm_id": 1610612764,
                "v_tm_id": 1610612746,
                "TECH": 1,
                "FLAGRANT": 0,
                "PTS": 0,
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
                "OREB": 0,
                "DRB": 0,
                "REB": 0,
            },
        ]
    )
    candidate_rows = baseline_rows.iloc[1:].copy()
    baseline_rows.to_parquet(baseline_dir / f"darko_{season}.parquet", index=False)
    candidate_rows.to_parquet(candidate_dir / f"darko_{season}.parquet", index=False)
    pd.DataFrame(columns=["game_id", "error"]).to_csv(
        baseline_dir / f"event_stats_errors_{season}.csv", index=False
    )
    pd.DataFrame(columns=["game_id", "error"]).to_csv(
        candidate_dir / f"event_stats_errors_{season}.csv", index=False
    )
    pd.DataFrame(columns=["deleted_EVENTNUM"]).to_csv(
        baseline_dir / f"rebound_fallback_deletions_{season}.csv", index=False
    )
    pd.DataFrame(columns=["deleted_EVENTNUM"]).to_csv(
        candidate_dir / f"rebound_fallback_deletions_{season}.csv", index=False
    )

    baseline_metrics = summarize_output_dir(
        baseline_dir, season, normalization_profile=INVALID_TEAM_TECH_NORMALIZATION
    )
    candidate_metrics = summarize_output_dir(
        candidate_dir, season, normalization_profile=INVALID_TEAM_TECH_NORMALIZATION
    )
    regressions, improvements, notes = compare_runs(baseline_metrics, candidate_metrics)

    assert baseline_metrics.raw_darko_rows == 3
    assert baseline_metrics.darko_rows == 2
    assert baseline_metrics.normalized_filtered_row_count == 1
    assert candidate_metrics.raw_darko_rows == 2
    assert candidate_metrics.darko_rows == 2
    assert candidate_metrics.normalized_filtered_row_count == 0
    assert regressions == []
    assert improvements == []
    assert any("filtered normalization rows: baseline=1, candidate=0" in item for item in notes)


def test_invalid_team_tech_normalization_handles_missing_reb_column(tmp_path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()

    season = 2020
    summary = {"season": season, "player_rows": 2, "failed_games": 0, "event_stats_errors": 0}
    (run_dir / f"summary_{season}.json").write_text(json.dumps(summary), encoding="utf-8")

    rows = pd.DataFrame(
        [
            {
                "Game_SingleGame": 21900339,
                "NbaDotComID": 0,
                "Team_SingleGame": 2623,
                "FullName": "Team Stats (2623)",
                "h_tm_id": 1610612764,
                "v_tm_id": 1610612746,
                "TECH": 1,
                "FLAGRANT": 0,
                "PTS": 0,
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
                "OREB": 0,
                "DRB": 0,
            },
            {
                "Game_SingleGame": 21900339,
                "NbaDotComID": 202397,
                "Team_SingleGame": 1610612764,
                "FullName": "Ish Smith",
                "h_tm_id": 1610612764,
                "v_tm_id": 1610612746,
                "TECH": 1,
                "FLAGRANT": 0,
                "PTS": 0,
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
                "OREB": 0,
                "DRB": 0,
            },
        ]
    )
    rows.to_parquet(run_dir / f"darko_{season}.parquet", index=False)
    pd.DataFrame(columns=["game_id", "error"]).to_csv(run_dir / f"event_stats_errors_{season}.csv", index=False)
    pd.DataFrame(columns=["deleted_EVENTNUM"]).to_csv(
        run_dir / f"rebound_fallback_deletions_{season}.csv", index=False
    )

    metrics = summarize_output_dir(run_dir, season, normalization_profile=INVALID_TEAM_TECH_NORMALIZATION)

    assert metrics.raw_darko_rows == 2
    assert metrics.darko_rows == 1
    assert metrics.normalized_filtered_row_count == 1


def test_invalid_team_tech_normalization_filters_team_stats_zero_placeholder(tmp_path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()

    season = 1997
    summary = {"season": season, "player_rows": 2, "failed_games": 0, "event_stats_errors": 0}
    (run_dir / f"summary_{season}.json").write_text(json.dumps(summary), encoding="utf-8")

    rows = pd.DataFrame(
        [
            {
                "Game_SingleGame": 29600332,
                "NbaDotComID": 0,
                "Team_SingleGame": 0,
                "FullName": "Team Stats (0)",
                "h_tm_id": 1610612760,
                "v_tm_id": 1610612744,
                "TECH": 0,
                "FLAGRANT": 0,
                "PTS": 0,
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
                "OREB": 0,
                "DRB": 0,
            },
            {
                "Game_SingleGame": 29601151,
                "NbaDotComID": 0,
                "Team_SingleGame": 1610612738,
                "FullName": "Team Stats (1610612738)",
                "h_tm_id": 1610612738,
                "v_tm_id": 1610612766,
                "TECH": 1,
                "FLAGRANT": 0,
                "PTS": 0,
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
                "OREB": 0,
                "DRB": 0,
            },
        ]
    )
    rows.to_parquet(run_dir / f"darko_{season}.parquet", index=False)
    pd.DataFrame(columns=["game_id", "error"]).to_csv(run_dir / f"event_stats_errors_{season}.csv", index=False)
    pd.DataFrame(columns=["deleted_EVENTNUM"]).to_csv(
        run_dir / f"rebound_fallback_deletions_{season}.csv", index=False
    )

    metrics = summarize_output_dir(run_dir, season, normalization_profile=INVALID_TEAM_TECH_NORMALIZATION)

    assert metrics.raw_darko_rows == 2
    assert metrics.darko_rows == 1
    assert metrics.normalized_filtered_row_count == 1
