import json
from pathlib import Path

from historic_backfill.runners.summarize_opening_cluster_pass0 import _delta_metrics, _extract_metrics


def test_extract_metrics_reads_lineup_and_boxscore_fields() -> None:
    summary = {
        "failed_games": 0,
        "event_stats_errors": 0,
        "boxscore_audit": {"audit_failures": 0, "team_rows_with_mismatch": 0, "player_rows_with_mismatch": 0},
        "lineup_audit": {
            "minutes_plus_minus": {"minutes_mismatches": 1, "minutes_outliers": 0, "plus_minus_mismatches": 2},
            "problem_games": 3,
            "event_on_court": {"issue_rows": 4, "issue_games": 2},
        },
    }

    metrics = _extract_metrics(summary)

    assert metrics["minutes_mismatches"] == 1
    assert metrics["plus_minus_mismatches"] == 2
    assert metrics["problem_games"] == 3
    assert metrics["event_on_court_issue_rows"] == 4


def test_delta_metrics_is_result_minus_baseline() -> None:
    result = {"minutes_outliers": 0, "event_on_court_issue_rows": 1}
    baseline = {"minutes_outliers": 2, "event_on_court_issue_rows": 3}

    deltas = _delta_metrics(result, baseline)

    assert deltas == {"minutes_outliers": -2, "event_on_court_issue_rows": -2}
