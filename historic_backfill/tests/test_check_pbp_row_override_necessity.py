import pandas as pd

from historic_backfill.runners.check_pbp_row_override_necessity import _compare_boxes, _result_row
from historic_backfill.common.override_necessity_utils import GameVariantMetrics, diff_pipeline_metrics


def _box_row(player_id: int, team_id: int, **stats: int) -> dict:
    row = {"player_id": player_id, "team_id": team_id}
    row.update(stats)
    return row


def test_compare_boxes_counts_changed_players_and_cells_including_pf():
    box_with = pd.DataFrame(
        [
            _box_row(1, 10, PTS=10, AST=2, REB=5, PF=2),
            _box_row(2, 10, PTS=7, AST=1, REB=3, PF=4),
        ]
    )
    box_without = pd.DataFrame(
        [
            _box_row(1, 10, PTS=10, AST=1, REB=5, PF=2),
            _box_row(2, 10, PTS=6, AST=1, REB=2, PF=3),
        ]
    )

    mismatch_players, mismatch_cells = _compare_boxes(box_with, box_without)

    assert mismatch_players == 2
    assert mismatch_cells == 4


def test_result_row_marks_without_override_parse_errors_as_active():
    game_rows = pd.DataFrame([{"game_id": "20000001", "action": "drop", "event_num": "10"}])
    with_metrics = GameVariantMetrics()
    without_metrics = GameVariantMetrics(error="EventOrderError: broken ordering without manual row fix")

    row = _result_row(
        "0020000001",
        game_rows,
        with_metrics=with_metrics,
        without_metrics=without_metrics,
        changed_pipeline_metrics=diff_pipeline_metrics(with_metrics, without_metrics),
    )

    assert row["status"] == "active"
    assert row["row_override_count"] == 1
    assert row["changed_players"] == 0
    assert row["changed_cells"] == 0
    assert "EventOrderError" in row["without_override_error"]


def test_result_row_marks_pipeline_metric_diffs_as_active():
    game_rows = pd.DataFrame([{"game_id": "20000002", "action": "drop", "event_num": "11"}])
    with_metrics = GameVariantMetrics(rebound_deletions=0, audit_player_rows=0)
    without_metrics = GameVariantMetrics(rebound_deletions=1, audit_player_rows=0)

    row = _result_row(
        "0020000002",
        game_rows,
        with_metrics=with_metrics,
        without_metrics=without_metrics,
        changed_pipeline_metrics=diff_pipeline_metrics(with_metrics, without_metrics),
    )

    assert row["status"] == "active"
    assert row["changed_pipeline_metrics"] == "rebound_deletions:1->0"


def test_result_row_marks_with_override_parse_errors_for_review():
    game_rows = pd.DataFrame([{"game_id": "20000003", "action": "move_before", "event_num": "11"}])
    with_metrics = GameVariantMetrics(error="RuntimeError: unexpected failure with override enabled")
    without_metrics = GameVariantMetrics()

    row = _result_row(
        "0020000003",
        game_rows,
        with_metrics=with_metrics,
        without_metrics=without_metrics,
        changed_pipeline_metrics=diff_pipeline_metrics(with_metrics, without_metrics),
    )

    assert row["status"] == "needs_review_with_override_error"
    assert "RuntimeError" in row["with_override_error"]
