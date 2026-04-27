import pandas as pd

from check_pbp_stat_override_necessity import _result_row
from override_necessity_utils import GameVariantMetrics


def _box_row(player_id=1, team_id=2, **stats):
    row = {"player_id": player_id, "team_id": team_id}
    row.update(stats)
    return pd.DataFrame([row])


def test_result_row_marks_changed_basic_stat_active():
    row = {
        "game_id": "0020000001",
        "norm_game_id": "0020000001",
        "player_id": "1",
        "team_id": "2",
        "stat_key": "DeadBallTurnovers",
    }
    with_box = _box_row(TOV=3)
    without_box = _box_row(TOV=2)

    result = _result_row(
        row,
        box_with=with_box,
        with_metrics=GameVariantMetrics(),
        box_without=without_box,
        without_metrics=GameVariantMetrics(),
    )

    assert result["status"] == "active"
    assert result["changed_stats"] == "TOV:2->3"
    assert result["changed_pipeline_metrics"] == ""


def test_result_row_marks_pipeline_only_dependency_active():
    row = {
        "game_id": "0020000002",
        "norm_game_id": "0020000002",
        "player_id": "1",
        "team_id": "2",
        "stat_key": "FtsMissed",
    }
    with_box = _box_row(FTA=2, FTM=1)
    without_box = _box_row(FTA=2, FTM=1)

    result = _result_row(
        row,
        box_with=with_box,
        with_metrics=GameVariantMetrics(rebound_deletions=0),
        box_without=without_box,
        without_metrics=GameVariantMetrics(rebound_deletions=1),
    )

    assert result["status"] == "active"
    assert result["changed_stats"] == ""
    assert result["changed_pipeline_metrics"] == "rebound_deletions:1->0"


def test_result_row_marks_unsupported_stat_key():
    row = {
        "game_id": "0020000003",
        "norm_game_id": "0020000003",
        "player_id": "1",
        "team_id": "2",
        "stat_key": "Darko_10to17Ft_Att",
    }

    result = _result_row(
        row,
        box_with=None,
        with_metrics=GameVariantMetrics(),
        box_without=None,
        without_metrics=GameVariantMetrics(),
    )

    assert result["status"] == "unsupported_stat_key"
    assert result["changed_stats"] == ""
    assert result["changed_pipeline_metrics"] == ""
