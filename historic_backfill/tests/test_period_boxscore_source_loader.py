from pathlib import Path

import pandas as pd

from historic_backfill.common.period_boxscore_source_loader import (
    PeriodBoxscoreSourceLoader,
)


def _write_parquet(path: Path, rows: list[dict]) -> None:
    pd.DataFrame(rows).to_parquet(path, index=False)


def test_period_boxscore_loader_returns_parquet_rt2_payload(tmp_path):
    parquet_path = tmp_path / "period_starters_v5.parquet"
    _write_parquet(
        parquet_path,
        [
            {
                "game_id": "0020100162",
                "period": 5,
                "away_team_id": 1610612762,
                "away_player1": 123,
                "away_player2": 124,
                "away_player3": 125,
                "away_player4": 126,
                "away_player5": 127,
                "home_team_id": 1610612751,
                "home_player1": 223,
                "home_player2": 224,
                "home_player3": 225,
                "home_player4": 226,
                "home_player5": 227,
                "resolved": True,
            }
        ],
    )

    loader = PeriodBoxscoreSourceLoader(parquet_path=parquet_path)

    payload = loader.load_data("0020100162", 5, "rt2_start_window")

    assert payload == {
        "periodStarterSource": {"name": "v5"},
        "boxScoreTraditional": {
            "awayTeam": {
                "teamId": 1610612762,
                "players": [
                    {"personId": 123, "statistics": {"minutes": "0:01"}},
                    {"personId": 124, "statistics": {"minutes": "0:01"}},
                    {"personId": 125, "statistics": {"minutes": "0:01"}},
                    {"personId": 126, "statistics": {"minutes": "0:01"}},
                    {"personId": 127, "statistics": {"minutes": "0:01"}},
                ],
            },
            "homeTeam": {
                "teamId": 1610612751,
                "players": [
                    {"personId": 223, "statistics": {"minutes": "0:01"}},
                    {"personId": 224, "statistics": {"minutes": "0:01"}},
                    {"personId": 225, "statistics": {"minutes": "0:01"}},
                    {"personId": 226, "statistics": {"minutes": "0:01"}},
                    {"personId": 227, "statistics": {"minutes": "0:01"}},
                ],
            },
        },
    }


def test_period_boxscore_loader_ignores_unresolved_rows(tmp_path):
    parquet_path = tmp_path / "period_starters_v5.parquet"
    _write_parquet(
        parquet_path,
        [
            {
                "game_id": "0020100162",
                "period": 5,
                "away_team_id": 1610612762,
                "away_player1": 123,
                "away_player2": 124,
                "away_player3": 125,
                "away_player4": 126,
                "away_player5": 127,
                "home_team_id": 1610612751,
                "home_player1": 223,
                "home_player2": 224,
                "home_player3": 225,
                "home_player4": 226,
                "home_player5": 227,
                "resolved": False,
            }
        ],
    )

    loader = PeriodBoxscoreSourceLoader(parquet_path=parquet_path)

    assert loader.load_data("0020100162", 5, "rt2_start_window") is None


def test_period_boxscore_loader_only_answers_rt2_requests(tmp_path):
    parquet_path = tmp_path / "period_starters_v5.parquet"
    _write_parquet(
        parquet_path,
        [
            {
                "game_id": "0020100162",
                "period": 5,
                "away_team_id": 1610612762,
                "away_player1": 123,
                "away_player2": 124,
                "away_player3": 125,
                "away_player4": 126,
                "away_player5": 127,
                "home_team_id": 1610612751,
                "home_player1": 223,
                "home_player2": 224,
                "home_player3": 225,
                "home_player4": 226,
                "home_player5": 227,
                "resolved": True,
            }
        ],
    )

    loader = PeriodBoxscoreSourceLoader(parquet_path=parquet_path)

    assert loader.load_data("0020100162", 5, "rt1_period_participants") is None
    assert loader.load_data("0020100162", 6, "rt2_start_window") is None


def test_period_boxscore_loader_uses_first_matching_parquet_in_precedence_order(
    tmp_path,
):
    v6_path = tmp_path / "period_starters_v6.parquet"
    v5_path = tmp_path / "period_starters_v5.parquet"
    _write_parquet(
        v6_path,
        [
            {
                "game_id": "0020100162",
                "period": 5,
                "away_team_id": 1610612762,
                "away_player1": 301,
                "away_player2": 302,
                "away_player3": 303,
                "away_player4": 304,
                "away_player5": 305,
                "home_team_id": 1610612751,
                "home_player1": 401,
                "home_player2": 402,
                "home_player3": 403,
                "home_player4": 404,
                "home_player5": 405,
                "resolved": True,
            }
        ],
    )
    _write_parquet(
        v5_path,
        [
            {
                "game_id": "0020100162",
                "period": 5,
                "away_team_id": 1610612762,
                "away_player1": 123,
                "away_player2": 124,
                "away_player3": 125,
                "away_player4": 126,
                "away_player5": 127,
                "home_team_id": 1610612751,
                "home_player1": 223,
                "home_player2": 224,
                "home_player3": 225,
                "home_player4": 226,
                "home_player5": 227,
                "resolved": True,
            }
        ],
    )

    loader = PeriodBoxscoreSourceLoader(parquet_paths=[v6_path, v5_path])

    payload = loader.load_data("0020100162", 5, "rt2_start_window")

    assert [
        player["personId"]
        for player in payload["boxScoreTraditional"]["awayTeam"]["players"]
    ] == [
        301,
        302,
        303,
        304,
        305,
    ]
    assert payload["periodStarterSource"] == {"name": "v6"}


def test_period_boxscore_loader_falls_back_to_later_parquet_when_first_is_unresolved(
    tmp_path,
):
    v6_path = tmp_path / "period_starters_v6.parquet"
    v5_path = tmp_path / "period_starters_v5.parquet"
    _write_parquet(
        v6_path,
        [
            {
                "game_id": "0020100162",
                "period": 5,
                "away_team_id": 1610612762,
                "away_player1": 301,
                "away_player2": 302,
                "away_player3": 303,
                "away_player4": 304,
                "away_player5": 305,
                "home_team_id": 1610612751,
                "home_player1": 401,
                "home_player2": 402,
                "home_player3": 403,
                "home_player4": 404,
                "home_player5": 405,
                "resolved": False,
            }
        ],
    )
    _write_parquet(
        v5_path,
        [
            {
                "game_id": "0020100162",
                "period": 5,
                "away_team_id": 1610612762,
                "away_player1": 123,
                "away_player2": 124,
                "away_player3": 125,
                "away_player4": 126,
                "away_player5": 127,
                "home_team_id": 1610612751,
                "home_player1": 223,
                "home_player2": 224,
                "home_player3": 225,
                "home_player4": 226,
                "home_player5": 227,
                "resolved": True,
            }
        ],
    )

    loader = PeriodBoxscoreSourceLoader(parquet_paths=[v6_path, v5_path])

    payload = loader.load_data("0020100162", 5, "rt2_start_window")

    assert [
        player["personId"]
        for player in payload["boxScoreTraditional"]["awayTeam"]["players"]
    ] == [
        123,
        124,
        125,
        126,
        127,
    ]
    assert payload["periodStarterSource"] == {"name": "v5"}


def test_period_boxscore_loader_filters_to_allowed_seasons(tmp_path):
    parquet_path = tmp_path / "period_starters_v6.parquet"
    _write_parquet(
        parquet_path,
        [
            {
                "game_id": "0029600060",
                "period": 5,
                "away_team_id": 1610612737,
                "away_player1": 1,
                "away_player2": 2,
                "away_player3": 3,
                "away_player4": 4,
                "away_player5": 5,
                "home_team_id": 1610612748,
                "home_player1": 6,
                "home_player2": 7,
                "home_player3": 8,
                "home_player4": 9,
                "home_player5": 10,
                "resolved": True,
            },
            {
                "game_id": "0021800143",
                "period": 6,
                "away_team_id": 1610612752,
                "away_player1": 11,
                "away_player2": 12,
                "away_player3": 13,
                "away_player4": 14,
                "away_player5": 15,
                "home_team_id": 1610612751,
                "home_player1": 16,
                "home_player2": 17,
                "home_player3": 18,
                "home_player4": 19,
                "home_player5": 20,
                "resolved": True,
            },
        ],
    )

    loader = PeriodBoxscoreSourceLoader(
        parquet_path=parquet_path, allowed_seasons=[1997]
    )

    assert loader.load_data("0029600060", 5, "rt2_start_window") is not None
    assert loader.load_data("0021800143", 6, "rt2_start_window") is None


def test_period_boxscore_loader_filters_to_allowed_game_ids(tmp_path):
    parquet_path = tmp_path / "period_starters_v6.parquet"
    _write_parquet(
        parquet_path,
        [
            {
                "game_id": "0029700060",
                "period": 5,
                "away_team_id": 1610612737,
                "away_player1": 1,
                "away_player2": 2,
                "away_player3": 3,
                "away_player4": 4,
                "away_player5": 5,
                "home_team_id": 1610612748,
                "home_player1": 6,
                "home_player2": 7,
                "home_player3": 8,
                "home_player4": 9,
                "home_player5": 10,
                "resolved": True,
            },
            {
                "game_id": "0029700061",
                "period": 5,
                "away_team_id": 1610612737,
                "away_player1": 11,
                "away_player2": 12,
                "away_player3": 13,
                "away_player4": 14,
                "away_player5": 15,
                "home_team_id": 1610612748,
                "home_player1": 16,
                "home_player2": 17,
                "home_player3": 18,
                "home_player4": 19,
                "home_player5": 20,
                "resolved": True,
            },
        ],
    )

    loader = PeriodBoxscoreSourceLoader(
        parquet_path=parquet_path,
        allowed_game_ids=["0029700060"],
    )

    assert loader.load_data("0029700060", 5, "rt2_start_window") is not None
    assert loader.load_data("0029700061", 5, "rt2_start_window") is None


def test_period_boxscore_loader_normalizes_float_like_game_ids(tmp_path):
    parquet_path = tmp_path / "period_starters_v6.parquet"
    _write_parquet(
        parquet_path,
        [
            {
                "game_id": 29700060.0,
                "period": 5,
                "away_team_id": 1610612737,
                "away_player1": 1,
                "away_player2": 2,
                "away_player3": 3,
                "away_player4": 4,
                "away_player5": 5,
                "home_team_id": 1610612748,
                "home_player1": 6,
                "home_player2": 7,
                "home_player3": 8,
                "home_player4": 9,
                "home_player5": 10,
                "resolved": True,
            },
            {
                "game_id": 29700061.0,
                "period": 5,
                "away_team_id": 1610612737,
                "away_player1": 11,
                "away_player2": 12,
                "away_player3": 13,
                "away_player4": 14,
                "away_player5": 15,
                "home_team_id": 1610612748,
                "home_player1": 16,
                "home_player2": 17,
                "home_player3": 18,
                "home_player4": 19,
                "home_player5": 20,
                "resolved": True,
            },
        ],
    )

    loader = PeriodBoxscoreSourceLoader(
        parquet_path=parquet_path,
        allowed_game_ids=["29700060.0"],
    )

    assert loader.load_data("0029700060", 5, "rt2_start_window") is not None
    assert loader.load_data("0029700061", 5, "rt2_start_window") is None
