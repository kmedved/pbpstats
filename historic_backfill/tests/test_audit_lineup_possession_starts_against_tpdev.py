from __future__ import annotations

import pandas as pd

from historic_backfill.audits.cross_source.lineup_possession_starts import (
    _lineup_match_disposition,
    _load_tpdev_team_rows,
    _parser_lineup_at_possession_start,
)


class _BaseEvent:
    def __init__(self, period, clock, current_players, description=""):
        self.period = period
        self.clock = clock
        self.current_players = current_players
        self.description = description
        self.event_num = 0
        self.team_id = 0
        self.player1_id = 0
        self.player2_id = 0


class StatsFieldGoal(_BaseEvent):
    pass


class StatsFoul(_BaseEvent):
    pass


class StatsSubstitution(_BaseEvent):
    pass


class StatsFreeThrow(_BaseEvent):
    pass


def test_parser_lineup_at_possession_start_uses_last_exact_clock_event():
    events = [
        StatsFieldGoal(4, "4:20", {1: [1, 2, 3, 4, 5]}),
        StatsFoul(4, "4:16", {1: [1, 2, 3, 4, 5]}, description="foul"),
        StatsSubstitution(4, "4:16", {1: [1, 2, 3, 4, 6]}, description="sub"),
        StatsFreeThrow(4, "4:16", {1: [1, 2, 3, 4, 6]}, description="ft"),
        StatsFieldGoal(4, "4:08", {1: [1, 2, 3, 4, 6]}),
    ]

    lineup_ids, anchor_kind, anchor_event, anchor_window = _parser_lineup_at_possession_start(
        events=events,
        period=4,
        time_remaining_start=256.0,
        team_id=1,
    )

    assert lineup_ids == [1, 2, 3, 4, 6]
    assert anchor_kind == "exact_clock"
    assert anchor_event.description == "ft"
    assert [event.description for event in anchor_window] == ["foul", "sub", "ft"]


def test_load_tpdev_team_rows_expands_home_and_away_rows(tmp_path):
    df = pd.DataFrame(
        [
            {
                "game_id": 29700001,
                "Quarter": 4,
                "TimeRemainingStart": 256,
                "LengthInSeconds": 8,
                "event_id": 123,
                "PossString": "TO",
                "offenseTeamId1": 1610612752.0,
                "h_tm_id": 1610612738.0,
                "v_tm_id": 1610612752.0,
                "h1": 11,
                "h2": 12,
                "h3": 13,
                "h4": 14,
                "h5": 15,
                "v1": 21,
                "v2": 22,
                "v3": 23,
                "v4": 24,
                "v5": 25,
            }
        ]
    )
    parquet_path = tmp_path / "tpdev.parq"
    df.to_parquet(parquet_path, index=False)

    team_rows = _load_tpdev_team_rows(parquet_path, "0029700001")

    assert len(team_rows) == 2
    home_row = team_rows.loc[team_rows["team_side"] == "home"].iloc[0]
    away_row = team_rows.loc[team_rows["team_side"] == "away"].iloc[0]

    assert home_row["team_id"] == 1610612738
    assert home_row["tpdev_lineup_ids"] == [11, 12, 13, 14, 15]
    assert home_row["time_remaining_end"] == 248.0
    assert away_row["team_id"] == 1610612752
    assert away_row["tpdev_lineup_ids"] == [21, 22, 23, 24, 25]


def test_lineup_match_disposition_detects_end_only_matches():
    disposition = _lineup_match_disposition(
        parser_start_lineup_ids=[1, 2, 3, 4, 5],
        parser_end_lineup_ids=[1, 2, 3, 4, 6],
        tpdev_lineup_ids=[1, 2, 3, 4, 6],
    )

    assert disposition == "matches_end_only"
