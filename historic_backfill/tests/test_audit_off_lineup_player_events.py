from __future__ import annotations

import json

from audit_off_lineup_player_events import _find_off_lineup_player_rows, _nearest_substitution_context


class _BaseEvent:
    def __init__(
        self,
        period,
        clock,
        current_players,
        team_id=0,
        player1_id=0,
        player2_id=0,
        player3_id=0,
        description="",
        event_num=0,
    ):
        self.period = period
        self.clock = clock
        self.current_players = current_players
        self.team_id = team_id
        self.player1_id = player1_id
        self.player2_id = player2_id
        self.player3_id = player3_id
        self.description = description
        self.event_num = event_num


class StatsFieldGoal(_BaseEvent):
    pass


class StatsSubstitution(_BaseEvent):
    pass


class StatsJumpBall(_BaseEvent):
    pass


def test_find_off_lineup_player_rows_flags_missing_live_event_players():
    events = [
        StatsFieldGoal(
            period=3,
            clock="3:25",
            current_players={1: [10, 11, 12, 13, 14], 2: [20, 21, 22, 23, 24]},
            team_id=2,
            player1_id=30,
            player2_id=31,
            description="Missing-player make",
            event_num=445,
        )
    ]
    name_map = {30: "Terry Cummings", 31: "Chris Childs"}

    rows = _find_off_lineup_player_rows("0029701075", events, name_map)

    assert len(rows) == 2
    assert {row["player_name"] for row in rows} == {"Terry Cummings", "Chris Childs"}
    assert rows[0]["event_num"] == 445
    assert json.loads(rows[0]["current_lineups_json"])[0]["team_id"] == 1


def test_find_off_lineup_player_rows_ignores_players_on_other_team_lineup():
    events = [
        StatsFieldGoal(
            period=1,
            clock="8:41",
            current_players={1: [10, 11, 12, 13, 14], 2: [20, 21, 22, 23, 24]},
            team_id=2,
            player1_id=20,
            player3_id=12,
            description="Blocked shot",
            event_num=28,
        )
    ]

    rows = _find_off_lineup_player_rows("0029701075", events, {})

    assert rows == []


def test_find_off_lineup_player_rows_ignores_jump_balls_and_substitutions():
    events = [
        StatsJumpBall(
            period=1,
            clock="12:00",
            current_players={},
            team_id=0,
            player1_id=10,
            player2_id=20,
            player3_id=21,
            description="Jump ball",
            event_num=1,
        ),
        StatsSubstitution(
            period=4,
            clock="3:02",
            current_players={1: [10, 11, 12, 13, 14], 2: [20, 21, 22, 23, 24]},
            team_id=1,
            player1_id=30,
            player2_id=10,
            description="SUB: Walters FOR McKie",
            event_num=496,
        ),
    ]

    rows = _find_off_lineup_player_rows("0029700438", events, {30: "Aaron McKie"})

    assert rows == []


def test_nearest_substitution_context_tracks_prev_and_next_subs():
    events = [
        StatsSubstitution(period=3, clock="4:30", current_players={}, player1_id=40, player2_id=30, event_num=10),
        StatsFieldGoal(
            period=3,
            clock="4:00",
            current_players={1: [1, 2, 3, 4, 5], 2: [20, 21, 22, 23, 24]},
            team_id=2,
            player1_id=30,
            event_num=11,
        ),
        StatsSubstitution(period=3, clock="3:30", current_players={}, player1_id=30, player2_id=41, event_num=12),
    ]

    context = _nearest_substitution_context(events, event_index=1, player_id=30, period=3)

    assert context["prev_sub_in_event_num"] == 10
    assert context["prev_sub_in_clock"] == "4:30"
    assert context["next_sub_out_event_num"] == 12
    assert context["next_sub_out_clock"] == "3:30"
