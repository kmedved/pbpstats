import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from nba_stats_parser.nba_stats_parser import models, parser


def build_events(rows):
    raw_pbp = {"PlayByPlay": rows}
    raw_boxscore = {
        "PlayerStats": [
            {"TEAM_ID": 1, "PLAYER_ID": 101, "START_POSITION": "G"},
            {"TEAM_ID": 2, "PLAYER_ID": 201, "START_POSITION": "G"},
        ]
    }
    raw_shot_charts = {}
    poss = parser.parse_game_data("gid", raw_pbp, raw_boxscore, raw_shot_charts)
    events = []
    for p in poss:
        events.extend(p.events)
    return events


def ft_row(event_num, team_id=1, desc="FT", action_type=12):
    return {
        "GAME_ID": "gid",
        "EVENTNUM": event_num,
        "EVENTMSGTYPE": 3,
        "EVENTMSGACTIONTYPE": action_type,
        "PCTIMESTRING": "10:00",
        "PERIOD": 1,
        "PLAYER1_ID": 101 if team_id == 1 else 201,
        "PLAYER1_TEAM_ID": team_id,
        "HOMEDESCRIPTION": desc,
    }


def reb_row(event_num, team_id):
    return {
        "GAME_ID": "gid",
        "EVENTNUM": event_num,
        "EVENTMSGTYPE": 4,
        "EVENTMSGACTIONTYPE": 0,
        "PCTIMESTRING": "10:00",
        "PERIOD": 1,
        "PLAYER1_ID": 200 + team_id,
        "PLAYER1_TEAM_ID": team_id,
        "HOMEDESCRIPTION": "REB",
    }


def test_final_ft_rebounded_by_opponent_counts_as_miss():
    rows = [ft_row(1, desc="FT 2 of 2"), reb_row(2, 2)]
    events = build_events(rows)
    ft = next(e for e in events if isinstance(e, models.FreeThrow))
    assert ft.was_ambiguous_raw
    assert ft.is_made is False


def test_final_ft_rebounded_by_shooting_team_counts_as_make():
    rows = [ft_row(1, desc="FT 2 of 2"), reb_row(2, 1)]
    events = build_events(rows)
    ft = next(e for e in events if isinstance(e, models.FreeThrow))
    assert ft.is_made is True


def test_final_ft_with_explicit_pts_always_make():
    rows = [ft_row(1, desc="makes FT (1 PTS)"), reb_row(2, 2)]
    events = build_events(rows)
    ft = next(e for e in events if isinstance(e, models.FreeThrow))
    assert ft.is_made is True


def test_mid_trip_ft_untouched_logic():
    rows = [ft_row(1, desc="FT 1 of 2", action_type=11), reb_row(2, 2)]
    events = build_events(rows)
    ft = next(e for e in events if isinstance(e, models.FreeThrow))
    assert ft.is_made is True


def test_cached_property_only_runs_once(monkeypatch):
    call_count = {"n": 0}
    orig_func = models.FreeThrow.is_made.func

    def wrapper(self):
        call_count["n"] += 1
        return orig_func(self)

    monkeypatch.setattr(models.FreeThrow.is_made, "func", wrapper, raising=False)
    rows = [ft_row(1, desc="FT 2 of 2"), reb_row(2, 2)]
    events = build_events(rows)
    ft = next(e for e in events if isinstance(e, models.FreeThrow))
    # access multiple times
    for _ in range(3):
        ft.is_made
    assert call_count["n"] == 1
