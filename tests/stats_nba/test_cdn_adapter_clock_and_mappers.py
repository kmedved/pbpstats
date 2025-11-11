# -*- coding: utf-8 -*-
import pytest

from pbpstats.data_loader.stats_nba.pbp.cdn_adapter import (
    FOUL_MAP,
    FT_MAP,
    SHOT_MAP,
    TOV_MAP,
    iso_to_pctimestring,
    map_eventmsgactiontype,
    map_eventmsgtype,
)


@pytest.mark.parametrize(
    "iso,expected",
    [
        ("PT12M00S", "12:00"),
        ("PT11M38.00S", "11:38"),
        ("PT0M00.50S", "0:00.5"),
        (None, "0:00"),
        ("bad", "0:00"),
    ],
)
def test_iso_to_pctimestring_cases(iso, expected):
    assert iso_to_pctimestring(iso) == expected


def test_map_eventmsgtype_for_shots_and_specials():
    made = {"actionType": "3pt", "shotResult": "Made"}
    missed = {"actionType": "2pt", "shotResult": "Missed"}
    ft = {"actionType": "FreeThrow"}
    replay = {"actionType": "InstantReplay"}
    heave = {"actionType": "Heave"}

    assert map_eventmsgtype(made) == 1
    assert map_eventmsgtype(missed) == 2
    assert map_eventmsgtype(ft) == 3
    assert map_eventmsgtype(replay) == 18
    assert map_eventmsgtype(heave) == 2


def test_map_eventmsgactiontype_for_common_actions():
    fg = {"actionType": "3pt", "shotResult": "Made", "subType": "JumpShot"}
    evt = map_eventmsgtype(fg)
    assert map_eventmsgactiontype(fg, evt) == SHOT_MAP["jumpshot"]

    ft = {"actionType": "FreeThrow", "subType": "2of2"}
    evt_ft = map_eventmsgtype(ft)
    assert map_eventmsgactiontype(ft, evt_ft) == FT_MAP["2of2"]

    ft_tech = {"actionType": "FreeThrow", "descriptor": "Technical"}
    assert map_eventmsgactiontype(ft_tech, 3) == 11

    turnover = {"actionType": "Turnover", "subType": "lostball"}
    assert map_eventmsgactiontype(turnover, 5) == TOV_MAP["lostball"]

    foul = {"actionType": "Foul", "descriptor": "Technical"}
    assert map_eventmsgactiontype(foul, 6) == FOUL_MAP["technical"]

    period_start = {"actionType": "Period", "subType": "start"}
    assert map_eventmsgactiontype(period_start, None) == 12
    period_end = {"actionType": "Period", "subType": "end"}
    assert map_eventmsgactiontype(period_end, None) == 13

