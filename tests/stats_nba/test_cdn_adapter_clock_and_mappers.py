# -*- coding: utf-8 -*-
import pytest

import pbpstats.data_loader.stats_nba.pbp.cdn_adapter as cdn_adapter
from pbpstats.data_loader.stats_nba.pbp.cdn_adapter import (
    FOUL_MAP,
    FT_MAP,
    SHOT_MAP,
    TOV_MAP,
    VIOL_MAP,
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
    heave_made = {"actionType": "Heave", "shotResult": "Made"}
    heave_missed = {"actionType": "Heave", "shotResult": "Missed"}

    assert map_eventmsgtype(made) == 1
    assert map_eventmsgtype(missed) == 2
    assert map_eventmsgtype(ft) == 3
    assert map_eventmsgtype(replay) == 18
    assert map_eventmsgtype(heave_made) == 1
    assert map_eventmsgtype(heave_missed) == 2


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
    assert map_eventmsgactiontype(period_start, 12) == 0
    period_end = {"actionType": "Period", "subType": "end"}
    assert map_eventmsgactiontype(period_end, 13) == 0


def test_shot_subtype_aliases_map_correctly():
    action = {
        "actionType": "3pt",
        "shotResult": "Made",
        "subType": "Finger Roll",
    }
    assert map_eventmsgactiontype(action, 1) == SHOT_MAP["fingerroll"]

    tip = {
        "actionType": "2pt",
        "shotResult": "Missed",
        "subType": "Tip-In",
    }
    assert map_eventmsgactiontype(tip, 2) == SHOT_MAP["tipin"]


def test_violation_subtypes_use_defined_codes():
    action = {"actionType": "Violation", "subType": "Delay of Game"}
    assert map_eventmsgactiontype(action, 7) == VIOL_MAP["delayofgame"]

    desc_action = {
        "actionType": "Violation",
        "descriptor": "Jump Ball Violation",
    }
    assert map_eventmsgactiontype(desc_action, 7) == VIOL_MAP["jumpballviolation"]


def test_unknown_actiontype_logs_once_and_returns_zero(caplog):
    cdn_adapter._seen_unknown.clear()
    with caplog.at_level("WARNING"):
        action = {"actionType": "Turnover", "subType": "Mystery"}
        assert map_eventmsgactiontype(action, 5) == 0
        # second call should not duplicate warning
        assert map_eventmsgactiontype(action, 5) == 0
    assert len(caplog.records) == 1
    assert "Unmapped PBP subtype" in caplog.text


def test_common_meta_families_do_not_emit_warnings(caplog):
    cdn_adapter._seen_unknown.clear()
    with caplog.at_level("WARNING"):
        assert map_eventmsgactiontype(
            {"actionType": "Rebound", "subType": "Defensive"}, None
        ) == 0
        assert map_eventmsgactiontype(
            {"actionType": "Rebound", "subType": "Offensive"}, 4
        ) == 0
        assert (
            map_eventmsgactiontype(
                {"actionType": "Timeout", "subType": "Full"}, None
            )
            == 0
        )
        assert (
            map_eventmsgactiontype(
                {
                    "actionType": "JumpBall",
                    "descriptor": "StartPeriod",
                    "subType": "Recovered",
                },
                None,
            )
            == 0
        )
        assert (
            map_eventmsgactiontype({"actionType": "Substitution", "subType": "Out"}, 8)
            == 0
        )
        assert (
            map_eventmsgactiontype({"actionType": "Substitution", "subType": "In"}, None)
            == 0
        )
        assert (
            map_eventmsgactiontype(
                {"actionType": "Stoppage", "descriptor": "Out of Bounds"}, None
            )
            == 0
        )
    assert len(caplog.records) == 0
