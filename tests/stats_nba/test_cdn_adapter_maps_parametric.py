# -*- coding: utf-8 -*-
import pytest

from pbpstats.data_loader.stats_nba.pbp.cdn_adapter import (
    FOUL_MAP,
    TOV_MAP,
    VIOL_MAP,
    map_eventmsgactiontype,
    map_eventmsgtype,
)


@pytest.mark.parametrize(
    "sub,expected",
    [
        ("Kicked Ball", VIOL_MAP["kickedball"]),
        ("defensive-goaltending", VIOL_MAP["defensivegoaltending"]),
        ("Delay of Game", VIOL_MAP["delayofgame"]),
        ("Lane", VIOL_MAP["lane"]),
        ("Double Lane", VIOL_MAP["doublelane"]),
        ("Jump Ball Violation", VIOL_MAP["jumpballviolation"]),
    ],
)
def test_violation_subtypes_parametric(sub, expected):
    action = {"actionType": "Violation", "subType": sub}
    evt = map_eventmsgtype(action)
    assert evt == 7
    assert map_eventmsgactiontype(action, evt) == expected


@pytest.mark.parametrize(
    "sub,expected",
    [
        ("Double Dribble", TOV_MAP["doubledribble"]),
        ("Backcourt", TOV_MAP["backcourt"]),
        ("Five Second", TOV_MAP["fivesecond"]),
        ("Eight-Second", TOV_MAP["eightsecond"]),
        ("Offensive Goaltending", TOV_MAP["offensivegoaltending"]),
        ("Step Out Of Bounds", TOV_MAP["stepoutofbounds"]),
        ("Carry", TOV_MAP["carry"]),
        ("Carrying", TOV_MAP["carrying"]),
        ("Palming", TOV_MAP["palming"]),
        ("3-Second Violation", TOV_MAP["3secondviolation"]),
    ],
)
def test_turnover_subtypes_parametric(sub, expected):
    action = {"actionType": "Turnover", "subType": sub}
    evt = map_eventmsgtype(action)
    assert evt == 5
    assert map_eventmsgactiontype(action, evt) == expected


@pytest.mark.parametrize(
    "desc,expected",
    [
        ("Blocking", FOUL_MAP["blocking"]),
        ("Personal", FOUL_MAP["personal"]),
        ("Away-from-Play", FOUL_MAP["awayfromplay"]),
        ("Clear-Path", FOUL_MAP["clearpath"]),
        ("Defensive 3 Second", FOUL_MAP["defensive3second"]),
        ("Illegal Defense", FOUL_MAP["illegaldefense"]),
        ("Double Technical", FOUL_MAP["doubletechnical"]),
        ("Transition Take", FOUL_MAP["transitiontake"]),
        ("Flagrant Type 1", FOUL_MAP["flagranttype1"]),
        ("Flagrant-Type-2", FOUL_MAP["flagranttype2"]),
    ],
)
def test_foul_descriptors_parametric(desc, expected):
    action = {"actionType": "Foul", "descriptor": desc}
    evt = map_eventmsgtype(action)
    assert evt == 6
    assert map_eventmsgactiontype(action, evt) == expected
