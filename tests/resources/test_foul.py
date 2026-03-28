import pytest

import pbpstats
from pbpstats.resources.enhanced_pbp.enhanced_pbp_item import (
    IncompleteEventStatsContextError,
)
from pbpstats.resources.enhanced_pbp.stats_nba.foul import StatsFoul


def _build_foul(action_type: int) -> StatsFoul:
    return StatsFoul(
        {
            "GAME_ID": "0029700171",
            "EVENTNUM": 1,
            "PCTIMESTRING": "11:06",
            "PERIOD": 4,
            "EVENTMSGACTIONTYPE": action_type,
            "EVENTMSGTYPE": 6,
            "PLAYER1_ID": 891,
            "PLAYER1_TEAM_ID": 1610612752,
            "PLAYER2_ID": 901,
            "VIDEO_AVAILABLE_FLAG": 0,
            "HOMEDESCRIPTION": "Test foul",
            "VISITORDESCRIPTION": "",
        },
        0,
    )


def test_malformed_foul_without_team_or_committer_raises_incomplete_context(
    monkeypatch,
):
    foul = StatsFoul(
        {
            "GAME_ID": "0029600021",
            "EVENTNUM": 11,
            "PCTIMESTRING": "10:49",
            "PERIOD": 1,
            "EVENTMSGACTIONTYPE": 7,
            "EVENTMSGTYPE": 6,
            "PLAYER1_ID": 891,
            "PLAYER1_TEAM_ID": 0,
            "PLAYER2_ID": 722,
            "VIDEO_AVAILABLE_FLAG": 0,
            "HOMEDESCRIPTION": "",
            "VISITORDESCRIPTION": "",
        },
        0,
    )
    foul.previous_event = type(
        "PrevEvent",
        (),
        {
            "current_players": {
                1610612742: [376, 467, 45, 684, 754],
                1610612758: [722, 178, 782, 258, 51],
            }
        },
    )()
    monkeypatch.setattr(
        StatsFoul,
        "base_stats",
        property(lambda self: [{"stat_key": "sentinel", "stat_value": 1}]),
    )

    with pytest.raises(IncompleteEventStatsContextError):
        _ = foul.event_stats


def test_elbow_foul_counts_as_personal_foul():
    foul = _build_foul(7)

    assert foul.is_personal_foul is True
    assert foul.counts_as_personal_foul is True
    assert foul.counts_towards_penalty is True
    assert foul.foul_type_string == pbpstats.PERSONAL_FOUL_TYPE_STRING


def test_punch_foul_counts_as_personal_foul():
    foul = _build_foul(8)

    assert foul.is_personal_foul is True
    assert foul.counts_as_personal_foul is True
    assert foul.counts_towards_penalty is True
    assert foul.foul_type_string == pbpstats.PERSONAL_FOUL_TYPE_STRING


def test_coach_technical_foul_infers_team_from_linked_technical_free_throw(monkeypatch):
    foul = StatsFoul(
        {
            "GAME_ID": "0029900070",
            "EVENTNUM": 537,
            "PCTIMESTRING": "1:37",
            "PERIOD": 4,
            "EVENTMSGACTIONTYPE": 11,
            "EVENTMSGTYPE": 6,
            "PLAYER1_ID": 1243,
            "PLAYER1_TEAM_ID": None,
            "VIDEO_AVAILABLE_FLAG": 0,
            "HOMEDESCRIPTION": "",
            "VISITORDESCRIPTION": "Coach technical",
        },
        0,
    )
    foul.previous_event = type(
        "PrevEvent",
        (),
        {
            "current_players": {
                1610612761: [53, 177, 688, 919, 1713],
                1610612765: [228, 6888, 9199, 1777, 2222],
            }
        },
    )()
    foul.next_event = type(
        "TechnicalFreeThrow",
        (),
        {
            "clock": "1:37",
            "team_id": 1610612761,
            "is_technical_ft": True,
            "next_event": None,
        },
    )()
    monkeypatch.setattr(StatsFoul, "base_stats", property(lambda self: []))

    stats = foul.event_stats
    technical_rows = [
        row
        for row in stats
        if row["stat_key"] == pbpstats.TECHNICAL_FOULS_COMMITTED_STRING
    ]
    assert technical_rows == [
        {
            "player_id": 0,
            "team_id": 1610612765,
            "opponent_team_id": 1610612761,
            "lineup_id": "1777-2222-228-6888-9199",
            "opponent_lineup_id": "1713-177-53-688-919",
            "stat_key": pbpstats.TECHNICAL_FOULS_COMMITTED_STRING,
            "stat_value": 1,
        }
    ]
