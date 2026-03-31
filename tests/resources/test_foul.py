import pytest

import pbpstats
from pbpstats.resources.enhanced_pbp.enhanced_pbp_item import (
    IncompleteEventStatsContextError,
)
from pbpstats.resources.enhanced_pbp.stats_nba.foul import StatsFoul


class DummyEvent:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.previous_event = kwargs.get("previous_event")
        self.next_event = kwargs.get("next_event")


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


def test_unresolved_coach_technical_returns_base_stats_and_logs_warning(
    monkeypatch, caplog
):
    foul = StatsFoul(
        {
            "GAME_ID": "0021900339",
            "EVENTNUM": 99,
            "PCTIMESTRING": "10:04",
            "PERIOD": 1,
            "EVENTMSGACTIONTYPE": 11,
            "EVENTMSGTYPE": 6,
            "PLAYER1_ID": 2623,
            "PLAYER1_TEAM_ID": None,
            "VIDEO_AVAILABLE_FLAG": 0,
            "NEUTRALDESCRIPTION": "Scott Brooks Foul:T.FOUL (J.Williams)",
        },
        0,
    )
    foul.previous_event = type(
        "PrevEvent",
        (),
        {
            "current_players": {
                1610612746: [201933, 202331, 203110, 203114, 203143],
                1610612764: [1626162, 1629678, 202722, 203078, 203490],
            }
        },
    )()
    foul.next_event = None
    monkeypatch.setattr(
        StatsFoul,
        "base_stats",
        property(lambda self: [{"stat_key": "sentinel", "stat_value": 1}]),
    )

    with caplog.at_level("WARNING"):
        stats = foul.event_stats

    assert stats == [{"stat_key": "sentinel", "stat_value": 1}]
    assert any(
        "source_limited_bench_technical_no_team" in message
        for message in caplog.messages
    )


def test_coach_technical_foul_infers_team_from_paired_same_clock_technical(monkeypatch):
    foul = StatsFoul(
        {
            "GAME_ID": "0021900339",
            "EVENTNUM": 99,
            "PCTIMESTRING": "4:39",
            "PERIOD": 1,
            "EVENTMSGACTIONTYPE": 11,
            "EVENTMSGTYPE": 6,
            "PLAYER1_ID": 2623,
            "PLAYER1_TEAM_ID": None,
            "VIDEO_AVAILABLE_FLAG": 0,
            "NEUTRALDESCRIPTION": "Scott Brooks Foul:T.FOUL (J.Williams)",
        },
        0,
    )
    foul.previous_event = type(
        "PrevEvent",
        (),
        {
            "current_players": {
                1610612746: [201976, 202331, 202695, 203090, 1626149],
                1610612764: [202397, 202722, 203078, 1628972, 101133],
            }
        },
    )()
    paired_tech = DummyEvent(
        event_type=6,
        is_technical=True,
        is_double_technical=False,
        team_id=1610612746,
        clock="4:39",
    )
    foul.next_event = paired_tech
    paired_tech.previous_event = foul
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
            "team_id": 1610612764,
            "opponent_team_id": 1610612746,
            "lineup_id": "101133-1628972-202397-202722-203078",
            "opponent_lineup_id": "1626149-201976-202331-202695-203090",
            "stat_key": pbpstats.TECHNICAL_FOULS_COMMITTED_STRING,
            "stat_value": 1,
        }
    ]


def test_coach_technical_foul_infers_team_from_same_clock_defensive_three_seconds(
    monkeypatch,
):
    foul = StatsFoul(
        {
            "GAME_ID": "0040200212",
            "EVENTNUM": 152,
            "PCTIMESTRING": "7:01",
            "PERIOD": 2,
            "EVENTMSGACTIONTYPE": 11,
            "EVENTMSGTYPE": 6,
            "PLAYER1_ID": 1776,
            "PLAYER1_TEAM_ID": None,
            "VIDEO_AVAILABLE_FLAG": 0,
            "NEUTRALDESCRIPTION": "Byron Scott Foul:T.FOUL",
        },
        0,
    )
    foul.previous_event = type(
        "PrevEvent",
        (),
        {
            "current_players": {
                1610612738: [1499, 1718, 1888, 1999, 2000],
                1610612751: [446, 954, 1425, 1889, 2001],
            }
        },
    )()
    defensive_three_seconds = DummyEvent(
        event_type=6,
        is_technical=False,
        is_double_technical=False,
        is_defensive_3_seconds=True,
        team_id=1610612738,
        clock="7:01",
        current_players={
            1610612738: [1499, 1718, 1888, 1999, 2000],
            1610612751: [446, 954, 1425, 1889, 2001],
        },
    )
    foul.previous_event = defensive_three_seconds
    defensive_three_seconds.next_event = foul
    defensive_three_seconds.previous_event = type(
        "PrevEvent",
        (),
        {
            "current_players": {
                1610612738: [1499, 1718, 1888, 1999, 2000],
                1610612751: [446, 954, 1425, 1889, 2001],
            }
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
            "team_id": 1610612751,
            "opponent_team_id": 1610612738,
            "lineup_id": "1425-1889-2001-446-954",
            "opponent_lineup_id": "1499-1718-1888-1999-2000",
            "stat_key": pbpstats.TECHNICAL_FOULS_COMMITTED_STRING,
            "stat_value": 1,
        }
    ]


def test_coach_technical_foul_infers_team_from_cross_period_boundary_cluster(
    monkeypatch,
):
    foul = StatsFoul(
        {
            "GAME_ID": "0029901056",
            "EVENTNUM": 249,
            "PCTIMESTRING": "0:00",
            "PERIOD": 2,
            "EVENTMSGACTIONTYPE": 11,
            "EVENTMSGTYPE": 6,
            "PLAYER1_ID": 1353,
            "PLAYER1_TEAM_ID": None,
            "VIDEO_AVAILABLE_FLAG": 0,
            "NEUTRALDESCRIPTION": "Lionel Hollins Foul:T.FOUL",
        },
        0,
    )
    foul.previous_event = type(
        "PrevEvent",
        (),
        {
            "current_players": {
                1610612742: [714, 959, 1717, 1722, 1761],
                1610612763: [156, 192, 949, 1710, 1960],
            }
        },
    )()
    unresolved_partner = DummyEvent(
        event_type=6,
        is_technical=True,
        is_double_technical=False,
        team_id=2007,
        clock="0:00",
        period=2,
    )
    ejection = DummyEvent(event_type=11, clock="0:00", period=2)
    end_of_period = DummyEvent(event_type=13, clock="0:00", period=2)
    start_of_period = DummyEvent(event_type=12, clock="12:00", period=3)
    technical_ft_one = DummyEvent(
        event_type=3,
        is_technical_ft=True,
        team_id=1610612742,
        clock="12:00",
        period=3,
    )
    technical_ft_two = DummyEvent(
        event_type=3,
        is_technical_ft=True,
        team_id=1610612742,
        clock="12:00",
        period=3,
    )
    live_event = DummyEvent(event_type=1, clock="11:46", period=3)

    foul.next_event = unresolved_partner
    unresolved_partner.previous_event = foul
    unresolved_partner.next_event = ejection
    ejection.previous_event = unresolved_partner
    ejection.next_event = end_of_period
    end_of_period.previous_event = ejection
    end_of_period.next_event = None
    start_of_period.previous_event = None
    start_of_period.next_event = technical_ft_one
    technical_ft_one.previous_event = start_of_period
    technical_ft_one.next_event = technical_ft_two
    technical_ft_two.previous_event = technical_ft_one
    technical_ft_two.next_event = live_event
    live_event.previous_event = technical_ft_two

    foul.next_event_any_period = unresolved_partner
    unresolved_partner.previous_event_any_period = foul
    unresolved_partner.next_event_any_period = ejection
    ejection.previous_event_any_period = unresolved_partner
    ejection.next_event_any_period = end_of_period
    end_of_period.previous_event_any_period = ejection
    end_of_period.next_event_any_period = start_of_period
    start_of_period.previous_event_any_period = end_of_period
    start_of_period.next_event_any_period = technical_ft_one
    technical_ft_one.previous_event_any_period = start_of_period
    technical_ft_one.next_event_any_period = technical_ft_two
    technical_ft_two.previous_event_any_period = technical_ft_one
    technical_ft_two.next_event_any_period = live_event
    live_event.previous_event_any_period = technical_ft_two

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
            "team_id": 1610612763,
            "opponent_team_id": 1610612742,
            "lineup_id": "156-1710-192-1960-949",
            "opponent_lineup_id": "1717-1722-1761-714-959",
            "stat_key": pbpstats.TECHNICAL_FOULS_COMMITTED_STRING,
            "stat_value": 1,
        }
    ]


def test_boundary_cluster_resolver_recovers_all_unresolved_coach_technicals(
    monkeypatch,
):
    current_players = {
        1610612743: [338, 708, 711, 1500, 1718],
        1610612756: [339, 361, 467, 915, 1059],
    }
    foul_one = StatsFoul(
        {
            "GAME_ID": "0029900526",
            "EVENTNUM": 252,
            "PCTIMESTRING": "0:00",
            "PERIOD": 2,
            "EVENTMSGACTIONTYPE": 11,
            "EVENTMSGTYPE": 6,
            "PLAYER1_ID": 2007,
            "PLAYER1_TEAM_ID": None,
            "VIDEO_AVAILABLE_FLAG": 0,
            "NEUTRALDESCRIPTION": "Dan Issel Foul:T.FOUL",
        },
        0,
    )
    foul_two = StatsFoul(
        {
            "GAME_ID": "0029900526",
            "EVENTNUM": 254,
            "PCTIMESTRING": "0:00",
            "PERIOD": 2,
            "EVENTMSGACTIONTYPE": 11,
            "EVENTMSGTYPE": 6,
            "PLAYER1_ID": 1784,
            "PLAYER1_TEAM_ID": None,
            "VIDEO_AVAILABLE_FLAG": 0,
            "NEUTRALDESCRIPTION": "Louie Dampier Foul:T.FOUL",
        },
        1,
    )
    for foul in (foul_one, foul_two):
        foul.previous_event = type("PrevEvent", (), {"current_players": current_players})()

    ejection = DummyEvent(event_type=11, clock="0:00", period=2, current_players=current_players)
    end_of_period = DummyEvent(
        event_type=13, clock="0:00", period=2, current_players=current_players
    )
    technical_ft_one = DummyEvent(
        event_type=3,
        is_technical_ft=True,
        team_id=1610612756,
        clock="12:00",
        period=3,
        current_players=current_players,
    )
    technical_ft_two = DummyEvent(
        event_type=3,
        is_technical_ft=True,
        team_id=1610612756,
        clock="12:00",
        period=3,
        current_players=current_players,
    )
    start_of_period = DummyEvent(
        event_type=12, clock="12:00", period=3, current_players=current_players
    )
    live_event = DummyEvent(event_type=5, clock="11:46", period=3, current_players=current_players)

    foul_one.next_event = ejection
    ejection.previous_event = foul_one
    ejection.next_event = foul_two
    foul_two.previous_event = ejection
    foul_two.next_event = end_of_period
    end_of_period.previous_event = foul_two
    end_of_period.next_event = None
    start_of_period.previous_event = None
    start_of_period.next_event = technical_ft_one
    technical_ft_one.previous_event = start_of_period
    technical_ft_one.next_event = technical_ft_two
    technical_ft_two.previous_event = technical_ft_one
    technical_ft_two.next_event = live_event
    live_event.previous_event = technical_ft_two

    foul_one.next_event_any_period = ejection
    ejection.previous_event_any_period = foul_one
    ejection.next_event_any_period = foul_two
    foul_two.previous_event_any_period = ejection
    foul_two.next_event_any_period = end_of_period
    end_of_period.previous_event_any_period = foul_two
    end_of_period.next_event_any_period = start_of_period
    start_of_period.previous_event_any_period = end_of_period
    start_of_period.next_event_any_period = technical_ft_one
    technical_ft_one.previous_event_any_period = start_of_period
    technical_ft_one.next_event_any_period = technical_ft_two
    technical_ft_two.previous_event_any_period = technical_ft_one
    technical_ft_two.next_event_any_period = live_event
    live_event.previous_event_any_period = technical_ft_two

    monkeypatch.setattr(StatsFoul, "base_stats", property(lambda self: []))

    for foul in (foul_one, foul_two):
        stats = foul.event_stats
        technical_rows = [
            row
            for row in stats
            if row["stat_key"] == pbpstats.TECHNICAL_FOULS_COMMITTED_STRING
        ]
        assert technical_rows[0]["team_id"] == 1610612743
        assert technical_rows[0]["opponent_team_id"] == 1610612756


def test_unresolved_double_technical_returns_base_stats_and_logs_warning(
    monkeypatch, caplog
):
    foul = StatsFoul(
        {
            "GAME_ID": "0029900427",
            "EVENTNUM": 353,
            "PCTIMESTRING": "12:00",
            "PERIOD": 3,
            "EVENTMSGACTIONTYPE": 16,
            "EVENTMSGTYPE": 6,
            "PLAYER1_ID": 1243,
            "PLAYER1_TEAM_ID": None,
            "PLAYER2_ID": 1941,
            "VIDEO_AVAILABLE_FLAG": 0,
            "NEUTRALDESCRIPTION": "Alvin Gentry Foul:DOUBLE.TECHNICAL.FOUL",
        },
        0,
    )
    foul.previous_event = type(
        "PrevEvent",
        (),
        {
            "current_players": {
                1610612753: [353, 395, 727, 1732, 1894],
                1610612765: [283, 363, 711, 966, 1505],
            }
        },
    )()
    monkeypatch.setattr(
        StatsFoul,
        "base_stats",
        property(lambda self: [{"stat_key": "sentinel", "stat_value": 1}]),
    )

    with caplog.at_level("WARNING"):
        stats = foul.event_stats

    assert stats == [{"stat_key": "sentinel", "stat_value": 1}]
    assert any(
        "source_limited_bench_technical_no_team" in message
        for message in caplog.messages
    )


def test_double_technical_foul_infers_team_from_player2_team(monkeypatch):
    foul = StatsFoul(
        {
            "GAME_ID": "0021900282",
            "EVENTNUM": 186,
            "PCTIMESTRING": "8:46",
            "PERIOD": 2,
            "EVENTMSGACTIONTYPE": 16,
            "EVENTMSGTYPE": 6,
            "PLAYER1_ID": 2384,
            "PLAYER1_TEAM_ID": None,
            "PLAYER2_ID": 201566,
            "PLAYER2_TEAM_ID": 1610612745,
            "VIDEO_AVAILABLE_FLAG": 0,
            "NEUTRALDESCRIPTION": "Melvin Hunt Foul:DOUBLE.TECHNICAL.FOUL (K.Lane)",
        },
        0,
    )
    foul.previous_event = type(
        "PrevEvent",
        (),
        {
            "current_players": {
                1610612737: [101, 102, 103, 104, 105],
                1610612745: [201566, 202324, 202335, 203903, 204028],
            }
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
            "team_id": 1610612737,
            "opponent_team_id": 1610612745,
            "lineup_id": "101-102-103-104-105",
            "opponent_lineup_id": "201566-202324-202335-203903-204028",
            "stat_key": pbpstats.TECHNICAL_FOULS_COMMITTED_STRING,
            "stat_value": 1,
        }
    ]


def test_double_technical_foul_infers_team_from_player2_on_court(monkeypatch):
    foul = StatsFoul(
        {
            "GAME_ID": "0041300162",
            "EVENTNUM": 207,
            "PCTIMESTRING": "1:27",
            "PERIOD": 2,
            "EVENTMSGACTIONTYPE": 16,
            "EVENTMSGTYPE": 6,
            "PLAYER1_ID": 1941,
            "PLAYER1_TEAM_ID": None,
            "PLAYER2_ID": 979,
            "VIDEO_AVAILABLE_FLAG": 0,
            "NEUTRALDESCRIPTION": "Glenn Rivers Foul:DOUBLE.TECHNICAL.FOUL",
        },
        0,
    )
    foul.previous_event = type(
        "PrevEvent",
        (),
        {
            "current_players": {
                1610612741: [979, 202710, 202330, 203490, 203500],
                1610612746: [101, 102, 103, 104, 105],
            }
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
            "team_id": 1610612746,
            "opponent_team_id": 1610612741,
            "lineup_id": "101-102-103-104-105",
            "opponent_lineup_id": "202330-202710-203490-203500-979",
            "stat_key": pbpstats.TECHNICAL_FOULS_COMMITTED_STRING,
            "stat_value": 1,
        }
    ]
