import pytest

from pbpstats.resources.enhanced_pbp.stats_nba.field_goal import StatsFieldGoal
from pbpstats.resources.enhanced_pbp.stats_nba.rebound import StatsRebound
from pbpstats.resources.enhanced_pbp.stats_nba.turnover import StatsTurnover


def _field_goal_event(**overrides):
    event = {
        "GAME_ID": overrides.get("GAME_ID", "0021900001"),
        "EVENTNUM": overrides.get("EVENTNUM", 1),
        "PCTIMESTRING": overrides.get("PCTIMESTRING", "12:00"),
        "PERIOD": overrides.get("PERIOD", 1),
        "HOMEDESCRIPTION": overrides.get(
            "HOMEDESCRIPTION", "Player  2PT Jump Shot"
        ),
        "EVENTMSGACTIONTYPE": overrides.get("EVENTMSGACTIONTYPE", 1),
        "EVENTMSGTYPE": overrides.get("EVENTMSGTYPE", 1),
        "PLAYER1_ID": overrides.get("PLAYER1_ID", 1),
        "PLAYER1_TEAM_ID": overrides.get("PLAYER1_TEAM_ID", 1610612737),
        "PLAYER2_ID": overrides.get("PLAYER2_ID"),
        "PLAYER2_TEAM_ID": overrides.get("PLAYER2_TEAM_ID"),
        "PLAYER3_ID": overrides.get("PLAYER3_ID"),
        "PLAYER3_TEAM_ID": overrides.get("PLAYER3_TEAM_ID"),
    }
    return event


def _rebound_event(**overrides):
    event = {
        "GAME_ID": overrides.get("GAME_ID", "0021900001"),
        "EVENTNUM": overrides.get("EVENTNUM", 2),
        "PCTIMESTRING": overrides.get("PCTIMESTRING", "01:14"),
        "PERIOD": overrides.get("PERIOD", 1),
        "HOMEDESCRIPTION": overrides.get("HOMEDESCRIPTION", "Player Rebound"),
        "EVENTMSGACTIONTYPE": overrides.get("EVENTMSGACTIONTYPE", 0),
        "EVENTMSGTYPE": 4,
        "PLAYER1_ID": overrides.get("PLAYER1_ID", 2),
        "PLAYER1_TEAM_ID": overrides.get("PLAYER1_TEAM_ID", 1610612737),
        "PLAYER2_ID": overrides.get("PLAYER2_ID"),
        "PLAYER2_TEAM_ID": overrides.get("PLAYER2_TEAM_ID"),
        "PLAYER3_ID": overrides.get("PLAYER3_ID"),
        "PLAYER3_TEAM_ID": overrides.get("PLAYER3_TEAM_ID"),
    }
    return event


def _turnover_event(**overrides):
    event = {
        "GAME_ID": overrides.get("GAME_ID", "0021900001"),
        "EVENTNUM": overrides.get("EVENTNUM", 3),
        "PCTIMESTRING": overrides.get("PCTIMESTRING", "08:15"),
        "PERIOD": overrides.get("PERIOD", 1),
        "HOMEDESCRIPTION": overrides.get("HOMEDESCRIPTION", "Bad Pass Turnover"),
        "EVENTMSGACTIONTYPE": overrides.get("EVENTMSGACTIONTYPE", 1),
        "EVENTMSGTYPE": 5,
        "PLAYER1_ID": overrides.get("PLAYER1_ID", 3),
        "PLAYER1_TEAM_ID": overrides.get("PLAYER1_TEAM_ID", 1610612737),
        "PLAYER3_ID": overrides.get("PLAYER3_ID", 4),
        "PLAYER3_TEAM_ID": overrides.get("PLAYER3_TEAM_ID", 1610612747),
    }
    return event


def _stats_field_goal(order=1, **overrides):
    return StatsFieldGoal(_field_goal_event(**overrides), order)


def _stats_rebound(order=1, **overrides):
    return StatsRebound(_rebound_event(**overrides), order)


def _stats_turnover(order=1, **overrides):
    return StatsTurnover(_turnover_event(**overrides), order)


def test_game_seconds_elapsed_counts_game_time():
    fg_event = _stats_field_goal(PERIOD=2, PCTIMESTRING="11:30", EVENTNUM=10)
    assert fg_event.game_seconds_elapsed == pytest.approx(750)


def test_game_seconds_elapsed_handles_wnba_overtime_length():
    fg_event = _stats_field_goal(
        GAME_ID="1022000001", PERIOD=5, PCTIMESTRING="04:00", EVENTNUM=11
    )
    assert fg_event.game_seconds_elapsed == pytest.approx(2460)


def test_event_length_seconds_uses_next_event_time():
    first_event = _stats_field_goal(PCTIMESTRING="10:00", EVENTNUM=21)
    second_event = _stats_field_goal(order=2, PCTIMESTRING="09:45", EVENTNUM=22)
    first_event.next_event = second_event
    second_event.previous_event = first_event
    assert first_event.event_length_seconds == pytest.approx(15)


def test_event_length_seconds_returns_zero_without_next_event():
    final_event = _stats_field_goal(PCTIMESTRING="00:01", EVENTNUM=30)
    assert final_event.event_length_seconds == 0


def test_is_three_and_block_flags():
    three_event = _stats_field_goal(
        HOMEDESCRIPTION="Player  3PT Jump Shot",
        PCTIMESTRING="09:59",
        EVENTNUM=40,
    )
    blocked_event = _stats_field_goal(
        EVENTMSGTYPE=2,
        PCTIMESTRING="09:30",
        PLAYER3_ID=42,
        HOMEDESCRIPTION="Player  2PT Jump Shot",
        EVENTNUM=41,
    )
    assert three_event.is_three is True
    assert blocked_event.is_block is True


def test_is_steal_true_for_turnover_and_false_otherwise():
    turnover_event = _stats_turnover()
    field_goal_event = _stats_field_goal(EVENTNUM=50)
    assert turnover_event.is_steal is True
    assert field_goal_event.is_steal is False


def test_rebound_putback_flag_within_three_seconds():
    miss = _stats_field_goal(EVENTMSGTYPE=2, PCTIMESTRING="01:15", EVENTNUM=60)
    rebound = _stats_rebound(order=2, PCTIMESTRING="01:14", EVENTNUM=61)
    putback = _stats_field_goal(order=3, PCTIMESTRING="01:12", EVENTNUM=62)
    miss.previous_event = None
    miss.next_event = rebound
    rebound.previous_event = miss
    rebound.next_event = putback
    putback.previous_event = rebound
    putback.next_event = None
    assert rebound.is_putback is True


def test_rebound_putback_flag_false_outside_window():
    miss = _stats_field_goal(EVENTMSGTYPE=2, PCTIMESTRING="02:00", EVENTNUM=70)
    rebound = _stats_rebound(order=2, PCTIMESTRING="01:59", EVENTNUM=71)
    follow = _stats_field_goal(order=3, PCTIMESTRING="01:54", EVENTNUM=72)
    miss.next_event = rebound
    rebound.previous_event = miss
    rebound.next_event = follow
    follow.previous_event = rebound
    assert rebound.is_putback is False
