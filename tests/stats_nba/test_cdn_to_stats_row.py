# -*- coding: utf-8 -*-
from pbpstats.data_loader.stats_nba.pbp.cdn_adapter import cdn_to_stats_row


GAME_ID = "0029999999"


def test_made_three_with_assist_sets_expected_fields():
    action = {
        "actionNumber": 15,
        "orderNumber": 20,
        "period": 1,
        "clock": "PT11M38S",
        "actionType": "3pt",
        "shotResult": "Made",
        "subType": "JumpShot",
        "description": "Player makes 3PT jump shot (assist)",
        "teamId": 1610612737,
        "personId": 123,
        "assistPersonId": 456,
        "scoreHome": 3,
        "scoreAway": 0,
        "timeActual": "2024-01-01T00:00:10Z",
    }

    row = cdn_to_stats_row(action, GAME_ID)

    assert row["GAME_ID"] == GAME_ID
    assert row["EVENTNUM"] == 15
    assert row["PERIOD"] == 1
    assert row["PCTIMESTRING"] == "11:38"
    assert row["WCTIMESTRING"] == "2024-01-01T00:00:10Z"
    assert row["EVENTMSGTYPE"] == 1
    assert row["EVENTMSGACTIONTYPE"] == 1
    assert row["PLAYER1_ID"] == 123
    assert row["PLAYER2_ID"] == 456
    assert row["SCORE"] == "3-0"
    assert row["SCOREMARGIN"] == "3"


def test_missed_two_with_block_sets_player3():
    action = {
        "actionNumber": 40,
        "orderNumber": 45,
        "period": 2,
        "clock": "PT08M00S",
        "actionType": "2pt",
        "shotResult": "Missed",
        "subType": "Layup",
        "blockPersonId": 789,
        "personId": 123,
        "teamId": 1,
        "scoreHome": 25,
        "scoreAway": 24,
        "timeActual": "2024-01-01T00:05:00Z",
    }

    row = cdn_to_stats_row(action, GAME_ID)

    assert row["EVENTMSGTYPE"] == 2
    assert row["PLAYER3_ID"] == 789
    assert row["SCOREMARGIN"] == "1"


def test_turnover_with_steal_sets_player2():
    action = {
        "actionNumber": 55,
        "orderNumber": 60,
        "period": 3,
        "clock": "PT05M11S",
        "actionType": "Turnover",
        "subType": "lostball",
        "personId": 100,
        "stealPersonId": 200,
        "timeActual": "2024-01-01T00:05:15Z",
    }

    row = cdn_to_stats_row(action, GAME_ID)

    assert row["EVENTMSGTYPE"] == 5
    assert row["PLAYER2_ID"] == 200


def test_foul_drawn_person_sets_player2():
    action = {
        "actionNumber": 65,
        "orderNumber": 70,
        "period": 3,
        "clock": "PT03M02S",
        "actionType": "Foul",
        "descriptor": "Shooting",
        "personId": 250,
        "foulDrawnPersonId": 300,
        "timeActual": "2024-01-01T00:05:30Z",
    }

    row = cdn_to_stats_row(action, GAME_ID)

    assert row["EVENTMSGTYPE"] == 6
    assert row["PLAYER2_ID"] == 300


def test_free_throw_trip_sets_eventmsgactiontype():
    action = {
        "actionNumber": 75,
        "orderNumber": 80,
        "period": 4,
        "clock": "PT01M15S",
        "actionType": "FreeThrow",
        "shotResult": "Made",
        "subType": "2of2",
        "timeActual": "2024-01-01T00:06:00Z",
    }

    row = cdn_to_stats_row(action, GAME_ID)

    assert row["EVENTMSGTYPE"] == 3
    assert row["EVENTMSGACTIONTYPE"] == 11


def test_jumpball_recovered_sets_player3():
    action = {
        "actionNumber": 5,
        "orderNumber": 5,
        "period": 1,
        "clock": "PT12M00S",
        "actionType": "JumpBall",
        "jumpBallWonPersonId": 111,
        "jumpBallLostPersonId": 222,
        "jumpBallRecoverdPersonId": 333,
        "timeActual": "2024-01-01T00:06:30Z",
    }

    row = cdn_to_stats_row(action, GAME_ID)

    assert row["EVENTMSGTYPE"] == 10
    assert row["PLAYER1_ID"] == 111
    assert row["PLAYER2_ID"] == 222
    assert row["PLAYER3_ID"] == 333


def test_period_start_and_end_map_to_events():
    start = {
        "actionNumber": 1,
        "period": 1,
        "clock": "PT12M00S",
        "actionType": "Period",
        "subType": "start",
        "timeActual": "2024-01-01T00:00:00Z",
    }
    end = {
        "actionNumber": 120,
        "period": 1,
        "clock": "PT0M00S",
        "actionType": "Period",
        "subType": "end",
        "timeActual": "2024-01-01T00:15:00Z",
    }

    row_start = cdn_to_stats_row(start, GAME_ID)
    row_end = cdn_to_stats_row(end, GAME_ID)

    assert row_start["EVENTMSGTYPE"] == 12
    assert row_start["EVENTMSGACTIONTYPE"] == 0
    assert row_end["EVENTMSGTYPE"] == 13
    assert row_end["EVENTMSGACTIONTYPE"] == 0
    assert row_start["WCTIMESTRING"] == "2024-01-01T00:00:00Z"
    assert row_end["WCTIMESTRING"] == "2024-01-01T00:15:00Z"


def test_missing_description_defaults_to_empty_string():
    action = {
        "actionNumber": 33,
        "orderNumber": 40,
        "period": 2,
        "clock": "PT05M00S",
        "actionType": "Timeout",
        "subType": "Official",
        "timeActual": "2024-01-01T00:10:00Z",
    }

    row = cdn_to_stats_row(action, GAME_ID)

    assert row["NEUTRALDESCRIPTION"] == ""
    assert row["WCTIMESTRING"] == "2024-01-01T00:10:00Z"


def test_unknown_event_types_map_to_zero_codes():
    action = {
        "actionNumber": 77,
        "orderNumber": 90,
        "period": 2,
        "clock": "PT04M59S",
        "actionType": "Stoppage",
        "timeActual": "2024-01-01T00:11:00Z",
    }

    row = cdn_to_stats_row(action, GAME_ID)

    assert row["EVENTMSGTYPE"] == 0
    assert row["EVENTMSGACTIONTYPE"] == 0
