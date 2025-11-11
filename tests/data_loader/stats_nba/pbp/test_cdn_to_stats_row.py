# -*- coding: utf-8 -*-
"""Tests for CDN to Stats row adapter"""
import pytest

from pbpstats.data_loader.stats_nba.pbp.cdn_adapter import cdn_to_stats_row


class TestCdnToStatsRow:
    """Test CDN action to v2 row conversion"""

    def test_made_3pt_with_assist(self):
        """Test made 3-pointer with assist"""
        action = {
            "actionNumber": 25,
            "period": 1,
            "clock": "PT10M17.00S",
            "actionType": "3pt",
            "subType": "jumpshot",
            "shotResult": "Made",
            "personId": 1628384,
            "teamId": 1610612761,
            "teamTricode": "TOR",
            "assistPersonId": 1627832,
            "scoreHome": 4,
            "scoreAway": 4,
            "description": "Anunoby 25' 3PT Jump Shot (3 PTS) (VanVleet 1 AST)",
        }

        row = cdn_to_stats_row(action, "0021900001")

        assert row["GAME_ID"] == "0021900001"
        assert row["EVENTNUM"] == 25
        assert row["PERIOD"] == 1
        assert row["PCTIMESTRING"] == "10:17"
        assert row["EVENTMSGTYPE"] == 1  # Made shot
        assert row["EVENTMSGACTIONTYPE"] == 1  # Jump shot
        assert row["PLAYER1_ID"] == 1628384
        assert row["PLAYER1_TEAM_ID"] == 1610612761
        assert row["PLAYER1_TEAM_ABBREVIATION"] == "TOR"
        assert row["PLAYER2_ID"] == 1627832  # Assist
        assert row["PLAYER2_TEAM_ID"] == 1610612761
        assert row["SCORE"] == "4-4"
        assert row["SCOREMARGIN"] == "TIE"
        assert "Anunoby" in row["NEUTRALDESCRIPTION"]

    def test_missed_2pt_with_block(self):
        """Test missed 2-pointer with block"""
        action = {
            "actionNumber": 10,
            "orderNumber": 10,
            "period": 1,
            "clock": "PT11M29.00S",
            "actionType": "2pt",
            "subType": "layup",
            "shotResult": "Missed",
            "personId": 1628384,
            "teamId": 1610612761,
            "blockPersonId": 202324,
            "description": "MISS Anunoby 3' Driving Layup",
        }

        row = cdn_to_stats_row(action, "0021900001")

        assert row["EVENTMSGTYPE"] == 2  # Missed shot
        assert row["EVENTMSGACTIONTYPE"] == 5  # Layup
        assert row["PLAYER1_ID"] == 1628384
        assert row["PLAYER3_ID"] == 202324  # Block
        assert row["SCORE"] is None
        assert row["SCOREMARGIN"] is None

    def test_turnover_with_steal(self):
        """Test turnover with steal"""
        action = {
            "actionNumber": 50,
            "period": 2,
            "clock": "PT5M30.00S",
            "actionType": "turnover",
            "subType": "badpass",
            "personId": 200768,
            "teamId": 1610612761,
            "stealPersonId": 1627742,
            "description": "Lowry Bad Pass Turnover (P1.T3)",
        }

        row = cdn_to_stats_row(action, "0021900001")

        assert row["EVENTMSGTYPE"] == 5  # Turnover
        assert row["EVENTMSGACTIONTYPE"] == 1  # Bad pass
        assert row["PLAYER1_ID"] == 200768
        assert row["PLAYER2_ID"] == 1627742  # Steal

    def test_shooting_foul(self):
        """Test shooting foul"""
        action = {
            "actionNumber": 18,
            "period": 1,
            "clock": "PT10M54.00S",
            "actionType": "foul",
            "subType": "shooting",
            "personId": 200755,
            "teamId": 1610612740,
            "foulDrawnPersonId": 200768,
            "description": "Redick S.FOUL (P1.T1) (T.Brown)",
        }

        row = cdn_to_stats_row(action, "0021900001")

        assert row["EVENTMSGTYPE"] == 6  # Foul
        assert row["EVENTMSGACTIONTYPE"] == 1  # Shooting foul
        assert row["PLAYER1_ID"] == 200755
        assert row["PLAYER2_ID"] == 200768  # Foul drawn

    def test_free_throw_2of2_made(self):
        """Test made free throw 2 of 2"""
        action = {
            "actionNumber": 21,
            "period": 1,
            "clock": "PT10M54.00S",
            "actionType": "freethrow",
            "subType": "2of2",
            "shotResult": "Made",
            "personId": 200768,
            "teamId": 1610612761,
            "scoreHome": 2,
            "scoreAway": 1,
            "description": "Lowry Free Throw 2 of 2 (2 PTS)",
        }

        row = cdn_to_stats_row(action, "0021900001")

        assert row["EVENTMSGTYPE"] == 3  # Free throw
        assert row["EVENTMSGACTIONTYPE"] == 11  # 2 of 2
        assert row["PLAYER1_ID"] == 200768
        assert row["SCORE"] == "2-1"
        assert row["SCOREMARGIN"] == "1"

    def test_rebound(self):
        """Test rebound"""
        action = {
            "actionNumber": 8,
            "period": 1,
            "clock": "PT11M47.00S",
            "actionType": "rebound",
            "personId": 202324,
            "teamId": 1610612740,
            "description": "Favors REBOUND (Off:1 Def:0)",
            "shotActionNumber": 7,
        }

        row = cdn_to_stats_row(action, "0021900001")

        assert row["EVENTMSGTYPE"] == 4  # Rebound
        assert row["PLAYER1_ID"] == 202324
        assert row["shotActionNumber"] == 7  # Extra field preserved

    def test_jumpball_with_recovered(self):
        """Test jump ball with recovered player"""
        action = {
            "actionNumber": 4,
            "period": 1,
            "clock": "PT12M00.00S",
            "actionType": "jumpball",
            "personId": 201188,
            "teamId": 1610612761,
            "jumpBallWonPersonId": 202324,
            "jumpBallRecoverdPersonId": 1628366,
            "description": "Jump Ball Gasol vs. Favors: Tip to Ball",
        }

        row = cdn_to_stats_row(action, "0021900001")

        assert row["EVENTMSGTYPE"] == 10  # Jump ball
        assert row["PLAYER1_ID"] == 201188
        assert row["PLAYER2_ID"] == 202324  # Won
        assert row["PLAYER3_ID"] == 1628366  # Recovered

    def test_period_start(self):
        """Test period start"""
        action = {
            "actionNumber": 2,
            "orderNumber": 1,
            "period": 1,
            "clock": "PT12M00.00S",
            "actionType": "period",
            "subType": "start",
        }

        row = cdn_to_stats_row(action, "0021900001")

        assert row["EVENTMSGTYPE"] == 12  # Start
        assert row["EVENTMSGACTIONTYPE"] == 12
        assert row["PERIOD"] == 1
        assert row["PCTIMESTRING"] == "12:00"

    def test_period_end(self):
        """Test period end"""
        action = {
            "actionNumber": 300,
            "period": 1,
            "clock": "PT0M00.00S",
            "actionType": "period",
            "subType": "end",
        }

        row = cdn_to_stats_row(action, "0021900001")

        assert row["EVENTMSGTYPE"] == 13  # End
        assert row["EVENTMSGACTIONTYPE"] == 13

    def test_game_start(self):
        """Test game start"""
        action = {
            "actionNumber": 0,
            "period": 0,
            "clock": "PT0M00.00S",
            "actionType": "game",
            "subType": "start",
        }

        row = cdn_to_stats_row(action, "0021900001")

        assert row["EVENTMSGTYPE"] == 12  # Start
        assert row["EVENTMSGACTIONTYPE"] == 12

    def test_substitution(self):
        """Test substitution"""
        action = {
            "actionNumber": 100,
            "period": 2,
            "clock": "PT8M00.00S",
            "actionType": "substitution",
            "personId": 203076,
            "teamId": 1610612761,
            "description": "SUB: Holiday IN, VanVleet OUT",
        }

        row = cdn_to_stats_row(action, "0021900001")

        assert row["EVENTMSGTYPE"] == 8  # Substitution
        assert row["PLAYER1_ID"] == 203076

    def test_timeout(self):
        """Test timeout"""
        action = {
            "actionNumber": 75,
            "period": 2,
            "clock": "PT6M30.00S",
            "actionType": "timeout",
            "teamId": 1610612761,
            "description": "Raptors Timeout: Regular",
        }

        row = cdn_to_stats_row(action, "0021900001")

        assert row["EVENTMSGTYPE"] == 9  # Timeout
        assert row["PLAYER1_TEAM_ID"] == 1610612761

    def test_violation(self):
        """Test violation"""
        action = {
            "actionNumber": 24,
            "period": 1,
            "clock": "PT10M27.00S",
            "actionType": "violation",
            "personId": 202324,
            "teamId": 1610612740,
            "description": "Favors Violation:Kicked Ball",
        }

        row = cdn_to_stats_row(action, "0021900001")

        assert row["EVENTMSGTYPE"] == 7  # Violation

    def test_instant_replay(self):
        """Test instant replay"""
        action = {
            "actionNumber": 150,
            "period": 3,
            "clock": "PT4M22.00S",
            "actionType": "instantreplay",
            "description": "Instant Replay - Support Ruling",
        }

        row = cdn_to_stats_row(action, "0021900001")

        assert row["EVENTMSGTYPE"] == 18  # Instant replay

    def test_negative_score_margin(self):
        """Test negative score margin"""
        action = {
            "actionNumber": 27,
            "period": 1,
            "clock": "PT10M11.00S",
            "actionType": "3pt",
            "shotResult": "Made",
            "personId": 1628366,
            "teamId": 1610612740,
            "scoreHome": 7,
            "scoreAway": 4,
            "description": "Ball 25' 3PT Jump Shot",
        }

        row = cdn_to_stats_row(action, "0021900001")

        assert row["SCORE"] == "7-4"
        assert row["SCOREMARGIN"] == "3"

    def test_extra_fields_preserved(self):
        """Test that extra CDN fields are preserved"""
        action = {
            "actionNumber": 50,
            "period": 2,
            "clock": "PT7M00.00S",
            "actionType": "2pt",
            "shotResult": "Made",
            "personId": 123456,
            "teamId": 1610612740,
            "x": 25.5,
            "y": 10.2,
            "xLegacy": 100,
            "yLegacy": 200,
            "shotDistance": 15,
            "area": "mid-range",
            "areaDetail": "left wing",
            "timeActual": "2023-10-24T01:05:30Z",
            "orderNumber": 51,
            "qualifiers": ["driving", "contested"],
            "descriptor": "pullup",
            "edited": True,
            "isTargetScoreLastPeriod": False,
        }

        row = cdn_to_stats_row(action, "0021900001")

        assert row["x"] == 25.5
        assert row["y"] == 10.2
        assert row["shotDistance"] == 15
        assert row["area"] == "mid-range"
        assert row["timeActual"] == "2023-10-24T01:05:30Z"
        assert row["edited"] is True
        assert row["isTargetScoreLastPeriod"] is False

    def test_all_required_v2_fields_present(self):
        """Test that all required v2 fields are present in output"""
        action = {
            "actionNumber": 1,
            "period": 1,
            "clock": "PT12M00.00S",
            "actionType": "period",
            "subType": "start",
        }

        row = cdn_to_stats_row(action, "0021900001")

        # All required v2 fields
        required_fields = [
            "GAME_ID",
            "EVENTNUM",
            "EVENTMSGTYPE",
            "EVENTMSGACTIONTYPE",
            "PERIOD",
            "PCTIMESTRING",
            "HOMEDESCRIPTION",
            "NEUTRALDESCRIPTION",
            "VISITORDESCRIPTION",
            "SCORE",
            "SCOREMARGIN",
            "WCTIMESTRING",
            "PERSON1TYPE",
            "PLAYER1_ID",
            "PLAYER1_NAME",
            "PLAYER1_TEAM_ID",
            "PLAYER1_TEAM_CITY",
            "PLAYER1_TEAM_NICKNAME",
            "PLAYER1_TEAM_ABBREVIATION",
            "PERSON2TYPE",
            "PLAYER2_ID",
            "PLAYER2_NAME",
            "PLAYER2_TEAM_ID",
            "PLAYER2_TEAM_CITY",
            "PLAYER2_TEAM_NICKNAME",
            "PLAYER2_TEAM_ABBREVIATION",
            "PERSON3TYPE",
            "PLAYER3_ID",
            "PLAYER3_NAME",
            "PLAYER3_TEAM_ID",
            "PLAYER3_TEAM_CITY",
            "PLAYER3_TEAM_NICKNAME",
            "PLAYER3_TEAM_ABBREVIATION",
            "VIDEO_AVAILABLE_FLAG",
        ]

        for field in required_fields:
            assert field in row, f"Missing required field: {field}"
