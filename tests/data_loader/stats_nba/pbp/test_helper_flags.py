# -*- coding: utf-8 -*-
"""Tests for helper boolean flags"""
import pytest

from pbpstats.data_loader.stats_nba.pbp.cdn_adapter import cdn_to_stats_row


class TestHelperFlags:
    """Test helper boolean flags added to v2 rows"""

    def test_shooting_foul_flag(self):
        """Test is_shooting_foul flag"""
        action = {
            "actionNumber": 18,
            "period": 1,
            "clock": "PT10M54.00S",
            "actionType": "foul",
            "subType": "shooting",
            "personId": 200755,
            "teamId": 1610612740,
        }

        row = cdn_to_stats_row(action, "0021900001")

        assert row["is_shooting_foul"] is True
        assert row["is_offensive_foul"] is False

    def test_offensive_foul_flag(self):
        """Test is_offensive_foul flag"""
        action = {
            "actionNumber": 29,
            "period": 1,
            "clock": "PT10M04.00S",
            "actionType": "foul",
            "subType": "offensive",
            "personId": 200768,
            "teamId": 1610612761,
        }

        row = cdn_to_stats_row(action, "0021900001")

        assert row["is_offensive_foul"] is True
        assert row["is_shooting_foul"] is False

    def test_charge_flag(self):
        """Test is_charge flag"""
        action = {
            "actionNumber": 50,
            "period": 2,
            "clock": "PT8M30.00S",
            "actionType": "foul",
            "subType": "charge",
            "personId": 203076,
            "teamId": 1610612740,
        }

        row = cdn_to_stats_row(action, "0021900001")

        assert row["is_charge"] is True
        assert row["is_offensive_foul"] is True  # Charge is also offensive

    def test_technical_foul_flag(self):
        """Test is_technical flag"""
        action = {
            "actionNumber": 75,
            "period": 2,
            "clock": "PT6M15.00S",
            "actionType": "foul",
            "subType": "technical",
            "personId": 201950,
            "teamId": 1610612740,
        }

        row = cdn_to_stats_row(action, "0021900001")

        assert row["is_technical"] is True

    def test_flagrant_foul_flag(self):
        """Test is_flagrant flag"""
        action = {
            "actionNumber": 100,
            "period": 3,
            "clock": "PT5M00.00S",
            "actionType": "foul",
            "descriptor": "flagrant type 1",
            "personId": 202324,
            "teamId": 1610612740,
        }

        row = cdn_to_stats_row(action, "0021900001")

        assert row["is_flagrant"] is True

    def test_take_foul_flag(self):
        """Test is_transition_take_foul flag"""
        action = {
            "actionNumber": 120,
            "period": 4,
            "clock": "PT2M00.00S",
            "actionType": "foul",
            "subType": "take",
            "personId": 1627742,
            "teamId": 1610612740,
        }

        row = cdn_to_stats_row(action, "0021900001")

        assert row["is_transition_take_foul"] is True

    def test_loose_ball_foul_flag(self):
        """Test is_loose_ball_foul flag"""
        action = {
            "actionNumber": 85,
            "period": 3,
            "clock": "PT7M45.00S",
            "actionType": "foul",
            "subType": "looseball",
            "personId": 200768,
            "teamId": 1610612761,
        }

        row = cdn_to_stats_row(action, "0021900001")

        assert row["is_loose_ball_foul"] is True

    def test_free_throw_1of1_flags(self):
        """Test FT flags for 1 of 1"""
        action = {
            "actionNumber": 25,
            "period": 1,
            "clock": "PT9M30.00S",
            "actionType": "freethrow",
            "subType": "1of1",
            "shotResult": "Made",
            "personId": 200768,
            "teamId": 1610612761,
        }

        row = cdn_to_stats_row(action, "0021900001")

        assert row["is_end_ft"] is True
        assert row["num_ft_for_trip"] == 1

    def test_free_throw_1of2_flags(self):
        """Test FT flags for 1 of 2"""
        action = {
            "actionNumber": 20,
            "period": 1,
            "clock": "PT10M54.00S",
            "actionType": "freethrow",
            "subType": "1of2",
            "shotResult": "Made",
            "personId": 200768,
            "teamId": 1610612761,
        }

        row = cdn_to_stats_row(action, "0021900001")

        assert row["is_end_ft"] is False
        assert row["num_ft_for_trip"] == 2

    def test_free_throw_2of2_flags(self):
        """Test FT flags for 2 of 2"""
        action = {
            "actionNumber": 21,
            "period": 1,
            "clock": "PT10M54.00S",
            "actionType": "freethrow",
            "subType": "2of2",
            "shotResult": "Made",
            "personId": 200768,
            "teamId": 1610612761,
        }

        row = cdn_to_stats_row(action, "0021900001")

        assert row["is_end_ft"] is True
        assert row["num_ft_for_trip"] == 2

    def test_free_throw_3of3_flags(self):
        """Test FT flags for 3 of 3"""
        action = {
            "actionNumber": 45,
            "period": 2,
            "clock": "PT7M20.00S",
            "actionType": "freethrow",
            "subType": "3of3",
            "shotResult": "Missed",
            "personId": 1627742,
            "teamId": 1610612740,
        }

        row = cdn_to_stats_row(action, "0021900001")

        assert row["is_end_ft"] is True
        assert row["num_ft_for_trip"] == 3

    def test_technical_free_throw_flag(self):
        """Test is_technical_ft flag"""
        action = {
            "actionNumber": 80,
            "period": 2,
            "clock": "PT6M00.00S",
            "actionType": "freethrow",
            "subType": "1of1",
            "descriptor": "technical",
            "shotResult": "Made",
            "personId": 200768,
            "teamId": 1610612761,
        }

        row = cdn_to_stats_row(action, "0021900001")

        assert row["is_technical_ft"] is True
        assert row["is_flagrant_ft"] is False

    def test_flagrant_free_throw_flag(self):
        """Test is_flagrant_ft flag"""
        action = {
            "actionNumber": 105,
            "period": 3,
            "clock": "PT4M30.00S",
            "actionType": "freethrow",
            "subType": "1of2",
            "descriptor": "flagrant type 1",
            "shotResult": "Made",
            "personId": 1627742,
            "teamId": 1610612740,
        }

        row = cdn_to_stats_row(action, "0021900001")

        assert row["is_flagrant_ft"] is True
        assert row["is_technical_ft"] is False

    def test_target_score_last_period_flag(self):
        """Test is_target_score_last_period flag"""
        action = {
            "actionNumber": 500,
            "period": 4,
            "clock": "PT0M05.00S",
            "actionType": "2pt",
            "shotResult": "Made",
            "personId": 200768,
            "teamId": 1610612761,
            "isTargetScoreLastPeriod": True,
        }

        row = cdn_to_stats_row(action, "0021900001")

        assert row["is_target_score_last_period"] is True

    def test_no_flags_for_non_foul_non_ft(self):
        """Test that non-foul, non-FT events don't have these flags"""
        action = {
            "actionNumber": 10,
            "period": 1,
            "clock": "PT11M00.00S",
            "actionType": "2pt",
            "shotResult": "Made",
            "personId": 123,
            "teamId": 1610612740,
        }

        row = cdn_to_stats_row(action, "0021900001")

        # These flags shouldn't be present for non-fouls/non-FTs
        assert "is_shooting_foul" not in row
        assert "is_end_ft" not in row

    def test_multiple_foul_flags_can_be_true(self):
        """Test that multiple foul flags can be true simultaneously"""
        action = {
            "actionNumber": 150,
            "period": 4,
            "clock": "PT1M00.00S",
            "actionType": "foul",
            "subType": "offensive",
            "descriptor": "charge offensive",
            "personId": 202324,
            "teamId": 1610612740,
        }

        row = cdn_to_stats_row(action, "0021900001")

        # Both offensive and charge should be true
        assert row["is_offensive_foul"] is True
        assert row["is_charge"] is True
