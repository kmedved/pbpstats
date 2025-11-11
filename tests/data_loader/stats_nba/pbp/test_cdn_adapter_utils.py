# -*- coding: utf-8 -*-
"""Tests for CDN adapter utility functions"""
import pytest

from pbpstats.data_loader.stats_nba.pbp.cdn_adapter import (
    iso_to_pctimestring,
    map_eventmsgtype,
    map_eventmsgactiontype,
    SHOT_MAP,
    FT_MAP,
    TOV_MAP,
    FOUL_MAP,
)


class TestIsoToPctimestring:
    """Test ISO8601 to PCTIMESTRING conversion"""

    def test_full_time_with_seconds(self):
        """Test full time with minutes and seconds"""
        assert iso_to_pctimestring("PT11M38.00S") == "11:38"
        assert iso_to_pctimestring("PT11M38S") == "11:38"

    def test_with_fractional_seconds(self):
        """Test time with fractional seconds"""
        assert iso_to_pctimestring("PT0M00.50S") == "0:00.5"
        assert iso_to_pctimestring("PT0M00.05S") == "0:00.05"
        assert iso_to_pctimestring("PT5M30.75S") == "5:30.75"

    def test_full_minutes(self):
        """Test full minutes without seconds"""
        assert iso_to_pctimestring("PT12M00S") == "12:00"
        assert iso_to_pctimestring("PT12M00.00S") == "12:00"

    def test_only_seconds(self):
        """Test only seconds without minutes"""
        assert iso_to_pctimestring("PT45S") == "0:45"
        assert iso_to_pctimestring("PT05S") == "0:05"

    def test_zero_time(self):
        """Test zero time"""
        assert iso_to_pctimestring("PT0M00S") == "0:00"
        assert iso_to_pctimestring("PT0M00.00S") == "0:00"

    def test_none_and_empty(self):
        """Test None and empty string"""
        assert iso_to_pctimestring(None) == "0:00"
        assert iso_to_pctimestring("") == "0:00"

    def test_malformed(self):
        """Test malformed input"""
        assert iso_to_pctimestring("invalid") == "0:00"
        assert iso_to_pctimestring("12:00") == "0:00"


class TestMapEventmsgtype:
    """Test CDN actionType to v2 EVENTMSGTYPE mapping"""

    def test_made_shot(self):
        """Test made shots map to type 1"""
        assert map_eventmsgtype({"actionType": "2pt", "shotResult": "Made"}) == 1
        assert map_eventmsgtype({"actionType": "3pt", "shotResult": "Made"}) == 1

    def test_missed_shot(self):
        """Test missed shots map to type 2"""
        assert map_eventmsgtype({"actionType": "2pt", "shotResult": "Missed"}) == 2
        assert map_eventmsgtype({"actionType": "3pt", "shotResult": "Missed"}) == 2

    def test_free_throw(self):
        """Test free throw maps to type 3"""
        assert map_eventmsgtype({"actionType": "freethrow"}) == 3
        assert map_eventmsgtype({"actionType": "Free Throw"}) == 3

    def test_rebound(self):
        """Test rebound maps to type 4"""
        assert map_eventmsgtype({"actionType": "rebound"}) == 4

    def test_turnover(self):
        """Test turnover maps to type 5"""
        assert map_eventmsgtype({"actionType": "turnover"}) == 5

    def test_foul(self):
        """Test foul maps to type 6"""
        assert map_eventmsgtype({"actionType": "foul"}) == 6

    def test_violation(self):
        """Test violation maps to type 7"""
        assert map_eventmsgtype({"actionType": "violation"}) == 7

    def test_substitution(self):
        """Test substitution maps to type 8"""
        assert map_eventmsgtype({"actionType": "substitution"}) == 8

    def test_timeout(self):
        """Test timeout maps to type 9"""
        assert map_eventmsgtype({"actionType": "timeout"}) == 9

    def test_jumpball(self):
        """Test jump ball maps to type 10"""
        assert map_eventmsgtype({"actionType": "jumpball"}) == 10
        assert map_eventmsgtype({"actionType": "jump ball"}) == 10

    def test_ejection(self):
        """Test ejection maps to type 11"""
        assert map_eventmsgtype({"actionType": "ejection"}) == 11

    def test_instant_replay(self):
        """Test instant replay maps to type 18"""
        assert map_eventmsgtype({"actionType": "instantreplay"}) == 18
        assert map_eventmsgtype({"actionType": "instant replay"}) == 18

    def test_period_and_game(self):
        """Test period and game return None (handled separately)"""
        assert map_eventmsgtype({"actionType": "period"}) is None
        assert map_eventmsgtype({"actionType": "game"}) is None

    def test_unknown_action_type(self):
        """Test unknown action type returns None"""
        assert map_eventmsgtype({"actionType": "unknown"}) is None
        assert map_eventmsgtype({}) is None


class TestMapEventmsgactiontype:
    """Test CDN subType/descriptor to v2 EVENTMSGACTIONTYPE mapping"""

    def test_shot_types(self):
        """Test various shot types"""
        # Jump shot
        assert (
            map_eventmsgactiontype({"actionType": "2pt", "subType": "jumpshot"}, 1)
            == 1
        )
        assert (
            map_eventmsgactiontype(
                {"actionType": "2pt", "subType": "jump shot"}, 1
            )
            == 1
        )

        # Layup
        assert (
            map_eventmsgactiontype({"actionType": "2pt", "subType": "layup"}, 2) == 5
        )

        # Dunk
        assert (
            map_eventmsgactiontype({"actionType": "2pt", "subType": "dunk"}, 1) == 7
        )

        # Hook
        assert (
            map_eventmsgactiontype({"actionType": "2pt", "subType": "hook"}, 1) == 3
        )
        assert (
            map_eventmsgactiontype({"actionType": "2pt", "subType": "hookshot"}, 1)
            == 3
        )

        # Tip in
        assert (
            map_eventmsgactiontype({"actionType": "2pt", "subType": "tipin"}, 1) == 2
        )

    def test_free_throw_types(self):
        """Test free throw types"""
        assert (
            map_eventmsgactiontype({"actionType": "freethrow", "subType": "1of1"}, 3)
            == 10
        )
        assert (
            map_eventmsgactiontype({"actionType": "freethrow", "subType": "1of2"}, 3)
            == 10
        )
        assert (
            map_eventmsgactiontype({"actionType": "freethrow", "subType": "2of2"}, 3)
            == 11
        )
        assert (
            map_eventmsgactiontype({"actionType": "freethrow", "subType": "1of3"}, 3)
            == 13
        )
        assert (
            map_eventmsgactiontype({"actionType": "freethrow", "subType": "2of3"}, 3)
            == 14
        )
        assert (
            map_eventmsgactiontype({"actionType": "freethrow", "subType": "3of3"}, 3)
            == 15
        )

    def test_free_throw_technical_flagrant(self):
        """Test technical and flagrant free throws"""
        assert (
            map_eventmsgactiontype(
                {"actionType": "freethrow", "descriptor": "technical"}, 3
            )
            == 16
        )
        assert (
            map_eventmsgactiontype(
                {"actionType": "freethrow", "descriptor": "flagrant type 1"}, 3
            )
            == 17
        )

    def test_turnover_types(self):
        """Test turnover types"""
        assert (
            map_eventmsgactiontype({"actionType": "turnover", "subType": "badpass"}, 5)
            == 1
        )
        assert (
            map_eventmsgactiontype(
                {"actionType": "turnover", "subType": "bad pass"}, 5
            )
            == 1
        )
        assert (
            map_eventmsgactiontype(
                {"actionType": "turnover", "subType": "lostball"}, 5
            )
            == 2
        )
        assert (
            map_eventmsgactiontype(
                {"actionType": "turnover", "subType": "traveling"}, 5
            )
            == 4
        )
        assert (
            map_eventmsgactiontype(
                {"actionType": "turnover", "subType": "shotclock"}, 5
            )
            == 11
        )
        assert (
            map_eventmsgactiontype(
                {"actionType": "turnover", "subType": "3-second-violation"}, 5
            )
            == 7
        )
        assert (
            map_eventmsgactiontype(
                {"actionType": "turnover", "subType": "outofbounds"}, 5
            )
            == 40
        )

    def test_foul_types(self):
        """Test foul types"""
        assert (
            map_eventmsgactiontype({"actionType": "foul", "subType": "shooting"}, 6)
            == 1
        )
        assert (
            map_eventmsgactiontype(
                {"actionType": "foul", "subType": "looseball"}, 6
            )
            == 5
        )
        assert (
            map_eventmsgactiontype({"actionType": "foul", "subType": "offensive"}, 6)
            == 2
        )
        assert (
            map_eventmsgactiontype({"actionType": "foul", "subType": "charge"}, 6)
            == 3
        )
        assert (
            map_eventmsgactiontype({"actionType": "foul", "subType": "technical"}, 6)
            == 11
        )
        assert (
            map_eventmsgactiontype(
                {"actionType": "foul", "descriptor": "flagrant type 1"}, 6
            )
            == 14
        )
        assert (
            map_eventmsgactiontype(
                {"actionType": "foul", "descriptor": "flagrant type 2"}, 6
            )
            == 15
        )

    def test_period_game_start_end(self):
        """Test period and game start/end"""
        assert (
            map_eventmsgactiontype({"actionType": "period", "subType": "start"}, None)
            == 12
        )
        assert (
            map_eventmsgactiontype({"actionType": "period", "subType": "end"}, None)
            == 13
        )
        assert (
            map_eventmsgactiontype({"actionType": "game", "subType": "start"}, None)
            == 12
        )
        assert (
            map_eventmsgactiontype({"actionType": "game", "subType": "end"}, None)
            == 13
        )

    def test_unknown_returns_none(self):
        """Test unknown types return None"""
        assert (
            map_eventmsgactiontype(
                {"actionType": "2pt", "subType": "unknown"}, 1
            )
            is None
        )
        assert map_eventmsgactiontype({"actionType": "unknown"}, None) is None


class TestMappingDictionaries:
    """Test that mapping dictionaries have expected keys"""

    def test_shot_map_has_common_types(self):
        """Test SHOT_MAP has common shot types"""
        assert "jumpshot" in SHOT_MAP
        assert "layup" in SHOT_MAP
        assert "dunk" in SHOT_MAP

    def test_ft_map_has_common_types(self):
        """Test FT_MAP has common free throw types"""
        assert "1of1" in FT_MAP
        assert "1of2" in FT_MAP
        assert "2of2" in FT_MAP

    def test_tov_map_has_common_types(self):
        """Test TOV_MAP has common turnover types"""
        assert "badpass" in TOV_MAP
        assert "lostball" in TOV_MAP
        assert "traveling" in TOV_MAP

    def test_foul_map_has_common_types(self):
        """Test FOUL_MAP has common foul types"""
        assert "shooting" in FOUL_MAP
        assert "offensive" in FOUL_MAP
        assert "technical" in FOUL_MAP
