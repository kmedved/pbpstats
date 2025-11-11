# -*- coding: utf-8 -*-
"""Tests for dedupe and polish logic"""
import pytest
from unittest.mock import patch

from pbpstats.data_loader.stats_nba.pbp.web import StatsNbaPbpWebLoader


class TestDedupePolish:
    """Test deduplication and polish logic"""

    def test_dedupe_prefers_edited_version(self):
        """Test that edited versions replace originals"""
        loader = StatsNbaPbpWebLoader()

        mock_cdn_data = {
            "game": {
                "actions": [
                    {
                        "actionNumber": 10,
                        "period": 1,
                        "clock": "PT10M00.00S",
                        "actionType": "2pt",
                        "shotResult": "Made",
                        "personId": 123,
                        "teamId": 1610612740,
                        "timeActual": "2023-10-24T01:05:00Z",
                        "description": "Original",
                    },
                    {
                        "actionNumber": 10,
                        "period": 1,
                        "clock": "PT10M00.00S",
                        "actionType": "2pt",
                        "shotResult": "Made",
                        "personId": 123,
                        "teamId": 1610612740,
                        "timeActual": "2023-10-24T01:05:00Z",
                        "edited": True,
                        "description": "Edited",
                    },
                ]
            }
        }

        with patch(
            "pbpstats.data_loader.stats_nba.pbp.web.get_pbp_actions",
            return_value=mock_cdn_data,
        ):
            result = loader.load_data("0021900001")

            rows = result["resultSets"][0]["rowSet"]
            # Should only have 1 row after deduping
            assert len(rows) == 1

            # Verify it's the edited version
            headers = result["resultSets"][0]["headers"]
            row_dict = dict(zip(headers, rows[0]))
            # The description should be from the edited version
            assert "Edited" in row_dict["NEUTRALDESCRIPTION"]

    def test_rebound_follows_shot_when_shotActionNumber_present(self):
        """Test that rebounds are reordered to follow their shot"""
        loader = StatsNbaPbpWebLoader()

        mock_cdn_data = {
            "game": {
                "actions": [
                    {
                        "actionNumber": 5,
                        "orderNumber": 5,
                        "period": 1,
                        "clock": "PT11M00.00S",
                        "actionType": "rebound",
                        "personId": 456,
                        "teamId": 1610612761,
                        "shotActionNumber": 10,  # References shot 10
                    },
                    {
                        "actionNumber": 10,
                        "orderNumber": 10,
                        "period": 1,
                        "clock": "PT11M30.00S",
                        "actionType": "2pt",
                        "subType": "layup",
                        "shotResult": "Missed",
                        "personId": 123,
                        "teamId": 1610612740,
                    },
                ]
            }
        }

        with patch(
            "pbpstats.data_loader.stats_nba.pbp.web.get_pbp_actions",
            return_value=mock_cdn_data,
        ):
            result = loader.load_data("0021900001")

            rows = result["resultSets"][0]["rowSet"]
            assert len(rows) == 2

            # Verify order: shot should come before rebound
            headers = result["resultSets"][0]["headers"]
            first_row = dict(zip(headers, rows[0]))
            second_row = dict(zip(headers, rows[1]))

            assert first_row["EVENTMSGTYPE"] == 2  # Missed shot
            assert first_row["EVENTNUM"] == 10
            assert second_row["EVENTMSGTYPE"] == 4  # Rebound
            assert second_row["EVENTNUM"] == 5

    def test_no_cross_period_reordering(self):
        """Test that rebound reordering doesn't cross period boundaries"""
        loader = StatsNbaPbpWebLoader()

        mock_cdn_data = {
            "game": {
                "actions": [
                    {
                        "actionNumber": 100,
                        "period": 2,
                        "clock": "PT12M00.00S",
                        "actionType": "rebound",
                        "personId": 456,
                        "teamId": 1610612761,
                        "shotActionNumber": 50,  # References shot from period 1
                    },
                    {
                        "actionNumber": 50,
                        "period": 1,
                        "clock": "PT5M00.00S",
                        "actionType": "2pt",
                        "shotResult": "Missed",
                        "personId": 123,
                        "teamId": 1610612740,
                    },
                ]
            }
        }

        with patch(
            "pbpstats.data_loader.stats_nba.pbp.web.get_pbp_actions",
            return_value=mock_cdn_data,
        ):
            result = loader.load_data("0021900001")

            rows = result["resultSets"][0]["rowSet"]
            headers = result["resultSets"][0]["headers"]

            # Verify periods stay separate (rebound still in period 2, shot in period 1)
            first_row = dict(zip(headers, rows[0]))
            second_row = dict(zip(headers, rows[1]))

            # Shot should stay in its period, rebound in its period
            assert first_row["PERIOD"] == 1
            assert second_row["PERIOD"] == 2

    def test_multiple_dedupes_in_sequence(self):
        """Test handling multiple duplicates"""
        loader = StatsNbaPbpWebLoader()

        mock_cdn_data = {
            "game": {
                "actions": [
                    {
                        "actionNumber": 10,
                        "period": 1,
                        "clock": "PT10M00.00S",
                        "actionType": "2pt",
                        "shotResult": "Made",
                        "personId": 123,
                        "teamId": 1610612740,
                        "timeActual": "2023-10-24T01:05:00Z",
                    },
                    {
                        "actionNumber": 10,
                        "period": 1,
                        "clock": "PT10M00.00S",
                        "actionType": "2pt",
                        "shotResult": "Made",
                        "personId": 123,
                        "teamId": 1610612740,
                        "timeActual": "2023-10-24T01:05:00Z",
                    },
                    {
                        "actionNumber": 10,
                        "period": 1,
                        "clock": "PT10M00.00S",
                        "actionType": "2pt",
                        "shotResult": "Made",
                        "personId": 123,
                        "teamId": 1610612740,
                        "timeActual": "2023-10-24T01:05:00Z",
                        "edited": True,
                    },
                ]
            }
        }

        with patch(
            "pbpstats.data_loader.stats_nba.pbp.web.get_pbp_actions",
            return_value=mock_cdn_data,
        ):
            result = loader.load_data("0021900001")

            rows = result["resultSets"][0]["rowSet"]
            # Should only have 1 row after deduping
            assert len(rows) == 1

    def test_different_timeactual_not_deduped(self):
        """Test that events with same EVENTNUM but different timeActual are kept"""
        loader = StatsNbaPbpWebLoader()

        mock_cdn_data = {
            "game": {
                "actions": [
                    {
                        "actionNumber": 10,
                        "period": 1,
                        "clock": "PT10M00.00S",
                        "actionType": "2pt",
                        "shotResult": "Made",
                        "personId": 123,
                        "teamId": 1610612740,
                        "timeActual": "2023-10-24T01:05:00Z",
                    },
                    {
                        "actionNumber": 10,
                        "period": 1,
                        "clock": "PT10M00.00S",
                        "actionType": "2pt",
                        "shotResult": "Made",
                        "personId": 123,
                        "teamId": 1610612740,
                        "timeActual": "2023-10-24T01:05:01Z",  # Different time
                    },
                ]
            }
        }

        with patch(
            "pbpstats.data_loader.stats_nba.pbp.web.get_pbp_actions",
            return_value=mock_cdn_data,
        ):
            result = loader.load_data("0021900001")

            rows = result["resultSets"][0]["rowSet"]
            # Should have 2 rows (different timeActual)
            assert len(rows) == 2

    def test_rebound_without_shotActionNumber_not_reordered(self):
        """Test that rebounds without shotActionNumber maintain original order"""
        loader = StatsNbaPbpWebLoader()

        mock_cdn_data = {
            "game": {
                "actions": [
                    {
                        "actionNumber": 5,
                        "period": 1,
                        "clock": "PT11M00.00S",
                        "actionType": "rebound",
                        "personId": 456,
                        "teamId": 1610612761,
                        # No shotActionNumber
                    },
                    {
                        "actionNumber": 10,
                        "period": 1,
                        "clock": "PT11M30.00S",
                        "actionType": "2pt",
                        "shotResult": "Missed",
                        "personId": 123,
                        "teamId": 1610612740,
                    },
                ]
            }
        }

        with patch(
            "pbpstats.data_loader.stats_nba.pbp.web.get_pbp_actions",
            return_value=mock_cdn_data,
        ):
            result = loader.load_data("0021900001")

            rows = result["resultSets"][0]["rowSet"]
            headers = result["resultSets"][0]["headers"]

            # Verify original order is maintained (rebound before shot)
            first_row = dict(zip(headers, rows[0]))
            second_row = dict(zip(headers, rows[1]))

            assert first_row["EVENTNUM"] == 5  # Rebound
            assert second_row["EVENTNUM"] == 10  # Shot
