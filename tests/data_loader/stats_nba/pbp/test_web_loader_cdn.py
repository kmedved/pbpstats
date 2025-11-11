# -*- coding: utf-8 -*-
"""Tests for StatsNbaPbpWebLoader with CDN integration"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import requests

from pbpstats.data_loader.stats_nba.pbp.web import StatsNbaPbpWebLoader


class TestStatsNbaPbpWebLoaderCdn:
    """Test CDN integration in StatsNbaPbpWebLoader"""

    def test_load_data_uses_cdn_for_nba_games(self):
        """Test that NBA games use CDN"""
        loader = StatsNbaPbpWebLoader()

        # Mock CDN response
        mock_cdn_data = {
            "meta": {"version": 1},
            "game": {
                "gameId": "0021900001",
                "actions": [
                    {
                        "actionNumber": 2,
                        "period": 1,
                        "clock": "PT12M00.00S",
                        "actionType": "period",
                        "subType": "start",
                    },
                    {
                        "actionNumber": 4,
                        "period": 1,
                        "clock": "PT12M00.00S",
                        "actionType": "jumpball",
                        "personId": 201188,
                        "teamId": 1610612761,
                        "teamTricode": "TOR",
                        "jumpBallWonPersonId": 202324,
                        "jumpBallRecoverdPersonId": 1628366,
                        "description": "Jump Ball",
                    },
                    {
                        "actionNumber": 7,
                        "period": 1,
                        "clock": "PT11M48.00S",
                        "actionType": "2pt",
                        "subType": "layup",
                        "shotResult": "Missed",
                        "personId": 1628366,
                        "teamId": 1610612740,
                        "description": "MISS Ball Layup",
                    },
                ],
            },
        }

        with patch(
            "pbpstats.data_loader.stats_nba.pbp.web.get_pbp_actions",
            return_value=mock_cdn_data,
        ) as mock_get:
            result = loader.load_data("0021900001")

            # Verify CDN was called
            mock_get.assert_called_once_with("0021900001")

            # Verify result structure
            assert "resultSets" in result
            assert len(result["resultSets"]) == 1
            assert result["resultSets"][0]["name"] == "PlayByPlay"
            assert "headers" in result["resultSets"][0]
            assert "rowSet" in result["resultSets"][0]

            # Verify rows were created
            rows = result["resultSets"][0]["rowSet"]
            assert len(rows) == 3

            # Verify first row (period start)
            assert rows[0][0] == "0021900001"  # GAME_ID
            assert rows[0][1] == 2  # EVENTNUM
            assert rows[0][2] == 12  # EVENTMSGTYPE (period start)

    def test_load_data_converts_actions_to_v2_format(self):
        """Test that CDN actions are properly converted to v2 format"""
        loader = StatsNbaPbpWebLoader()

        mock_cdn_data = {
            "game": {
                "actions": [
                    {
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
                        "description": "Anunoby 3PT",
                    }
                ]
            }
        }

        with patch(
            "pbpstats.data_loader.stats_nba.pbp.web.get_pbp_actions",
            return_value=mock_cdn_data,
        ):
            result = loader.load_data("0021900001")

            headers = result["resultSets"][0]["headers"]
            row = result["resultSets"][0]["rowSet"][0]

            # Create a dict from headers and row for easier checking
            row_dict = dict(zip(headers, row))

            assert row_dict["GAME_ID"] == "0021900001"
            assert row_dict["EVENTNUM"] == 25
            assert row_dict["PERIOD"] == 1
            assert row_dict["PCTIMESTRING"] == "10:17"
            assert row_dict["EVENTMSGTYPE"] == 1  # Made shot
            assert row_dict["EVENTMSGACTIONTYPE"] == 1  # Jump shot
            assert row_dict["PLAYER1_ID"] == 1628384
            assert row_dict["PLAYER1_TEAM_ID"] == 1610612761
            assert row_dict["PLAYER2_ID"] == 1627832  # Assist
            assert row_dict["SCORE"] == "4-4"
            assert row_dict["SCOREMARGIN"] == "TIE"

    def test_load_data_dedupes_events(self):
        """Test that duplicate events are removed"""
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
                        "edited": True,  # This one should be kept
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

    def test_load_data_sorts_by_order_number(self):
        """Test that actions are sorted by orderNumber"""
        loader = StatsNbaPbpWebLoader()

        mock_cdn_data = {
            "game": {
                "actions": [
                    {
                        "actionNumber": 30,
                        "orderNumber": 30,
                        "period": 1,
                        "clock": "PT9M00.00S",
                        "actionType": "timeout",
                        "teamId": 1610612761,
                    },
                    {
                        "actionNumber": 10,
                        "orderNumber": 10,
                        "period": 1,
                        "clock": "PT11M00.00S",
                        "actionType": "2pt",
                        "shotResult": "Made",
                        "personId": 123,
                        "teamId": 1610612740,
                    },
                    {
                        "actionNumber": 20,
                        "orderNumber": 20,
                        "period": 1,
                        "clock": "PT10M00.00S",
                        "actionType": "rebound",
                        "personId": 456,
                        "teamId": 1610612761,
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
            # Verify sorted order by EVENTNUM
            assert rows[0][1] == 10  # EVENTNUM of first row
            assert rows[1][1] == 20  # EVENTNUM of second row
            assert rows[2][1] == 30  # EVENTNUM of third row

    def test_load_data_uses_legacy_for_gleague(self):
        """Test that G-League games use legacy API"""
        loader = StatsNbaPbpWebLoader()

        # Mock the parent class method
        with patch.object(
            loader, "_load_request_data", return_value={"resultSets": []}
        ) as mock_legacy:
            with patch(
                "pbpstats.data_loader.stats_nba.pbp.web.get_pbp_actions"
            ) as mock_cdn:
                result = loader.load_data("1021900001")  # G-League game ID

                # Verify legacy API was called, not CDN
                mock_legacy.assert_called_once()
                mock_cdn.assert_not_called()

                # Verify legacy URL was set
                assert "playbyplayv2" in loader.base_url

    def test_load_data_uses_legacy_for_wnba(self):
        """Test that WNBA games use legacy API"""
        loader = StatsNbaPbpWebLoader()

        with patch.object(
            loader, "_load_request_data", return_value={"resultSets": []}
        ) as mock_legacy:
            with patch(
                "pbpstats.data_loader.stats_nba.pbp.web.get_pbp_actions"
            ) as mock_cdn:
                result = loader.load_data("1021900001")  # WNBA game ID

                # Verify legacy API was called, not CDN
                mock_legacy.assert_called_once()
                mock_cdn.assert_not_called()

    def test_load_data_raises_on_cdn_error(self):
        """Test that CDN errors are raised"""
        loader = StatsNbaPbpWebLoader()

        with patch(
            "pbpstats.data_loader.stats_nba.pbp.web.get_pbp_actions",
            side_effect=requests.HTTPError("404 Not Found"),
        ):
            with pytest.raises(requests.HTTPError):
                loader.load_data("0021900001")

    def test_headers_match_v2_format(self):
        """Test that headers match v2 playbyplayv2 format"""
        loader = StatsNbaPbpWebLoader()

        mock_cdn_data = {
            "game": {
                "actions": [
                    {
                        "actionNumber": 1,
                        "period": 1,
                        "clock": "PT12M00.00S",
                        "actionType": "period",
                        "subType": "start",
                    }
                ]
            }
        }

        with patch(
            "pbpstats.data_loader.stats_nba.pbp.web.get_pbp_actions",
            return_value=mock_cdn_data,
        ):
            result = loader.load_data("0021900001")

            headers = result["resultSets"][0]["headers"]

            # Verify key v2 headers are present
            expected_headers = [
                "GAME_ID",
                "EVENTNUM",
                "EVENTMSGTYPE",
                "EVENTMSGACTIONTYPE",
                "PERIOD",
                "PCTIMESTRING",
                "SCORE",
                "SCOREMARGIN",
                "PLAYER1_ID",
                "PLAYER1_TEAM_ID",
                "PLAYER2_ID",
                "PLAYER3_ID",
            ]

            for header in expected_headers:
                assert header in headers, f"Missing header: {header}"
