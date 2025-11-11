# -*- coding: utf-8 -*-
"""Tests for CDN client"""
import pytest
from unittest.mock import Mock, patch
import requests

from pbpstats.net.cdn_client import get_pbp_actions, CDN_PBP_URL


class TestGetPbpActions:
    """Test cases for get_pbp_actions function"""

    def test_successful_fetch(self):
        """Test successful fetch returns parsed JSON with actions"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "meta": {"version": 1},
            "game": {
                "gameId": "0021900001",
                "actions": [
                    {"actionNumber": 1, "actionType": "period"},
                    {"actionNumber": 2, "actionType": "jumpball"},
                ],
            },
        }

        with patch("requests.Session.get", return_value=mock_response) as mock_get:
            result = get_pbp_actions("0021900001")

            assert "game" in result
            assert "actions" in result["game"]
            assert len(result["game"]["actions"]) == 2
            assert result["game"]["actions"][0]["actionNumber"] == 1

            # Verify correct URL was called
            mock_get.assert_called_once()
            call_args = mock_get.call_args
            assert "0021900001" in call_args[0][0]

    def test_http_error_raises(self):
        """Test that HTTP errors are raised"""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.HTTPError("Not Found")

        with patch("requests.Session.get", return_value=mock_response):
            with pytest.raises(requests.HTTPError):
                get_pbp_actions("0021900001")

    def test_malformed_json_missing_game(self):
        """Test that ValueError is raised when game key is missing"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"meta": {"version": 1}}

        with patch("requests.Session.get", return_value=mock_response):
            with pytest.raises(ValueError, match="Malformed CDN PBP"):
                get_pbp_actions("0021900001")

    def test_malformed_json_missing_actions(self):
        """Test that ValueError is raised when actions is missing"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "meta": {"version": 1},
            "game": {"gameId": "0021900001"},
        }

        with patch("requests.Session.get", return_value=mock_response):
            with pytest.raises(ValueError, match="Malformed CDN PBP"):
                get_pbp_actions("0021900001")

    def test_malformed_json_actions_not_list(self):
        """Test that ValueError is raised when actions is not a list"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "meta": {"version": 1},
            "game": {"gameId": "0021900001", "actions": "not a list"},
        }

        with patch("requests.Session.get", return_value=mock_response):
            with pytest.raises(ValueError, match="Malformed CDN PBP"):
                get_pbp_actions("0021900001")

    def test_custom_session(self):
        """Test that custom session is used when provided"""
        mock_session = Mock(spec=requests.Session)
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "game": {"gameId": "0021900001", "actions": []}
        }
        mock_session.get.return_value = mock_response

        result = get_pbp_actions("0021900001", session=mock_session)

        assert "game" in result
        mock_session.get.assert_called_once()

    def test_correct_headers(self):
        """Test that correct headers are sent"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"game": {"gameId": "0021900001", "actions": []}}

        with patch("requests.Session.get", return_value=mock_response) as mock_get:
            get_pbp_actions("0021900001")

            call_args = mock_get.call_args
            headers = call_args[1]["headers"]
            assert "User-Agent" in headers
            assert "pbpstats" in headers["User-Agent"]
            assert "Accept-Encoding" in headers
