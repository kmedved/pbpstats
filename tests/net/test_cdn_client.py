# -*- coding: utf-8 -*-
import pytest
import requests

from pbpstats.net.cdn_client import CDN_PBP_URL, get_pbp_actions


class DummyResponse:
    def __init__(self, data, status_code=200):
        self._data = data
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code != 200:
            raise requests.HTTPError("boom")

    def json(self):
        return self._data


class DummySession:
    def __init__(self, response):
        self._response = response
        self.calls = []

    def get(self, url, headers=None, timeout=None):
        self.calls.append({"url": url, "headers": headers, "timeout": timeout})
        return self._response


def test_get_pbp_actions_fetches_and_validates_payload():
    data = {"meta": {}, "game": {"gameId": "0021234567", "actions": []}}
    session = DummySession(DummyResponse(data))

    result = get_pbp_actions("0021234567", session=session)

    assert result == data
    assert session.calls[0]["url"] == CDN_PBP_URL.format(game_id="0021234567")
    assert session.calls[0]["headers"]["User-Agent"] == "pbpstats/cdn-client"
    assert session.calls[0]["timeout"] == (5, 15)


def test_get_pbp_actions_raises_on_missing_actions():
    data = {"meta": {}, "game": {"gameId": "0021234567"}}
    session = DummySession(DummyResponse(data))

    with pytest.raises(ValueError, match="Malformed CDN PBP"):
        get_pbp_actions("0021234567", session=session)

