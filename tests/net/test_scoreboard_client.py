# -*- coding: utf-8 -*-
import requests

from pbpstats.net.scoreboard_client import get_games_for_date


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
    def __init__(self, resp):
        self._resp = resp
        self.calls = []

    def get(self, url, headers=None, timeout=None):
        self.calls.append((url, headers, timeout))
        return self._resp


def test_get_games_for_date_returns_list():
    data = {"scoreboard": {"games": [{"gameId": "0029999999"}]}}
    sess = DummySession(DummyResponse(data))
    out = get_games_for_date("20240101", session=sess)
    assert out and out[0]["gameId"] == "0029999999"
    url, headers, timeout = sess.calls[0]
    assert "scoreboard_20240101" in url
    assert headers["User-Agent"] == "pbpstats/scoreboard-client"
    assert timeout == (5, 15)
