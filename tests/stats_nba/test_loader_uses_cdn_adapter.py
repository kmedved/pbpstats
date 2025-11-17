# -*- coding: utf-8 -*-
import requests

from pbpstats.data_loader.stats_nba.pbp.loader import StatsNbaPbpLoader
from pbpstats.data_loader.stats_nba.pbp.web import StatsNbaPbpWebLoader
from pbpstats.resources.enhanced_pbp.stats_nba.enhanced_pbp_factory import (
    StatsNbaEnhancedPbpFactory,
)


def test_stats_pbp_web_loader_uses_cdn_adapter(monkeypatch):
    calls = []

    actions = [
        {
            "actionNumber": 1,
            "orderNumber": 1,
            "period": 1,
            "clock": "PT12M00S",
            "actionType": "Period",
            "subType": "start",
            "timeActual": "2024-01-01T00:00:00Z",
        },
        {
            "actionNumber": 2,
            "orderNumber": 2,
            "period": 1,
            "clock": "PT11M48S",
            "actionType": "2pt",
            "subType": "Layup",
            "shotResult": "Made",
            "description": "Sample layup",
            "teamId": 1610612737,
            "personId": 2001,
            "assistPersonId": 2002,
            "scoreHome": 2,
            "scoreAway": 0,
            "timeActual": "2024-01-01T00:00:10Z",
        },
        {
            "actionNumber": 3,
            "orderNumber": 3,
            "period": 1,
            "clock": "PT11M40S",
            "actionType": "Turnover",
            "subType": "lostball",
            "personId": 2003,
            "stealPersonId": 2004,
            "timeActual": "2024-01-01T00:00:15Z",
            "scoreHome": 2,
            "scoreAway": 0,
        },
    ]

    def fake_get(game_id, session=None):
        calls.append(game_id)
        return {"meta": {}, "game": {"gameId": game_id, "actions": actions}}

    monkeypatch.setattr(
        "pbpstats.data_loader.stats_nba.pbp.web.get_pbp_actions", fake_get
    )

    source_loader = StatsNbaPbpWebLoader()
    pbp_loader = StatsNbaPbpLoader("0021234567", source_loader)

    assert calls == ["0021234567"]
    assert len(pbp_loader.items) == 3
    assert pbp_loader.items[1].eventmsgtype == 1
    assert pbp_loader.items[1].player2_id == 2002
    assert pbp_loader.items[2].player2_id == 2004

    headers = source_loader.source_data["resultSets"][0]["headers"]
    score_index = headers.index("SCORE")
    wc_index = headers.index("WCTIMESTRING")
    row_set = source_loader.source_data["resultSets"][0]["rowSet"]
    assert row_set[1][score_index] == "2-0"
    assert row_set[0][wc_index] == "2024-01-01T00:00:00Z"


def test_cdn_loader_falls_back_to_legacy(monkeypatch):
    calls = []
    response = requests.Response()
    response.status_code = 503

    def fake_get(game_id, session=None):
        calls.append(game_id)
        raise requests.HTTPError(response=response)

    monkeypatch.setattr(
        "pbpstats.data_loader.stats_nba.pbp.web.get_pbp_actions", fake_get
    )

    sentinel = {
        "resultSets": [
            {"headers": ["GAME_ID"], "rowSet": [["0021234567"]]},
        ]
    }

    def fake_load_request_data(self):
        self.source_data = sentinel
        return sentinel

    monkeypatch.setattr(
        StatsNbaPbpWebLoader,
        "_load_request_data",
        fake_load_request_data,
    )

    loader = StatsNbaPbpWebLoader()
    data = loader.load_data("0021234567")

    assert calls == ["0021234567"]
    assert data is sentinel


def test_cdn_loader_falls_back_on_request_exception(monkeypatch):
    calls = []

    def fake_get(game_id, session=None):
        calls.append(game_id)
        raise requests.ConnectionError("network error")

    monkeypatch.setattr(
        "pbpstats.data_loader.stats_nba.pbp.web.get_pbp_actions", fake_get
    )

    sentinel = {
        "resultSets": [
            {"headers": ["GAME_ID"], "rowSet": [["0027654321"]]},
        ]
    }

    def fake_load_request_data(self):
        self.source_data = sentinel
        return sentinel

    monkeypatch.setattr(
        StatsNbaPbpWebLoader,
        "_load_request_data",
        fake_load_request_data,
    )

    loader = StatsNbaPbpWebLoader()
    data = loader.load_data("0027654321")

    assert calls == ["0027654321"]
    assert data is sentinel


def test_dedupe_prefers_latest_edited_action():
    actions = [
        {
            "actionNumber": 1,
            "orderNumber": 1,
            "timeActual": "2024-01-01T00:00:00Z",
            "edited": False,
            "description": "original",
        },
        {
            "actionNumber": 1,
            "orderNumber": 1,
            "timeActual": "2024-01-01T00:00:00Z",
            "edited": True,
            "description": "edited",
        },
    ]

    deduped = StatsNbaPbpWebLoader._dedupe_actions(actions)
    assert len(deduped) == 1
    assert deduped[0]["edited"]
    assert deduped[0]["description"] == "edited"


def test_dedupe_keeps_events_with_distinct_order_numbers():
    actions = [
        {"actionNumber": 2, "orderNumber": 1, "timeActual": None},
        {"actionNumber": 2, "orderNumber": 2, "timeActual": None},
    ]

    deduped = StatsNbaPbpWebLoader._dedupe_actions(actions)
    assert len(deduped) == 2


def test_build_stats_payload_rows_align_with_headers():
    loader = StatsNbaPbpWebLoader()
    rows = [
        {
            "GAME_ID": "0020000001",
            "EVENTNUM": 1,
            "EVENTMSGTYPE": 12,
            "EVENTMSGACTIONTYPE": 0,
            "PERIOD": 1,
            "WCTIMESTRING": "2024-01-01T00:00:00Z",
            "PCTIMESTRING": "12:00",
            "NEUTRALDESCRIPTION": "",
        }
    ]

    payload = loader._build_stats_payload(rows)

    headers = payload["resultSets"][0]["headers"]
    row_set = payload["resultSets"][0]["rowSet"]
    assert all(len(row) == len(headers) for row in row_set)
    wc_index = headers.index("WCTIMESTRING")
    pct_index = headers.index("PCTIMESTRING")
    assert row_set[0][wc_index] == "2024-01-01T00:00:00Z"
    assert row_set[0][pct_index] == "12:00"


def test_coalescer_preserves_already_paired_rows():
    loader = StatsNbaPbpWebLoader()
    paired = {
        "actionType": "Substitution",
        "orderNumber": 10,
        "actionNumber": 20,
        "period": 1,
        "clock": "PT10M00S",
        "teamId": 1610612737,
        "subOutPersonId": 100,
        "subInPersonId": 200,
    }

    coalesced = loader._coalesce_substitution_pairs([paired])

    assert len(coalesced) == 1
    assert coalesced[0]["subOutPersonId"] == 100
    assert coalesced[0]["subInPersonId"] == 200


def test_coalescer_keeps_unmatched_half_subs():
    loader = StatsNbaPbpWebLoader()
    half = {
        "actionType": "Substitution",
        "subType": "out",
        "orderNumber": 11,
        "actionNumber": 30,
        "period": 1,
        "clock": "PT09M30S",
        "teamId": 1610612737,
        "personId": 101,
    }

    coalesced = loader._coalesce_substitution_pairs([half])

    assert len(coalesced) == 1
    assert coalesced[0]["subType"] == "out"
    assert coalesced[0]["personId"] == 101


def test_coalescer_pairs_when_timeactual_is_missing():
    loader = StatsNbaPbpWebLoader()
    out_event = {
        "actionType": "Substitution",
        "subType": "out",
        "orderNumber": 12,
        "actionNumber": 40,
        "period": 1,
        "clock": "PT08M00S",
        "teamId": 1610612737,
        "personId": 201,
        "subOutPersonId": 201,
        "timeActual": None,
    }
    in_event = {
        "actionType": "Substitution",
        "subType": "in",
        "orderNumber": 13,
        "actionNumber": 41,
        "period": 1,
        "clock": "PT08M00S",
        "teamId": 1610612737,
        "personId": 202,
        "subInPersonId": 202,
        "timeActual": "2024-01-01T00:03:00Z",
    }

    coalesced = loader._coalesce_substitution_pairs([out_event, in_event])

    assert len(coalesced) == 1
    merged = coalesced[0]
    assert merged["subOutPersonId"] == 201
    assert merged["subInPersonId"] == 202
    assert merged["orderNumber"] == 12
    assert merged["actionNumber"] == 40


def test_loader_filters_supplemental_actions(monkeypatch):
    loader = StatsNbaPbpWebLoader()
    loader.game_id = "0020000001"

    actions = [
        {
            "actionNumber": 1,
            "orderNumber": 1,
            "period": 1,
            "clock": "PT12M00S",
            "actionType": "Period",
            "subType": "start",
            "timeActual": "2024-01-01T00:00:00Z",
        },
        {
            "actionNumber": 2,
            "orderNumber": 2,
            "period": 1,
            "clock": "PT11M59S",
            "actionType": "Steal",
            "teamId": 1610612737,
        },
        {
            "actionNumber": 3,
            "orderNumber": 3,
            "period": 1,
            "clock": "PT11M58S",
            "actionType": "Block",
            "teamId": 1610612737,
        },
        {
            "actionNumber": 4,
            "orderNumber": 4,
            "period": 1,
            "clock": "PT11M57S",
            "actionType": "Stoppage",
            "description": "Delay of game",
            "teamId": 1610612737,
        },
        {
            "actionNumber": 5,
            "orderNumber": 5,
            "period": 1,
            "clock": "PT11M50S",
            "actionType": "2pt",
            "subType": "Layup",
            "shotResult": "Made",
            "teamId": 1610612737,
            "personId": 123,
            "scoreHome": 2,
            "scoreAway": 0,
        },
    ]

    def fake_get(game_id, session=None):
        return {"meta": {}, "game": {"actions": actions}}

    monkeypatch.setattr(
        "pbpstats.data_loader.stats_nba.pbp.web.get_pbp_actions", fake_get
    )

    payload = loader._load_from_cdn()
    headers = payload["resultSets"][0]["headers"]
    rows = payload["resultSets"][0]["rowSet"]
    evt_index = headers.index("EVENTMSGTYPE")

    assert len(rows) == 3  # period start, stoppage, field goal make
    assert all(row[evt_index] != 0 for row in rows)
    assert any(row[evt_index] == 20 for row in rows)


def test_factory_creates_stoppage_events():
    factory = StatsNbaEnhancedPbpFactory()
    event_cls = factory.get_event_class(20)
    event = {
        "EVENTMSGTYPE": 20,
        "EVENTNUM": 99,
        "PERIOD": 1,
        "PCTIMESTRING": "10:00",
        "NEUTRALDESCRIPTION": "Stoppage",
    }

    instance = event_cls(event, 1)

    assert instance.event_type == 20
    assert instance.event_stats == []
