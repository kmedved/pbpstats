# -*- coding: utf-8 -*-
from pbpstats.data_loader.stats_nba.pbp.loader import StatsNbaPbpLoader
from pbpstats.data_loader.stats_nba.pbp.web import StatsNbaPbpWebLoader


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

    def fake_get(game_id):
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
    row_set = source_loader.source_data["resultSets"][0]["rowSet"]
    assert row_set[1][score_index] == "2-0"
