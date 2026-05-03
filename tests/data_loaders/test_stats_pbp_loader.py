import json
from pathlib import Path

import pytest
import responses
from furl import furl

from pbpstats.data_loader.stats_nba.pbp.file import StatsNbaPbpFileLoader
from pbpstats.data_loader.stats_nba.pbp.loader import StatsNbaPbpLoader
from pbpstats.data_loader.stats_nba.pbp.web import StatsNbaPbpWebLoader
from pbpstats.data_loader.stats_nba.pbp.v3_synthetic import (
    V2_HEADERS,
    build_synthetic_v2_pbp_response,
)
from pbpstats.resources.pbp.stats_nba_pbp_item import StatsNbaPbpItem


SYNTHETIC_GAME_ID = "0022400001"
SYNTHETIC_HOME_ID = 1610612764


def _minimal_v3_source_data():
    return {
        "game": {
            "actions": [
                {
                    "gameId": SYNTHETIC_GAME_ID,
                    "actionNumber": 1,
                    "actionId": 1,
                    "actionType": "made shot",
                    "subType": "jump shot",
                    "teamId": SYNTHETIC_HOME_ID,
                    "teamTricode": "WAS",
                    "location": "h",
                    "personId": 204456,
                    "playerName": "Tyus Jones",
                    "description": "Jones Jump Shot (2 PTS)",
                    "period": 1,
                    "clock": "PT11M42.00S",
                    "scoreHome": 2,
                    "scoreAway": 0,
                    "videoAvailable": 0,
                }
            ]
        }
    }


def _synthetic_response(event_num=1):
    response = build_synthetic_v2_pbp_response(
        SYNTHETIC_GAME_ID, _minimal_v3_source_data()
    )
    event_num_index = V2_HEADERS.index("EVENTNUM")
    response["resultSets"][0]["rowSet"][0][event_num_index] = event_num
    return response


def _write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data))


def _pbp_v2_url(game_id):
    base_url = "https://stats.nba.com/stats/playbyplayv2"
    query_params = {
        "GameId": game_id,
        "StartPeriod": 0,
        "EndPeriod": 10,
        "RangeType": 2,
        "StartRange": 0,
        "EndRange": 55800,
    }
    return furl(base_url).add(query_params).url


def _pbp_v3_url(game_id):
    base_url = "https://stats.nba.com/stats/playbyplayv3"
    query_params = {"GameID": game_id, "StartPeriod": 0, "EndPeriod": 10}
    return furl(base_url).add(query_params).url


class TestStatsPbpLoader:
    game_id = "0021600270"
    data_directory = "tests/data"
    expected_first_item_data = {
        "game_id": "0021600270",
        "eventnum": 0,
        "eventmsgtype": 12,
        "eventmsgactiontype": 0,
        "period": 1,
        "wctimestring": "8:11 PM",
        "pctimestring": "12:00",
        "homedescription": None,
        "neutraldescription": None,
        "visitordescription": None,
        "score": None,
        "scoremargin": None,
        "person1type": 0,
        "player1_id": 0,
        "player1_name": None,
        "player1_team_id": None,
        "player1_team_city": None,
        "player1_team_nickname": None,
        "player1_team_abbreviation": None,
        "person2type": 0,
        "player2_id": 0,
        "player2_name": None,
        "player2_team_id": None,
        "player2_team_city": None,
        "player2_team_nickname": None,
        "player2_team_abbreviation": None,
        "person3type": 0,
        "player3_id": 0,
        "player3_name": None,
        "player3_team_id": None,
        "player3_team_city": None,
        "player3_team_nickname": None,
        "player3_team_abbreviation": None,
        "video_available_flag": 0,
        "order": 0,
    }

    def test_file_loader_loads_data(self):
        source_loader = StatsNbaPbpFileLoader(self.data_directory)
        pbp_loader = StatsNbaPbpLoader(self.game_id, source_loader)
        assert len(pbp_loader.items) == 540
        assert isinstance(pbp_loader.items[0], StatsNbaPbpItem)
        assert pbp_loader.items[0].data == self.expected_first_item_data

    @responses.activate
    def test_web_loader_loads_data(self):
        with open(f"{self.data_directory}/pbp/stats_{self.game_id}.json") as f:
            pbp_response = json.loads(f.read())
        pbp_url = _pbp_v2_url(self.game_id)
        responses.add(responses.GET, pbp_url, json=pbp_response, status=200)

        source_loader = StatsNbaPbpWebLoader(self.data_directory)
        pbp_loader = StatsNbaPbpLoader(self.game_id, source_loader)
        assert len(pbp_loader.items) == 540
        assert isinstance(pbp_loader.items[0], StatsNbaPbpItem)
        assert pbp_loader.items[0].data == self.expected_first_item_data

    @responses.activate
    def test_web_loader_v3_synthetic_uses_v3_and_separate_cache(self, tmp_path):
        responses.add(
            responses.GET,
            _pbp_v3_url(SYNTHETIC_GAME_ID),
            json=_minimal_v3_source_data(),
            status=200,
        )

        source_loader = StatsNbaPbpWebLoader(
            str(tmp_path), endpoint_strategy="v3_synthetic"
        )
        pbp_loader = StatsNbaPbpLoader(SYNTHETIC_GAME_ID, source_loader)

        assert len(pbp_loader.items) == 1
        assert pbp_loader.items[0].eventmsgtype == 1
        assert not Path(tmp_path, "pbp", f"stats_{SYNTHETIC_GAME_ID}.json").exists()
        assert Path(
            tmp_path, "pbp_v3", f"stats_pbpv3_{SYNTHETIC_GAME_ID}.json"
        ).exists()
        assert Path(
            tmp_path, "pbp_synthetic_v3", f"stats_{SYNTHETIC_GAME_ID}.json"
        ).exists()

    @responses.activate
    def test_web_loader_auto_falls_back_only_for_malformed_v2(self, tmp_path):
        responses.add(
            responses.GET,
            _pbp_v2_url(SYNTHETIC_GAME_ID),
            json={"resultSets": [{"headers": V2_HEADERS, "rowSet": []}]},
            status=200,
        )
        responses.add(
            responses.GET,
            _pbp_v3_url(SYNTHETIC_GAME_ID),
            json=_minimal_v3_source_data(),
            status=200,
        )

        source_loader = StatsNbaPbpWebLoader(str(tmp_path), endpoint_strategy="auto")
        pbp_loader = StatsNbaPbpLoader(SYNTHETIC_GAME_ID, source_loader)

        assert len(pbp_loader.items) == 1
        assert not Path(tmp_path, "pbp", f"stats_{SYNTHETIC_GAME_ID}.json").exists()
        assert Path(
            tmp_path, "pbp_synthetic_v3", f"stats_{SYNTHETIC_GAME_ID}.json"
        ).exists()

    def test_file_loader_v3_synthetic_reads_synthetic_cache(self, tmp_path):
        _write_json(
            Path(tmp_path, "pbp_synthetic_v3", f"stats_{SYNTHETIC_GAME_ID}.json"),
            _synthetic_response(),
        )

        source_loader = StatsNbaPbpFileLoader(
            str(tmp_path), endpoint_strategy="v3_synthetic"
        )
        pbp_loader = StatsNbaPbpLoader(SYNTHETIC_GAME_ID, source_loader)

        assert len(pbp_loader.items) == 1
        assert pbp_loader.items[0].eventnum == 1

    def test_file_loader_auto_prefers_true_v2_cache(self, tmp_path):
        _write_json(
            Path(tmp_path, "pbp", f"stats_{SYNTHETIC_GAME_ID}.json"),
            _synthetic_response(event_num=77),
        )
        _write_json(
            Path(tmp_path, "pbp_synthetic_v3", f"stats_{SYNTHETIC_GAME_ID}.json"),
            _synthetic_response(event_num=1),
        )

        source_loader = StatsNbaPbpFileLoader(str(tmp_path), endpoint_strategy="auto")
        pbp_loader = StatsNbaPbpLoader(SYNTHETIC_GAME_ID, source_loader)

        assert pbp_loader.items[0].eventnum == 77

    def test_file_loader_auto_falls_back_to_synthetic_cache(self, tmp_path):
        _write_json(
            Path(tmp_path, "pbp", f"stats_{SYNTHETIC_GAME_ID}.json"),
            {"resultSets": [{"headers": V2_HEADERS, "rowSet": []}]},
        )
        _write_json(
            Path(tmp_path, "pbp_synthetic_v3", f"stats_{SYNTHETIC_GAME_ID}.json"),
            _synthetic_response(event_num=1),
        )

        source_loader = StatsNbaPbpFileLoader(str(tmp_path), endpoint_strategy="auto")
        pbp_loader = StatsNbaPbpLoader(SYNTHETIC_GAME_ID, source_loader)

        assert pbp_loader.items[0].eventnum == 1

    def test_file_loader_v3_synthetic_missing_cache_raises(self, tmp_path):
        source_loader = StatsNbaPbpFileLoader(
            str(tmp_path), endpoint_strategy="v3_synthetic"
        )

        with pytest.raises(Exception, match="does not exist"):
            StatsNbaPbpLoader(SYNTHETIC_GAME_ID, source_loader)
