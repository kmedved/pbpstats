import json
import os
from typing import Any, Dict, List, Tuple

import requests

from pbpstats import G_LEAGUE_STRING, NBA_STRING
from pbpstats.data_loader.stats_nba.pbp.cdn_adapter import cdn_to_stats_row
from pbpstats.data_loader.stats_nba.web_loader import StatsNbaWebLoader
from pbpstats.net.cdn_client import get_pbp_actions

_STATS_PBP_HEADERS: List[str] = [
    "GAME_ID",
    "EVENTNUM",
    "EVENTMSGTYPE",
    "EVENTMSGACTIONTYPE",
    "PERIOD",
    "WCTIMESTRING",
    "PCTIMESTRING",
    "HOMEDESCRIPTION",
    "NEUTRALDESCRIPTION",
    "VISITORDESCRIPTION",
    "SCORE",
    "SCOREMARGIN",
    "PERSON1TYPE",
    "PLAYER1_ID",
    "PLAYER1_NAME",
    "PLAYER1_TEAM_ID",
    "PLAYER1_TEAM_CITY",
    "PLAYER1_TEAM_NICKNAME",
    "PLAYER1_TEAM_ABBREVIATION",
    "PERSON2TYPE",
    "PLAYER2_ID",
    "PLAYER2_NAME",
    "PLAYER2_TEAM_ID",
    "PLAYER2_TEAM_CITY",
    "PLAYER2_TEAM_NICKNAME",
    "PLAYER2_TEAM_ABBREVIATION",
    "PERSON3TYPE",
    "PLAYER3_ID",
    "PLAYER3_NAME",
    "PLAYER3_TEAM_ID",
    "PLAYER3_TEAM_CITY",
    "PLAYER3_TEAM_NICKNAME",
    "PLAYER3_TEAM_ABBREVIATION",
    "VIDEO_AVAILABLE_FLAG",
]

_EXTRA_HEADERS = [
    "x",
    "y",
    "xLegacy",
    "yLegacy",
    "shotDistance",
    "area",
    "areaDetail",
    "isTargetScoreLastPeriod",
    "timeActual",
    "descriptor",
    "qualifiers",
    "personIdsFilter",
    "possession",
    "periodType",
]

_HEADER_DEFAULTS: Dict[str, Any] = {
    "GAME_ID": None,
    "EVENTNUM": 0,
    "EVENTMSGTYPE": 0,
    "EVENTMSGACTIONTYPE": 0,
    "PERIOD": 0,
    "WCTIMESTRING": None,
    "PCTIMESTRING": "0:00",
    "HOMEDESCRIPTION": None,
    "NEUTRALDESCRIPTION": "",
    "VISITORDESCRIPTION": None,
    "SCORE": None,
    "SCOREMARGIN": None,
    "PERSON1TYPE": 0,
    "PLAYER1_ID": 0,
    "PLAYER1_NAME": None,
    "PLAYER1_TEAM_ID": 0,
    "PLAYER1_TEAM_CITY": None,
    "PLAYER1_TEAM_NICKNAME": None,
    "PLAYER1_TEAM_ABBREVIATION": None,
    "PERSON2TYPE": 0,
    "PLAYER2_ID": 0,
    "PLAYER2_NAME": None,
    "PLAYER2_TEAM_ID": 0,
    "PLAYER2_TEAM_CITY": None,
    "PLAYER2_TEAM_NICKNAME": None,
    "PLAYER2_TEAM_ABBREVIATION": None,
    "PERSON3TYPE": 0,
    "PLAYER3_ID": 0,
    "PLAYER3_NAME": None,
    "PLAYER3_TEAM_ID": 0,
    "PLAYER3_TEAM_CITY": None,
    "PLAYER3_TEAM_NICKNAME": None,
    "PLAYER3_TEAM_ABBREVIATION": None,
    "VIDEO_AVAILABLE_FLAG": 0,
}


class StatsNbaPbpWebLoader(StatsNbaWebLoader):
    """
    A ``StatsNbaPbpWebLoader`` object should be instantiated and passed into ``StatsNbaPbpLoader`` when loading data directly from the NBA Stats API

    :param str file_directory: (optional, use it if you want to store the response data on disk)
        Directory in which data should be either stored.
        The specific file location will be `stats_<game_id>.json` in the `/pbp` subdirectory.
        If not provided response data will not be saved on disk.
    """

    def __init__(self, file_directory=None):
        super().__init__()
        self.file_directory = file_directory
        self._session = requests.Session()

    def load_data(self, game_id):
        self.game_id = game_id
        if self._should_use_cdn():
            try:
                return self._load_from_cdn()
            except (
                requests.HTTPError,
                requests.RequestException,
                ValueError,
                KeyError,
            ):
                # Fallback to legacy stats endpoint if CDN is unavailable or malformed
                pass
        return self._load_from_legacy()

    def _load_from_legacy(self):
        league_url_part = (
            f"{G_LEAGUE_STRING}.{NBA_STRING}"
            if self.league == G_LEAGUE_STRING
            else self.league
        )
        self.base_url = f"https://stats.{league_url_part}.com/stats/playbyplayv2"
        self.parameters = {
            "GameId": self.game_id,
            "StartPeriod": 0,
            "EndPeriod": 10,
            "RangeType": 2,
            "StartRange": 0,
            "EndRange": 55800,
        }
        return self._load_request_data()

    def _load_from_cdn(self):
        data = get_pbp_actions(self.game_id, session=self._session)
        game = data.get("game") or {}
        actions = game.get("actions") or []
        actions.sort(key=self._action_sort_key)
        deduped = self._dedupe_actions(actions)
        rows = [cdn_to_stats_row(action, self.game_id) for action in deduped]
        self.source_data = self._build_stats_payload(rows)
        self._save_data_to_file()
        return self.source_data

    def _save_data_to_file(self):
        if self.file_directory is not None and os.path.isdir(self.file_directory):
            file_path = f"{self.file_directory}/pbp/stats_{self.game_id}.json"
            with open(file_path, "w") as outfile:
                json.dump(self.source_data, outfile)

    def _should_use_cdn(self) -> bool:
        if self.league != NBA_STRING:
            return False
        use_cdn_env = os.getenv("PBPSTATS_USE_CDN", "1")
        try:
            use_cdn = int(use_cdn_env) != 0
        except ValueError:
            use_cdn = use_cdn_env.strip().lower() in ("true", "yes", "on")
        return use_cdn

    @staticmethod
    def _action_sort_key(action: Dict[str, Any]) -> Tuple[int, int]:
        order = action.get("orderNumber")
        event = action.get("actionNumber")
        return (
            order if isinstance(order, int) else order or event or 0,
            event or 0,
        )

    @staticmethod
    def _dedupe_actions(actions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        deduped: List[Dict[str, Any]] = []
        index_by_key: Dict[Tuple[Any, Any, Any], int] = {}
        for action in actions:
            key = (
                action.get("actionNumber"),
                action.get("timeActual"),
                action.get("orderNumber"),
            )
            if key in index_by_key:
                idx = index_by_key[key]
                existing = deduped[idx]
                if action.get("edited") and not existing.get("edited"):
                    deduped[idx] = action
                continue
            index_by_key[key] = len(deduped)
            deduped.append(action)
        return deduped

    def _build_stats_payload(self, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        headers = _STATS_PBP_HEADERS + _EXTRA_HEADERS
        row_set = self._rows_to_row_set(rows, headers)
        return {
            "resource": "playbyplay",
            "resultSets": [
                {"name": "PlayByPlay", "headers": headers, "rowSet": row_set}
            ],
        }

    def _rows_to_row_set(
        self, rows: List[Dict[str, Any]], headers: List[str]
    ) -> List[List[Any]]:
        row_set: List[List[Any]] = []
        for row in rows:
            row_values = []
            for header in headers:
                if header in row:
                    row_values.append(row[header])
                else:
                    row_values.append(_HEADER_DEFAULTS.get(header))
            row_set.append(row_values)
        return row_set
