import json
import os
from collections import Counter, deque
from typing import Any, Deque, Dict, List, Tuple

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

SUPPORTED_EVENT_TYPES = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 20}


class StatsNbaPbpWebLoader(StatsNbaWebLoader):
    """
    A ``StatsNbaPbpWebLoader`` object should be instantiated and passed into ``StatsNbaPbpLoader`` when loading data directly from the NBA Stats API

    :param str file_directory: (optional, use it if you want to store the response data on disk)
        Directory in which data should be either stored.
        The specific file location will be `stats_<game_id>.json` in the `/pbp` subdirectory.
        If not provided response data will not be saved on disk.
    """

    def __init__(self, file_directory=None):
        # Ensure base class sees the file_directory
        super().__init__(file_directory)
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
        # Drop CDN-only meta/attribute actions before converting to stats rows.
        actions = [action for action in actions if self._include_cdn_action(action)]
        actions = self._coalesce_substitution_pairs(actions)
        actions.sort(key=self._action_sort_key)
        deduped = self._dedupe_actions(actions)
        rows = [cdn_to_stats_row(action, self.game_id) for action in deduped]
        if os.getenv("PBPSTATS_DEBUG_EVENT_TYPES"):
            counts = Counter(row.get("EVENTMSGTYPE") for row in rows)
            print("EVENTMSGTYPE distribution:", dict(counts))
        rows = [
            row for row in rows if row.get("EVENTMSGTYPE") in SUPPORTED_EVENT_TYPES
        ]
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
    def _include_cdn_action(action: Dict[str, Any]) -> bool:
        """
        Drop supplemental CDN-only metadata rows that don't translate to Stats v2
        events:
          - 'steal' and 'block' already exist as attributes on turnovers/field goals
          - 'game' and 'edit' are administrative markers
          - 'stoppage_meta' is an attributes-only duplicate when a true stoppage exists
        """
        t = (action.get("actionType") or "").lower()
        return t not in {
            "steal",
            "block",
            "game",
            "edit",
            "stoppage_meta",
        }

    @staticmethod
    def _sub_key(action: Dict[str, Any]) -> Tuple[Any, Any, Any]:
        """
        Group potential substitution pairs by period/clock/team.
        Some feeds omit ``timeActual`` on one half of the pair, so we intentionally
        ignore it here to improve hit rates.
        """
        return (
            action.get("period"),
            action.get("clock"),
            action.get("teamId"),
        )

    def _coalesce_substitution_pairs(
        self, actions: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Merge consecutive substitution rows (subType 'out'/'in') that share the same
        (period, clock, teamId) into paired substitutions that contain both
        outgoing/incoming players. Already paired rows are preserved verbatim and
        unmatched halves are passed through so downstream lineup logic can still see
        those anchors.
        """
        result: List[Dict[str, Any]] = []
        i = 0
        n = len(actions)
        while i < n:
            action = actions[i]
            if (action.get("actionType") or "").lower() != "substitution":
                result.append(action)
                i += 1
                continue

            key = self._sub_key(action)
            cluster: List[Dict[str, Any]] = []
            j = i
            while j < n:
                candidate = actions[j]
                if (candidate.get("actionType") or "").lower() != "substitution":
                    break
                if self._sub_key(candidate) != key:
                    break
                cluster.append(candidate)
                j += 1

            pending_outs: Deque[Dict[str, Any]] = deque()
            pending_ins: Deque[Dict[str, Any]] = deque()
            merged_cluster: List[Dict[str, Any]] = []
            for event in cluster:
                already_paired = event.get("subOutPersonId") and event.get(
                    "subInPersonId"
                )
                if already_paired:
                    merged_cluster.append(event)
                    continue
                subtype = (event.get("subType") or "").lower()
                if subtype == "out":
                    pending_outs.append(event)
                elif subtype == "in":
                    pending_ins.append(event)
                else:
                    merged_cluster.append(event)
                while pending_outs and pending_ins:
                    merged_cluster.append(
                        self._merge_sub_events(
                            pending_outs.popleft(), pending_ins.popleft()
                        )
                    )

            if pending_outs:
                merged_cluster.extend(list(pending_outs))
            if pending_ins:
                merged_cluster.extend(list(pending_ins))

            result.extend(merged_cluster)

            i = j
        return result

    @staticmethod
    def _merge_sub_events(
        out_ev: Dict[str, Any], in_ev: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Combine outgoing and incoming substitutions into a single CDN action.
        """
        template = out_ev or in_ev
        base = dict(template)
        base["actionType"] = "substitution"
        base["subType"] = None
        base["subOutPersonId"] = out_ev.get("subOutPersonId") or out_ev.get("personId")
        base["subInPersonId"] = in_ev.get("subInPersonId") or in_ev.get("personId")
        # Prefer whichever half carries the richer metadata for key stamps.
        for key in ("teamId", "period", "clock", "timeActual"):
            if base.get(key) is None:
                base[key] = out_ev.get(key) if out_ev.get(key) is not None else in_ev.get(
                    key
                )
        description = (
            out_ev.get("description")
            or in_ev.get("description")
            or template.get("description")
            or "Substitution"
        )
        base["description"] = description
        order_candidates = [
            value
            for value in (
                out_ev.get("orderNumber"),
                in_ev.get("orderNumber"),
            )
            if value is not None
        ]
        if order_candidates:
            base["orderNumber"] = min(order_candidates)
        action_candidates = [
            value
            for value in (
                out_ev.get("actionNumber"),
                in_ev.get("actionNumber"),
            )
            if value is not None
        ]
        if action_candidates:
            base["actionNumber"] = min(action_candidates)
        return base

    @staticmethod
    def _action_sort_key(action: Dict[str, Any]) -> Tuple[int, int]:
        def _to_int(value: Any) -> int:
            try:
                return int(value)
            except Exception:
                return 0

        order = _to_int(action.get("orderNumber"))
        event = _to_int(action.get("actionNumber"))
        return (order, event)

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
