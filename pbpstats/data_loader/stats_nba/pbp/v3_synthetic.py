import math
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

from pbpstats import G_LEAGUE_GAME_ID_PREFIX, NBA_GAME_ID_PREFIX, WNBA_GAME_ID_PREFIX


ENDPOINT_STRATEGY_V2 = "v2"
ENDPOINT_STRATEGY_V3_SYNTHETIC = "v3_synthetic"
ENDPOINT_STRATEGY_AUTO = "auto"
VALID_ENDPOINT_STRATEGIES = {
    ENDPOINT_STRATEGY_V2,
    ENDPOINT_STRATEGY_V3_SYNTHETIC,
    ENDPOINT_STRATEGY_AUTO,
}

V2_HEADERS = [
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

ESSENTIAL_V2_HEADERS = {
    "GAME_ID",
    "EVENTNUM",
    "EVENTMSGTYPE",
    "EVENTMSGACTIONTYPE",
    "PERIOD",
    "PCTIMESTRING",
}


class StatsNbaV2PbpResponseError(RuntimeError):
    """
    Raised when a playbyplayv2 response or cache is missing the v2 row contract.
    """


class UnsupportedV3SyntheticSchemaError(RuntimeError):
    """
    Raised when synthetic v3 PBP is requested for an unvalidated league/schema.
    """


class StatsNbaV3SyntheticMappingError(RuntimeError):
    """
    Raised when a v3 actionType/subType cannot be mapped to v2 event codes.
    """

    def __init__(self, game_id, action, message=None):
        self.game_id = game_id
        self.action_number = action.get("actionNumber")
        self.action_id = action.get("actionId")
        self.action_type = action.get("actionType")
        self.sub_type = action.get("subType")
        self.description = action.get("description")
        error = message or "Unmapped playbyplayv3 action"
        super().__init__(
            f"{error}: game_id={self.game_id}, action_number={self.action_number}, "
            f"action_id={self.action_id}, action_type={self.action_type!r}, "
            f"sub_type={self.sub_type!r}, description={self.description!r}"
        )


class StatsNbaV3SyntheticRoleError(RuntimeError):
    """
    Raised when a v3 action group lacks a role required by the v2 contract.
    """

    def __init__(self, game_id, action, missing_role):
        self.game_id = game_id
        self.action_number = action.get("actionNumber")
        self.action_id = action.get("actionId")
        self.action_type = action.get("actionType")
        self.sub_type = action.get("subType")
        self.description = action.get("description")
        self.missing_role = missing_role
        super().__init__(
            f"Could not synthesize v2 role {missing_role!r}: "
            f"game_id={self.game_id}, action_number={self.action_number}, "
            f"action_id={self.action_id}, action_type={self.action_type!r}, "
            f"sub_type={self.sub_type!r}, description={self.description!r}"
        )


class StatsNbaV3SyntheticRoleSupplementError(RuntimeError):
    """
    Raised when a league requires a v2 role supplement and it is missing or invalid.
    """


class StatsNbaV3SyntheticParityError(RuntimeError):
    """
    Raised when v3 actions and a v2 role supplement do not align exactly.
    """


@dataclass(frozen=True)
class _SyntheticLeaguePolicy:
    league: str
    allow_v3_only: bool
    require_v2_role_supplement: bool


_NBA_SYNTHETIC_POLICY = _SyntheticLeaguePolicy(
    league="nba",
    allow_v3_only=True,
    require_v2_role_supplement=False,
)
_WNBA_SYNTHETIC_POLICY = _SyntheticLeaguePolicy(
    league="wnba",
    allow_v3_only=False,
    require_v2_role_supplement=True,
)


def validate_endpoint_strategy(endpoint_strategy):
    if endpoint_strategy not in VALID_ENDPOINT_STRATEGIES:
        raise ValueError(
            "endpoint_strategy must be one of " f"{sorted(VALID_ENDPOINT_STRATEGIES)}"
        )


def validate_v2_pbp_response(source_data):
    if not isinstance(source_data, dict):
        raise StatsNbaV2PbpResponseError("playbyplayv2 response is not a dict")
    result_sets = source_data.get("resultSets")
    if not result_sets or not isinstance(result_sets, list):
        raise StatsNbaV2PbpResponseError("playbyplayv2 response has no resultSets")
    first_result_set = result_sets[0] if result_sets else None
    if not isinstance(first_result_set, dict):
        raise StatsNbaV2PbpResponseError("playbyplayv2 resultSets[0] is malformed")
    headers = first_result_set.get("headers")
    rows = first_result_set.get("rowSet")
    if not headers or not isinstance(headers, list):
        raise StatsNbaV2PbpResponseError("playbyplayv2 response has no headers")
    if rows is None or not isinstance(rows, list):
        raise StatsNbaV2PbpResponseError("playbyplayv2 response has no rowSet")
    if not rows:
        raise StatsNbaV2PbpResponseError("playbyplayv2 response rowSet is empty")
    missing_headers = ESSENTIAL_V2_HEADERS.difference(headers)
    if missing_headers:
        raise StatsNbaV2PbpResponseError(
            "playbyplayv2 response is missing essential headers: "
            f"{sorted(missing_headers)}"
        )
    return True


def is_valid_v2_pbp_response(source_data):
    try:
        validate_v2_pbp_response(source_data)
        return True
    except StatsNbaV2PbpResponseError:
        return False


def build_synthetic_v2_pbp_response(
    game_id, v3_source_data, shotchartdetail=None, v2_role_supplement=None
):
    """
    Build a playbyplayv2-shaped response from playbyplayv3 actions.

    ``shotchartdetail`` is accepted for validation callers, but this transformer
    deliberately emits only v2 PBP fields.
    """
    del shotchartdetail

    policy = _get_synthetic_league_policy(game_id)

    if not isinstance(v3_source_data, dict):
        raise StatsNbaV2PbpResponseError("playbyplayv3 response is not a dict")
    game_data = v3_source_data.get("game", {})
    if not isinstance(game_data, dict):
        raise StatsNbaV2PbpResponseError("playbyplayv3 game data is malformed")
    actions = game_data.get("actions", [])
    if not actions or not isinstance(actions, list):
        raise StatsNbaV2PbpResponseError("playbyplayv3 response has no actions")

    context = _GameContext(actions)
    player_index = _PlayerIndex(actions)
    grouped_actions = _group_actions(actions)
    if not grouped_actions:
        raise StatsNbaV2PbpResponseError(
            "playbyplayv3 response has no actions with actionNumber"
        )
    role_supplement = None
    if policy.require_v2_role_supplement:
        role_supplement = _V2RoleSupplement.from_source_data(
            game_id, v2_role_supplement, grouped_actions
        )
    elif v2_role_supplement is not None:
        role_supplement = _V2RoleSupplement.from_source_data(
            game_id, v2_role_supplement, grouped_actions
        )

    rows = []
    for group in grouped_actions:
        builder = _SyntheticEventBuilder(game_id, group, context, player_index)
        if policy.require_v2_role_supplement:
            rows.append(builder.build_row_from_v2_supplement(role_supplement))
        else:
            rows.append(builder.build_row())
    return {
        "resultSets": [{"name": "PlayByPlay", "headers": V2_HEADERS, "rowSet": rows}]
    }


def _get_synthetic_league_policy(game_id):
    prefix = str(game_id)[:2]
    if prefix == NBA_GAME_ID_PREFIX:
        return _NBA_SYNTHETIC_POLICY
    if prefix == WNBA_GAME_ID_PREFIX:
        return _WNBA_SYNTHETIC_POLICY
    if prefix == G_LEAGUE_GAME_ID_PREFIX:
        raise UnsupportedV3SyntheticSchemaError(
            "Synthetic playbyplayv3 PBP is not validated for G League games"
        )
    raise UnsupportedV3SyntheticSchemaError(
        f"Synthetic playbyplayv3 PBP is not validated for game id {game_id!r}"
    )


class _V2RoleSupplement:
    def __init__(self, game_id, headers, rows_by_event_num):
        self.game_id = str(game_id).zfill(10)
        self.headers = headers
        self.rows_by_event_num = rows_by_event_num

    @classmethod
    def from_source_data(cls, game_id, source_data, grouped_actions):
        if source_data is None:
            raise StatsNbaV3SyntheticRoleSupplementError(
                f"v2 role supplement is required for game_id={game_id}"
            )
        try:
            validate_v2_pbp_response(source_data)
        except StatsNbaV2PbpResponseError as exc:
            raise StatsNbaV3SyntheticRoleSupplementError(
                f"Invalid v2 role supplement for game_id={game_id}: {exc}"
            ) from exc

        result_set = source_data["resultSets"][0]
        headers = result_set["headers"]
        missing_headers = set(V2_HEADERS).difference(headers)
        if missing_headers:
            raise StatsNbaV3SyntheticRoleSupplementError(
                "v2 role supplement is missing required headers: "
                f"{sorted(missing_headers)}"
            )

        rows_by_event_num = {}
        for raw_row in result_set["rowSet"]:
            row = dict(zip(headers, raw_row))
            event_num = _coerce_int(row.get("EVENTNUM"))
            if event_num is None:
                raise StatsNbaV3SyntheticRoleSupplementError(
                    f"v2 role supplement has row without EVENTNUM: game_id={game_id}"
                )
            if event_num in rows_by_event_num:
                raise StatsNbaV3SyntheticRoleSupplementError(
                    "v2 role supplement has duplicate EVENTNUM: "
                    f"game_id={game_id}, event_num={event_num}"
                )
            rows_by_event_num[event_num] = row

        v3_event_nums = {
            _coerce_int(group[0].get("actionNumber"))
            for group in grouped_actions
            if _coerce_int(group[0].get("actionNumber")) is not None
        }
        v2_event_nums = set(rows_by_event_num)
        if v3_event_nums != v2_event_nums:
            missing_in_v2 = sorted(v3_event_nums.difference(v2_event_nums))
            missing_in_v3 = sorted(v2_event_nums.difference(v3_event_nums))
            raise StatsNbaV3SyntheticParityError(
                "v3 actionNumber groups and v2 EVENTNUM rows do not align: "
                f"game_id={game_id}, missing_in_v2={missing_in_v2}, "
                f"missing_in_v3={missing_in_v3}"
            )

        return cls(game_id, headers, rows_by_event_num)

    def row_for_event_num(self, event_num):
        row = self.rows_by_event_num.get(event_num)
        if row is None:
            raise StatsNbaV3SyntheticParityError(
                f"v2 role supplement has no row for game_id={self.game_id}, "
                f"event_num={event_num}"
            )
        return row

    def build_validated_row(
        self, event_num, period, clock, event_type, event_action_type
    ):
        row = self.row_for_event_num(event_num)
        self._assert_equal("GAME_ID", row.get("GAME_ID"), self.game_id, event_num)
        self._assert_equal("PERIOD", row.get("PERIOD"), period, event_num)
        self._assert_clock_equal(row.get("PCTIMESTRING"), clock, event_num)
        self._assert_equal(
            "EVENTMSGTYPE", row.get("EVENTMSGTYPE"), event_type, event_num
        )
        self._assert_equal(
            "EVENTMSGACTIONTYPE",
            row.get("EVENTMSGACTIONTYPE"),
            event_action_type,
            event_num,
        )
        return [row.get(header) for header in V2_HEADERS]

    def _assert_equal(self, field, actual, expected, event_num):
        if str(actual) != str(expected):
            raise StatsNbaV3SyntheticParityError(
                f"v3/v2 supplement mismatch for {field}: game_id={self.game_id}, "
                f"event_num={event_num}, v2={actual!r}, v3={expected!r}"
            )

    def _assert_clock_equal(self, actual, expected, event_num):
        if _v2_v3_clock_values_match(actual, expected):
            return
        raise StatsNbaV3SyntheticParityError(
            "v3/v2 supplement mismatch for PCTIMESTRING: "
            f"game_id={self.game_id}, event_num={event_num}, "
            f"v2={actual!r}, v3={expected!r}"
        )


@dataclass(frozen=True)
class _PlayerRef:
    player_id: int
    name: Optional[str]
    team_id: Optional[int]


def _group_actions(actions):
    grouped = defaultdict(list)
    for action in actions:
        action_number = _coerce_int(action.get("actionNumber"))
        if action_number is None:
            continue
        grouped[action_number].append(action)

    def group_sort_key(group):
        action_ids = [
            _coerce_int(action.get("actionId"))
            for action in group
            if _coerce_int(action.get("actionId")) is not None
        ]
        return min(action_ids) if action_ids else 0

    grouped_actions = list(grouped.values())
    grouped_actions.sort(key=group_sort_key)
    for group in grouped_actions:
        group.sort(
            key=lambda action: (
                _coerce_int(action.get("actionId")) is None,
                _coerce_int(action.get("actionId")) or 0,
            )
        )
    return grouped_actions


class _GameContext:
    def __init__(self, actions):
        self.home_team_id = None
        self.visitor_team_id = None
        self.team_abbreviations = {}
        for action in actions:
            location = _clean_text(action.get("location")).lower()
            team_id = _event_team_id(action)
            if team_id:
                team_tricode = _clean_text(action.get("teamTricode")) or None
                if team_tricode:
                    self.team_abbreviations[team_id] = team_tricode
                if location == "h" and self.home_team_id is None:
                    self.home_team_id = team_id
                elif location == "v" and self.visitor_team_id is None:
                    self.visitor_team_id = team_id

    def description_column(self, team_id, location=None):
        location = _clean_text(location).lower()
        if team_id is not None and team_id == self.home_team_id:
            return "HOMEDESCRIPTION"
        if team_id is not None and team_id == self.visitor_team_id:
            return "VISITORDESCRIPTION"
        if location == "h":
            return "HOMEDESCRIPTION"
        if location == "v":
            return "VISITORDESCRIPTION"
        return "NEUTRALDESCRIPTION"

    def person_type(self, team_id, player_id=0, is_team_event=False):
        effective_team_id = team_id
        if effective_team_id in (None, 0) and _looks_like_team_id(player_id):
            effective_team_id = player_id
        if effective_team_id == self.home_team_id:
            return 2 if is_team_event else 4
        if effective_team_id == self.visitor_team_id:
            return 3 if is_team_event else 5
        return 0

    def abbreviation(self, team_id):
        return self.team_abbreviations.get(team_id)


class _PlayerIndex:
    def __init__(self, actions):
        self._players_by_team_name = defaultdict(lambda: defaultdict(dict))
        self._players_by_name = defaultdict(dict)
        for action in actions:
            player_id = _coerce_int(action.get("personId"))
            team_id = _event_team_id(action)
            name = _clean_text(action.get("playerName")) or None
            if not player_id or _looks_like_team_id(player_id) or not name:
                continue
            ref = _PlayerRef(player_id, name, team_id)
            for variant in _name_variants(name):
                self._players_by_name[variant][player_id] = ref
                if team_id:
                    self._players_by_team_name[team_id][variant][player_id] = ref

    def primary_ref(self, action, required_role):
        player_id = _coerce_int(action.get("personId"))
        team_id = _event_team_id(action)
        if not player_id or _looks_like_team_id(player_id):
            raise StatsNbaV3SyntheticRoleError(
                action.get("gameId"), action, required_role
            )
        return _PlayerRef(
            player_id, _clean_text(action.get("playerName")) or None, team_id
        )

    def resolve(self, name, team_id=None):
        normalized = _normalize_name(name)
        if not normalized:
            return None
        search_indexes = []
        if team_id:
            search_indexes.append(self._players_by_team_name.get(team_id, {}))
        search_indexes.append(self._players_by_name)
        for index in search_indexes:
            candidates = dict(index.get(normalized, {}))
            if not candidates:
                candidates = {}
                for indexed_name, refs in index.items():
                    if normalized.endswith(indexed_name) or indexed_name.endswith(
                        normalized
                    ):
                        candidates.update(refs)
            if len(candidates) == 1:
                return next(iter(candidates.values()))
            if len(candidates) > 1:
                return None
        return None


class _SyntheticEventBuilder:
    def __init__(self, game_id, group, context, player_index):
        self.game_id = str(game_id).zfill(10)
        self.group = group
        self.context = context
        self.player_index = player_index
        self.primary = self._find_primary_action()
        self.row = {header: None for header in V2_HEADERS}
        self.row["GAME_ID"] = self.game_id
        self.row["EVENTNUM"] = _coerce_int(self.primary.get("actionNumber"), 0)
        self.row["PERIOD"] = _coerce_int(self.primary.get("period"), 0)
        self.row["PCTIMESTRING"] = _format_v3_clock(self.primary.get("clock"))
        self.row["VIDEO_AVAILABLE_FLAG"] = _coerce_int(
            self.primary.get("videoAvailable"), 0
        )
        for slot in (1, 2, 3):
            self.row[f"PERSON{slot}TYPE"] = 0
            self.row[f"PLAYER{slot}_ID"] = 0

    def build_row(self):
        event_type, event_action_type = self._map_event_type()
        self.row["EVENTMSGTYPE"] = event_type
        self.row["EVENTMSGACTIONTYPE"] = event_action_type
        self._apply_score(event_type)

        action_type = _clean_text(self.primary.get("actionType")).lower()
        if action_type in {"made shot", "missed shot"}:
            self._build_field_goal()
        elif action_type == "free throw":
            self._build_free_throw()
        elif action_type == "rebound":
            self._build_rebound()
        elif action_type == "foul":
            self._build_foul()
        elif action_type == "turnover":
            self._build_turnover()
        elif action_type == "substitution":
            self._build_substitution()
        elif action_type == "jump ball":
            self._build_jump_ball()
        elif action_type == "violation":
            self._build_player_or_team_event()
        elif action_type == "timeout":
            self._build_timeout()
        elif action_type in {"period", "instant replay", "ejection"}:
            self._build_admin_event()
        else:
            raise StatsNbaV3SyntheticMappingError(self.game_id, self.primary)

        return [self.row[header] for header in V2_HEADERS]

    def build_row_from_v2_supplement(self, role_supplement):
        event_type, event_action_type = self._map_event_type()
        event_num = self.row["EVENTNUM"]
        return role_supplement.build_validated_row(
            event_num,
            self.row["PERIOD"],
            self.row["PCTIMESTRING"],
            event_type,
            event_action_type,
        )

    def _find_primary_action(self):
        substitution_action = self._find_primary_substitution_action()
        if substitution_action is not None:
            return substitution_action
        for action in self.group:
            action_type = _clean_text(action.get("actionType")).lower()
            if action_type in _PRIMARY_ACTION_TYPES:
                return action
        for action in self.group:
            if _clean_text(action.get("actionType")):
                return action
        return self.group[0]

    def _find_primary_substitution_action(self):
        substitution_actions = [
            action
            for action in self.group
            if _clean_text(action.get("actionType")).lower() == "substitution"
        ]
        if not substitution_actions:
            return None
        for action in substitution_actions:
            outgoing_name = _parse_substitution_outgoing_name(action.get("description"))
            if _normalize_name(action.get("playerName")) == _normalize_name(
                outgoing_name
            ):
                return action
        return substitution_actions[0]

    def _map_event_type(self):
        action_type = _clean_text(self.primary.get("actionType"))
        sub_type = _clean_text(self.primary.get("subType"))
        action_key = action_type.lower()
        sub_key = sub_type.lower()
        if action_key == "period":
            if sub_key == "start":
                return 12, 0
            if sub_key == "end":
                return 13, 0
        if action_key == "made shot":
            return 1, _map_from_table(_SHOT_ACTION_TYPES, sub_type, self)
        if action_key == "missed shot":
            return 2, _map_from_table(_SHOT_ACTION_TYPES, sub_type, self)
        if action_key == "free throw":
            return 3, _map_from_table(_FREE_THROW_ACTION_TYPES, sub_type, self)
        if action_key == "rebound":
            return 4, _map_from_table(_REBOUND_ACTION_TYPES, sub_type, self)
        if action_key == "turnover":
            return 5, _map_from_table(_TURNOVER_ACTION_TYPES, sub_type, self)
        if action_key == "foul":
            return 6, _map_from_table(_FOUL_ACTION_TYPES, sub_type, self)
        if action_key == "violation":
            return 7, _map_from_table(_VIOLATION_ACTION_TYPES, sub_type, self)
        if action_key == "substitution":
            return 8, 0
        if action_key == "timeout":
            return 9, _map_from_table(self._timeout_action_types, sub_type, self)
        if action_key == "jump ball":
            return 10, 0
        if action_key == "ejection":
            return 11, 0
        if action_key == "instant replay":
            return 18, _map_from_table(self._replay_action_types, sub_type, self)
        raise StatsNbaV3SyntheticMappingError(self.game_id, self.primary)

    @property
    def _is_wnba(self):
        return self.game_id[:2] == WNBA_GAME_ID_PREFIX

    @property
    def _timeout_action_types(self):
        return _WNBA_TIMEOUT_ACTION_TYPES if self._is_wnba else _TIMEOUT_ACTION_TYPES

    @property
    def _replay_action_types(self):
        return _WNBA_REPLAY_ACTION_TYPES if self._is_wnba else _REPLAY_ACTION_TYPES

    def _build_field_goal(self):
        shooter = self.player_index.primary_ref(self.primary, "shooter")
        self._fill_player_slot(1, shooter)
        self._assign_description(
            self.primary.get("description"),
            shooter.team_id,
            self.primary.get("location"),
        )
        assist_ref = self._find_assist_ref(shooter.team_id)
        if assist_ref is not None:
            self._fill_player_slot(2, assist_ref)
        block_ref = self._find_side_actor("block")
        if block_ref is not None:
            self._fill_player_slot(3, block_ref)
            self._assign_description(
                self._side_actor_action("block").get("description"),
                block_ref.team_id,
                self._side_actor_action("block").get("location"),
            )

    def _build_free_throw(self):
        shooter = self.player_index.primary_ref(self.primary, "free_throw_shooter")
        self._fill_player_slot(1, shooter)
        self._assign_description(
            self.primary.get("description"),
            shooter.team_id,
            self.primary.get("location"),
        )

    def _build_rebound(self):
        team_id = _event_team_id(self.primary)
        if not team_id:
            raise StatsNbaV3SyntheticRoleError(
                self.game_id, self.primary, "rebound_team"
            )
        player_id = _coerce_int(self.primary.get("personId"))
        if player_id and not _looks_like_team_id(player_id):
            self._fill_player_slot(
                1,
                _PlayerRef(
                    player_id,
                    _clean_text(self.primary.get("playerName")) or None,
                    team_id,
                ),
            )
        else:
            self._fill_team_slot(1, team_id)
        self._assign_description(
            self.primary.get("description"), team_id, self.primary.get("location")
        )

    def _build_foul(self):
        team_id = _event_team_id(self.primary)
        if not team_id:
            raise StatsNbaV3SyntheticRoleError(self.game_id, self.primary, "foul_team")
        player_id = _coerce_int(self.primary.get("personId"))
        if player_id and not _looks_like_team_id(player_id):
            self._fill_player_slot(
                1,
                _PlayerRef(
                    player_id,
                    _clean_text(self.primary.get("playerName")) or None,
                    team_id,
                ),
            )
        else:
            self._fill_team_slot(1, team_id)
        self._assign_description(
            self.primary.get("description"), team_id, self.primary.get("location")
        )
        drawn_ref = self._find_side_actor("foul_drawn")
        if drawn_ref is not None:
            self._fill_player_slot(2, drawn_ref)

    def _build_turnover(self):
        team_id = _event_team_id(self.primary)
        if not team_id:
            raise StatsNbaV3SyntheticRoleError(
                self.game_id, self.primary, "turnover_team"
            )
        player_id = _coerce_int(self.primary.get("personId"))
        if player_id and not _looks_like_team_id(player_id):
            self._fill_player_slot(
                1,
                _PlayerRef(
                    player_id,
                    _clean_text(self.primary.get("playerName")) or None,
                    team_id,
                ),
            )
        else:
            self._fill_team_slot(1, team_id)
        self._assign_description(
            self.primary.get("description"), team_id, self.primary.get("location")
        )
        steal_ref = self._find_side_actor("steal")
        if steal_ref is not None:
            self._fill_player_slot(2, steal_ref)
            self._assign_description(
                self._side_actor_action("steal").get("description"),
                steal_ref.team_id,
                self._side_actor_action("steal").get("location"),
            )

    def _build_substitution(self):
        outgoing = self.player_index.primary_ref(
            self.primary, "substitution_outgoing_player"
        )
        incoming_name = _parse_substitution_incoming_name(
            self.primary.get("description")
        )
        incoming = self.player_index.resolve(incoming_name, outgoing.team_id)
        if incoming is None:
            raise StatsNbaV3SyntheticRoleError(
                self.game_id, self.primary, "substitution_incoming_player"
            )
        self._fill_player_slot(1, outgoing)
        self._fill_player_slot(2, incoming)
        self._assign_description(
            self.primary.get("description"),
            outgoing.team_id,
            self.primary.get("location"),
        )

    def _build_jump_ball(self):
        jumper1 = self.player_index.primary_ref(self.primary, "jump_ball_player1")
        jumper2_name, tip_name = _parse_jump_ball_names(self.primary.get("description"))
        jumper2 = self.player_index.resolve(jumper2_name)
        tip_player = self.player_index.resolve(tip_name)
        if jumper2 is None:
            raise StatsNbaV3SyntheticRoleError(
                self.game_id, self.primary, "jump_ball_player2"
            )
        if tip_player is None:
            raise StatsNbaV3SyntheticRoleError(
                self.game_id, self.primary, "jump_ball_tip_to_player"
            )
        self._fill_player_slot(1, jumper1)
        self._fill_player_slot(2, jumper2)
        self._fill_player_slot(3, tip_player)
        self._assign_description(
            self.primary.get("description"),
            jumper1.team_id,
            self.primary.get("location"),
        )

    def _build_player_or_team_event(self):
        team_id = _event_team_id(self.primary)
        player_id = _coerce_int(self.primary.get("personId"))
        if player_id and not _looks_like_team_id(player_id):
            self._fill_player_slot(
                1,
                _PlayerRef(
                    player_id,
                    _clean_text(self.primary.get("playerName")) or None,
                    team_id,
                ),
            )
        elif team_id:
            self._fill_team_slot(1, team_id)
        self._assign_description(
            self.primary.get("description"), team_id, self.primary.get("location")
        )

    def _build_timeout(self):
        team_id = _event_team_id(self.primary)
        if team_id:
            self._fill_team_slot(1, team_id)
        self._assign_description(
            self.primary.get("description"), team_id, self.primary.get("location")
        )

    def _build_admin_event(self):
        team_id = _event_team_id(self.primary)
        player_id = _coerce_int(self.primary.get("personId"))
        if player_id and not _looks_like_team_id(player_id):
            self._fill_player_slot(
                1,
                _PlayerRef(
                    player_id,
                    _clean_text(self.primary.get("playerName")) or None,
                    team_id,
                ),
            )
        elif (
            team_id and _clean_text(self.primary.get("actionType")).lower() != "period"
        ):
            self._fill_team_slot(1, team_id)
        if _clean_text(self.primary.get("actionType")).lower() in {
            "instant replay",
            "ejection",
        }:
            self._assign_description(
                self.primary.get("description"), team_id, self.primary.get("location")
            )

    def _fill_player_slot(self, slot, player_ref):
        self.row[f"PERSON{slot}TYPE"] = self.context.person_type(
            player_ref.team_id, player_ref.player_id
        )
        self.row[f"PLAYER{slot}_ID"] = player_ref.player_id
        self.row[f"PLAYER{slot}_NAME"] = player_ref.name
        self.row[f"PLAYER{slot}_TEAM_ID"] = player_ref.team_id
        self.row[f"PLAYER{slot}_TEAM_ABBREVIATION"] = self.context.abbreviation(
            player_ref.team_id
        )

    def _fill_team_slot(self, slot, team_id):
        self.row[f"PERSON{slot}TYPE"] = self.context.person_type(
            team_id, team_id, is_team_event=True
        )
        self.row[f"PLAYER{slot}_ID"] = team_id
        self.row[f"PLAYER{slot}_NAME"] = None
        self.row[f"PLAYER{slot}_TEAM_ID"] = None
        self.row[f"PLAYER{slot}_TEAM_ABBREVIATION"] = self.context.abbreviation(team_id)

    def _assign_description(self, description, team_id=None, location=None):
        description = _clean_text(description) or None
        if description is None:
            return
        column = self.context.description_column(team_id, location)
        if self.row[column]:
            self.row[column] = f"{self.row[column]}: {description}"
        else:
            self.row[column] = description

    def _find_assist_ref(self, team_id):
        description = _clean_text(self.primary.get("description"))
        match = re.search(r"\((?P<name>.+?)\s+\d+\s+AST\)", description)
        if not match:
            return None
        return self.player_index.resolve(match.group("name"), team_id)

    def _side_actor_action(self, kind):
        for action in self.group:
            if action is self.primary:
                continue
            description = _clean_text(action.get("description")).upper()
            if kind == "block" and "BLOCK" in description:
                return action
            if kind == "steal" and "STEAL" in description:
                return action
            if kind == "foul_drawn" and (
                "DRAWN" in description
                or _clean_text(action.get("actionType")).lower()
                in {"foul drawn", "drawn foul"}
            ):
                return action
        return {}

    def _find_side_actor(self, kind):
        action = self._side_actor_action(kind)
        if not action:
            return None
        player_id = _coerce_int(action.get("personId"))
        team_id = _event_team_id(action)
        if not player_id or _looks_like_team_id(player_id):
            raise StatsNbaV3SyntheticRoleError(self.game_id, action, f"{kind}_player")
        return _PlayerRef(
            player_id, _clean_text(action.get("playerName")) or None, team_id
        )

    def _apply_score(self, event_type):
        score_home = _coerce_score(self.primary.get("scoreHome"))
        score_away = _coerce_score(self.primary.get("scoreAway"))
        if score_home is None or score_away is None:
            return
        if event_type == 12 and _coerce_int(self.primary.get("period"), 1) == 1:
            return
        if event_type not in {1, 3, 12, 13}:
            return
        self.row["SCORE"] = f"{score_away} - {score_home}"
        margin = score_home - score_away
        self.row["SCOREMARGIN"] = "TIE" if margin == 0 else str(margin)


def _map_from_table(mapping, sub_type, builder):
    key = _normalize_subtype_key(sub_type)
    if key in mapping:
        return mapping[key]
    raise StatsNbaV3SyntheticMappingError(
        builder.game_id,
        builder.primary,
        message="Unmapped playbyplayv3 subType",
    )


def _event_team_id(action):
    team_id = _coerce_int(action.get("teamId"))
    if team_id:
        return team_id
    person_id = _coerce_int(action.get("personId"))
    if _looks_like_team_id(person_id):
        return person_id
    return None


def _looks_like_team_id(value):
    value = _coerce_int(value)
    return value is not None and value >= 1610000000


def _coerce_int(value, default=None):
    if _is_missing(value):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_score(value):
    if _is_missing(value):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _is_missing(value):
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    return False


def _clean_text(value):
    if _is_missing(value):
        return ""
    return str(value).strip()


def _normalize_subtype_key(value):
    key = _clean_text(value).lower()
    key = re.sub(r"[^a-z0-9]+", " ", key)
    return re.sub(r"\s+", " ", key).strip()


def _format_v3_clock(clock):
    text = _clean_text(clock)
    match = re.match(r"PT(?P<minutes>\d+)M(?P<seconds>[\d.]+)S", text)
    if not match:
        return text
    minutes = int(match.group("minutes"))
    seconds = float(match.group("seconds"))
    if seconds == int(seconds):
        return f"{minutes}:{int(seconds):02d}"
    return f"{minutes}:{seconds:05.2f}"


def _v2_v3_clock_values_match(v2_clock, v3_clock):
    if str(v2_clock) == str(v3_clock):
        return True
    v2_seconds = _clock_to_seconds(v2_clock)
    v3_seconds = _clock_to_seconds(v3_clock)
    if v2_seconds is None or v3_seconds is None:
        return False
    return int(v3_seconds) == int(v2_seconds)


def _clock_to_seconds(clock):
    text = _clean_text(clock)
    if ":" not in text:
        return None
    minutes, seconds = text.split(":", 1)
    try:
        return int(minutes) * 60 + float(seconds)
    except (TypeError, ValueError):
        return None


def _normalize_name(name):
    return re.sub(r"[^a-z0-9]", "", _clean_text(name).lower())


def _name_variants(name):
    normalized = _normalize_name(name)
    if not normalized:
        return []
    parts = [_normalize_name(part) for part in _clean_text(name).split()]
    variants = {normalized}
    if parts:
        variants.add(parts[-1])
    if len(parts) >= 2 and parts[-1] in {"jr", "sr", "ii", "iii", "iv"}:
        variants.add(parts[-2] + parts[-1])
    return [variant for variant in variants if variant]


def _parse_substitution_incoming_name(description):
    match = re.search(r"SUB:\s*(?P<incoming>.+?)\s+FOR\s+.+", _clean_text(description))
    return match.group("incoming").strip() if match else ""


def _parse_substitution_outgoing_name(description):
    match = re.search(r"SUB:\s*.+?\s+FOR\s+(?P<outgoing>.+)", _clean_text(description))
    return match.group("outgoing").strip() if match else ""


def _parse_jump_ball_names(description):
    match = re.search(
        r"Jump Ball\s+(?P<jumper1>.+?)\s+vs\.?\s+(?P<jumper2>.+?)(?::\s*Tip to\s+(?P<tip>.+))?$",
        _clean_text(description),
    )
    if not match:
        return "", ""
    return (match.group("jumper2") or "").strip(), (match.group("tip") or "").strip()


_SHOT_ACTION_TYPES = {
    "jump shot": 1,
    "running jump shot": 2,
    "hook shot": 3,
    "tip shot": 4,
    "layup shot": 5,
    "driving layup shot": 6,
    "dunk shot": 7,
    "alley oop layup shot": 43,
    "driving dunk shot": 9,
    "turnaround shot": 47,
    "turnaround jump shot": 47,
    "running reverse layup shot": 74,
    "finger roll layup shot": 71,
    "driving finger roll layup shot": 75,
    "running finger roll layup shot": 76,
    "cutting finger roll layup shot": 99,
    "putback layup shot": 72,
    "jump bank shot": 66,
    "driving hook shot": 57,
    "driving bank hook shot": 93,
    "turnaround bank hook shot": 96,
    "alley oop dunk shot": 52,
    "tip layup shot": 97,
    "cutting layup shot": 98,
    "driving floating jump shot": 101,
    "driving floating bank jump shot": 102,
    "running layup shot": 41,
    "running dunk shot": 50,
    "reverse layup shot": 44,
    "driving reverse layup shot": 73,
    "floating jump shot": 78,
    "pullup jump shot": 79,
    "running pull up jump shot": 103,
    "step back jump shot": 80,
    "turnaround hook shot": 58,
    "turnaround fadeaway shot": 86,
    "fadeaway jump shot": 63,
    "cutting dunk shot": 108,
}

_FREE_THROW_ACTION_TYPES = {
    "free throw 1 of 1": 10,
    "free throw 1 of 2": 11,
    "free throw 2 of 2": 12,
    "free throw 1 of 3": 13,
    "free throw 2 of 3": 14,
    "free throw 3 of 3": 15,
    "free throw technical": 16,
    "technical free throw": 16,
    "free throw flagrant 1 of 1": 20,
    "flagrant free throw 1 of 1": 20,
    "free throw flagrant 1 of 2": 11,
    "free throw flagrant 2 of 2": 12,
    "flagrant free throw 1 of 2": 11,
    "flagrant free throw 2 of 2": 12,
    "free throw flagrant 1 of 3": 27,
    "flagrant free throw 1 of 3": 27,
    "free throw flagrant 2 of 3": 14,
    "free throw flagrant 3 of 3": 15,
    "free throw clear path 1 of 2": 11,
    "free throw clear path 2 of 2": 12,
    "clear path free throw 1 of 2": 11,
    "clear path free throw 2 of 2": 12,
}

_REBOUND_ACTION_TYPES = {
    "": 0,
    "unknown": 0,
    "normal rebound": 1,
}

_FOUL_ACTION_TYPES = {
    "personal": 1,
    "personal foul": 1,
    "shooting": 2,
    "shooting foul": 2,
    "loose ball": 3,
    "loose ball foul": 3,
    "offensive": 4,
    "offensive foul": 4,
    "inbound": 5,
    "inbound foul": 5,
    "away from play": 6,
    "away from play foul": 6,
    "clear path": 9,
    "clear path foul": 9,
    "double personal": 10,
    "double personal foul": 10,
    "technical": 11,
    "technical foul": 11,
    "non unsportsmanlike technical": 12,
    "hanging technical": 13,
    "flagrant type 1": 14,
    "flagrant type 1 foul": 14,
    "flagrant type 2": 15,
    "flagrant type 2 foul": 15,
    "double technical": 16,
    "double technical foul": 16,
    "defensive 3 seconds": 17,
    "defensive 3 seconds technical": 17,
    "defensive three seconds": 17,
    "defensive three seconds technical": 17,
    "delay technical": 18,
    "delay technical foul": 18,
    "delay of game": 18,
    "delay of game technical": 18,
    "taunting technical": 19,
    "offensive charge": 26,
    "personal block": 27,
    "take": 28,
    "take foul": 28,
    "personal take": 28,
    "shooting block": 29,
    "shooting block foul": 29,
    "transition take": 31,
    "transition take foul": 31,
}

_TURNOVER_ACTION_TYPES = {
    "": 0,
    "no turnover": 0,
    "bad pass": 1,
    "bad pass turnover": 1,
    "lost ball": 2,
    "lost ball turnover": 2,
    "double dribble": 6,
    "discontinued dribble": 7,
    "traveling": 4,
    "traveling violation": 4,
    "traveling turnover": 4,
    "3 second violation": 8,
    "three second violation": 8,
    "5 second violation": 9,
    "five second violation": 9,
    "8 second violation": 10,
    "eight second violation": 10,
    "shot clock": 11,
    "shot clock turnover": 11,
    "shot clock violation": 11,
    "backcourt violation": 13,
    "offensive goaltending": 15,
    "offensive goaltending violation": 15,
    "offensive foul turnover": 37,
    "lane violation": 17,
    "kicked ball": 19,
    "kicked ball violation": 19,
    "palming": 21,
    "palming violation": 21,
    "step out of bounds": 39,
    "step out of bounds turnover": 39,
    "lost ball out of bounds": 40,
    "lost ball out of bounds turnover": 40,
    "out of bounds lost ball turnover": 40,
    "bad pass out of bounds": 45,
    "bad pass out of bounds turnover": 45,
    "out of bounds bad pass turnover": 45,
}

_VIOLATION_ACTION_TYPES = {
    "delay of game": 1,
    "delay of game violation": 1,
    "defensive goaltending": 2,
    "defensive goaltending violation": 2,
    "lane": 3,
    "lane violation": 3,
    "jump ball": 4,
    "jump ball violation": 4,
    "kicked ball": 5,
    "kicked ball violation": 5,
    "double lane": 6,
    "double lane violation": 6,
}

_TIMEOUT_ACTION_TYPES = {
    "regular": 1,
    "short": 2,
    "official": 3,
    "mandatory": 1,
    "coach challenge": 7,
}

_WNBA_TIMEOUT_ACTION_TYPES = {
    **_TIMEOUT_ACTION_TYPES,
    "official": 4,
}

_REPLAY_ACTION_TYPES = {
    "replay center": 1,
    "support ruling": 4,
    "coach challenge support ruling": 4,
    "overturn ruling": 5,
    "coach challenge overturn ruling": 5,
    "ruling stands": 6,
    "coach challenge ruling stands": 6,
}

_WNBA_REPLAY_ACTION_TYPES = {
    "replay center": 1,
    "support ruling": 0,
    "coach challenge support ruling": 4,
    "overturn ruling": 1,
    "coach challenge overturn ruling": 5,
    "ruling stands": 3,
    "coach challenge ruling stands": 6,
}

_PRIMARY_ACTION_TYPES = {
    "made shot",
    "missed shot",
    "free throw",
    "rebound",
    "foul",
    "turnover",
    "substitution",
    "jump ball",
    "violation",
    "timeout",
    "period",
    "instant replay",
    "ejection",
}
