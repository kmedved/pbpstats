import abc
import json
import os

import requests

from pbpstats import (
    G_LEAGUE_GAME_ID_PREFIX,
    G_LEAGUE_STRING,
    HEADERS,
    NBA_GAME_ID_PREFIX,
    NBA_STRING,
    REQUEST_TIMEOUT,
    WNBA_GAME_ID_PREFIX,
    WNBA_STRING,
)
from pbpstats.overrides import IntDecoder
from pbpstats.resources.enhanced_pbp import (
    Ejection,
    EndOfPeriod,
    FieldGoal,
    Foul,
    FreeThrow,
    JumpBall,
    Substitution,
    Timeout,
    Turnover,
)


class InvalidNumberOfStartersException(Exception):
    """
    Class for exception when a team's 5 period starters can't be determined.

    You can add the correct period starters to
    overrides/missing_period_starters.json in your data directory to fix this.
    """

    pass


class StartOfPeriod(metaclass=abc.ABCMeta):
    """
    Class for start of period events
    """

    @abc.abstractclassmethod
    def get_period_starters(self, file_directory):
        """
        Gets player ids of players who started the period for each team

        :param str file_directory: directory in which overrides subdirectory exists
            containing period starter overrides when period starters can't be determined
            from parsing pbp events
        :returns: dict with list of player ids for each team
            with players on the floor at start of period
        :raises: :obj:`~pbpstats.resources.enhanced_pbp.start_of_period.InvalidNumberOfStartersException`:
            If all 5 players that start the period for a team can't be determined.
        """
        pass

    @property
    def current_players(self):
        """
        returns period starters
        """
        return self.period_starters

    @property
    def _raw_current_players(self):
        """
        returns period starters for raw lineup propagation
        """
        return self.period_starters

    @property
    def league(self):
        """
        Returns League for game id.

        First 2 in game id represent league - 00 for nba, 10 for wnba, 20 for g-league
        """
        if self.game_id[0:2] == NBA_GAME_ID_PREFIX:
            return NBA_STRING
        elif self.game_id[0:2] == G_LEAGUE_GAME_ID_PREFIX:
            return G_LEAGUE_STRING
        elif self.game_id[0:2] == WNBA_GAME_ID_PREFIX:
            return WNBA_STRING

    @property
    def league_url_part(self):
        if self.game_id[0:2] == NBA_GAME_ID_PREFIX:
            return NBA_STRING
        elif self.game_id[0:2] == G_LEAGUE_GAME_ID_PREFIX:
            return f"{G_LEAGUE_STRING}.{NBA_STRING}"
        elif self.game_id[0:2] == WNBA_GAME_ID_PREFIX:
            return WNBA_STRING

    def _get_period_start_tenths(self):
        if self.league == WNBA_STRING:
            regulation_tenths = 6000
        else:
            regulation_tenths = 7200

        if self.period == 1:
            return 0
        if self.period <= 4:
            return int(regulation_tenths * (self.period - 1))
        return int(4 * regulation_tenths + 3000 * (self.period - 5))

    def _get_period_boxscore_request_params(self, mode):
        period_start_tenths = self._get_period_start_tenths()
        if mode == "rt2_start_window":
            return {
                "GameId": self.game_id,
                "StartPeriod": 0,
                "EndPeriod": 0,
                "RangeType": 2,
                "StartRange": period_start_tenths,
                "EndRange": period_start_tenths + 10,
            }
        if mode == "rt1_period_participants":
            return {
                "GameId": self.game_id,
                "StartPeriod": self.period,
                "EndPeriod": self.period,
                "RangeType": 1,
                "StartRange": 0,
                "EndRange": 0,
            }
        raise ValueError(f"Unknown period boxscore mode: {mode}")

    def _fetch_period_boxscore_response(self, mode):
        base_url = (
            f"https://stats.{self.league_url_part}.com/stats/boxscoretraditionalv3"
        )
        response = requests.get(
            base_url,
            self._get_period_boxscore_request_params(mode),
            headers=HEADERS,
            timeout=REQUEST_TIMEOUT,
        )
        if response.status_code == 200:
            return response.json()
        response.raise_for_status()

    def _load_period_boxscore_response(self, mode):
        loader_obj = getattr(self, "period_boxscore_source_loader", None)
        if loader_obj is not None:
            try:
                return loader_obj.load_data(self.game_id, self.period, mode)
            except Exception:
                return None
        try:
            return self._fetch_period_boxscore_response(mode)
        except Exception:
            return None

    def _normalize_boxscore_players_by_team(self, players_by_team):
        normalized = {}
        for team_id, players in players_by_team.items():
            if not isinstance(team_id, int) or team_id <= 0:
                continue
            seen = set()
            unique_players = []
            for player_id in players:
                if not isinstance(player_id, int) or player_id <= 0:
                    continue
                if player_id in seen:
                    continue
                seen.add(player_id)
                unique_players.append(player_id)
            if unique_players:
                normalized[team_id] = unique_players
        return normalized

    def _extract_period_boxscore_candidates_by_team(self, response_json):
        if not isinstance(response_json, dict):
            return {}
        boxscore = response_json.get("boxScoreTraditional")
        if not isinstance(boxscore, dict):
            return {}

        players_by_team = {}
        for team_key in ["awayTeam", "homeTeam"]:
            team_data = boxscore.get(team_key)
            if not isinstance(team_data, dict):
                continue
            team_id = team_data.get("teamId")
            players = team_data.get("players", [])
            if not isinstance(players, list):
                continue
            players_by_team[team_id] = [
                player.get("personId")
                for player in players
                if isinstance(player, dict)
            ]
        return self._normalize_boxscore_players_by_team(players_by_team)

    def _get_period_boxscore_source_name(self, response_json):
        if not isinstance(response_json, dict):
            return None
        source_info = response_json.get("periodStarterSource")
        if not isinstance(source_info, dict):
            return None
        source_name = source_info.get("name")
        if isinstance(source_name, str) and source_name.strip():
            return source_name.strip()
        return None

    def _is_exact_starter_map(self, starters_by_team):
        starters_by_team = self._normalize_boxscore_players_by_team(starters_by_team)
        return (
            len(starters_by_team) == 2
            and all(len(starters) == 5 for starters in starters_by_team.values())
            and len(
                {
                    player_id
                    for starters in starters_by_team.values()
                    for player_id in starters
                }
            )
            == 10
        )

    def _iter_period_events(self):
        event = getattr(self, "first_period_event", None)
        if event is None:
            event = getattr(self, "next_event", None)
        while event is not None and getattr(event, "period", None) == self.period:
            yield event
            event = event.next_event

    def _get_period_substitution_order_lookup(self):
        substitution_order_lookup = {}
        for event_order, event in enumerate(self._iter_period_events(), start=1):
            if not isinstance(event, Substitution):
                continue
            team_id = getattr(event, "team_id", None)
            if not isinstance(team_id, int) or team_id <= 0:
                continue
            for kind, player_id in [
                ("in", getattr(event, "incoming_player_id", None)),
                ("out", getattr(event, "outgoing_player_id", None)),
            ]:
                if not isinstance(player_id, int) or player_id <= 0:
                    continue
                team_lookup = substitution_order_lookup.setdefault(team_id, {})
                player_lookup = team_lookup.setdefault(player_id, {"in": [], "out": []})
                player_lookup[kind].append(event_order)
        return substitution_order_lookup

    def _classify_period_boxscore_candidate(self, team_id, player_id, substitution_lookup):
        player_lookup = (
            substitution_lookup.get(team_id, {}).get(
                player_id, {"in": [], "out": []}
            )
        )
        sub_in_orders = player_lookup["in"]
        sub_out_orders = player_lookup["out"]
        has_sub_in = len(sub_in_orders) > 0
        has_sub_out = len(sub_out_orders) > 0

        if not has_sub_in and not has_sub_out:
            return True
        if has_sub_out and not has_sub_in:
            return True
        if has_sub_in and not has_sub_out:
            return False

        first_sub_in_order = min(sub_in_orders)
        first_sub_out_order = min(sub_out_orders)
        if first_sub_out_order < first_sub_in_order:
            return True
        if first_sub_in_order < first_sub_out_order:
            return False
        return None

    def _narrow_period_boxscore_candidates_to_starters(self, candidates_by_team):
        candidates_by_team = self._normalize_boxscore_players_by_team(candidates_by_team)
        if len(candidates_by_team) != 2:
            return None

        substitution_lookup = self._get_period_substitution_order_lookup()
        starters_by_team = {}
        for team_id, candidates in candidates_by_team.items():
            starters = []
            for player_id in candidates:
                is_starter = self._classify_period_boxscore_candidate(
                    team_id, player_id, substitution_lookup
                )
                if is_starter is None:
                    return None
                if is_starter:
                    starters.append(player_id)
            starters_by_team[team_id] = starters

        if self._is_exact_starter_map(starters_by_team):
            return starters_by_team
        return None

    def _strict_starters_are_impossible(self, starters_by_team):
        """
        Return True when a strict PBP starter map is internally contradictory.

        This stays intentionally narrow: it only flags impossible lineup states,
        not merely suspicious ones. The main target is the case where a claimed
        starter later has a same-period substitution pattern that proves they
        were not actually on the floor when the period began.
        """
        if not isinstance(starters_by_team, dict) or len(starters_by_team) != 2:
            return True

        substitution_lookup = self._get_period_substitution_order_lookup()
        seen_players = set()

        for team_id, starters in starters_by_team.items():
            if not isinstance(team_id, int) or team_id <= 0:
                return True
            if not isinstance(starters, list) or len(starters) != 5:
                return True

            team_seen = set()
            for player_id in starters:
                if not isinstance(player_id, int) or player_id <= 0:
                    return True
                if player_id in team_seen or player_id in seen_players:
                    return True

                team_seen.add(player_id)
                seen_players.add(player_id)

                starter_classification = self._classify_period_boxscore_candidate(
                    team_id, player_id, substitution_lookup
                )
                if starter_classification is not True:
                    return True

        return False

    def _get_starters_from_boxscore_request(self):
        """
        Use period-level boxscoretraditionalv3 fallback to resolve starters.

        The fallback prefers a one-second start-of-period RangeType=2 window.
        If that window does not resolve to an exact 5-on-5 lineup, it narrows the
        returned candidates with substitution timing. It then repeats the same
        narrowing with a RangeType=1 participant set if needed.
        """
        rt2_response = self._load_period_boxscore_response("rt2_start_window")
        rt2_candidates = self._extract_period_boxscore_candidates_by_team(rt2_response)
        if self._is_exact_starter_map(rt2_candidates):
            return rt2_candidates

        if rt2_candidates:
            starters_by_team = self._narrow_period_boxscore_candidates_to_starters(
                rt2_candidates
            )
            if starters_by_team is not None:
                return starters_by_team

        rt1_response = self._load_period_boxscore_response("rt1_period_participants")
        rt1_candidates = self._extract_period_boxscore_candidates_by_team(rt1_response)
        if rt1_candidates:
            starters_by_team = self._narrow_period_boxscore_candidates_to_starters(
                rt1_candidates
            )
            if starters_by_team is not None:
                return starters_by_team

        raise InvalidNumberOfStartersException(
            f"GameId: {self.game_id}, Period: {self.period}, Starters: {rt2_candidates or rt1_candidates}"
        )

    def _get_exact_local_period_boxscore_starters(self):
        loader_obj = getattr(self, "period_boxscore_source_loader", None)
        if loader_obj is None:
            return None, None

        rt2_response = self._load_period_boxscore_response("rt2_start_window")
        if not isinstance(rt2_response, dict):
            return None, None

        rt2_candidates = self._extract_period_boxscore_candidates_by_team(rt2_response)
        if not self._is_exact_starter_map(rt2_candidates):
            return None, self._get_period_boxscore_source_name(rt2_response)

        return (
            self._normalize_boxscore_players_by_team(rt2_candidates),
            self._get_period_boxscore_source_name(rt2_response),
        )

    def get_team_starting_with_ball(self):
        """
        returns team id for team on starting period with the ball
        """
        if (self.period == 1 or self.period >= 5) and isinstance(
            self.next_event, JumpBall
        ):
            # period starts with jump ball - team that wins starts with the ball
            return self.next_event.team_id
        else:
            # find team id on first shot, non technical ft or turnover
            next_event = self.next_event
            while not (
                isinstance(next_event, (FieldGoal, Turnover))
                or (
                    isinstance(next_event, FreeThrow) and not next_event.is_technical_ft
                )
            ):
                next_event = next_event.next_event
            return next_event.team_id

    def get_offense_team_id(self):
        """
        returns team id for team on starting period on offense
        """
        return self.team_starting_with_ball

    def _get_known_team_ids_for_period(self):
        team_ids = set()
        prev_lineups = getattr(self, "previous_period_end_lineups", None)
        if isinstance(prev_lineups, dict):
            for team_id in prev_lineups.keys():
                if isinstance(team_id, int) and team_id > 0:
                    team_ids.add(team_id)

        event = self
        while event is not None and not isinstance(event, EndOfPeriod):
            for attr in [
                "team_id",
                "player1_team_id",
                "player2_team_id",
                "player3_team_id",
            ]:
                team_id = getattr(event, attr, None)
                if isinstance(team_id, int) and team_id > 0:
                    team_ids.add(team_id)
            event = event.next_event
        return team_ids

    def _is_valid_starter_candidate(self, player_id, known_team_ids):
        if not isinstance(player_id, int) or player_id <= 0:
            return False
        if player_id in known_team_ids:
            return False
        # Team ids in malformed historical rows are 10-digit values.
        if player_id >= 100000000:
            return False
        return True

    def _record_starter_candidate(
        self,
        player_id,
        starters,
        subbed_in_players,
        player_first_seen_order,
        known_team_ids,
        event_order,
        player_first_seen_seconds_remaining=None,
        seconds_remaining=None,
    ):
        if not self._is_valid_starter_candidate(player_id, known_team_ids):
            return
        if player_id not in player_first_seen_order:
            player_first_seen_order[player_id] = event_order
        if (
            player_first_seen_seconds_remaining is not None
            and player_id not in player_first_seen_seconds_remaining
        ):
            player_first_seen_seconds_remaining[player_id] = (
                float(seconds_remaining)
                if seconds_remaining is not None
                else float("-inf")
            )
        if player_id not in starters and player_id not in subbed_in_players:
            starters.append(player_id)

    def _get_first_team_substitution_order(self):
        first_sub_order = {}
        event = self
        event_order = 0
        while event is not None and not isinstance(event, EndOfPeriod):
            event_order += 1
            if isinstance(event, Substitution) and isinstance(
                getattr(event, "team_id", None), int
            ):
                team_id = event.team_id
                if team_id > 0 and team_id not in first_sub_order:
                    first_sub_order[team_id] = event_order
            event = event.next_event
        return first_sub_order

    def _trim_excess_starters(
        self, starters_by_team, player_first_seen_order, known_team_ids
    ):
        first_team_sub_order = self._get_first_team_substitution_order()
        for team_id, starters in list(starters_by_team.items()):
            if len(starters) <= 5:
                continue

            unique_starters = []
            for player_id in starters:
                if (
                    self._is_valid_starter_candidate(player_id, known_team_ids)
                    and player_id not in unique_starters
                ):
                    unique_starters.append(player_id)

            if len(unique_starters) <= 5:
                starters_by_team[team_id] = unique_starters
                continue

            team_sub_order = first_team_sub_order.get(team_id, float("inf"))

            def _sort_key(player_id):
                first_seen = player_first_seen_order.get(player_id, float("inf"))
                appears_before_team_sub = 0 if first_seen < team_sub_order else 1
                unseen_penalty = 0 if first_seen != float("inf") else 1
                return (
                    appears_before_team_sub,
                    unseen_penalty,
                    first_seen,
                    unique_starters.index(player_id),
                )

            starters_by_team[team_id] = sorted(unique_starters, key=_sort_key)[:5]

        return starters_by_team

    def _get_players_who_started_period_with_team_map(self):
        starters = []
        subbed_in_players = []
        player_team_map = {}  # only player1 has team id in event, this is to track team
        player_first_seen_order = {}
        player_first_seen_seconds_remaining = {}
        player_first_sub_in_seconds_remaining = {}
        known_team_ids = self._get_known_team_ids_for_period()
        event = self
        event_order = 0
        while event is not None and not isinstance(event, EndOfPeriod):
            event_order += 1
            if (
                not isinstance(event, Timeout)
                and self._is_valid_starter_candidate(event.player1_id, known_team_ids)
                and hasattr(event, "team_id")
            ):
                player_id = event.player1_id
                if not isinstance(event, JumpBall):
                    # on jump balls team id is winning team, not guaranteed to be player1 team
                    player_team_map[player_id] = event.team_id
                if (
                    isinstance(event, Substitution)
                    and event.incoming_player_id is not None
                ):
                    if self._is_valid_starter_candidate(
                        event.incoming_player_id, known_team_ids
                    ):
                        player_team_map[event.incoming_player_id] = event.team_id
                        if event.incoming_player_id not in player_first_sub_in_seconds_remaining:
                            player_first_sub_in_seconds_remaining[event.incoming_player_id] = float(
                                getattr(event, "seconds_remaining", float("-inf"))
                            )
                    if self._is_valid_starter_candidate(
                        event.incoming_player_id, known_team_ids
                    ) and (
                        event.incoming_player_id not in starters
                        and event.incoming_player_id not in subbed_in_players
                    ):
                        subbed_in_players.append(event.incoming_player_id)
                    if player_id not in starters and player_id not in subbed_in_players:
                        self._record_starter_candidate(
                            player_id,
                            starters,
                            subbed_in_players,
                            player_first_seen_order,
                            known_team_ids,
                            event_order,
                            player_first_seen_seconds_remaining=player_first_seen_seconds_remaining,
                            seconds_remaining=getattr(event, "seconds_remaining", None),
                        )

                is_technical_foul = isinstance(event, Foul) and (
                    event.is_technical or event.is_double_technical
                )
                if player_id not in starters and player_id not in subbed_in_players:
                    tech_ft_at_period_start = (
                        isinstance(event, FreeThrow) and event.clock == "12:00"
                    )
                    if not (
                        is_technical_foul
                        or isinstance(event, Ejection)
                        or tech_ft_at_period_start
                    ):
                        # ignore all techs because a player could get a technical foul when they aren't in the game
                        self._record_starter_candidate(
                            player_id,
                            starters,
                            subbed_in_players,
                            player_first_seen_order,
                            known_team_ids,
                            event_order,
                            player_first_seen_seconds_remaining=player_first_seen_seconds_remaining,
                            seconds_remaining=getattr(event, "seconds_remaining", None),
                        )
                # need player2_id and player3_id for players who play full period and never appear in an event as player_id - ex assists, blocks, steals, foul drawn
                if not isinstance(event, Substitution) and not (
                    is_technical_foul or isinstance(event, Ejection)
                ):
                    # ignore all techs because a player could get a technical foul when they aren't in the game
                    if hasattr(event, "player2_id"):
                        self._record_starter_candidate(
                            event.player2_id,
                            starters,
                            subbed_in_players,
                            player_first_seen_order,
                            known_team_ids,
                            event_order,
                            player_first_seen_seconds_remaining=player_first_seen_seconds_remaining,
                            seconds_remaining=getattr(event, "seconds_remaining", None),
                        )
                    if hasattr(event, "player3_id"):
                        self._record_starter_candidate(
                            event.player3_id,
                            starters,
                            subbed_in_players,
                            player_first_seen_order,
                            known_team_ids,
                            event_order,
                            player_first_seen_seconds_remaining=player_first_seen_seconds_remaining,
                            seconds_remaining=getattr(event, "seconds_remaining", None),
                        )
            event = event.next_event

        # Some malformed recent windows log a player's first action before the
        # substitution that actually brought them into the game. If the first
        # explicit sub-in happens earlier in game time, or at the exact same
        # clock, treat them as a sub, not a starter.
        for player_id, sub_seconds_remaining in player_first_sub_in_seconds_remaining.items():
            first_seen_seconds_remaining = player_first_seen_seconds_remaining.get(player_id)
            if first_seen_seconds_remaining is None:
                continue
            if sub_seconds_remaining + 0.001 >= first_seen_seconds_remaining:
                starters = [starter_id for starter_id in starters if starter_id != player_id]
                if player_id not in subbed_in_players:
                    subbed_in_players.append(player_id)
        return starters, player_team_map, player_first_seen_order, subbed_in_players

    def _split_up_starters_by_team(self, starters, player_team_map):
        starters_by_team = {}
        known_team_ids = {
            team_id for team_id in player_team_map.values() if isinstance(team_id, int)
        }
        # for players who don't appear in event as player1 - won't be in player_team_map
        dangling_starters = []
        for player_id in starters:
            if not self._is_valid_starter_candidate(player_id, known_team_ids):
                continue
            team_id = player_team_map.get(player_id)
            if team_id is not None:
                if team_id not in starters_by_team.keys():
                    starters_by_team[team_id] = []
                starters_by_team[team_id].append(player_id)
            else:
                dangling_starters.append(player_id)
        # if there is one dangling starter we can add it to team missing a starter
        if len(dangling_starters) == 1 and len(starters) == 10:
            for _, team_starters in starters_by_team.items():
                if len(team_starters) == 4:
                    team_starters += dangling_starters
        return starters_by_team

    def _load_period_starter_overrides(self, file_directory):
        if file_directory is None:
            return {}

        override_files = [
            f"{file_directory}/overrides/missing_period_starters.json",
            f"{file_directory}/overrides/period_starters_overrides.json",
        ]
        merged_overrides = {}

        for override_file_path in override_files:
            if not os.path.isfile(override_file_path):
                continue
            with open(override_file_path) as f:
                override_data = json.loads(f.read(), cls=IntDecoder)
            for game_id, game_periods in override_data.items():
                merged_overrides.setdefault(game_id, {})
                for period, team_map in game_periods.items():
                    merged_overrides[game_id].setdefault(period, {})
                    merged_overrides[game_id][period].update(team_map)

        return merged_overrides

    def _apply_period_starter_overrides(self, starters_by_team, file_directory):
        overrides = self._load_period_starter_overrides(file_directory)
        game_id_keys = [self.game_id]
        try:
            game_id_keys.append(int(self.game_id))
        except (TypeError, ValueError):
            pass

        team_overrides = {}
        for game_id in game_id_keys:
            team_overrides.update(overrides.get(game_id, {}).get(self.period, {}))
        if not team_overrides:
            return starters_by_team

        updated = dict(starters_by_team)
        for team_id, starters in team_overrides.items():
            updated[team_id] = starters
        return updated

    def _has_period_starter_override(self, file_directory):
        if file_directory is None:
            return False

        overrides = self._load_period_starter_overrides(file_directory)
        game_id_keys = [self.game_id]
        try:
            game_id_keys.append(int(self.game_id))
        except (TypeError, ValueError):
            pass

        for game_id in game_id_keys:
            if overrides.get(game_id, {}).get(self.period):
                return True
        return False

    def _check_both_teams_have_5_starters(self, starters_by_team, file_directory):
        """
        raises exception if either team does not have 5 starters
        """
        for team_id, starters in starters_by_team.items():
            if len(starters) != 5:
                # check if game and period are in overrides file
                if file_directory is None:
                    raise InvalidNumberOfStartersException(
                        f"GameId: {self.game_id}, Period: {self.period}, TeamId: {team_id}, Players: {starters}"
                    )

                missing_period_starters_file_path = (
                    f"{file_directory}/overrides/missing_period_starters.json"
                )
                if not os.path.isfile(missing_period_starters_file_path):
                    raise InvalidNumberOfStartersException(
                        f"GameId: {self.game_id}, Period: {self.period}, TeamId: {team_id}, Players: {starters}"
                    )

                with open(missing_period_starters_file_path) as f:
                    # hard code corrections for games with incorrect number of starters exceptions
                    missing_period_starters = json.loads(f.read(), cls=IntDecoder)
                game_id = (
                    self.game_id if self.league == NBA_STRING else int(self.game_id)
                )
                if (
                    game_id in missing_period_starters.keys()
                    and self.period in missing_period_starters[game_id].keys()
                    and team_id in missing_period_starters[game_id][self.period].keys()
                ):
                    starters_by_team[team_id] = missing_period_starters[game_id][
                        self.period
                    ][team_id]
                else:
                    raise InvalidNumberOfStartersException(
                        f"GameId: {game_id}, Period: {self.period}, TeamId: {team_id}, Players: {starters}"
                    )

    def _get_period_start_substitutions(self):
        """
        Get players substituted in/out at the exact start of this period.

        This is needed to correctly handle period-start lineup swaps when
        filling missing starters from the previous period's ending lineup.

        Note: In live data, substitution events at period start may appear
        BEFORE the StartOfPeriod event in the event list. We scan forward
        from the first event of this period to catch all period-start subs.

        Returns:
            dict: {team_id: {"in": set of player_ids, "out": set of player_ids}}
        """
        result = {}
        start_seconds = self._get_period_start_seconds()

        # Scan from first event of this period (may be before StartOfPeriod marker)
        first_event = getattr(self, "first_period_event", None)
        event = first_event if first_event is not None else self.next_event

        while event is not None:
            if getattr(event, "period", None) != self.period:
                break

            event_seconds = getattr(event, "seconds_remaining", None)
            if event_seconds is None:
                event = event.next_event
                continue

            # Stop if we've moved past period start time
            if event_seconds < start_seconds - 0.001:
                break

            # Skip events not at exact period start time
            if abs(event_seconds - start_seconds) > 0.001:
                event = event.next_event
                continue

            # Process substitution events at period start
            if isinstance(event, Substitution):
                if self._should_delay_period_start_substitution(event, start_seconds):
                    event = event.next_event
                    continue
                team_id = getattr(event, "team_id", None)
                if team_id is not None:
                    if team_id not in result:
                        result[team_id] = {"in": set(), "out": set()}

                    incoming = getattr(event, "incoming_player_id", None)
                    outgoing = getattr(event, "outgoing_player_id", None)
                    if incoming is not None:
                        result[team_id]["in"].add(incoming)
                    if outgoing is not None:
                        result[team_id]["out"].add(outgoing)

            event = event.next_event

        return result

    def _period_start_cluster_credits_outgoing_player_before_sub(
        self, event, sub_team_id, outgoing_player_id
    ):
        if getattr(event, "player1_id", None) != outgoing_player_id:
            return False

        event_team_id = getattr(event, "team_id", None)
        same_team_credit = event_team_id == sub_team_id or event_team_id in [0, None]

        if isinstance(event, FreeThrow):
            return event_team_id == sub_team_id

        if isinstance(event, Foul):
            return same_team_credit and (
                event.is_technical or event.is_double_technical or event.is_flagrant
            )

        if isinstance(event, Ejection):
            return same_team_credit

        return False

    def _should_delay_period_start_substitution(self, sub_event, start_seconds):
        """
        Some period-start dead-ball clusters record the substitution before the
        outgoing player's technical/flagrant sequence has fully resolved.

        In those windows the outgoing player is still on the floor for the
        opening cluster, and the substitution should only take effect after the
        cluster ends.
        """
        sub_team_id = getattr(sub_event, "team_id", None)
        outgoing_player_id = getattr(sub_event, "outgoing_player_id", None)
        if not isinstance(sub_team_id, int) or sub_team_id <= 0:
            return False
        if not isinstance(outgoing_player_id, int) or outgoing_player_id <= 0:
            return False

        first_event = getattr(self, "first_period_event", None)
        event = first_event if first_event is not None else self.next_event

        while event is not None:
            if getattr(event, "period", None) != self.period:
                break

            event_seconds = getattr(event, "seconds_remaining", None)
            if event_seconds is None:
                event = event.next_event
                continue
            if event_seconds < start_seconds - 0.001:
                break
            if abs(event_seconds - start_seconds) > 0.001:
                event = event.next_event
                continue

            if event is not sub_event and self._period_start_cluster_credits_outgoing_player_before_sub(
                event, sub_team_id, outgoing_player_id
            ):
                return True

            event = event.next_event

        return False

    def _period_start_v6_diff_matches_delayed_sub_cluster(
        self, team_id, strict_players, local_boxscore_players, start_seconds
    ):
        strict_set = set(strict_players or [])
        local_set = set(local_boxscore_players or [])
        strict_only = strict_set - local_set
        local_only = local_set - strict_set
        if len(strict_only) != 1 or len(local_only) != 1:
            return False

        outgoing_player_id = next(iter(strict_only))
        incoming_player_id = next(iter(local_only))
        first_event = getattr(self, "first_period_event", None)
        event = first_event if first_event is not None else self.next_event

        while event is not None:
            if getattr(event, "period", None) != self.period:
                break

            event_seconds = getattr(event, "seconds_remaining", None)
            if event_seconds is None:
                event = event.next_event
                continue
            if event_seconds < start_seconds - 0.001:
                break
            if abs(event_seconds - start_seconds) > 0.001:
                event = event.next_event
                continue

            if (
                isinstance(event, Substitution)
                and getattr(event, "team_id", None) == team_id
                and getattr(event, "incoming_player_id", None) == incoming_player_id
                and getattr(event, "outgoing_player_id", None) == outgoing_player_id
                and self._should_delay_period_start_substitution(event, start_seconds)
            ):
                return True

            event = event.next_event

        return False

    def _should_prefer_strict_starters_over_exact_v6(
        self, strict_starters, local_boxscore_starters
    ):
        if not self._is_exact_starter_map(strict_starters):
            return False
        if not self._is_exact_starter_map(local_boxscore_starters):
            return False

        start_seconds = self._get_period_start_seconds()
        saw_supported_difference = False
        for team_id, strict_players in strict_starters.items():
            local_players = local_boxscore_starters.get(team_id, [])
            if set(strict_players) == set(local_players):
                continue
            if not self._period_start_v6_diff_matches_delayed_sub_cluster(
                team_id, strict_players, local_players, start_seconds
            ):
                return False
            saw_supported_difference = True

        return saw_supported_difference

    def _get_period_start_seconds(self):
        if self.period <= 4:
            if self.league == WNBA_STRING:
                return 600.0
            return 720.0
        return 300.0

    def _get_later_period_sub_in_players(self):
        """
        Track players who explicitly sub IN after the period-start window.

        These players are not safe carryover candidates when we try to
        backfill missing starters from the previous period's ending lineup.
        """
        result = {}
        start_seconds = self._get_period_start_seconds()

        first_event = getattr(self, "first_period_event", None)
        event = first_event if first_event is not None else self.next_event

        while event is not None:
            if getattr(event, "period", None) != self.period:
                break

            event_seconds = getattr(event, "seconds_remaining", None)
            if event_seconds is None:
                event = event.next_event
                continue

            if event_seconds >= start_seconds - 0.001:
                event = event.next_event
                continue

            if isinstance(event, Substitution):
                team_id = getattr(event, "team_id", None)
                incoming = getattr(event, "incoming_player_id", None)
                if team_id is not None and incoming is not None:
                    result.setdefault(team_id, set()).add(incoming)

            event = event.next_event

        return result

    def _fill_missing_starters_from_previous_period_end(self, starters_by_team):
        """
        Fill in missing period starters using the previous period's ending lineup.

        This handles the case where a player who started the period wasn't detected
        because they had no events (e.g., they were immediately subbed out or had
        no stats recorded).

        The method accounts for period-start substitutions: if a player was subbed
        OUT at period start, they shouldn't be added as a missing starter (they
        were replaced). If a player was subbed IN at period start, they shouldn't
        cause the subset check to fail (they're a valid new addition).
        """
        prev_lineups = getattr(self, "previous_period_end_lineups", None)
        if not isinstance(prev_lineups, dict):
            return starters_by_team
        prev_period = getattr(self, "previous_period_end_period", None)
        if prev_period != self.period - 1:
            return starters_by_team

        period_start_subs = self._get_period_start_substitutions()
        later_period_sub_ins = self._get_later_period_sub_in_players()

        for team_id, prev_players in prev_lineups.items():
            if not isinstance(prev_players, list) or len(prev_players) != 5:
                continue
            cur = starters_by_team.get(team_id, [])
            if not cur:
                continue
            if len(cur) >= 5:
                continue

            cur_set = set(cur)
            prev_set = set(prev_players)
            team_subs = period_start_subs.get(team_id, {"in": set(), "out": set()})
            subbed_in_at_start = team_subs["in"]
            subbed_out_at_start = team_subs["out"]

            implied_carryover = (cur_set - subbed_in_at_start) | subbed_out_at_start

            missing = [
                player
                for player in prev_players
                if player not in cur_set and player not in subbed_out_at_start
            ]
            need = 5 - len(cur)
            if need <= 0:
                continue

            if implied_carryover.issubset(prev_set):
                fill_candidates = missing
            else:
                # Relax the subset gate only when the remaining carryover
                # candidates are uniquely identifiable after excluding players
                # who later have an explicit sub-in event in the same period.
                later_sub_ins = later_period_sub_ins.get(team_id, set())
                fill_candidates = [
                    player for player in missing if player not in later_sub_ins
                ]
                if len(fill_candidates) != need:
                    continue

            filled = cur + fill_candidates[:need]
            seen = set()
            starters_by_team[team_id] = [
                player for player in filled if not (player in seen or seen.add(player))
            ]
        return starters_by_team

    def _get_period_starters_from_period_events(
        self, file_directory, ignore_missing_starters=False
    ):
        starters, player_team_map, player_first_seen_order, subbed_in_players = (
            self._get_players_who_started_period_with_team_map()
        )
        known_team_ids = self._get_known_team_ids_for_period()
        starters = [
            player_id for player_id in starters if player_id not in subbed_in_players
        ]

        starters_by_team = self._split_up_starters_by_team(starters, player_team_map)
        starters_by_team = self._fill_missing_starters_from_previous_period_end(
            starters_by_team
        )
        if ignore_missing_starters:
            starters_by_team = self._trim_excess_starters(
                starters_by_team, player_first_seen_order, known_team_ids
            )
        starters_by_team = self._apply_period_starter_overrides(
            starters_by_team, file_directory
        )
        if not ignore_missing_starters:
            self._check_both_teams_have_5_starters(starters_by_team, file_directory)

        return starters_by_team

    @property
    def event_stats(self):
        """
        returns list of dicts with all stats for event
        """
        return self.base_stats
