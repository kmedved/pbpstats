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

    def _get_starters_from_boxscore_request(self):
        """
        makes request to boxscore url for time from period start to first event to get period starters
        """
        base_url = (
            f"https://stats.{self.league_url_part}.com/stats/boxscoretraditionalv2"
        )
        event = self
        while event is not None and event.seconds_remaining == self.seconds_remaining:
            event = event.next_event
        seconds_to_first_event = self.seconds_remaining - event.seconds_remaining

        if self.league == WNBA_STRING:
            seconds_in_period = 6000
        else:
            seconds_in_period = 7200

        if self.period == 1:
            start_range = 0
        elif self.period <= 4:
            start_range = int(seconds_in_period * (self.period - 1))
        else:
            start_range = int(4 * seconds_in_period + 3000 * (self.period - 5))
        end_range = int(start_range + seconds_to_first_event * 10)
        params = {
            "GameId": self.game_id,
            "StartPeriod": 0,
            "EndPeriod": 0,
            "RangeType": 2,
            "StartRange": start_range,
            "EndRange": end_range,
        }
        starters_by_team = {}
        response = requests.get(
            base_url, params, headers=HEADERS, timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            response_json = response.json()
        else:
            response.raise_for_status()

        headers = response_json["resultSets"][0]["headers"]
        rows = response_json["resultSets"][0]["rowSet"]
        players = [dict(zip(headers, row)) for row in rows]
        starters = sorted(
            players, key=lambda k: int(k["MIN"].split(":")[1]), reverse=True
        )

        if len(starters) < 10:
            raise InvalidNumberOfStartersException(
                f"GameId: {self.game_id}, Period: {self.period}, Starters: {starters}"
            )

        for starter in starters[0:10]:
            team_id = starter["TEAM_ID"]
            player_id = starter["PLAYER_ID"]
            if team_id not in starters_by_team.keys():
                starters_by_team[team_id] = []
            starters_by_team[team_id].append(player_id)

        for team_id, starters in starters_by_team.items():
            if len(starters) != 5:
                raise InvalidNumberOfStartersException(
                    f"GameId: {self.game_id}, Period: {self.period}, TeamId: {team_id}, Players: {starters}"
                )

        return starters_by_team

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
        # explicit sub-in happens earlier in game time than the player's first
        # recorded starter-like event, treat them as a sub, not a starter.
        for player_id, sub_seconds_remaining in player_first_sub_in_seconds_remaining.items():
            first_seen_seconds_remaining = player_first_seen_seconds_remaining.get(player_id)
            if first_seen_seconds_remaining is None:
                continue
            if sub_seconds_remaining > first_seen_seconds_remaining + 0.001:
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

        if self.period <= 4:
            if self.league == WNBA_STRING:
                start_seconds = 600.0
            else:
                start_seconds = 720.0
        else:
            start_seconds = 300.0

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

            if not implied_carryover.issubset(prev_set):
                continue
            missing = [
                player
                for player in prev_players
                if player not in cur_set and player not in subbed_out_at_start
            ]
            need = 5 - len(cur)
            if need <= 0:
                continue
            filled = cur + missing[:need]
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
