"""
``EnhancedPbpItem`` is an abstract base class for all enhanced pbp event types
"""

import abc
import logging

import pbpstats
from pbpstats.game_id import normalize_game_id, uses_wnba_twenty_minute_halves
from pbpstats.resources.enhanced_pbp import FieldGoal, Foul, FreeThrow, Rebound

logger = logging.getLogger(__name__)


class IncompleteEventStatsContextError(RuntimeError):
    """
    Raised when an event cannot emit fully keyed stat rows.
    """


class EnhancedPbpItem(metaclass=abc.ABCMeta):
    def __repr__(self):
        return (
            f"<{type(self).__name__} "
            f"GameId: {getattr(self, 'game_id', 'unknown')}, "
            f"Description: {getattr(self, 'description', '')}, "
            f"Time: {getattr(self, 'clock', 'unknown')}, "
            f"EventNum: {getattr(self, 'event_num', 'unknown')}>"
        )

    @abc.abstractproperty
    def is_possession_ending_event(self):
        """
        returns True if event ends a possession, False otherwise
        """
        pass

    @abc.abstractproperty
    def event_stats(self):
        """
        returns list of dicts with all stats for event
        """
        pass

    @abc.abstractmethod
    def get_offense_team_id(self):
        """
        returns team id for team on offense for event
        """
        pass

    @abc.abstractproperty
    def seconds_remaining(self):
        """
        returns seconds remaining in period as a ``float``
        """
        pass

    @property
    def shot_clock(self):
        """
        Approximate shot clock (seconds remaining) at the start of this event,
        or ``None`` if it has not been annotated.

        Populated by NbaEnhancedPbpLoader via the shot_clock annotator.
        """
        return getattr(self, "_shot_clock", None)

    @shot_clock.setter
    def shot_clock(self, value):
        self._shot_clock = value
        # Also expose under a public name so event.data["shot_clock"] works
        self.__dict__["shot_clock"] = value

    @property
    def shot_clock_bucket(self):
        """
        Coarse bucket of shot clock time:
          - 'Early'    : 16+ seconds
          - 'Middle'   : 8–15.9 seconds
          - 'Late'     : 4–7.9 seconds
          - 'VeryLate' : 0–3.9 seconds
          - None       : shot clock not available
        """
        sc = self.shot_clock
        if sc is None:
            return None
        if sc >= 16:
            return "Early"
        if sc >= 8:
            return "Middle"
        if sc >= 4:
            return "Late"
        return "VeryLate"

    @property
    def base_stats(self):
        """
        returns list of dicts with all seconds played and possession count stats for event
        """
        return (
            self._get_seconds_played_stats_items()
            + self._get_possessions_played_stats_items()
        )

    def get_all_events_at_current_time(self):
        """
        returns list of all events that take place as the same time as the current event
        """
        events = [self]
        # going backwards
        event = self
        while event is not None and self.seconds_remaining == event.seconds_remaining:
            if event != self:
                events.append(event)
            event = event.previous_event
        # going forwards
        event = self
        while event is not None and self.seconds_remaining == event.seconds_remaining:
            if event != self:
                events.append(event)
            event = event.next_event
        return sorted(events, key=lambda k: k.order)

    def _get_previous_raw_players(self):
        prev = getattr(self, "previous_event", None)
        if prev is None:
            logger.debug(
                "No previous_event for %r (game_id=%s); returning empty current_players.",
                self,
                getattr(self, "game_id", "unknown"),
            )
            return {}
        try:
            return prev._raw_current_players
        except AttributeError:
            try:
                return prev.current_players
            except Exception as e:
                logger.debug(
                    "Error walking current_players chain for %r (game_id=%s): %s; "
                    "returning empty current_players.",
                    self,
                    getattr(self, "game_id", "unknown"),
                    e,
                )
                return {}
        except Exception as e:
            logger.debug(
                "Error walking current_players chain for %r (game_id=%s): %s; "
                "returning empty current_players.",
                self,
                getattr(self, "game_id", "unknown"),
                e,
            )
            return {}

    @property
    def _raw_current_players(self):
        return self._get_previous_raw_players()

    def _apply_lineup_overrides(self, players):
        overrides = getattr(self, "lineup_override_by_team", None)
        if not overrides:
            return players

        updated_players = {}
        for team_id, team_players in getattr(players, "items", lambda: [])():
            if isinstance(team_players, (list, tuple, set)):
                updated_players[team_id] = list(team_players)
            else:
                updated_players[team_id] = team_players

        for team_id, team_players in overrides.items():
            if isinstance(team_players, (list, tuple, set)):
                updated_players[team_id] = list(team_players)
            else:
                updated_players[team_id] = team_players
        return updated_players

    @property
    def current_players(self):
        """
        returns dict with list of player ids for each team
        with players on the floor for current event

        For all non subsitution events current players are just
        the same as previous event

        This gets overwritten in :obj:`~pbpstats.resources.enhanced_pbp.substitution.Substitution`
        since those are the only event types where players are not the same as the previous event
        """
        return self._apply_lineup_overrides(self._raw_current_players)

    @staticmethod
    def _lineup_ids_for_players(players_by_team):
        lineup_ids = {}
        for team_id, team_players in players_by_team.items():
            players = [str(player_id) for player_id in team_players]
            lineup_ids[team_id] = "-".join(sorted(players))
        return lineup_ids

    def _resolve_event_stat_context(
        self,
        *,
        current_players=None,
        lineup_ids=None,
        context_name="current_players",
    ):
        players_by_team = (
            self.current_players if current_players is None else current_players
        )
        if not isinstance(players_by_team, dict):
            raise IncompleteEventStatsContextError(
                f"{type(self).__name__} requires dict {context_name}, got "
                f"{type(players_by_team).__name__}"
            )

        team_ids = list(players_by_team.keys())
        if len(team_ids) != 2:
            raise IncompleteEventStatsContextError(
                f"{type(self).__name__} requires exactly 2 teams in {context_name}, "
                f"got {len(team_ids)} for game_id={getattr(self, 'game_id', 'unknown')}"
            )

        resolved_lineup_ids = (
            self._lineup_ids_for_players(players_by_team)
            if lineup_ids is None
            else dict(lineup_ids)
        )
        missing_lineup_ids = [
            team_id for team_id in team_ids if team_id not in resolved_lineup_ids
        ]
        if missing_lineup_ids:
            raise IncompleteEventStatsContextError(
                f"{type(self).__name__} missing lineup ids for teams {missing_lineup_ids} "
                f"in {context_name} for game_id={getattr(self, 'game_id', 'unknown')}"
            )

        return players_by_team, team_ids, resolved_lineup_ids

    def _add_event_stat_context(
        self,
        stats,
        *,
        current_players=None,
        lineup_ids=None,
        context_name="current_players",
    ):
        if not stats:
            return stats

        players_by_team, team_ids, resolved_lineup_ids = (
            self._resolve_event_stat_context(
                current_players=current_players,
                lineup_ids=lineup_ids,
                context_name=context_name,
            )
        )
        for stat in stats:
            missing_keys = [
                key
                for key in ("player_id", "team_id", "stat_key", "stat_value")
                if key not in stat
            ]
            if missing_keys:
                raise IncompleteEventStatsContextError(
                    f"{type(self).__name__} emitted stat row missing keys {missing_keys} "
                    f"for game_id={getattr(self, 'game_id', 'unknown')}"
                )

            team_id = stat["team_id"]
            if team_id not in players_by_team:
                raise IncompleteEventStatsContextError(
                    f"{type(self).__name__} emitted stat row for unknown team_id={team_id} "
                    f"in {context_name} for game_id={getattr(self, 'game_id', 'unknown')}"
                )
            opponent_team_id = team_ids[0] if team_id == team_ids[1] else team_ids[1]
            stat["opponent_team_id"] = opponent_team_id
            stat["lineup_id"] = resolved_lineup_ids[team_id]
            stat["opponent_lineup_id"] = resolved_lineup_ids[opponent_team_id]
        return stats

    def _require_team_in_event_stat_context(
        self,
        team_id,
        *,
        current_players=None,
        lineup_ids=None,
        context_name="current_players",
    ):
        players_by_team, team_ids, resolved_lineup_ids = (
            self._resolve_event_stat_context(
                current_players=current_players,
                lineup_ids=lineup_ids,
                context_name=context_name,
            )
        )
        if team_id not in players_by_team:
            raise IncompleteEventStatsContextError(
                f"{type(self).__name__} team_id={team_id} missing from {context_name} "
                f"for game_id={getattr(self, 'game_id', 'unknown')}"
            )
        opponent_team_id = team_ids[0] if team_id == team_ids[1] else team_ids[1]
        return players_by_team, team_ids, resolved_lineup_ids, opponent_team_id

    @property
    def score_margin(self):
        """
        returns the score margin from perspective of offense team before the event took place
        """
        if self.previous_event is None:
            score = self.score
        else:
            score = self.previous_event.score
        offense_team_id = self.get_offense_team_id()
        offense_points = score[offense_team_id]
        defense_points = 0
        for team_id, points in score.items():
            if team_id != offense_team_id:
                defense_points = points
        return offense_points - defense_points

    @property
    def lineup_ids(self):
        """
        returns dict with lineup ids for each team for current event.
        Lineup ids are hyphen separated sorted player id strings.
        """
        return self._lineup_ids_for_players(self.current_players)

    @property
    def seconds_since_previous_event(self):
        """
        returns the number of seconds that have elapsed since the previous event
        """
        if self.previous_event is None:
            return 0
        prev_period = getattr(self.previous_event, "period", None)
        if prev_period != self.period and self._is_at_period_start_clock():
            # Between-period subs or start markers where previous_event is from a
            # different period. Without this guard the result would be negative.
            return 0
        deferred_elapsed = self._elapsed_from_deferred_duplicate_clock_backtrack()
        if deferred_elapsed is not None:
            return deferred_elapsed
        if self._defers_elapsed_to_later_duplicate_clock_backtrack():
            return 0
        previous_clock = self._period_elapsed_watermark_seconds()
        if previous_clock is None:
            previous_clock = getattr(self.previous_event, "seconds_remaining", None)
        if previous_clock is None:
            return 0
        elapsed = previous_clock - self.seconds_remaining
        return max(elapsed, 0)

    @staticmethod
    def _event_seconds_remaining(event):
        try:
            return float(event.seconds_remaining)
        except (TypeError, ValueError, AttributeError):
            return None

    @staticmethod
    def _same_clock(first, second):
        if first is None or second is None:
            return False
        return abs(first - second) <= 0.001

    def _defers_elapsed_to_later_duplicate_clock_backtrack(self):
        """
        True when this event is an early duplicate lower-clock anchor.

        Some feeds insert an event at a lower clock, jump back to a higher clock
        for same-deadball administration, and then return to the same lower
        clock. The elapsed segment should be credited once, at the later
        duplicate where the post-admin lineup is known.
        """
        current_seconds = self._event_seconds_remaining(self)
        previous_seconds = self._event_seconds_remaining(self.previous_event)
        if current_seconds is None or previous_seconds is None:
            return False
        if self._elapsed_under_period_watermark(self) <= 0:
            return False

        event = getattr(self, "next_event", None)
        saw_clock_backtrack = False
        while event is not None:
            if getattr(event, "period", None) != self.period:
                return False
            event_seconds = self._event_seconds_remaining(event)
            if event_seconds is None:
                event = getattr(event, "next_event", None)
                continue
            if event_seconds < current_seconds - 0.001:
                return False
            if event_seconds > current_seconds + 0.001:
                saw_clock_backtrack = True
            elif saw_clock_backtrack and self._same_clock(event_seconds, current_seconds):
                return True
            event = getattr(event, "next_event", None)
        return False

    def _elapsed_from_deferred_duplicate_clock_backtrack(self):
        """
        Return elapsed seconds deferred by an earlier duplicate lower-clock event.
        """
        current_seconds = self._event_seconds_remaining(self)
        if current_seconds is None:
            return None

        event = getattr(self, "previous_event", None)
        saw_clock_backtrack = False
        while event is not None:
            if getattr(event, "period", None) != self.period:
                return None
            event_seconds = self._event_seconds_remaining(event)
            if event_seconds is None:
                event = getattr(event, "previous_event", None)
                continue
            if event_seconds < current_seconds - 0.001:
                return None
            if event_seconds > current_seconds + 0.001:
                saw_clock_backtrack = True
            elif saw_clock_backtrack and self._same_clock(event_seconds, current_seconds):
                return self._elapsed_under_period_watermark(event)
            event = getattr(event, "previous_event", None)
        return None

    def _elapsed_under_period_watermark(self, event):
        current_seconds = self._event_seconds_remaining(event)
        if current_seconds is None:
            return 0

        previous_clock = self._period_elapsed_watermark_seconds_for_event(event)
        if previous_clock is None:
            previous_clock = self._event_seconds_remaining(
                getattr(event, "previous_event", None)
            )
        if previous_clock is None:
            return 0
        return max(previous_clock - current_seconds, 0)

    def _period_elapsed_watermark_seconds(self):
        return self._period_elapsed_watermark_seconds_for_event(self)

    @classmethod
    def _period_elapsed_watermark_seconds_for_event(cls, event):
        """
        Return the lowest clock already reached in this event's period.

        Some feeds can insert same-period events after the play stream has
        already advanced to a lower clock. Crediting from the immediate previous
        event would double count the repeated interval; using the lowest prior
        clock keeps seconds played monotonic within a period.
        """
        period = getattr(event, "period", None)
        previous_event = getattr(event, "previous_event", None)
        prior_seconds_remaining = []
        while previous_event is not None:
            if getattr(previous_event, "period", None) != period:
                break
            try:
                prior_seconds_remaining.append(float(previous_event.seconds_remaining))
            except (TypeError, ValueError, AttributeError):
                pass
            previous_event = getattr(previous_event, "previous_event", None)
        if not prior_seconds_remaining:
            return None
        return min(prior_seconds_remaining)

    def _normalize_game_id_for_inference(self):
        return normalize_game_id(
            getattr(self, "game_id", ""),
            league=getattr(self, "loader_league", None),
        )

    def _infer_league_from_game_id(self):
        game_id = self._normalize_game_id_for_inference()
        if game_id.startswith(pbpstats.NBA_GAME_ID_PREFIX):
            return pbpstats.NBA_STRING
        if game_id.startswith(pbpstats.WNBA_GAME_ID_PREFIX):
            return pbpstats.WNBA_STRING
        if game_id.startswith(pbpstats.G_LEAGUE_GAME_ID_PREFIX):
            return pbpstats.G_LEAGUE_STRING
        return None

    def _infer_season_year_from_game_id(self):
        game_id = self._normalize_game_id_for_inference()
        if len(game_id) < 5:
            return None
        try:
            suffix = int(game_id[3:5])
        except ValueError:
            return None
        return 2000 + suffix if suffix < 90 else 1900 + suffix

    def _period_start_seconds(self):
        try:
            period = int(self.period)
        except (TypeError, ValueError):
            return 720.0

        league = (
            getattr(self, "loader_league", None)
            or self._infer_league_from_game_id()
            or pbpstats.NBA_STRING
        )
        season_year = self._infer_season_year_from_game_id()
        if uses_wnba_twenty_minute_halves(league, season_year):
            return 1200.0 if period <= 2 else 300.0
        if period > 4:
            return 300.0
        if league == pbpstats.WNBA_STRING:
            return 600.0
        return 720.0

    def _is_at_period_start_clock(self):
        try:
            seconds_remaining = float(self.seconds_remaining)
        except (TypeError, ValueError):
            return False
        return abs(seconds_remaining - self._period_start_seconds()) <= 0.001

    def is_second_chance_event(self):
        """
        returns True if the event takes place after an offensive rebound
        on the current possession, False otherwise
        """
        event = self.previous_event
        if isinstance(event, Rebound) and event.is_real_rebound and event.oreb:
            return True
        while not (event is None or event.is_possession_ending_event):
            if isinstance(event, Rebound) and event.is_real_rebound and event.oreb:
                return True
            event = event.previous_event
        return False

    def is_penalty_event(self):
        """
        returns True if the team on offense is in the penalty, False otherwise
        """
        if hasattr(self, "fouls_to_give"):
            current_players = self.current_players
            if len(current_players) < 2:
                return False
            team_ids = list(current_players.keys())
            offense_team_id = self.get_offense_team_id()
            defense_team_id = (
                team_ids[0] if offense_team_id == team_ids[1] else team_ids[1]
            )
            if self.fouls_to_give[defense_team_id] == 0:
                if isinstance(self, (Foul, FreeThrow, Rebound)):
                    # if foul or free throw or rebound on a missed ft
                    # check foul event and should return false is foul
                    # was shooting foul and team had a foul to give
                    if isinstance(self, Foul):
                        foul_event = self
                    elif isinstance(self, FreeThrow):
                        foul_event = self.foul_that_led_to_ft
                    else:
                        # if rebound is on missed ft, also need to look at foul that led to FT
                        if not self.oreb and isinstance(self.missed_shot, FreeThrow):
                            foul_event = self.missed_shot.foul_that_led_to_ft
                        else:
                            return True
                    if foul_event is None:
                        return True
                    fouls_to_give_prior_to_foul = (
                        foul_event.previous_event.fouls_to_give[defense_team_id]
                    )
                    if fouls_to_give_prior_to_foul > 0:
                        return False
                return True
        return False

    @property
    def count_as_possession(self):
        """
        returns True if event is possession changing event
        that should count as a real possession, False otherwise.

        In order to not include possessions which a very low probability of scoring in possession counts,
        possession won't be counted as a possession if it starts with <= 2 seconds left
        and no points are scored before period ends
        """
        if self.is_possession_ending_event:
            if self.seconds_remaining > 2:
                return True
            # check when previous possession ended
            prev_event = self.previous_event
            while prev_event is not None and not prev_event.is_possession_ending_event:
                prev_event = prev_event.previous_event
            if prev_event is None or prev_event.seconds_remaining > 2:
                return True
            # possession starts in final 2 seconds
            # return True if there is a FT or FGM between now and end of period
            next_event = prev_event.next_event
            while next_event is not None:
                if isinstance(next_event, FreeThrow) or (
                    isinstance(next_event, FieldGoal) and next_event.is_made
                ):
                    return True
                next_event = next_event.next_event
        return False

    def _get_seconds_played_stats_items(self):
        """
        makes event stats items for:
        - seconds played
        - seconds played for number of fouls
        - second chance seconds played
        - penalty seconds played
        """
        stat_items = []
        current_players = self.current_players
        previous_players = getattr(self.previous_event, "current_players", {})
        if len(current_players) < 2 or len(previous_players) < 2:
            return stat_items
        team_ids = list(current_players.keys())
        offense_team_id = self.get_offense_team_id()
        is_penalty_event = self.is_penalty_event()
        is_second_chance_event = self.is_second_chance_event()
        seconds_since_previous_event = self.seconds_since_previous_event
        if seconds_since_previous_event != 0:
            for team_id, players in previous_players.items():
                seconds_stat_key = (
                    pbpstats.SECONDS_PLAYED_OFFENSE_STRING
                    if team_id == offense_team_id
                    else pbpstats.SECONDS_PLAYED_DEFENSE_STRING
                )
                opponent_team_id = (
                    team_ids[0] if team_id == team_ids[1] else team_ids[1]
                )
                previous_poss_lineup_ids = self.previous_event.lineup_ids
                for player_id in players:
                    keys_to_add = [seconds_stat_key]
                    player_fouls = self.previous_event.player_game_fouls[player_id]
                    period = self.period if self.period <= 4 else "OT"
                    foul_tracking_seconds_stat_key = (
                        f"Period{period}Fouls{player_fouls}{seconds_stat_key}"
                    )
                    keys_to_add.append(foul_tracking_seconds_stat_key)
                    if is_second_chance_event:
                        seconds_chance_seconds_stat_key = (
                            f"{pbpstats.SECOND_CHANCE_STRING}{seconds_stat_key}"
                        )
                        keys_to_add.append(seconds_chance_seconds_stat_key)
                    if is_penalty_event:
                        penalty_seconds_stat_key = (
                            f"{pbpstats.PENALTY_STRING}{seconds_stat_key}"
                        )
                        keys_to_add.append(penalty_seconds_stat_key)
                    for stat_key in keys_to_add:
                        stat_item = {
                            "player_id": player_id,
                            "team_id": team_id,
                            "opponent_team_id": opponent_team_id,
                            "lineup_id": previous_poss_lineup_ids[team_id],
                            "opponent_lineup_id": previous_poss_lineup_ids[
                                opponent_team_id
                            ],
                            "stat_key": stat_key,
                            "stat_value": seconds_since_previous_event,
                        }
                        stat_items.append(stat_item)
        return stat_items

    def _get_possessions_played_stats_items(self):
        """
        makes event stats items for:
        - possessions played
        - second chance possessions played
        - penalty possessions played
        """
        stat_items = []
        current_players = self.current_players
        if len(current_players) < 2:
            return stat_items
        team_ids = list(current_players.keys())
        offense_team_id = self.get_offense_team_id()
        is_penalty_event = self.is_penalty_event()
        is_second_chance_event = self.is_second_chance_event()
        if self.count_as_possession:
            if isinstance(self, FreeThrow):
                current_players = self.event_for_efficiency_stats.current_players
                lineup_ids = self.event_for_efficiency_stats.lineup_ids
            else:
                current_players = self.current_players
                lineup_ids = self.lineup_ids
            for team_id, players in current_players.items():
                possessions_stat_key = (
                    pbpstats.OFFENSIVE_POSSESSION_STRING
                    if team_id == offense_team_id
                    else pbpstats.DEFENSIVE_POSSESSION_STRING
                )
                opponent_team_id = (
                    team_ids[0] if team_id == team_ids[1] else team_ids[1]
                )
                for player_id in players:
                    keys_to_add = [possessions_stat_key]
                    if is_second_chance_event:
                        seconds_chance_possessions_stat_key = (
                            f"{pbpstats.SECOND_CHANCE_STRING}{possessions_stat_key}"
                        )
                        keys_to_add.append(seconds_chance_possessions_stat_key)
                    if is_penalty_event:
                        penalty_possessions_stat_key = (
                            f"{pbpstats.PENALTY_STRING}{possessions_stat_key}"
                        )
                        keys_to_add.append(penalty_possessions_stat_key)
                    for stat_key in keys_to_add:
                        stat_item = {
                            "player_id": player_id,
                            "team_id": team_id,
                            "opponent_team_id": opponent_team_id,
                            "lineup_id": lineup_ids[team_id],
                            "opponent_lineup_id": lineup_ids[opponent_team_id],
                            "stat_key": stat_key,
                            "stat_value": 1,
                        }
                        stat_items.append(stat_item)

        return stat_items
