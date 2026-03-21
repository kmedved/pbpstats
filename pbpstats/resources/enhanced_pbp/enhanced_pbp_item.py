"""
``EnhancedPbpItem`` is an abstract base class for all enhanced pbp event types
"""
import abc
import logging

import pbpstats
from pbpstats.resources.enhanced_pbp import FieldGoal, Foul, FreeThrow, Rebound

logger = logging.getLogger(__name__)


class EnhancedPbpItem(metaclass=abc.ABCMeta):
    def __repr__(self):
        return f"<{type(self).__name__} GameId: {self.game_id}, Description: {self.description}, Time: {self.clock}, EventNum: {self.event_num}>"

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

    def _copy_players_dict(self, players):
        copied_players = {}
        for team_id, team_players in getattr(players, "items", lambda: [])():
            if isinstance(team_players, (list, tuple, set)):
                copied_players[team_id] = list(team_players)
            else:
                copied_players[team_id] = team_players
        return copied_players

    @property
    def _same_clock_pre_sub_actor_ids(self):
        return []

    def _is_period_start_clock(self):
        period = int(getattr(self, "period", 0) or 0)
        if period <= 0:
            return False
        expected_clock = "5:00" if period > 4 else "12:00"
        return str(getattr(self, "clock", "") or "") == expected_clock

    def _same_clock_pre_sub_lineup_for_actor(self, actor_id):
        if actor_id in [None, 0, "0"] or self._is_period_start_clock():
            return None
        current_players = self._copy_players_dict(self._raw_current_players)
        if not current_players:
            return None
        try:
            same_clock_events = self.get_all_events_at_current_time()
        except Exception:
            return None
        prior_subs = sorted(
            [
                event
                for event in same_clock_events
                if getattr(event, "event_type", None) == 8
                and getattr(event, "order", -1) < getattr(self, "order", -1)
            ],
            key=lambda event: event.order,
            reverse=True,
        )
        for sub_event in prior_subs:
            if getattr(sub_event, "outgoing_player_id", 0) != actor_id:
                continue
            team_id = getattr(sub_event, "team_id", None)
            if team_id not in current_players:
                continue
            pre_sub_players = self._copy_players_dict(sub_event._get_previous_raw_players())
            if team_id not in pre_sub_players:
                continue
            current_players[team_id] = list(pre_sub_players[team_id])
            return current_players
        return None

    def _same_clock_current_players_override(self):
        for actor_id in self._same_clock_pre_sub_actor_ids:
            override_players = self._same_clock_pre_sub_lineup_for_actor(actor_id)
            if override_players is not None:
                return override_players
        return None

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
        override_players = self._same_clock_current_players_override()
        if override_players is not None:
            return self._apply_lineup_overrides(override_players)
        return self._apply_lineup_overrides(self._raw_current_players)

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
        lineup_ids = {}
        for team_id, team_players in self.current_players.items():
            players = [str(player_id) for player_id in team_players]
            sorted_player_ids = sorted(players)
            lineup_id = "-".join(sorted_player_ids)
            lineup_ids[team_id] = lineup_id
        return lineup_ids

    @property
    def seconds_since_previous_event(self):
        """
        returns the number of seconds that have elapsed since the previous event
        """
        if self.previous_event is None:
            return 0
        prev_period = getattr(self.previous_event, "period", None)
        if self.seconds_remaining == 720 and prev_period != self.period:
            # Between-period subs (live data) where previous event is from a
            # different period.  Without this guard the result would be negative
            # (e.g. 0 - 720 = -720).
            return 0
        if self.seconds_remaining == 300 and self.period > 4 and prev_period != self.period:
            # Same guard for overtime period boundaries.
            return 0
        return self.previous_event.seconds_remaining - self.seconds_remaining

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
        if self.seconds_since_previous_event != 0:
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
                            "stat_value": self.seconds_since_previous_event,
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
