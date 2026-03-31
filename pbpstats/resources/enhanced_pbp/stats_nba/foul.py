from pbpstats.resources.enhanced_pbp import Foul
from pbpstats.resources.enhanced_pbp.stats_nba.enhanced_pbp_item import (
    StatsEnhancedPbpItem,
)


class StatsFoul(Foul, StatsEnhancedPbpItem):
    """
    Class for foul events
    """

    event_type = 6

    def __init__(self, *args):
        super().__init__(*args)

    def _get_malformed_team_person_id(self):
        player1_id = getattr(self, "player1_id", 0)
        if player1_id not in [0, None, "0"]:
            return player1_id
        team_id = getattr(self, "team_id", None)
        if team_id in [0, None, "0"]:
            return None
        return team_id

    @staticmethod
    def _coerce_int(value):
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _get_opposite_team_id(team_ids, team_id):
        if len(team_ids) != 2 or team_id not in team_ids:
            return None
        return team_ids[0] if team_ids[1] == team_id else team_ids[1]

    @staticmethod
    def _next_event_across_periods(event):
        if event is None:
            return None
        return getattr(event, "next_event_any_period", getattr(event, "next_event", None))

    @staticmethod
    def _is_technical_foul_event(event):
        return getattr(event, "event_type", None) == 6 and (
            getattr(event, "is_technical", False)
            or getattr(event, "is_double_technical", False)
        )

    @staticmethod
    def _is_technical_anchor_foul_event(event):
        return getattr(event, "event_type", None) == 6 and (
            getattr(event, "is_technical", False)
            or getattr(event, "is_double_technical", False)
            or getattr(event, "is_defensive_3_seconds", False)
        )

    @staticmethod
    def _is_boundary_admin_event(event):
        return getattr(event, "event_type", None) in {8, 9, 11, 12, 13, 18}

    def _resolve_same_clock_paired_technical_team_id(self, team_ids):
        valid_team_ids = set()
        for direction in ("previous_event", "next_event"):
            event = getattr(self, direction, None)
            while event is not None and getattr(event, "clock", None) == self.clock:
                if self._is_technical_anchor_foul_event(event):
                    event_team_id = getattr(event, "team_id", None)
                    if event_team_id in team_ids:
                        valid_team_ids.add(event_team_id)
                event = getattr(event, direction, None)
        if len(valid_team_ids) != 1:
            return None
        return self._get_opposite_team_id(team_ids, next(iter(valid_team_ids)))

    def _resolve_boundary_cluster_technical_team_id(self, team_ids):
        if self._coerce_int(getattr(self, "period", None)) is None:
            return None
        if getattr(self, "clock", None) != "0:00":
            return None

        current_period = self._coerce_int(self.period)
        next_period = current_period + 1
        technical_ft_team_ids = set()
        saw_next_period_cluster = False
        event = self._next_event_across_periods(self)

        while event is not None:
            event_period = self._coerce_int(getattr(event, "period", None))
            if event_period not in {current_period, next_period}:
                break

            if getattr(event, "event_type", None) == 3 and getattr(
                event, "is_technical_ft", False
            ):
                if event_period != next_period:
                    break
                shooter_team_id = getattr(event, "team_id", None)
                if shooter_team_id not in team_ids:
                    return None
                saw_next_period_cluster = True
                technical_ft_team_ids.add(shooter_team_id)
                event = self._next_event_across_periods(event)
                continue

            if self._is_boundary_admin_event(event):
                if event_period == next_period:
                    saw_next_period_cluster = True
                event = self._next_event_across_periods(event)
                continue

            if self._is_technical_foul_event(event):
                if event_period == next_period:
                    saw_next_period_cluster = True
                event = self._next_event_across_periods(event)
                continue

            break

        if not saw_next_period_cluster or len(technical_ft_team_ids) != 1:
            return None
        return self._get_opposite_team_id(team_ids, next(iter(technical_ft_team_ids)))

    def _resolve_technical_team_id(self):
        team_id = getattr(self, "team_id", None)
        current_players = getattr(self, "current_players", {})
        team_ids = list(current_players.keys())
        if team_id in current_players:
            return team_id

        if not (self.is_technical or self.is_double_technical):
            return None

        candidate_person_id = self._get_malformed_team_person_id()
        if candidate_person_id not in [0, None, "0"]:
            for current_team_id, players in current_players.items():
                if candidate_person_id in players:
                    return current_team_id

        linked_ft = self._get_linked_technical_free_throw()
        if (
            linked_ft is not None
            and getattr(linked_ft, "team_id", None) in current_players
            and len(team_ids) == 2
        ):
            return team_ids[0] if team_ids[1] == linked_ft.team_id else team_ids[1]

        paired_team_id = self._resolve_same_clock_paired_technical_team_id(team_ids)
        if paired_team_id is not None:
            return paired_team_id

        boundary_team_id = self._resolve_boundary_cluster_technical_team_id(team_ids)
        if boundary_team_id is not None:
            return boundary_team_id

        player2_team_id = getattr(self, "player2_team_id", None)
        if (
            self.is_double_technical
            and player2_team_id in current_players
            and len(team_ids) == 2
        ):
            return team_ids[0] if team_ids[1] == player2_team_id else team_ids[1]

        player2_id = getattr(self, "player3_id", 0)
        if self.is_double_technical and player2_id not in [0, None, "0"] and len(team_ids) == 2:
            for current_team_id, players in current_players.items():
                if player2_id in players:
                    return team_ids[0] if team_ids[1] == current_team_id else team_ids[1]

        return None

    def _is_unresolved_source_limited_technical(self):
        current_players = getattr(self, "current_players", {})
        if not (self.is_technical or self.is_double_technical):
            return False
        if getattr(self, "team_id", None) in current_players:
            return False
        return self._resolve_technical_team_id() is None

    @property
    def event_stats(self):
        # Some legacy stats.nba rows are foul events with no valid committing
        # team or player. Treat them as unattributable source corruption and
        # preserve only base stats instead of raising during lineup attachment.
        if getattr(self, "team_id", 0) in [0, None, "0"] and getattr(
            self, "player1_id", 0
        ) in [0, None, "0"]:
            self._log_source_limited_guard("source_limited_bench_technical_no_team")
            return self.base_stats
        if self._is_unresolved_source_limited_technical():
            self._log_source_limited_guard("source_limited_bench_technical_no_team")
            return self.base_stats
        return super().event_stats

    def _get_linked_technical_free_throw(self):
        for direction in ("previous_event", "next_event"):
            event = getattr(self, direction, None)
            while event is not None and getattr(event, "clock", None) == self.clock:
                if getattr(event, "is_technical_ft", False):
                    return event
                event = getattr(event, direction, None)
        return None

    @property
    def event_stat_team_id(self):
        resolved_team_id = self._resolve_technical_team_id()
        if resolved_team_id is not None:
            return resolved_team_id
        return getattr(self, "team_id", None)

    @property
    def number_of_fta_for_foul(self):
        """
        returns the number of free throws resulting from the foul
        """
        clock = self.clock
        event = self
        while (
            event is not None
            and event.clock == clock
            and not (
                hasattr(event, "is_first_ft")
                and not event.is_technical_ft
                and self.team_id != event.team_id
            )
        ):
            event = event.next_event

        if (
            event is not None
            and hasattr(event, "is_first_ft")
            and not event.is_technical_ft
            and event.clock == clock
            and (not hasattr(self, "player3_id") or self.player3_id == event.player1_id)
        ):
            # player3 id check is to make sure player who got fouled is player shooting free throws, prior to 2005-06 because foul drawning player isn't in pbp
            if "of 1" in event.description:
                return 1
            elif "of 2" in event.description:
                return 2
            elif "of 3" in event.description:
                return 3

        # if we haven't found ft yet, try going backwards
        event = self
        while (
            event is not None
            and event.clock == clock
            and not (
                hasattr(event, "is_first_ft")
                and not event.is_technical_ft
                and self.team_id != event.team_id
            )
        ):
            event = event.previous_event

        if (
            event is not None
            and hasattr(event, "is_first_ft")
            and not event.is_technical_ft
            and event.clock == clock
            and (not hasattr(self, "player3_id") or self.player3_id == event.player1_id)
        ):
            # player3 id check is to make sure player who got fouled is player shooting free throws, prior to 2005-06 because foul drawning player isn't in pbp
            if "of 1" in event.description:
                return 1
            elif "of 2" in event.description:
                return 2
            elif "of 3" in event.description:
                return 3
        return None

    @property
    def is_personal_foul(self):
        return self.event_action_type in [1, 7, 8]

    @property
    def is_shooting_foul(self):
        return self.event_action_type == 2

    @property
    def is_loose_ball_foul(self):
        return self.event_action_type == 3

    @property
    def is_offensive_foul(self):
        return self.event_action_type == 4

    @property
    def is_inbound_foul(self):
        return self.event_action_type == 5

    @property
    def is_away_from_play_foul(self):
        return self.event_action_type == 6

    @property
    def is_clear_path_foul(self):
        return self.event_action_type == 9

    @property
    def is_double_foul(self):
        return self.event_action_type == 10

    @property
    def is_technical(self):
        return self.event_action_type in [11, 12, 13, 18, 19, 25, 30]

    @property
    def is_flagrant1(self):
        return self.event_action_type == 14

    @property
    def is_flagrant2(self):
        return self.event_action_type == 15

    @property
    def is_double_technical(self):
        return self.event_action_type == 16

    @property
    def is_defensive_3_seconds(self):
        return self.event_action_type == 17

    @property
    def is_delay_of_game(self):
        return self.event_action_type == 18

    @property
    def is_charge(self):
        return self.event_action_type == 26

    @property
    def is_personal_block_foul(self):
        return self.event_action_type == 27

    @property
    def is_personal_take_foul(self):
        return self.event_action_type == 28

    @property
    def is_shooting_block_foul(self):
        return self.event_action_type == 29

    @property
    def is_transition_take_foul(self):
        return self.event_action_type == 31
