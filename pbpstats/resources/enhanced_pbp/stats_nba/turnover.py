from pbpstats.resources.enhanced_pbp import Turnover
from pbpstats.resources.enhanced_pbp.stats_nba.enhanced_pbp_item import (
    StatsEnhancedPbpItem,
)


class StatsTurnover(Turnover, StatsEnhancedPbpItem):
    """
    Class for Turnover events
    """

    event_type = 5

    def __init__(self, *args):
        super().__init__(*args)

    @property
    def event_stats(self):
        # Some legacy stats.nba turnover rows have no valid committing team or
        # player and are just unattributable source corruption. Preserve only
        # base stats instead of trying to attach turnover stats to team_id 0.
        if getattr(self, "team_id", 0) in [0, None, "0"] and getattr(
            self, "player1_id", 0
        ) in [0, None, "0"]:
            return self.base_stats
        return super().event_stats

    def get_offense_team_id(self):
        """
        returns team id for team on offense for event
        """
        if self.is_no_turnover and not self.is_steal:
            previous_event = getattr(self, "previous_event", None)
            if previous_event is None:
                return self.team_id
            try:
                return previous_event.get_offense_team_id()
            except RecursionError:
                # Some malformed same-clock period-start clusters bounce between
                # a foul and a synthetic "No Turnover" row. Fall back to the
                # turnover row's own team id instead of recursing indefinitely.
                return self.team_id
        return self.team_id

    @property
    def is_no_turnover(self):
        description = str(getattr(self, "description", "") or "")
        return self.event_action_type == 0 and "No Turnover" in description

    @property
    def is_bad_pass(self):
        return self.event_action_type == 1 and self.is_steal

    @property
    def is_lost_ball(self):
        return self.event_action_type == 2 and self.is_steal

    @property
    def is_travel(self):
        return self.event_action_type == 4

    @property
    def is_3_second_violation(self):
        return self.event_action_type == 8

    @property
    def is_shot_clock_violation(self):
        return self.event_action_type == 11

    @property
    def is_offensive_goaltending(self):
        return self.event_action_type == 15

    @property
    def is_lane_violation(self):
        return self.event_action_type == 17

    @property
    def is_kicked_ball(self):
        return self.event_action_type == 19

    @property
    def is_step_out_of_bounds(self):
        return self.event_action_type == 39

    @property
    def is_lost_ball_out_of_bounds(self):
        # some labelled as lost ball but should be lost ball out of bounds (missing player3 id)
        return self.event_action_type == 40 or (
            self.event_action_type == 2 and not self.is_steal
        )

    @property
    def is_bad_pass_out_of_bounds(self):
        # some labelled as bad pass but should be bad pass out of bounds (missing player3 id)
        return self.event_action_type == 45 or (
            self.event_action_type == 1 and not self.is_steal
        )
