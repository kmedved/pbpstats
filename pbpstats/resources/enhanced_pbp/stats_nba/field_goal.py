from pbpstats.resources.enhanced_pbp import FieldGoal
from pbpstats.resources.enhanced_pbp.stats_nba.enhanced_pbp_item import (
    StatsEnhancedPbpItem,
)


class StatsFieldGoal(FieldGoal, StatsEnhancedPbpItem):
    """
    Class for field goal events
    """

    event_type = [1, 2]

    def __init__(self, *args):
        super().__init__(*args)

    @property
    def _is_source_limited_anonymous_no_shot(self):
        return (
            getattr(self, "event_action_type", None) == 0
            and getattr(self, "team_id", 0) in [0, None, "0"]
            and getattr(self, "player1_id", 0) in [0, None, "0"]
            and getattr(self, "player2_id", 0) in [0, None, "0"]
            and getattr(self, "player3_id", 0) in [0, None, "0"]
        )

    @property
    def event_stats(self):
        if self._is_source_limited_anonymous_no_shot:
            self._log_source_limited_guard("source_limited_anonymous_no_shot")
            return self.base_stats
        return super().event_stats

    @property
    def is_made(self):
        """
        returns True if shot was made, False otherwise
        """
        return self.event_type == 1

    @property
    def shot_value(self):
        """
        returns 3 if shot is a 3 point attempt, 2 otherwise
        """
        return 3 if " 3PT " in self.description else 2

    def get_offense_team_id(self):
        """
        returns team id that took the shot
        """
        return self.team_id
