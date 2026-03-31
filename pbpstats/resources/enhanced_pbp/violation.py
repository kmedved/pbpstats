import abc

from pbpstats import DEFENSIVE_GOALTENDING_STRING


class Violation(object):
    """
    Class for violation events
    """

    @abc.abstractclassmethod
    def is_delay_of_game(self):
        pass

    @abc.abstractclassmethod
    def is_goaltend_violation(self):
        pass

    @abc.abstractclassmethod
    def is_lane_violation(self):
        pass

    @abc.abstractclassmethod
    def is_jumpball_violation(self):
        pass

    @abc.abstractclassmethod
    def is_kicked_ball_violation(self):
        pass

    @abc.abstractclassmethod
    def is_double_lane_violation(self):
        pass

    @property
    def event_stats(self):
        """
        returns list of dicts with all stats for event
        """
        stats = []
        if self.is_goaltend_violation:
            stats.append(
                {
                    "player_id": self.player1_id,
                    "team_id": self.team_id,
                    "stat_key": DEFENSIVE_GOALTENDING_STRING,
                    "stat_value": 1,
                }
            )
            stats = self._add_event_stat_context(stats)
        return self.base_stats + stats
