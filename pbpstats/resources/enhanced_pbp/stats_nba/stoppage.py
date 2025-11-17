from pbpstats.resources.enhanced_pbp.stats_nba.enhanced_pbp_item import (
    StatsEnhancedPbpItem,
)


class StatsStoppage(StatsEnhancedPbpItem):
    """
    Lightweight event for stoppage metadata (out-of-bounds, delays, etc.).
    """

    event_type = 20

    def __init__(self, *args):
        super().__init__(*args)

    @property
    def is_possession_ending_event(self):
        """
        Stoppages do not change possession by themselves.
        """
        return False

    @property
    def event_stats(self):
        """
        Stoppages produce no box score stats.
        """
        return []

    def get_offense_team_id(self):
        """
        Prefer the previous event's offense context when available.
        """
        prev_event = getattr(self, "previous_event", None)
        if prev_event is not None:
            return prev_event.get_offense_team_id()
        possession_hint = getattr(self, "possession_team_id", None)
        return possession_hint or getattr(self, "team_id", None)
