from pbpstats.resources.enhanced_pbp.live.enhanced_pbp_item import LiveEnhancedPbpItem


class LiveGameEnd(LiveEnhancedPbpItem):
    """
    Class for game end events in the live feed.
    """

    action_type = "game"
    sub_type = "end"

    def get_offense_team_id(self):
        offense_team_id = super().get_offense_team_id()
        if offense_team_id in (0, None) and getattr(self, "previous_event", None):
            return self.previous_event.get_offense_team_id()
        return offense_team_id
