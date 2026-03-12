import pbpstats
from pbpstats.resources.enhanced_pbp import FieldGoal
from pbpstats.resources.enhanced_pbp.live.enhanced_pbp_item import LiveEnhancedPbpItem


class LiveFieldGoal(FieldGoal, LiveEnhancedPbpItem):
    """
    Class for field goal events from the live data feed.
    """

    action_type = ["2pt", "3pt", "heave"]

    def __init__(self, *args):
        super().__init__(*args)

        if getattr(self, "action_type", None) == "heave":
            # '0' is TEAM_STAT_PLAYER_ID in pbpstats.__init__
            self.player1_id = int(pbpstats.TEAM_STAT_PLAYER_ID)

    @property
    def shot_value(self):
        """
        Returns numeric shot value.

        Prefer explicit shotValue from the feed when present, otherwise:
        - Treat '3pt' and 'heave' as 3
        - Everything else as 2
        """
        if hasattr(self, "shotValue"):
            try:
                return int(self.shotValue)
            except (TypeError, ValueError):
                pass

        return 3 if getattr(self, "action_type", None) in ("3pt", "heave") else 2

    @property
    def is_made(self):
        """
        returns True if shot was made, False otherwise.

        Live 'heave' events (and potentially other edge cases) may not carry
        a `shotResult` field in the raw JSON, in which case `shot_result`
        is never set on the object. To avoid blowing up on those, treat a
        missing shot_result as "not made".
        """
        return getattr(self, "shot_result", None) == "Made"
