from pbpstats.resources.enhanced_pbp import Substitution
from pbpstats.resources.enhanced_pbp.stats_nba.enhanced_pbp_item import (
    StatsEnhancedPbpItem,
)


class StatsSubstitution(Substitution, StatsEnhancedPbpItem):
    """
    Class for Substitution events
    """

    event_type = 8

    def __init__(self, *args):
        super().__init__(*args)

    @property
    def outgoing_player_id(self):
        """
        returns player id of player going out of the game
        """
        return self.player1_id

    @property
    def current_players(self):
        # Some legacy rows are blank substitution placeholders with no valid
        # team or player context. Treat them as no-op lineup changes.
        if getattr(self, "team_id", 0) in [0, None, "0"] or getattr(
            self, "player1_id", 0
        ) in [0, None, "0"]:
            return getattr(self.previous_event, "current_players", {})
        return super().current_players

    @property
    def incoming_player_id(self):
        """
        returns player id of player coming in to the game
        """
        if hasattr(self, "player2_id") and getattr(self, "player2_id", 0) not in [
            0,
            None,
            "0",
        ]:
            return self.player2_id
        # Malformed historical rows occasionally omit the incoming player.
        # Treat these as no-op substitutions so lineup tracking can continue.
        return self.player1_id
