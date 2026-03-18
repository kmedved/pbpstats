from pbpstats.resources.enhanced_pbp import Substitution
from pbpstats.resources.enhanced_pbp.live.enhanced_pbp_item import LiveEnhancedPbpItem


class LiveSubstitution(Substitution, LiveEnhancedPbpItem):
    """
    Class for Substitution events
    """

    action_type = "substitution"

    def __init__(self, *args):
        super().__init__(*args)

    @property
    def incoming_player_id(self):
        """
        returns player id of player coming in to the game
        """
        if self.sub_type == "out":
            return None
        return self.player1_id

    @property
    def outgoing_player_id(self):
        """
        returns player id of player coming in to the game
        """
        if self.sub_type == "in":
            return None
        return self.player1_id

    @property
    def _raw_current_players(self):
        """
        returns dict with list of player ids for each team
        with players on the floor following the sub
        """
        previous_players = self._get_previous_raw_players()
        players = {
            team_id: [player_id for player_id in team_players]
            for team_id, team_players in previous_players.items()
        }
        if self.player1_id is not None and self.team_id in players:
            if self.sub_type == "in":
                players[self.team_id].append(self.player1_id)
            elif self.sub_type == "out" and self.player1_id in players[self.team_id]:
                players[self.team_id].remove(self.player1_id)
            players[self.team_id] = list(dict.fromkeys(players[self.team_id]))
        return players

    @property
    def current_players(self):
        """
        returns dict with list of player ids for each team
        with players on the floor following the sub
        """
        return self._apply_lineup_overrides(self._raw_current_players)
