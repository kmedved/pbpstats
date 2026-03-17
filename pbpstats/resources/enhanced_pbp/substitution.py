import abc


class Substitution(object):
    """
    Class for Substitution events
    """

    @abc.abstractproperty
    def outgoing_player_id(self):
        pass

    @abc.abstractproperty
    def incoming_player_id(self):
        pass

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
        # Some malformed historical rows carry a valid team_id on the substitution
        # but the preceding lineup context is missing that team's players entirely.
        # Treat those as no-op substitutions so lineup tracing can continue.
        if self.team_id not in players:
            return players
        players[self.team_id] = [
            self.incoming_player_id if player == self.outgoing_player_id else player
            for player in players[self.team_id]
        ]
        return players

    @property
    def current_players(self):
        return self._apply_lineup_overrides(self._raw_current_players)

    @property
    def event_stats(self):
        """
        returns list of dicts with all stats for event
        """
        return self.base_stats
