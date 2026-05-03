from pbpstats.resources.enhanced_pbp import StartOfPeriod
from pbpstats.resources.enhanced_pbp.live.enhanced_pbp_item import LiveEnhancedPbpItem


class LiveStartOfPeriod(StartOfPeriod, LiveEnhancedPbpItem):
    """
    Class for start of period events
    """

    action_type = "period"
    sub_type = "start"

    def __init__(self, *args):
        super().__init__(*args)

    def get_offense_team_id(self):
        """
        For live data, use the explicit "possession" field unless the canonical
        period-start inference produces a conflicting non-zero team.
        """
        explicit = self._coerce_team_id(getattr(self, "offense_team_id", 0))
        try:
            inferred = self._coerce_team_id(self.get_team_starting_with_ball())
        except AttributeError:
            if explicit is not None:
                return explicit
            raise

        if explicit is not None and inferred is not None and explicit != inferred:
            return inferred
        if explicit is not None:
            return explicit
        return inferred

    @staticmethod
    def _coerce_team_id(value):
        try:
            value = int(value)
        except (TypeError, ValueError):
            return None
        return value or None

    def get_period_starters(self, file_directory=None, ignore_missing_starters=False):
        """
        Gets player ids of players who started the period for each team

        :param str file_directory: directory in which overrides subdirectory exists
            containing period starter overrides when period starters can't be determined
            from parsing pbp events
        :param bool ignore_missing_starters: when True won't reaise missing starters exception
        :returns: dict with list of player ids for each team
            with players on the floor at start of period
        :raises: :obj:`~pbpstats.resources.enhanced_pbp.start_of_period.InvalidNumberOfStartersException`:
            If all 5 players that start the period for a team can't be determined.
        """
        return self._get_period_starters_from_period_events(
            file_directory, ignore_missing_starters=ignore_missing_starters
        )
