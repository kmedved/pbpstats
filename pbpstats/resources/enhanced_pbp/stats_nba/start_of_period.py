from pbpstats.resources.enhanced_pbp import (
    InvalidNumberOfStartersException,
    StartOfPeriod,
)
from pbpstats.resources.enhanced_pbp.stats_nba.enhanced_pbp_item import (
    StatsEnhancedPbpItem,
)


class StatsStartOfPeriod(StartOfPeriod, StatsEnhancedPbpItem):
    """
    Class for start of period events
    """

    event_type = 12

    def __init__(self, *args):
        super().__init__(*args)

    def get_period_starters(self, file_directory=None):
        """
        Try:
          1) PBP-based inference.
          2) If that fails and a boxscore_source_loader is attached, use it locally.
          3) Only if #2 is unavailable, fall back to the original stats.nba.com request.
        """
        try:
            return self._get_period_starters_from_period_events(file_directory)
        except InvalidNumberOfStartersException:
            starters = self._get_period_starters_from_boxscore_loader()
            if starters is not None:
                return starters

            if getattr(self, "boxscore_source_loader", None) is not None:
                raise InvalidNumberOfStartersException(
                    f"Offline: Cannot determine starters for GameId: {self.game_id}, Period: {self.period}"
                )

            return self._get_starters_from_boxscore_request()

    def _get_period_starters_from_boxscore_loader(self):
        """
        Use a locally-supplied boxscore loader (file/json/memory) to get
        period starters, if available. Returns dict[team_id] -> [player_ids]
        or None if it can't determine them.
        """
        loader_obj = getattr(self, "boxscore_source_loader", None)
        if loader_obj is None:
            return None

        from pbpstats.data_loader.stats_nba.boxscore.loader import StatsNbaBoxscoreLoader

        try:
            boxscore_loader = StatsNbaBoxscoreLoader(self.game_id, loader_obj)
        except Exception:
            return None

        players = [item.data for item in boxscore_loader.items if hasattr(item, "player_id")]

        starters_by_team = {}

        if self.period == 1:
            for p in players:
                team_id = p.get("team_id")
                start_pos = p.get("start_position")
                if not team_id:
                    continue
                if start_pos is None or str(start_pos).strip() == "":
                    continue
                starters_by_team.setdefault(team_id, []).append(p["player_id"])

            if not starters_by_team:
                return None
            for team_id, starters in starters_by_team.items():
                if len(starters) != 5:
                    return None
            return starters_by_team

        return None
