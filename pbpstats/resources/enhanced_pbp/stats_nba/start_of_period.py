from pbpstats.data_loader.stats_nba.boxscore.loader import StatsNbaBoxscoreLoader
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
        Try, in order:
          1) PBP-based inference (strict, including overrides).
          2) Local boxscore-based starters (Period 1 via START_POSITION).
          3) Offline best-effort PBP inference (ignore_missing_starters=True).
          4) Legacy web fallback ONLY if no local boxscore loader was provided.
        """
        # 1) Strict PBP-based inference
        try:
            return self._get_period_starters_from_period_events(file_directory)
        except InvalidNumberOfStartersException:
            pass

        # 2) Local boxscore-based starters (Period 1)
        starters = self._get_period_starters_from_boxscore_loader()
        if starters is not None:
            return starters

        # 3) Offline best-effort PBP inference:
        #    if we have a local boxscore loader, stay offline and don't crash.
        if getattr(self, "boxscore_source_loader", None) is not None:
            # ignore_missing_starters=True skips the strict 5-per-team assertion
            return self._get_period_starters_from_period_events(
                file_directory, ignore_missing_starters=True
            )

        # 4) Legacy behavior: only use web if no local loader exists at all.
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
