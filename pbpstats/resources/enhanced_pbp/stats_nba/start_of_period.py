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
             If the strict result is internally impossible, treat it as failure.
          2) Local boxscore-based starters (Period 1 via START_POSITION).
          3) Period-level V3 boxscore fallback (only when strict PBP failed).
          4) Best-effort PBP inference (ignore_missing_starters=True).
        """
        # 1) Strict PBP-based inference
        try:
            starters = self._get_period_starters_from_period_events(file_directory)
        except InvalidNumberOfStartersException:
            starters = None

        if starters is not None and not self._strict_starters_are_impossible(starters):
            # Explicit local period-starter overrides should beat any parquet
            # fallback source, including exact v6 gamerotation rows.
            if self._has_period_starter_override(file_directory):
                return starters
            # When a local gamerotation-backed v6 row exists and disagrees with
            # strict PBP, prefer the exact boundary map over fragile carryover.
            local_boxscore_starters, local_boxscore_source = (
                self._get_exact_local_period_boxscore_starters()
            )
            if (
                local_boxscore_source == "v6"
                and local_boxscore_starters is not None
                and local_boxscore_starters != starters
            ):
                if self._should_prefer_strict_starters_over_exact_v6(
                    starters, local_boxscore_starters
                ):
                    return starters
                return local_boxscore_starters
            return starters

        # 2) Local boxscore-based starters (Period 1)
        starters = self._get_period_starters_from_boxscore_loader()
        if starters is not None:
            return starters

        # 3) Period-level V3 boxscore fallback (PBP failed to find 10).
        try:
            starters = self._get_starters_from_boxscore_request()
        except InvalidNumberOfStartersException:
            starters = None
        if starters is not None:
            return starters

        # 4) Best-effort PBP inference.
        return self._get_period_starters_from_period_events(
            file_directory, ignore_missing_starters=True
        )

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
