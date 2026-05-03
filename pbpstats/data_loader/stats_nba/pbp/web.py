import json
import os

from pbpstats import G_LEAGUE_STRING, NBA_STRING
from pbpstats.data_loader.stats_nba.pbp.v3_synthetic import (
    ENDPOINT_STRATEGY_AUTO,
    ENDPOINT_STRATEGY_V2,
    ENDPOINT_STRATEGY_V3_SYNTHETIC,
    build_synthetic_v2_pbp_response,
    validate_endpoint_strategy,
    validate_v2_pbp_response,
)
from pbpstats.data_loader.stats_nba.web_loader import StatsNbaWebLoader


class StatsNbaPbpWebLoader(StatsNbaWebLoader):
    """
    A ``StatsNbaPbpWebLoader`` object should be instantiated and passed into ``StatsNbaPbpLoader`` when loading data directly from the NBA Stats API

    :param str file_directory: (optional, use it if you want to store the response data on disk)
        Directory in which data should be either stored.
        The specific file location will be `stats_<game_id>.json` in the `/pbp` subdirectory.
        If not provided response data will not be saved on disk.
    :param str endpoint_strategy: `v2`, `v3_synthetic`, or `auto`.
        Defaults to `v2` for compatibility. `auto` only falls back to synthetic
        v3 rows when the v2 endpoint response is unavailable or malformed.
    """

    def __init__(self, file_directory=None, endpoint_strategy=ENDPOINT_STRATEGY_V2):
        self.file_directory = file_directory
        validate_endpoint_strategy(endpoint_strategy)
        self.endpoint_strategy = endpoint_strategy
        self._defer_v2_save = False

    def load_data(self, game_id):
        self.game_id = game_id
        if self.endpoint_strategy == ENDPOINT_STRATEGY_V3_SYNTHETIC:
            return self._load_v3_synthetic_data()

        try:
            source_data = self._load_v2_data()
            validate_v2_pbp_response(source_data)
        except Exception:
            if self.endpoint_strategy != ENDPOINT_STRATEGY_AUTO:
                raise
            return self._load_v3_synthetic_data()
        self.source_data = source_data
        self._save_data_to_file()
        return self.source_data

    def _load_v2_data(self):
        league_url_part = (
            f"{G_LEAGUE_STRING}.{NBA_STRING}"
            if self.league == G_LEAGUE_STRING
            else self.league
        )
        self.base_url = f"https://stats.{league_url_part}.com/stats/playbyplayv2"
        self.parameters = {
            "GameId": self.game_id,
            "StartPeriod": 0,
            "EndPeriod": 10,
            "RangeType": 2,
            "StartRange": 0,
            "EndRange": 55800,
        }
        self._defer_v2_save = True
        try:
            return self._load_request_data()
        finally:
            self._defer_v2_save = False

    def _load_v3_synthetic_data(self):
        v3_loader = StatsNbaPbpV3WebLoader(self.file_directory)
        v3_source_data = v3_loader.load_data(self.game_id)
        self.source_data = build_synthetic_v2_pbp_response(
            self.game_id, v3_source_data
        )
        self._save_synthetic_v3_data_to_file()
        return self.source_data

    def _save_data_to_file(self):
        if self._defer_v2_save:
            return
        if self.file_directory is not None and os.path.isdir(self.file_directory):
            file_path = f"{self.file_directory}/pbp/stats_{self.game_id}.json"
            with open(file_path, "w") as outfile:
                json.dump(self.source_data, outfile)

    def _save_synthetic_v3_data_to_file(self):
        if self.file_directory is not None and os.path.isdir(self.file_directory):
            dir_path = os.path.join(self.file_directory, "pbp_synthetic_v3")
            os.makedirs(dir_path, exist_ok=True)
            file_path = os.path.join(dir_path, f"stats_{self.game_id}.json")
            with open(file_path, "w") as outfile:
                json.dump(self.source_data, outfile)


class StatsNbaPbpV3WebLoader(StatsNbaWebLoader):
    """
    Helper loader for stats.nba.com playbyplayv3 endpoint.

    This is used internally by StatsNbaEnhancedPbpLoader to reconcile
    event ordering using actionId/actionNumber. It is deliberately
    *not* registered as a top-level resource (no `resource` or
    `parent_object` attributes), so DataLoaderFactory will ignore it.
    """

    def __init__(self, file_directory=None):
        self.file_directory = file_directory

    def load_data(self, game_id):
        self.game_id = game_id
        league_url_part = (
            f"{G_LEAGUE_STRING}.{NBA_STRING}"
            if self.league == G_LEAGUE_STRING
            else self.league
        )
        # v3 endpoint
        self.base_url = f"https://stats.{league_url_part}.com/stats/playbyplayv3"
        self.parameters = {
            "GameID": self.game_id,
            "StartPeriod": 0,
            "EndPeriod": 10,
        }
        return self._load_request_data()

    def _save_data_to_file(self):
        """
        Optionally cache v3 responses if a file_directory was provided.
        This mirrors the pattern of the v2 loader but writes under a
        separate `pbp_v3` subdirectory.
        """
        if self.file_directory is not None and os.path.isdir(self.file_directory):
            dir_path = os.path.join(self.file_directory, "pbp_v3")
            os.makedirs(dir_path, exist_ok=True)
            file_path = os.path.join(dir_path, f"stats_pbpv3_{self.game_id}.json")
            with open(file_path, "w") as outfile:
                json.dump(self.source_data, outfile)
