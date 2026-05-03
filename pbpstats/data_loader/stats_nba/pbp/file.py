from pbpstats.data_loader.abs_data_loader import check_file_directory
from pbpstats.data_loader.stats_nba.file_loader import StatsNbaFileLoader
from pbpstats.data_loader.stats_nba.pbp.v3_synthetic import (
    ENDPOINT_STRATEGY_AUTO,
    ENDPOINT_STRATEGY_V2,
    ENDPOINT_STRATEGY_V3_SYNTHETIC,
    validate_endpoint_strategy,
    validate_v2_pbp_response,
)


class StatsNbaPbpFileLoader(StatsNbaFileLoader):
    """
    A ``StatsNbaPbpFileLoader`` object should be instantiated and passed into ``StatsNbaPbpLoader`` when loading data from file

    :param str file_directory:
        Directory in which data should be loaded from.
        The specific file location will be `stats_<game_id>.json` in the `/pbp` subdirectory.
    :param str endpoint_strategy: `v2`, `v3_synthetic`, or `auto`.
        Defaults to `v2`. `auto` prefers true v2 cache and falls back to the
        synthetic v3 cache only when the true v2 cache is missing or malformed.
    """

    def __init__(self, file_directory, endpoint_strategy=ENDPOINT_STRATEGY_V2):
        self.file_directory = file_directory
        validate_endpoint_strategy(endpoint_strategy)
        self.endpoint_strategy = endpoint_strategy

    @check_file_directory
    def load_data(self, game_id):
        self.game_id = game_id
        self.loaded_endpoint_strategy = None
        if self.endpoint_strategy == ENDPOINT_STRATEGY_V3_SYNTHETIC:
            self.loaded_endpoint_strategy = ENDPOINT_STRATEGY_V3_SYNTHETIC
            return self._load_synthetic_v3_cache()
        if self.endpoint_strategy == ENDPOINT_STRATEGY_AUTO:
            return self._load_auto_cache()
        source_data = self._load_v2_cache()
        validate_v2_pbp_response(source_data)
        self.loaded_endpoint_strategy = ENDPOINT_STRATEGY_V2
        return source_data

    def _load_v2_cache(self):
        self.file_path = f"{self.file_directory}/pbp/stats_{self.game_id}.json"
        return self._load_data_from_file()

    def _load_synthetic_v3_cache(self):
        self.file_path = (
            f"{self.file_directory}/pbp_synthetic_v3/stats_{self.game_id}.json"
        )
        return self._load_data_from_file()

    def _load_auto_cache(self):
        try:
            source_data = self._load_v2_cache()
            validate_v2_pbp_response(source_data)
            self.loaded_endpoint_strategy = ENDPOINT_STRATEGY_V2
            return source_data
        except Exception:
            self.loaded_endpoint_strategy = ENDPOINT_STRATEGY_V3_SYNTHETIC
            return self._load_synthetic_v3_cache()
