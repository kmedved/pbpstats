from pbpstats.data_loader.stats_nba.pbp.file import StatsNbaPbpFileLoader
from pbpstats.data_loader.stats_nba.pbp.v3_synthetic import (
    ENDPOINT_STRATEGY_V2,
    validate_endpoint_strategy,
)
from pbpstats.data_loader.stats_nba.shots.file import StatsNbaShotsFileLoader


class StatsNbaEnhancedPbpFileLoader(StatsNbaPbpFileLoader):
    """
    A ``StatsNbaEnhancedPbpFileLoader`` object should be instantiated and passed into ``StatsNbaEnhancedPbpLoader`` when loading data from file

    :param str file_directory:
        Directory in which data should be loaded from.
        The specific file location will be `stats_<game_id>.json` in the `/pbp` subdirectory.
    """

    def __init__(self, file_directory, endpoint_strategy=ENDPOINT_STRATEGY_V2):
        self.file_directory = file_directory
        validate_endpoint_strategy(endpoint_strategy)
        self.endpoint_strategy = endpoint_strategy
        self.shots_source_loader = StatsNbaShotsFileLoader(file_directory)
