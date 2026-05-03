from pbpstats.data_loader.stats_nba.enhanced_pbp.web import StatsNbaEnhancedPbpWebLoader
from pbpstats.data_loader.stats_nba.pbp.v3_synthetic import ENDPOINT_STRATEGY_V2


class StatsNbaPossessionWebLoader(object):
    """
    A ``StatsNbaPossessionWebLoader`` object should be instantiated and passed into ``StatsNbaPossessionLoader`` when loading data directly from the NBA Stats API

    :param str file_directory: (optional, use it if you want to store the response data on disk)
        Directory in which data should be either stored.
        The specific file location will be `stats_<game_id>.json` in the `/pbp` subdirectory.
        If not provided response data will not be saved on disk.
    """

    def __init__(self, file_directory=None, endpoint_strategy=ENDPOINT_STRATEGY_V2):
        self.file_directory = file_directory
        self.endpoint_strategy = endpoint_strategy
        self.enhanced_pbp_source_loader = StatsNbaEnhancedPbpWebLoader(
            file_directory, endpoint_strategy=endpoint_strategy
        )
