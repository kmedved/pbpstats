from pbpstats.data_loader.stats_nba.base import StatsNbaLoaderBase


class StatsNbaJsonLoader(StatsNbaLoaderBase):
    """
    Helper loader for using in-memory stats.nba.com-style JSON with the
    existing stats_nba data loader ecosystem.

    This is useful when responses are coming from a database, cache, or
    any other non-file / non-web source.

    Example usage:

        from pbpstats.data_loader.stats_nba.json_loader import StatsNbaJsonLoader
        from pbpstats.data_loader.stats_nba.pbp.loader import StatsNbaPbpLoader

        pbp_json = <dict loaded from DB, shaped like the API response>
        source_loader = StatsNbaJsonLoader(pbp_json)
        pbp_loader = StatsNbaPbpLoader("0021900001", source_loader)
        events = pbp_loader.items

    `StatsNbaJsonLoader` is *not* registered in DataLoaderFactory and
    is intended for direct use.
    """

    def __init__(self, source_data, file_directory=None):
        """
        :param dict source_data: stats.nba.com-style JSON response dict.
        :param str file_directory: optional directory for loaders that
            may call `_save_data_to_file`; if not needed, pass None.
        """
        self.source_data = source_data
        self.file_directory = file_directory

    def load_data(self, *args, **kwargs):
        """
        Keep the same interface as other source loaders. Positional or
        keyword arguments are ignored and the pre-supplied source_data
        is returned as-is.
        """
        return self.source_data
