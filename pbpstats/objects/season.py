"""
Instantiating a ``Season`` object will load all resources for the ``Season``
object that were set in the settings when the client was instantiated

The following code will instantiate the client and get all games for the
2019-20 NBA Regular Season and store the schedule response in a
``/response_data`` subdirectory

.. code-block:: python

    from pbpstats.client import Client

    settings = {
        "dir": "/response_data",
        "Games": {"source": "web", "data_provider": "data_nba"}
    }
    client = Client(settings)
    season = client.Season("nba", "2019-20", "Regular Season")
    for game in season.games.items:
        print(game)
"""
import inspect

import pbpstats.client as client


class Season(object):
    """
    Class for loading resource data from data loaders with a ``parent_object`` of ``Season``

    :param str league: Options are 'nba', 'wnba' or 'gleague'
    :param str season: Can be formatted as either 2019-20 or 2019.
    :param str season_type: Options are 'Regular Season' or 'Playoffs' or 'Play In'
    """

    def __init__(self, league, season, season_type):
        self.league = league
        self.season = season
        self.season_type = season_type
        attributes = inspect.getmembers(self, lambda a: not (inspect.isroutine(a)))
        data_loaders = [
            a for a in attributes if a[0].endswith(client.DATA_LOADER_SUFFIX)
        ]
        data_source_map = {
            a[0].replace(client.DATA_SOURCE_SUFFIX, ""): a[1]
            for a in attributes
            if a[0].endswith(client.DATA_SOURCE_SUFFIX)
        }
        for data_loader in data_loaders:
            attr_name = data_loader[0].replace(client.DATA_LOADER_SUFFIX, "")
            source_loader_cls = data_source_map[attr_name]
            source_loader = source_loader_cls(self.data_directory)
            try:
                data = data_loader[1](
                    league,
                    season,
                    season_type,
                    source_loader,
                    enable_data_nba_fallback=self.enable_data_nba_fallback,
                )
            except TypeError:
                data = data_loader[1](league, season, season_type, source_loader)
            resource_cls = getattr(self, attr_name)
            setattr(
                self,
                client.PATTERN.sub("_", attr_name).lower(),
                resource_cls(data.items),
            )
