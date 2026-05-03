import pytest

import pbpstats.objects as objects
from pbpstats.client import Client
from pbpstats.data_loader.stats_nba.boxscore.file import StatsNbaBoxscoreFileLoader
from pbpstats.data_loader.stats_nba.boxscore.loader import StatsNbaBoxscoreLoader
from pbpstats.data_loader.stats_nba.pbp.file import StatsNbaPbpFileLoader
from pbpstats.data_loader.stats_nba.pbp.loader import StatsNbaPbpLoader
from pbpstats.resources.boxscore.boxscore import Boxscore
from pbpstats.resources.pbp.pbp import Pbp


def test_client_sets_object_attrs():
    settings = {}
    client = Client(settings)
    assert hasattr(client, "Game")
    assert hasattr(client, "Day")
    assert hasattr(client, "Season")


def test_client_sets_data_directory():
    settings = {
        "dir": "tmp",
    }
    client = Client(settings)
    assert client.data_directory == settings["dir"]


def test_client_sets_resource():
    settings = {
        "dir": "tmp",
        "Boxscore": {"source": "file", "data_provider": "stats_nba"},
    }
    client = Client(settings)
    assert client.Game.BoxscoreDataLoaderClass == StatsNbaBoxscoreLoader
    assert client.Game.BoxscoreDataSource == StatsNbaBoxscoreFileLoader
    assert client.Game.Boxscore == Boxscore


def test_client_threads_known_source_loader_options_only():
    settings = {
        "dir": "tmp",
        "Pbp": {
            "source": "file",
            "data_provider": "stats_nba",
            "endpoint_strategy": "v3_synthetic",
            "ignored_loader_option": "ignored",
        },
    }
    client = Client(settings)
    assert client.Game.PbpDataLoaderClass == StatsNbaPbpLoader
    assert client.Game.PbpDataSource == StatsNbaPbpFileLoader
    assert client.Game.Pbp == Pbp
    assert client.Game.PbpDataSourceOptions == {
        "endpoint_strategy": "v3_synthetic"
    }


def test_client_does_not_thread_endpoint_strategy_to_unsupported_resource():
    settings = {
        "dir": "tmp",
        "Boxscore": {
            "source": "file",
            "data_provider": "stats_nba",
            "endpoint_strategy": "v3_synthetic",
        },
    }
    client = Client(settings)
    assert client.Game.BoxscoreDataSourceOptions == {}


def test_client_does_not_thread_endpoint_strategy_to_non_stats_nba_provider():
    settings = {
        "dir": "tmp",
        "Pbp": {
            "source": "file",
            "data_provider": "data_nba",
            "endpoint_strategy": "v3_synthetic",
        },
    }
    client = Client(settings)
    assert client.Game.PbpDataSourceOptions == {}


def test_client_instances_do_not_share_bound_resource_classes():
    pbp_client = Client(
        {
            "dir": "tmp",
            "Pbp": {
                "source": "file",
                "data_provider": "stats_nba",
                "endpoint_strategy": "v3_synthetic",
            },
        }
    )
    boxscore_client = Client(
        {
            "dir": "tmp",
            "Boxscore": {"source": "file", "data_provider": "stats_nba"},
        }
    )

    assert pbp_client.Game is not boxscore_client.Game
    assert hasattr(pbp_client.Game, "PbpDataLoaderClass")
    assert not hasattr(pbp_client.Game, "BoxscoreDataLoaderClass")
    assert hasattr(boxscore_client.Game, "BoxscoreDataLoaderClass")
    assert not hasattr(boxscore_client.Game, "PbpDataLoaderClass")


def test_client_clears_legacy_global_resource_bindings():
    objects.Game.PbpDataLoaderClass = StatsNbaPbpLoader
    try:
        boxscore_client = Client(
            {
                "dir": "tmp",
                "Boxscore": {"source": "file", "data_provider": "stats_nba"},
            }
        )
        assert not hasattr(boxscore_client.Game, "PbpDataLoaderClass")
        assert not hasattr(objects.Game, "PbpDataLoaderClass")
    finally:
        if hasattr(objects.Game, "PbpDataLoaderClass"):
            delattr(objects.Game, "PbpDataLoaderClass")


def test_client_loads_data():
    settings = {
        "dir": "tests/data",
        "Boxscore": {"source": "file", "data_provider": "stats_nba"},
    }
    client = Client(settings)
    game = client.Game("0021600270")
    assert len(game.boxscore.items) > 0


def test_value_error_raised_when_dir_missing():
    settings = {
        "Boxscore": {"source": "file", "data_provider": "data_nba"},
    }
    client = Client(settings)
    with pytest.raises(ValueError):
        assert client.Game("0021600270")
