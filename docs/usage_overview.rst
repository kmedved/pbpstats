Usage Overview
==============

This guide provides a tour of the key features in ``pbpstats`` and how they fit together to turn raw play-by-play data into possession level data that can feed a RAPM model.  The examples assume you have already installed ``pbpstats`` and are familiar with the basic concepts in :ref:`quickstart`.

.. contents:: Table of Contents
   :local:
   :depth: 2

Installation
------------

``pbpstats`` is published on PyPI and works on Python 3.8 or later.  Install it with ``pip``:

.. code-block:: bash

    pip install pbpstats

A data directory is recommended so that downloaded responses are cached locally.  Create a directory with ``pbp``, ``game_details``, ``overrides`` and ``schedule`` subdirectories.  Then include the directory path under the ``dir`` key when creating a :class:`pbpstats.client.Client`.

Initializing the Client
-----------------------

All data in ``pbpstats`` is accessed through a ``Client`` instance.  The client accepts a settings dictionary describing which resources to enable and where they should load data from.  Resources correspond to modules under :mod:`pbpstats.resources`.  A minimal configuration might look like the snippet below.

.. code-block:: python

    from pbpstats.client import Client

    settings = {
        "dir": "/path/to/data_directory",
        "Boxscore": {"source": "web", "data_provider": "stats_nba"},
        "EnhancedPbp": {"source": "web", "data_provider": "stats_nba"},
        "Possessions": {"source": "web", "data_provider": "stats_nba"},
        "Shots": {"source": "web", "data_provider": "stats_nba"},
    }
    client = Client(settings)

Calling ``client.Game(game_id)`` returns a game object.  Each resource specified in the settings can be accessed as an attribute on that game.  For example ``game.boxscore.items`` returns the boxscore dictionary and ``game.enhanced_pbp.items`` returns a list of enhanced play-by-play events.

Resource Overview
-----------------

* **Boxscore** – player and team boxscore stats.  Useful for minutes and starting lineup information.
* **Pbp** – raw play-by-play data as returned by the NBA endpoints.
* **EnhancedPbp** – enriched play-by-play with additional annotations such as lineups on the floor, possessions, fouls to give and running score.
* **Possessions** – a convenience layer that groups enhanced events into possessions and exposes helpers for aggregating statistics by team, lineup or player.
* **Shots** – shot level data with coordinates and shot zone metadata.
* **Games / Season** – utilities for pulling schedules and iterating through many games.

The sections below demonstrate how to work with these objects in practice.

Working with Boxscore Data
--------------------------

``Boxscore`` data is useful for quick summaries and for retrieving the players that were available in a given game.  The example below prints the points scored by each player in a single game.

.. code-block:: python

    game = client.Game("0021900001")
    for player in game.boxscore.player_items:
        print(player["name"], player["pts"])

You can combine this with pandas to build more complex reports.  ``game.boxscore.player_boxscore" returns a pandas ``DataFrame`` with one row per player.

Working with Play-By-Play and EnhancedPbp
-----------------------------------------

The raw play-by-play events (``game.pbp.items``) are lists of dictionaries that replicate the data from ``stats.nba.com``.  ``EnhancedPbp`` objects add extra context and normalize some common issues.  Each event is represented by a class under ``pbpstats.resources.enhanced_pbp``.

.. code-block:: python

    events = game.enhanced_pbp.items
    first = events[0]
    print(first.event_num, type(first).__name__, first.description)

Enhanced events expose helpers for things like ``lineup_ids`` and ``get_offense_team_id`` which are needed for possession parsing.  When ``Possessions`` is enabled, ``game.possessions.items`` is a list of :class:`pbpstats.resources.possessions.possession.Possession` objects built from enhanced events.

Aggregating Possession Statistics
---------------------------------

``Possessions`` includes convenience methods for aggregating stats across an entire game or collection of games.  The example below aggregates points by team for the game that was loaded above.

.. code-block:: python

    poss = game.possessions
    for stat in poss.team_stats:
        print(stat["team_id"], stat["stat_key"], stat["stat_value"])

The ``lineup_stats`` and ``player_stats`` properties aggregate by lineup or player respectively.  ``lineup_opponent_stats`` groups by the opposing lineup which is useful for on/off calculations.

Exporting Possession Data for RAPM
----------------------------------

The following example demonstrates a simple workflow for building a possession level CSV that can serve as the input to a RAPM model.  Each row contains identifiers for the lineup on offense and defense along with the points scored on that possession.

.. code-block:: python

    import csv
    from pbpstats.client import Client

    settings = {
        "dir": "/path/to/data_directory",
        "EnhancedPbp": {"source": "web", "data_provider": "stats_nba"},
        "Possessions": {"source": "web", "data_provider": "stats_nba"},
    }
    client = Client(settings)
    game = client.Game("0021900001")

    with open("rapm_possessions.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "game_id",
            "period",
            "number",
            "offense_lineup",
            "defense_lineup",
            "points",
        ])
        for poss in game.possessions.items:
            off_lineup = poss.lineup_id
            def_lineup = poss.opponent_lineup_id
            pts = sum(
                stat["stat_value"]
                for stat in poss.possession_stats
                if stat["team_id"] == poss.offense_team_id
                and stat["stat_key"] == "pts"
            )
            writer.writerow([
                poss.game_id,
                poss.period,
                poss.number,
                off_lineup,
                def_lineup,
                pts,
            ])

The resulting ``rapm_possessions.csv`` can be merged with data from other games or seasons and fed into a RAPM pipeline.  Lineup identifiers are ``"-"`` separated player ids sorted in string order (``"201939-202691-203110-203954-1627749"`` for example).  Splitting these into individual player columns produces the classic play by play matrix for RAPM.

Processing a Season of Games
----------------------------

The :class:`pbpstats.resources.games.Games` class can be used to obtain a list of game ids for a season or date range.  Combining this with the workflow above allows you to build a possession-level dataset for many games.  The snippet below downloads all regular season games for 2019 and exports a CSV with a row per possession.

.. code-block:: python

    from pbpstats.resources.games import Games

    settings = {
        "dir": "/path/to/data_directory",
        "EnhancedPbp": {"source": "web", "data_provider": "stats_nba"},
        "Possessions": {"source": "web", "data_provider": "stats_nba"},
    }
    client = Client(settings)
    season_games = Games("nba", season=2019, season_type="Regular Season")
    game_ids = [g["game_id"] for g in season_games.get_data()]

    with open("season_rapm_possessions.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "game_id",
            "period",
            "number",
            "offense_lineup",
            "defense_lineup",
            "points",
        ])
        for game_id in game_ids:
            game = client.Game(game_id)
            for poss in game.possessions.items:
                pts = sum(
                    stat["stat_value"]
                    for stat in poss.possession_stats
                    if stat["team_id"] == poss.offense_team_id
                    and stat["stat_key"] == "pts"
                )
                writer.writerow([
                    poss.game_id,
                    poss.period,
                    poss.number,
                    poss.lineup_id,
                    poss.opponent_lineup_id,
                    pts,
                ])

Running this script may take a while the first time because ``pbpstats`` will download the boxscore, play-by-play and shot data for each game.  Once cached locally, subsequent runs read from disk and are much faster.

Additional Helpers
------------------

* **Shots Data** – ``game.shots.items`` returns dictionaries with ``x``/``y`` coordinates and shot zone metadata.  You can merge this with possession stats to analyze shooting efficiency by lineup or player.
* **Overrides** – Issues with play by play ordering can be resolved by placing override JSON files in the ``overrides`` directory.  See the wiki for more details.
* **Live Data** – ``data_provider="live"`` can be used with some resources to fetch near real time stats during games.

Putting It All Together
-----------------------

``pbpstats`` provides flexible building blocks to go from raw NBA stats endpoints to enriched event data, possessions and aggregated statistics.  By configuring a ``Client`` with the resources you need and iterating over the resulting objects you can craft pipelines for advanced lineup metrics, shooting analysis and RAPM models with only a few dozen lines of code.

