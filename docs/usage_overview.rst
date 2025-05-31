Usage Overview
==============

This page provides a high level overview of the main features available in
``pbpstats`` and basic examples for using them.  If you are new to the package
start with :ref:`quickstart` and then use the sections below as a reference
for the available functionality.

Getting Data
------------

The easiest way to load data is through the :class:`pbpstats.client.Client`
class.  The client accepts a dictionary of settings describing which resources
should be loaded and where they should be loaded from.  Resources correspond to
the modules inside :mod:`pbpstats.resources` (``Boxscore``, ``Pbp``, ``EnhancedPbp``,
``Possessions``, ``Shots``, ``Games`` and ``Season``).

``source`` controls whether data is read from a directory on disk (``"file"``)
or pulled from the API (``"web"``).  ``data_provider`` specifies the provider to
use.  The two main providers are ``"stats_nba"`` and ``"data_nba"``; a ``"live"``
provider is also available for some endpoints.

The example below loads boxscore and possession data from files stored under
``/response_data``.

.. code-block:: python

    from pbpstats.client import Client

    settings = {
        "dir": "/response_data",
        "Boxscore": {"source": "file", "data_provider": "stats_nba"},
        "Possessions": {"source": "file", "data_provider": "stats_nba"},
    }
    client = Client(settings)
    game = client.Game("0021900001")

``game.boxscore.items`` now contains boxscore data and ``game.possessions.items``
holds a list of :class:`~pbpstats.resources.possessions.possession.Possession`
objects.

Resources
---------

* **Boxscore** – basic boxscore statistics.
* **Pbp** – raw play–by–play event data.
* **EnhancedPbp** – play–by–play with additional annotations such as score,
  fouls to give, possession information and lineups on the floor.
* **Possessions** – convenient wrapper that splits enhanced play–by–play into
  individual possessions with helpers for aggregating stats.
* **Shots** – shot level data from ``stats_nba``.
* **Games/Season** – utilities for fetching lists of games for a day or season.

For more details on each resource see the API documentation under
:mod:`pbpstats.resources` and :mod:`pbpstats.data_loader`.

.. _rapm-possession-example:

RAPM Ready Possession Data
--------------------------

The ``Possessions`` resource can be used to create possession level datasets that
are suitable for RAPM style analyses.  The example below demonstrates how to
load a game, iterate over the possessions and write a simplified CSV containing
one row per possession with the offense and defense lineups and the points scored
on that possession.

.. code-block:: python

    import csv
    from pbpstats.client import Client

    settings = {
        "dir": "/response_data",
        "Possessions": {"source": "file", "data_provider": "stats_nba"},
    }
    client = Client(settings)
    game = client.Game("0021900001")

    with open("possessions.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "game_id",
            "period",
            "possession_number",
            "offense_lineup",
            "defense_lineup",
            "points",
        ])
        for poss in game.possessions.items:
            offense = poss.lineup_id
            defense = poss.opponent_lineup_id
            points = sum(
                stat["stat_value"]
                for stat in poss.possession_stats
                if stat["team_id"] == poss.offense_team_id
                and stat["stat_key"] == "pts"
            )
            writer.writerow([
                poss.game_id,
                poss.period,
                poss.number,
                offense,
                defense,
                points,
            ])

The resulting ``possessions.csv`` can be used as the input to a RAPM model.  Each
row contains the lineup on offense, the lineup on defense and the points scored
on that possession so you can aggregate by lineup or explode the lineups into
player identifiers as required by your modelling approach.

Processing A Full Season
-----------------------

To generate a RAPM dataset for an entire season you can iterate over the
schedule and append possessions for each game.  The schedule resource
returns a list of game IDs that can be fed to ``client.Game`` one by one.
Below is a minimal example that writes a CSV containing all regular
season possessions.

.. code-block:: python

    import csv
    from pbpstats.client import Client

    settings = {
        "dir": "/response_data",
        "Games": {"source": "file", "data_provider": "stats_nba"},
        "Possessions": {"source": "file", "data_provider": "stats_nba"},
    }
    client = Client(settings)

    season = client.Season("nba", "2023-24", season_type="Regular Season")
    with open("season_possessions.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "game_id", "period", "possession_number",
            "offense_lineup", "defense_lineup", "points"
        ])
        for game_id in season.game_ids:
            game = client.Game(game_id)
            for poss in game.possessions.items:
                offense = poss.lineup_id
                defense = poss.opponent_lineup_id
                points = sum(
                    stat["stat_value"]
                    for stat in poss.possession_stats
                    if stat["team_id"] == poss.offense_team_id
                    and stat["stat_key"] == "pts"
                )
                writer.writerow([
                    poss.game_id, poss.period, poss.number,
                    offense, defense, points
                ])

Advanced: Building Possessions From EnhancedPbp
-----------------------------------------------

Occasionally the built in possession parser struggles with games that
have events out of order or missing data.  ``EnhancedPbp`` exposes every
play with additional metadata so you can build possessions manually when
needed.  The approach below loops over events, groups them by offense
team and detects possession boundaries.

.. code-block:: python

    from pbpstats.client import Client
    from pbpstats.resources.enhanced_pbp import FieldGoal, FreeThrow, Rebound, Turnover

    def to_possessions(events):
        possessions = []
        current = []
        offense = None
        for event in events:
            new_offense = getattr(event, "offense_team_id", offense)
            if offense is None:
                offense = new_offense
            if new_offense is not None and new_offense != offense:
                possessions.append(current)
                current = []
                offense = new_offense
            current.append(event)
            if (
                isinstance(event, FieldGoal) and event.is_made
            ) or isinstance(event, Turnover) or (
                isinstance(event, Rebound) and not getattr(event, "oreb", False)
            ):
                possessions.append(current)
                current = []
                offense = None
        if current:
            possessions.append(current)
        return possessions

    settings = {"EnhancedPbp": {"source": "web", "data_provider": "stats_nba"}}
    client = Client(settings)
    game = client.Game("0022300041")
    events = game.enhanced_pbp.items
    possessions = to_possessions(events)
    print(f"Found {len(possessions)} possessions")

Each element of ``possessions`` is a list of enhanced play-by-play
objects for one trip down the floor.  You can summarise each possession
by calculating points scored by the offense team, recording the lineup
information if available and creating a custom descriptor string.

Further Reading
---------------

Refer to the API docs under :mod:`pbpstats.resources` for detailed
attributes on the various objects.  The :mod:`pbpstats.objects` module
contains helper classes for teams and players which are automatically
attached to games when using :class:`Client`.  With these tools you can
extract virtually any game statistic or reconstruct possessions in a
manner suited to your analysis.
