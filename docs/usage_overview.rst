Usage Overview
==============

This guide provides a broad and practical tour through the most
important pieces of ``pbpstats``.  Many users primarily know the
package for breaking down play by play data into possessions, but it can
do considerably more.  The goal of this document is to cover the common
workflows and dive deeply into the RAPM-style possession exports that
were requested.  The topics covered are deliberately verbose and include
multiple code examples so that you can copy/paste and adapt them to your
own projects.

.. contents:: Table of Contents
    :depth: 2
    :local:

Getting Started
---------------

Installation is as straightforward as ``pip install pbpstats``.  The
package supports Python 3.8 and above.  In addition to the package
itself, consider installing ``pandas`` and ``numpy`` if you plan on
working with the data in a dataframe-based workflow.  Those packages are
not strict requirements of ``pbpstats`` but they make many of the
examples easier to follow.

A common pattern is to keep a local directory of API responses.  This
serves as a cache so that you do not repeatedly hit the stats APIs when
you run analyses multiple times.  It is also a convenient place to store
any manual corrections to the raw data.  The directory typically has the
following subdirectories::

    response_data/
        boxscore/
        pbp/
        enhanced_pbp/
        possessions/
        schedule/
        game_details/
        overrides/

The ``overrides`` folder is where you place files that fix known issues
with specific games (such as events that are out of order or incorrect
period starters).  The ``pbpstats`` wiki contains a collection of those
files if you want to download them directly.

Basic Usage Pattern
-------------------

The :class:`pbpstats.client.Client` class is the main entry point.  You
pass it a settings dictionary that indicates which resources you want
and where they should be loaded from.  "Resources" correspond to the
modules under :mod:`pbpstats.resources`.  The core ones are
``Boxscore``, ``Pbp``, ``EnhancedPbp``, ``Possessions``, ``Shots``, and
``Games``.  Each resource can load data either from disk
(``source='file'``) or directly from the API (``source='web'``).  When
you use ``source='web'`` you can still specify a ``dir`` so that the
response is cached locally for later use.

Here's a minimal example that loads boxscore and possession data for a
single game from local files::

    from pbpstats.client import Client

    settings = {
        "dir": "/response_data",
        "Boxscore": {"source": "file", "data_provider": "stats_nba"},
        "Possessions": {"source": "file", "data_provider": "stats_nba"},
    }
    client = Client(settings)
    game = client.Game("0021900001")

Once instantiated you can access ``game.boxscore.items`` for boxscore
rows and ``game.possessions.items`` for a list of
:class:`~pbpstats.resources.possessions.possession.Possession` objects.
Each possession object references the underlying events that belong to
the possession.

Anatomy of a Game Object
------------------------

A ``Game`` created through the client acts as a hub for the resources you
configured.  If you include ``Pbp`` and ``EnhancedPbp`` in your settings
you can access ``game.pbp.items`` (a list of play by play events) and
``game.enhanced_pbp.items`` (the same events plus added information
about lineups, fouls to give, possession start markers, and so on).  In
practice you will often work with ``EnhancedPbp`` even if your end goal
is to generate possession data because it includes lineup information
that the raw play by play lacks.

Every event class such as
:class:`~pbpstats.resources.enhanced_pbp.field_goal.FieldGoal` or
:class:`~pbpstats.resources.enhanced_pbp.rebound.Rebound` contains a rich
set of attributes: who was involved, the score at the time, whether a
shot was assisted, and more.  You can iterate over the events and check
their ``event_type`` or ``action_type`` properties to filter to the
specific events you care about.  The API docs under
:mod:`pbpstats.resources.enhanced_pbp` describe these classes in detail.

Working with Boxscore Data
--------------------------

Boxscore items provide team and player level summary statistics.  The
fields available depend on the data provider.  When using
``stats_nba`` you have access to the traditional box score columns along
with advanced stats.  Each entry is represented by a
:class:`~pbpstats.resources.boxscore.boxscore_item.BoxscoreItem` object.

Here's a quick example that prints the minutes played for each player::

    for item in game.boxscore.items:
        print(item.player_name, item.minutes)

Boxscore data can be aggregated across games by using the ``Season``
resource or by manually loading multiple games and combining their
``boxscore.items`` lists.  Because ``pbpstats`` represents the data as
simple Python objects you can easily convert them to dictionaries or
dataframes for further analysis.

Play by Play and Enhanced Play by Play
--------------------------------------

The ``Pbp`` resource gives you raw events exactly as they come from the
NBA endpoints.  Many of the events are out of order, missing crucial
information, or otherwise messy.  The ``EnhancedPbp`` resource cleans up
many of those issues, annotates additional properties (such as team on
offense, the lineup on the floor, which fouls cause free throws, etc.)
and also groups events into logical possessions.

When you iterate over ``game.enhanced_pbp.items`` you will encounter
objects like ``FieldGoal`` or ``Turnover``.  Each event exposes event
statistics through an ``event_stats`` list.  For example, a made three
pointer includes a stat entry for ``pts`` (+3) and one for ``fg3m`` (+1).
Those stats are later aggregated at the possession or game level.

Splitting into Possessions
--------------------------

Possession data is produced by running the enhanced play by play through
a possession data loader.  The resulting list of
:class:`~pbpstats.resources.possessions.possession.Possession` objects is
available as ``game.possessions.items``.  Each possession knows when it
started and ended, what the score margin was at both moments, which team
was on offense, the lineup IDs for both teams, and the events that
occurred.  ``pbpstats`` assigns a unique lineup identifier that is a
hyphen-separated string of player IDs sorted in order.  You can use that
identifier to join with lineup data from other sources.

Common possession attributes include:

``period``
    The quarter (1-4) or overtime number.
``number``
    The possession number within the game (starting at 1).
``lineup_id``
    Lineup on offense as a ``-`` separated string of player IDs.
``opponent_lineup_id``
    Defensive lineup.
``possession_stats``
    Aggregated stats for the possession broken down by player, team and
    lineup.
``events``
    The list of :class:`~pbpstats.resources.enhanced_pbp.enhanced_pbp_item.EnhancedPbpItem`
    objects belonging to this possession.

Aggregating Possessions
-----------------------

The :class:`~pbpstats.resources.possessions.possessions.Possessions`
container provides helpers to aggregate stats across the list of
possessions.  Properties include ``team_stats``, ``opponent_stats``,
``player_stats`` and ``lineup_stats``.  Each property returns a list of
dictionaries with ``stat_key`` and ``stat_value`` pairs grouped by the
appropriate identifier.

Example: compute points scored by each lineup for the game::

    lineup_pts = {
        row["lineup_id"]: row["stat_value"]
        for row in game.possessions.lineup_stats
        if row["stat_key"] == "pts"
    }
    print(lineup_pts)

Advanced Stats via Event Stats
------------------------------

Event objects and possession objects expose a list of statistics called
``event_stats`` or ``possession_stats``.  Each entry is a dictionary with
keys ``player_id``, ``team_id``, ``opponent_team_id``, ``lineup_id``,
``opponent_lineup_id`` and ``stat_key``/``stat_value``.  You can combine
these with ``itertools.groupby`` or with a library like ``pandas`` to
compute virtually any metric imaginable.

For example, to compute offensive rebounding rate by lineup::

    import pandas as pd

    stats = [stat for p in game.possessions.items for stat in p.possession_stats]
    df = pd.DataFrame(stats)
    grouped = df[df.stat_key == "oreb"].groupby("lineup_id")
    oreb = grouped.stat_value.sum() / grouped.stat_value.count()
    print(oreb)

Working with Shot Data
----------------------

The ``Shots`` resource pulls shot-level data from ``stats_nba``.  Each
``Shot`` item has coordinates, distance, shot value and whether it was
made.  Here's how to load shots for a game and create a simple hexbin
plot using ``matplotlib``::

    from pbpstats.client import Client
    import matplotlib.pyplot as plt

    settings = {
        "Shots": {"source": "web", "data_provider": "stats_nba"},
    }
    client = Client(settings)
    game = client.Game("0021900001")

    x = [s.x for s in game.shots.items]
    y = [s.y for s in game.shots.items]
    plt.hexbin(x, y, gridsize=30, extent=(-25, 25, -5, 45))
    plt.show()

Game and Season Utilities
-------------------------

To process multiple games you can use the ``Games`` and ``Season``
resources.  ``Games`` can fetch schedules for a given day or return the
full set of games for a season.  ``Season`` additionally exposes helpers
like ``final_games`` which only returns games that have a final status.

Example: get all games for a specific season and save their IDs::

    settings = {
        "Games": {"source": "web", "data_provider": "data_nba"},
    }
    client = Client(settings)
    season = client.Season("nba", "2019-20", "Regular Season")
    game_ids = [g.game_id for g in season.games.final_games]

    with open("game_ids.txt", "w") as f:
        for gid in game_ids:
            f.write(gid + "\n")

Fetching Data for a Day works similarly::

    settings = {
        "Games": {"source": "web", "data_provider": "stats_nba"},
    }
    client = Client(settings)
    day = client.Day("12/05/2019", "nba")
    for final_game in day.games.final_games:
        print(final_game)

Live Resources
--------------

Some endpoints provide "live" data that updates during games.  The
``live`` data provider works with a subset of resources
(``Boxscore``, ``Pbp``, ``EnhancedPbp`` and ``Possessions``).  To use it
simply specify ``data_provider='live'`` and ``source='web'``.  Because
the data changes as the game progresses you may want to repeatedly
instantiate the client and reload the same game ID at intervals.

Pandemic Note: the ``live`` provider sometimes changes the structure of
its responses between seasons, so double check the field names if you
encounter errors.

Example::

    settings = {
        "dir": "/response_data",
        "Pbp": {"source": "web", "data_provider": "live"},
    }
    client = Client(settings)
    game = client.Game("0021900001")
    latest_event = game.pbp.items[-1]
    print(latest_event.description)

Using Pandas with Possessions
-----------------------------

Many analysts prefer to use ``pandas`` for exploration.  Below is a more
complete workflow that loads possession data for multiple games, puts it
into a dataframe and computes a custom statistic.

.. code-block:: python

    import pandas as pd
    from pbpstats.client import Client

    settings = {
        "dir": "/response_data",
        "Possessions": {"source": "file", "data_provider": "stats_nba"},
    }
    client = Client(settings)
    game_ids = ["0021900001", "0021900002", "0021900003"]

    records = []
    for gid in game_ids:
        game = client.Game(gid)
        for poss in game.possessions.items:
            stats = {
                "game_id": poss.game_id,
                "period": poss.period,
                "number": poss.number,
                "offense": poss.lineup_id,
                "defense": poss.opponent_lineup_id,
                "pts": sum(
                    s["stat_value"]
                    for s in poss.possession_stats
                    if s["team_id"] == poss.offense_team_id
                    and s["stat_key"] == "pts"
                ),
            }
            records.append(stats)

    df = pd.DataFrame(records)
    offensive_rating = (
        df.groupby("offense")["pts"].sum()
        / df.groupby("offense")["number"].count()
        * 100
    )
    print(offensive_rating.sort_values(ascending=False).head())

Step-by-Step RAPM Export
------------------------

The following example shows in painstaking detail how to export
possession level data suitable for RAPM or other regularized adjusted
plus/minus models.  It expands on the brief snippet earlier in this
file.

1. **Load the Game**

   Use the ``Possessions`` resource with ``stats_nba`` or ``data_nba`` as
   the provider.  For RAPM analysis the provider typically does not
   matter because the possession data is derived from enhanced play by
   play which is nearly identical between the two.

   .. code-block:: python

       from pbpstats.client import Client

       settings = {
           "dir": "/response_data",
           "Possessions": {"source": "file", "data_provider": "stats_nba"},
       }
       client = Client(settings)
       game = client.Game("0021900001")

2. **Iterate over the Possessions**

   Each possession exposes ``lineup_id`` and ``opponent_lineup_id``.  The
   string values use ``-`` between the sorted player IDs.  A helper
   function can split the lineup into individual players if you need
   player-level exposures.

   .. code-block:: python

       def parse_lineup(lineup_id):
           return lineup_id.split("-") if lineup_id else []

       for poss in game.possessions.items:
           offense_players = parse_lineup(poss.lineup_id)
           defense_players = parse_lineup(poss.opponent_lineup_id)
           # do something with the lists

3. **Compute Points Scored**

   The ``possession_stats`` property contains all the aggregated event
   stats for that possession.  To get the points scored by the offensive
   team use a comprehension like the one below.

   .. code-block:: python

       def possession_points(poss):
           return sum(
               stat["stat_value"]
               for stat in poss.possession_stats
               if stat["team_id"] == poss.offense_team_id
               and stat["stat_key"] == "pts"
           )

4. **Collect the Output Rows**

   Build a dictionary for each possession containing whatever fields you
   want.  The minimal RAPM dataset usually includes the offensive lineup,
   the defensive lineup and points scored.

   .. code-block:: python

       rows = []
       for poss in game.possessions.items:
           rows.append(
               {
                   "game_id": poss.game_id,
                   "period": poss.period,
                   "number": poss.number,
                   "offense": poss.lineup_id,
                   "defense": poss.opponent_lineup_id,
                   "points": possession_points(poss),
               }
           )

5. **Write to CSV**

   Use Python's built in ``csv`` module (or ``pandas``) to write the
   rows.  The snippet below uses ``csv`` for clarity.

   .. code-block:: python

       import csv

       with open("possessions.csv", "w", newline="") as f:
           writer = csv.DictWriter(
               f,
               fieldnames=["game_id", "period", "number", "offense", "defense", "points"],
           )
           writer.writeheader()
           for row in rows:
               writer.writerow(row)

6. **Verify the Output**

   After running the script open ``possessions.csv`` in a spreadsheet to
   confirm the lineups and point totals match your expectations.  It is a
   good idea to spot check a few possessions in the original play by play
   to make sure your local data cache is correct.

Going Further
-------------

The ``pbpstats`` package includes additional helpers for advanced
statistics.  ``Possession`` objects know about the event that ended the
possession (``possession_ending_event``) so you can filter by turnovers
versus made shots or by the type of rebound.  The ``lineup_stats``
aggregations automatically divide some stats by five when summing across
players so that lineup totals equal team totals.  There are also modules
for interacting with the NBA Stats API directly if you need endpoints
that the client does not cover.

For a deep dive into the underlying data loaders see the docs under
:mod:`pbpstats.data_loader`.  Each loader class details the exact fields
available in the raw responses.  You can subclass the loaders if you
need to tweak how files are read or how requests are made.

Common Pitfalls
---------------

* **Missing Data** – Some older seasons lack certain statistics,
  especially shot distances or advanced box score fields.  Check the
  ``data_nba`` vs ``stats_nba`` provider difference if something is
  missing.
* **Event Order Problems** – Occasionally an event in the play by play is
  out of order.  ``pbpstats`` attempts to fix many of these, but if you
  run into parsing errors you may need to manually edit the pbp file in
  your data directory.
* **Unusual Lineups** – The package uses the players on the court at the
  start of the possession to determine the lineup.  Late substitutions
  during a free throw sequence can lead to split-second lineups that may
  or may not be relevant depending on your analysis.

Tips and Tricks
---------------

* Use the ``dir`` cache even when downloading from the web so you avoid
  re-downloading the same data every run.
* Keep a set of override files handy in your ``overrides`` directory to
  patch known issues with older games.
* If you work with ``pandas`` frequently, consider creating helper
  functions that convert ``Possession`` or ``Boxscore`` objects directly
  into dataframes.  The objects are simple enough that ``pd.DataFrame``
  usually works out of the box.
* ``pbpstats`` returns IDs as strings to match the official NBA Stats
  identifiers.  Be mindful of this when merging datasets from other
  sources.

Reference Materials
-------------------

* :ref:`quickstart` – step-by-step installation and initial data pull.
* :mod:`pbpstats.resources` – API documentation for each resource class.
* `GitHub Repository <https://github.com/dblackrun/pbpstats>`_ – source
  code and issue tracker.
* `Override Files Wiki <https://github.com/dblackrun/pbpstats/wiki/Overrides-to-fix-issues-parsing-pbp>`_
  – list of known play by play fixes.

Changelog
---------

``v1`` – Initial creation of the overview and RAPM example.

``v2`` – Expanded guide with detailed explanations, pandas workflow and
        additional tips.

``v3`` – This version, significantly lengthened with even more step by
        step descriptions and a comprehensive RAPM export section.


Appendix: Full Example Script
-----------------------------

For convenience, below is a consolidated script that ties together many
of the concepts discussed above.  It downloads possessions for an entire
season, aggregates lineup statistics and writes both possession-level and
lineup-level CSV files.  Use it as a template for your own workflows.

.. code-block:: python

    import csv
    from pbpstats.client import Client

    SETTINGS = {
        "dir": "/response_data",
        "Possessions": {"source": "file", "data_provider": "stats_nba"},
        "Games": {"source": "web", "data_provider": "data_nba"},
    }

    client = Client(SETTINGS)
    season = client.Season("nba", "2019-20", "Regular Season")

    possession_rows = []
    lineup_totals = {}
    for game_info in season.games.final_games:
        game = client.Game(game_info.game_id)
        for poss in game.possessions.items:
            pts = sum(
                s["stat_value"]
                for s in poss.possession_stats
                if s["team_id"] == poss.offense_team_id
                and s["stat_key"] == "pts"
            )
            possession_rows.append(
                [
                    poss.game_id,
                    poss.period,
                    poss.number,
                    poss.lineup_id,
                    poss.opponent_lineup_id,
                    pts,
                ]
            )
            lineup_totals.setdefault(poss.lineup_id, 0)
            lineup_totals[poss.lineup_id] += pts

    with open("season_possessions.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "game_id",
            "period",
            "possession_number",
            "offense_lineup",
            "defense_lineup",
            "points",
        ])
        writer.writerows(possession_rows)

    with open("lineup_totals.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["lineup_id", "points"])
        for lineup_id, pts in lineup_totals.items():
            writer.writerow([lineup_id, pts])

This script purposely omits error handling for brevity, but in practice
you may want to add retry logic around the network calls and command
line arguments for selecting the season.  The intent is to demonstrate
a larger end-to-end example so that you can see how all of the pieces
fit together in a real project.

