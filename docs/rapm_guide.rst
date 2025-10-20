.. _rapm_guide:

=====================================
Calculating NBA RAPM with pbpstats
=====================================

This guide explains how to use pbpstats to calculate Regularized Adjusted Plus-Minus (RAPM),
one of the most sophisticated player evaluation metrics in basketball analytics.

What is RAPM?
=============

**Regularized Adjusted Plus-Minus (RAPM)** is an advanced statistical metric that estimates
each player's contribution to their team's point differential while controlling for the quality
of teammates and opponents.

Key Concepts
------------

1. **Plus-Minus**: The point differential while a player is on the court

   * Traditional plus-minus is heavily influenced by teammates and opponents
   * A great player with poor teammates might have negative plus-minus
   * A mediocre player with excellent teammates might have positive plus-minus

2. **Adjusted Plus-Minus (APM)**: Uses regression to estimate individual player value

   * Accounts for all 10 players on court simultaneously
   * Solves: "What individual contributions best explain the observed point differentials?"
   * Requires solving a large system of equations (one per player)

3. **Regularization**: Stabilizes estimates for players with limited playing time

   * Uses ridge regression (L2 penalty) to prevent overfitting
   * Shrinks extreme values toward zero (the average player value)
   * Balances sample size with estimation accuracy

Why RAPM Matters
----------------

* **Context-Aware**: Accounts for who players play with and against
* **Predictive**: Better predictor of future performance than box score stats
* **Comprehensive**: Uses every possession, not just scoring events
* **Unbiased**: Doesn't rely on subjective judgments about what stats matter

How RAPM Works
==============

Mathematical Foundation
-----------------------

RAPM solves a ridge regression problem:

.. code-block:: text

    minimize: ||y - Xβ||² + λ||β||²

    Where:
    - y = vector of point differentials for each possession
    - X = design matrix (players on court)
    - β = player effects we're solving for
    - λ = regularization parameter (typically 0.01 to 5000)

For each possession:

* Offensive team's 5 players get +1 in their columns
* Defensive team's 5 players get -1 in their columns
* Target value is points scored by offensive team on that possession

Example
-------

Possession with Lakers on offense (LeBron, AD, Russ, etc.) vs Warriors defense (Steph, Klay, Dray, etc.):

.. code-block:: text

    If Lakers score 2 points:

    X matrix row: [+1, +1, +1, +1, +1, -1, -1, -1, -1, -1, 0, 0, ...]
                  [LeBron, AD, Russ, ...  , Steph, Klay, Dray, ..., other players]

    y value: 2

After solving for all possessions across a season, each player's coefficient (β value)
represents their estimated contribution in points per possession.

Data Requirements
=================

To calculate RAPM, you need:

1. **Possession-level data** with lineups for both teams
2. **Point differentials** or points scored for each possession
3. **Multiple games** (ideally a full season or more)
4. **Sufficient playing time** for each player (regularization helps with limited data)

pbpstats provides all of this through the ``Possessions`` resource.

Implementation Guide
====================

Step 1: Install Dependencies
-----------------------------

In addition to pbpstats, you'll need:

.. code-block:: bash

    pip install pbpstats numpy scikit-learn pandas

Step 2: Load Season Data
-------------------------

First, load possession data for all games in a season:

.. code-block:: python

    from pbpstats.client import Client
    import numpy as np
    from sklearn.linear_model import Ridge
    from collections import defaultdict

    # Configure client
    settings = {
        "dir": "/response_data",  # Optional: cache responses
        "Possessions": {"source": "web", "data_provider": "data_nba"},
        "Games": {"source": "web", "data_provider": "data_nba"},
    }
    client = Client(settings)

    # Load all games for a season
    season = client.Season("nba", "2023-24", "Regular Season")
    game_ids = [game['game_id'] for game in season.games.final_games]

    print(f"Found {len(game_ids)} games")

Step 3: Extract Possession Data
--------------------------------

Process each game to extract lineup and scoring information:

.. code-block:: python

    possession_data = []

    for game_id in game_ids:
        try:
            game = client.Game(game_id)

            for possession in game.possessions.items:
                # Get lineup for offensive team
                off_team_id = possession.offense_team_id
                def_team_id = possession.defense_team_id

                # Extract lineup IDs from first event
                if possession.events:
                    first_event = possession.events[0]

                    # Get 5-man lineups
                    off_lineup = first_event.lineup_ids.get(off_team_id, "")
                    def_lineup = first_event.lineup_ids.get(def_team_id, "")

                    if off_lineup and def_lineup:
                        # Get points scored on this possession
                        points = possession.get_offense_team_points()

                        possession_data.append({
                            'off_lineup': off_lineup,
                            'def_lineup': def_lineup,
                            'points': points,
                        })
        except Exception as e:
            print(f"Error processing game {game_id}: {e}")
            continue

    print(f"Collected {len(possession_data)} possessions")

Step 4: Build Design Matrix
----------------------------

Create the player-possession matrix:

.. code-block:: python

    # Extract all unique players
    all_players = set()
    for poss in possession_data:
        off_players = poss['off_lineup'].split('-')
        def_players = poss['def_lineup'].split('-')
        all_players.update(off_players)
        all_players.update(def_players)

    # Remove empty strings
    all_players.discard('')
    all_players = sorted(list(all_players))
    player_to_idx = {player: idx for idx, player in enumerate(all_players)}

    print(f"Found {len(all_players)} unique players")

    # Build design matrix
    n_possessions = len(possession_data)
    n_players = len(all_players)

    X = np.zeros((n_possessions, n_players))
    y = np.zeros(n_possessions)

    for i, poss in enumerate(possession_data):
        # Offensive players get +1
        for player_id in poss['off_lineup'].split('-'):
            if player_id and player_id in player_to_idx:
                X[i, player_to_idx[player_id]] = 1.0

        # Defensive players get -1
        for player_id in poss['def_lineup'].split('-'):
            if player_id and player_id in player_to_idx:
                X[i, player_to_idx[player_id]] = -1.0

        # Target is points scored
        y[i] = poss['points']

    print(f"Design matrix shape: {X.shape}")

Step 5: Solve for RAPM
-----------------------

Use ridge regression to estimate player effects:

.. code-block:: python

    # Set regularization parameter
    # Common values: 0.01 to 5000
    # Higher = more regularization = more shrinkage toward zero
    lambda_value = 1000.0

    # Fit ridge regression
    ridge_model = Ridge(alpha=lambda_value, fit_intercept=False)
    ridge_model.fit(X, y)

    # Extract player coefficients
    rapm_coefficients = ridge_model.coef_

    # Create results dictionary
    rapm_results = {}
    for player_id, idx in player_to_idx.items():
        rapm_per_possession = rapm_coefficients[idx]
        rapm_per_100 = rapm_per_possession * 100  # Scale to per-100-possessions
        rapm_results[player_id] = {
            'rapm_per_possession': rapm_per_possession,
            'rapm_per_100': rapm_per_100,
        }

Step 6: Analyze Results
-----------------------

View and interpret the RAPM values:

.. code-block:: python

    import pandas as pd

    # Convert to DataFrame for easier viewing
    df = pd.DataFrame.from_dict(rapm_results, orient='index')
    df.index.name = 'player_id'
    df = df.sort_values('rapm_per_100', ascending=False)

    print("Top 10 Players by RAPM (per 100 possessions):")
    print(df.head(10))

    print("\nBottom 10 Players by RAPM (per 100 possessions):")
    print(df.tail(10))

Complete Example Script
========================

Here's a complete script that calculates RAPM for a season:

.. code-block:: python

    """
    Calculate NBA RAPM for a season using pbpstats
    """
    from pbpstats.client import Client
    import numpy as np
    from sklearn.linear_model import Ridge
    import pandas as pd

    def calculate_rapm(season_type="2023-24", league="nba", season_segment="Regular Season"):
        """Calculate RAPM for an NBA season"""

        # Setup client
        settings = {
            "Possessions": {"source": "web", "data_provider": "data_nba"},
            "Games": {"source": "web", "data_provider": "data_nba"},
        }
        client = Client(settings)

        # Get all games
        print(f"Loading {league} {season_type} {season_segment}...")
        season = client.Season(league, season_type, season_segment)
        game_ids = [game['game_id'] for game in season.games.final_games]
        print(f"Found {len(game_ids)} games\n")

        # Collect possession data
        print("Processing possessions...")
        possession_data = []

        for idx, game_id in enumerate(game_ids):
            if idx % 50 == 0:
                print(f"  Processed {idx}/{len(game_ids)} games...")

            try:
                game = client.Game(game_id)

                for possession in game.possessions.items:
                    off_team_id = possession.offense_team_id
                    def_team_id = possession.defense_team_id

                    if possession.events:
                        first_event = possession.events[0]
                        off_lineup = first_event.lineup_ids.get(off_team_id, "")
                        def_lineup = first_event.lineup_ids.get(def_team_id, "")

                        if off_lineup and def_lineup:
                            points = possession.get_offense_team_points()
                            possession_data.append({
                                'off_lineup': off_lineup,
                                'def_lineup': def_lineup,
                                'points': points,
                            })
            except Exception as e:
                print(f"  Error processing game {game_id}: {e}")
                continue

        print(f"Collected {len(possession_data)} possessions\n")

        # Extract unique players
        print("Building design matrix...")
        all_players = set()
        for poss in possession_data:
            all_players.update(poss['off_lineup'].split('-'))
            all_players.update(poss['def_lineup'].split('-'))
        all_players.discard('')
        all_players = sorted(list(all_players))
        player_to_idx = {player: idx for idx, player in enumerate(all_players)}
        print(f"Found {len(all_players)} unique players\n")

        # Build matrices
        n_possessions = len(possession_data)
        n_players = len(all_players)
        X = np.zeros((n_possessions, n_players))
        y = np.zeros(n_possessions)

        for i, poss in enumerate(possession_data):
            for player_id in poss['off_lineup'].split('-'):
                if player_id in player_to_idx:
                    X[i, player_to_idx[player_id]] = 1.0

            for player_id in poss['def_lineup'].split('-'):
                if player_id in player_to_idx:
                    X[i, player_to_idx[player_id]] = -1.0

            y[i] = poss['points']

        # Calculate RAPM
        print("Calculating RAPM...")
        lambda_value = 1000.0
        ridge_model = Ridge(alpha=lambda_value, fit_intercept=False)
        ridge_model.fit(X, y)

        # Format results
        rapm_results = {}
        for player_id, idx in player_to_idx.items():
            rapm_results[player_id] = ridge_model.coef_[idx] * 100

        # Create DataFrame
        df = pd.DataFrame.from_dict(rapm_results, orient='index', columns=['RAPM'])
        df.index.name = 'player_id'
        df = df.sort_values('RAPM', ascending=False)

        return df

    if __name__ == "__main__":
        rapm_df = calculate_rapm("2023-24", "nba", "Regular Season")

        print("\n=== Top 20 Players by RAPM ===")
        print(rapm_df.head(20))

        print("\n=== Bottom 20 Players by RAPM ===")
        print(rapm_df.tail(20))

        # Save to CSV
        rapm_df.to_csv("nba_rapm_2023_24.csv")
        print("\nResults saved to nba_rapm_2023_24.csv")

Advanced Topics
===============

Offensive and Defensive RAPM
-----------------------------

You can split RAPM into offensive and defensive components:

.. code-block:: python

    # For offensive RAPM: only use +1 for offensive players
    X_offense = np.zeros((n_possessions, n_players))
    for i, poss in enumerate(possession_data):
        for player_id in poss['off_lineup'].split('-'):
            if player_id in player_to_idx:
                X_offense[i, player_to_idx[player_id]] = 1.0

    # Fit offensive model
    off_model = Ridge(alpha=lambda_value, fit_intercept=True)
    off_model.fit(X_offense, y)
    offensive_rapm = off_model.coef_ * 100

    # For defensive RAPM: only use +1 for defensive players
    # (note: defensive RAPM is typically reported as positive = better defense)
    X_defense = np.zeros((n_possessions, n_players))
    for i, poss in enumerate(possession_data):
        for player_id in poss['def_lineup'].split('-'):
            if player_id in player_to_idx:
                X_defense[i, player_to_idx[player_id]] = 1.0

    # Fit defensive model (negate y because we want points prevented)
    def_model = Ridge(alpha=lambda_value, fit_intercept=True)
    def_model.fit(X_defense, -y)
    defensive_rapm = def_model.coef_ * 100

Multi-Year RAPM
---------------

For more stable estimates, combine multiple seasons:

.. code-block:: python

    seasons = ["2021-22", "2022-23", "2023-24"]
    all_possession_data = []

    for season_year in seasons:
        season = client.Season("nba", season_year, "Regular Season")
        game_ids = [game['game_id'] for game in season.games.final_games]

        # Process games... (same as before)
        # all_possession_data.extend(possession_data)

    # Then build matrix and calculate RAPM as usual

Choosing Lambda (Regularization Parameter)
-------------------------------------------

The lambda parameter controls the regularization strength:

* **Low lambda (0.01-10)**: Less regularization

  * Pros: Captures true skill differences better
  * Cons: Unstable for players with limited minutes

* **High lambda (1000-5000)**: More regularization

  * Pros: More stable, especially for role players
  * Cons: May under-estimate star players

You can use cross-validation to choose lambda:

.. code-block:: python

    from sklearn.model_selection import cross_val_score

    lambdas = [0.01, 0.1, 1, 10, 100, 1000, 5000]
    scores = []

    for lam in lambdas:
        model = Ridge(alpha=lam, fit_intercept=False)
        cv_scores = cross_val_score(model, X, y, cv=5,
                                     scoring='neg_mean_squared_error')
        scores.append(-cv_scores.mean())

    best_lambda = lambdas[np.argmin(scores)]
    print(f"Best lambda: {best_lambda}")

Filtering by Playing Time
--------------------------

For more meaningful results, filter out players with minimal playing time:

.. code-block:: python

    # Count possessions per player
    possessions_played = defaultdict(int)

    for poss in possession_data:
        for player_id in poss['off_lineup'].split('-'):
            if player_id:
                possessions_played[player_id] += 1
        for player_id in poss['def_lineup'].split('-'):
            if player_id:
                possessions_played[player_id] += 1

    # Filter players with < 500 possessions
    min_possessions = 500
    qualified_players = {p for p, count in possessions_played.items()
                        if count >= min_possessions}

    # Rebuild matrix with only qualified players
    # ... (same matrix building process)

Interpreting RAPM Values
=========================

What the Numbers Mean
---------------------

RAPM is expressed in points per 100 possessions:

* **+5.0**: Elite (top 5-10 players in the league)
* **+3.0**: All-Star level
* **+1.0**: Above average starter
* **0.0**: Average NBA player (by definition)
* **-1.0**: Below average / end of bench
* **-3.0**: Significant negative impact

Example Interpretation
----------------------

If a player has RAPM of +4.2:

    "When this player is on the court, their team outscores opponents by an
    additional 4.2 points per 100 possessions, controlling for teammates and
    opponents."

Limitations of RAPM
-------------------

1. **Sample Size**: Requires significant playing time for stable estimates
2. **Multicollinearity**: Players who always play together are hard to separate
3. **No Context**: Doesn't explain *why* a player is valuable
4. **Coaching Effects**: Can't separate player skill from coaching/system
5. **Injury**: May reflect team composition changes during injury periods

Combining with Other Metrics
-----------------------------

RAPM works best alongside other metrics:

* **Box Score Stats**: Show *what* players do
* **On/Off Stats**: Simpler but less controlled comparison
* **Tracking Data**: Provide defensive metrics RAPM can't capture
* **Win Shares**: Team success context

Best Practices
==============

Data Quality
------------

1. **Use full seasons**: At least 1,000+ games for stable estimates
2. **Filter by playing time**: Exclude players with <500 possessions
3. **Handle trades carefully**: Players changing teams mid-season
4. **Check for data issues**: Missing lineups, incorrect scores

Performance Optimization
------------------------

1. **Cache game data**: Use pbpstats' file caching (``source: "file"``)
2. **Parallel processing**: Process games in parallel
3. **Use sparse matrices**: For large datasets (``scipy.sparse``)
4. **Incremental updates**: Add new games without reprocessing all

.. code-block:: python

    from scipy.sparse import lil_matrix

    # For large datasets, use sparse matrices
    X_sparse = lil_matrix((n_possessions, n_players))
    # ... (same filling process)

Validation
----------

1. **Cross-validation**: Check out-of-sample prediction accuracy
2. **Correlation with wins**: RAPM should correlate with team success
3. **Stability over time**: Compare season splits
4. **Eye test**: Do results match expert opinions for known players?

References and Further Reading
===============================

Key Papers
----------

* Rosenbaum, D. (2004). "Measuring How NBA Players Help Their Teams Win"
* Sill, J. (2010). "Improved NBA Adjusted +/- using Regularization and Out-of-Sample Testing"
* Ilardi, S. & Barzilai, A. (2008). "Adjusted Plus-Minus Ratings: New and Improved for 2007-2008"

Online Resources
----------------

* `APBRmetrics Forum <http://www.apbr.org/metrics/>`_ - Statistical basketball analysis
* `Basketball Reference <https://www.basketball-reference.com/>`_ - Historical stats
* `pbpstats.com <https://www.pbpstats.com/>`_ - Advanced NBA statistics

Related Metrics
---------------

* **RPM (Real Plus-Minus)**: ESPN's proprietary version with box score priors
* **PIPM (Player Impact Plus-Minus)**: Combines RAPM with box score stats
* **RAPTOR**: FiveThirtyEight's player rating system
* **LEBRON**: BBall Index's comprehensive player metric

Summary
=======

RAPM is a powerful metric for evaluating NBA players that:

1. Uses every possession to estimate player value
2. Controls for teammates and opponents
3. Requires regularization for stability
4. Works best with large sample sizes
5. Complements rather than replaces other metrics

The pbpstats library provides all the data needed to calculate RAPM:

* Detailed possession-level tracking
* Accurate lineup information
* Scoring data for each possession
* Support for multiple seasons and leagues

With the code examples in this guide, you can calculate RAPM for any NBA season
and gain deeper insights into player contributions beyond traditional statistics.
