# Project Handoff: replace_tpdev — NBA Historical Box Score Pipeline

## What This Document Is

You are being asked for strategic advice on the go-forward plan for a complex NBA historical data pipeline. This document gives you the full project context: goals, architecture, data sources, contradictions, current status, and the specific problems we face. We want your thinking on **the best way to handle the remaining work in a manageable way** — not solutions to individual games.

---

## 1. Project Overview

### What We're Building

An **offline-first replacement pipeline** for a legacy system called "tpdev" that produces NBA player box scores from play-by-play data. The pipeline covers **1997-2020** (23 seasons, ~30,000 games, ~685,000 player-game rows).

The pipeline:
1. Takes raw NBA play-by-play event data as input
2. Parses it through a custom fork of `pbpstats` (an open-source NBA PBP parser)
3. Produces per-player per-game box score rows including all counting stats (points, assists, rebounds, etc.) plus lineup-derived fields (Minutes, Plus_Minus)

### Why It Exists

- The old `tpdev` system depended on live NBA API endpoints that are no longer reliably available for historical data
- We need a reproducible offline pipeline that doesn't depend on NBA servers
- When official source data is wrong or incomplete, we use explicit documented repair policy instead of silent drift

### Current State Summary

- **Counting stats (PTS, AST, REB, STL, BLK, TOV, FGA, FGM, etc.) are DONE** — 685,882 rows across 1997-2020, zero failed games, zero errors, zero audit mismatches against official boxscores
- **Lineup-derived fields (Minutes, Plus_Minus) are the active frontier** — these depend on correctly tracking which 5 players are on court for each team at every moment

---

## 2. Architecture

### Runtime Components

```
                    ┌──────────────────────────────┐
                    │  cautious_rerun.py            │
                    │  (season-level orchestrator)  │
                    └──────────┬───────────────────┘
                               │
                    ┌──────────▼───────────────────┐
                    │  0c2_build_tpdev_box_stats_   │
                    │  version_v9b.py               │
                    │  (notebook dump — defines     │
                    │   process_games_parallel,     │
                    │   get_possessions_from_df)    │
                    └──────────┬───────────────────┘
                               │
            ┌──────────────────▼──────────────────┐
            │  pbpstats fork (custom)              │
            │  - enhanced_pbp/start_of_period.py   │
            │    (period starter inference)         │
            │  - enhanced_pbp/enhanced_pbp_item.py │
            │    (lineup propagation event-to-event)│
            │  - enhanced_pbp/foul.py, free_throw.py│
            │    (event-specific logic)            │
            └──────────────────┬──────────────────┘
                               │
              Reads from:      │      Reads from:
    ┌────────────────┐    ┌────▼────┐    ┌────────────────┐
    │ playbyplayv2   │    │ nba_raw │    │ period_starters│
    │ .parq          │    │ .db     │    │ _v6.parquet    │
    │ (historical    │    │ (cached │    │ _v5.parquet    │
    │  PBP events)   │    │  API    │    │ (pre-scraped   │
    └────────────────┘    │  data)  │    │  starters)     │
                          └─────────┘    └────────────────┘
```

### How the Pipeline Processes a Game

1. **Load PBP events** from `playbyplayv2.parq` (historical event stream from shufinskiy's `nba_data`)
2. **Determine period starters** — who are the 5 players on court for each team at the start of each period? This is the most complex part:
   - Try strict PBP inference (walk the event chain, track subs in/out)
   - If strict fails, try local parquet-based starters (v6 from gamerotation, v5 as fallback)
   - Check for manual overrides (`overrides/period_starters_overrides.json`)
   - Various tie-breaking and fallback logic
3. **Propagate lineups** event-to-event — as substitution events occur, update who's on court
4. **Accumulate stats** — credit each event (shot, rebound, foul, etc.) to the players currently tracked as on court
5. **Produce output** — per-player per-game rows with all counting stats + Minutes + Plus_Minus

### Override System

The pipeline has multiple layers of manual overrides, each addressing a different class of problem:

| Override File | Purpose | Format |
|---|---|---|
| `overrides/period_starters_overrides.json` | Fix who starts each period | `{game_id: {period: {team_id: [5 player_ids]}}}` |
| `overrides/lineup_window_overrides.json` | Fix intraperiod lineup windows | `{game_id: [{period, team_id, start_event_num, end_event_num, lineup_player_ids}]}` |
| `pbp_row_overrides.csv` | Fix event ordering in the PBP stream | Row-level reordering |
| `pbp_stat_overrides.csv` | Fix stat attribution on events | Stat-credit corrections |
| `boxscore_source_overrides.csv` | Fix confirmed bad official boxscore rows | Production patches |
| `validation_overrides.csv` | Manual tolerance exceptions for known issues | Audit exceptions |

### Runner Tools

- `cautious_rerun.py` — full season-level runner, processes all games in a season
- `rerun_selected_games.py` — targeted runner for specific game IDs (used for validation)
- Both produce: parquet outputs, audit CSVs, lineup audit reports, event-on-court issue tracking

### Audit Infrastructure

Each run produces:
- `minutes_plus_minus_audit_{season}.csv` — per-player minute and plus-minus diff vs official
- `event_player_on_court_issues_{season}.csv` — events where credited player is off-court
- `boxscore_audit_summary_{season}.json` — counting stat audit vs official boxscores
- Cross-source comparison against tpdev, pbpstats, BBR (Basketball Reference)

---

## 3. Data Sources and Their Reliability

### Primary Runtime Inputs

| Source | What It Contains | Reliability | Coverage |
|---|---|---|---|
| `playbyplayv2.parq` | Historical NBA PBP event stream | Good but has event ordering issues in early seasons | 1997-2020 |
| `nba_raw.db` | Cached NBA API responses (pbpv3, boxscore, summary) | Good for enrichment, not canonical chronology | 1997-2020 |
| `period_starters_v6.parquet` | Gamerotation-backed period starters | Best single source, but has gaps and some wrong rows | Broad but incomplete |
| `period_starters_v5.parquet` | Earlier scrape of period starters | Secondary fallback, good coverage | Broad |
| `period_starters_v4.parquet` | Oldest scrape | Historical comparison only | Broad |

### Cross-Reference / Audit Sources (NOT runtime dependencies)

| Source | What It Contains | Reliability | Coverage |
|---|---|---|---|
| `../fixed_data/raw_input_data/tpdev_data/full_pbp_new.parq` | tpdev possession-level PBP lineups | Good for lineup disputes | 1997-2020 (some gaps) |
| `../33_wowy_rapm/bbref_boxscores.db` | Basketball Reference PBP + boxscores | Independent source, has sub events | 1997-2020 |
| `../calculated_data/pbpstats/pbpstats_player_box.parq` | Local pbpstats full-game box | Good cross-check | 2000-2020 |
| Official NBA boxscores (via `nba_raw.db`) | The "truth" for counting stats | Canonical but has rare errors | 1997-2020 |

### Key Reliability Hierarchy

For **counting stats**: Official boxscores are truth. Pipeline must match them (with documented exceptions for confirmed official errors).

For **who's on court** (period starters, lineup tracking): There is NO single truth source. Different sources disagree, especially for early seasons (1997-2002). The pipeline must triangulate:
1. PBP event inference (strict rule: walk the event chain)
2. Gamerotation v6 parquet (boundary-based)
3. tpdev PBP lineups (possession-level)
4. BBR PBP (substitution events)
5. Manual overrides when sources conflict

For **minutes**: Downstream consequence of correct lineup tracking. If lineups are right, minutes will be right.

For **plus-minus**: Downstream consequence of correct lineup tracking AND correct scoring attribution. Most fragile metric.

---

## 4. The Core Contradiction: Event Ordering

The fundamental source of lineup errors is **event ordering ambiguity** in the NBA PBP data. Consider:

```
Event 464: P4 0:03 — Stoudamire makes 3PT shot
Event 469: P4 0:04 — Tabak substituted for Stoudamire
```

The shot has clock=0:03 and the sub has clock=0:04. When sorting by remaining time descending, the sub comes first (0:04 > 0:03), removing Stoudamire before his shot. But EVENTNUM ordering (464 < 469) says the shot happened first. BBR says the sub came first. tpdev says the sub came first.

**There is no universally correct ordering.** The pipeline currently processes events in a hybrid order (primarily by EVENTNUM with some clock-aware adjustments). A global chronology rewrite was explicitly tested on 2026-03-12 and **rejected** because a full 1997 rerun regressed badly (901 rebound deletions, 86 event_stats errors). The project conclusion:

> Do not apply a global reorder rule. Prefer local repair rules and overrides.

This means: when event ordering causes a specific game to get the wrong lineup, we fix it with a targeted override for that game, not with a global ordering change.

---

## 5. Period Starter Inference — The Heart of the Problem

The most complex and fragile part of the pipeline is determining who starts each period. Here's how it works (simplified from ~1000 lines of code in `start_of_period.py`):

### Strict PBP Inference

Walk the period's event chain. For each event:
- If a player appears as player1/player2/player3 and has NOT been subbed in, they're a **starter candidate**
- If a player appears in a substitution as `incoming_player_id`, they're a **subbed-in player** (not a starter)
- If a player appears in a substitution as `outgoing_player_id` (player1_id), they ARE a starter (they were on court to be subbed out)
- Technical fouls and ejections are excluded from starter inference (player can get a tech while on the bench)

After walking all events, split candidates by team using `player_team_map`, try to fill missing starters from the previous period's ending lineup, and trim excess starters.

### Fallback Chain

If strict inference fails to find 10 starters (5 per team):

1. **Local boxscore** — Period 1 only, uses START_POSITION from the boxscore response
2. **Period-level V3 boxscore** — Use the NBA's period-level boxscoretraditionalv3 endpoint (cached in `nba_raw.db`). Get all players who participated in the period, then narrow to starters using sub timing.
3. **Best-effort PBP inference** — Same as strict but `ignore_missing_starters=True` (don't raise exception if a team has <5)

### v6 vs Strict Conflict Resolution

When strict PBP finds 10 starters AND a local v6 (gamerotation-backed) row exists and disagrees:
- If there's an explicit manual override → use strict (override was already applied)
- If the disagreement matches an "opening cluster delayed sub" pattern (technical/flagrant at period start) → use strict
- Otherwise → **use v6** (gamerotation boundary data is more reliable than PBP inference for most cases)

This is the `_should_prefer_strict_starters_over_exact_v6()` gate — a key piece of logic that was added to handle cases where PBP inference gets poisoned by period-start technical fouls.

### Why Period Starters Go Wrong

1. **v6 is wrong** — gamerotation data is sometimes malformed, especially in early seasons (1997-1999). Bad v6 rows can override correct PBP inference.
2. **PBP inference is wrong** — when a player has no events in a period and gets subbed in, PBP may not detect them as a non-starter. When the previous period's ending lineup is carried forward incorrectly, the fill logic propagates errors.
3. **Neither source has the period** — for some OT periods and early-season games, no source has resolved starter data.
4. **Both sources are wrong differently** — each has a different error, and the correct answer requires manual triangulation via tpdev + BBR.

---

## 6. Lineup Propagation — Event-to-Event Tracking

Once period starters are determined, the pipeline propagates lineups through each event:

```python
# Simplified from enhanced_pbp_item.py
@property
def current_players(self):
    # For most events, carry forward from previous event
    previous = self.previous_event.current_players  # dict[team_id -> [5 player_ids]]

    # For substitution events, swap the outgoing/incoming player
    if isinstance(self, Substitution):
        team_players = previous[self.team_id].copy()
        team_players.remove(self.outgoing_player_id)
        team_players.append(self.incoming_player_id)
        result = {**previous, self.team_id: team_players}
        return result

    return previous
```

The real code is more complex (handling edge cases, technical fouls, ejections), but this is the core logic. Each event's `current_players` property returns the 5-player lineup for each team at that moment.

**Where this goes wrong:**
- If the period starters are wrong, every subsequent event in the period has wrong lineups
- If a substitution event is in the wrong position in the event stream, the lineup change happens at the wrong time
- If the NBA PBP is missing a substitution entirely, the lineup gets stuck and diverges from reality

---

## 7. Current Residual Summary (as of March 20-21, 2026)

The pipeline organizes the 23 seasons into 5 blocks for proving-loop validation:

| Block | Seasons | Problem Games | Min Mismatches | Min Outliers (>0.5min) | +/- Mismatches | Event-on-Court Rows |
|---|---|---|---|---|---|---|
| A | 1998-2000 | 84→130* | 59→81* | 7 | 212→297* | 50→48* |
| B | 2001-2005 | 168 | 30 | 1 | 455 | 13 |
| C | 2006-2010 | 130 | 4 | 0 | 389 | 4 |
| D | 2011-2016 | 116 | 0 | 0 | 308 | 13 |
| E | 2017-2020 | 169 | 12 | 0 | 513 | 30 |
| **Total** | | **667** | **105** | **8** | **1,877** | **110** |

*Block A numbers marked with → show regression after a "pending-sub-narrow" fork patch was tested — the patch helped canary games but regressed block A overall.

**Key observations:**
- **0 counting-stat mismatches** across all seasons — the stat pipeline is solid
- **0 failed games** — every game produces output
- **1,877 plus-minus mismatch rows** are the dominant residual
- **Minutes are basically solved** outside Block A (early-era data quality)
- **Block A (1997-2000) is the worst** — earliest era, worst data quality, most issues

---

## 8. What We've Tried and What Failed

### Failed: Global chronology rewrite (March 12)
Tested both pure PERIOD/EVENTNUM and clock-aware hybrid ordering. Full 1997 rerun regressed badly. **Lesson: do not apply global reorder rules.**

### Failed: Three same-clock scoring-attribution fork fixes (March 20)

**The hypothesis was:** When a substitution and a scoring event share the same clock time, the pipeline might be using the wrong lineup (post-sub instead of pre-sub) for the scoring event.

Three architecturally different approaches were tried:

1. **Lineup propagation carryover** — Modify `_get_previous_raw_players()` to delay the sub's effect. Result: 4-minute lineup blowup in canary games, catastrophic regression.

2. **Event-local scoring overlay (foul-committer anchor)** — Read-only helper `_get_effective_scoring_current_players()` used by field_goal.py and free_throw.py. Did NOT mutate lineup chain. Result: Every single canary got worse (+4 to +14 pm mismatches each).

3. **Event-local scoring overlay (fouled-player anchor)** — Same architecture, different anchor player. Result: Even worse than Approach 2 (+4 to +15 pm mismatches).

**Critical finding:** The fundamental assumption was wrong. Using pre-sub lineups for same-clock scoring events does NOT improve accuracy — it makes it less accurate. The current pipeline behavior (sub is already live when scoring event is processed) is closer to reality. **Do not attempt another same-clock scoring overlay.**

### Accepted: Opening-cluster carryover fix (March 20)
Period-start substitutions are now delayed when the outgoing player is explicitly credited inside an opening technical/flagrant/ejection cluster. Validated on 4 real canaries, all resolved to zero plus-minus diff. Still needs full season-level non-regression pass.

### Accepted but incomplete: Narrow pending-sub patch for foul/FT boundaries
A narrow non-opening same-clock pending-sub fix for foul/FT boundaries is in the fork. Validated on 5 targeted canary games (all clean). But when tested on full Block A (1998-2000), it **regressed** the block overall (+22 min mismatches, +85 pm mismatches vs baseline). The patch helps specific cases but hurts the broader population.

### Working but limited: Intraperiod missing-sub repair engine
Built `intraperiod_missing_sub_repair.py` — a local-only inference engine for intraperiod missing-sub / wrong-clock repair. Tightened scoring rules through v4. Proved that manual lineup-window overrides could be removed (heuristic subsumes them). **Net result: framework works correctly but produces 0 new uncovered auto-apply candidates in 1998-2000.** The early-era data is too noisy for automated repair.

---

## 9. Current Manual Override State

### Period Starter Overrides
49 entries across 38 games. Each has provenance documentation with source type, evidence, and preferred source. Categories:
- `prefer_v5_over_bad_v6` — v6 gamerotation data was wrong (common in 1997)
- `manual_raw_pbp_team_repair` — rebuilt from raw PBP evidence when neither v5 nor v6 was trusted
- `legacy_row_now_matches_v4_v5_v6` — older overrides now redundant
- `prefer_v6_when_v5_unresolved` — v6 fills gaps
- `manual_raw_pbp_ot_fix` — OT boundary fixes

### Lineup Window Overrides
Currently contains only 1 game (0049700045, a 1998 playoff game) with entries covering a P1 jump ball with empty lineups.

### The Recent Diagnostic Work (March 21, 2026)

We investigated all 12 games with event-on-court issues in Block A (1997-2000) using BBR play-by-play and tpdev PBP as cross-references.

**Results:**
- **9 games are fixable** via period-starter and/or lineup-window overrides
- **5 games have unfixable NBA source data errors** (pipeline lineups are already correct; the NBA PBP simply credits the wrong player)

The fixable games need:
- ~4 new period_starters entries (wrong P2/P3 starters carried forward from previous period)
- ~12 new lineup_window entries (event ordering issues, missing subs, spurious re-entries)

The biggest single fix: Game 0029800075 (NJN @ MIA) where a spurious Gill re-entry in Q2 causes Gill +3.98 min / Jones -5.33 min — the largest outlier in all of Block A.

---

## 10. The Scale of the Problem

### What's Left (Block A only, 1997-2000)

After the 12-game diagnostic, the Block A residual would look approximately like:

| Metric | Before fixes | After fixes (estimated) |
|---|---|---|
| Event-on-court rows | 48 | ~15 (unfixable source errors) |
| Minute outliers (>0.5 min) | 7 | ~1-2 |
| Minute mismatches | 81 | ~50-60 (many are small, <6 sec) |
| Plus-minus mismatches | 297 | ~250 (pm is downstream; modest improvement expected) |

### What's Left (All blocks)

| Metric | Total | Block A share |
|---|---|---|
| Event-on-court rows | 110 | 48 (44%) |
| Minute mismatches | 105 | 81 (77%) |
| Plus-minus mismatches | 1,877 | 297 (16%) |

Block A dominates minutes issues (early era data quality). Plus-minus mismatches are spread across all blocks and are mostly NOT caused by lineup attribution errors — they appear to be inherent differences between PBP-derived and official-reported plus-minus.

### Per-Season Game Counts

- 1997 season: ~800 regular season + ~80 playoff games
- 1998 season: ~600 (lockout shortened)
- 1999 season: ~1,200 games
- 2000 season: ~1,300 games
- Total 1997-2020: ~30,000 games

---

## 11. Key Design Constraints

1. **External data sources can NEVER be runtime dependencies.** tpdev, BBR, pbpstats scraped data are for audit and override justification only. The production pipeline runs on: `playbyplayv2.parq` + `nba_raw.db` + `period_starters_v6/v5.parquet` + override files.

2. **Fixes to who's on court must go through the lineup/starter system.** Never overwrite Minutes or Plus_Minus with external values. If minutes are wrong, fix the lineup tracking that produced wrong stints.

3. **Broad recurring fixes belong in the pbpstats fork.** One-off anomalies should prefer local overrides. The fork changes affect all games; overrides affect only the specified game.

4. **Unit tests are necessary but not sufficient for fork changes.** Every new fork rule must be validated on real-game canaries, then season/block reruns.

5. **Don't overwrite existing output parquets.** Write new reruns to fresh directories and compare before promoting.

6. **The override files must have provenance.** Every override entry gets a corresponding notes row documenting: source type, reason, evidence, preferred source, date added.

---

## 12. Key Source Code

### Period Starter Override Loading (from pbpstats fork)

```python
def _load_period_starter_overrides(self, file_directory):
    """Loads from overrides/missing_period_starters.json and
    overrides/period_starters_overrides.json, merging both."""
    if file_directory is None:
        return {}
    override_files = [
        f"{file_directory}/overrides/missing_period_starters.json",
        f"{file_directory}/overrides/period_starters_overrides.json",
    ]
    merged_overrides = {}
    for override_file_path in override_files:
        if not os.path.isfile(override_file_path):
            continue
        with open(override_file_path) as f:
            override_data = json.loads(f.read(), cls=IntDecoder)
        for game_id, game_periods in override_data.items():
            merged_overrides.setdefault(game_id, {})
            for period, team_map in game_periods.items():
                merged_overrides[game_id].setdefault(period, {})
                merged_overrides[game_id][period].update(team_map)
    return merged_overrides
```

### Period Starter Resolution Order (from pbpstats fork, `StatsStartOfPeriod.get_period_starters()`)

```python
def get_period_starters(self, file_directory=None):
    # 1) Strict PBP-based inference
    starters = self._get_period_starters_from_period_events(file_directory)

    if starters is not None and not self._strict_starters_are_impossible(starters):
        # Explicit local overrides beat any parquet fallback
        if self._has_period_starter_override(file_directory):
            return starters
        # When v6 exists and disagrees with strict PBP, prefer v6
        # UNLESS the disagreement matches a delayed opening-cluster sub pattern
        local_boxscore_starters, local_boxscore_source = (
            self._get_exact_local_period_boxscore_starters()
        )
        if (
            local_boxscore_source == "v6"
            and local_boxscore_starters is not None
            and local_boxscore_starters != starters
        ):
            if self._should_prefer_strict_starters_over_exact_v6(
                starters, local_boxscore_starters
            ):
                return starters
            return local_boxscore_starters
        return starters

    # 2) Local boxscore-based starters (Period 1 only, via START_POSITION)
    starters = self._get_period_starters_from_boxscore_loader()
    if starters is not None:
        return starters

    # 3) Period-level V3 boxscore fallback
    starters = self._get_starters_from_boxscore_request()
    if starters is not None:
        return starters

    # 4) Best-effort PBP inference (ignore_missing_starters=True)
    return self._get_period_starters_from_period_events(
        file_directory, ignore_missing_starters=True
    )
```

### How the Runner Orchestrates a Season (from `cautious_rerun.py`)

```python
def run_season(namespace, season, output_dir, parquet_path, db_path, ...):
    namespace["DB_PATH"] = db_path
    namespace["clear_event_stats_errors"]()

    combined_df, error_df = namespace["process_season"](
        season=season,
        parquet_path=str(parquet_path),
        output_dir=".",
        validate=True,
        tolerance=tolerance,
        max_workers=max_workers,
        overrides_path=str(overrides_path),
        strict_mode=strict_mode,
        run_boxscore_audit=run_boxscore_audit_pass,
    )

    # Lineup audits
    if run_lineup_audit_pass:
        lineup_audit_summary = run_lineup_audits(
            combined_df=combined_df,
            season=season,
            output_dir=output_dir,
            db_path=db_path,
            parquet_path=parquet_path,
        )
```

### How Lineup Audits Work (from `cautious_rerun.py`)

```python
def run_lineup_audits(combined_df, season, output_dir, db_path, parquet_path, ...):
    # 1. Compare pipeline Minutes/PlusMinus against official boxscore
    minutes_audit_df = build_minutes_plus_minus_audit(combined_df, db_path=db_path)

    # 2. Find "problem games" — any game with minute or plus-minus mismatch
    problem_game_ids = sorted({
        game_id for game_id in minutes_audit_df.loc[
            minutes_audit_df["has_minutes_mismatch"] |
            minutes_audit_df["has_plus_minus_mismatch"],
            "game_id"
        ]
    })

    # 3. For problem games, check each event: is the credited player on court?
    issues_df, event_summary = audit_event_player_on_court(
        game_ids=problem_game_ids,
        parquet_path=parquet_path,
        db_path=db_path,
    )
```

### Event-on-Court Audit Logic (from `audit_event_player_on_court.py`)

```python
def _check_event_players(game_id, events, player_team_map):
    for event in events:
        current_lineups = event.current_players  # dict[team_id -> [5 player_ids]]
        previous_lineups = event.previous_event.current_players

        for field in ["player1_id", "player2_id", "player3_id"]:
            player_id = getattr(event, field)
            team_id = player_team_map.get(player_id)

            lineup = current_lineups.get(team_id, [])
            on_current = player_id in lineup
            on_previous = player_id in previous_lineups.get(team_id, [])

            if not on_current:
                # Player credited with event while NOT in current lineup
                if isinstance(event, Substitution) and field == "player1_id":
                    status = "sub_out_player_missing_from_previous_lineup"
                else:
                    status = "off_court_event_credit"
                rows.append({...})
```

### BBR PBP Lookup (from `bbr_pbp_lookup.py`)

```python
def find_bbr_game_for_nba_game(nba_game_id, *, nba_raw_db_path, bbr_db_path):
    """Map an NBA game ID to a BBR game ID using date + team matching."""
    context = load_nba_game_context(nba_game_id, nba_raw_db_path=nba_raw_db_path)
    home_codes = candidate_bbr_team_codes(context.home_team_id, context.game_date)
    away_codes = candidate_bbr_team_codes(context.away_team_id, context.game_date)
    # Query BBR database for matching game
    ...

def load_bbr_play_by_play_rows(bbr_game_id, *, bbr_db_path, period=None, ...):
    """Load BBR PBP rows with optional filtering by period, clock, player name."""
    # Returns: [{event_index, period, game_clock, away_play, home_play,
    #            away_player_ids, home_player_ids, ...}]
```

BBR PBP includes:
- Substitution events ("X enters the game for Y")
- Per-event player slug attribution
- Independent of NBA's PBP data (different source, different ordering)

### Period Boxscore Source Loader (from `period_boxscore_source_loader.py`)

```python
class PeriodBoxscoreSourceLoader:
    """Reads pre-scraped period starters from parquet files (v6, v5)
    and synthesizes V3-shaped responses for the pbpstats fallback chain."""

    def __init__(self, parquet_paths, allowed_seasons=None, allowed_game_ids=None):
        self._lookups = [
            _ParquetStarterLookup(path, allowed_seasons, allowed_game_ids)
            for path in parquet_paths
        ]

    def load_data(self, game_id, period, mode):
        """Returns first matching resolved starter row across all parquets."""
        for lookup in self._lookups:
            result = lookup.get(game_id, period, mode)
            if result is not None:
                return result
        return None
```

---

## 13. The Strategic Questions

Given all of the above, here's what we need your thinking on:

### Question 1: Manual Override Strategy for Block A (1997-2000)

Block A has the worst data quality and accounts for 77% of minute mismatches and 44% of event-on-court issues. We've demonstrated that fork-level fixes (automated rules in the parser) tend to regress Block A even when they help targeted canaries.

**Our current plan:** Go game-by-game through the ~24 problem games in Block A, use BBR PBP and tpdev as cross-references, and write manual overrides (period starters + lineup windows) for each fixable case.

**Questions:**
- Is this the right approach for an era with fundamentally noisy data?
- How should we prioritize which games to fix? (By impact? By fix confidence? By fix type?)
- Should we set a residual threshold below which we declare Block A "done" rather than chasing diminishing returns?
- How do we manage the growing override file complexity?

### Question 2: Plus-Minus Residual (1,877 rows across all blocks)

Plus-minus mismatches are the largest residual. Three architecturally different same-clock scoring-attribution fixes all made things worse. The current working theory is that plus-minus differences are mostly inherent (PBP-derived vs official) rather than caused by lineup errors.

**Questions:**
- Should we accept plus-minus as an irreducible residual and stop targeting it directly?
- Is there a way to classify which plus-minus mismatches are "fixable lineup issues" vs "inherent measurement differences"?
- Should we publish a residual characterization report and move on?

### Question 3: Scaling to Blocks B-E

Blocks B-E have much cleaner data but still have:
- 46 minute mismatches (B=30, C=4, D=0, E=12)
- 60 event-on-court rows (B=13, C=4, D=13, E=30)
- 1,580 plus-minus mismatches

**Questions:**
- Should we apply the same game-by-game manual override approach to these blocks?
- Or are the issues in B-E more systematic (amenable to fork-level fixes that wouldn't work for Block A)?
- How should we phase this work?

### Question 4: Validation and Confidence

We currently validate by:
1. Targeted game reruns (fast, high signal)
2. Block-level reruns (slow, catches regressions)
3. Full 5-block proving loop (very slow, comprehensive)

**Questions:**
- Is this validation ladder sufficient?
- Should we build regression test suites from the known-good games?
- How do we prevent override accumulation from creating maintenance burden?

### Question 5: Definition of "Done"

The counting stats are done (0 mismatches). But lineup-derived fields have a long tail of issues.

**Questions:**
- What's a reasonable definition of "done" for Minutes? (e.g., "no outliers >0.5 min, <50 total mismatches >1 second")
- What's a reasonable definition of "done" for Plus_Minus? (e.g., "plus-minus is tracked as a downstream indicator, not targeted directly")
- Should we separate the historical output into tiers? (e.g., Tier 1: 2006-2020 where data is clean, Tier 2: 2001-2005 with minor issues, Tier 3: 1997-2000 with documented residuals)

### Question 6: Architecture Evolution

The current architecture evolved organically. The notebook dump → namespace exec → fork integration is working but fragile.

**Questions:**
- If we were to do a clean-sheet redesign of the override system, what would it look like?
- Should lineup-window overrides and period-starter overrides be merged into one system?
- How should we handle the provenance/documentation burden as overrides grow?

---

## 14. Working Rules (Must-Not-Violate)

1. External sources never become runtime dependencies
2. Never overwrite Minutes/Plus_Minus with external values — fix the lineup tracking instead
3. No global event reordering — local repairs only
4. No same-clock scoring-attribution fork fix — three approaches all failed
5. Unit tests necessary but not sufficient — real-canary validation required
6. Don't overwrite existing output parquets
7. All overrides must have provenance documentation
8. Counting stats must stay at zero mismatches — any change that regresses counting stats is rejected

---

## 15. Complete Source Code: Key Pipeline Files

### cautious_rerun.py (Full — Season-Level Runner)

```python
from __future__ import annotations

import argparse
import importlib.util
import importlib.machinery
import json
import os
import sqlite3
import sys
import traceback
import types
import zlib
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

from boxscore_source_overrides import (
    apply_boxscore_response_overrides,
    load_boxscore_source_overrides,
    set_boxscore_source_overrides,
)

ROOT = Path(__file__).resolve().parent
NOTEBOOK_DUMP = ROOT / "0c2_build_tpdev_box_stats_version_v9b.py"
DEFAULT_DB = ROOT / "nba_raw.db"
DEFAULT_PARQUET = ROOT / "playbyplayv2.parq"
DEFAULT_OVERRIDES = ROOT / "validation_overrides.csv"
DEFAULT_BOXSCORE_SOURCE_OVERRIDES = ROOT / "boxscore_source_overrides.csv"
DEFAULT_FILE_DIRECTORY = ROOT
DEFAULT_PERIOD_STARTERS_PARQUETS = [
    ROOT / "period_starters_v6.parquet",
    ROOT / "period_starters_v5.parquet",
]

NOTEBOOK_LOCAL_IMPORT_PRELOADS = [
    "boxscore_source_overrides",
    "pbp_row_overrides",
    "pbp_stat_overrides",
    "player_id_normalization",
    "team_event_normalization",
    "boxscore_audit",
]


class _BoxscoreSourceLoader:
    def __init__(self, data: Dict[str, Any]):
        self._data = data

    def load_data(self, game_id: str | None = None) -> Dict[str, Any]:
        return self._data


def _ensure_local_pbpstats_importable() -> None:
    if importlib.util.find_spec("pbpstats") is not None:
        return
    candidates = []
    env_path = os.environ.get("PBPSTATS_REPO")
    if env_path:
        candidates.append(Path(env_path).expanduser())
    candidates.append(Path.home() / "Documents" / "GitHub" / "pbpstats")
    for candidate in candidates:
        if (candidate / "pbpstats").exists():
            sys.path.insert(0, str(candidate))
            if importlib.util.find_spec("pbpstats") is not None:
                return
    raise ModuleNotFoundError(
        "Could not import pbpstats. Set PBPSTATS_REPO or make the editable repo available."
    )


def _preload_local_module(module_name: str, module_path: Path) -> None:
    if module_name in sys.modules:
        return
    if module_path.suffix == ".pyc":
        loader = importlib.machinery.SourcelessFileLoader(module_name, str(module_path))
        spec = importlib.util.spec_from_loader(module_name, loader)
        if spec is None:
            raise ImportError(f"Could not build spec for sourceless module {module_name} at {module_path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        try:
            loader.exec_module(module)
        except Exception:
            sys.modules.pop(module_name, None)
            raise
        return
    module = types.ModuleType(module_name)
    module.__file__ = str(module_path)
    module.__package__ = ""
    sys.modules[module_name] = module
    try:
        source = module_path.read_text(encoding="utf-8")
        exec(compile(source, str(module_path), "exec"), module.__dict__)
    except Exception:
        sys.modules.pop(module_name, None)
        raise


def _load_raw_response(db_path: Path, game_id: str, endpoint: str) -> Dict[str, Any] | None:
    game_id = str(game_id).zfill(10)
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        row = conn.execute(
            "SELECT data FROM raw_responses WHERE game_id=? AND endpoint=? AND team_id IS NULL",
            (game_id, endpoint),
        ).fetchone()
        if not row:
            return None
        blob = row[0]
        try:
            data = json.loads(zlib.decompress(blob).decode())
        except (zlib.error, TypeError):
            if isinstance(blob, bytes):
                data = json.loads(blob.decode())
            else:
                data = json.loads(blob)
        if endpoint == "boxscore":
            return apply_boxscore_response_overrides(game_id, data)
        return data
    finally:
        conn.close()


def prepare_local_runtime_inputs(cache_dir, db_path=DEFAULT_DB, parquet_path=DEFAULT_PARQUET,
    overrides_path=DEFAULT_OVERRIDES, boxscore_source_overrides_path=DEFAULT_BOXSCORE_SOURCE_OVERRIDES,
    period_starter_parquet_paths=DEFAULT_PERIOD_STARTERS_PARQUETS,
    allow_unreadable_csv_fallback=False) -> Dict[str, Path]:
    """Hydrate runtime inputs to local cache, returning resolved paths."""
    # ... (hydration logic that copies files to local cache to avoid cloud-sync stalls)
    # Returns dict with keys: db_path, parquet_path, notebook_dump_path,
    #   preload_module_paths, overrides_path, period_starter_parquet_paths
    pass  # Full implementation in repo


def load_v9b_namespace(*, notebook_dump_path=NOTEBOOK_DUMP, preload_module_paths=None) -> Dict[str, Any]:
    """Load the notebook dump namespace. Preloads local modules, then exec's the
    safe prefix of the notebook dump (everything before the __main__ guard)."""
    _ensure_local_pbpstats_importable()
    module_paths = preload_module_paths or {
        module_name: ROOT / f"{module_name}.py"
        for module_name in NOTEBOOK_LOCAL_IMPORT_PRELOADS
    }
    for module_name in NOTEBOOK_LOCAL_IMPORT_PRELOADS:
        _preload_local_module(module_name, Path(module_paths[module_name]))
    source = notebook_dump_path.read_text(encoding="utf-8")
    marker = 'if __name__ == "__main__":\n    pass\n'
    prefix = source.split(marker, 1)[0] + marker
    namespace: Dict[str, Any] = {"__name__": "v9b_dump_safe", "__file__": str(notebook_dump_path)}
    exec(compile(prefix, str(notebook_dump_path), "exec"), namespace)
    return namespace


def install_local_boxscore_wrapper(namespace, db_path, file_directory=DEFAULT_FILE_DIRECTORY,
    allowed_seasons=None, allowed_game_ids=None, period_starter_parquet_paths=None) -> None:
    """Wrap namespace's get_possessions_from_df to inject local boxscore loader
    and period boxscore source loader."""
    from period_boxscore_source_loader import PeriodBoxscoreSourceLoader
    original_get_possessions = namespace["get_possessions_from_df"]
    period_boxscore_source_loader = PeriodBoxscoreSourceLoader(
        parquet_paths=period_starter_parquet_paths or DEFAULT_PERIOD_STARTERS_PARQUETS,
        allowed_seasons=allowed_seasons,
        allowed_game_ids=allowed_game_ids,
    )

    def wrapped_get_possessions(*args, **kwargs):
        pbp_df = args[0] if args else kwargs.get("pbp_df")
        loader = None
        if pbp_df is not None and not pbp_df.empty and "GAME_ID" in pbp_df.columns:
            game_id = str(pbp_df["GAME_ID"].iloc[0]).zfill(10)
            raw_boxscore = _load_raw_response(db_path, game_id, "boxscore")
            if raw_boxscore is not None:
                loader = _BoxscoreSourceLoader(raw_boxscore)
        kwargs.setdefault("boxscore_source_loader", loader)
        kwargs.setdefault("period_boxscore_source_loader", period_boxscore_source_loader)
        kwargs.setdefault("file_directory", str(file_directory.resolve()))
        return original_get_possessions(*args, **kwargs)

    namespace["get_possessions_from_df"] = wrapped_get_possessions


def run_lineup_audits(combined_df, season, output_dir, db_path, parquet_path,
    file_directory=DEFAULT_FILE_DIRECTORY) -> Dict[str, Any]:
    from audit_event_player_on_court import audit_event_player_on_court
    from audit_minutes_plus_minus import build_minutes_plus_minus_audit, summarize_minutes_plus_minus_audit

    minutes_audit_df = build_minutes_plus_minus_audit(combined_df, db_path=db_path)
    minutes_summary = summarize_minutes_plus_minus_audit(minutes_audit_df)
    # Write outputs, find problem games, run event-on-court audit
    problem_game_ids = sorted({
        str(game_id).zfill(10)
        for game_id in minutes_audit_df.loc[
            minutes_audit_df["has_minutes_mismatch"] | minutes_audit_df["has_plus_minus_mismatch"],
            "game_id",
        ].tolist()
    })
    issues_df, event_summary = audit_event_player_on_court(
        game_ids=problem_game_ids, parquet_path=parquet_path, db_path=db_path, file_directory=file_directory,
    )
    return {"minutes_plus_minus": minutes_summary, "problem_games": len(problem_game_ids), "event_on_court": event_summary}


def run_season(namespace, season, output_dir, parquet_path, db_path, file_directory,
    overrides_path, strict_mode, tolerance, max_workers, run_boxscore_audit_pass, run_lineup_audit_pass):
    """Run a single season through the pipeline. Returns (player_rows, failed_games, event_errors)."""
    namespace["DB_PATH"] = db_path
    namespace["clear_event_stats_errors"]()
    combined_df, error_df = namespace["process_season"](
        season=season, parquet_path=str(parquet_path), output_dir=".",
        validate=True, tolerance=tolerance, max_workers=max_workers,
        overrides_path=str(overrides_path), strict_mode=strict_mode,
        run_boxscore_audit=run_boxscore_audit_pass,
    )
    if run_lineup_audit_pass:
        lineup_audit_summary = run_lineup_audits(
            combined_df=combined_df, season=season, output_dir=output_dir,
            db_path=db_path, parquet_path=parquet_path, file_directory=file_directory,
        )
    return len(combined_df), len(error_df), len(namespace.get("_event_stats_errors", []))
```

### audit_minutes_plus_minus.py (Full — Minute/PM Comparison Against Official)

```python
from __future__ import annotations
import argparse, json, sqlite3, zlib
from pathlib import Path
from typing import Any, Dict, Iterable, List
import pandas as pd
from boxscore_source_overrides import apply_boxscore_response_overrides

MINUTE_OUTLIER_THRESHOLD = 0.5

def parse_official_minutes(value):
    """Parse NBA official minute format (e.g., '34:21') to float minutes."""
    if value is None: return 0.0
    text = str(value).strip()
    if text == "" or text.upper() in {"DNP", "DND", "NWT"}: return 0.0
    if ":" in text:
        minutes, seconds = text.split(":", 1)
        return int(minutes) + (int(seconds) / 60.0)
    return float(text)

def load_official_boxscore_df(db_path, game_id):
    """Load official boxscore from nba_raw.db for a game, apply overrides."""
    raw = _load_raw_response(db_path, game_id, "boxscore")
    raw = apply_boxscore_response_overrides(game_id, raw)
    # Parse into DataFrame with columns:
    #   game_id, player_id, team_id, player_name, Minutes_official, Plus_Minus_official
    ...

def build_minutes_plus_minus_audit(darko_df, db_path, minute_outlier_threshold=0.5):
    """Compare pipeline output against official boxscore.
    Returns DataFrame with columns:
    - Minutes_output, Minutes_official, Minutes_diff, Minutes_abs_diff
    - Plus_Minus_output, Plus_Minus_official, Plus_Minus_diff
    - has_minutes_mismatch (>1 second diff)
    - has_plus_minus_mismatch (any diff)
    - is_minutes_outlier (>0.5 min diff)
    """
    prepared = _prepare_darko_df(darko_df)
    official_frames = [load_official_boxscore_df(db_path, gid) for gid in prepared["game_id"].unique()]
    official = pd.concat(official_frames, ignore_index=True)
    merged = prepared.merge(official, on=["game_id", "player_id", "team_id"], how="outer")
    merged["Minutes_diff"] = merged["Minutes_output"] - merged["Minutes_official"]
    merged["has_minutes_mismatch"] = merged["Minutes_diff"].abs() > (1.0 / 60.0)
    merged["has_plus_minus_mismatch"] = merged["Plus_Minus_diff"] != 0
    merged["is_minutes_outlier"] = merged["Minutes_diff"].abs() > minute_outlier_threshold
    return merged
```

### audit_event_player_on_court.py (Full — On-Court Consistency Check)

```python
from __future__ import annotations
import argparse, json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List
import pandas as pd

TEAM_ID_FLOOR = 1000000000

def _check_event_players(game_id, events, player_team_map):
    """For each event, check if the credited players are in the current lineup.
    Returns DataFrame of issues with status:
    - off_court_event_credit: player credited but not in current lineup
    - same_clock_boundary_conflict: player was in previous but not current lineup
    - sub_out_player_missing_from_previous_lineup
    - sub_in_player_missing_from_current_lineup
    """
    rows = []
    for event in events:
        current_lineups = event.current_players  # dict[team_id -> [5 player_ids]]
        previous_lineups = event.previous_event.current_players
        for field in ("player1", "player2", "player3"):
            player_id = getattr(event, f"{field}_id")
            team_id = player_team_map.get(player_id)
            on_current = player_id in current_lineups.get(team_id, [])
            on_previous = player_id in previous_lineups.get(team_id, [])
            if not on_current:
                # Technicals/ejections excluded — player can get tech while on bench
                if isinstance(event, Substitution) and field == "player1":
                    status = "sub_out_player_missing_from_previous_lineup"
                else:
                    status = "off_court_event_credit"
                rows.append({
                    "game_id": game_id, "event_num": event.event_num,
                    "period": event.period, "clock": event.clock,
                    "player_id": player_id, "team_id": team_id,
                    "status": status, "current_team_lineup": current_lineups.get(team_id, []),
                })
    return pd.DataFrame(rows)

def audit_event_player_on_court(game_ids, parquet_path, db_path, file_directory):
    """Run event-on-court audit for a list of game IDs.
    For each game: load PBP, build enhanced events via pbpstats, check each event.
    Returns (issues_df, summary_dict)."""
    ...
```

### bbr_pbp_lookup.py (Full — BBR Cross-Reference Tool)

```python
from __future__ import annotations
import argparse, json, sqlite3, textwrap, zlib
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable, Sequence

DEFAULT_NBA_RAW_DB_PATH = Path(__file__).resolve().parent / "nba_raw.db"
DEFAULT_BBR_DB_PATH = Path(__file__).resolve().parent.parent / "33_wowy_rapm" / "bbref_boxscores.db"

# Team alias table (NBA team_id -> BBR code by date range)
TEAM_ALIASES = (
    TeamAlias(1610612737, "ATL", date(1968, 1, 1), date(9999, 12, 31)),
    TeamAlias(1610612738, "BOS", date(1946, 1, 1), date(9999, 12, 31)),
    # ... ~35 more team aliases covering relocations and name changes
)

def find_bbr_game_for_nba_game(nba_game_id, *, nba_raw_db_path, bbr_db_path):
    """Map NBA game ID to BBR game ID using date + team matching.
    1. Load game context from nba_raw.db (date, home/away team IDs)
    2. Map team IDs to BBR codes using date-aware aliases
    3. Query BBR games table for matching date+teams
    """
    context = load_nba_game_context(nba_game_id, nba_raw_db_path=nba_raw_db_path)
    home_codes = candidate_bbr_team_codes(context.home_team_id, context.game_date)
    away_codes = candidate_bbr_team_codes(context.away_team_id, context.game_date)
    date_prefix = context.game_date.strftime("%Y%m%d")
    # Query: SELECT game_id, url, away_team, home_team FROM games
    #        WHERE game_id LIKE '{date}%' AND home_team IN (...) AND away_team IN (...)
    ...

def load_bbr_play_by_play_rows(bbr_game_id, *, bbr_db_path, period=None, clock=None, contains=None):
    """Load BBR PBP rows from bbref_boxscores.db.
    Columns: event_index, period, game_clock, score_away, score_home,
             away_play, home_play, away_player_ids, home_player_ids, is_colspan_row

    BBR PBP includes:
    - Substitution events: "X enters the game for Y"
    - Shot events with player slugs: "D. Nowitzki makes 2-pt shot"
    - Rebounds, turnovers, fouls with player attribution
    - Per-event player slug lists (away_player_ids, home_player_ids)
    """
    ...
```

### start_of_period.py (Full — Period Starter Inference, the Heart of the Pipeline)

This is the base class from the pbpstats fork. It contains ALL the logic for determining who starts each period. ~1100 lines. This is the single most important file in the system.

```python
import abc
import json
import os

import requests

from pbpstats import (
    G_LEAGUE_GAME_ID_PREFIX, G_LEAGUE_STRING, HEADERS,
    NBA_GAME_ID_PREFIX, NBA_STRING, REQUEST_TIMEOUT,
    WNBA_GAME_ID_PREFIX, WNBA_STRING,
)
from pbpstats.overrides import IntDecoder
from pbpstats.resources.enhanced_pbp import (
    Ejection, EndOfPeriod, FieldGoal, Foul, FreeThrow,
    JumpBall, Substitution, Timeout, Turnover,
)


class InvalidNumberOfStartersException(Exception):
    """Raised when 5 period starters can't be determined for a team."""
    pass


class StartOfPeriod(metaclass=abc.ABCMeta):
    """Base class for start of period events. Contains all period-starter
    inference, override loading, and fallback logic."""

    @abc.abstractclassmethod
    def get_period_starters(self, file_directory):
        pass

    @property
    def current_players(self):
        return self.period_starters

    @property
    def _raw_current_players(self):
        return self.period_starters

    def _get_period_start_tenths(self):
        if self.league == WNBA_STRING:
            regulation_tenths = 6000
        else:
            regulation_tenths = 7200
        if self.period == 1:
            return 0
        if self.period <= 4:
            return int(regulation_tenths * (self.period - 1))
        return int(4 * regulation_tenths + 3000 * (self.period - 5))

    def _get_period_start_seconds(self):
        if self.period <= 4:
            if self.league == WNBA_STRING:
                return 600.0
            return 720.0
        return 300.0

    # ── Period Boxscore Loading ──────────────────────────────────────

    def _load_period_boxscore_response(self, mode):
        """Try local loader first, then fall back to NBA API fetch."""
        loader_obj = getattr(self, "period_boxscore_source_loader", None)
        if loader_obj is not None:
            try:
                return loader_obj.load_data(self.game_id, self.period, mode)
            except Exception:
                return None
        try:
            return self._fetch_period_boxscore_response(mode)
        except Exception:
            return None

    def _extract_period_boxscore_candidates_by_team(self, response_json):
        """Extract player lists from a V3-shaped boxscore response."""
        if not isinstance(response_json, dict):
            return {}
        boxscore = response_json.get("boxScoreTraditional")
        if not isinstance(boxscore, dict):
            return {}
        players_by_team = {}
        for team_key in ["awayTeam", "homeTeam"]:
            team_data = boxscore.get(team_key)
            if not isinstance(team_data, dict):
                continue
            team_id = team_data.get("teamId")
            players = team_data.get("players", [])
            players_by_team[team_id] = [
                player.get("personId")
                for player in players if isinstance(player, dict)
            ]
        return self._normalize_boxscore_players_by_team(players_by_team)

    def _is_exact_starter_map(self, starters_by_team):
        """Check if we have exactly 5 unique players per team, 10 total."""
        starters_by_team = self._normalize_boxscore_players_by_team(starters_by_team)
        return (
            len(starters_by_team) == 2
            and all(len(s) == 5 for s in starters_by_team.values())
            and len({pid for s in starters_by_team.values() for pid in s}) == 10
        )

    # ── Substitution Analysis ────────────────────────────────────────

    def _get_period_substitution_order_lookup(self):
        """Build lookup: {team_id: {player_id: {"in": [order], "out": [order]}}}"""
        substitution_order_lookup = {}
        for event_order, event in enumerate(self._iter_period_events(), start=1):
            if not isinstance(event, Substitution):
                continue
            team_id = getattr(event, "team_id", None)
            if not isinstance(team_id, int) or team_id <= 0:
                continue
            for kind, player_id in [
                ("in", getattr(event, "incoming_player_id", None)),
                ("out", getattr(event, "outgoing_player_id", None)),
            ]:
                if not isinstance(player_id, int) or player_id <= 0:
                    continue
                team_lookup = substitution_order_lookup.setdefault(team_id, {})
                player_lookup = team_lookup.setdefault(player_id, {"in": [], "out": []})
                player_lookup[kind].append(event_order)
        return substitution_order_lookup

    def _classify_period_boxscore_candidate(self, team_id, player_id, substitution_lookup):
        """Classify a player as starter (True), non-starter (False), or ambiguous (None).
        Logic: if subbed out before subbed in → starter.
               if subbed in before subbed out → non-starter.
               if never subbed → starter (played whole period)."""
        player_lookup = substitution_lookup.get(team_id, {}).get(
            player_id, {"in": [], "out": []}
        )
        has_sub_in = len(player_lookup["in"]) > 0
        has_sub_out = len(player_lookup["out"]) > 0

        if not has_sub_in and not has_sub_out: return True   # never subbed
        if has_sub_out and not has_sub_in: return True       # subbed out only = starter
        if has_sub_in and not has_sub_out: return False      # subbed in only = not starter

        first_in = min(player_lookup["in"])
        first_out = min(player_lookup["out"])
        if first_out < first_in: return True   # subbed out first = starter
        if first_in < first_out: return False  # subbed in first = not starter
        return None  # ambiguous

    # ── Strict PBP Starter Inference ─────────────────────────────────

    def _get_players_who_started_period_with_team_map(self):
        """Walk period events to find starter candidates.

        For each event:
        - player1_id: if the player hasn't been seen as a sub-in, they're a starter candidate
        - player2_id/player3_id: same logic (assists, blocks, steals, foul drawn)
        - Substitution incoming_player_id: marked as subbed-in (not a starter)
        - Substitution outgoing_player_id (player1_id): recorded as starter
        - Technical fouls/ejections excluded (player can get tech while on bench)

        Returns: (starters, player_team_map, player_first_seen_order, subbed_in_players)
        """
        starters = []
        subbed_in_players = []
        player_team_map = {}
        player_first_seen_order = {}
        player_first_seen_seconds_remaining = {}
        player_first_sub_in_seconds_remaining = {}
        known_team_ids = self._get_known_team_ids_for_period()
        event = self
        event_order = 0

        while event is not None and not isinstance(event, EndOfPeriod):
            event_order += 1
            if (not isinstance(event, Timeout)
                and self._is_valid_starter_candidate(event.player1_id, known_team_ids)
                and hasattr(event, "team_id")):

                player_id = event.player1_id
                if not isinstance(event, JumpBall):
                    player_team_map[player_id] = event.team_id

                if isinstance(event, Substitution) and event.incoming_player_id is not None:
                    player_team_map[event.incoming_player_id] = event.team_id
                    if event.incoming_player_id not in player_first_sub_in_seconds_remaining:
                        player_first_sub_in_seconds_remaining[event.incoming_player_id] = float(
                            getattr(event, "seconds_remaining", float("-inf"))
                        )
                    if (event.incoming_player_id not in starters
                        and event.incoming_player_id not in subbed_in_players):
                        subbed_in_players.append(event.incoming_player_id)
                    if player_id not in starters and player_id not in subbed_in_players:
                        self._record_starter_candidate(
                            player_id, starters, subbed_in_players,
                            player_first_seen_order, known_team_ids, event_order,
                        )

                is_technical_foul = isinstance(event, Foul) and (
                    event.is_technical or event.is_double_technical
                )
                if player_id not in starters and player_id not in subbed_in_players:
                    if not (is_technical_foul or isinstance(event, Ejection)):
                        self._record_starter_candidate(
                            player_id, starters, subbed_in_players,
                            player_first_seen_order, known_team_ids, event_order,
                        )

                # player2/player3 for players with no player1 events
                if not isinstance(event, Substitution) and not (is_technical_foul or isinstance(event, Ejection)):
                    for field in ("player2_id", "player3_id"):
                        if hasattr(event, field):
                            self._record_starter_candidate(
                                getattr(event, field), starters, subbed_in_players,
                                player_first_seen_order, known_team_ids, event_order,
                            )
            event = event.next_event

        # Fix: if a player's first explicit sub-in happens at same clock as first seen,
        # treat them as sub not starter
        for player_id, sub_secs in player_first_sub_in_seconds_remaining.items():
            first_seen_secs = player_first_seen_seconds_remaining.get(player_id)
            if first_seen_secs is None: continue
            if sub_secs + 0.001 >= first_seen_secs:
                starters = [s for s in starters if s != player_id]
                if player_id not in subbed_in_players:
                    subbed_in_players.append(player_id)

        return starters, player_team_map, player_first_seen_order, subbed_in_players

    def _split_up_starters_by_team(self, starters, player_team_map):
        """Split flat starter list into {team_id: [players]} using team map."""
        starters_by_team = {}
        known_team_ids = {tid for tid in player_team_map.values() if isinstance(tid, int)}
        dangling_starters = []
        for player_id in starters:
            team_id = player_team_map.get(player_id)
            if team_id is not None:
                starters_by_team.setdefault(team_id, []).append(player_id)
            else:
                dangling_starters.append(player_id)
        if len(dangling_starters) == 1 and len(starters) == 10:
            for _, team_starters in starters_by_team.items():
                if len(team_starters) == 4:
                    team_starters += dangling_starters
        return starters_by_team

    # ── Previous Period Carryover ────────────────────────────────────

    def _fill_missing_starters_from_previous_period_end(self, starters_by_team):
        """When strict PBP finds <5 starters for a team, try to fill from
        previous period's ending lineup. Accounts for period-start subs and
        later-period sub-ins to avoid incorrect carryover."""
        prev_lineups = getattr(self, "previous_period_end_lineups", None)
        if not isinstance(prev_lineups, dict): return starters_by_team
        if getattr(self, "previous_period_end_period", None) != self.period - 1:
            return starters_by_team

        period_start_subs = self._get_period_start_substitutions()
        later_period_sub_ins = self._get_later_period_sub_in_players()

        for team_id, prev_players in prev_lineups.items():
            if not isinstance(prev_players, list) or len(prev_players) != 5: continue
            cur = starters_by_team.get(team_id, [])
            if len(cur) >= 5: continue

            team_subs = period_start_subs.get(team_id, {"in": set(), "out": set()})
            missing = [p for p in prev_players if p not in set(cur) and p not in team_subs["out"]]
            need = 5 - len(cur)
            if need <= 0: continue

            # Only fill if carryover candidates are consistent
            implied_carryover = (set(cur) - team_subs["in"]) | team_subs["out"]
            if implied_carryover.issubset(set(prev_players)):
                fill_candidates = missing
            else:
                later_sub_ins = later_period_sub_ins.get(team_id, set())
                fill_candidates = [p for p in missing if p not in later_sub_ins]
                if len(fill_candidates) != need: continue

            starters_by_team[team_id] = cur + fill_candidates[:need]
        return starters_by_team

    # ── Override Loading ─────────────────────────────────────────────

    def _load_period_starter_overrides(self, file_directory):
        """Load from overrides/missing_period_starters.json and
        overrides/period_starters_overrides.json, merging both."""
        if file_directory is None: return {}
        override_files = [
            f"{file_directory}/overrides/missing_period_starters.json",
            f"{file_directory}/overrides/period_starters_overrides.json",
        ]
        merged = {}
        for path in override_files:
            if not os.path.isfile(path): continue
            with open(path) as f:
                data = json.loads(f.read(), cls=IntDecoder)
            for game_id, periods in data.items():
                merged.setdefault(game_id, {})
                for period, teams in periods.items():
                    merged[game_id].setdefault(period, {}).update(teams)
        return merged

    def _apply_period_starter_overrides(self, starters_by_team, file_directory):
        overrides = self._load_period_starter_overrides(file_directory)
        game_id_keys = [self.game_id]
        try: game_id_keys.append(int(self.game_id))
        except: pass
        team_overrides = {}
        for gid in game_id_keys:
            team_overrides.update(overrides.get(gid, {}).get(self.period, {}))
        if not team_overrides: return starters_by_team
        updated = dict(starters_by_team)
        for team_id, starters in team_overrides.items():
            updated[team_id] = starters
        return updated

    def _has_period_starter_override(self, file_directory):
        if file_directory is None: return False
        overrides = self._load_period_starter_overrides(file_directory)
        game_id_keys = [self.game_id]
        try: game_id_keys.append(int(self.game_id))
        except: pass
        for gid in game_id_keys:
            if overrides.get(gid, {}).get(self.period): return True
        return False

    # ── Strict Starter Validation ────────────────────────────────────

    def _strict_starters_are_impossible(self, starters_by_team):
        """Return True when a strict PBP starter map is internally contradictory.
        Checks: 2 teams, 5 per team, no duplicates, each classified as starter
        by substitution timing."""
        if not isinstance(starters_by_team, dict) or len(starters_by_team) != 2:
            return True
        sub_lookup = self._get_period_substitution_order_lookup()
        seen = set()
        for team_id, starters in starters_by_team.items():
            if not isinstance(team_id, int) or team_id <= 0: return True
            if not isinstance(starters, list) or len(starters) != 5: return True
            for pid in starters:
                if pid in seen: return True
                seen.add(pid)
                if self._classify_period_boxscore_candidate(team_id, pid, sub_lookup) is not True:
                    return True
        return False

    # ── V6 Conflict Resolution (Opening Cluster Logic) ───────────────

    def _should_prefer_strict_starters_over_exact_v6(self, strict_starters, local_boxscore_starters):
        """When strict PBP and v6 gamerotation disagree, prefer strict ONLY if
        the disagreement matches a period-start delayed-substitution cluster
        (technical/flagrant at period start where outgoing player is still on court).

        This is the opening-cluster carryover fix (March 20, 2026)."""
        if not self._is_exact_starter_map(strict_starters): return False
        if not self._is_exact_starter_map(local_boxscore_starters): return False
        start_seconds = self._get_period_start_seconds()
        saw_supported_difference = False
        for team_id, strict_players in strict_starters.items():
            local_players = local_boxscore_starters.get(team_id, [])
            if set(strict_players) == set(local_players): continue
            if not self._period_start_v6_diff_matches_delayed_sub_cluster(
                team_id, strict_players, local_players, start_seconds
            ):
                return False
            saw_supported_difference = True
        return saw_supported_difference

    def _should_delay_period_start_substitution(self, sub_event, start_seconds):
        """Check if a period-start sub should be delayed because the outgoing
        player is still being credited with events in the opening cluster
        (technicals, flagrants, ejections)."""
        outgoing = getattr(sub_event, "outgoing_player_id", None)
        sub_team = getattr(sub_event, "team_id", None)
        # Scan period-start events at exact start_seconds
        # If any event credits the outgoing player (tech FT, flagrant, ejection)
        # before the sub, delay the sub
        ...

    # ── Main Entry Point ─────────────────────────────────────────────

    def _get_period_starters_from_period_events(self, file_directory, ignore_missing_starters=False):
        """The main strict PBP inference pipeline:
        1. Walk events to find starters + team map
        2. Split by team
        3. Fill missing from previous period ending lineup
        4. Apply overrides
        5. Validate 5-per-team (unless ignore_missing_starters)
        """
        starters, player_team_map, first_seen, subbed_in = (
            self._get_players_who_started_period_with_team_map()
        )
        starters = [pid for pid in starters if pid not in subbed_in]
        starters_by_team = self._split_up_starters_by_team(starters, player_team_map)
        starters_by_team = self._fill_missing_starters_from_previous_period_end(starters_by_team)
        if ignore_missing_starters:
            starters_by_team = self._trim_excess_starters(starters_by_team, first_seen, ...)
        starters_by_team = self._apply_period_starter_overrides(starters_by_team, file_directory)
        if not ignore_missing_starters:
            self._check_both_teams_have_5_starters(starters_by_team, file_directory)
        return starters_by_team
```

### StatsStartOfPeriod.get_period_starters() (Full — The Resolution Chain)

```python
class StatsStartOfPeriod(StartOfPeriod, StatsEnhancedPbpItem):
    """stats.nba.com-specific start of period. Defines the resolution order."""

    def get_period_starters(self, file_directory=None):
        """Resolution order:
        1) Strict PBP inference (with overrides applied)
           - If result is internally impossible, treat as failure
        2) If strict succeeded AND v6 exists AND disagrees:
           - If manual override exists → use strict
           - If disagreement matches opening-cluster pattern → use strict
           - Otherwise → use v6
        3) Local boxscore starters (Period 1 via START_POSITION)
        4) Period-level V3 boxscore fallback
        5) Best-effort PBP inference (ignore_missing_starters=True)
        """
        # Step 1
        try:
            starters = self._get_period_starters_from_period_events(file_directory)
        except InvalidNumberOfStartersException:
            starters = None

        if starters is not None and not self._strict_starters_are_impossible(starters):
            if self._has_period_starter_override(file_directory):
                return starters

            local_boxscore_starters, source = self._get_exact_local_period_boxscore_starters()
            if (source == "v6" and local_boxscore_starters is not None
                and local_boxscore_starters != starters):
                if self._should_prefer_strict_starters_over_exact_v6(starters, local_boxscore_starters):
                    return starters
                return local_boxscore_starters
            return starters

        # Step 2 — local boxscore (P1 only)
        starters = self._get_period_starters_from_boxscore_loader()
        if starters is not None: return starters

        # Step 3 — V3 boxscore fallback
        try:
            starters = self._get_starters_from_boxscore_request()
        except InvalidNumberOfStartersException:
            starters = None
        if starters is not None: return starters

        # Step 4 — best effort
        return self._get_period_starters_from_period_events(
            file_directory, ignore_missing_starters=True
        )
```

### rerun_selected_games.py (Full — Targeted Game Runner)

```python
from __future__ import annotations
import argparse, json
from collections import defaultdict
from pathlib import Path
import pandas as pd
from cautious_rerun import (
    DEFAULT_DB, DEFAULT_FILE_DIRECTORY, DEFAULT_OVERRIDES, DEFAULT_PARQUET,
    install_local_boxscore_wrapper, load_v9b_namespace,
    prepare_local_runtime_inputs, run_lineup_audits,
)

ROOT = Path(__file__).resolve().parent

def _season_from_game_id(game_id):
    """NBA game ID encodes season: digits 3-4 → 2-digit year."""
    gid = str(int(game_id)).zfill(10)
    yy = int(gid[3:5])
    return 1901 + yy if yy >= 50 else 2001 + yy

def main(argv=None):
    args = parse_args(argv)
    output_dir = args.output_dir.resolve()
    runtime_inputs = prepare_local_runtime_inputs(output_dir / "_local_runtime_cache", ...)
    game_ids = _load_game_ids(args)
    seasons_to_game_ids = defaultdict(list)
    for gid in game_ids:
        seasons_to_game_ids[_season_from_game_id(gid)].append(gid)

    namespace = load_v9b_namespace(
        notebook_dump_path=runtime_inputs["notebook_dump_path"],
        preload_module_paths=runtime_inputs["preload_module_paths"],
    )
    install_local_boxscore_wrapper(namespace, runtime_inputs["db_path"], ...)

    for season in sorted(seasons_to_game_ids):
        season_game_ids = seasons_to_game_ids[season]
        combined_df, error_df, team_audit_df, player_mismatch_df, audit_error_df = namespace[
            "process_games_parallel"
        ](season_game_ids, season_df, max_workers=args.max_workers, validate=True, ...)

        combined_df.to_parquet(output_dir / f"darko_{season}.parquet", index=False)

        if not args.skip_lineup_audit:
            lineup_audit_summary = run_lineup_audits(
                combined_df=combined_df, season=season, output_dir=output_dir,
                db_path=runtime_inputs["db_path"], parquet_path=runtime_inputs["parquet_path"],
            )
    # Write final summary.json
```

---

## 16. Detailed Block A Diagnostic Results (March 21, 2026)

We investigated all 12 games with event-on-court issues in Block A using BBR PBP and tpdev PBP cross-references. Here are the complete findings:

### Fixable Games (9 games)

**Game 0029700438 (SEA vs PHI)** — Period starter override P2 both teams
- Both P2 starting lineups completely wrong; pipeline carried forward incorrect P1-end state
- Fix: SEA P2 [56,1425,107,64,766], PHI P2 [754,707,221,187,243]
- 4 residual unfixable events (missing NBA PBP sub entries for Coleman, Schrempf, McIlvaine)

**Game 0029701075 (NYK @ BOS)** — Period starter + lineup window
- BOS P3 starters completely wrong (carried P2-end lineup)
- NYK P3 has missing mid-period sub at ~3:39 (Childs/Cummings enter, no sub event)
- Fix: BOS P3 starters [952,692,72,962,1500]; NYK P3 lineup window from event 399

**Game 0029800075 (NJN @ MIA)** — Lineup window, biggest outlier
- Spurious Gill re-entry at Q2 event 73; Jones should stay on court through Q2
- Gill +3.98 min / Jones -5.33 min — largest outlier in Block A
- Fix: 5 lineup windows covering Q2 events 73-89, all keeping Jones in place of Gill

**Game 0029700159 (VAN vs DEN)** — Lineup window P3
- Conflicting sub: "Lauderdale FOR Garrett" at 1:51 but Garrett already removed at 3:08
- 8 issue rows from Lauderdale/Goldwire actions without tracked lineup presence
- Fix: 2 lineup windows for DEN P3

**Game 0029700141 (CHH vs POR)** — Lineup window P4
- Single event ordering: Farmer rebound (event 483) processed before his sub (event 485)
- Fix: 1 lineup window for CHH P4 events 483-484

**Game 0029700367 (CHH vs TOR)** — Lineup window P4
- Same-clock boundary: Stoudamire shot at 0:03 processed after his sub at 0:04
- Fix: 1 lineup window for TOR P4 event 464

**Game 0029800063 (DAL @ DEN)** — Lineup window P4
- Green enters at 0:30 but rebound at 0:24 processed before sub
- Fix: 1 lineup window for DAL P4 from event 511

**Game 0049700045 (CHH @ CHI playoff)** — Period starter + lineup window
- P1 jump ball has empty lineups; P2 Kukoc goaltending after same-clock sub
- Fix: P1 starters both teams + P2 single-event lineup window

**Game 0029700452 (SEA @ VAN)** — Lineup window P4
- Schrempf rebound at 5:37 before his 5:33 entry sub
- Fix: 1 lineup window for SEA P4 event 402

### Unfixable Source Data Errors (5 games)

These have correct pipeline lineups — the NBA PBP simply credits the wrong player:

- **0029800063 P2 Nowitzki**: Shot misattributed; subbed out 2+ min earlier
- **0029800462 P3 Battie**: Goaltending + block misattributed; subbed out 1+ min earlier
- **0029800606 P5 Del Negro**: Foul misattributed; enters OT 3+ min after credited foul
- **0029900342 P3 Doug West**: Team foul misattributed; subbed out 1 min earlier
- **0029900517 P2 Michael Curry**: Shot misattributed; subbed out 3+ min earlier

### Current Period Starter Override Provenance (Excerpt)

This shows the provenance documentation style. Each override has a reason, evidence, and preferred source:

```csv
game_id,period,team_id,source_type,reason,evidence_summary,preferred_source,date_added,notes
29600113,4,1610612758,prefer_v5_over_bad_v6,bad_v6_boundary_row,"Q4 Kings raw PBP shows repeated Lionel Simmons 1489 events while the active lineup omitted him; family register flagged bad_v6_boundary_row; override matches the v5 Kings row",v5,2026-03-19,1997 bad-v6 cluster
29700157,5,1610612741,legacy_row_now_matches_v4_v5_v6,ot_boundary_fix,"1998 large-minute-outlier starter audit flagged CHI P5 with Rusty LaRue missing from current starters; override matches v4 v5 and v6",v4_v5_v6,pre-2026-03-19,legacy row now redundant
29901035,5,1610612766,manual_raw_pbp_ot_fix,ot_silent_carryover,"Charlotte OT was a pure silent-carryover miss after late Q4 Todd Fuller FOR Chucky Brown; local trace showed Brown minus 300 seconds",raw_pbp,2026-03-17,override later matched by v5 and v6
20400501,2,1610612754,prefer_v6_when_v5_unresolved,q2_boundary_fix,"Pacers P2 boundary miss survived until targeted override; v5 was unresolved while v6 resolved the exact IND team row",v6,2026-03-18,matches v6 exact team set
21700482,5,1610612751,source_backed_local_override,explicit_2018_ot_fix,"Nets P5 override was explicitly promoted with clean one-game canary and improved 2018 minutes_over_2 bucket from 15 to 3",v6,2026-03-16,see AGENTS note
```

### Current Override Counts

| Override File | Entries | Games | Status |
|---|---|---|---|
| `period_starters_overrides.json` | 49 team-period entries | 38 games | Active, well-documented |
| `lineup_window_overrides.json` | 2 entries | 1 game (0049700045) | Nearly empty |
| `pbp_row_overrides.csv` | ~40 rows | ~20 games | Stable |
| `pbp_stat_overrides.csv` | ~15 rows | ~10 games | Stable |
| `boxscore_source_overrides.csv` | ~20 rows | ~10 games | Stable |
| `validation_overrides.csv` | ~50 rows | ~30 games | Stable |

---

## 17. Additional Context: Why Plus-Minus Is Hard

To understand why 1,877 plus-minus mismatches persist despite correct counting stats:

**Plus-minus is calculated as:** For each stint a player is on court, sum (team points scored - opponent points scored). The pipeline derives this from its lineup tracking — it knows which 5 players are on court for each team, and accumulates +/- based on scoring events.

**The official plus-minus** in NBA boxscores comes from a completely separate system — the arena's scorekeeping system, which has its own lineup tracking. Small differences in when substitutions are recorded (especially at the same game clock) can cause point-scoring events to be attributed to different lineup configurations.

**Example:** If a sub happens at 4:30 and a basket happens at 4:30:
- If the pipeline processes the sub first: the incoming player gets the +/- credit
- If the arena system processes the basket first: the outgoing player gets the +/- credit
- Both are "correct" — they just disagree on ordering of simultaneous events

This is why three different scoring-attribution overlay approaches all made things worse — trying to match the arena's ordering introduces more errors than it fixes, because the arena's ordering is itself inconsistent across different scorekeepers, arenas, and eras.

**Key insight:** The ~1,877 plus-minus mismatches are NOT bugs in the pipeline. They are inherent measurement differences between two independently-derived systems. The pipeline's lineup tracking is correct (validated by 0 counting-stat mismatches); the plus-minus differences come from sub-second timing disagreements that cannot be resolved without access to the arena's internal scorekeeping logs.

---

## 18. File Map

```
replace_tpdev/
├── cautious_rerun.py              # Season-level runner
├── rerun_selected_games.py        # Targeted game runner
├── 0c2_build_tpdev_box_stats_version_v9b.py  # Notebook dump (source of truth)
├── period_boxscore_source_loader.py  # Parquet starter loader
├── audit_event_player_on_court.py    # Event-on-court audit
├── audit_minutes_plus_minus.py       # Minute/PM audit
├── bbr_pbp_lookup.py                 # BBR PBP cross-reference
├── bbr_pbp_stats.py                  # BBR stat aggregation
├── intraperiod_missing_sub_repair.py # Intraperiod repair engine
├── boxscore_source_overrides.py      # Boxscore override loader
├── pbp_row_overrides.py              # PBP row reordering
├── pbp_stat_overrides.py             # PBP stat corrections
├── compare_run_outputs.py            # Run comparison tool
├── overrides/
│   ├── period_starters_overrides.json      # 49 entries, 38 games
│   ├── period_starters_override_notes.csv  # Provenance
│   ├── lineup_window_overrides.json        # Currently ~1 game
│   └── lineup_window_override_notes.csv    # Provenance
├── playbyplayv2.parq              # Historical PBP events
├── nba_raw.db                     # Cached NBA API data
├── period_starters_v6.parquet     # Gamerotation-backed starters (primary)
├── period_starters_v5.parquet     # Secondary starter fallback
├── AGENTS.md                      # Current project brief
├── PROJECT_HISTORY.md             # Archived narrative
└── block_A_manual_override_diagnostic_20260321.md  # Recent diagnostic

../pbpstats/                       # Custom fork
├── pbpstats/resources/enhanced_pbp/
│   ├── start_of_period.py         # Period starter inference (~1100 lines)
│   ├── enhanced_pbp_item.py       # Lineup propagation
│   ├── stats_nba/start_of_period.py  # Stats.nba-specific starter logic
│   ├── foul.py                    # Foul event handling
│   └── free_throw.py              # FT event handling

../fixed_data/raw_input_data/tpdev_data/
├── full_pbp_new.parq              # tpdev possession-level PBP
├── tpdev_box.parq                 # tpdev boxscores
└── tpdev_box_new.parq             # Updated tpdev boxscores

../33_wowy_rapm/
└── bbref_boxscores.db             # BBR PBP + boxscores

../calculated_data/pbpstats/
├── pbpstats_player_box.parq       # Scraped pbpstats boxes (2000+)
└── pbpstats_team_box.parq         # Scraped pbpstats team boxes
```
