# Project Handoff: replace_tpdev — NBA Historical Box Score Pipeline
## March 22, 2026 — Policy Decision Edition

Historical note:
- this handoff is the pre-adoption reviewed-policy decision deck
- the adopted March 22 release-policy result is published separately in:
  - `/Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev/reviewed_release_policy_decision_20260322_v1.md`
  - `/Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev/phase6_reviewed_frontier_inventory_20260322_v1/summary.json`

## What This Document Is

You are being asked for **policy decisions** on a complex NBA historical data pipeline. The pipeline is functionally complete — counting stats are locked clean, lineup-derived fields have been extensively investigated, and the remaining open cases have been triaged to the point where the project needs strategic decisions rather than more technical investigation.

This document gives you the full project context, summarizes all work completed since the last strategic review (March 21, 2026), and then presents 7 specific policy questions with deep game-level evidence for each. We need your recommendations on each question.

---

## 1. Project Overview

### What We're Building

An **offline-first replacement pipeline** for a legacy system called "tpdev" that produces NBA player box scores from play-by-play data. The pipeline covers **1997-2020** (23 seasons, ~30,000 games, ~685,000 player-game rows).

The pipeline:
1. Takes raw NBA play-by-play event data as input (`playbyplayv2.parq`, ~1997-2020)
2. Parses it through a custom fork of `pbpstats` (an open-source NBA PBP parser)
3. Produces per-player per-game box score rows including all counting stats (points, assists, rebounds, etc.) plus lineup-derived fields (Minutes, Plus_Minus)

### Why It Exists

- The old `tpdev` system depended on live NBA API endpoints that are no longer reliably available for historical data
- We need a reproducible offline pipeline that doesn't depend on NBA servers
- When official source data is wrong or incomplete, we use explicit documented repair policy instead of silent drift

### Current State Summary

- **Counting stats (PTS, AST, REB, STL, BLK, TOV, FGA, FGM, etc.) are DONE** — 685,882 rows across 1997-2020, zero failed games, zero errors, zero audit mismatches against official boxscores
- **Lineup-derived fields (Minutes, Plus_Minus) are the active frontier** — these depend on correctly tracking which 5 players are on court for each team at every moment
- The project has moved from parser experimentation to **bounded exception management** — the remaining work is about classifying and closing a fixed set of residual cases, not open-ended R&D

## What Changed Since The First Phase 6 Draft

This handoff was originally drafted before the final frontier-close evidence pass. The reviewed state is now:

- **0 bespoke investigation cases** remain in the reviewed shortlist
- **17 live open games = 7 `documented_hold` + 10 `policy_frontier_non_local`**
- `0021700394` stayed a broader same-clock accumulator defect after the bounded four-source comparison
- `0029700159` live vs archived candidate was recoverable and rerun; the result was `tradeoff_or_worse`, so the live state stays unchanged
- `0029800606` remains **infrastructure debt / `unstable_control`**, not part of the policy-frontier blocker queue
- Live correction inventory remains **54 active corrections = 48 period-start + 6 window/event**

Reviewed frontier artifacts:
- `phase6_true_blocker_shortlist_20260322_v1.csv`
- `phase6_blocker_policy_frontier_20260322_v1.md`
- `phase6_open_blocker_inventory_20260322_v1.csv`

Important artifact caveat:
- Some older selected-lane and raw issue CSVs have broken `player_name` fields that repeat the numeric `player_id`. The narrative names in this handoff are manually remapped from the underlying event evidence and should be trusted over those raw `player_name` columns.

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

The pipeline has a canonical correction manifest (`overrides/correction_manifest.json`) that compiles into runtime override files. The manifest currently contains **54 active corrections** (48 period-start, 6 window/event), **~25 rejected corrections** (with documented reasons), and **~25 accepted residual annotations** (source-limited cases that document known upstream errors without changing runtime behavior).

| Override File | Purpose | Format |
|---|---|---|
| `overrides/period_starters_overrides.json` | Fix who starts each period | `{game_id: {period: {team_id: [5 player_ids]}}}` |
| `overrides/lineup_window_overrides.json` | Fix intraperiod lineup windows | `{game_id: [{period, team_id, start_event_num, end_event_num, lineup_player_ids}]}` |
| `overrides/correction_manifest.json` | Canonical authoring surface | Full correction records with provenance |
| `pbp_row_overrides.csv` | Fix event ordering in the PBP stream | Row-level reordering |
| `pbp_stat_overrides.csv` | Fix stat attribution on events | Stat-credit corrections |
| `boxscore_source_overrides.csv` | Fix confirmed bad official boxscore rows | Production patches |
| `validation_overrides.csv` | Manual tolerance exceptions for known issues | Audit exceptions |

### Runner Tools and Provenance

- `cautious_rerun.py` — full season-level runner
- `rerun_selected_games.py` — targeted runner for specific game IDs
- `run_golden_canary_suite.py` — 27-case regression gate
- `build_lineup_residual_outputs.py` — per-game quality classification and blocker/raw count splits
- `build_plus_minus_reference_report.py` — PM characterization across all blocks

As of the latest hardening pass, fresh reruns default to `runtime_input_cache_mode = fresh-copy`: core inputs are copied into per-run cache directories, override surfaces are snapshotted per-run, and each run emits `runtime_input_provenance.json` for full auditability.

### Cross-Reference / Audit Sources (NOT runtime dependencies)

| Source | What It Contains | Reliability | Coverage |
|---|---|---|---|
| `full_pbp_new.parq` (tpdev) | Possession-level PBP lineups | Good for lineup disputes | 1997-2020 (some gaps) |
| `bbref_boxscores.db` (BBR) | Basketball Reference PBP + boxscores | Independent source, has sub events | 1997-2020 |
| `pbpstats_player_box.parq` | Local pbpstats full-game box | Good cross-check | 2000-2020 |
| Official NBA boxscores (via `nba_raw.db`) | The "truth" for counting stats | Canonical but has rare errors | 1997-2020 |

---

## 3. Data Sources and Their Reliability

### Primary Runtime Inputs

| Source | What It Contains | Reliability | Coverage |
|---|---|---|---|
| `playbyplayv2.parq` | Historical NBA PBP event stream | Good but has event ordering issues in early seasons | 1997-2020 |
| `nba_raw.db` | Cached NBA API responses (pbpv3, boxscore, summary) | Good for enrichment, not canonical chronology | 1997-2020 |
| `period_starters_v6.parquet` | Gamerotation-backed period starters | Best single source, but has gaps and some wrong rows | Broad but incomplete |
| `period_starters_v5.parquet` | Earlier scrape of period starters | Secondary fallback, good coverage | Broad |

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

## 4. The Core Problem: Event Ordering Ambiguity

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

## 5. What Has Been Tried and Failed

### Failed: Global chronology rewrite (March 12)
Tested both pure PERIOD/EVENTNUM and clock-aware hybrid ordering. Full 1997 rerun regressed badly. **Lesson: do not apply global reorder rules.**

### Failed: Three same-clock scoring-attribution fork fixes (March 20)

**The hypothesis was:** When a substitution and a scoring event share the same clock time, the pipeline might be using the wrong lineup (post-sub instead of pre-sub) for the scoring event.

Three architecturally different approaches were tried:

1. **Lineup propagation carryover** — Modify `_get_previous_raw_players()` to delay the sub's effect. Result: 4-minute lineup blowup in canary games, catastrophic regression.

2. **Event-local scoring overlay (foul-committer anchor)** — Read-only helper `_get_effective_scoring_current_players()` used by field_goal.py and free_throw.py. Did NOT mutate lineup chain. Result: Every single canary got worse (+4 to +14 pm mismatches each).

3. **Event-local scoring overlay (fouled-player anchor)** — Same architecture, different anchor player. Result: Even worse than Approach 2 (+4 to +15 pm mismatches).

**Critical finding:** The fundamental assumption was wrong. Using pre-sub lineups for same-clock scoring events does NOT improve accuracy — it makes it less accurate. The current pipeline behavior (sub is already live when scoring event is processed) is closer to reality. **Do not attempt another same-clock scoring overlay.**

### Failed: Narrow pending-sub patch for foul/FT boundaries (March 21)

A narrow non-opening same-clock pending-sub fix for foul/FT boundaries improved focused canary games but **regressed Block A overall** (+22 min mismatches, +85 pm mismatches vs baseline). Reverted.

### Accepted: Opening-cluster carryover fix (March 20, proved March 21-22)

Period-start substitutions are now delayed when the outgoing player is explicitly credited inside an opening technical/flagrant/ejection cluster. Validated on 4 real canaries (all resolved to zero plus-minus diff), then proved safe at season scale across 2013, 2014, 2015, 2019 with zero counting-stat regressions and zero minute regressions.

### Working but exhausted: Intraperiod missing-sub repair engine

Built `intraperiod_missing_sub_repair.py`. Tightened scoring rules through v4. **Net result: framework works correctly but produces 0 new uncovered auto-apply candidates in 1998-2000.** The early-era data is too noisy for automated repair.

---

## 6. Source Code: Key Pipeline Files

### Period Starter Resolution (the heart of the pipeline)

```python
class StatsStartOfPeriod(StartOfPeriod, StatsEnhancedPbpItem):
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

        # Fallback chain: local boxscore → V3 boxscore → best-effort PBP
        starters = self._get_period_starters_from_boxscore_loader()
        if starters is not None: return starters
        try:
            starters = self._get_starters_from_boxscore_request()
        except InvalidNumberOfStartersException:
            starters = None
        if starters is not None: return starters
        return self._get_period_starters_from_period_events(
            file_directory, ignore_missing_starters=True
        )
```

### Lineup Propagation (event-to-event tracking)

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

If the period starters are wrong, every subsequent event in the period has wrong lineups. If a substitution event is in the wrong position, the lineup change happens at the wrong time. If the NBA PBP is missing a substitution entirely, the lineup gets stuck.

### Event-on-Court Audit (how issues are detected)

```python
def _check_event_players(game_id, events, player_team_map):
    for event in events:
        current_lineups = event.current_players
        previous_lineups = event.previous_event.current_players
        for field in ("player1_id", "player2_id", "player3_id"):
            player_id = getattr(event, field)
            team_id = player_team_map.get(player_id)
            on_current = player_id in current_lineups.get(team_id, [])
            if not on_current:
                if isinstance(event, Substitution) and field == "player1_id":
                    status = "sub_out_player_missing_from_previous_lineup"
                else:
                    status = "off_court_event_credit"
                rows.append({...})
```

An `off_court_event_credit` row means a player is credited with a game event (shot, rebound, foul, etc.) while the pipeline does not have them in the current lineup. This is the primary signal for lineup tracking errors — or for same-clock event ordering differences.

### Minute/PM Audit (how minutes are compared)

```python
def build_minutes_plus_minus_audit(darko_df, db_path, minute_outlier_threshold=0.5):
    """Compare pipeline output against official boxscore.
    Returns DataFrame with:
    - Minutes_diff, has_minutes_mismatch (>1 second), is_minutes_outlier (>0.5 min)
    - Plus_Minus_diff, has_plus_minus_mismatch (any diff)
    """
    merged = prepared.merge(official, on=["game_id", "player_id", "team_id"])
    merged["Minutes_diff"] = merged["Minutes_output"] - merged["Minutes_official"]
    merged["has_minutes_mismatch"] = merged["Minutes_diff"].abs() > (1.0 / 60.0)
    merged["is_minutes_outlier"] = merged["Minutes_diff"].abs() > minute_outlier_threshold
    return merged
```

The two long verbatim code payloads that used to live here have been moved intact to **Section H of `handoff_appendix_game_evidence_20260322.md`** so the main handoff can stay focused on the current policy frontier.

Behavior-level summary of the moved code:
- `cautious_rerun.py` is the provenance-hardened runner that snapshots runtime inputs, loads the notebook dump safely, injects local boxscore/period sources, and runs lineup audits after reruns.
- `StatsStartOfPeriod.get_period_starters()` is the period-start resolution chain that arbitrates strict PBP inference, manual overrides, exact v6/v5 boxscore-backed starters, and the opening-cluster carryover exception.

### Correction Manifest Schema

The canonical correction manifest (`overrides/correction_manifest.json`) is the single authoring surface for all lineup corrections and residual annotations. The compiler (`build_override_runtime_views.py`) reads the manifest and produces the runtime JSON/CSV override files.

#### Correction record schema

Each `corrections` record must include:
- `correction_id` — unique atomic runtime mutation ID
- `episode_id` — groups related corrections by root-cause investigation
- `status` — enum: `proposed | active | retired | rejected`
- `domain` — v1: only `lineup`
- `scope_type` — enum with explicit semantics:
  - `period_start`: replaces `StartOfPeriod.current_players` for the given team and period
  - `window`: applies to `event.current_players` for all events where `start_event_num <= event_num <= end_event_num` (inclusive)
  - `event_single`: convenience alias for `window` with `start_event_num == end_event_num`
- `game_id`, `period`, `team_id`
- `start_event_num`, `end_event_num` — when applicable; windows are inclusive
- `lineup_player_ids` — full 5-player array for explicit mode
- `swap_out`, `swap_in` — optional delta mode; compiler resolves to full lineup against inferred state
- `defect_class` — from the residual taxonomy
- `reason_code` — structured enum (e.g., `bad_v6_boundary`, `missing_sub_in_source`, `event_ordering`, `ot_carryover_miss`, `silent_carryover`)
- `evidence_summary` — structured text
- `source_primary`, `source_secondary` — enums from controlled vocabulary: `raw_pbp | v6 | v5 | bbr | tpdev | boxscore_start_position | period_boxscore_v3 | manual_trace`
- `preferred_source` — which source was trusted
- `confidence` — enum: `high | medium | low`
- `validation_artifacts` — array of artifact paths/names
- `date_added`
- `supersedes` — optional array of correction_ids this replaces
- `notes` — free text

#### Residual annotation schema

`residual_annotations` stores only **manual overrides, accepted exceptions, and disputed cases** — not exhaustive hand-labeling of every PM row. Default classification is computed algorithmically.

Each record:
- `annotation_id`
- `game_id`, optional `period`, `team_id`, `player_id`, `event_num`
- `residual_class` — from the taxonomy (see Section 7)
- `status` — enum: `open | accepted | disputed`
- `scope_type` — optional, for event-level annotations
- `source_primary`, `source_secondary`
- `confidence`
- `linked_episode_id` — optional, links to correction episode
- `validation_artifacts` — array
- `notes`

Rejected fix families (same-clock overlays, pending-sub regressions) do NOT go in residual annotations. They belong in the Golden Canary sentinel suite and in `PROJECT_HISTORY.md`.

#### Compiler lint checks

The compiler enforces:
- 5 unique players per lineup
- `team_id` belongs to the game
- All player IDs belong to that team's roster for the game
- No player appears on both teams
- No overlapping active windows for same game/period/team
- No duplicate correction IDs
- Provenance fields complete for active corrections
- Deterministic output ordering
- v1 rejects non-lineup active corrections

#### Current manifest state (March 22, 2026)

- **54 active corrections** (48 period-start + 6 window/event)
- **~25 rejected corrections** with documented rejection reasons and scratch validation artifacts
- **~25 accepted residual annotations** (source-limited upstream errors)
- Compile summary: `overrides/correction_manifest_compile_summary.json`

---

## 7. Correction Evaluation Criteria

Every proposed correction must satisfy ALL of:
- No counting-stat mismatches
- No failed games
- No event-stats errors
- No increase in severe minute outliers (> 0.5 min)
- `max_abs_minute_diff` does not worsen for affected games
- `sum_abs_minute_diff_over_0_1` improves or holds
- Actionable event-on-court rows decrease or hold
- PM worsening allowed only when lineup-integrity metrics improve

This is why 5 scratch correction attempts were rejected in Phase 6 and only 1 was accepted — the bar is high and strictly enforced.

### Validation Ladder

Every change must pass in order:
1. Compile-equivalence gate — registry compiles to runtime artifacts, lint passes
2. Static lint — 5 unique players per lineup, team/roster validation, no overlapping windows
3. Affected-game rerun
4. Golden Canary suite (27 cases, 5 categories)
5. Block rerun
6. Full proving loop (before historical baseline promotion)

### Residual Classification Taxonomy

| Residual class | Signals | Default action |
|---|---|---|
| `fixable_lineup_defect` | Event-on-court issue + clear external agreement | Add correction |
| `candidate_systematic_defect` | Same archetype repeats across seasons | Consider fork change after proving |
| `source_limited_upstream_error` | Player impossible but pipeline lineup coherent; external audit confirms source error | Document only |
| `candidate_boundary_difference` | Counts exact, minutes clean, PM differs | Provisional default for PM-only |
| `accepted_boundary_difference` | Same as above, promoted after review | Closed, not chased |

---

## 8. Complete Work Done Since Last Strategic Review (March 21, 2026)

### Phase 0: Registry Migration — COMPLETE

Built `overrides/correction_manifest.json` as the canonical authoring surface. Each correction record includes:
- `correction_id`, `episode_id`, `status` (proposed/active/retired/rejected)
- `domain` (v1: only `lineup`), `scope_type` (period_start/window/event_single)
- `game_id`, `period`, `team_id`, `start_event_num`, `end_event_num`
- `lineup_player_ids` (full 5-player array)
- `defect_class`, `reason_code`, `evidence_summary`
- `source_primary`, `source_secondary`, `preferred_source`, `confidence`
- `validation_artifacts`, `date_added`, `notes`

Built `build_override_runtime_views.py` — compiler that enforces roster/team validation and produces the runtime JSON/CSV override files. Round-trip proved byte-equivalent.

Seeded from all existing live runtime corrections: 48 period-start + 5 window/event = 53 active at migration time. No-op migration proof recorded. 9 tests pass.

### Phase 1: Opening-Cluster Season Proof — COMPLETE

Proved the opening-cluster fork fix safe across 2013, 2014, 2015, 2019:
- 0 counting-stat regressions across all 4 seasons
- 0 minute regressions
- All 4 opening-cluster canary games stayed improved:
  - `0021200444` P4: Shannon Brown / Markieff Morris → plus_minus_diff = 0
  - `0021300594` P3: David West / Luis Scola → plus_minus_diff = 0
  - `0021400336` P2: LeBron James / Kyrie Irving → plus_minus_diff = 0
  - `0021800748` P3: Lou Williams / Shai Gilgeous-Alexander → plus_minus_diff = 0

The pending-sub-narrow patch was reverted (regressed Block A: +22 min mismatches, +85 PM mismatches).

### Phase 2-3: Block A Closure — COMPLETE (as far as local overrides can go)

Triaged every Block A candidate game. Each was tested with a scratch validation rerun:

| Game | Candidate | Result | Reason |
|---|---|---|---|
| `0029700438` | P2 dual-team starter | **Rejected** | Full no-op vs live baseline |
| `0029800075` | Q2 Jones/Gill window | **Rejected** | Introduced counting-stat mismatches, 1 minute outlier, 6 event-on-court rows |
| `0029800063` | P4 Green rebound window | **Rejected** | Fixes 1 event row but worsens minutes (1→3 mismatches), leaves Dirk P2 E211 open |
| `0029700141` | P4 Farmer event-order | **Deferred** | Keep documented, revive only if reappears in live queue |
| `0029701075` | P3 BOS starter | **Rejected** | Proposed lineup already matches current inferred starters |

Reviewed Block A closure outcomes landed in three distinct categories:
- **True `source_limited_upstream_error` promotions:** `0029800063`, `0029800462`, `0029800606`, `0029900342`, `0029900517`, `0029800075`, `0029700438`, `0029700452`, `0049700045`
- **Documented game-level `candidate_systematic_defect` holds:** `0029701075`, `0029800661`
- **Row-grain source-limited split inside a still-open holdout:** `0029700159` P3 E349 broken Garrett sub row

Net Block A movement: 13 open games → 2, 50 actionable event rows → 13.

### Phase 4: Golden Canary Suite + Residual Reporting — COMPLETE

Populated all 5 manifest sections:

| Category | Cases | Purpose |
|---|---|---|
| `positive_canaries` | 4 | Opening-cluster success cases — must stay clean |
| `fixed_dirty_games` | 3 | Games with active corrections — must stay within envelope |
| `failed_patch_anti_canaries` | 5 | Rejected same-clock/pending-sub cases — must stay stable |
| `source_limited_negative_controls` | 5 | Documented upstream errors — must not get "fixed" |
| `pm_only_boundary_controls` | 10 | Clean modern games with PM delta — stability canaries |

Suite v3: 27 cases, 0 failures, `suite_pass = true`.

Added explicit `stability_class` semantics: 1 case (`0029800606`) tagged `unstable_control` due to a known block-vs-single-game runner parity split. Suite now emits both `suite_pass_all_cases` and `suite_pass_stable_cases_only`.

Full residual reporting layer produces: `actionable_queue.csv`, `source_limited_residuals.csv`, `boundary_difference_residuals.csv`, `game_quality.csv`, `summary.json` with both `raw_counts` and `blocker_counts`.

### Phase 4.5: Runner Provenance Hardening

Fresh reruns now default to `runtime_input_cache_mode = fresh-copy`. Override surfaces are snapshotted per-run. This directly closed override-lineage drift that caused a spurious reproducibility alarm on game `0029700159` (the saved Block A residual was built from an older lineup-window override payload, while the current live manifest uses a different player in the override slot — different inputs, different outputs, not nondeterminism).

### Phase 5: PM Characterization — COMPLETE

Built `build_plus_minus_reference_report.py` across all 5 blocks. **1,856 total PM reference delta rows** classified:

| Class | Rows | Games | % |
|---|---|---|---|
| `candidate_boundary_difference` | 1,744 | 611 | 93.9% |
| `source_limited_upstream_error` | 81 | 11 | 4.4% |
| `open_lineup_blocker` | 31 | 13 | 1.7% |
| **Total** | **1,856** | | |

Stratified sample of 22 boundary-difference rows (balanced across eras and game types): all clean, no hidden recurring bug class.

The PM residual analysis supports the core project finding: **the ~1,744 plus-minus boundary differences are inherent measurement differences between two independently-derived systems.** The pipeline's lineup tracking is correct (validated by 0 counting-stat mismatches); the plus-minus differences come from sub-second timing disagreements that cannot be resolved without access to the arena's internal scorekeeping logs.

Formal PM deliverable published with lane map, open game queue, source-limited list, and sample.

### Phase 6: Blocks B-E Lineup-Integrity Queue + Frontier Close — COMPLETE AS A POLICY FRONTIER

**Source-limited promotions landed (8 games across B and E):**

| Game | Block | What Was Wrong | Why Source-Limited |
|---|---|---|---|
| `0020300778` | B | Jeffries phantom-three + whole-game source split | Output matches pbpstats on 23/23 players, BBR on 22/23; official matches 0/23 |
| `0020400526` | B | Jeffries P3 E453 | Same Jeffries phantom event pattern |
| `0020101009` | B | Malformed P4 ejection/flagrant cluster | Named outgoing players already exited earlier in Q4 |
| `0021700482` | E | Contradictory OT cleanup cluster in cached pbpv3 | `full_pbp_new` supports only settled closing state |
| `0021700653` | E | Dwight Howard FTs before same-clock sub | Raw PBP credits FTs before substitution at same clock |
| `0021700813` | E | Jodie Meeks tech FT before period start | Raw PBP places tech FT before Start of 4th Period |
| `0021900622` | E | John Henson FT before same-clock sub | Same pattern as `0021700653` |
| `0041700117` | E | Marcus Smart FTs before same-clock sub | Same pattern |

**One real active correction accepted:**

`0021700886` — Knicks P3 E482, Mudiay field goal event-only window. Raw `pbpv3` and `playbyplayv2` both show Mudiay's made FG at 1:31 followed by same-clock subs Burke-for-Mudiay and Thomas-for-Beasley. The prior live issue row had advanced to the post-sub lineup. Validation: event-on-court 1→0, PM 6→2, minutes stayed at 0, counting stats clean. **Manifest now at 54 active corrections.**

**Scratch correction attempts tested and rejected (5 games):**

| Game | What Was Tested | Result | Why Rejected |
|---|---|---|---|
| `0021900487` | Event-only Memphis P2 E246 (Jackson Jr. rebound) | Blocker moved to P2 E239, introduced 6 minute mismatches | Trades one blocker for another, worsens minutes |
| `0041900155` | Event-only + widened window LAC P2 E348-E353 (Harrell rebound) | Blocker moved to P2 E348, 2 minute mismatches | Both attempts shifted blocker backward |
| `0021900920` | Widened window MEM P2 E307-E312 (Tolliver rebound) | True no-op — exact same blocker survived | Window correction had no effect |
| `0021900201` | Event-only OKC P3 E398 (Noel rebound) | Blocker moved to P3 E395, 2 minute + 2 PM mismatches | Trades one blocker for another |
| `0020000628` | Window NJN P2 E227-E230 (Van Horn foul) | Blocker moved from E227 to E229, 0.25-min tail unchanged, 2 PM mismatches | Trades one blocker for another |

**Diagnostic investigations completed:**

- `0021700394` — cluster-ledger trace built showing +9s minute residue distributed across 7 same-clock clusters and 10 affected players. 2 players (Westbrook, Kidd-Gilchrist) have no direct stint boundary on target cluster clocks. Diagnosed as broader same-clock accumulator defect, not a local override target.
- `0020400335` — confirmed severe-minute holdout with badly split cross-source minutes. Candidate engine returns `insufficient_local_context`.
- `0020900189`, `0021300593` — both confirmed as true period-start contradictions with genuinely conflicting sources.

**Frontier-close confirmation pass (after the earlier queue reduction):**

- `0021700394` received a bounded four-source confirmation pass across the known 7 clusters. No clearly one-sided disagreement was isolated; `full_pbp_new` was the coarsest source on repeated same-clock FT/sub boundaries, which sharpened but did not change the diagnosis.
- `0029700159` received a paired live-vs-archived candidate rerun comparison. The archived state was recoverable exactly, but the result was `tradeoff_or_worse`: mismatch/outlier/PM counts stayed flat and raw issue-row count worsened from `1 -> 2`, so the live state stayed unchanged.
- The reviewed frontier is now published in:
  - `phase6_true_blocker_shortlist_20260322_v1.csv`
  - `phase6_blocker_policy_frontier_20260322_v1.md`
  - `phase6_open_blocker_inventory_20260322_v1.csv`

**Net result outside Block A:** Open games: 22 → 15. Source-limited games: 0 → 10. Actionable event rows reduced from ~60 to ~20.

**Execution-lane result after frontier close:** `0` bespoke investigation cases, `7` `documented_hold`, `10` `policy_frontier_non_local`.

---

## 9. Current Residual State (March 22, 2026)

### Full 5-Block Quality Status

| Block | Seasons | Open | Source-Limited | Boundary-Diff | Override-Corrected | Exact |
|---|---|---|---|---|---|---|
| A | 1998-2000 | 2 | 10 | 68 | 40 | 4 |
| B | 2001-2005 | 2 | 4 | 160 | 43 | 2 |
| C | 2006-2010 | 1 | 1 | 124 | 43 | 3 |
| D | 2011-2016 | 1 | 0 | — | 43 | — |
| E | 2017-2020 | 11 | 5 | 149 | 43 | — |
| **Total** | | **17** | **20** | | | |

### Blocker Counts

| Metric | Block A | Block B | Block C | Block D | Block E | Total |
|---|---|---|---|---|---|---|
| Actionable event-on-court rows | 13 | 6 | 1 | 2 | 11 | **33** |
| Actionable residual rows | 20 | 8 | 1 | — | 21 | **~50** |
| Material minute rows (>0.1 min) | 7 | 2 | 0 | 0 | 0 | **~9** |
| Severe minute rows (>0.5 min) | 4 | 1 | 0 | 0 | 0 | **5** |

### Reviewed Execution Lanes

| Execution lane | Games | Meaning |
|---|---:|---|
| `documented_hold` | 7 | Still open, but not worth more local override churn without a policy change or new evidence |
| `policy_frontier_non_local` | 10 | Reviewed non-local lanes: same-clock controls / rebound survivors that are no longer active reduction targets |
| No remaining bespoke investigation lane | 0 | The frontier-close pass exhausted the last targeted evidence work |

### PM Characterization (all blocks combined)

| Class | Rows | Games |
|---|---|---|
| `candidate_boundary_difference` | 1,744 | 611 |
| `source_limited_upstream_error` | 81 | 11 |
| `open_lineup_blocker` | 31 | 13 |
| **Total PM reference delta rows** | **1,856** | |

---

## 10. The Current Done Definition (from AGENTS.md)

- **Counting stats:** zero mismatches, zero failed games, zero event-stats errors ✅ **MET**
- **Minutes:** no unresolved high-confidence outlier > 0.5 min; no open actionable issue > 0.1 min ❌ **NOT MET** (5 severe minute rows remain across 3 games)
- **Event-on-court:** no open actionable rows ❌ **NOT MET** (33 rows across 15 of the 17 open games)
- **Plus-minus:** all remaining deltas classified and reported ✅ **MET** (1,744 boundary-difference, 81 source-limited, 31 open-lineup-blocker — all classified)

The project cannot meet the strict done definition without a **blocker-policy choice**:
- there are no remaining bespoke investigation cases in the reviewed shortlist
- the remaining queue is already partitioned into `10` `policy_frontier_non_local` cases and `7` `documented_hold` cases
- further progress is therefore primarily about whether some reviewed lanes stop counting as blockers, not about an unfinished local-fix backlog

---

## 11. The 17 Open Games — Complete Evidence

> **Deep cross-source evidence appendix:** For full minute audit tables, raw event sequences, lineup states, scratch validation results, and cross-source comparisons for every game below, see the companion file `handoff_appendix_game_evidence_20260322.md`. The summaries below are drawn from that evidence.
>
> **Lane-name note:** The narrative “LANE 1-6” buckets below are policy-oriented groupings for readability. The canonical live lane ids remain the artifact labels in `phase6_open_blocker_inventory_20260322_v1.csv` (full blocker frontier) and `phase5_pm_deliverable_ABCDE_20260322_v6/pm_lane_summary.csv` (PM-only open frontier).

### LANE 1: Same-Clock Control / Guardrail Games (5 games, 6 event-on-court rows)

These are same-clock **controls / guardrails / review cases** in the same-clock canary infrastructure. They are NOT override targets. They exist to catch regressions from any future parser changes. All 5 are in Block E (2017-2020), the cleanest data era.

#### `0021700337` — SAS @ MEM, Period 3 (2018 season)
- **Role:** Main negative tripwire for the non-opening FT/sub same-clock family
- **2 event-on-court rows:**
  - P3 E528: Joffrey Lauvergne (player 203530, SAS) — `off_court_event_credit`
  - P3 E529: Joffrey Lauvergne (player 203530, SAS) — `off_court_event_credit`
- **Minutes:** max_abs_diff = 0.0, sum_abs_diff_over_0.1 = 0.0
- **PM reference delta:** 2 rows
- **What's happening:** A substitution and two scoring events occur at the same game clock. The pipeline processes the sub first, removing Lauvergne from the lineup before his events. The official scoring system credits Lauvergne.
- **Investigation history:** This game was the main guardrail for the three rejected same-clock scoring overlay approaches (all three made this game worse, confirming the pipeline's ordering is closer to correct).

#### `0021700377` — LAL @ CLE, Period 3 (2018 season)
- **Role:** Scorer/sub negative tripwire — explicitly frozen per AGENTS.md
- **1 event-on-court row:** P3 E421 — Jordan Clarkson (player 203903, LAL) — `off_court_event_credit`
- **Minutes:** max_abs_diff = 0.0
- **PM reference delta:** 2 rows
- **What's happening:** Same pattern — same-clock sub and scoring event, pipeline processes sub first.

#### `0021700514` — UTA @ PHX, Period 2 (2018 season)
- **Role:** Active same-clock teaching/review case in `foul_free_throw_sub_same_clock_ordering` family
- **1 event-on-court row:** P2 E243 — Royce O'Neale (player 1626220, UTA) — `off_court_event_credit`
- **Minutes:** max_abs_diff = 0.0
- **PM reference delta:** 2 rows

#### `0021801067` — BOS @ WAS, Period 3 (2019 season)
- **Role:** Registered same-clock boundary-review case
- **1 event-on-court row:** P3 E374 — Marcus Smart (player 203935, BOS) — `off_court_event_credit`
- **Minutes:** max_abs_diff = 0.0
- **PM reference delta:** 2 rows

#### `0021900333` — PHX @ WAS, Period 4 (2020 season)
- **Role:** Companion same-clock canary/review case
- **1 event-on-court row:** P4 E659 — Aron Baynes (player 1629661, PHX) — `off_court_event_credit`
- **Minutes:** max_abs_diff = 0.0
- **PM reference delta:** 2 rows

**Lane summary:** 5 games, 6 total event-on-court rows, **zero** minute issues of any kind, tiny PM residue only. These games have no lineup bug — they are **event-ordering convention differences** at substitution boundaries. The pipeline's convention (process sub before same-clock event) is internally consistent and was validated as closer to correct by the three failed same-clock overlay approaches.

---

### LANE 2: Rebound-Credit Survivors (5 games, 5 event-on-court rows)

These are games where a player is credited with a rebound while tracked as off-court, due to the pipeline processing a same-clock substitution before the rebound event. Every local correction attempt failed. All 5 are in Block E (2017-2020).

#### `0021900201` — LAC @ OKC, Period 3 (2020 season)
- **1 event-on-court row:** P3 E398 — Nerlens Noel (player 203457, OKC) — `off_court_event_credit`
- **Minutes:** max_abs_diff = 0.0, sum_abs_diff_over_0.1 = 0.0
- **PM reference delta:** 2 rows
- **What's happening:** Raw `pbpv3`/`playbyplayv2` show: P3 E392 Adams foul at 7:45 → P3 E395 SUB: Noel FOR Adams → P3 E397 missed FT → P3 E398 Noel REBOUND. The sub processes before the rebound, so Noel is credited while technically subbed out by the pipeline. `full_pbp_new` also flips OKC from Adams to Noel at this boundary.
- **Scratch attempt:** Event-only correction to put Noel on court at E398 was tested. It cleared E398 but the blocker moved to P3 E395 (the sub event itself), and introduced 2 small minute mismatches (Noel -0.05, Adams +0.05) and 2 PM mismatches. **Rejected.**

#### `0021900419` — POR @ LAC, Period 2 (2020 season)
- **1 event-on-court row:** P2 E258 — Maurice Harkless (player 203090, LAC) — `off_court_event_credit`
- **Minutes:** max_abs_diff = 0.00167
- **PM reference delta:** 2 rows
- **Scratch attempt:** Event-only correction cleared E258 but moved blocker to P2 E255, introduced 2 minute + 2 PM mismatches. **Rejected.**

#### `0021900487` — POR @ MEM, Period 2 (2020 season)
- **1 event-on-court row:** P2 E246 — Jaren Jackson Jr. (player 1628991, MEM) — `off_court_event_credit`
- **Minutes:** max_abs_diff = 0.00333
- **PM reference delta:** 4 rows
- **Scratch attempt:** Event-only correction cleared E246 but left P2 E239 live and introduced 6 small minute mismatches. **Rejected.**

#### `0021900920` — MEM @ NOP, Period 2 (2020 season)
- **1 event-on-court row:** P2 E312 — Anthony Tolliver (player 201229, MEM) — `off_court_event_credit`
- **Minutes:** max_abs_diff = 0.00667
- **PM reference delta:** 4 rows
- **Scratch attempts:** Event-only attempt previously rejected. Widened window P2 E307-E312 was a true no-op — exact same blocker survived unchanged. **Both rejected.**

#### `0041900155` — DAL @ LAC, Period 2 (2020 playoffs)
- **1 event-on-court row:** P2 E353 — Montrezl Harrell (player 1626149, LAC) — `off_court_event_credit`
- **Minutes:** max_abs_diff = 0.0
- **PM reference delta:** 2 rows
- **Scratch attempts:** Event-only traded E353 for P2 E348 with 2 Harrell/Zubac minute mismatches. Widened window P2 E348-E353 still left E348 live. **Both rejected.**

**Lane summary:** 5 games, 5 total event-on-court rows, **zero material minute issues (>0.1 min)**. Three of the five games carry only tiny non-material tails (`0.00167`, `0.00333`, `0.00667`). Every correction attempt (5 tested, 3 with wider windows) failed — each shifted the blocker to the substitution event itself or introduced minute mismatches. The pattern is identical to the same-clock control lane, just manifested at rebound events rather than scoring events: **the rebound and the substitution are at the same clock time, and whichever event the pipeline processes first, the other becomes an off-court credit.**

---

### LANE 3: Period-Start Contradiction Cases (2 games, 3 event-on-court rows)

These are games where multiple authoritative sources genuinely disagree about who started a period. The disagreement is not a clear upstream error — both versions are plausible.

#### `0020900189` — DEN @ MIN, Period 2 (2010 season, Block C)
- **1 event-on-court row:** P2 E217 — Chauncey Billups (player 1497, DEN) — `sub_out_player_missing_from_previous_lineup`
- **Minutes:** max_abs_diff = 0.00167 (essentially zero)
- **PM reference delta:** 2 rows
- **Source contradiction in detail:**
  - `period_starters_v6` (gamerotation boundary): Denver P2 starts with Ty Lawson (201951) already active, Billups off
  - `full_pbp_new` (tpdev possession-level): first Q2 possession at clock 720 still has Billups on the Denver lineup; Lawson doesn't appear until clock 702
  - Cached `pbpv3` from `nba_raw.db` shows a 12:00 cluster: `Start of 2nd Period` → technical foul → `SUB: Lawson FOR Billups` → technical FT — all at 12:00
  - `period_starters_v6` says the sub happened before the technical (Lawson starts); the PBP says the sub happened after (Billups starts, then gets subbed). Both are valid readings of the ambiguous 12:00 cluster.
- **Why not source-limited:** The sub event EXISTS in the source data. This is a boundary ordering dispute, not a missing or impossible event.
- **Why not a manual override:** No single source is more trustworthy for this specific cluster. The pipeline picked one valid interpretation; another valid interpretation exists.

#### `0021300593` — MIA @ CHA, Period 2 (2014 season, Block D)
- **2 event-on-court rows:**
  - P2 E96 — Norris Cole (player 202708, MIA) — `off_court_event_credit`
  - P2 E99 — Norris Cole (player 202708, MIA) — `sub_out_player_missing_from_previous_lineup`
- **Minutes:** max_abs_diff = 0.0 (zero)
- **PM reference delta:** 2 rows
- **Source contradiction in detail:**
  - `period_starters_v6`: Miami P2 starts with Roger Mason Jr. (2427) active, no Norris Cole
  - `full_pbp_new`: first Q2 boundary row at clock 720, event_id 42, still has Cole on the Miami lineup. The next row at the same clock, event_id 43, flips to Mason.
  - Cached `pbpv3`: `Cole S.FOUL` → `SUB: Mason Jr. FOR Cole` — both at 12:00, with FTs split around them
  - The question is whether Cole's foul and FT happen before or after he's subbed out. Cole is credited with the foul, then immediately subbed out, then credited with free throws. Whether he's "on court" for the FTs depends entirely on sub timing.
- **Why not source-limited:** The sub event exists. The foul event exists. The FTs exist. The ordering of {foul, sub, FTs} at 12:00 is genuinely ambiguous.

**Lane summary:** 2 games, 3 total event-on-court rows, **zero** minute issues. Both games are near-perfectly clean aside from these boundary rows. These are genuine same-clock ordering ambiguities at period starts, not upstream errors or pipeline bugs.

---

### LANE 4: Remaining Minute-Impact Documented Holds (2 games)

These are the two remaining reviewed minute-impact documented holds outside Block A. They do **not** share the same archetype: `0020400335` is a severe-minute insufficient-context holdout, while `0020000628` is a mixed-source contradiction case with a small minute tail.

#### `0020400335` — NOH @ HOU, Period 2 (2005 season, Block B)
- **5 event-on-court rows:** all P2, all Al Harrington (player 2454, team 1610612740/NOH) — `off_court_event_credit` at events E162, E181, E196, E223, E226
- **Minutes:** **1.2167 max abs diff (SEVERE)**, 1.2167 sum abs diff over 0.1
- **PM reference delta:** 0 rows
- **What's happening:** Harrington is tracked as off-court for ~1.2 minutes of P2 play. The pipeline doesn't have him on the floor, but he's credited with 5 events during that stretch. BBR confirms he was playing.
- **Cross-source minutes (total game, Harrington):**
  - Output / BBR: ~24.95 minutes
  - Official / tpdev_box: ~26.17 minutes
  - pbpstats_box: ~31.73 minutes
  - tpdev_pbp: ~32.70 minutes
  - **No two independent sources agree.** The spread is ~8 minutes.
- **Why no override:** The candidate engine returns `insufficient_local_context` for P2 deadball-window candidates at 5:30 and 4:34. The first contradiction event (E162) predates both candidate windows.
- **Why not source-limited:** The events are real. Harrington IS credited with plays and BBR confirms. The pipeline's lineup is wrong, but we cannot determine the correct lineup with confidence.

#### `0020000628` — NJN @ TOR, Period 2 (2001 season, Block B)
- **1 event-on-court row:** P2 E227 — Keith Van Horn (player 1496, NJN) — `off_court_event_credit`
- **Minutes:** **0.25 max abs diff**, 0.25 sum abs diff over 0.1
- **PM reference delta:** 0 rows
- **What's happening:** Raw PBP has Van Horn's Q2 2:23 shooting foul (E227) before the same-clock `SUB: Van Horn FOR Williams` (E229). The pipeline processes the sub first, removing Van Horn before his foul. `full_pbp_new` already has Van Horn on the Nets lineup for the 2:23 possession.
- **Scratch attempt:** Tested P2 E227-E230 window. Counting stats stayed clean, but the correction only moved the blocker to E229 (the sub event), the 0.25-minute tail stayed unchanged, and 2 PM mismatches appeared. **Rejected.**
- **Cross-source minutes (total game, Van Horn):**
  - Output / pbpstats_box: 32.73 minutes
  - Official / tpdev_box / BBR: 32.98 minutes
  - tpdev_pbp: 31.92 minutes
- **Note:** The 0.25-minute tail is below the severe threshold (>0.5 min) but above the material threshold (>0.1 min). This is arguably the same same-clock convention difference as the control lane, but with a small minute tail attached.

---

### LANE 5: Block A Documented Holdouts (2 games)

#### `0029700159` — DEN @ VAN, Period 3 (1998 season, Block A)
- **0 actionable event-on-court rows** remaining (P3 E349 was annotated as source-limited)
- **Minutes:** **1.85 max abs diff (SEVERE)**, 2.8683 sum abs diff over 0.1
  - Bryant Stith: 1.85 minutes diff
  - Priest Lauderdale: 0.72 minutes diff
  - Dean Garrett: 0.30 minutes diff
- **PM reference delta:** 3 rows
- **2 active corrections:** Denver P3 lineup window overrides for the Lauderdale/Goldwire stint
- **1 source-limited annotation:** P3 E349 broken Garrett sub-out row
- **Status:** The existing P3 window overrides are the least-bad validated state. The remaining minute tradeoff (Stith/Lauderdale/Garrett) cannot be improved without making other metrics worse. The game has been investigated from multiple angles:
  - The opening-cluster fix was confirmed NOT to fire on this game (no v6 rows, no opening-cluster-shaped events)
  - The apparent reproducibility issue (1.85 vs 11.87 max minute diff) was traced to override-lineage drift — the saved Block A residual used an older override payload. With the provenance-hardened runner, both states reproduce consistently.
  - The paired comparison of alternative override states has now been completed. The archived candidate payload was recovered exactly, but mismatch/outlier/PM counts stayed flat and the raw issue-row count worsened from `1 -> 2`, so the live state stayed unchanged.
  - The paired comparison is best read as a **raw rerun envelope comparison**. In the reviewed blocker inventory, the lone raw issue row (`P3 E349`) is already source-limited and therefore contributes `0` actionable blocker rows.

#### `0029701075` — NYK @ BOS, Period 3 (1998 season, Block A)
- **13 actionable event-on-court rows:** all P3, all `off_court_event_credit`
  - Chris Childs (164, NYK): 5 rows at events E345, E346, E351, E399, E445
  - Terry Cummings (187, NYK): 6 rows at events E342, E345, E351, E445, E448, E474
  - Andrew DeClercq (692, BOS): 1 row at E339
  - Tyus Edney (721, BOS): 1 row at E473
- **Minutes:** **1.0333 max abs diff (SEVERE)**, 2.4817 sum abs diff over 0.1
  - Chris Childs: 0.95 minutes
  - Terry Cummings: 1.03 minutes
- **PM reference delta:** 0 rows
- **Root cause:** NBA PBP has 2 time-jump discontinuities in P3. Childs and Cummings enter around 4.2 minutes remaining with no substitution event in the source data — they just start appearing in plays. The pipeline never gets them on court. BBR confirms all 4 players were playing. tpdev confirms 3 of 4 (Edney resolved only by BBR).
- **Investigation history:**
  - BOS P3 starter correction tested: proposed lineup already matched current inferred starters → no-op
  - NYK window probe: contradictory chronology — each attempted correction trades one problem for another
  - Classification: `candidate_systematic_defect` — the game has a systematic source-data issue (scrambled P3 with missing substitutions) that cannot be resolved by any single local override

---

### LANE 6: Same-Clock Accumulator Holdout (1 game)

#### `0021700394` — OKC @ CHA, full game (2018 season, Block E)
- **0 actionable event-on-court rows**
- **Minutes:** **0.1533 max abs diff**, 1.4933 sum abs diff over 0.1 — distributed across 10 players, each showing exactly +9 seconds
- **PM reference delta:** 0 rows
- **What makes this unique:** This game has NO event-on-court issues and NO PM issues. Its only blocker is a distributed minute residue of exactly +9 seconds across 10 players.
- **Cluster-ledger investigation:** A targeted trace artifact identified 7 same-clock substitution-scoring clusters across P1-P3:
  - P1 1:44, P2 9:50, P2 5:38, P2 0:57.20, P3 3:48, P3 3:39, P3 3:27
  - All 7 clusters touch at least one affected player
  - **2 affected players (Russell Westbrook, Michael Kidd-Gilchrist) have no direct stint boundary on any target cluster clock** — the +9s drift is a broader accumulator effect, not attributable to any single local window
- **Bounded four-source confirmation pass:** No clearly one-sided cluster/source disagreement was isolated. In `6/7` informative clusters, `full_pbp_new` was the coarsest source and held the scoring/FT sequence in the pre-sub possession row at the shared clock before flipping on the next row. That sharpens the diagnosis but does not make the game source-limited or locally fixable.
- **Why not a local override:** No single event range or period fixes the distributed drift
- **Why not source-limited:** Pipeline minutes don't match official/BBR/pbpstats_box — sources agree the pipeline is wrong by ~9 seconds. It's a real pipeline artifact, not an upstream error.
- **What would fix it:** A parser-level change to how same-clock scoring/sub clusters accumulate time — but the project has explicitly frozen broad fork work.

---

## 12. Why Plus-Minus Is Hard (Context for Policy Decisions)

**Plus-minus is calculated as:** For each stint a player is on court, sum (team points scored - opponent points scored). The pipeline derives this from its lineup tracking.

**The official plus-minus** comes from a completely separate system — the arena's scorekeeping system, which has its own lineup tracking. Small differences in when substitutions are recorded can cause point-scoring events to be attributed to different lineup configurations.

**Example:** If a sub happens at 4:30 and a basket happens at 4:30:
- If the pipeline processes the sub first: the incoming player gets the +/- credit
- If the arena system processes the basket first: the outgoing player gets the +/- credit
- Both are "correct" — they just disagree on ordering of simultaneous events

This is why three different scoring-attribution overlay approaches all made things worse — trying to match the arena's ordering introduces more errors than it fixes.

The ~1,744 `candidate_boundary_difference` PM rows are NOT bugs in the pipeline. They are inherent measurement differences. A stratified sample of 22 rows across all eras and game types found no hidden recurring bug class.

---

## 13. Design Constraints (Must-Not-Violate)

1. External sources never become runtime dependencies
2. Never overwrite Minutes/Plus_Minus with external values — fix lineup tracking instead
3. No global event reordering — local repairs only
4. No same-clock scoring-attribution fork fix — three approaches all failed
5. Unit tests necessary but not sufficient — real-canary validation required
6. Don't overwrite existing output parquets
7. All overrides must have provenance documentation
8. Counting stats must stay at zero mismatches
9. Source-limited rows stay visible and annotated, never hidden
10. Reclassification is a policy decision, not a technical fix — reclassified games remain visible in raw counts

---

## 14. The Policy Questions

The reviewed shortlist is **10 `policy_frontier_non_local` games plus 7 `documented_hold` games**. There are **0** remaining bespoke investigation cases. Under the current strict done definition ("no open actionable event-on-court rows" and "no unresolved high-confidence outlier > 0.5 min"), the blocker is now **policy choice**, not an unfinished local-fix queue.

### Question 1: Should same-clock control/guardrail games be reclassified from "open blocker" to a non-blocking status?

**Games:** `0021700337`, `0021700377`, `0021700514`, `0021801067`, `0021900333`
**Impact:** 5 games, 6 event-on-court rows removed from blocker count

**The case for reclassification:**
- These games have **zero minute issues** — the pipeline's minute output is correct
- The event-on-court rows are event-ordering convention differences, not lineup bugs
- They are explicitly designated as negative tripwires/controls — fixing them would undermine their purpose
- They would stay in monitoring infrastructure regardless of blocker status
- The pipeline's convention (process sub before same-clock event) is internally consistent
- Three failed same-clock overlay approaches confirmed the pipeline's ordering is closer to correct

**The case against:**
- The done definition says "no open actionable event-on-court rows" without exception for convention differences
- Reclassifying sets a precedent that event-on-court rows can be acceptable
- If a future parser change causes these to flip, the reclassification might mask a real regression

**Possible classification:** `accepted_convention_difference` or `accepted_boundary_difference`

---

### Question 2: Should rebound-credit survivors be reclassified?

**Games:** `0021900201`, `0021900419`, `0021900487`, `0021900920`, `0041900155`
**Impact:** 5 games, 5 event-on-court rows removed from blocker count

**The case for reclassification:**
- All 5 are the exact same pattern: rebound credited while rebounder was subbed out at same clock
- Every correction attempt (5 tested, 3 with wider windows) failed the acceptance criteria in the same way
- **Zero material minute issues (>0.1 min)** — only tiny non-material tails remain in 3 of the 5 games
- The underlying mechanism is identical to the control lane (same-clock convention difference)

**The case against:**
- Unlike the controls, these aren't explicitly designated as canaries
- The done definition doesn't distinguish between "unfixable convention difference" and "not yet fixed"
- A future parser-level same-clock ordering change could potentially fix these

**Possible classification:** Same as Question 1 — `accepted_convention_difference`

---

### Question 3: Should period-start contradiction cases be reclassified?

**Games:** `0020900189`, `0021300593`
**Impact:** 2 games, 3 event-on-court rows removed from blocker count

**The case for reclassification:**
- Multiple authoritative sources genuinely disagree about the period-start lineup
- Neither the pipeline's answer nor the alternative is clearly wrong
- Zero minute issues
- No manual override or source-limited annotation fits because both interpretations are valid
- Raw-source traces were done for both games (direct reads of v6, full_pbp_new, pbpv3) — the contradiction is confirmed, not speculative

**The case against:**
- These are legitimate cases where the pipeline might have the wrong starter for a period
- If better source data becomes available (e.g., official gamerotation corrections), these could be resolved
- Reclassifying says "we accept that our pipeline might have the wrong starter for these periods"

**Possible classification:** `accepted_unresolvable_contradiction` or leave as `open` but exempt from done definition

---

### Question 4: Should `0020400335` (severe minute holdout) remain a release blocker?

**Game:** `0020400335` (NOH @ HOU, 2005)
**Impact:** 1 game, 5 event-on-court rows, 1 severe minute row removed from blocker count

**The case for reclassifying:**
- The candidate engine returns `insufficient_local_context` — there is no known fix
- Cross-source minutes are badly split across 4 sources with ~8 minute spread — no consensus
- No correction has been found despite thorough investigation
- It's a single game from 2005

**The case against:**
- This is a **real 1.2-minute error** — Harrington is off-court for a meaningful stretch
- The severe minute threshold (>0.5 min) exists specifically for cases like this
- Accepting this sets a precedent for accepting severe minute errors if they're "too hard to fix"
- BBR confirms Harrington was playing during the issue stretch

**Possible classification:** `candidate_systematic_defect` (the source data is insufficient for repair) or leave as the single hardest remaining blocker

---

### Question 5: Should `0020000628` (mixed-source case) remain a release blocker?

**Game:** `0020000628` (NJN @ TOR, 2001)
**Impact:** 1 game, 1 event-on-court row removed from blocker count

**The case for reclassifying:**
- The 0.25-minute tail is below the severe threshold (>0.5 min) but above material (>0.1 min)
- The scratch correction was rejected (trades one blocker for another)
- The reviewed shortlist keeps it as a **mixed-source comparison case**, not as a standard same-clock control lane
- All evidence-backed local work is exhausted without producing a safer alternative state

**The case against:**
- The event-on-court row is real — Van Horn is credited with a foul while off-court
- The minute tail means this has a small but real minutes consequence, unlike the zero-minute control/rebound lanes
- It wasn't classified as a same-clock control because it has the minute tail and does not collapse to one clean source-limited interpretation

---

### Question 6: Should `0029701075` (Block A systematic defect) remain a release blocker?

This is a **singleton carveout beyond the reviewed 4-option frontier packages** below.

**Game:** `0029701075` (NYK @ BOS, 1998)
**Impact:** 1 game, 13 event-on-court rows, 2 severe minute rows removed from blocker count

**The case for reclassifying:**
- The NBA source data for this game is fundamentally broken (scrambled P3 with missing subs)
- Both starter and window correction attempts produced contradictory results
- 13 event-on-court rows and severe minute drift are all concentrated in one broken period (P3)
- BBR and tpdev confirm the correct lineups, but the NBA PBP cannot be reconciled
- Similar pattern to `0029800661`, which was reclassified as `source_limited_upstream_error` at the game level

**The case against:**
- This is the single worst remaining game (13 event-on-court rows, most of any game in the pipeline)
- It has real severe minute drift affecting named players (Childs 0.95 min, Cummings 1.03 min)
- `candidate_systematic_defect` means a future systematic fix could theoretically help
- Reclassifying the worst game feels like giving up

**Possible classification:** keep as `candidate_systematic_defect` but exempt it from blocker status, or more aggressively promote it to a whole-game documented carveout analogous to `0029800661`

---

### Question 7: Should `0021700394` (accumulator holdout) remain a release blocker?

This is a **singleton carveout beyond the reviewed 4-option frontier packages** below.

**Game:** `0021700394` (OKC @ CHA, 2018)
**Impact:** 1 game, 0 event-on-court rows, 10 material minute rows removed from blocker count

**The case for reclassifying:**
- It has **zero** event-on-court rows — the pipeline tracks who's on court correctly
- The max minute diff (0.15 min = 9 seconds) is below the severe threshold
- The drift is a distributed accumulator effect across 7 same-clock clusters, confirmed by cluster-ledger artifact — not attributable to any identifiable lineup error
- It would require a parser-level change to fix (frozen per project policy)

**The case against:**
- 10 material minute rows is a lot for one game
- The pipeline IS wrong by 9 seconds per player — official/BBR/pbpstats all agree
- Accepting this normalizes a known accumulator defect
- 1.49 total minutes of absolute minute drift across the game is non-trivial

**Possible classification:** a non-blocking documented accumulator-hold status, or `candidate_systematic_defect` with exemption from the strict done definition

---

## 15. Summary of Policy Options

These are the **reviewed 4-option frontier packages** from `phase6_blocker_policy_frontier_20260322_v1.md`. They stop before singleton carveouts on `0029701075` and `0021700394`, which are covered separately in Questions 6 and 7.

| Option | Reclassified Games | Remaining Blockers | Risk |
|---|---:|---:|---|
| 1. Status quo | 0 | 17 | No policy risk, but the done-definition stays far away and the queue remains dominated by already-reviewed non-local lanes. |
| 2. Reclassify same-clock controls + rebound survivors | 10 | 7 | Low; these are already documented as non-override lanes with no material minute issue. |
| 3. Also reclassify contradiction cases | 12 | 5 | Medium; two genuine boundary contradictions stop counting as blockers even though they remain unresolved. |
| 4. Also reclassify `0020000628` and `0020400335` | 14 | 3 | Higher; one severe-minute holdout and one mixed-source comparison case stop blocking finalization. |

After Option 4, the remaining blockers would be:
- `0021700394`
- `0029700159`
- `0029701075`

---

## 16. What We're Asking You

For each of the 7 questions above, please recommend:

1. **Your recommendation** (reclassify or keep as blocker)
2. **What classification to use** if reclassifying (e.g., `accepted_convention_difference`, `source_limited_upstream_error`, `accepted_unresolvable_contradiction`)
3. **Any conditions or caveats** on the reclassification
4. **Whether the done definition itself should be revised** to accommodate the reality that some event-on-court rows are irreducible convention differences rather than bugs

Please answer **Questions 1-7 individually** in that format rather than only choosing among the packaged options above.

We are also open to a recommendation that the **done definition should be split into tiers** — e.g.:
- **Tier 1:** No counting-stat mismatches, no severe minute issues from fixable defects, no fixable event-on-court rows
- **Tier 2:** All convention differences and contradictions also resolved (requires new tooling or parser changes)

If you think the current binary done/not-done framing is wrong for a project with fundamentally ambiguous source data, tell us. The project produces data for a basketball analytics system (DARKO), so the practical question is: **is this data reliable enough to ship for downstream consumption, with documented quality metadata per game?**

One final meta question: **Should the done definition itself become tiered, or should it remain binary but with explicit non-blocking carveouts for reviewed frontier lanes?**

---

## 17. File Map

```
replace_tpdev/
├── handoff_for_external_llm_20260322.md        # Main reviewed-policy handoff
├── handoff_appendix_game_evidence_20260322.md  # Game evidence + verbatim runtime code appendix
├── cautious_rerun.py              # Season-level runner (provenance-hardened)
├── rerun_selected_games.py        # Targeted game runner (provenance-hardened)
├── run_golden_canary_suite.py     # 27-case regression gate
├── build_lineup_residual_outputs.py  # Per-game quality classification
├── build_plus_minus_reference_report.py  # PM characterization
├── build_override_runtime_views.py  # Manifest → runtime compiler
├── 0c2_build_tpdev_box_stats_version_v9b.py  # Notebook dump
├── audit_event_player_on_court.py    # Event-on-court audit
├── audit_minutes_plus_minus.py       # Minute/PM audit
├── overrides/
│   ├── correction_manifest.json           # 54 active, ~25 rejected, ~25 annotations
│   ├── correction_manifest_compile_summary.json
│   ├── period_starters_overrides.json     # 48 entries
│   ├── lineup_window_overrides.json       # 6 entries
│   └── *_notes.csv                        # Provenance sidecars
├── golden_canary_manifest_20260321_v1.json  # 27 cases, 5 categories
├── phase6_open_blocker_inventory_20260322_v1.csv  # 17 open games
├── phase6_true_blocker_shortlist_20260322_v1.csv
├── phase6_blocker_policy_frontier_20260322_v1.md
├── phase5_pm_review_lane_map_20260322_v3.csv
├── phase5_pm_deliverable_ABCDE_20260322_v6/  # Published PM characterization
├── golden_canary_suite_20260322_v3/          # Latest clean suite run
├── block_A_live_postqueue_20260322_v1_residual/  # Block A residuals
├── phase5_block_residuals_20260322_v*/       # Block B-E residuals (various versions)
├── playbyplayv2.parq, nba_raw.db, period_starters_v6/v5.parquet  # Runtime inputs
├── AGENTS.md                      # Current project brief
├── PROJECT_HISTORY.md             # Archived narrative
└── resume_notes.md                # Running checkpoint log

../pbpstats/                       # Custom fork (opening-cluster fix active)
../fixed_data/raw_input_data/tpdev_data/  # tpdev cross-reference
../33_wowy_rapm/bbref_boxscores.db        # BBR cross-reference
../calculated_data/pbpstats/              # pbpstats cross-reference
```
