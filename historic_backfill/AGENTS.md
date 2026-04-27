# replace_tpdev: current brief

This repo is an offline-first replacement pipeline for the old `tpdev` box-score build.

`AGENTS.md` is the compact working brief.
`PROJECT_HISTORY.md` is the archived project narrative and milestone log.

## Project goals

- Build reliable offline historical outputs without depending on live NBA endpoints.
- Preserve a reproducible `1997-2020` historical counting-stat pipeline.
- Match official boxscore counting stats when the official source is trustworthy.
- When official source data is wrong or incomplete, use explicit documented repair policy instead of silent drift.
- Keep the custom `pbpstats` fork focused on broad recurring repairs and feed semantics, while pushing one-off anomalies into local override layers.
- Improve lineup-derived fields (`Minutes`, `Plus_Minus`) without regressing the proven counting-stat path.

## Source of truth and runtime

- Notebook source of truth:
  - `0c2_build_tpdev_box_stats_version_v9b.ipynb`
- Raw dump only:
  - `0c2_build_tpdev_box_stats_version_v9b.py`
  - this file is still not import-safe because notebook cells remain at top level
- Safest runner:
  - `cautious_rerun.py`
  - it loads the safe prefix of the dump and runs the offline path
  - current default period-starter runtime source stack is:
    - `period_starters_v6.parquet`
    - then `period_starters_v5.parquet`
  - local `overrides/period_starters_overrides.json` still remains active through the fork-side override hook
  - `period_starters_v4.parquet` is still useful for comparison / migration history, but it is no longer the default runtime source
  - `period_starters_v3.parquet` is an abandoned intermediate artifact and should not be used as the default runtime source
- Custom parser dependency:
  - external `pbpstats` fork
  - expected to be imported from an editable repo checkout, not a copied site-packages snapshot
- Runtime environment names:
  - Windows: `darko311`
  - macOS: `DARKO`

## Canonical local inputs

The historical batch pipeline depends on these local files:

- `playbyplayv2.parq`
  - baseline historical event stream
  - built from shufinskiy `nba_data`
- `nba_raw.db`
  - local cache of `pbpv3`, `summary`, `boxscore`, and related responses
- `validation_overrides.csv`
  - manual tolerance exceptions for known source-data issues
- `boxscore_source_overrides.csv`
  - production patches for confirmed bad official boxscore rows or missing official players
- `boxscore_audit_overrides.csv`
  - audit-only exceptions for confirmed source anomalies where parser-side stats should be treated as canonical during audit comparison
- `manual_poss_fixes.json`
  - one-off possession repair overrides
- `pbp_row_overrides.csv`
  - production row-order repairs
- `pbp_stat_overrides.csv`
  - production stat-credit repairs
- `overrides/lineup_window_overrides.json`
  - explicit manual intraperiod lineup-window repairs
  - runtime consumes this file before any system-generated intraperiod repair
- `overrides/lineup_window_override_notes.csv`
  - human-readable provenance sidecar for `lineup_window_overrides.json`
  - one row per override window
  - runtime does not consume this file
- `overrides/correction_manifest.json`
  - canonical authoring surface for active lineup corrections and sparse manual residual annotations
  - v1 active runtime domain is `lineup` only
  - compile this manifest back into the runtime JSON/CSV override files before reruns
- `overrides/correction_manifest_compile_summary.json`
  - latest compiler output summary for the canonical correction manifest
  - useful for no-op migration proof and active-correction inventory checks
- `overrides/period_starters_overrides.json`
  - local lineup repair hook for bad inferred period starters
- `overrides/period_starters_override_notes.csv`
  - human-readable provenance sidecar for `period_starters_overrides.json`
  - one row per `game_id/period/team_id`
  - runtime does not consume this file
  - placeholder provenance is acceptable for older legacy rows until backfill work is done
- `period_starters_v6.parquet`
  - primary parquet-backed period-starter source for the offline runtime when a resolved row exists
  - built from `gamerotation` boundary activity
  - schema / comparison notes for `v5` vs `v6`: `PERIOD_STARTERS_V5_V6_SCHEMA_NOTES.md`
  - current fork behavior: when a local exact `v6` row exists and disagrees with strict PBP starters, the runtime now prefers the `v6` row
- `period_starters_v5.parquet`
  - secondary parquet-backed period-starter fallback source for the offline runtime
  - schema / comparison notes for `v5` vs `v6`: `PERIOD_STARTERS_V5_V6_SCHEMA_NOTES.md`
- `period_starters_v4.parquet`
  - superseded earlier fallback source
  - keep for comparison / migration history, not as the default runtime source
- `period_starters_v3.parquet`
  - deprecated earlier scrape artifact
  - keep only for historical comparison / research, not as the default runtime source
- `gamerotation_stints_v6.parquet`
  - full-game normalized `gamerotation` stint / rotation data from the `v6` scrape
  - useful for investigations beyond period starters, especially boundary-state and lineup-occupancy questions
  - do not treat it as definitive truth on its own; it is a strong research source, but it can still be incomplete or malformed for some games / periods
- `intraperiod_missing_sub_repair.py`
  - repo-side local-only inference engine for intraperiod missing-sub / wrong-clock repair
  - use this as the audit/suggester reference implementation when comparing against the fork
- `suggest_intraperiod_missing_subs.py`
  - repo-side suggester CLI for intraperiod repair candidates
  - may use `tpdev` / `pbpstats` / official / BBR only as external audit scoring inputs
- `intraperiod_canary_manifest_1998_2020.json`
  - canonical block and micro-canary manifest for the current intraperiod proving loop
- `build_intraperiod_canary_register.py`
  - builds a cross-block canary/candidate register from a proving-loop output tree
- `run_intraperiod_proving_loop.py`
  - orchestrates the `1998-2020` intraperiod proving ladder
  - runs micro canaries, block reruns, cross-source reports, block candidate harvests, and final canary register build
  - production runtime must not depend on those external sources
- `same_clock_canary_manifest_20260320_v1/same_clock_canary_manifest.json`
  - canonical manifest for all same-clock attribution families
  - includes positive canaries, negative tripwires, reviewed manual rejects
- `same_clock_canary_manifest_non_opening_ft_sub_20260320_v1.json`
  - detailed manifest for the non-opening FT sub family
  - includes the `0021700337 P3` main guardrail
- `build_same_clock_canary_manifest.py`
  - builds same-clock canary manifests from proving-loop artifacts
- `run_same_clock_canary_suite.py`
  - runs a same-clock canary suite against a manifest
- `build_override_runtime_views.py`
  - compiles `overrides/correction_manifest.json` into the runtime starter/window override artifacts
  - enforces roster/team validation and rejects non-lineup active domains in v1
- `seed_correction_manifest.py`
  - seeds the canonical correction manifest from the live runtime override JSON/CSV files
- `build_lineup_residual_outputs.py`
  - builds blocker-vs-raw residual outputs, sparse residual annotation views, and per-game quality flags from rerun outputs
- `run_golden_canary_suite.py`
  - dated lineup-integrity canary suite runner
  - supports positive canaries, fixed dirty games, anti-canaries, and per-case expected envelopes
- `golden_canary_manifest_20260321_v1.json`
  - dated canary manifest for the exception-management phase
  - positive canaries must stay clean; dirty/anti-canary cases must stay within their declared envelopes
- `summarize_opening_cluster_pass0.py`
  - summarizes the four-season opening-cluster Pass 0 gate against the locked baseline season summaries

Useful supporting local data:

- sibling `../fixed_data/raw_input_data/tpdev_data/full_pbp_new.parq`
  - possession-level `tpdev` lineup/pbp reference
  - for lineup-derived minute disputes, this is the relevant `tpdev` check before consulting `tpdev_box*`
  - not an event-order authority
- sibling `../fixed_data/raw_input_data/tpdev_data/tpdev_box.parq`
- sibling `../fixed_data/raw_input_data/tpdev_data/tpdev_box_new.parq`
- sibling `../fixed_data/raw_input_data/tpdev_data/tpdev_box_cdn.parq`
  - useful as boxscore/player-stat tiebreakers
  - do not treat these box files as the preferred `tpdev` minutes authority when they conflict with `full_pbp_new.parq`
- sibling `../33_wowy_rapm/bbref_boxscores.db`
  - Basketball Reference boxscore tiebreaker
- sibling `../calculated_data/pbpstats/pbpstats_player_box.parq`
  - local scraped `pbpstats` full-game player box output
  - useful as an additional full-game minutes / boxscore cross-check
  - verified local coverage currently starts at the `2000` season
- sibling `../calculated_data/pbpstats/pbpstats_team_box.parq`
  - local scraped `pbpstats` full-game team box output
- sibling `../calculated_data/pbpstats/pbpstats_player_box_raw.parq`
  - raw-form local `pbpstats` player box scrape artifact
- sibling `../calculated_data/pbpstats/pbpstats_team_box_raw.parq`
  - raw-form local `pbpstats` team box scrape artifact
  - these `pbpstats` files are full-game only and should not be treated as period-level starter truth by themselves

Useful starter-schema reference:

- `PERIOD_STARTERS_V5_V6_SCHEMA_NOTES.md`
  - explains how `v5` and `v6` differ conceptually
  - documents resolved / unresolved / failure schemas
  - explains what the runtime loader actually consumes
  - explains how to distinguish a real disagreement from a `v5` coverage gap or a `v6` unresolved boundary
  - includes notes on `gamerotation_stints_v6.parquet` as a broader investigation source

## Data reality

- `playbyplayv2` on `stats.nba.com` is not a dependable live historical source for this project anymore.
- Cached `pbpv3` in `nba_raw.db` is still useful, but it should be treated as enrichment / repair input, not canonical chronology.
- The CDN live feed is the current path for recent seasons and has been verified from `2019-20` onward.
- Historical runs are not reconstructible from live NBA endpoints alone.
- `playbyplayv2.parq` and `nba_raw.db` are both critical artifacts.

## Historical gotchas

- In `playbyplayv2.parq`, historical game ids are stored without the leading `00`.
- In `playbyplayv2.parq`, `EVENTMSGTYPE` is stored as strings.
- `nba_raw.db` stores payloads under short names like `pbpv3`, `summary`, and `boxscore`.
- When a historical game looks missing, check both the parquet and the DB before assuming it is absent.

## Current status

As of March 20, 2026:

### Historical counting-stat path

- Historical `1997-2020` counting stats are effectively complete and well proved.
- Frozen clean-season manifest:
  - `historical_baseline_manifest_20260315_v2/`
- Main proof artifacts:
  - `override_provenance_20260315_v3/`
  - `override_consensus_20260315_v3/`
  - `source_conflict_register_20260315_v3/`
  - `pbp_row_override_bbr_window_audit_20260315_v3/`
  - `fork_repair_catalog_20260315_v3/`
  - `fork_repair_disposition_20260315_v3/`
  - `historical_cross_source_summary_20260315_v3/`
- Locked full-history rerun:
  - `full_history_1997_2020_20260316_v1/`
  - consolidated parquet: `full_history_1997_2020_20260316_v1/darko_1997_2020.parquet`
  - rows: `685,882`
  - columns: `188`
- Full locked run status across `1997-2020`:
  - `0` failed games
  - `0` `event_stats` errors
  - `0` integrated audit mismatch games
  - `0` integrated audit mismatch player rows
- Important nuance:
  - that locked full-history run is not literally zero-fallback-deletion in every season
  - remaining deletions are audit-benign TEAM orphan rebound cleanups, not live boxscore mismatches
- Documented source conflicts are isolated rather than open-ended:
  - `18` production override rows across `9` games in `source_conflict_register_20260315_v3/`
- Latest fork disposition:
  - no remaining evidence-backed manual-override candidate rules
  - three ultra-narrow processor rules were removed safely on March 15, 2026

### Lineup-derived fields

- The March 16 locked full-history output is not the final perfect baseline for `Minutes` and `Plus_Minus`.
- Dedicated lineup audit tools now exist:
  - `audit_minutes_plus_minus.py`
  - `build_minutes_cross_source_report.py`
  - `audit_period_starters_against_tpdev.py`
  - `audit_event_player_on_court.py`
  - `build_large_minute_outlier_triage.py`
  - `build_large_minute_outlier_family_register.py`
  - `bbr_boxscore_loader.py`
- Focused rerun baseline for current lineup work:
  - `audit_minutes_fix_2017_2020_20260316_v3/`
- Full baseline triage for large historical minute outliers:
  - `large_minute_outlier_triage_baseline_20260316_v1/`
  - summary:
    - `454` rows over `0.5` minutes
    - `331` games
    - `193` candidate `game/period/team` rows
    - only `4` simple one-for-one later-sub-in rows in the baseline
- Current measured residue:
  - `2017`: `0` minute outliers over `0.5` minutes, `70` plus-minus mismatch rows
  - `2020`: `0` minute outliers over `0.5` minutes, `96` plus-minus mismatch rows
- Cross-source minute reports show no remaining large minute errors:
  - `minutes_cross_source_2017_20260316_v4/`
  - `minutes_cross_source_2020_20260316_v4/`
  - remaining minute drift is only `1-3` seconds
- For minute-lineup reconciliation, preferred external minute sources are now:
  - `full_pbp_new.parq` possession/PBP-derived lineup minutes first
  - `pbpstats_player_box.parq` next, when available
  - official box / `tpdev_box*` / Basketball Reference after that
- Verified local coverage notes:
  - `pbpstats_player_box.parq` begins in `2000`
  - `full_pbp_new.parq` is broad from `1997` forward, with sparse `1996` and partial `1998` coverage rather than one cleanly missing full season
- For lineup-minute triage, run `audit_event_player_on_court.py` early rather than relying only on end-of-game minute deltas.
- Treat "player credited with event while off court" as a high-signal clue for missing substitutions, bad boundary state, or source disagreement.
- A full large-outlier family register now exists for `>2` minute minute mismatches:
  - `large_minute_outlier_family_register_20260316_v2/`
  - summary:
    - `259` rows across `196` games
    - row families:
      - `starter_complex_candidate`: `124`
      - `v3_ordering_candidate`: `88`
      - `period_sized_residual`: `38`
      - `source_conflict_or_missing_source`: `9`
    - game-level primary families:
      - `starter_complex_candidate`: `107`
      - `v3_ordering_candidate`: `59`
      - `period_sized_residual`: `26`
      - `source_conflict_or_missing_source`: `4`
- Main open target:
  - plus-minus / same-clock scoring-substitution attribution
  - large historical minute outliers remain a separate secondary frontier, now split into starter-complex vs V3-ordering families instead of one undifferentiated bucket

### Current 5-block residual summary (March 20, 2026)

Full proving-loop baseline: `intraperiod_proving_1998_2020_20260319_v2_vs_baseline_20260320_v1/summary.json`

| Block | Seasons | Problem Games | Min Mismatches | Min Outliers | +/- Mismatches | Event-on-Court Rows |
|---|---|---|---|---|---|---|
| A | 1998-2000 | 84 | 59 | 7 | 212 | 50 |
| B | 2001-2005 | 168 | 30 | 1 | 455 | 13 |
| C | 2006-2010 | 130 | 4 | 0 | 389 | 4 |
| D | 2011-2016 | 116 | 0 | 0 | 308 | 13 |
| E | 2017-2020 | 169 | 12 | 0 | 513 | 30 |
| **Total** | | **667** | **105** | **8** | **1,877** | **110** |

- `0` counting-stat mismatches across all seasons
- `0` failed games
- dominant remaining problem: **1,877 plus-minus mismatch rows**, mostly same-clock scoring-substitution attribution
- minutes are basically solved outside Block A (early-era data quality)

### Intraperiod missing-sub infrastructure (March 19-20, 2026)

- Built local-only inference engine: `intraperiod_missing_sub_repair.py` (repo), `intraperiod_lineup_repair.py` (fork)
- Built suggester: `suggest_intraperiod_missing_subs.py`
- Tightened scoring rules through v4; current scorer is conservative and safe
- Proved that the old `0029800075` manual lineup-window overrides could be removed: the heuristic subsumes them
- That March 19 empty-file state is no longer current. New live manual lineup-window overrides were later added for `0029700159`, `0029700367`, and `0049700045`.
- **Net result**: framework works correctly but produces `0` new uncovered auto-apply candidates in 1998-2000
- This infrastructure is stable; do not re-widen the auto-apply rules unless new evidence surfaces

### Opening-cluster carryover fix (March 20, 2026)

- **Status: landed in fork, validated on real canaries, accepted**
- Added `_should_prefer_strict_starters_over_exact_v6()` gate in:
  - `pbpstats/resources/enhanced_pbp/start_of_period.py`
  - `pbpstats/resources/enhanced_pbp/stats_nba/start_of_period.py`
- Behavior: period-start substitutions are now delayed for carryover purposes when the outgoing player is explicitly credited inside an exact-start technical/flagrant/ejection cluster; strict PBP can beat exact v6 only for that narrow opening-cluster shape
- `29` tests pass in `tests/test_period_starters_carryover.py`
- Validated on `4` real canaries with per-game reruns — all `4` resolved to zero plus-minus diff for both targeted players:
  - `0021200444` P4: Shannon Brown / Markieff Morris → both `plus_minus_diff = 0`
  - `0021300594` P3: David West / Luis Scola → both `plus_minus_diff = 0`
  - `0021400336` P2: LeBron James / Kyrie Irving → both `plus_minus_diff = 0`
  - `0021800748` P3: Lou Williams / Shai Gilgeous-Alexander → both `plus_minus_diff = 0`
- All `4` games: `build_ok = true`, `team_totals_ok = true`, `event_stats_errors = 0`
- Artifacts: `opening_cluster_postpatch_validation_20260320_v1/`
- **Still needs full season-level non-regression pass** on `2013`, `2014`, `2015`, `2019` (the per-game validation shows improvement, but doesn't prove no regression across the other ~5,000 games in those seasons)

### Failed same-clock fork fixes (March 20, 2026) — THREE approaches tried, all rejected

**Approach 1: Lineup propagation carryover** (non-opening FT/sub)
- Tried in `enhanced_pbp_item.py` via `_get_previous_raw_players()`
- `34` unit tests passed, but real canaries failed catastrophically:
  - `0021700236` P1 blew up by ~4 minutes (lineup poisoning past cluster end)
  - `0021700337` P3 negative tripwire regressed
- Rule was correctly reverted; `test_same_clock_ft_carryover.py` was deleted
- Artifacts: `same_clock_ft_carryover_validation_20260320_v1/`
- **Lesson**: modifying lineup propagation does not work for non-opening same-clock cases

**Approach 2: Event-local scoring overlay, foul-committer anchor**
- Tried as a read-only `_get_effective_scoring_current_players()` helper on `enhanced_pbp_item.py`
- Used by `field_goal.py` and `free_throw.py` for scoring attribution only
- Did NOT mutate lineup chain for later events (addressed Approach 1's failure mode)
- Unit tests passed
- **Every single real canary got worse** — including all 4 positives:
  - `0021700236` (positive): +9 pm mismatches
  - `0021700917` (positive): +4 pm mismatches
  - `0021900333` (positive): +7 pm mismatches
  - `0021700337` (negative tripwire): +12 pm mismatches
  - `0029900517` (positive): +14 pm mismatches
- Artifacts: `same_clock_scoring_overlay_validation_20260320_v1/`

**Approach 3: Event-local scoring overlay, fouled-player anchor**
- Same architecture as Approach 2, but used `player3_id` (fouled player) instead of `player1_id` (foul committer) for FT attribution anchor
- **Results were even worse than Approach 2**:
  - `0021700236` (positive): +12 pm mismatches
  - `0021700917` (positive): +4 pm mismatches
  - `0021900333` (positive): +13 pm mismatches
  - `0021700337` (negative tripwire): +13 pm mismatches
  - `0029900517` (positive): +15 pm mismatches
- Artifacts: `same_clock_scoring_overlay_validation_20260320_v2/`

**All three approaches were correctly reverted. The fork is clean — only the opening-cluster fix remains.**

**Critical finding**: the fundamental assumption behind all three approaches was wrong. "Using the pre-sub lineup for scoring events at the same clock as a substitution" does not make plus-minus more accurate — it makes it **less** accurate. The current pipeline behavior (sub is already live when the scoring event is processed) is closer to reality than any overlay.

**Do not attempt another same-clock scoring overlay.** The plus-minus residual is not caused by wrong lineup attribution at the scoring layer.

### Runner infrastructure improvements (March 20, 2026)

- `cautious_rerun.py`: broader explicit preload set for notebook-prefix local modules (fixes `player_id_normalization` import hang)
- `period_boxscore_source_loader.py`: column pruning always on, `allowed_game_ids` pushed into parquet read
- `rerun_selected_games.py`: CSV hydration workaround for cloud-backed override files

### Current active lineup project and pause state (March 21, 2026)

- **Current project**: upstream-first lineup repair, with `event-on-court` issues as the top-of-funnel signal.
- The active fork baseline is now **opening-cluster only**. The narrow non-opening same-clock pending-sub fix for foul / FT boundaries was reverted on March 21, 2026 after it regressed Block A at season scale.
- Keep the pending-sub validation artifacts as decision history and anti-canaries only:
  - `same_clock_scoring_overlay_validation_20260320_v3/`
  - `same_clock_boundary_frontier_validation_pending_sub_narrow_candidate_20260321_v2/`
  - `block_A_pending_sub_narrow_candidate_20260321_v3/`
  - `block_E_pending_sub_narrow_candidate_20260321_v3/`
- The only fork-side lineup rule still eligible for promotion is the accepted opening-cluster carryover fix, and it still needs the season-level non-regression pass on `2013`, `2014`, `2015`, and `2019`.
- Current cleaned same-clock teaching queue:
  - foul / FT boundary family: `0021700917 P1`, `0021700236 P1`, `0021700514 P2`
  - cluster boundary family: `0029800063 P2`, `0029800063 P4`, `0020400526 P3`
  - keep opening-cluster cases as controls only, not teaching cases
- Current cleaned active frontier artifact set:
  - `same_clock_boundary_queue_20260320_v2/`
  - `same_clock_boundary_frontier_summary_20260320_v1/`
  - `same_clock_boundary_casebook_20260320_v1/`
  - `same_clock_boundary_frontier_validation_20260320_v2/selected_lanes.csv`
- Current rerun/runtime hardening already landed in `cautious_rerun.py` and `rerun_selected_games.py`:
  - hydrated notebook dump and preload-module paths are now passed through `load_v9b_namespace(...)`
  - `period_boxscore_source_loader` import is now lazy inside `install_local_boxscore_wrapper(...)`
  - cached-copy reuse now exists for:
    - `nba_raw.db`
    - `playbyplayv2.parq`
    - `0c2_build_tpdev_box_stats_version_v9b.py`
    - `boxscore_source_overrides.csv`
    - `period_starters_v6.parquet`
    - `period_starters_v5.parquet`
  - `pbp_row_overrides` can now preload from `.pyc` when source-file reads are unreliable
- **Pause reason**: OneDrive-backed runtime files are still causing intermittent startup stalls during real reruns / validators.
- Confirmed trouble files / behaviors:
  - `boxscore_source_overrides.csv` unreadable / slow from the synced path
  - `pbp_row_overrides.py` source-file reads can stall from the synced path
  - `period_starters_v6.parquet` and `period_starters_v5.parquet` can stall on schema / read at startup
  - eager warm-copying of `nba_raw.db` and `playbyplayv2.parq` is expensive enough to mask real progress when the synced path is unhealthy
- **Immediate resume plan after moving inputs to a non-synced drive**:
  1. repoint the runner to the non-synced copies of the core runtime files
  2. rerun the 3-game serial core canary batch:
     - `0021700236`
     - `0021700917`
     - `0021900333`
  3. rerun `validate_same_clock_boundary_frontier.py`
  4. only if that passes, widen to block / season validation
- **Important migration correction (March 20, 2026, late)**:
  - do **not** use a clean `repos/... + data/...` layout if it changes the runtime-relative paths
  - the current runtime assumes:
    - core inputs live in the `replace_tpdev` repo root
    - supporting reference data lives at sibling paths like `../fixed_data/...`, `../33_wowy_rapm/...`, and `../calculated_data/pbpstats/...`
  - a safe off-OneDrive migration should therefore use a **compatibility-first layout**:
    - `/LOCAL_ROOT/migrate_tpdev/replace_tpdev/`
    - `/LOCAL_ROOT/migrate_tpdev/pbpstats/`
    - `/LOCAL_ROOT/migrate_tpdev/fixed_data/raw_input_data/tpdev_data/`
    - `/LOCAL_ROOT/migrate_tpdev/calculated_data/pbpstats/`
    - `/LOCAL_ROOT/migrate_tpdev/33_wowy_rapm/`
    - plus curated `artifacts/` and `docs/`
  - put runtime inputs directly in the migrated `replace_tpdev` repo root, not only in a separate `data/runtime_inputs/` folder
  - if a partial bundle already exists under `/Users/konstantinmedvedovsky/migrate_tpdev` with the earlier `repos/... + data/...` layout, treat it as **obsolete** and rebuild it before resuming work

### Same-clock canary infrastructure (March 20, 2026)

- `same_clock_canary_manifest_20260320_v1/same_clock_canary_manifest.json`:
  - canonical manifest for all same-clock families
  - `3` positive families: `cluster_start_vs_cluster_end_timing`, `foul_free_throw_sub_same_clock_ordering`, `scorer_sub_same_clock_ordering`
  - `12` negative micro-canaries
  - `7` reviewed manual rejects
- `same_clock_canary_manifest_non_opening_ft_sub_20260320_v1.json`:
  - detailed manifest for the non-opening FT sub family specifically
  - positive core canaries: `0021700236 P1`, `0021700917 P1`
  - companion canaries: `0021900333 P4`, `0021700337 P3` (guardrail), `0029900517 P2`
  - main negative tripwire: `0021700337 P3`
- `build_same_clock_canary_manifest.py`, `run_same_clock_canary_suite.py`: tooling for building and running same-clock canary suites
- scorer/sub family (`0021800484 P3`, `0021700377 P3`) stays **frozen as negative tripwires**, not positive teaching cases

## Specific policy notes

- `0021600056` is a documented shot-source conflict, not a clean parser-fix case.
- `0021600096` currently stays on the preferred branch without a hyper-specific manual row drop:
  - accept the one benign fallback deletion
  - keep the clean Butler box row
- Broad recurring fixes belong in the fork.
- One-off anomalies should prefer local overrides over ultra-narrow new fork branches.
- External data sources (official boxscores, `tpdev_box`, Basketball Reference) are used for two purposes only:
  - auditing, comparing pipeline output against external sources to identify bugs and measure quality
  - targeted overrides, in rare, specific cases where the fix is structurally sound (for example, rebounding event ordering corrections or period starter overrides backed by source evidence)
- For lineup-derived minute checks labeled as `tpdev`, prefer the possession/PBP feed in `../fixed_data/raw_input_data/tpdev_data/full_pbp_new.parq`.
- For lineup-derived minute checks labeled as `pbpstats`, prefer `../calculated_data/pbpstats/pbpstats_player_box.parq` over official / BBR when it is available.
- Treat `tpdev_box.parq` and related `tpdev_box*` files as secondary boxscore references only; they may disagree with the `tpdev` possession feed on player-minute allocation.
- External sources must never be used to wholesale replace derived output fields such as `Minutes`, `Plus_Minus`, or any other stint-aggregated stat.
- If derived `Minutes` are wrong, the fix must be in the parser or lineup logic that produced the wrong stints, not in a post-hoc overwrite that breaks internal consistency between minutes and on/off data.
- If a generalized parser fix is not available, document the residual error and leave it rather than masking it with an external overwrite.
- March 16, 2026 specific lesson:
  - a narrow best-effort starter-swap repair was tested in the fork and then reverted
  - acceptance canary: `audit_large_minute_simple_case_rerun_20260316_v1/`
  - it kept counting stats clean on `1999`, `2005`, `2008`, and `2018`, but it did **not** resolve the original simple target rows and it introduced new `2008` minute regressions (`0020700204`, `0020700599`)
  - keep the triage tooling and the failed-canary artifacts, but do not reintroduce that exact parser branch
- March 16, 2026 explicit period-starter override result:
  - added `21700482` period `5` Nets lineup to `overrides/period_starters_overrides.json`
  - validation canary: `audit_2018_period_starter_override_21700482_20260316_v1/`
  - counting-stat audit stayed clean (`0` failed games, `0` event-stat errors, `0` audit mismatches)
  - refreshed minute report: `minutes_cross_source_2018_20260316_override_v1/`
  - `2018` `minutes_over_2` improved `15 -> 3`
  - the simple large-minute-outlier bucket is now gone from the global family register; remaining `2018` survivors are all `starter_complex_candidate` rows:
    - `21700607` `Michael Beasley`
    - `21700692` `Darius Miller`
    - `41700175` `Joe Ingles`
- March 20, 2026 same-clock architectural lesson (UPDATED after 3 failed approaches):
  - **do not attempt any same-clock scoring-attribution fix** — three architecturally different approaches all made plus-minus worse on every canary
  - Approach 1 (lineup propagation carryover): 4-minute blowup, lineup chain poisoning
  - Approach 2 (event-local overlay, foul-committer anchor): every canary regressed (+4 to +14 pm mismatches)
  - Approach 3 (event-local overlay, fouled-player anchor): even worse than Approach 2 (+4 to +15 pm mismatches)
  - **the fundamental assumption was wrong**: using pre-sub lineups for same-clock scoring events does not improve accuracy
  - the current pipeline behavior (sub is already live when scoring event is processed) is closer to reality
- the remaining plus-minus residual is NOT primarily caused by same-clock lineup attribution
- artifacts: `same_clock_ft_carryover_validation_20260320_v1/`, `same_clock_scoring_overlay_validation_20260320_v1/`, `same_clock_scoring_overlay_validation_20260320_v2/`
- March 20, 2026 validation lesson:
  - **unit tests are necessary but not sufficient** for fork rule changes
  - every new fork rule must be validated on real-game canaries before being kept
  - the validation flow must be: unit tests -> direct real-canary validation -> season/block reruns
  - do not expand scope (season reruns, block proving) until real-canary pass is confirmed
- March 19-20, 2026 intraperiod result:
  - the old `0029800075` manual lineup-window overrides were safely removed; the heuristic subsumes them
  - later Block A manual corrections repopulated `overrides/lineup_window_overrides.json` with live windows for `0029700159`, `0029700367`, and `0049700045`
  - the tightened scorer produces `0` uncovered auto-apply candidates in 1998-2000
  - this is expected and correct behavior; do not re-widen the auto-apply rules
- March 20, 2026 scorer/sub family status:
  - `0021800484 P3` and `0021700377 P3` are **negative tripwires**, not positive teaching cases
  - do not build rules that target these games as fixes
- if a broader same-clock rule happens to improve them, treat that as a secondary benefit, not a design goal

### Exception-management pivot (March 21, 2026)

- The project has moved from parser experimentation to correction management.
- The narrow non-opening pending-sub fork patch has been reverted and is now a rejected family / anti-canary, not an active baseline candidate.
- Active fork baseline for lineup proving is now:
  - opening-cluster carryover fix present
  - pending-sub narrow absent
  - failed same-clock scoring overlays absent
- A canonical correction authoring layer now exists at `overrides/correction_manifest.json`.
- The no-op registry migration is complete:
  - active lineup corrections migrated: `53`
  - active period-start corrections: `48`
  - active lineup window/event corrections: `5`
  - compile summary: `registry_migration_noop_20260321_v1/summary.json`
- v1 correction-system rules:
  - only `domain = lineup` may be active
  - runtime still reads compiled JSON/CSV override views
  - residual annotations are sparse/manual only; default residual classification is computed downstream
- Current blocker policy:
  - counting stats: blocker
  - minute outliers / actionable event-on-court rows: blocker
  - plus-minus-only rows: reference signal unless paired with lineup-integrity evidence
- Immediate gate in progress:
  - opening-cluster season proof on `2013`, `2014`, `2015`, `2019`
  - do not proceed to Block A closure until that gate is clean

## Recommended baselines by task

For historical counting-stat work:

- compare against `full_history_1997_2020_20260316_v1/`
- use `historical_baseline_manifest_20260315_v2/` for the promoted clean-season proof set
- use the March 15 proof artifacts before reopening any override or fork question

For lineup-derived work:

- start from `audit_minutes_fix_2017_2020_20260316_v3/`
- current 5-block proving loop baseline: `intraperiod_proving_1998_2020_20260319_v2_vs_baseline_20260320_v1/summary.json`
- use:
  - `large_minute_outlier_triage_baseline_20260316_v1/`
  - `large_minute_outlier_family_register_20260316_v2/`
  - `minutes_cross_source_2018_20260316_override_v1/`
  - `minutes_cross_source_2017_20260316_v4/`
  - `minutes_cross_source_2020_20260316_v4/`
  - `period_starters_vs_tpdev_20260316_v1/`
  - `plus_minus_game_classification_20260316_v1_2017/`
  - `plus_minus_game_classification_20260316_v1_2020/`

For same-clock attribution work (reference only — same-clock fork fixes are exhausted):

- canonical manifests:
  - `same_clock_canary_manifest_20260320_v1/same_clock_canary_manifest.json`
  - `same_clock_canary_manifest_non_opening_ft_sub_20260320_v1.json`
- failed rule artifacts (do not repeat any of these approaches):
  - `same_clock_ft_carryover_validation_20260320_v1/` (Approach 1: lineup propagation)
  - `same_clock_scoring_overlay_validation_20260320_v1/` (Approach 2: event-local overlay, foul-committer anchor)
  - `same_clock_scoring_overlay_validation_20260320_v2/` (Approach 3: event-local overlay, fouled-player anchor)
- opening-cluster validation (accepted): `opening_cluster_postpatch_validation_20260320_v1/`
- intraperiod baseline blocks: `intraperiod_baseline_blocks_1998_2020_20260320_v1/`
- validator script: `validate_same_clock_scoring_overlay_canaries.py`

## Working rules

- Keep all testing and reruns offline-first.
- Intraperiod missing-sub production behavior must remain local-only:
  - raw enhanced PBP
  - current inferred lineups
  - logged substitutions
  - dead-ball windows
  - explicit local overrides
- `tpdev` may be used for research, audits, sanity checks, and manual override justification, but never as a runtime dependency for the production intraperiod repair path.
- Mainline season reruns now treat lineup-derived checks as first-class audits alongside counting-stat audit. Each season output should include:
  - `minutes_plus_minus_audit_<season>.csv`
  - `minutes_plus_minus_summary_<season>.json`
  - `lineup_problem_games_<season>.txt`
  - `event_player_on_court_issues_<season>.csv`
  - `event_on_court_summary_<season>.json`
  - and a `lineup_audit` block in `summary_<season>.json`
- Do not overwrite existing `darko_*.parquet` outputs.
- Write any new rerun to a fresh output directory and compare before promoting it.
- Compare candidate runs across:
  - parquet row counts
  - failed games
  - `event_stats_errors_*.csv`
  - `rebound_fallback_deletions_*.csv`
  - integrated audit summaries
  - minutes / plus-minus reports when lineup-derived fields are in scope
- Avoid another broad historical chronology rewrite unless the evidence shows a real repeated bug family and both narrow canaries and season-level reruns prove the broader change is better.
- Use `playbyplayv2.parq` as the baseline historical event order.
- Use cached `pbpv3` selectively for enrichment and repair, not as a sole ordering authority.
- **Unit tests are necessary but not sufficient** for fork rule changes. Every new rule must pass real-game canary validation before being kept. Validation flow: unit tests -> direct real-canary validation -> season/block reruns.
- **Do not attempt any same-clock scoring-attribution fork fix.** Three architecturally different approaches (lineup propagation carryover, event-local overlay with foul-committer anchor, event-local overlay with fouled-player anchor) all made plus-minus worse on every canary. The fundamental assumption — that pre-sub lineups are more accurate for same-clock scoring events — is wrong. The current pipeline behavior is closer to reality.
- Same-clock scorer/sub edge cases (`0021800484 P3`, `0021700377 P3`) are **negative tripwires**, not positive teaching cases. Do not design rules targeting these games as fixes.

## Guiding principle for lineup-derived work

**Plus-minus is a downstream consequence of correct on-court tracking.** Do not try to fix plus-minus directly at the scoring-attribution layer — three attempts all made things worse. Instead, fix which players are tracked as on-court (period starters, intraperiod subs, event ordering). When the lineups are right, the minutes will be right, and the plus-minus will follow.

The primary metrics to work on are:
1. **Event-on-court issues** (`110` rows) — direct evidence of wrong lineup tracking
2. **Minute mismatches** (`105` rows) and **minute outliers** (`8` rows) — indirect evidence of wrong stints
3. Plus-minus mismatches (`1,877` rows) should be **tracked as a downstream indicator**, not targeted directly

## Go-forward workplan: exception-management pivot (March 21, 2026)

The project shifts from parser experimentation to correction management. The remaining work is a bounded exception program, not open-ended parser R&D.

### Strategic policy changes

1. **Freeze broad fork work.** No new chronology rewrites, same-clock overlays, or broad pending-sub rules. The pending-sub-narrow patch that regressed Block A should be reverted from the fork. The opening-cluster fix stays only if it passes its season-level non-regression proof.
2. **Block A is override territory.** For 1997-2000, manual-first is the only safe policy. No new fork rules for Block A unless a repeated archetype appears in at least 5 games across at least 2 blocks.
3. **Ship on lineup integrity, not official plus-minus equality.** Counting stats, minutes, and actionable event-on-court rows are release blockers. Official plus-minus is a diagnostic reference signal — only blocker-class when paired with minute drift or event-on-court evidence.
4. **Source-limited rows are never hidden.** Annotate them and exclude from blocker counts, but keep them visible in raw outputs. Never suppress rows from audit visibility.
5. **Blocks B-E are manual-first, not manual-only.** Default to overrides. A narrow fork rule is only justified if the same defect archetype appears in 5+ independent games across 2+ seasons and passes the full proving ladder.

### Residual classification taxonomy

Every residual row gets a class. Default classification is computed algorithmically; manual annotations override only exceptions.

| Residual class | Signals | Default action |
|---|---|---|
| `fixable_lineup_defect` | Event-on-court issue, minute outlier, clear BBR/tpdev/raw PBP agreement | Add period-starter or lineup-window correction |
| `candidate_systematic_defect` | Same archetype repeats across seasons/blocks | Consider narrow fork change after strict proving |
| `source_limited_upstream_error` | Credited player is impossible but pipeline lineup is coherent; external audit confirms source error | Document; do not change runtime data |
| `candidate_boundary_difference` | Counts exact, minutes clean, no event-on-court issue, PM differs | Provisional default for PM-only cases |
| `accepted_boundary_difference` | Same as above, promoted after stratified sampling confirms no hidden lineup bug | Characterized and closed; not chased as blockers |
| `unknown` | Does not fit above categories | Needs investigation |

Important: do NOT auto-accept all PM-only clean-minute rows as `accepted_boundary_difference` on day one. They start as `candidate_boundary_difference` and promote only after a stratified sample is reviewed across eras, absolute PM delta size, and game types.

### Blocker rules

| Metric | Blocking condition | Acceptable residual |
|---|---|---|
| Counting stats | Any mismatch | None |
| Minutes | Any unresolved high-confidence outlier > 0.5 min, or any open actionable issue > 0.1 min | <= 0.1 min tail, plus documented source-limited cases |
| Event-on-court | Any open actionable row | Documented source-limited or low-confidence rows |
| Plus-minus | Only when labeled `fixable_lineup_defect` (co-occurs with lineup evidence) | PM-only boundary differences, characterized and reported |

Raw counts and blocker counts are both emitted in every report. Promotion decisions read from blocker counts only.

### Correction evaluation criteria

Any correction (override or fork change) must satisfy all of:
- no counting-stat mismatches
- no failed games
- no event-stats errors
- no increase in severe minute outliers (> 0.5 min)
- `max_abs_minute_diff` does not worsen for affected games
- `sum_abs_minute_diff_over_0_1` improves or holds
- actionable event-on-court rows decrease or hold
- no new unexplained event-on-court class
- PM worsening is allowed only on affected games and only when lineup-integrity metrics improve

### Validation ladder

Every change must pass this ladder in order:

0. **Compile-equivalence gate** — registry compiles to runtime artifacts; lint passes
1. **Static lint** — schema validity, 5 unique players per lineup, no same-player-on-both-teams, team/roster validation for the game context, provenance fields complete, no overlapping active windows, no duplicate correction IDs
2. **Affected-game rerun** — targeted games only
3. **Golden Canary suite** — no unexpected diffs; intentional diffs must be enumerated; anti-canaries stable
4. **One dirty block + one clean block** — for any change affecting more than a handful of games
5. **Full proving loop** — before promotion to the historical baseline

The sentinel/canary logic uses: no **unexpected** diffs, expected diffs must be enumerated for targeted games, no diffs outside targeted or declared-expected scope.

---

### Phase 0: Behavior-preserving registry migration

Before any new correction is added:

1. Build `overrides/correction_manifest.json` with `corrections` and `residual_annotations` sections
2. Migrate **all** current live runtime corrections into the manifest:
   - all 49 entries from `period_starters_overrides.json` + `period_starters_override_notes.csv`
   - all entries from `lineup_window_overrides.json` + `lineup_window_override_notes.csv`
3. Build `build_override_runtime_views.py` — v1 compiler supports **only `domain = lineup`** for active corrections; any other active domain fails fast
4. Compile the manifest back to runtime artifacts
5. Prove compiled outputs are byte-equivalent to current runtime files
6. Rerun a baseline proof (Block A at minimum) and confirm no behavior change

The migration is not complete until every active live correction round-trips through the registry.

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
- `residual_class` — from the taxonomy above
- `status` — enum: `open | accepted | disputed`
- `scope_type` — optional, for event-level annotations
- `source_primary`, `source_secondary`
- `confidence`
- `linked_episode_id` — optional, links to correction episode
- `validation_artifacts` — array
- `notes`

Rejected fix families (same-clock overlays, pending-sub regressions) do NOT go in residual annotations. They belong in the Golden Canary sentinel suite and in `PROJECT_HISTORY.md`.

#### Compiler lint checks

- 5 unique players per lineup
- `team_id` belongs to the game
- all player IDs belong to that team's roster for the game
- no player appears on both teams
- no overlapping active windows for same game/period/team
- no duplicate correction IDs
- provenance fields complete for active corrections
- deterministic output ordering
- v1 rejects non-lineup active corrections

---

### Phase 1: Prove opening-cluster fix at season level

The opening-cluster fix is landed in the fork and validated on 4 real canaries. It still needs a full season-level non-regression pass.

Run seasons `2013`, `2014`, `2015`, `2019` in full. Compare against the current baseline.

Acceptance:
- counting stats stay clean across all games
- no increase in severe minute outliers
- `max_abs_minute_diff` does not worsen
- `sum_abs_minute_diff_over_0_1` improves or holds
- the four opening-cluster canaries stay improved

If any season regresses counting stats or creates new severe minute outliers, **revert the fix** and update the baseline before proceeding.

Also in this phase: **revert the pending-sub-narrow patch** from the fork. It regressed Block A overall (+22 min mismatches, +85 PM mismatches vs baseline). If a tighter gate is found later that passes the full proving ladder, it can be re-proposed.

---

### Phase 2: Block A closure — Pass 1 (starter fixes only)

Work only high-confidence period-start corrections first. Use BBR + tpdev + raw official PBP evidence. Activate only the shortest deterministic starter correction.

Each proposed correction must carry an `episode_id`, expected affected metrics, and explicit evidence basis before activation.

Current Pass 1 state after scratch triage:
- `0029700438` P2 both teams — **rejected**; scratch validation was a full no-op versus live baseline
- `0029701075` P3 BOS — **rejected as redundant**; proposed lineup already matches current inferred Boston P3 starters

If Pass 0 clears and no new starter episode appears, Pass 1 can be skipped.

Do NOT migrate the already-validated `0049700045` P1 jump-ball bootstrap from lineup-window form to starter form in this pass. Keep the current validated representation unchanged.

After Pass 1:
1. Compile manifest → runtime views
2. Run lint gate
3. Rerun only affected games
4. Run Golden Canary suite
5. Rerun Block A
6. Rebuild residual annotations and blocker counts
7. Confirm Pass 1 shows attributable movement only from starter fixes

---

### Phase 3: Block A closure — Pass 2 (lineup-window fixes only)

Work only the remaining high-confidence Block A window corrections.

Keep already-validated windows as part of the base:
- `0049700045` P1 bootstrap (existing)

Current Pass 2 state after scratch triage:
- `0029700141` P4 Farmer event-order — **rejected for the current cycle**; keep the shape documented and only revive it if it reappears in the live actionable queue
- `0029700159` P3 DEN Lauderdale/Goldwire — already live in the current window baseline
- `0029700367` P4 Stoudamire same-clock boundary — already live in the current window baseline
- `0029700452` P4 Schrempf event-order — do not promote from the old diagnostic without new evidence
- `0029701075` P3 NYK Childs/Cummings missing sub — keep out of the queue unless a narrower window shape is proved
- `0029800063` P4 Green sub-before-rebound — **rejected for the current cycle**; isolated one-game A/B validation against the current live manifest reduced event rows `2 -> 1` but worsened minute mismatches `1 -> 3` and left Dirk `P2 E211` as an open actionable row
- `0029800075` Q2 Jones/Gill spurious re-entry — **rejected**; scratch validation created counting-stat mismatches, a minute outlier, and new event rows

Explicitly excluded unless new evidence changes the shape:
- `0049700045` P2 Kukoc same-clock window — moderate confidence, same-clock ambiguity
- `0029700438` intraperiod Coleman/Schrempf/McIlvaine — multiple missing subs in NBA source data, low confidence on exact lineup reconstruction

After Pass 2:
1. Compile manifest → runtime views
2. Run lint gate
3. Rerun affected games
4. Run Golden Canary suite
5. Rerun Block A
6. Rebuild residual annotations and blocker counts
7. Confirm Pass 2 shows attributable movement only from window fixes
8. **Stop Block A** when no actionable blocker rows remain, even if raw residual counts are non-zero

---

### Phase 4: Build Golden Canary suite and residual reporting

Build `run_golden_canary_suite.py` backed by `golden_canary_manifest.json`.

Manifest categories:
- `positive_repairs` — games with validated manual corrections
- `fixed_dirty_games` — games that were broken and are now clean
- `failed_patch_anti_canaries` — games from same-clock overlay failures, pending-sub regression
- `source_limited_negative_controls` — documented source errors that must remain annotated, not "fixed"
- `pm_only_boundary_controls` — clean-minute PM-only games for stability

Must include:
- all override games from the correction manifest
- opening-cluster success cases (`0021200444`, `0021300594`, `0021400336`, `0021800748`)
- failed pending-sub anti-canaries from `block_A_postpatch_20260321_v2`
- same-clock overlay anti-canaries from March 20 artifacts
- source-limited games (`0029800063` P2, `0029800462` P3, `0029800606` P5, `0029900342` P3, `0029900517` P2)
- 10+ clean modern controls

Suite outputs:
- `summary.json` with per-game metrics
- explicit pass/fail on blocker metrics only
- no unexpected diffs; expected diffs enumerated

Build residual reporting layer so every proving run produces:
- `residual_annotations.csv` — full residual row table with computed, manual, and effective classification fields
- `actionable_queue.csv`
- `source_limited_residuals.csv`
- `boundary_difference_residuals.csv`
- `plus_minus_reference_delta_register.csv`
- `game_quality.csv`
- `summary.json` with both `raw_counts` and `blocker_counts`

---

### Phase 5: Plus-minus characterization report

Build a classification pass over the ~1,877 PM-only rows:
- `lineup_related` — PM diff + event-on-court issue or material minute diff
- `source_limited_upstream_error` — credited player impossible, lineup coherent, external audit confirms
- `candidate_boundary_difference` — counts exact, minutes clean, no event-on-court issue
- `unknown` — everything else

Then do a stratified sample of the `candidate_boundary_difference` bucket:
- sample across eras, absolute PM delta size, representative game types
- verify no hidden recurring bug class
- promote sampled-clean rows to `accepted_boundary_difference`

Publish a residual characterization report. Rename "plus-minus mismatch" to `plus_minus_reference_delta` in reporting. The report is what justifies closing the PM queue.

---

### Phase 6: Blocks B-E lineup-integrity queue

After Block A closure, apply the same two-lane approach:

**Lane 1: lineup-integrity queue** (manual overrides)
- Event-on-court issues: B=13, C=4, D=13, E=30
- Minute mismatches: B=30, C=4, D=0, E=12
- Same game-by-game diagnostic approach as Block A using BBR + tpdev

**Lane 2: PM-only characterization**
- Do not manually review PM-only games at scale
- Characterize and close unless sampling uncovers a repeated hidden class

Phase order: B and E first (larger residuals), then C, then D (zero minute mismatches, low urgency).

If a repeated narrow defect archetype emerges across multiple seasons in clean-era data, consider one fork rule — but only after the full proving ladder.

---

### Phase 7: Per-game quality metadata and final proving

Use `game_quality.csv` as the companion per-game quality table for the historical parquet.

Current March 22, 2026 release-policy note:
- the canonical downstream join surface is the sparse sidecar:
  - `reviewed_release_quality_sidecar_20260322_v1/game_quality_sparse.csv`
  - `reviewed_release_quality_sidecar_20260322_v1/join_contract.json`
- join by `game_id`
- if a game is absent from the sparse sidecar, treat it as the default exact / non-blocking case
- this pass does **not** add a per-row quality flag column to parquet

Fields:
- `game_id`
- `primary_quality_status` — enum with precedence: `open > source_limited > boundary_difference > override_corrected > exact`
- `release_gate_status`
- `release_reason_code`
- `execution_lane`
- `blocks_release`
- `research_open`
- `policy_source`
- `has_active_correction`
- `has_source_limited_residual`
- `has_boundary_difference`
- `has_material_minute_issue` (> 0.1 min)
- `has_severe_minute_issue` (> 0.5 min)
- `has_event_on_court_issue`
- `n_active_corrections`
- `n_actionable_event_rows`
- `max_abs_minute_diff`
- `sum_abs_minute_diff_over_0_1`
- `n_pm_reference_delta_rows`

Run the full 5-block proving loop. Compare against `intraperiod_proving_1998_2020_20260319_v2_vs_baseline_20260320_v1/summary.json`.

### Done definition

Use the tiered reviewed-policy model:

- **Tier 1: release-ready / analytics-grade**
  - counting stats: zero mismatches, zero failed games, zero event-stats errors
  - zero `blocks_release = true` games
  - all remaining residuals classified into:
    - `accepted_boundary_difference`
    - `accepted_unresolvable_contradiction`
    - `source_limited_upstream_error`
    - `documented_hold`
  - PM-only deltas stay classified and reported; they are not blockers by themselves

- **Tier 2: frontier-closed / research-closed**
  - all Tier 1 conditions
  - zero `research_open = true` games

Current adopted status (March 22, 2026):
- `tier1_release_ready = true`
- `tier2_frontier_closed = false`

### Era-level summary (for communication)

- **2006-2020:** mostly exact/corrected
- **2001-2005:** mostly corrected with some minor source-limited residuals
- **1997-2000:** highest use of local corrections and source-limited residuals; bounded by published manifest of known upstream data issues

Per-game quality metadata is the authoritative granularity; era summaries are for communication only.

### Secondary frontiers (out of scope for this plan)

- `2019-20+` CDN / live-data path
- `reorder_with_v3()` auditability cleanup
- intraperiod auto-apply rule widening (blocked until new uncovered candidates emerge)
- same-clock scoring-attribution fork fixes (exhausted — three approaches all rejected)
- orchestrator refactoring (`load_v9b_namespace` / notebook dump) — working, do not touch before shipping data
- extending the correction manifest to non-lineup domains (`pbp_row`, `pbp_stat`, `boxscore_source`, `validation`)
