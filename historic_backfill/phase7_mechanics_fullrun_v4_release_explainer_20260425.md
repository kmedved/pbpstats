# Phase 7 Mechanics Fullrun V4 Release Explainer

Date written: 2026-04-25

This is the closure note for the 1997-2020 lineup/source frontier after the
mechanics pass that promoted the Harrington synthetic substitution and the
same-clock/control-event audit rules.

The short version:

- The full 1997-2020 rerun completed with `0 failed_games` and
  `0 event_stats_errors`.
- The reviewed release gate is green:
  `release_blocking_game_count=0`, `research_open_game_count=0`,
  `tier1_release_ready=true`, `tier2_frontier_closed=true`.
- The raw classifier still calls 13 games `open`, but those 13 now have
  explicit reviewed production lanes. They are not unfinished release blockers.
- `0020400335` is included in those 13 because the synthetic Harrington
  substitution fixes the event reality but leaves a large official MIN/PM
  source split.

## What Changed

### 1. Synthetic Substitution Support

`pbp_row_overrides.py` now supports explicit synthetic substitution rows through
`insert_sub_before` / `insert_sub_after`.

The canary and only new synthetic missing-sub repair in this pass is:

```csv
0020400335,insert_sub_before,148,149,Synthesize missing Q2 7:59 Junior Harrington FOR J.R. Smith sub using BBR and tpdev possession support,2,7:59,,home,2747,JR Smith,1610612740,2454,Junior Harrington,1610612740
```

Basketball story:

- Junior Harrington has five real Q2 actions.
- BBR has Harrington entering for J.R. Smith at Q2 `7:59`.
- tpdev possession state supports Harrington occupying that slot.
- The official NBA/tpdev-box MIN/PM line disagrees, so the reviewed output
  accepts that source/reference split rather than leaving five impossible
  Harrington event credits.

Implementation detail:

- The synthetic row is marked with `PBP_ROW_OVERRIDE_ACTION`.
- The `pbpstats` v3 dedupe path now preserves explicit synthetic override rows
  even when v3 has no matching action number.

### 2. Same-Clock / Control Event Audit Semantics

The event-on-court audit now handles these control cases as valid when guarded
same-clock evidence supports them:

- Live foul before same-clock sub-out of the fouler.
- Exact-period-start live credit before same-clock sub-out.
- Technical free throw shooter eligibility.
- Replacement free throw shooter eligibility.
- Same-clock sub-in before a subsequent FT-rebound continuation.

This is an audit semantics change, not a broad possession/minute rewrite. It
prevents dead-ball/control rows from being treated as impossible live-lineup
proof when basketball causality says the row order is only a same-clock feed
artifact.

The mechanics pass cleared these previously reviewed rule-family games from the
active overlay:

```text
0021300593
0021700236
0021700337
0021700377
0021700514
0021700917
0049600063
```

### 3. Local Row Overrides Kept / Promoted

The final reviewed state includes these local override decisions:

- `0020000628`: keep `E229 Van Horn FOR Williams` before `E227 Van Horn foul`.
- `0029600204`: promote `E338 Ron Harper FOR Jordan` before `E339 Ron Harper
  foul`.

Previously promoted/validated local fixes remain part of the active row
override set, including:

- `0020900189`: stale exact-start `Lawson FOR Billups` phantom row dropped;
  Lawson remains active from Q2 start and Billups re-enters later.
- `0021800484`: Curry and-one foul before Warriors same-clock substitutions.
- `0021801067`: Smart flagrant/ejection before Brown replacement.
- `0021700394`: period-start marker ordering fix in `pbpstats`; this game is
  no longer part of the open frontier.

## What "13 Open Games" Means Now

The word `open` appears in the raw residual bundle because the raw mechanical
classifier only knows whether a game is exact versus the raw audit criteria. It
does not know the reviewed policy decision unless the overlay is applied.

So there are two different readings:

| Layer | Meaning | Count |
| --- | --- | ---: |
| Raw residuals | Parser output still has a non-exact residual | 13 |
| Reviewed residuals | Residual has an explicit accepted production lane | 13 accepted |
| Release gate | Blocks release | 0 |
| Research queue | Still open research work | 0 |

Use the reviewed overlay/sidecar for release decisions. Do not use raw
`primary_quality_status=open` by itself as a release blocker.

Authoritative release fields:

- `release_gate_status`
- `release_reason_code`
- `execution_lane`
- `blocks_release`
- `research_open`

## Final 13 Reviewed Production Decisions

| Game | Lane | Raw residual | Production decision |
| --- | --- | --- | --- |
| `0020000628` | `local_override_chosen` | Van Horn/Williams 0.25 minute and +/-2 PM split remains after event fix | Keep `E229` before `E227`; accept source/reference split |
| `0020400335` | `synthetic_sub_chosen` | Harrington/J.R. Smith official MIN/PM split remains after synthetic sub | Synthesize Q2 `7:59` Harrington for J.R. Smith; accept official-box split |
| `0029600070` | `status_quo_chosen` | Marciulionis 22-second old-era minute tail | Keep parser-visible stints; no event or PM contradiction |
| `0029600171` | `status_quo_chosen` | Michael Smith has 6 off-court Q3 event credits; outgoing player unanchored | Accept source defect; do not hallucinate an outgoing King |
| `0029600175` | `status_quo_chosen` | Chris Carr one-shot, 14-second tail; outgoing unknown | Accept single-event source defect; no synthetic one-possession stint |
| `0029600204` | `local_override_chosen` | Ron Harper minute tail and Harper/Jordan +/-2 PM split remains | Move `E338` before `E339`; event reality beats PM exactness |
| `0029600332` | `status_quo_chosen` | Huge Mullin/DeClercq box/PBP rotation split | Keep visible PBP stints; no event anchor for box-minute reconstruction |
| `0029600370` | `status_quo_chosen` | Huge Dallas/Seattle multi-player box/PBP split | Keep visible PBP rotations; no event anchor for role-based reconstruction |
| `0029600585` | `status_quo_chosen` | Dell Curry has 2 event contradictions before visible entry | True legal entry/outgoing is unanchored; do not move row backward |
| `0029600657` | `status_quo_chosen` | Duplicate Maxwell/Del Negro sub source split | Keep current output; both tested suppressions were worse |
| `0029601163` | `status_quo_chosen` | Dudley/Robinson minute-only old-era tail | Keep current output; no event or PM corroboration |
| `0029700159` | `policy_overlay_chosen` | Lauderdale IN credible, Garrett OUT impossible, true OUT unanchored | Keep current Denver P3 live state; accept source-limited tradeoff |
| `0029701075` | `policy_overlay_chosen` | Scrambled Knicks/Celtics P3 with 13 event contradictions | Keep current parser output; do not fake a partial P3 reconstruction |

## Why These Are Covered, Not Deferred

The final policy is not "do nothing." It is:

1. Fix what is anchored by row order, synthetic source evidence, or same-clock
   basketball causality.
2. Prefer live event reality over official MIN/PM when the repair is anchored.
3. Do not synthesize rotations when the outgoing player or legal entry point is
   unanchored.
4. When both choices are imperfect, choose and name the least-wrong production
   output.

The 13 games are therefore covered by explicit decisions:

- `local_override_chosen`: 2 games
- `synthetic_sub_chosen`: 1 game
- `status_quo_chosen`: 8 games
- `policy_overlay_chosen`: 2 games

The status quo lanes are still active decisions. They mean "we intentionally
keep visible parser output because the alternative would invent unsupported
rotation state."

## Validation Results

Focused tests:

```text
replace_tpdev focused tests: 15 passed
pbpstats full suite: 303 passed
```

Full rerun:

```text
run_dir: full_history_1997_2020_20260424_mechanics_fullrun_v4
seasons: 1997-2020
player_rows: 681578
failed_games: 0
event_stats_errors: 0
```

Raw residual summary:

```text
raw open games: 13
raw release_blocking_game_count: 13
raw research_open_game_count: 13
```

Reviewed residual summary:

```text
release_blocking_game_count: 0
research_open_game_count: 0
tier1_release_ready: true
tier2_frontier_closed: true
```

Sidecar/join validation:

```text
reviewed_override_game_count: 13
join_passed: true
reviewed_rows_survive_join_unchanged: true
```

PM report:

```text
release_blocker_game_count: 0
```

## Key Artifacts

Full run:

- `full_history_1997_2020_20260424_mechanics_fullrun_v4/summary.json`

Raw residuals:

- `phase7_raw_residuals_1997_2020_20260424_mechanics_fullrun_v4/summary.json`
- `phase7_raw_residuals_1997_2020_20260424_mechanics_fullrun_v4/game_quality.csv`

Reviewed residuals:

- `phase7_reviewed_residuals_1997_2020_20260424_mechanics_fullrun_v4/summary.json`
- `phase7_reviewed_residuals_1997_2020_20260424_mechanics_fullrun_v4/game_quality.csv`

Reviewed policy overlay:

- `reviewed_frontier_policy_overlay_20260424_mechanics_fullrun_v4.csv`
- `reviewed_frontier_policy_overlay_20260424_mechanics_fullrun_v4.summary.json`

Current frontier inventory:

- `phase7_open_blocker_inventory_20260424_mechanics_fullrun_v4.csv`
- `phase7_true_blocker_shortlist_20260424_mechanics_fullrun_v4.csv`
- `phase7_reviewed_frontier_inventory_20260424_mechanics_fullrun_v4/summary.json`

Release sidecar:

- `reviewed_release_quality_sidecar_20260424_mechanics_fullrun_v4/game_quality_sparse.csv`
- `reviewed_release_quality_sidecar_20260424_mechanics_fullrun_v4/join_contract.json`
- `reviewed_release_quality_sidecar_20260424_mechanics_fullrun_v4/summary.json`

Sidecar smoke:

- `reviewed_release_quality_sidecar_join_smoke_20260424_mechanics_fullrun_v4/summary.json`
- `reviewed_release_quality_sidecar_join_smoke_20260424_mechanics_fullrun_v4/darko_reviewed_join_sample.parquet`

PM characterization:

- `phase7_reviewed_pm_reference_report_1997_2020_20260424_mechanics_fullrun_v4/summary.json`
- `phase7_reviewed_pm_reference_report_1997_2020_20260424_mechanics_fullrun_v4/pm_reference_characterization.csv`

Overrides:

- `pbp_row_overrides.csv`

## Code Touchpoints

`replace_tpdev`:

- `pbp_row_overrides.py`
- `pbp_row_overrides.csv`
- `audit_event_player_on_court.py`
- `tests/test_pbp_row_overrides.py`
- `tests/test_audit_event_player_on_court.py`

`pbpstats` fork:

- `pbpstats/offline/ordering.py`
- `tests/test_offline_ordering.py`

## How To Read This Later

If someone asks "are there still open games?", answer:

> Raw, yes: 13 games still have non-exact residuals. Reviewed/release, no:
> all 13 have explicit accepted lanes, and none block release or remain open
> research.

If someone asks "does `0020400335` really have the synthetic sub?", answer:

> Yes. It is a real `insert_sub_before` row in `pbp_row_overrides.csv`, and
> `pbpstats` preserves it through v3 dedupe. It remains raw-open only because
> the synthetic correction exposes an accepted official MIN/PM source split.

If someone asks "should we keep working these 13?", answer:

> Not for this release. Work them only if new external source evidence appears
> for an unanchored outgoing player, legal entry point, or full-period
> reconstruction. Otherwise the current reviewed overlay is the production
> decision.
