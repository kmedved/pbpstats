# Reviewed Release Policy Decision (March 22, 2026)

## Scope

This pass is reporting-layer and policy-layer only.

Hard constraints:
- no runtime parser changes
- no active-correction changes
- no compiled override JSON/CSV changes
- no player parquet changes
- no rerun-baseline changes

If a validation step shows drift in those artifacts, the pass is wrong.

## Reviewed Policy Overlay

Authoritative overlay:
- `reviewed_frontier_policy_overlay_20260322_v1.csv`

Provenance rule:
- the overlay has the same review discipline as the correction manifest
- it is versioned
- changes require documented justification
- it is not a back door for silent reclassification

Overlay booleans:
- `blocks_release`
- `research_open`

Release-facing statuses:
- `exact`
- `override_corrected`
- `source_limited_upstream_error`
- `accepted_boundary_difference`
- `accepted_unresolvable_contradiction`
- `documented_hold`
- `open_actionable`

Execution lanes:
- `exact`
- `override_corrected`
- `source_limited`
- `policy_frontier_non_local`
- `accepted_contradiction`
- `documented_hold`
- `unreviewed_open`

## Current Reviewed Frontier

Authoritative reviewed inventory:
- `phase6_reviewed_frontier_inventory_20260322_v1/reviewed_frontier_inventory.csv`

Current split:
- `17` raw-open games
- `0` release blockers
- `release_blocking_game_ids = []`
- `5` research-open games

Reviewed release mapping:

| `release_reason_code` | Games | `release_gate_status` | `execution_lane` | `blocks_release` | `research_open` |
|---|---|---|---|---|---|
| `same_clock_control` | `0021700337`, `0021700377`, `0021700514`, `0021801067`, `0021900333` | `accepted_boundary_difference` | `policy_frontier_non_local` | `false` | `false` |
| `same_clock_rebound_survivor` | `0021900201`, `0021900419`, `0021900487`, `0021900920`, `0041900155` | `accepted_boundary_difference` | `policy_frontier_non_local` | `false` | `false` |
| `period_start_contradiction` | `0020900189`, `0021300593` | `accepted_unresolvable_contradiction` | `accepted_contradiction` | `false` | `false` |
| `mixed_source_boundary_tail` | `0020000628` | `documented_hold` | `documented_hold` | `false` | `true` |
| `severe_minute_insufficient_local_context` | `0020400335` | `documented_hold` | `documented_hold` | `false` | `true` |
| `scrambled_pbp_missing_subs_blockA` | `0029701075` | `documented_hold` | `documented_hold` | `false` | `true` |
| `same_clock_accumulator_nonlocal` | `0021700394` | `documented_hold` | `documented_hold` | `false` | `true` |
| `source_limited_tradeoff_hold` | `0029700159` | `documented_hold` | `documented_hold` | `false` | `true` |

Research-open set:
- `0020000628`
- `0020400335`
- `0021700394`
- `0029700159`
- `0029701075`

Resolved contradictions are not research-open. They are accepted final classifications, not pending investigations.

## Tier Semantics

Tier 1:
- release-ready / analytics-grade
- requires `blocks_release = false` everywhere

Tier 2:
- frontier-closed / research-closed
- requires `research_open = false` everywhere

Current corpus result:
- `tier1_release_ready = true`
- `tier2_frontier_closed = false`

## PM Reporting

Release-facing PM report:
- `phase6_reviewed_pm_reference_report_ABCDE_20260322_v1`
- built from the authoritative raw PM residual bundles, with the reviewed policy overlay applied at report-build time

Raw PM taxonomy is preserved unchanged in `pm_residual_class`:
- `candidate_boundary_difference`
- `source_limited_upstream_error`
- `lineup_related`

Additive release-facing PM classes:
- `reference_only_boundary`
- `accepted_contradiction`
- `source_limited_upstream`
- `documented_hold`
- `open_actionable_lineup_blocker`

Current PM release result:
- raw counts remain named by class:
  - `candidate_boundary_difference = 1744`
  - `source_limited_upstream_error = 81`
  - `lineup_related = 31`
- release-facing counts are:
  - `reference_only_boundary = 1768`
  - `accepted_contradiction = 4`
  - `documented_hold = 3`
  - `source_limited_upstream = 81`
  - `open_actionable_lineup_blocker = 0`

`0020000628` wording rule:
- current live state: `0` PM reference rows
- rejected scratch attempt: introduced a small `+/-2` PM swap
- classification stays `documented_hold` because the `0.25` minute tail persists, not because of PM

## DARKO Export Decision

Decision:
- do not add a per-row `data_quality_flag` column to the DARKO parquet in this pass
- DARKO consumers should join `game_quality.csv` by `game_id`

Reason:
- parquet changes are explicitly out of scope for this pass
- the reviewed release-policy layer lives cleanly in the sidecar quality outputs already

Join contract:
- release-facing game flags come from `game_quality.csv`
- frontier inventories remain companion audit artifacts, not the integration join surface

Canonical sidecar artifact:
- `reviewed_release_quality_sidecar_20260322_v1/game_quality_sparse.csv`
- `reviewed_release_quality_sidecar_20260322_v1/join_contract.json`
- `reviewed_release_quality_sidecar_20260322_v1/integration_notes.md`

Sparse-join rule:
- left join on `game_id`
- if a game is absent from the sidecar, treat it as the default exact / non-blocking case

## Additional Notes

`0029800606` remains outside the reviewed frontier overlay:
- `stability_class = unstable_control`
- infrastructure debt only
- not a reviewed frontier override case
