## Phase 7 Block 0 triage (March 23, 2026)

This note records the one-pass review of the 10 new `1997` raw-open games surfaced by the March 22 full-history rerun. The proof standard for this batch was intentionally constrained:

- do not rerun seasons
- use the March 22 candidate run as fixed input
- use best-available local sources only
- do not spend extra cycles hunting corroboration known to be absent
- if a game could not be classified safely in one pass, keep it raw-open and classify it in the reviewed overlay as `documented_hold`

Coverage reality for this batch:

- `official` local coverage was available for all 10 through the rerun outputs and `nba_raw.db`
- `playbyplayv2.parq` was available for all 10
- `nba_raw.db/pbpv3` was available for all 10
- `tpdev/full_pbp_new.parq` was available only for `0029600070`
- `pbpstats_player_box.parq` had no rows for these 10 games
- local BBR support was not practically available in one pass for the moderate/severe cases, though the catastrophic pair did have matching BBR game-level coverage

Decision summary:

| Game | Decision | Raw promotion? | Scratch probe? | Notes |
|---|---|---|---|---|
| `0029600070` | `documented_hold` | no | no | Minute-only Sarunas tail, no safe repair path after one pass even with `tpdev` coverage |
| `0029600171` | `documented_hold` | no | no | Six Q3 off-court credits on Michael Smith; no safe Kings Q3 boundary repair surfaced |
| `0029600175` | `documented_hold` | no | no | One Chris Carr event-credit row plus a small minute tail, under-evidenced after one pass |
| `0029600204` | `documented_hold` | no | no | Two Chicago off-court credit rows across periods, no safe local fix |
| `0029600332` | `documented_hold` | no | not now | Catastrophic whole-game minute swap, but not self-evidently upstream-broken enough for raw `source_limited` |
| `0029600370` | `documented_hold` | no | not now | Catastrophic paired swaps, but not self-evidently upstream-broken enough for raw `source_limited` |
| `0029600585` | `documented_hold` | no | no | Localized Dell Curry P3 cluster, still not enough for a safe repair |
| `0029600657` | `documented_hold` | no | not now | Concrete duplicate-sub cluster, but not enough corroboration to choose the wrong row safely |
| `0029601163` | `documented_hold` | no | no | Clean minute-only drift with no event-on-court or plus-minus corroboration |
| `0049600063` | `accepted_boundary_difference` | no | no | Best fit is same-clock / event-credit survivor: tiny minute drift, PM-only tail, distributed foul credits |

Key consequences:

- no new raw `source_limited_upstream_error` promotions landed from this Block 0 pass
- no live lineup corrections were activated from this Block 0 pass
- no scratch correction probes were launched because no candidate cleared the one-pass safety bar
- the final reviewed `20260323_v2` frontier therefore covers all `30` current raw-open games:
  - `17` carried from the March 22 reviewed frontier
  - `3` modern same-clock additions
  - `10` new `1997` Block 0 reviews
