# Block A Exception Queue (2026-03-21)

This is the current queue state under the exception-management policy:

- promote only live actionable lineup-integrity fixes
- do not add redundant/stale corrections
- keep source-limited cases visible but out of the correction path
- treat plus-minus-only movement as non-blocking unless it co-occurs with lineup-integrity evidence

## Post-queue rerun snapshot (2026-03-22)

Fresh live rerun and residual bundle:
- `/Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev/block_A_live_postqueue_20260322_v1`
- `/Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev/block_A_live_postqueue_20260322_v1_residual/summary.json`

Compared with the old Block A baseline:
- actionable event rows: `50 -> 39`
- actionable residual rows: `83 -> 72`
- material minute rows: `33 -> 33`
- severe minute rows: `7 -> 8`
- open games: `13 -> 12`

Interpretation:
- the live correction set clearly improves the Block A event-on-court surface
- Block A is not fully closure-ready yet because the severe-minute tail is still present and is slightly worse than the old frontier baseline
- there are no remaining straightforward manual override promotions in this queue
- `0029700367` has fallen out of the open queue and now behaves like `boundary_difference`
- the remaining likely next work is annotation/source-limited classification plus possible `starter-complex` investigation, not more window/event overrides

## Current residual state after sparse annotations (2026-03-22, later)

Latest live residual bundle:
- `/Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev/block_A_live_postqueue_20260322_v1_residual/summary.json`

Current blocker counts:
- actionable event rows: `18`
- actionable residual rows: `42`
- material minute rows: `24`
- severe minute rows: `4`
- open games: `5`
- source-limited games: `7`

Current open tail:
- `0029700159`
  - keep open/documented-unresolved
  - the live DEN P3 windows are already the best-known repair
  - do not add source-limited annotations while the Stith/Lauderdale minute split still disagrees materially with official/BBR
- `0029701075`
  - keep open as `candidate_systematic_defect`
  - late-quarter chronology/source split; scratch windows remain contradictory
- `0029800063`
  - only `A.C. Green P4 E511` remains open
  - Dirk `P2 E211` is already `source_limited_upstream_error`
  - do not activate the Green one-event flip; it still worsens minute profile
- `0029800661`
  - keep open as `candidate_systematic_defect`
  - boundary-state / starter-complex style minute investigation, not a source-limited or manual-window case
- `0049700045`
  - keep open
  - PM-only residue already lives in `candidate_boundary_difference`
  - remaining 3 event rows are still actionable same-clock boundary cases, not source-limited

Current newly accepted source-limited promotions:
- `0029800075` Gill / Jones
- `0029700438` Schrempf / McIlvaine / Jackson / Ratliff / Coleman cluster
- `0029700452` Wingate plus Schrempf `P4 E402`

## Current residual state after A.C. Green promotion (2026-03-22, latest)

Latest live residual bundle:
- `/Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev/block_A_live_postqueue_20260322_v1_residual/summary.json`

Current blocker counts:
- actionable event rows: `17`
- actionable residual rows: `41`
- material minute rows: `24`
- severe minute rows: `4`
- open games: `4`
- source-limited games: `8`

Delta from the prior live bundle:
- actionable event rows: `18 -> 17`
- actionable residual rows: `42 -> 41`
- open games: `5 -> 4`

Current true open tail:
- `0029700159`
  - open / documented-unresolved
  - keep the two live Denver P3 windows as the least-bad validated state
- `0029701075`
  - open as `candidate_systematic_defect`
  - no narrower safe NYK/BOS correction survived validation
- `0029800661`
  - open as `candidate_systematic_defect`
  - whole-game boundary-state/starter-complex minute pattern
- `0049700045`
  - open same-clock boundary tail
  - no remaining safe local correction to promote on the current live baseline

Latest newly accepted source-limited promotion:
- `0029800063` `P4 E511` A.C. Green rebound
  - current live Green/Bradley minutes already match official / tpdev_pbp / BBR almost exactly
  - the single-event Green flip remains rejected because it worsens the minute profile

## Current residual state after `0029700159 E349` split (2026-03-22, latest)

Latest live residual bundle:
- `/Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev/block_A_live_postqueue_20260322_v1_residual/summary.json`

Current blocker counts:
- actionable event rows: `16`
- actionable residual rows: `40`
- material minute rows: `24`
- severe minute rows: `4`
- open games: `4`
- source-limited games: `8`

Delta from the prior live bundle:
- actionable event rows: `17 -> 16`
- actionable residual rows: `41 -> 40`
- open games: `4 -> 4`

Latest newly accepted source-limited promotion:
- `0029700159` `P3 E349` broken Garrett sub-out row
  - keep the game open on minute residuals, but the lone surviving event row is now explicitly treated as broken upstream source text

Current true open tail:
- `0029700159`
  - open / documented-unresolved on minutes only
- `0029701075`
  - open as `candidate_systematic_defect`
- `0029800661`
  - open as `candidate_systematic_defect`
- `0049700045`
  - keep plain `open` as same-clock boundary tail
  - do not add a game-level `candidate_systematic_defect` annotation

## Current residual state after source-limited consolidation (2026-03-22, latest latest)

Latest live residual bundle:
- `/Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev/block_A_live_postqueue_20260322_v1_residual/summary.json`

Current blocker counts:
- actionable event rows: `13`
- actionable residual rows: `20`
- material minute rows: `7`
- severe minute rows: `4`
- open games: `2`
- source-limited games: `10`

Delta from the prior live bundle:
- actionable event rows: `16 -> 13`
- actionable residual rows: `40 -> 20`
- material minute rows: `24 -> 7`
- open games: `4 -> 2`

Latest newly accepted source-limited promotions:
- `0029800661`
  - whole-game minute pattern now treated as `source_limited_upstream_error`
  - reason:
    - no event-on-court rows
    - no severe minute outlier
    - live output matches BBR on `18/20` player-minute rows
    - the two BBR misses still sit closer to `tpdev_pbp` than to the BBR row
- `0049700045`
  - row-grain same-clock source-limited event tails:
    - `P2 E172` Kukoc
    - `P2 E185` Armstrong
    - `P4 E376` Kerr
  - reason:
    - all three occur at the exact sub-out clock
    - current live state already keeps the minute profile exact-to-near-exact
    - tested local fixes worsen minutes rather than improving the game cleanly

Current true open tail:
- `0029700159`
  - still open / documented-unresolved on minutes only
- `0029701075`
  - still open as `candidate_systematic_defect`
  - do not split the game further without new evidence

## Current residual state after `0029800661` source-limited promotion (2026-03-22, latest)

Latest live residual bundle:
- `/Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev/block_A_live_postqueue_20260322_v1_residual/summary.json`

Current blocker counts:
- actionable event rows: `16`
- actionable residual rows: `23`
- material minute rows: `7`
- severe minute rows: `4`
- open games: `3`
- source-limited games: `9`

Delta from the prior live bundle:
- actionable residual rows: `40 -> 23`
- material minute rows: `24 -> 7`
- open games: `4 -> 3`
- source-limited games: `8 -> 9`

Latest newly accepted source-limited promotion:
- `0029800661` game-level minute conflict
  - live output matches BBR minutes on `18/20` player rows
  - the only two BBR misses are one Vaughn/Hendrickson swap, and `tpdev_pbp` stays closer to the live output than to BBR on that pair
  - official minutes match only `1/20` player rows
  - no event-on-court rows remain, so this now fits `source_limited_upstream_error` better than `candidate_systematic_defect`

Current true open tail:
- `0029700159`
  - open / documented-unresolved on minutes only
- `0029701075`
  - open as `candidate_systematic_defect`
- `0049700045`
  - keep plain `open` as same-clock boundary tail
  - do not add a game-level `candidate_systematic_defect` annotation

## Pass 1 starter queue

### `0029700438`
- Status: reject after scratch validation
- Notes:
  - use an explicit dual-team P2 starter episode, not a delta
  - proposed team-period rows:
    - SEA `1610612760`: `[56, 1425, 64, 107, 766]`
    - PHI `1610612755`: `[754, 221, 187, 707, 243]`
  - scratch rerun was a full no-op versus live baseline:
    - `minutes_mismatches = 4`
    - `minutes_outliers = 1`
    - `plus_minus_mismatches = 0`
    - `event_on_court issue_rows = 11`
  - do not activate this episode; later P2 residuals are still the harder intraperiod/source-limited family

### `0029701075` BOS P3
- Status: reject as redundant
- Proposed lineup `[952, 692, 72, 962, 1500]` already matches the current inferred Boston P3 starters.
- Do not add a manifest correction for this team-period.
- Keep only the NYK missing-sub window family in reserve if later evidence reopens the game.

## Pass 2 window/event queue

### `0029700141`
- Status: reject for current cycle, keep documented
- High-confidence ordering case, but not present in the current live actionable Block A queue.
- The manifest keeps the shape documented but rejected for now.
- Ready-to-revive shape if it reappears:
  - `scope_type = event`
  - `period = 4`
  - `team_id = 1610612766`
  - `event_num = 483`
  - delta: `swap_out_player_id = 779`, `swap_in_player_id = 1108`

### `0029800063`
- Status: reject for current cycle after isolated A/B validation
- Corrective episode is only:
  - `scope_type = event`
  - `period = 4`
  - `team_id = 1610612742`
  - `event_num = 511`
  - delta: `swap_out_player_id = 762`, `swap_in_player_id = 920`
- Do not use the older `511-end` window.
- Treat `P2 E211` Dirk as `source_limited_upstream_error`, not a correction target.
- Isolated one-game A/B result versus the current live manifest:
  - `event_on_court issue_rows: 2 -> 1`
  - `minutes_mismatches: 1 -> 3`
  - `material minute rows: 0 -> 1`
  - the new tails are `0.095` minutes for Bradley and Green, while Dirk remains at `0.100`
  - `plus_minus_mismatches: 0 -> 0`
  - `0` boxscore mismatches, `0` event-stats errors, `0` failed games
- Decision:
  - do not activate this correction
  - keep `P4 E511` documented as a tempting local flip that still fails blocker-first policy because `P2 E211` remains open and the minute profile gets worse

### `0029800075`
- Status: reject after scratch validation
- Old diagnostic window numbers are stale and should not be copied.
- Current enhanced-event episode begins at `E187` and should be encoded as six windows, all with:
  - `period = 2`
  - `team_id = 1610612751`
  - delta: `swap_out_player_id = 383`, `swap_in_player_id = 1800`
- Current window spans to use:
  - `187-188`
  - `190-204`
  - `205-211`
  - `212-222`
  - `223-232`
  - `233-244`
- The scratch rerun is a hard reject:
  - introduced `2` team box mismatches and `2` player box mismatches
  - created `1` minute outlier
  - created `6` event-on-court rows
- Remove this episode from the future activation queue.

## Source paths

- `/Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev/block_A_residual_baseline_20260321_v1`
- `/Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev/block_A_manual_override_diagnostic_20260321.md`
- `/Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev/_tmp_pass2_probe_0029800063_0029800075`
- `/Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev/_tmp_validate_0029800063_live_baseline_20260322_v1`
- `/Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev/_tmp_validate_0029800063_candidate_20260322_v1`
- `/Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev/_tmp_trace_0029701075_compare`

## Authoritative current frontier (2026-03-22)

This is the current live Block A state after the `0029800661` source-limited promotion and the `0049700045` same-clock row-grain source-limited annotations.

Current blocker counts:
- actionable event rows: `13`
- actionable residual rows: `20`
- material minute rows: `7`
- severe minute rows: `4`
- open games: `2`
- source-limited games: `10`

Current true open tail:
- `0029700159`
  - open / documented-unresolved on minutes only
  - no actionable event rows remain
- `0029701075`
  - open as `candidate_systematic_defect`
  - late-quarter P3 chronology/source split remains contradictory rather than promotable

Moved out of the open lane this pass:
- `0029800661`
  - source-limited whole-game minute source conflict
- `0049700045`
  - source-limited same-clock boundary tail
  - event rows:
    - `P2 E172` Kukoc
    - `P2 E185` Armstrong
    - `P4 E376` Kerr

## `0029700159` opening-cluster theory check (2026-03-22)

- Investigated the claim that the new Bryant Stith severe minute row was introduced by the opening-cluster carryover fix.
- Conclusion: not plausible for this game.
- Mechanical reason:
  - the opening-cluster gate only matters when strict period starters disagree with an exact local `v6` row
  - `0029700159` has no `v6` period-starter rows at all
  - this game resolves through `v5` fallback plus the local Denver `P3` lineup windows
- Supporting evidence:
  - `/Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev/period_starters_v6.parquet` has no `0029700159` rows
  - `/Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev/period_starters_v5.parquet` has only periods `2-4`
  - the raw `12:00` rows in `playbyplayv2.parq` are plain start-of-period markers, not an opening technical/flagrant/ejection cluster
- Practical interpretation:
  - the current Stith / Lauderdale / Garrett minute residue is still the known Denver `P3` override tradeoff
  - `P3 E349` remains source-limited as the broken `Lauderdale FOR Garrett` source row
  - `0029700159` stays open, but it should not be used as evidence that the opening-cluster fork fix regressed Block A
