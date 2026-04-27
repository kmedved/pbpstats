# Project History

This file is the archived history for `replace_tpdev`.

`AGENTS.md` is the compact current brief.
This file is the longer historical memory: what changed, why it changed, which canaries mattered, and which artifacts became the durable baseline.

Recovery note:

- This archive was rebuilt after `AGENTS.md` was trimmed down.
- It is based on the recovered prior `AGENTS.md` diff, the surviving dated artifact directories, and the local audit markdowns.
- It should be treated as the recovered narrative history for the project, even though it is not guaranteed to be a byte-for-byte copy of the old long-form `AGENTS.md`.

## Contents

- [Historical Snapshot](#historical-snapshot)
- [March 12, 2026](#march-12-2026)
- [March 13, 2026](#march-13-2026)
- [March 14, 2026](#march-14-2026)
- [March 15, 2026](#march-15-2026)
- [March 16, 2026](#march-16-2026)
- [Durable Historical Conclusions](#durable-historical-conclusions)
- [Most Important Artifact Families](#most-important-artifact-families)
- [How To Use This File](#how-to-use-this-file)

## Historical Snapshot

At the start of the March 12-16, 2026 cleanup pass, the project had already become an offline-first replacement for the old `tpdev` box-score build.

Core assumptions from that period:

- Source-of-truth notebook:
  - `0c2_build_tpdev_box_stats_version_v9b.ipynb`
- Raw dump only:
  - `0c2_build_tpdev_box_stats_version_v9b.py`
- Canonical local inputs:
  - `playbyplayv2.parq`
  - `nba_raw.db`
  - `validation_overrides.csv`
  - `boxscore_source_overrides.csv`
  - `boxscore_audit_overrides.csv`
  - `manual_poss_fixes.json`
- Critical data-source reality:
  - live `playbyplayv2` on `stats.nba.com` was effectively dead for this project
  - cached `pbpv3` in `nba_raw.db` remained useful, but not as canonical chronology
  - CDN live feed was only a recent-season path, verified from `2019-20` onward
  - historical runs were not reconstructible from live NBA endpoints alone
- Historical gotchas:
  - `playbyplayv2.parq` stores historical game ids without the leading `00`
  - `EVENTMSGTYPE` is string-backed there
  - `nba_raw.db` stores cached payloads under short endpoint names such as `pbpv3`, `summary`, and `boxscore`
- Parser state:
  - the project depended on a custom `pbpstats` fork
  - Windows runtime name: `darko311`
  - macOS runtime name: `DARKO`
  - active runtime was expected to import directly from the editable repo checkout

## March 12, 2026

This was the first major stabilization day for the offline historical parser.

### Initial smoke recovery

- Previously failing games `0049600063` and `0049700045` reran successfully with:
  - patched custom `pbpstats`
  - local `playbyplayv2.parq`
  - local `nba_raw.db`
  - a temporary runner that preserved the whole flow offline

### Early fork repairs

- Period-boundary and retry-budget fixes:
  - output dir: `rerun_cautious_parallel_20260312_v3`
  - `1997`: `27,874` rows, `0` failed games, `330` rebound deletions, `68` `event_stats` errors
- Non-same-clock rebound-chain fix:
  - output dir: `rerun_cautious_parallel_20260312_relaxed_chain_v1`
  - `1997`: still `0` failed games, deletions improved `330 -> 233`, errors stayed `68`
  - archived `1998` problem-game canary did not improve
- Workspace-side malformed team-event normalization:
  - helper: `team_event_normalization.py`
  - tests: `tests/test_team_event_normalization.py`
  - output dir: `rerun_cautious_parallel_20260312_team_event_fix_v1`
  - `1997`: `27,871` rows, `227` deletions, `59` `event_stats` errors
  - this eliminated malformed team-side event-stat errors in `0029600332` and `0029600370` without touching the fork
- `patch_start_of_periods()` fix in the fork:
  - synthetic start-of-period rows were inserted without globally re-sorting by `EVENTNUM`
  - fork regression subset passed `37/37`
  - output dir: `rerun_cautious_parallel_20260312_patch_start_order_v1`
  - `1997`: `27,871` rows, `41` deletions, `59` `event_stats` errors
  - this beat the archived root `1997` run on both failures and rebound deletions

### Important conclusion from March 12

- A global chronology rewrite was explicitly tested and rejected.
- Pure `PERIOD, EVENTNUM` and a clock-aware hybrid both looked promising on tiny canaries, but a full `1997` rerun regressed badly under the hybrid:
  - `901` rebound deletions
  - `86` `event_stats` errors
- The project conclusion from that experiment was durable:
  - do not apply a global reorder rule
  - prefer local repair rules and overrides

### Late March 12 repair

- `SUB/TIMEOUT ... -> REBOUND -> delayed same-clock MISS` repair landed in the fork
- fork regression subset passed `38/38`
- output dir: `rerun_cautious_parallel_20260312_sub_timeout_pair_v1_1997`
  - `1997`: unchanged best-known result at `41` deletions and `59` `event_stats` errors
- output dir: `rerun_cautious_parallel_20260312_sub_timeout_pair_v1_1998`
  - `1998`: `28,107` rows, `0` failed games, `24` deletions, `2` `event_stats` errors
  - this specifically cleared the long-standing `0049700045` hard failure

## March 13, 2026

This was the densest cleanup day. The project moved from "historical parser can finish seasons" toward "historical parser plus audit can converge toward zero mismatches."

### Workspace-side helpers and audit integration

- `PF` handling was tightened:
  - `Double Fouls` now count inside `PF`
  - `Technical Fouls` and `Defensive 3 Seconds Violations` were tested and deliberately not folded in because they regressed `1997`
- `player_id_normalization.py` landed and was wired into both `generate_darko_hybrid` entrypoints
- The helper repaired bad historical ids using official same-game boxscore rosters plus description text:
  - `775 -> 511` (`Melvin Booker`)
  - `471 -> 1489` (`Lionel Simmons`)
- season scan:
  - `1997`: `21` games changed, `345` repaired rows
  - `1998`: `0` changed rows

### Early March 13 canaries

- `audit_pf_key_canary_20260313_1998_v2`
  - `1998`: `28,107` rows, `24` deletions, `2` `event_stats` errors, `27` mismatch games, `37` mismatch rows
- `audit_player_id_alias_canary_20260313_1997_v2`
  - `1997`: `27,871` rows, `41` deletions, `59` `event_stats` errors, `42` mismatch games, `65` mismatch rows
  - the bogus `775` / `471` output rows were eliminated entirely

### Malformed-event guards

- Missing incoming-player substitutions became no-op substitutions
- Incomplete-lineup event attachment now returns empty base stats instead of indexing into a one-team lineup
- fork regression subset passed `22/22`
- `audit_event_error_canary_20260313_1998_v1`
  - `1998`: `0` failed games, `24` deletions, `0` `event_stats` errors, `27` mismatch games, `37` mismatch rows

### Rebound-order repair sequence

March 13 then became a long series of increasingly narrow fork repairs, each tested on direct canaries and then on `1997` / `1998`.

Key sequence:

- Shadowing TEAM rebound repair:
  - `1997` deletions improved `41 -> 36`
  - `1998` deletions improved `24 -> 17`
  - `0049600063` improved from `6` deletions to `2`
  - `0049700045` improved from `10` deletions to `3`
- Delayed-second-rebound repair:
  - `0029700846` went from `3` deletions to `0`
  - `1998` deletions improved `17 -> 4`
  - `1997` deletions improved `36 -> 31`
- Shooting-foul / free-throw-block rebound repair:
  - `0029700652` became fully clean
  - `1998` deletions improved `4 -> 3`
  - `1997` stayed flat
- `0049700045` rebound-cluster cleanup:
  - direct single-game state became `1` remaining benign deletion and `0` audit mismatches
  - `1998` deletions improved `3 -> 1`
  - `1997` mismatch counts improved slightly without changing deletions
- Legacy foul cleanup plus audit overrides:
  - `boxscore_audit_overrides.csv` support landed in the workspace audit
  - legacy `ELBOW.FOUL` and `PUNCH.FOUL` rows became personal fouls
  - `audit_1998_cleanup_canary_20260313_v1` reached:
    - `0` failed
    - `1` deletion
    - `0` `event_stats` errors
    - `0` team mismatch games
    - `0` player mismatch rows
- Boxscore-source override support:
  - helper: `boxscore_source_overrides.py`
  - regression coverage: `tests/test_boxscore_source_overrides.py`
  - `0029600070` was moved from tolerance exception to production source fix
  - Denver team totals for that game reconciled exactly after the override
- Eventnum-predecessor rebound repair:
  - `0029600585` became fully clean
  - `1997` deletions improved `31 -> 26`
- Delayed player rebound behind FT block:
  - `0029600245` became boxscore-clean except for two TEAM placeholder deletions
  - `1997` deletions improved `26 -> 25`
- Stranded rebound behind future miss:
  - `0029600066` became fully clean
  - `1997` deletions improved `25 -> 22`
- Silent missed-FT rebound normalization:
  - fixed the three real FT-defensive rebounds in `0049600063`
  - `0049600063` became audit-clean, retaining only TEAM placeholder deletions

### March 13 seasonal state before manual cleanup

- latest verified `1997` / `1998` season baseline before the final manual cleanup pass:
  - output dir: `audit_rebound_0029600545_canary_20260313_v1`
  - `1997`: `27,871` rows, `14` deletions, `59` `event_stats` errors
  - `1998`: `28,107` rows, `1` deletion, `0` `event_stats` errors

### Broad safety through 2004

- output dir: `audit_safety_1997_2004_20260313_v1`
- first broad safety sweep showed:
  - `1997` and `1998` were already at promoted best-known levels
  - `1999-2004` were flat-to-better versus older baselines
  - remaining hard failures at that moment were isolated:
    - `0029800661` in `1999`
    - `0020300778` in `2004`
- later same-day safety reruns:
  - `audit_safety_1997_2004_20260313_v2`
  - `audit_rebound_0029600840_canary_20260313_v1`
  - `audit_rebound_0029600085_canary_20260313_v1`
  - `audit_rebound_0029600561_canary_20260313_v1`
  - these tightened more `1997` residue without introducing regressions elsewhere

### Documented source anomalies

Two historical source-side cases became important policy anchors:

- `0029600070`
  - not a parser bug
  - official cached boxscore omitted `Sarunas Marciulionis`
  - historical PBP plus Basketball Reference agreed on the missing Denver row
  - production policy: repair via `boxscore_source_overrides.csv`
- `0029700014`
  - likely a source-data boxscore split anomaly, not a parser bug
  - parser / BBR PBP agreed on the `OREB/DRB` split
  - official NBA / BBR boxscores agreed on a different split
  - production policy: document as source issue, do not prioritize as the next parser fix

### Final March 13 `1997` / `1998` cleanup pass

This was the first full cleanup baseline with integrated audit at zero mismatches, even though a small deletion tail remained.

- Workspace manual hooks now included:
  - `pbp_row_overrides.py` / `pbp_row_overrides.csv`
  - `pbp_stat_overrides.py` / `pbp_stat_overrides.csv`
- Fork-side semantic cleanup also included:
  - same-team self-steals stay on the player’s actual team
  - legacy `"No Turnover"` rows with an explicit credited stealer count as live-ball turnover / steal events
- Output dir: `audit_1997_1998_cleanup_canary_20260313_v1`
  - `1997`:
    - `27,870` rows
    - `0` failed games
    - `9` rebound deletions
    - `59` `event_stats` errors
    - `0` team mismatch games
    - `0` player mismatch rows
  - `1998`:
    - `28,107` rows
    - `0` failed games
    - `1` rebound deletion
    - `0` `event_stats` errors
    - `0` team mismatch games
    - `0` player mismatch rows

## March 14, 2026

March 14 was the day the counting-stat path became clean not just on mismatches, but on `event_stats` errors and then on deletions as well. It also expanded the cleanup season by season through `2020`.

### Malformed-foul guard

- malformed legacy foul rows with `team_id=0` and `player1_id=0` became safe no-op stat rows
- `0029600021` dropped from `22` `event_stats` errors to `0`
- output dir: `audit_event_stats_cleanup_canary_20260314_v1`
  - `1997`: `27,841` rows, `0` failed, `9` deletions, `0` `event_stats` errors, `0` mismatches
  - `1998`: `28,107` rows, `0` failed, `1` deletion, `0` `event_stats` errors, `0` mismatches
- important row-count note:
  - the `1997` row drop from `27,870 -> 27,841` was explained by removal of `29` bogus zero rows from playoff games

### Manual rebound-tail cleanup

- `pbp_row_overrides.csv` was extended to explicitly clean the remaining audit-benign rebound tail:
  - `0029600002`
  - `0029600245`
  - `0029600401`
  - `0029600615`
  - `0029600085`
  - `0049600063`
  - `0049700045`
- Output dirs:
  - `audit_manual_rebound_cleanup_canary_20260314_v1`
  - `audit_manual_rebound_cleanup_1998_safety_20260314_v1`
- Result:
  - `1997`: `27,841` rows, `0` failed, `0` deletions, `0` `event_stats` errors, `0` mismatches
  - `1998`: `28,107` rows, `0` failed, `0` deletions, `0` `event_stats` errors, `0` mismatches

### Promoted `1997` / `1998` clean baselines

- `1997`
  - output dir: `audit_manual_rebound_cleanup_canary_20260314_v1`
  - `27,841` rows, `0` failures, `0` deletions, `0` `event_stats` errors, `0` mismatches
- `1998`
  - output dir: `audit_manual_rebound_cleanup_1998_safety_20260314_v1`
  - `28,107` rows, `0` failures, `0` deletions, `0` `event_stats` errors, `0` mismatches

### Season-by-season cleanup expansion

The project then walked season by season through the remaining historical range.
These entries are intentionally outcome-focused: they preserve the key repair classes, promoted baselines, and policy decisions without reproducing every single intermediate canary from the old long-form log.

#### 1999

- `audit_1999_canary_20260314_v1`
  - `1` failed game
  - `2` deletions
  - `7` mismatch games
  - `8` mismatch rows
  - `1` audit failure
- `0029800661` became a documented source-boxscore repair:
  - cached summary and both local PBP feeds ended `DET 101, NJN 93`
  - cached boxscore summed New Jersey to `97`
  - production policy: trust local PBP plus summary over the bad boxscore row
- Promoted clean baseline:
  - `audit_1999_cleanup_canary_20260314_v1`
  - `17,959` rows, `0` failed, `0` deletions, `0` `event_stats` errors, `0` mismatches

#### 2000

- `audit_2000_canary_20260314_v1`
  - `3` deletions
  - `1` `event_stats` error
  - `10` mismatch games
  - `10` mismatch rows
- key work:
  - malformed-turnover guard
  - source-boxscore repairs such as `0029900712` and `0029901052`
  - row overrides for impossible rebound / FT tails
  - stat overrides for rebound-side source anomalies
- Promoted clean baseline:
  - `audit_2000_cleanup_canary_20260314_v4`
  - `28,557` rows, `0` failed, `0` deletions, `0` `event_stats` errors, `0` mismatches

#### 2001

- `audit_2001_canary_20260314_v1`
  - `5` deletions
  - `2` `event_stats` errors
  - `14` mismatch games
  - `19` mismatch rows
- later blank-substitution guard cleared the malformed substitution tail
- row-drop batch removed the remaining unresolved rebound rows
- stat-override batch restored the final source-backed rebound fixes
- Promoted clean baseline:
  - `audit_2001_cleanup_canary_20260314_v4`
  - `28,134` rows, `0` failed, `0` deletions, `0` `event_stats` errors, `0` mismatches

#### 2002

- `audit_2002_canary_20260314_v1`
  - `4` deletions
  - `8` mismatch games
  - `8` mismatch rows
- residue split cleanly into:
  - four TEAM placeholder rebound drops
  - eight official / BBR-backed player rebound fixes
- Promoted clean baseline:
  - `audit_2002_cleanup_canary_20260314_v2`
  - `28,049` rows, `0` failed, `0` deletions, `0` `event_stats` errors, `0` mismatches

#### 2003

- `audit_2003_canary_20260314_v1`
  - `2` deletions
  - `12` mismatch games
  - `12` mismatch rows
- work:
  - row drops for malformed PBP rebound placeholders
  - one O'Neal FT repair
  - multiple rebound-side BBR-backed stat fixes
- promoted clean baseline:
  - `audit_2003_cleanup_canary_20260314_v3`
- later safety rerun:
  - `audit_2003_cleanup_safety_20260314_v4`
  - stayed fully unchanged and clean
- March 15 follow-up:
  - old Jason Kidd `DeadBallTurnovers +1` override for playoff `0040200116` was removed
  - `audit_2003_kidd_override_removal_canary_20260315_v1` stayed fully clean

#### 2004

- `audit_2004_canary_20260314_v1`
  - `1` failed game
  - `2` deletions
  - `10` mismatch games
  - `10` mismatch rows
  - `1` audit failure
- two pivotal repairs:
  - ambiguous final FT followed by any rebound is treated as missed
  - `0020300778` after-the-buzzer Jeffries three was dropped as a bad raw PBP event
- promoted clean baseline:
  - `audit_2004_cleanup_canary_20260314_v2`
  - `28,301` rows, `0` failed, `0` deletions, `0` `event_stats` errors, `0` mismatches

#### Broad safety through 2004

- `audit_safety_1997_2004_20260314_v2`
- final result:
  - `1997-2004` all clean simultaneously
  - no broad regression from the late `1997-2004` parser and override work

#### 2005-2010

These seasons were then cleaned primarily through manual row-source and stat-source policy rather than new fork logic.

- `2005`
  - promoted clean baseline: `audit_2005_cleanup_canary_20260314_v2`
  - `29,437` rows, `0` failed, `0` deletions, `0` `event_stats` errors, `0` mismatches
- `2006`
  - promoted clean baseline: `audit_2006_cleanup_canary_20260314_v3`
  - `29,447` rows, `0` failed, `0` deletions, `0` `event_stats` errors, `0` mismatches
- `2007`
  - promoted clean baseline: `audit_2007_cleanup_canary_20260314_v2`
  - `29,470` rows, `0` failed, `0` deletions, `0` `event_stats` errors, `0` mismatches
- `2008`
  - promoted clean baseline: `audit_2008_cleanup_canary_20260314_v3`
  - `29,430` rows, `0` failed, `0` deletions, `0` `event_stats` errors, `0` mismatches
- `2009`
  - promoted clean baseline: `audit_2009_cleanup_canary_20260314_v3`
  - `29,166` rows, `0` failed, `0` deletions, `0` `event_stats` errors, `0` mismatches
- `2010`
  - promoted clean baseline: `audit_2010_cleanup_canary_20260314_v3`
  - `29,229` rows, `0` failed, `0` deletions, `0` `event_stats` errors, `0` mismatches

#### 2011-2016

These seasons mainly closed through explicit manual row/stat cleanup plus a few malformed-event guards.

- `2011`
  - promoted clean baseline: `audit_2011_cleanup_canary_20260314_v3`
  - `29,538` rows, `0` failed, `0` deletions, `0` `event_stats` errors, `0` mismatches
- `2012`
  - promoted clean baseline: `audit_2012_cleanup_canary_20260314_v2`
  - `24,759` rows, `0` failed, `0` deletions, `0` `event_stats` errors, `0` mismatches
- `2013`
  - promoted clean baseline: `audit_2013_cleanup_canary_20260314_v3`
  - `30,254` rows, `0` failed, `0` deletions, `0` `event_stats` errors, `0` mismatches
- `2014`
  - promoted clean baseline: `audit_2014_cleanup_canary_20260314_v5`
  - `30,277` rows, `0` failed, `0` deletions, `0` `event_stats` errors, `0` mismatches
- `2015`
  - promoted clean baseline: `audit_2015_cleanup_canary_20260314_v2`
  - `30,396` rows, `0` failed, `0` deletions, `0` `event_stats` errors, `0` mismatches
- `2016`
  - major malformed-tail fixes landed in the fork for substitutions, event factories, and terminal period-start handling
  - output dir: `audit_2016_cleanup_canary_20260314_v4`
  - `30,746` rows, `0` failed, `0` deletions, `0` `event_stats` errors, `0` mismatches

#### 2017-2020

This was the final historical cleanup frontier before the proof/documentation pass.

- `2017`
  - promoted clean baseline: `audit_2017_cleanup_canary_20260315_v1`
  - `30,648` rows, `0` failed, `0` deletions, `0` `event_stats` errors, `0` mismatches
  - the last two survivor rows were closed with audit-only source-anomaly overrides rather than parser changes
- `2018`
  - promoted clean baseline: `audit_2018_cleanup_canary_20260314_v6`
  - `30,586` rows, `0` failed, `0` deletions, `0` `event_stats` errors, `0` mismatches
  - a key semantic rule became explicit here:
    - dead-ball `"No Turnover"` rows count as real turnovers only for `2017-18` and later
- `2019`
  - promoted clean baseline: `audit_2019_cleanup_canary_20260314_v4`
  - `30,636` rows, `0` failed, `0` deletions, `0` `event_stats` errors, `0` mismatches
  - later safety rerun: `audit_2019_final_safety_20260315_v3`
  - stayed fully clean after `2020`-only additions
- `2020`
  - promoted clean baseline: `audit_2020_cleanup_canary_20260315_v3`
  - `26,455` rows, `0` failed, `0` deletions, `0` `event_stats` errors, `0` mismatches
  - key enabling tool:
    - local BBR play-by-play plus `bbr_pbp_lookup.py` as an event-order tiebreaker for the final tail

## March 15, 2026

March 15 was the proof-and-documentation day. The historical cleanup was no longer just "clean"; it became "audited, classified, and frozen."

### BBR play-by-play recheck and override-pruning

- local BBR play-by-play became a practical override-review aid
- helper modules:
  - `bbr_pbp_lookup.py`
  - `bbr_pbp_stats.py`
  - `recheck_overrides_against_bbr_pbp.py`
- targeted recheck snapshot:
  - `bbr_override_recheck_20260315_v3`
  - `boxscore_source_overrides.csv`: `139 / 140` checked cells `all_match`
  - lone survivor: `0029901052` Travis Knight `FTA`, where BBR PBP still carried an orphan FT row even though the BBR boxscore and project semantics supported `0 FTA`

Override-pruning pass:

- `check_pbp_stat_override_necessity.py`
  - full-game necessity pass: `pbp_stat_override_necessity_20260315_v2`
  - `332` rows checked
  - `326` active
  - `6` unsupported
  - `0` redundant
- `check_pbp_row_override_necessity.py`
  - early row-only pass over-pruned too aggressively
  - season safety rerun proved `14` rows still mattered because they prevented fallback deletions even if direct player-box cells did not change
  - confirming canary: `audit_row_override_prune_safety_20260315_v2`
  - upgraded whole-game pass: `pbp_row_override_necessity_20260315_v3`
  - result:
    - `126` games checked
    - `126` active
    - `0` redundant
    - current `pbp_row_overrides.csv` count after pruning: `175`, down from `206`

Durable rule from this pass:

- do not prune row or stat overrides from a narrow necessity report without a season safety rerun that also checks fallback deletions and integrated audit

### Fork rule audit

- concern under review:
  - whether the fork still contained hidden season-specific or game-specific historical hacks that should instead be manual overrides
- finding:
  - custom rebound-order repairs in `pbpstats/offline/processor.py` were pattern-based, not keyed on seasons or game ids
- explicit season-aware logic that remained:
  - `turnover.py` dead-ball `"No Turnover"` gate for `2017-18+`
  - `shot_clock.py` 14-second reset rules logic
- practical conclusion:
  - no historical season-specific rebound-order rule remained in the fork
  - for `1997-2020`, one-off anomalies should prefer manual overrides; fork rules should remain broad recurring patterns or true feed semantics

### Provenance and cross-check additions

These became the durable proof layer for the historical path.

- frozen clean historical baseline manifest:
  - script: `freeze_historical_baseline_manifest.py`
  - output dir: `historical_baseline_manifest_20260315_v1`
  - scope: `1997-2020`
  - result: `24` seasons frozen, no missing clean seasons
- unified override provenance:
  - script: `build_override_provenance_report.py`
  - output dir: `override_provenance_20260315_v1`
  - result:
    - `582` rows total
    - `175 / 175` row overrides active
    - `326` stat overrides active
    - `6` unsupported stat-key rows
- `0021600056` shot-source conflict bundle:
  - script: `audit_0021600056_shot_source_conflict.py`
  - output dir: `audit_0021600056_shot_source_conflict_20260315_v1`
  - result:
    - raw NBA PBP, BBR PBP, and original `tpdev_box.parq` support `Walker`
    - official shots cache, official NBA boxscore, and BBR boxscore support `Batum`
- row-level BBR same-clock audit:
  - script: `check_pbp_row_override_windows_against_bbr.py`
  - output dir: `pbp_row_override_bbr_window_audit_20260315_v1`
  - result:
    - `175` row overrides across `126` games
    - only `2` rows with no matching BBR clock
    - headline counts:
      - `43` `bbr_supports_move_after`
      - `7` `bbr_supports_move_before`
      - `33` `bbr_omits_target_like_event`
      - `21` `bbr_keeps_target_like_event`
      - `25` partial windows
      - `35` inconclusive windows
- override review shortlist:
  - script: `build_override_review_shortlist.py`
  - output dir: `override_review_shortlist_20260315_v1`
  - result:
    - `71` rows across `51` games
- turnover-gate audit:
  - script: `audit_no_turnover_gate.py`
  - full proof output dir: `audit_no_turnover_gate_20260315_v1`
  - impacted scope:
    - `370` qualifying `"No Turnover"` rows
    - `310` games
  - final result:
    - no game improved under either alternate policy
    - the current `2017-18+` gate was strongly justified by affected-game evidence
- final consensus pass:
  - script: `build_override_consensus_report.py`
  - output dir: `override_consensus_20260315_v1`
  - result:
    - `71 / 71` rows classified
    - `0` manual-review rows remaining
    - recommendations:
      - `keep_row_override = 32`
      - `keep_production_override = 15`
      - `keep_production_override_and_document = 18`
      - `keep_audit_override = 6`
- documented conflict register:
  - script: `build_source_conflict_register.py`
  - output dir: `source_conflict_register_20260315_v1`
  - result:
    - `18` documented production conflict rows across `9` games
    - split:
      - `16` box-vs-PBP conflicts
      - `2` shot-source conflicts
- fork repair catalog:
  - script: `build_fork_repair_catalog.py`
  - output dir: `fork_repair_catalog_20260315_v1`
  - result:
    - `28` repair families
    - `23` ordering repairs
    - `2` feed-semantics guards
    - `3` malformed-event guards

### `0021600096` policy shift

- attempted row surgery for `0021600096` was reverted
- BBR PBP kept the `Q3 5:22` Orlando team rebound row
- official boxscore and current parser output agreed on Jimmy Butler’s final line
- original `tpdev_box.parq` did not create a full-consensus case for a manual row drop
- current preferred policy became:
  - keep the generic parser behavior
  - accept one benign fallback deletion
  - avoid a brittle hyper-specific row override
- verified state:
  - `audit_2017_post_21600096_cleanup_20260315_v1`
  - `30,648` rows, `0` failed, `1` deletion, `0` `event_stats` errors, `0` mismatches

### Proof refresh and fork shrink

After the `0021600096` policy clarification, the proof layer was refreshed:

- `override_provenance_20260315_v3`
  - `583` override rows total
  - `176` row overrides
- `override_consensus_20260315_v3`
  - still `71` reviewed, `0` manual-review
- `source_conflict_register_20260315_v3`
  - still `18` documented conflict rows across `9` games
- `pbp_row_override_bbr_window_audit_20260315_v3`
  - `176` row overrides across `127` games
- `fork_repair_catalog_20260315_v2`
  - `30` repair families at that stage
- `fork_repair_usage_20260315_v2`
  - `128` row-override games audited
  - `15` repair families active in current production
  - `4` repair families only active with row overrides stripped out
  - manualization candidates reduced to `3` rules touching `4` games

That candidate set was then tested more aggressively.

- broad instant-replay candidate audit:
  - `fork_repair_usage_pattern2_candidates_20260315_v2`
  - `98` games audited
  - none of them actually needed the narrow instant-replay pattern
- three ultra-narrow processor rules were then removed safely:
  - `processor.m0_785_player_rebound_ahead_of_future_samemteam_missed_ft_placeholder`
  - `processor.1_previous_event_is_sub_timeout_(type_8_or_9)`
  - `processor.2_instant_replay_(type_18)_before_rebound`
- decisive safety canary:
  - `audit_candidate_rule_removal_safety_20260315_v1`
  - `1997`, `2017`, and `2018` all stayed clean, with the only survivor being the preferred benign `0021600096` deletion in `2017`
- refreshed post-shrink artifacts:
  - `fork_repair_catalog_20260315_v3`
    - `27` repair families
  - `fork_repair_disposition_20260315_v3`
    - `0` manual-override candidate rules
    - `0` candidate games
  - `historical_cross_source_summary_20260315_v3`
    - updated to reflect the smaller fork and the absence of remaining evidence-backed manualization candidates

### Later March 15 single-game follow-up

The next pass reviewed the remaining `keep_in_fork_single_game_active` rules one by one.

Confirmed keepers:

- `processor.4_shot_rebound_rebound_(first_rebound_out_of_place)`
  - removing it regressed `1998` and `2020`
- `processor.m0_46_earlier_rebound_stranded_behind_a_shootingmfoul_ft_block`
  - manual-row replacement looked plausible for `0029600085`, but broader fork regressions proved the rule was doing more than that one game
- `processor.m0_455_stacked_samemclock_misses_rebounds_before_opponent_rebound`
  - direct `1998` canary proved it still mattered for `0049700045`
- the `0049600063` cluster
  - disabling the silent-FT or immediate-rebound swap logic regressed `1997`

Conclusion from that pass:

- the earlier fork shrink was the safe one
- the remaining single-game-active branches were not good manual-override conversions with the evidence then available

## March 16, 2026

March 16 created the locked full-history output and then shifted attention to lineup-derived fields.

### Locked full-history output

- output dir:
  - `full_history_1997_2020_20260316_v1`
- command shape:
  - `python cautious_rerun.py --seasons 1997 ... 2020 --output-dir full_history_1997_2020_20260316_v1 --run-boxscore-audit --max-workers 4`
- consolidated parquet:
  - `full_history_1997_2020_20260316_v1/darko_1997_2020.parquet`
  - `685,882` rows
  - `188` columns
- full-run status across `1997-2020`:
  - `0` failed games
  - `0` `event_stats` errors
  - `0` team mismatch games
  - `0` player mismatch rows

Important nuance:

- the locked full-history run was fully clean on failures and integrated audit, but not literally zero-fallback-deletion in every season
- the surviving deletions were all TEAM orphan rebound cleanups and were audit-benign
- seasons with remaining benign TEAM deletions in the locked run:
  - `1999: 1`
  - `2000: 2`
  - `2001: 3`
  - `2002: 4`
  - `2003: 2`
  - `2005: 1`
  - `2007: 1`
  - `2009: 1`
  - `2014: 1`
  - `2015: 1`
  - `2016: 1`
  - `2017: 1`
  - `2019: 9`
  - `2020` remained at `0`

Historical handoff bundle from that point:

- `playbyplayv2.parq`
- `nba_raw.db`
- `full_history_1997_2020_20260316_v1/`
- `fixed_data/raw_input_data/tpdev_data/full_pbp_new.parq`
- `fixed_data/raw_input_data/tpdev_data/tpdev_box.parq`
- `fixed_data/raw_input_data/tpdev_data/tpdev_box_new.parq`
- `fixed_data/raw_input_data/tpdev_data/tpdev_box_cdn.parq`
- `fixed_data/crosswalks/player_master_crosswalk.csv`
- sibling Basketball Reference DB in `../33_wowy_rapm/bbref_boxscores.db`

### Full-history audit against old tpdev output

The long-form March 16 audit against `tpdev_box_new.parq` gave two durable conclusions:

- Counting stats were already a faithful replacement of old tpdev through `2020`
- The important remaining problems had shifted to derived and lineup-based columns

Headline from the audit:

- `596,351` matched player-game rows
- raw counting-stat differences were tiny:
  - generally below `0.15%` of rows by stat
- biggest single counting-stat difference source:
  - `0029600070` (`Sarunas Marciulionis` fix)

Two important derived-column findings:

- `POSS` definition mismatch:
  - old tpdev used two-way `POSS = POSS_OFF + POSS_DEF`
  - new pipeline used one-way `POSS = (POSS_OFF + POSS_DEF) / 2`
- Minutes and Plus_Minus required their own audit pass:
  - most minute diffs were just one-second rounding
  - but `428` rows had period-sized minute errors in the first March 16 comparison

### Minutes / Plus-Minus audit status

New lineup-derived audit tooling landed:

- `audit_minutes_plus_minus.py`
- `build_minutes_cross_source_report.py`
- `audit_period_starters_against_tpdev.py`
- `bbr_boxscore_loader.py`

Confirmed root bug family:

- period-start lineup inference inside the custom `pbpstats` fork
- major failure modes:
  - malformed team ids leaking through `player2_id` / `player3_id`
  - best-effort period starts returning overfull lineups
  - outgoing period-start substitutions not recording first-seen order, so the wrong player got trimmed

Verified fixes and progression:

- targeted sample fixes:
  - `0020100810` Brian Grant: `45.15 -> 33.15` minutes, official `33:08`
  - `0021900291` Rudy Gobert: `40.43 -> 28.43` minutes, official `28:26`
  - `0021900970` Cody Zeller stayed aligned
- `2020` progression:
  - locked baseline: `157` minute outliers over `0.5` minutes, `245` plus-minus mismatches
  - `audit_minutes_fix_2020_20260316_v1`: `55` outliers, `150` plus-minus mismatches
  - Hield / Bjelica period-starter fix: `55 -> 18` minute outliers
- generic local override hook for period starters then landed:
  - `overrides/period_starters_overrides.json`
  - `16` bad game/period/team starter sets seeded from original `full_pbp_new.parq` first-possession lineups

Focused March 16 rerun state:

- `audit_minutes_fix_2017_2020_20260316_v3`
  - `2017`:
    - `0` minute outliers over `0.5` minutes
    - `70` plus-minus mismatch rows
    - `1` benign rebound deletion remains (`0021600096`)
  - `2020`:
    - `0` minute outliers over `0.5` minutes
    - `96` plus-minus mismatch rows
    - `0` rebound deletions

Cross-source minute reports after override pass:

- `minutes_cross_source_2017_20260316_v4/`
- `minutes_cross_source_2020_20260316_v4/`

Key remaining residue:

- no remaining `7-15` second minute gaps
- largest remaining minute diff: `0.05` minutes (`3` seconds)
- residual minute drift is only in the `1-3` second range
- plus-minus remains the main open target

Plus-minus source-agreement read:

- `2017`
  - `70` mismatch rows across `26` games
  - official + BBR agree on `13` rows
  - output + BBR agree on `19` rows
  - takeaway: `2017` still has a real source-split component, not just parser error
- `2020`
  - `96` mismatch rows across `35` games
  - official + BBR agree on `76` rows
  - output + BBR agree on `20` rows
  - takeaway: `2020` plus-minus residue is mostly parser / lineup-attribution error, not a source disagreement

Durable implication from March 16:

- do not treat `full_history_1997_2020_20260316_v1/darko_1997_2020.parquet` as the final perfect baseline for lineup-derived fields
- counting stats are in strong shape
- minutes are materially cleaner and no longer have large outliers
- the main remaining target is plus-minus / same-clock attribution

## Durable Historical Conclusions

By the end of the March 12-16, 2026 arc:

- the historical counting-stat path for `1997-2020` was effectively complete
- the proof layer existed as durable artifacts rather than scattered canaries
- true disagreements had been reframed as documented source conflicts rather than open parser mysteries
- the fork had been shrunk where evidence supported removal
- the remaining active work had moved from counting stats to lineup-derived fields

## Most Important Artifact Families

If you need to revisit the historical counting-stat cleanup, the most important dated artifact families are:

- `historical_baseline_manifest_20260315_v2/`
- `override_provenance_20260315_v3/`
- `override_consensus_20260315_v3/`
- `source_conflict_register_20260315_v3/`
- `pbp_row_override_bbr_window_audit_20260315_v3/`
- `fork_repair_catalog_20260315_v3/`
- `fork_repair_disposition_20260315_v3/`
- `historical_cross_source_summary_20260315_v3/`
- `full_history_1997_2020_20260316_v1/`
- `audit_minutes_fix_2017_2020_20260316_v3/`
- `minutes_cross_source_2017_20260316_v4/`
- `minutes_cross_source_2020_20260316_v4/`

## How To Use This File

- Use `AGENTS.md` for the current project brief and current status.
- Use this file when you need the older project arc, not just the present state.
- For exact per-game or per-canary evidence, fall back to the dated output directories named in this file.
