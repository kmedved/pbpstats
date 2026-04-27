# Codex Handoff: Rebound/Sub-Block Repair

Date: 2026-04-24

## Executive State

Phase 1 is implemented in the local `pbpstats` fork and is committed/pushed on `main`.

The bug fixed here is the `REPAIR_OVERSHOOTS_SUB` / rebound-survivor family: a player is subbed in between free throws, then gets the defensive rebound, but the repair layer leaves the old player on court or hoists the rebound across the substitution block.

This clears the seven intended Phase 1 targets:

- `0041900155` E353 Harrell rebound after Harrell-for-Zubac.
- `0021900920` E312 Tolliver rebound after double sub.
- `0021900487` E246 Jaren Jackson Jr. rebound after triple sub.
- `0021900419` E258 Harkless rebound after sub.
- `0021900333` E659 Cameron Johnson rebound after double sub.
- `0021900201` E398 Noel rebound after sub.
- `0029600204` E153 Pippen rebound after quadruple sub.

It does not attempt to fix scorer-sub, post-sub stat-credit, instant-replay reordering, period-start contradictions, missing-sub data loss, scrambled 1990s PBP, or source-split minute tails.

## Working Directories

- Main project: `/Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev`
- Local `pbpstats` fork with patch: `/Users/konstantinmedvedovsky/migrate_tpdev/pbpstats`
- Original open-game bundle: `/Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev/open_game_pbp_bundle_20260423_v1`
- The original Claude plan was reviewed before implementation, but it was not copied; this handoff includes the relevant implementation details.

## Current Git State

In `/Users/konstantinmedvedovsky/migrate_tpdev/pbpstats`:

```text
## main...origin/main
61232b4 Fix FT rebound sub-block repair
worktree clean
```

Original patch diff stat:

```text
pbpstats/offline/processor.py | 198 ++++++++++++++++++++++++++++++++++++++++++
tests/test_offline_repair.py  | 110 +++++++++++++++++++++++
2 files changed, 308 insertions(+)
```

## Code Changes

File: `/Users/konstantinmedvedovsky/migrate_tpdev/pbpstats/pbpstats/offline/processor.py`

The patch adds two narrow pre-pass repairs inside `_repair_silent_ft_rebound_windows()`:

1. Two-shot terminal FT shape:

```text
FT1_A -> MISS_FT2_A -> REBOUND_B -> SUB(S)_B
or
FT1_A -> MISS_FT2_A -> SUB(S)_B -> REBOUND_B
```

If the substitution event numbers prove the subs belong before the terminal missed FT, and one substitution brings the rebounder onto the floor, the sub rows are moved before the missed terminal FT.

2. One-shot FT shape:

```text
MISS_FT_A -> SUB(S)_B -> REBOUND_B
```

If the contiguous sub block contains the rebounder as `PLAYER2_ID`, it is moved before the missed FT. This is what clears `0029600204` E153 Pippen.

The match is deliberately conservative:

- same period
- real player rebound, not team rebound
- FT team and rebound team must differ
- rebounder must be a `PLAYER2_ID` in the sub block
- sub rows must share the FT clock
- rebound clock must be within 5 seconds after the FT
- two-shot shape requires `rebound_event_num == terminal_ft_event_num + 1`

The patch also adjusts `_fix_event_order()` pattern `-0.895` so that when the fallback sees a sub block that brings the rebounder onto the floor, it moves the sub block before the missed FT instead of dragging the rebound backward over the substitutions.

Claude noted two acceptable limitations:

- The `terminal_ft_event_num + 1` constraint means an instant replay or other inserted row between the miss and rebound makes the fix decline to fire. That is intentional and safe for Phase 1.
- `rebound_clock_follows_ft()` returns `True` if either clock is unparseable. This is acceptable for the canary shapes because the event-num, team, and subbed-in-rebounder constraints still do the real gating. A stricter future cleanup could return `False`.

## Tests Added

File: `/Users/konstantinmedvedovsky/migrate_tpdev/pbpstats/tests/test_offline_repair.py`

Added:

- `_stats_sub()` helper.
- `test_fix_event_order_moves_subbed_in_rebounder_block_before_missed_ft`
- `test_repair_silent_ft_rebound_windows_moves_subs_before_terminal_ft_for_subbed_in_rebounder`
- `test_repair_silent_ft_rebound_windows_moves_sub_block_before_live_one_shot_ft`
- `test_repair_silent_ft_rebound_windows_leaves_sub_block_when_rebounder_not_subbed_in`

## Validation Already Run

Use the DARKO env on this machine:

```bash
cd /Users/konstantinmedvedovsky/migrate_tpdev/pbpstats
/opt/anaconda3/envs/darko311/bin/python -m pytest tests/test_offline_repair.py -q
```

Result:

```text
41 passed
```

Available local test subset:

```bash
cd /Users/konstantinmedvedovsky/migrate_tpdev/pbpstats
/opt/anaconda3/envs/darko311/bin/python -m pytest -q \
  tests/test_replace_tpdev_compatibility_smoke.py \
  tests/test_stats_nba_malformed_events.py \
  tests/test_offline_ordering.py \
  tests/test_period_starters_carryover.py \
  tests/test_lineup_window_overrides.py \
  tests/test_shot_clock.py \
  tests/test_offline_boxscore_loader.py \
  tests/test_offline_repair.py \
  tests/test_team_on_court_stats.py \
  tests/test_intraperiod_lineup_repair.py \
  tests/test_client.py
```

Result:

```text
136 passed
```

Processor lint:

```bash
cd /Users/konstantinmedvedovsky/migrate_tpdev/pbpstats
/opt/anaconda3/envs/darko311/bin/python -m ruff check pbpstats/offline/processor.py
```

Result:

```text
All checks passed.
```

Whitespace:

```bash
cd /Users/konstantinmedvedovsky/migrate_tpdev/pbpstats
git diff --check
```

Result: clean.

Full `pytest -q` was attempted but does not collect in this env because dependencies are missing:

- `responses`
- `tiktoken`

## Live Audit Artifacts

Seven-game Phase 1 canary audit:

```text
/Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev/_tmp_phase1_rebound_sub_fix_validation_20260423_final
```

Summary:

```json
{
  "games": 7,
  "issue_rows": 1,
  "issue_games": 1,
  "status_counts": {
    "off_court_event_credit": 1
  }
}
```

The one remaining row is out of scope:

```text
0029600204 E339 Harper P.FOUL, scorer-sub / scrambled-order family
```

Open-30 event audit:

```text
/Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev/_tmp_phase1_rebound_sub_fix_open30_audit_20260423_final
```

Summary:

```json
{
  "games": 30,
  "issue_rows": 47,
  "issue_games": 18,
  "status_counts": {
    "off_court_event_credit": 44,
    "sub_out_player_missing_from_previous_lineup": 3
  }
}
```

Important: `0` Phase 1 target rows remain in this open-30 audit.

Canary minutes/plus-minus audit:

```text
/Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev/_tmp_phase1_rebound_sub_fix_minutes_pm_canaries_20260423
```

This still shows known non-Phase-1 plus-minus and old-tail behavior, especially `0029600204` Ron Harper minute tail. Do not interpret this as a failure of the rebound/sub-block patch.

## Commands To Reproduce Live Audits

Seven canaries:

```bash
cd /Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev
PYTHONPATH=/Users/konstantinmedvedovsky/migrate_tpdev/pbpstats:/Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev \
PBPSTATS_REPO=/Users/konstantinmedvedovsky/migrate_tpdev/pbpstats \
/opt/anaconda3/envs/darko311/bin/python audit_event_player_on_court.py \
  --game-ids 0041900155 0021900920 0021900487 0021900419 0021900333 0021900201 0029600204 \
  --output-dir /Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev/_tmp_phase1_rebound_sub_fix_validation_YYYYMMDD
```

Open 30:

```bash
cd /Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev
PYTHONPATH=/Users/konstantinmedvedovsky/migrate_tpdev/pbpstats:/Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev \
PBPSTATS_REPO=/Users/konstantinmedvedovsky/migrate_tpdev/pbpstats \
/opt/anaconda3/envs/darko311/bin/python audit_event_player_on_court.py \
  --game-ids \
  0041900155 0021900920 0021900487 0021900419 0021900333 0021900201 \
  0021801067 0021800484 0021700917 0021700514 0021700394 0021700377 \
  0021700337 0021700236 0021300593 0020900189 0020400335 0020000628 \
  0029701075 0029700159 0049600063 0029601163 0029600657 0029600585 \
  0029600370 0029600332 0029600204 0029600175 0029600171 0029600070 \
  --output-dir /Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev/_tmp_phase1_rebound_sub_fix_open30_audit_YYYYMMDD
```

## Remaining Work

Recommended next step: rebuild the production outputs with the patched `pbpstats`, then rerun the full audit and regenerate/update the open-game documentation from the new state.

After that, Phase 2 should probably target scorer-sub / post-sub stat-credit cases:

- `POST_SUB_STAT_CREDIT`
  - `0021801067`
  - `0021800484`
  - `0021700917`
  - `0049600063`
- `SCORER_SUB`
  - `0021700377`
  - `0020000628`
  - `0029600585`
  - `0029600204` E339 side

Keep these separate from Phase 1. They are mirror-image ordering problems: the credited foul or technical FT lives on the wrong side of a substitution row. A rebound-specific fix should not be stretched to cover them.

Known out-of-scope / likely documented holds:

- `INSTANT_REPLAY_REORDERING`: `0021700337`, `0021700236`
- `PERIOD_START_CONTRADICTION`: `0021300593`, `0020900189`
- `MISSING_SUB_IN`: `0020400335`, `0029600171`
- `SCRAMBLED_PBP`: `0029701075`
- `CATASTROPHIC_ROTATION_BREAK`: `0029600370`, `0029600332`
- `DUPLICATE_SUB`: `0029600657`
- `MINUTE_TAIL_ONLY`: `0029601163`, `0029600070`, `0029600175`
- `MINUTE_TRADEOFF_HOLD`: `0029700159`
- `ACCUMULATOR_RESIDUE_NONLOCAL`: `0021700394`

## Practical Cautions

- Do not assume all 30 games are fixed. Only the rebound/sub-block family is fixed.
- Do not manually clear holds until production outputs are rebuilt and audited with this patch.
- If another machine has a different checkout path, update `PYTHONPATH` and `PBPSTATS_REPO` so the scripts import the patched local `pbpstats`, not an installed package.
- If the full `pbpstats` test suite is desired, install the missing test dependencies first (`responses`, `tiktoken`) or use an env that already has them.
- The `pbpstats` patch is committed/pushed as `61232b4 Fix FT rebound sub-block repair`; verify that commit after copying or recloning.
