# Period Starters `v5` / `v6` Schema Notes

This note is for investigation work on `period_starters_v5.parquet`, `period_starters_v6.parquet`, and related files.

## The Big Picture

- `v5` and `v6` are both period-starter fallback sources.
- They are not the same kind of data.
- `v5` is an inferred starter source built from a single `boxscoretraditionalv3` start-window call.
- `v6` is a boundary-activity source built from `gamerotation` stints.
- The runtime loader now checks `v6` first, then `v5`.
- The runtime loader only consumes the shared resolved starter fields.
- Most extra columns are provenance / debugging, not runtime inputs.

## Runtime Truth

The runtime loader is [period_boxscore_source_loader.py](/Users/kmedved/Library/CloudStorage/OneDrive-Personal/github/darko/replace_tpdev/period_boxscore_source_loader.py).

Runtime precedence:

1. `period_starters_v6.parquet`
2. `period_starters_v5.parquet`
3. fork-side `period_starters_overrides.json` logic still remains active for team-level repairs

Practical runtime nuance:

- strict PBP starters are still attempted first
- but when a local exact `v6` row exists and disagrees with strict PBP, the current runtime now prefers the `v6` row
- this is specifically meant to let gamerotation-backed boundary truth beat fragile strict carryover in weak-strict cases

What it actually uses from a resolved parquet row:

- `game_id`
- `period`
- `away_team_id`
- `home_team_id`
- `away_player1` .. `away_player5`
- `home_player1` .. `home_player5`
- optional `resolved` filter

What it ignores:

- `start_range`
- `end_range`
- `window_seconds`
- `resolver_mode`
- all `gr_*` columns
- gaps, anchors, scrape timestamps, etc.

Practical consequence:

- if a row is present and resolved with valid 5-and-5 player ids, runtime can use it
- extra schema differences between `v5` and `v6` do not matter to runtime

## Shared Resolved Schema

Both resolved parquets share this common starter payload:

- `game_id`
- `period`
- `away_team_id`
- `away_tricode`
- `home_team_id`
- `home_tricode`
- `away_player1` .. `away_player5`
- `home_player1` .. `home_player5`
- `start_range`
- `end_range`
- `window_seconds`
- `requested_window_seconds`
- `first_event_elapsed`
- `first_nonzero_event_elapsed`
- `anchor_elapsed`
- `first_sub_elapsed`
- `window_capped_by_sub`
- `total_returned`
- `away_gap`
- `home_gap`
- `min_gap`
- `resolver_mode`
- `resolved`
- `scrape_ts`

Important caution:

- compare starters as per-team sets, not by `player1`/`player2` slot identity
- slot order is not reliable semantic meaning

## `v5` Semantics

Primary file:

- [period_starters_v5.parquet](/Users/kmedved/Library/CloudStorage/OneDrive-Personal/github/darko/replace_tpdev/period_starters_v5.parquet)

Resolver source:

- [scrape_period_starters_v5.py](/Users/kmedved/Library/CloudStorage/OneDrive-Personal/github/darko/replace_tpdev/scrape_period_starters_v5.py)

Rule:

1. Find the first event with elapsed `> 0` excluding substitutions and jump-ball/start/end markers.
2. Anchor a single RT2 request on that point.
3. Request `max(anchor + 5, 20)` seconds.
4. Cap at `first_sub - 1` when needed.
5. Resolve starters from returned seconds with a gap check between players 5 and 6.

Meaning of the timing columns in `v5`:

- `start_range`, `end_range`: the actual RT2 request range in tenths
- `window_seconds`: actual window used
- `requested_window_seconds`: uncapped target window
- `first_event_elapsed`: first non-sub/non-start-end event in the local PBP windowing logic
- `first_nonzero_event_elapsed`: first eligible event with elapsed `> 0`
- `anchor_elapsed`: the event used to anchor the call
- `first_sub_elapsed`: first substitution in the period
- `window_capped_by_sub`: whether the window was truncated by first sub
- `away_gap`, `home_gap`, `min_gap`: separation between players 5 and 6 by returned seconds

Typical `v5` resolver modes:

- `single_call_anchor_plus5_floor20_cap_sub`
- `migrated_from_v4_same_window`

Important interpretation rule:

- a missing `v5` row often means a coverage gap, not disagreement

## `v5` Coverage / Gap Files

Unresolved `v5`:

- [period_starters_unresolved_v5.parquet](/Users/kmedved/Library/CloudStorage/OneDrive-Personal/github/darko/replace_tpdev/period_starters_unresolved_v5.parquet)

Schema is player-return-level, not starter-row-level. Common fields:

- `game_id`, `period`
- `personId`, `name`, `team_id`, `tricode`, `side`
- `minutes`, `seconds`
- `window_seconds`, `requested_window_seconds`
- `first_event_elapsed`, `first_nonzero_event_elapsed`, `anchor_elapsed`, `first_sub_elapsed`
- `window_capped_by_sub`
- `total_returned`
- `reason`
- `resolver_mode`
- `scrape_ts`

Interpretation:

- if a game/period is in unresolved `v5`, `v5` looked but could not produce a clean 5-and-5
- rows may contain returned players, or may be a sentinel row with null player fields and a reason like `window_nonpositive_after_sub_cap`

Failures `v5`:

- [period_starters_failures_v5.parquet](/Users/kmedved/Library/CloudStorage/OneDrive-Personal/github/darko/replace_tpdev/period_starters_failures_v5.parquet)

Fields:

- `game_id`, `period`, `error`, `scrape_ts`

Interpretation:

- fetch / transport / server failure, not a semantic disagreement

Migration / queue file:

- [period_starters_v5_rescrape_queue.parquet](/Users/kmedved/Library/CloudStorage/OneDrive-Personal/github/darko/replace_tpdev/period_starters_v5_rescrape_queue.parquet)

This is the key file for understanding `v5` coverage holes.

Useful fields:

- `game_id`, `period`
- `first_event_elapsed`, `first_nonzero_event_elapsed`, `anchor_elapsed`, `first_sub_elapsed`
- `requested_window_seconds`, `window_seconds`, `window_capped_by_sub`
- `v4_window_seconds`, `v4_resolver_mode`
- `rescrape_reason`
- `was_v4_resolved`, `was_v4_unresolved`, `was_v4_failure`
- `carried_forward`

Interpretation:

- if a game/period is absent from `period_starters_v5.parquet` but present in `period_starters_v5_rescrape_queue.parquet`, that is usually a `v5` coverage gap
- do not treat that as `v5` disagreeing with `v6`

## `v6` Semantics

Primary files:

- [period_starters_v6.parquet](/Users/kmedved/Library/CloudStorage/OneDrive-Personal/github/darko/replace_tpdev/period_starters_v6.parquet)
- [gamerotation_stints_v6.parquet](/Users/kmedved/Library/CloudStorage/OneDrive-Personal/github/darko/replace_tpdev/gamerotation_stints_v6.parquet)

Resolver source:

- [scrape_period_starters_v6.py](/Users/kmedved/Library/CloudStorage/OneDrive-Personal/github/darko/replace_tpdev/scrape_period_starters_v6.py)

Rule:

1. Fetch `gamerotation` once per game.
2. Normalize all returned stints.
3. For each expected local period boundary, mark players active when:
   - `in_time_real <= period_start_tenths < out_time_real`
4. Resolve only when each team has exactly 5 active players.

Meaning of the extra `v6` fields:

- `gr_period_start_tenths`: boundary tested for that period
- `gr_total_stints_returned`: total normalized stints in the game
- `gr_away_active_count`, `gr_home_active_count`: how many players were active at the boundary
- `gr_away_team_city`, `gr_away_team_name`, `gr_home_team_city`, `gr_home_team_name`: metadata from gamerotation

Important interpretation rules:

- `v6` timing fields inherited from the older resolved schema are mostly null and not meaningful for gamerotation resolution:
  - `end_range`
  - `window_seconds`
  - `requested_window_seconds`
  - `first_event_elapsed`
  - `first_nonzero_event_elapsed`
  - `anchor_elapsed`
  - `first_sub_elapsed`
  - `window_capped_by_sub`
  - `away_gap`, `home_gap`, `min_gap`
- `v6` does not use start-window logic
- `v6` disagreement with `v5` often means `v6` has direct boundary truth where `v5` had to infer

## `v6` Unresolved / Failure Files

Unresolved `v6`:

- [period_starters_unresolved_v6.parquet](/Users/kmedved/Library/CloudStorage/OneDrive-Personal/github/darko/replace_tpdev/period_starters_unresolved_v6.parquet)

Schema is boundary-activity-level, not inferred-window-level:

- `game_id`, `period`
- `personId`, `name`, `team_id`, `tricode`, `side`
- `in_time_real`, `out_time_real`
- `active_at_boundary`
- `period_start_tenths`
- `away_active_count`, `home_active_count`
- `total_returned`
- `total_stints_returned`
- `reason`
- `resolver_mode`
- `scrape_ts`

Interpretation:

- if `away_active_count` or `home_active_count` is not exactly `5`, `v6` cannot resolve the period
- this is often a gamerotation data-quality issue, not a starter disagreement

Failures `v6`:

- [period_starters_failures_v6.parquet](/Users/kmedved/Library/CloudStorage/OneDrive-Personal/github/darko/replace_tpdev/period_starters_failures_v6.parquet)

Fields:

- `game_id`, `period`, `error`, `scrape_ts`

Interpretation:

- fetch/server failure only

Stints file:

- [gamerotation_stints_v6.parquet](/Users/kmedved/Library/CloudStorage/OneDrive-Personal/github/darko/replace_tpdev/gamerotation_stints_v6.parquet)

Fields:

- `game_id`
- `side`
- `team_id`, `tricode`, `team_city`, `team_name`
- `personId`
- `player_first`, `player_last`, `name`
- `in_time_real`, `out_time_real`
- `stint_tenths`
- `player_pts`, `pt_diff`, `usg_pct`
- `scrape_ts`

Interpretation:

- this is the raw normalized boundary source behind `v6`
- when `v6` is unresolved, this is the best file to inspect
- it is also useful for broader investigation work beyond period starters:
  - full-game lineup occupancy
  - boundary-state questions
  - “who was plausibly active by this dead ball / possession boundary?” questions
- do not treat it as definitive truth on its own
- it can still be incomplete, malformed, or internally inconsistent for some games, so it should be used as strong evidence alongside local PBP, `tpdev_pbp`, and other audits rather than as an unquestioned overwrite source

## Practical Comparison Rules

When comparing `v5` and `v6`, use this order:

1. Check whether the game/period exists in `v5`.
2. If not, check `period_starters_unresolved_v5.parquet`, `period_starters_failures_v5.parquet`, and especially `period_starters_v5_rescrape_queue.parquet`.
3. Check whether the game/period exists in `v6`.
4. If not, check `period_starters_unresolved_v6.parquet`, `period_starters_failures_v6.parquet`, and then `gamerotation_stints_v6.parquet`.

Do not say:

- "`v5` disagrees with `v6`" when `v5` has no row and the period is still queued / unresolved
- "`v6` is wrong" when `v6` is unresolved because gamerotation returned `4/5`, `6/5`, etc.

Better language:

- "`v5` has no resolved row for this period; this is a v5 coverage gap"
- "`v6` has a resolved row and v5 does not"
- "`v6` is unresolved because gamerotation active counts are not 5-and-5"

## How To Use Them In Investigation

For starter-boundary questions:

- prefer `v6` when it has a resolved row
- use `v5` as the active runtime comparison source
- compare current parser vs `tpdev_pbp` starter sets
- compare `v5` and `v6` as sets per team, not as slot order

For missing-row questions:

- `v5`:
  - check `period_starters_v5_rescrape_queue.parquet`
- `v6`:
  - check `period_starters_unresolved_v6.parquet`
  - then inspect `gamerotation_stints_v6.parquet`

For runtime implications:

- current runtime default is `v5`
- a resolved `v6` row is strong evidence even though runtime does not use it by default yet
- a local override is still needed when:
  - `v5` misses the period
  - and `v6` is also unresolved or absent

## Common Investigation Mistakes

- Treating `player1..5` order as meaningful.
- Treating a missing `v5` row as a disagreement instead of a coverage hole.
- Using `v6` null `window_seconds` / `anchor_elapsed` fields as if they mean anything.
- Ignoring `period_starters_v5_rescrape_queue.parquet`.
- Ignoring `period_starters_unresolved_v6.parquet`.
- Forgetting that runtime only needs the shared resolved starter payload.
