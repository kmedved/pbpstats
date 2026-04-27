# pbpstats Fork Audit - 2026-03-17

- Fork repo: `/Users/kmedved/Documents/GitHub/pbpstats`
- Checked commit: `95b7ed42a2c4a78a705d31191ed3365d3ff748ff`
- Active offline runtime: `replace_tpdev/0c2_build_tpdev_box_stats_version_v9b.py:701-706` and `replace_tpdev/0c2_build_tpdev_box_stats_version_v9b.py:1083-1088`, typically wrapped by `replace_tpdev/cautious_rerun.py:68-82`
- AGENTS policy reminder: `playbyplayv2.parq` is the baseline historical chronology; cached `pbpv3` is enrichment / repair input, not canonical row authority

## Confirmed Findings

| ID | Prio | Category | Subsystem | Active-path impact | File / lines | Finding | Evidence |
| --- | --- | --- | --- | --- | --- | --- | --- |
| CF-01 | P1 | overly complicated / incorrect | Offline ordering | Active offline historical runtime | `pbpstats/offline/ordering.py:48-78`; runtime call site `replace_tpdev/0c2_build_tpdev_box_stats_version_v9b.py:701-706` | `dedupe_with_v3()` treats `pbpv3` as row-authority and drops every `pbpv2` row whose `EVENTNUM` is absent from v3 before override and repair layers run. | call-site proof |
| CF-02 | P1 | overly complicated / incorrect | Period starters | Active offline historical runtime | `pbpstats/resources/enhanced_pbp/stats_nba/start_of_period.py:39-45`; injection path `replace_tpdev/cautious_rerun.py:71-82` | When `boxscore_source_loader` is present, the offline path falls back to `_get_period_starters_from_period_events(..., ignore_missing_starters=True)` and silently accepts incomplete lineups instead of failing. | call-site proof |
| CF-03 | P1 | other mistake | Period starters | Active offline historical runtime | `pbpstats/resources/enhanced_pbp/start_of_period.py:489-526` | `_check_both_teams_have_5_starters()` never verifies that two teams exist. A single detected team with 5 starters passes strict validation unchanged. | tiny reproducer |
| CF-04 | P2 | other mistake | Period starters | Active offline historical runtime | `pbpstats/resources/enhanced_pbp/stats_nba/start_of_period.py:71-86` | `_get_period_starters_from_boxscore_loader()` accepts period-1 starters as long as every present team has 5 players; it does not require both teams to be present before returning. | code inspection |
| CF-05 | P2 | other mistake | Period starters | Active offline historical runtime | `pbpstats/resources/enhanced_pbp/start_of_period.py:614-619` | Carryover repair skips the zero-starter case entirely, even though that is the exact situation where previous-period lineups are most valuable. | code inspection |
| CF-06 | P1 | no-op | Event aggregation | Active offline historical runtime | `pbpstats/resources/possessions/possessions.py:33-62` | `Possessions._aggregate_event_stats()` now swallows any exception from `event.event_stats`, logs a warning, and drops the event's stats from the aggregate instead of failing the game. | tiny reproducer |
| CF-07 | P1 | overly complicated / incorrect | Field goal stats | Indirect active offline via broken lineups | `pbpstats/resources/enhanced_pbp/field_goal.py:388-410` | `FieldGoal.event_stats()` returns only `base_stats` plus distance/heave stats when opponent lineup context is incomplete, which drops the play's core FGA/FGM/3PA/3PM-style stat emission. | code inspection |
| CF-08 | P2 | overly complicated / incorrect | Free throw attribution | Indirect active offline via same-clock / lineup issues | `pbpstats/resources/enhanced_pbp/free_throw.py:311-337` | `event_for_efficiency_stats` falls back to the FT event itself when the foul cannot be found, silently assigning lineup-based FT stats to the FT-time lineup instead of the foul-time lineup. | code inspection |
| CF-09 | P2 | no-op | Free throw stats | Indirect active offline via broken lineups | `pbpstats/resources/enhanced_pbp/free_throw.py:426-431` | `FreeThrow.event_stats()` has partial-return branches on incomplete lineup context, silently omitting defender/opponent FT on-court stats and some lineup-linked output instead of failing loudly. | code inspection |
| CF-10 | P2 | no-op | Substitutions | Active offline historical runtime | `pbpstats/resources/enhanced_pbp/stats_nba/substitution.py:35-47` | Missing incoming-player rows become self-substitutions because `incoming_player_id` falls back to `player1_id`, turning a malformed substitution into a silent lineup no-op. | code inspection |
| CF-11 | P2 | no-op | Substitutions | Active offline historical runtime | `pbpstats/resources/enhanced_pbp/stats_nba/substitution.py:24-32` | Blank substitution placeholders preserve the previous lineup unchanged rather than surfacing a malformed-row failure. | code inspection |
| CF-12 | P2 | no-op | Substitutions | Active offline historical runtime | `pbpstats/resources/enhanced_pbp/substitution.py:28-32` | If the prior lineup context is missing the substitution's team entirely, `_raw_current_players` leaves the substitution unapplied and continues with the prior lineup. | code inspection |
| CF-13 | P2 | other mistake | Turnover stats | Indirect active offline via CF-06 | `pbpstats/resources/enhanced_pbp/turnover.py:94-98`; `pbpstats/resources/enhanced_pbp/turnover.py:222-228` | `Turnover.event_stats()` still assumes `current_players` contains exactly two teams and will raise on incomplete lineup context; CF-06 then turns that exception into silent stat loss. | code inspection |
| CF-14 | P2 | other mistake | Foul stats | Indirect active offline via CF-06 | `pbpstats/resources/enhanced_pbp/foul.py:238-292` | `Foul.event_stats()` has the same two-team indexing assumption and can fail on incomplete lineup state, after which aggregation now skips the event. | code inspection |
| CF-15 | P2 | other mistake | Violation stats | Indirect active offline via CF-06 | `pbpstats/resources/enhanced_pbp/violation.py:50-58` | `Violation.event_stats()` also assumes exactly two teams are present in `current_players`, so malformed lineup context can now erase violation stats silently through CF-06. | code inspection |
| CF-16 | P1 | other mistake | Direct StatsNBA rebound repair | Direct StatsNBA loader path | `pbpstats/data_loader/stats_nba/enhanced_pbp/loader.py:146-167` | `_check_rebound_event_order()` swallows both `EventOrderError` and generic exceptions after trying v3 ordering, so the last failure path no longer raises and can continue on invalid chronology. | code inspection |
| CF-17 | P2 | cleanup | Direct StatsNBA fallback | Direct StatsNBA loader path | `pbpstats/data_loader/stats_nba/enhanced_pbp/loader.py:526-539` | `_use_data_nba_event_order()` is still a live debug stub that prints and raises `RuntimeError` instead of providing a real fallback. | code inspection |
| CF-18 | P1 | overly complicated / incorrect | Direct StatsNBA rebound repair | Direct StatsNBA loader path | `pbpstats/data_loader/stats_nba/enhanced_pbp/loader.py:352-381` | `_fix_common_event_order_error()` deletes the later adjacent rebound whenever both candidates look like the same kind of rebound, which can delete a real event without any supporting source evidence. | code inspection |
| CF-19 | P3 | cleanup | Offline ordering | Active offline historical runtime | `pbpstats/offline/ordering.py:219-238` | `reorder_with_v3()` is an advertised v3 reorder stage that ignores both `game_id` and `fetch_pbp_v3_fn` and only re-coerces `EVENTNUM`, making the active offline flow harder to audit. | code inspection |
| CF-20 | P1 | overly complicated / incorrect | Starter inference / substitutions | Active offline historical runtime | `pbpstats/resources/enhanced_pbp/start_of_period.py:326-345`; `pbpstats/resources/enhanced_pbp/stats_nba/substitution.py:35-47` | A substitution row with no incoming player turns the outgoing player into the inferred `incoming_player_id`, so starter inference classifies that player as `subbed_in` and removes them from the starter candidate pool. | tiny reproducer |
| CF-21 | P1 | other mistake | Starter inference | Active offline historical runtime | `pbpstats/resources/enhanced_pbp/start_of_period.py:317-320`; `pbpstats/resources/enhanced_pbp/start_of_period.py:380-406` | `player2_id` and `player3_id` are only scanned for starter evidence when `player1_id` is already a valid starter candidate, so malformed rows with bad `player1_id` silently discard otherwise-usable starter evidence. | tiny reproducer |
| CF-22 | P1 | other mistake | Starter inference | Active offline historical runtime | `pbpstats/resources/enhanced_pbp/start_of_period.py:423-445` | `_split_up_starters_by_team()` discards all dangling starters unless there is exactly one dangling player and exactly 10 total starter candidates. Multiple player2/player3-only starters simply vanish from the inferred lineups. | tiny reproducer |
| CF-23 | P1 | no-op | Substitutions | Active offline historical runtime | `pbpstats/resources/enhanced_pbp/substitution.py:23-37` | If the outgoing player is not actually present in the current lineup, the substitution silently leaves the lineup unchanged instead of failing or applying a repair. | tiny reproducer |
| CF-24 | P1 | no-op | Lineup propagation | Active offline historical runtime | `pbpstats/resources/enhanced_pbp/enhanced_pbp_item.py:111-142` | `_get_previous_raw_players()` catches any lineup-chain error from the previous event and returns `{}`. A single broken `current_players` lookup can therefore wipe lineup context for downstream events in the period. | tiny reproducer |
| CF-25 | P1 | no-op | Minutes played / stint accounting | Active offline historical runtime | `pbpstats/resources/enhanced_pbp/enhanced_pbp_item.py:312-371` | `_get_seconds_played_stats_items()` emits no seconds-played stats whenever either the current or previous lineup has fewer than two teams, so minute accounting silently drops intervals once lineup state degrades. | code inspection |
| CF-26 | P2 | no-op | On/off possession accounting | Active offline historical runtime | `pbpstats/resources/enhanced_pbp/enhanced_pbp_item.py:373-428` | `_get_possessions_played_stats_items()` has the same incomplete-lineup early return, so on/off possession stats vanish whenever `current_players` is not a two-team lineup. | code inspection |
| CF-27 | P1 | other mistake | Starter inference | Active offline historical runtime | `pbpstats/resources/enhanced_pbp/start_of_period.py:361-368` | Period-start technical free throws are excluded from starter inference only when the clock string is exactly `"12:00"`, so overtime (`"5:00"`) and WNBA (`"10:00"`) technical FTs leak into the starter pool. | tiny reproducer |
| CF-28 | P1 | overly complicated / incorrect | Starter inference / substitutions | Active offline historical runtime | `pbpstats/resources/enhanced_pbp/start_of_period.py:409-419` | The late sub-in demotion repair only fires when the explicit sub happens earlier in game time than the player's first action. Equal-timestamp score/sub clusters leave the player in `starters` even when the feed later shows them entering at that same clock. | tiny reproducer |
| CF-29 | P1 | other mistake | Substitutions | Active offline historical runtime | `pbpstats/resources/enhanced_pbp/substitution.py:23-37` | If a malformed substitution names an incoming player who is already on the floor, `_raw_current_players` duplicates that player in the lineup and drops the outgoing player without any uniqueness or size validation. | tiny reproducer |
| CF-30 | P2 | other mistake | Period starters | Active offline historical runtime | `pbpstats/resources/enhanced_pbp/start_of_period.py:552-567` | `_get_period_start_substitutions()` stops scanning as soon as it encounters the first event below the period-start clock, so a later same-period `12:00` / `5:00` substitution is missed entirely when row order is malformed. | tiny reproducer |
| CF-31 | P1 | other mistake | Minutes played / stint accounting | Active offline historical runtime | `pbpstats/resources/enhanced_pbp/enhanced_pbp_item.py:213-228`; `pbpstats/resources/enhanced_pbp/enhanced_pbp_item.py:329-368` | `seconds_since_previous_event` can go negative on same-period clock reversals, and `_get_seconds_played_stats_items()` writes that negative value straight into `SecondsPlayed*` stats. | tiny reproducer |
| CF-32 | P1 | other mistake | Offline ordering / period starts | Active offline historical runtime | `pbpstats/offline/ordering.py:81-116`; `pbpstats/offline/ordering.py:207-214`; runtime call site `replace_tpdev/0c2_build_tpdev_box_stats_version_v9b.py:1994` | Synthetic StartOfPeriod rows always use `"12:00"`, so missing overtime starts are inserted with regulation clocks instead of `"5:00"`, which can distort OT stint starts and any start-of-period logic keyed off the clock. | tiny reproducer |
| CF-33 | P2 | no-op | Period starters / local boxscore source | Active offline historical runtime | `pbpstats/resources/enhanced_pbp/stats_nba/start_of_period.py:62-65`; `pbpstats/resources/enhanced_pbp/stats_nba/start_of_period.py:39-45` | Any exception while constructing the local boxscore loader is swallowed and treated like “no local starter source,” silently downgrading the active offline path to best-effort PBP starter inference. | code inspection |
| CF-34 | P1 | other mistake | Malformed-row normalization / starter inference | Active offline historical runtime | `pbpstats/resources/enhanced_pbp/stats_nba/enhanced_pbp_item.py:68-76`; `pbpstats/resources/enhanced_pbp/start_of_period.py:194-224` | When `PLAYER1_TEAM_ID` is missing on a non-replay row, the stats.nba item initializer rewrites `team_id = PLAYER1_ID` and zeroes out `player1_id`. That turns a real player into a fake team id, poisons `known_team_ids`, and erases starter evidence from that row. | tiny reproducer |
| CF-35 | P1 | overly complicated / incorrect | Field goal on/off attribution | Active offline historical runtime | `pbpstats/resources/enhanced_pbp/field_goal.py:412-563` | Made field goals use `self.current_players` directly for plus-minus and on-court TEAM/OPP FGA-style stats. A same-clock substitution before or after the shot therefore flips the credited five purely based on raw row order. | tiny reproducer |
| CF-36 | P1 | overly complicated / incorrect | Free throw on/off attribution | Active offline historical runtime | `pbpstats/resources/enhanced_pbp/free_throw.py:311-358`; `pbpstats/resources/enhanced_pbp/free_throw.py:405-430` | When a foul is logged after the FT and a same-clock substitution sits between them, `event_for_efficiency_stats` resolves to the post-sub foul event, so FT plus-minus and on-court FT stats are credited to the post-sub lineup instead of the foul-time lineup. | tiny reproducer |
| CF-37 | P3 | other mistake | Legacy web starter fallback | Legacy web/no-local-boxscore path only | `pbpstats/resources/enhanced_pbp/start_of_period.py:143-145` | `_get_starters_from_boxscore_request()` sorts candidate starters by `int(MIN.split(':')[1])`, i.e. seconds only. A `0:59` row therefore outranks `2:15`, so the web boxscore fallback can misidentify starters whenever the first-event window exceeds one minute. | tiny reproducer |
| CF-38 | P1 | overly complicated / incorrect | Period starters / carryover fill | Active offline historical runtime | `pbpstats/resources/enhanced_pbp/start_of_period.py:592-645` | `_fill_missing_starters_from_previous_period_end()` is all-or-nothing: once any detected starter is not in the previous-period ending five and no exact period-start sub row exists, the `implied_carryover.issubset(prev_set)` guard blocks all backfill. In the live historical runtime this leaves teams stuck at 3-4 starters even when the missing tpdev starter is simply a no-event carryover player. | call-site proof |
| CF-39 | P1 | overly complicated / incorrect | Period starters / best-effort trim | Active offline historical runtime | `pbpstats/resources/enhanced_pbp/start_of_period.py:268-301` | `_trim_excess_starters()` ranks candidates by raw first-seen order relative to the team's first substitution. When a sub-in player's rebound/shot is logged before the same-clock substitution row, the best-effort path keeps the sub-in and drops the true starter. | call-site proof |
| CF-40 | P1 | no-op | Local period-1 starter fallback integration | Active offline historical runtime | `replace_tpdev/cautious_rerun.py:21-26`; `pbpstats/resources/enhanced_pbp/stats_nba/start_of_period.py:50-65`; `pbpstats/data_loader/stats_nba/boxscore/loader.py:33-36` | The injected `_BoxscoreSourceLoader.load_data()` takes no `game_id`, but `StatsNbaBoxscoreLoader` calls `source_loader.load_data(self.game_id)`. That `TypeError` is swallowed in `_get_period_starters_from_boxscore_loader()`, so the local period-1 starter fallback is silently disabled even when the cached boxscore has correct `START_POSITION` data. | tiny reproducer |
| CF-41 | P1 | overly complicated / incorrect | Period starters / wrong carryover replacement | Active offline historical runtime | `pbpstats/resources/enhanced_pbp/start_of_period.py:633-641` | When direct starter inference finds only `3-4` players and the true missing starter is a silent period-start change who was not on the previous period's ending lineup, carryover fill blindly pulls the remaining slots from `prev_players`. That can produce a full but wrong five, masking the real missing starter instead of surfacing the ambiguity. | call-site proof |

## Reproducer Notes

Two tiny isolated checks were run against the fork while building this register:

```text
StartOfPeriod._check_both_teams_have_5_starters(...) returned_without_exception
{1610612737: [1, 2, 3, 4, 5]}

Possessions([BadEvent]).team_stats -> []
after logging:
"Skipping stats for event <BadEvent> (game_id=0029700001) ..."

OT_TECH_FT_STARTERS [999] {999: 100}
SAME_CLOCK_DEMOTION [9, 5] []
DUP_INCOMING_LINEUP {100: [1, 2, 3, 4, 1], 200: [11, 12, 13, 14, 15]}
MISSED_PERIOD_START_SUBS {}
NEGATIVE_SECONDS -10.0
PATCHED_OT_START_ROW {'PERIOD': 5, 'EVENTMSGTYPE': 12, 'PCTIMESTRING': '12:00', 'EVENTNUM': 199}
FOUL_ATTRS {'team_id': 123, 'player1_id': 0, 'player3_id': None}
KNOWN_TEAMS {123}
STARTER_SCAN ([], {}, {}, [])
CURRENT_PLAYERS_AFTER_EXCEPTION {}
SUB_OUTGOING_NOT_ON_FLOOR {100: [1, 2, 3, 4, 5], 200: [11, 12, 13, 14, 15]}
FG_PM_AFTER_SUB [1, 2, 3, 4, 6]
FG_PM_BEFORE_SUB [1, 2, 3, 4, 5]
FT_PM_FOUL_AFTER_SUB [1, 2, 3, 4, 6]
FT_EFFICIENCY_EVENT StatsFoul [1, 2, 3, 4, 6]
BOXSCORE_SORT_SECONDS_ONLY [(1, '0:59'), (2, '2:15'), (3, '2:14')]
```

## Fresh Verification Sweep

- `1997` same-clock plus-minus rechecks:
  - Fresh traces were rerun for `0029600062`, `0029600370`, `0029600386`, and `0029600478` into `theory_test_same_clock_1997_20260317_v1/`.
  - Same-clock scoring/substitution windows were present in all four games, but a stronger verifier comparing each scoring event's credited lineup against the lineup *before the same-clock window began* found `0` mismatch-player on/off flips in these samples.
  - Result: the same-clock FG/FT attribution bug remains real at code-path level, but it is **not yet game-backed as the dominant cause** in these earliest sampled `1997` residue games.

- `1997-1999` period-starter oracle gap:
  - `audit_period_starters_against_tpdev.py` cannot externally verify `1997-1999` starter-complex games with tpdev possession starters because sibling `fixed_data/raw_input_data/tpdev_data/full_pbp_new.parq` begins at `game_id` `20000001`.
  - The fresh `1997` starter audit run therefore returned empty `tpdev_starter_ids` and should **not** be interpreted as evidence that every starter row mismatches in those seasons.

- `2000` starter verification:
  - Fresh tpdev-backed starter audit for `0029900374` and `0029900871` (`theory_test_period_starters_2000_20260317_v1/`) found `2` real mismatch rows.
  - `0029900374`, period `1`, Dallas (`1610612742`): parser starters omit `Hubert Davis`; tpdev includes him. Fresh stint trace (`theory_test_stints_2000_20260317_v1/`) shows `Hubert Davis` with `720` missing seconds, classified as `silent carryover`.
  - `0029900871`, period `4`, Denver (`1610612743`): parser starters include `Popeye Jones` instead of `Nick Van Exel`. Fresh stint trace shows `Popeye Jones` with a `720`-second overage and `Nick Van Exel` with a `368`-second shortage, both classified as `silent carryover`.

- `2001` overtime / late-period starter verification:
  - Fresh tpdev-backed starter audit for `0020000341`, `0020000460`, `0020000494`, and `0020000964` (`theory_test_period_starters_2001_20260317_v1/`) found `5` mismatch rows across `4` games.
  - All verified starter mismatches are omitted players in the exact problematic periods:
    - `0020000341`, period `5`: missing `Wally Szczerbiak`
    - `0020000460`, period `5`: missing `Chris Childs`
    - `0020000494`, period `5`: missing `Bryce Drew`
    - `0020000494`, period `7`: missing `P.J. Brown` and `Baron Davis`
    - `0020000964`, period `4`: missing `Brent Barry`
  - Fresh stint traces (`theory_test_stints_2001_20260317_v1/`) show those same players carrying exact one-period deficits:
    - `Wally Szczerbiak`: `300` seconds
    - `Chris Childs`: `300` seconds
    - `Bryce Drew`: `300` seconds
    - `P.J. Brown`: `300` seconds
    - `Baron Davis`: `300` seconds
    - `Brent Barry`: `720` seconds
  - Result: the starter / carryover failure family is now externally verified from `2000` forward and clearly produces exact one-period minute drift.

- `2002` starter verification:
  - Fresh tpdev-backed starter audit for `0020100162`, `0020100664`, and `0020101105` (`theory_test_period_starters_2002_20260317_v1/`) found `3` mismatch rows across `3` games.
  - All three rows are omitted-starter failures in the flagged periods:
    - `0020100162`, period `5`: missing `Kerry Kittles` and `Richard Jefferson`
    - `0020100664`, period `3`: missing `Reggie Miller`
    - `0020101105`, period `4`: missing `Pat Garrity`
  - Fresh stint traces (`theory_test_stints_2002_20260317_v1/`) show those same players carrying exact one-period deficits:
    - `Kerry Kittles`: `300` seconds
    - `Richard Jefferson`: `300` seconds
    - `Reggie Miller`: `720` seconds
    - `Pat Garrity`: `720` seconds
  - These verified rows also show the runtime accepting visibly incomplete starter maps, e.g. `current_starter_ids` with only `3` players in `0020100162` period `5` and only `4` players in `0020100664` period `3` / `0020101105` period `4`, which is direct game-backed confirmation of the active offline incomplete-starter failure mode.

- Verified starter-family root cause, `2000-2003`:
  - Fresh individual traces now show that most verified starter-minute failures are not “wrong extra players” versus tpdev; they are incomplete lineups where the parser recognized the real new starters but refused to backfill the remaining unchanged starters.
  - In the `2000-2002` verified mismatch sample (`0029900374`, `0029900871`, `0020000341`, `0020000460`, `0020000494`, `0020000964`, `0020100162`, `0020100664`, `0020101105`), all `10` mismatch rows had at least one detected starter not present at the previous period end, and `9` of the `10` missing tpdev starters had no `player1` / sub-in / sub-out role anywhere in the bad period. That is exactly the shape that defeats the current subset-gated carryover fill in CF-38.
  - Concrete game-backed examples:
    - `0020000341`, period `5`: `LaPhonso Ellis` is a real new OT starter, but `Wally Szczerbiak` has no OT events, so the subset gate refuses to add him and leaves Minnesota at `4` starters.
    - `0020000460`, period `5`: `Allan Houston` is recognized as the real new OT starter, but `Chris Childs` has no OT events and is never backfilled, leaving New York at `4`.
    - `0020000494`, period `5`: `Elton Brand` is recognized as the real new OT starter, but `Bryce Drew` has no OT events and is never backfilled, leaving Chicago at `4`.
    - `0020000494`, period `7`: `Eddie Robinson` is recognized, but `P.J. Brown` has no period events and `Baron Davis` appears only as `player2` on a late assist, so the Hornets stay at `3`.
    - `0020000964`, period `4`: `Shammond Williams` and `Vin Baker` are recognized as real quarter-start changes, but `Brent Barry` has no Q4 events and is never restored, leaving Seattle at `4`.
    - `0020100162`, period `5`: `Todd MacCulloch` is recognized as the real new OT starter, `Richard Jefferson` has no OT events, and `Kerry Kittles` appears only as `player2` on a late assist; the Nets stay at `3`.
    - `0020100664`, period `3`: `Jalen Rose` / `Jermaine O'Neal` are recognized as new starters, but `Reggie Miller` has no third-quarter events and is never filled, leaving Indiana at `4`.
    - `0020101105`, period `4`: `Horace Grant` is recognized as a real new starter, but `Pat Garrity` has no Q4 events and is never filled, leaving Orlando at `4`.
  - The same structural pattern persists into `2003`:
    - `0020200261`, period `3`: current Kings starters are `[Bobby Jackson, Vlade Divac, Gerald Wallace, Chris Webber]` while tpdev adds `Doug Christie`; `Gerald Wallace` is a recognized new starter, `Doug Christie` has no third-quarter events, and Sacramento stays at `4`.
    - `0020200619`, period `5`: current Hawks starters are `[Theo Ratliff, Jason Terry, Shareef Abdur-Rahim, Glenn Robinson]` while tpdev adds `Corey Benjamin`; Atlanta recognizes the new OT starters but still stays at `4`.
  - A quick `2005` spot check matches the same pattern:
    - `0020401119`, period `5`: current Hawks starters are `[Josh Smith, Predrag Drobnjak, Tyronn Lue, Tony Delk]` while tpdev adds `Josh Childress`; Atlanta recognizes the real new OT starters but still leaves the team at `4`.
    - `0020400932`, period `5`: current Hawks starters are `[Josh Smith, Tyronn Lue, Al Harrington, Josh Childress]` while tpdev adds `Tom Gugliotta`; again, new OT starters are recognized but the missing carryover-style starter is never restored.

- `2005-2008` survivor walk:
  - A subagent walked the remaining `26` unique `starter_complex_candidate` situations from `2005-2008` one by one using the live offline runtime plus direct `StartOfPeriod` helper inspection.
  - Rollup:
    - carryover subset gate blocks backfill after real new starters are recognized: `18`
    - dangling `player2` / `player3` starter dropped by `_split_up_starters_by_team`: `4`
    - period-1 local boxscore fallback issue: `2`
    - same-clock `_trim_excess_starters()` wrong-five cases: `0`
    - new family, wrong carryover replacement when the real starter is a no-event period-start change not in the previous end lineup: `2`
  - The dangling-secondary-role confirmations match CF-22 on real games:
    - `0020400528`, period `5`, Clippers: `Bobby Simmons` appears only as `player2` on a late assist and is dropped from the team map.
    - `0020500717`, period `6`, Magic: `Steve Francis` appears only as `player2` on the opening `5:00` jump ball tip and is dropped.
    - `0020700449`, period `5`, Hornets: `Jeff McInnis` appears only as `player2` on a `4:16` assist and is dropped.
    - `0020700876`, period `5`, Mavericks: `Jerry Stackhouse` appears only as `player2` on a `3:41` assist and is dropped.
  - The period-1 local starter fallback issue in CF-40 is also repeated in real games:
    - `0020400736`, period `1`, Cavaliers: `Ira Newble` has no period events and the local starter loader dies on the swallowed `TypeError`.
    - `0020500102`, period `1`, Heat: `James Posey` has no period events and the same swallowed loader `TypeError` leaves Miami at `4`.
  - New wrong-carryover-replacement family (CF-41):
    - `0020401139`, period `5`, Spurs: carryover fills `Tony Parker` and `Brent Barry` from the previous lineup, but tpdev expects `Bruce Bowen` instead of `Brent Barry`.
    - `0020700319`, period `4`, Knicks: carryover fills `Nate Robinson` from the previous lineup, but tpdev expects `Fred Jones`.

- Verified same-clock trim failure:
  - `0029900871`, period `4`, Denver is a fresh game-backed proof of CF-39.
  - The raw starter scan finds `6` Nuggets starter candidates: `George McCloud`, `Keon Clark`, `Chris Herren`, `Raef LaFrentz`, `Popeye Jones`, and `Nick Van Exel`.
  - At `5:52`, the feed order is `MISS Hill FT` -> `SUB: Jones FOR McDyess` -> `SUB: Bowen FOR Van Exel` -> `Jones REBOUND`, but the event `order` puts the rebound ahead of both substitutions.
  - Because `Popeye Jones` is first seen on that rebound before the team’s first substitution order, `_trim_excess_starters()` keeps `Jones` and drops `Nick Van Exel`, producing the exact verified `720`-second minute swing from the stint trace.

- Verified local period-1 starter fallback failure:
  - `0029900374`, period `1`, Dallas is a fresh game-backed proof of CF-40.
  - The cached local boxscore for `0029900374` contains all five Dallas starters with valid `START_POSITION`, including `Hubert Davis`.
  - On the live `StatsStartOfPeriod` object, `boxscore_source_loader` is present, but `sop._get_period_starters_from_boxscore_loader()` returns `None`.
  - Directly constructing `StatsNbaBoxscoreLoader(game_id, sop.boxscore_source_loader)` raises `TypeError: _BoxscoreSourceLoader.load_data() takes 1 positional argument but 2 were given`.
  - That exception is swallowed by `_get_period_starters_from_boxscore_loader()`, so the runtime silently falls back to best-effort PBP starter inference and leaves Dallas with only `4` starters, missing `Hubert Davis`.

## March 17 Implementation Update

- Implemented a new cache-backed period-level `boxscoretraditionalv3` starter fallback in the working tree:
  - fork changes:
    - `pbpstats/resources/enhanced_pbp/start_of_period.py`
    - `pbpstats/resources/enhanced_pbp/stats_nba/start_of_period.py`
    - `pbpstats/offline/processor.py`
    - `pbpstats/data_loader/stats_nba/enhanced_pbp/loader.py`
  - runtime changes:
    - `replace_tpdev/period_boxscore_source_loader.py`
    - `replace_tpdev/cautious_rerun.py`
- New fallback order in the active offline runtime:
  1. strict PBP starters
  2. local full-game boxscore `START_POSITION` for period `1`
  3. period V3 fallback: `RangeType=2` start-window first, then `RangeType=1` participants if needed
  4. best-effort PBP starters
- The runtime now caches period V3 responses in `nba_raw.db` under `endpoint='boxscore_period_v3'` with one per-game JSON blob keyed by `period` then `mode`.

### Unit / Loader Verification

- `pytest -q /Users/kmedved/Documents/GitHub/pbpstats/tests/test_period_starters_carryover.py /Users/kmedved/Documents/GitHub/pbpstats/tests/test_offline_boxscore_loader.py`
  - `24 passed`
- `pytest -q /Users/kmedved/Documents/GitHub/pbpstats/tests/test_stats_nba_malformed_events.py /Users/kmedved/Documents/GitHub/pbpstats/tests/test_period_starters_carryover.py /Users/kmedved/Documents/GitHub/pbpstats/tests/test_offline_boxscore_loader.py`
  - `35 passed`
- `pytest -q /Users/kmedved/Library/CloudStorage/OneDrive-Personal/github/darko/replace_tpdev/test_period_boxscore_source_loader.py`
  - `4 passed`

### Game-Backed Canary Result

- Starter audit canary:
  - output: `v3_starter_fallback_canary_audit_20260317_v1/`
  - summary: `202` rows, `13` mismatch rows, `21` games
  - important nuance: `10` of the `13` mismatch rows are the known `1997` `tpdev`-oracle gap for `0029600585`; they are not real verified starter failures.
  - that was the state before the later `prefer cached V3 over strict PBP on post-Q1 periods` change and the cache-poisoning fix below.
- Stint/minutes canary:
  - output: `v3_starter_fallback_canary_trace_20260317_v1/`
  - fully cleared previously unresolved starter-complex games:
    - `0020000494`
    - `0020100162`
    - `0020100664`
    - `0020400528`
    - `0020500717`
  - other already-improved canaries stayed clean:
    - `0029900374`
    - `0029900871`
    - `0020000341`
    - `0020000460`
    - `0020000964`
    - `0020101105`
    - `0020200261`
    - `0020200619`
    - `0020400736`
    - `0020500102`
    - `0020401119`
    - `0020000383`
  - that earlier canary still left `0020400932`, `0020401139`, and `0020700319` as residue; those were rechecked after the later precedence/cache fixes below.
  - non-regression note:
    - `0020000383` stayed fully clean
    - `0029600585` did not surface a new starter-family failure; the remaining issue is still a separate `wrong substitution clock attribution` minute discrepancy for `Dell Curry`, and `tpdev` starter rows remain unavailable for that `1997` game

### Follow-Up Fixes After The First V3 Rollout

- A second pass changed `StatsStartOfPeriod.get_period_starters()` so that, when a cached period V3 loader is available, a clean post-Q1 V3 start-window result is allowed to override a strict PBP lineup. This closes the `wrong full five` family where strict PBP manufactured a plausible but wrong starter set and therefore never entered the non-10 fallback path.
- A cache bug was also fixed in `replace_tpdev/period_boxscore_source_loader.py`:
  - transient request exceptions are no longer cached as `"unavailable"`
  - only successful-but-unusable payloads are cached as unavailable
  - stale old `"unavailable"` rows without a reason are now treated as retryable cache misses

### Targeted Survivor Rechecks After Those Fixes

- `0020401139`:
  - outputs:
    - `v3_preferred_survivor_audit_20260317_v1/`
    - `v3_preferred_survivor_trace_20260317_v1/`
  - result:
    - Spurs period `5` starter mismatch cleared
    - `Brent Barry` / `Bruce Bowen` OT minute drift cleared
- `0020700319`:
  - same targeted outputs as above
  - result:
    - Knicks period `4` starter mismatch cleared
    - `Nate Robinson` / `Fred Jones` quarter-sized minute drift cleared
- `0020400932`:
  - cache-retry outputs:
    - `v3_retry_20400932_audit_20260317_v1/`
    - `v3_retry_20400932_trace_20260317_v1/`
  - result:
    - Hawks OT period `5` now includes `Tom Gugliotta`, and the `-300` second minute drift is gone
    - remaining audit mismatch is only:
      - Pistons, period `4`, where current strict+V3 starters are `[Chauncey Billups, Rasheed Wallace, Antonio McDyess, Lindsey Hunter, Tayshaun Prince]`
      - tpdev expects `[Chauncey Billups, Richard Hamilton, Tayshaun Prince, Ben Wallace, Rasheed Wallace]`
    - important nuance:
      - live `RangeType=2` V3 also returns the current Pistons five for that period, so this row is currently a `tpdev` disagreement, not live evidence of a remaining minute/on-off parser bug

## Suspected / Needs Game-Backed Confirmation

## March 17 V4 Scraper Validation

- New scraper artifact:
  - `period_starters_v4.parquet`
  - key change: unresolved `first_event_elapsed == 0` periods now retry a tighter `RangeType=2` window ending at `first_nonzero_event_elapsed`
- Targeted runtime canary:
  - `v4_runtime_canary_period_audit_20260317_v1/`
  - `v4_runtime_canary_stints_20260317_v1/`
  - result:
    - `0020100162` period `5` is now clean; Nets OT starters include both `Kerry Kittles` and `Richard Jefferson`, and the `-300/-300` minute drift is gone
    - `0020400932` period `5` is now clean at the minute level; Hawks OT includes `Tom Gugliotta`, and the `-300` second drift is gone
    - `0029700060` remains clean and still resolves via `resolver_mode = base_window`, so the earlier `1998` negative-control regression does not return
    - `0020401139` still fails in the live runtime even though `period_starters_v4.parquet` now contains a correct `resolver_mode = first_clock_change` row for Spurs OT period `5`
- Broad runtime canary:
  - `v4_runtime_broad_canary_period_audit_20260317_v1/`
  - `v4_runtime_broad_canary_stints_20260317_v1/`
  - result:
    - across `14` representative starter-family games, only `2` starter mismatch rows remain: `0020400932` and `0020401139`
    - only `1` game still has real minute mismatch residue: `0020401139`
    - all earlier fixed canaries in `1999-2003` stay clean under the current "strict PBP unless impossible" runtime policy
- Interpretation:
  - `period_starters_v4.parquet` is strong enough to rescue the zero-anchor unresolved family
  - but the current runtime still cannot use it for `0020401139`, because strict PBP returns a full `5/5` lineup there and the existing impossibility gate does not mark that lineup invalid
  - that leaves `0020401139` in the "strict succeeded but wrong full five" bucket, where the safest next move is likely a local `period_starters_overrides.json` entry unless a new generalized invalidation rule can be proven

## Suspected / Needs Game-Backed Confirmation

| ID | Subsystem | Risk | Where to verify next |
| --- | --- | --- | --- |
| S-01 | `pbpstats/resources/enhanced_pbp/stats_nba/enhanced_pbp_item.py:225-259` | The `get_offense_team_id()` fallback can reassign offense based on `team_id`, previous event, or `0` when lineup chains are broken, which may distort possession boundaries and plus-minus attribution. | Check against same-clock scoring/substitution survivors in the plus-minus audit families. |
| S-02 | `pbpstats/resources/enhanced_pbp/shot_clock.py:99-147` | Shot-clock and possession-change inference backfills through broken previous-event chains and swallows exceptions, so shot-clock buckets may be wrong without surfacing an error. | Compare shot-clock output on known same-clock substitution / foul clusters. |
| S-03 | `pbpstats/resources/enhanced_pbp/stats_nba/jump_ball.py:21-35` | `StatsJumpBall.get_offense_team_id()` still assumes two teams exist in `current_players` and may fail or misattribute offense on malformed start-of-period windows. | Verify on jump-ball-heavy starter failures. |
| S-04 | `pbpstats/resources/enhanced_pbp/stats_nba/rebound.py:32-44` | `StatsRebound.get_offense_team_id()` also assumes two teams on placeholder rebound paths and may break when lineup state is already degraded. | Verify on rebound-order repair cases that still surface lineup drift. |
| S-05 | `pbpstats/offline/ordering.py:9-21` | `_ensure_eventnum_int()` silently drops rows with nonnumeric `EVENTNUM`, and that helper is on the active offline path. | Search historical parquet/DB inputs for malformed `EVENTNUM` before promoting to confirmed. |
| S-06 | `pbpstats/data_loader/stats_nba/enhanced_pbp/loader.py:384-424` | The same-clock rebound move heuristic may relocate valid rows without external evidence, not just broken ones. | Spot-check direct StatsNBA games where this branch fires. |
| S-07 | `pbpstats/data_loader/stats_nba/enhanced_pbp/loader.py:428-499` | `_use_stats_nba_v3_event_order()` sorts by `actionId` and then `EVENTNUM`, which may not preserve workable same-clock chronology even when v3 exists. | Validate on direct StatsNBA problem games before reusing this logic anywhere offline. |
| S-08 | `pbpstats/resources/enhanced_pbp/field_goal.py:165-219` | `FieldGoal.get_shot_data()` now has partial-return behavior when opponent lineup metadata is missing, which may hide lineup corruption in downstream shot-detail exports. | Check shot-detail consumers against lineup-audit offenders. |
| S-09 | `pbpstats/resources/enhanced_pbp/enhanced_pbp_item.py:213-228` | `seconds_since_previous_event` has no same-period monotonicity guard, so any same-period clock increase from bad event order turns directly into negative stint seconds. | Check historical outlier games for negative or offsetting seconds-played intervals. |
| S-10 | `pbpstats/resources/enhanced_pbp/enhanced_pbp_item.py:81-109` | `get_all_events_at_current_time()` simply sorts same-clock windows by raw `order`, so any unresolved same-clock chronology issue leaks into every helper built on those windows, not just FG/FT attribution. Fresh `1997`/`2000` spot checks (`0029600062`, `0029600370`, `0029600386`, `0029600478`, `0029900374`, `0029900871`) did not show mismatch-player lineup flips between the pre-window lineup and the scoring-event lineup, so this remains unverified as a dominant early-year cause. | Push the next game-backed verification into later FT-heavy residue games after the starter family is exhausted from `2000+`. |
## Coverage Gaps

- `tests/test_offline_boxscore_loader.py:64-67` and `tests/test_offline_boxscore_loader.py:92-95` monkeypatch out `dedupe_with_v3`, `patch_start_of_periods`, `reorder_with_v3`, and `_ensure_eventnum_int`, so the active offline ordering path is not actually covered there.
- `tests/test_stats_nba_malformed_events.py:50-99` explicitly asserts the current substitution no-op behaviors, which documents the fork's present semantics but also hardens them as expected behavior.
- `tests/test_period_starters_carryover.py:115-126` explicitly asserts that carryover does nothing when a team has zero detected starters, so the zero-starter skip in CF-05 is currently codified as expected behavior.
- The new V3 fallback tests now cover exact-10 direct returns, RT2-to-RT1 fallback, same-clock sub-order tie-breaking, and processor forwarding of `period_boxscore_source_loader`, but there are still no direct tests covering the active notebook wrapper path in `replace_tpdev/cautious_rerun.py`.
- There are still no tests covering the starter-specific failure points in CF-20, CF-21, or CF-22 on real malformed historical rows.
- `tests/test_stats_nba_malformed_events.py:402-430` and `tests/resources/test_foul.py:25-55` similarly lock in the "return only `base_stats`" behavior for malformed turnover/foul rows.
- The earlier focused suite `pytest -q tests/test_period_starters_carryover.py tests/test_stats_nba_malformed_events.py tests/test_offline_repair.py tests/test_lineup_window_overrides.py tests/test_shot_clock.py` passed (`77 passed`), but it still does not exercise the active notebook path where cached `pbpv3` is forwarded into `get_possessions_from_df(...)`.
- `pytest -q tests/test_period_starters_carryover.py tests/test_stats_nba_malformed_events.py` still passes (`23 passed`), but those files do not cover CF-27, CF-29, CF-30, CF-31, or CF-32.
- `audit_period_starters_against_tpdev.py` cannot externally verify `1997-1999` starter-complex games via tpdev starters because `fixed_data/raw_input_data/tpdev_data/full_pbp_new.parq` begins at `game_id` `20000001`.
- There are no tests covering the malformed-row coercion in CF-34, where a missing `PLAYER1_TEAM_ID` can turn a real `player1_id` into a bogus `team_id` and remove that player's starter evidence.
- There are no tests covering same-clock scoring/substitution attribution for made field goals or free throws, including the foul-after-FT case in CF-36.
- There are no tests covering `_trim_excess_starters()` on same-clock multi-candidate windows in the active `ignore_missing_starters=True` fallback path.
- There are no current tests covering one-team starter acceptance, outgoing-player-missing substitution no-ops, field-goal partial-return behavior on incomplete lineups, `EnhancedPbpItem._get_previous_raw_players()` wiping lineup state to `{}`, or `Possessions._aggregate_event_stats()` swallowing event-stat exceptions.
- There are no tests covering the active local period-1 starter fallback wiring, including the `boxscore_source_loader.load_data(game_id)` integration path that currently throws in CF-40.

## Next Pass Queue

1. Finish the active offline path.
   Close out starter completeness, substitution no-ops, and incomplete-lineup propagation into field goals, free throws, fouls, turnovers, rebounds, and violations.
2. Finish the direct StatsNBA repair stack.
   Audit `_check_rebound_event_order`, `_fix_common_event_order_error`, `_use_stats_nba_v3_event_order`, and the disabled data.nba fallback for destructive heuristics and silent fallthrough.
3. Audit same-clock attribution.
   Focus on `FreeThrow.event_for_efficiency_stats`, offense-team fallback behavior, and shot-clock inference around same-clock foul/substitution clusters.
4. Cleanup / dead-code pass.
   Remove or isolate debug prints, broad exception swallowing, misleading helper names, and code paths that silently drop semantics instead of surfacing a repair decision.
