# Appendix: Detailed Per-Game Cross-Source Evidence
## Companion to handoff_for_external_llm_20260322.md

This appendix provides the raw evidence behind each policy question — full minute audit tables, event-on-court issue rows with lineups, cross-source comparisons, scratch validation results, and correction manifest entries. Each section corresponds to a game in the main handoff document.

It also now carries the **verbatim runtime code payload** that was moved out of the main handoff so the main document can stay focused on current policy evidence.

Artifact caveat:
- Some older selected-lane and raw issue CSVs have broken `player_name` fields that repeat the numeric `player_id`. The narrative names in this appendix are manually remapped from the event evidence and should be trusted over those raw `player_name` columns.

---

## A. Same-Clock Control / Guardrail Games (Lane 1)

### A1. `0021700337` — SAS @ MEM, Period 3 (2018)

**Event-on-court issue rows (from intraperiod proving loop):**

| Event | Period | Clock | Event Class | Player | Player ID | Team | Status | Current Lineup |
|---|---|---|---|---|---|---|---|---|
| E528 | 3 | 1:00 | StatsFreeThrow | Joffrey Lauvergne | 203530 | SAS | off_court_event_credit | [1627749, 1628389, 203114, 203932, 1627752] |
| E529 | 3 | 1:00 | StatsFreeThrow | Joffrey Lauvergne | 203530 | SAS | off_court_event_credit | [1627749, 1628389, 203114, 203932, 1627752] |

Both current and previous lineups are identical — no lineup change between these events. Lauvergne was subbed out at the same 1:00 clock.

**Canary manifest role:** Listed as `failed_patch_anti_canary` in `golden_canary_manifest_20260321_v1.json` with `stability_class = stable`. Also the main negative tripwire in `same_clock_canary_manifest_non_opening_ft_sub_20260320_v1.json` — "do not auto-promote a rule that fails this case."

**How the three failed overlay approaches performed on this game:**
- Approach 1 (lineup propagation carryover): +12 pm mismatches
- Approach 2 (foul-committer anchor overlay): +12 pm mismatches
- Approach 3 (fouled-player anchor overlay): +13 pm mismatches

**Minutes audit:** All players in this game match official minutes exactly. Zero minute mismatches, zero outliers.

---

### A2. `0021700377` — LAL @ CLE, Period 3 (2018)

**Event-on-court issue rows:**

| Event | Period | Clock | Event Class | Player | Player ID | Team | Status | Current Lineup |
|---|---|---|---|---|---|---|---|---|
| E421 | 3 | 3:03 | StatsFreeThrow | Jordan Clarkson | 203903 | LAL | off_court_event_credit | [1628398, 203076, 201599, 201566, 203507] |

Clarkson credited with a FT at 3:03 while off-court. The sub occurred at the same clock.

**Canary manifest role:** Listed as `failed_patch_anti_canary` — scorer/sub family negative tripwire, explicitly frozen per AGENTS.md.

**Minutes audit:** All players match official minutes. Zero mismatches.

---

### A3. `0021700514` — UTA @ PHX, Period 2 (2018)

**Event-on-court issue rows:**

| Event | Period | Clock | Event Class | Player | Player ID | Team | Status | Current Lineup |
|---|---|---|---|---|---|---|---|---|
| E243 | 2 | 5:09 | StatsFoul | Royce O'Neale | 1626220 | UTA | off_court_event_credit | [201588, 203497, 1627750, 203903, 1628378] |

O'Neale credited with a foul at 5:09 while off-court. The event cluster at 5:09 is: foul → 2 FTs → rebound → substitution.

**Minutes audit:** Zero minute mismatches.

---

### A4. `0021801067` — BOS @ WAS, Period 3 (2019)

**Event-on-court issue rows:**

| Event | Period | Clock | Event Class | Player | Player ID | Team | Status | Current Lineup |
|---|---|---|---|---|---|---|---|---|
| E374 | 3 | 11:06 | StatsFoul (flagrant) | Marcus Smart | 203935 | BOS | off_court_event_credit | [1628407, 203935, 1627759, 201143, 202694] |

Smart credited with a flagrant foul at 11:06 P3. This is part of a complex multi-event cluster involving ejection, technical, and flagrant FTs, with a lineup change from Smart to Brown within the same clock window.

**Minutes audit:** Zero minute mismatches.

---

### A5. `0021900333` — PHX @ WAS, Period 4 (2020)

**Event-on-court issue rows:**

| Event | Period | Clock | Event Class | Player | Player ID | Team | Status | Current Lineup |
|---|---|---|---|---|---|---|---|---|
| E659 | 4 | 5:26 | StatsRebound | Aron Baynes | 1629661 | PHX | off_court_event_credit | [1629028, 201609, 203994, 1628969, 202340] |

Baynes credited with a rebound at 5:26 P4 while off-court. Cluster includes double substitutions and FT sequence at the same clock.

**Minutes audit:** Zero minute mismatches.

---

### Lane 1 Cross-Source Summary

All 5 games share the identical pattern:
- Player credited with event at clock T
- Substitution involving that player also at clock T
- Pipeline processes sub first → player off-court when event is credited
- Official/arena system credits the event before the sub
- **Zero minute impact** — the pipeline's minute tracking is correct because the stint boundaries are the same either way; only the event credit differs
- **PM impact is exactly ±1 or ±2** per game — the boundary-difference residual

---

## B. Rebound-Credit Survivors (Lane 2)

### B1. `0021900201` — LAC @ OKC, Period 3 (2020)

**Event-on-court issue row (from intraperiod proving loop):**

| Event | Period | Clock | Event Class | Player | Player ID | Team | Status | Current Lineup |
|---|---|---|---|---|---|---|---|---|
| E398 | 3 | 7:42 | StatsRebound | Nerlens Noel | 203457 | OKC | off_court_event_credit | [101108, 201568, 1628983, 203500, 1628390] |

Noel not in current or previous lineup. Description: "Noel REBOUND (Off:2 Def:3)". The pipeline has Adams (203500) still on court; the sub (Noel FOR Adams) at E395 has already been processed.

**Raw PBP event sequence around the issue:**
- P3 E392 at 7:45: Adams foul
- P3 E395: SUB: Noel FOR Adams
- P3 E397: missed FT
- P3 E398: **Noel REBOUND** ← this is the issue event

`full_pbp_new` also flips OKC from Adams to Noel between the 7:49 and 7:42 possessions.

**Scratch validation result (`_tmp_validate_0021900201_event_20260322_v1`):**

| Metric | Before correction | After correction |
|---|---|---|
| Games completed | 1 | 1 |
| Failed games | 0 | 0 |
| Boxscore audit failures | 0 | 0 |
| Minutes mismatches | 0 | **2** (Noel -0.05, Adams +0.05) |
| PM mismatches | 0 | **2** (Caldwell-Pope -1, Caruso +1) |
| Event-on-court issues | 1 (E398 Noel rebound) | **1** (E395 SUB: Noel FOR Adams) |

The correction cleared E398 but the blocker moved to E395 (the sub event itself), and it introduced 2 minute + 2 PM mismatches. **Rejected** because it fails the "event-on-court rows decrease or hold" and "minute profile improves or holds" criteria.

**Full minute audit (selected players):**

| Player | Minutes Output | Minutes Official | Diff | PM Output | PM Official | PM Diff |
|---|---|---|---|---|---|---|
| Nerlens Noel | 23.85 | 23.85 | 0.0 | -1.0 | -1.0 | 0.0 |
| Steven Adams | 24.15 | 24.15 | 0.0 | -4.0 | -4.0 | 0.0 |
| Chris Paul | 30.20 | 30.20 | 0.0 | +1.0 | +1.0 | 0.0 |

All 20 players match on minutes. Zero minute diff for every player in the game.

---

### B2. `0021900419` — POR @ LAC, Period 2 (2020)

**Event-on-court issue row:**

| Event | Period | Clock | Event Class | Player | Player ID | Team | Status | Current Lineup |
|---|---|---|---|---|---|---|---|---|
| E258 | 2 | 4:38 | StatsRebound | Maurice Harkless | 203090 | LAC | off_court_event_credit | [201976, 101150, 1626149, 202695, 202331] |

Harkless not in current or previous lineup. Description: "Harkless REBOUND (Off:0 Def:1)".

**Scratch validation result (`_tmp_validate_0021900419_event_20260322_v1`):**

| Metric | Before | After |
|---|---|---|
| Minutes mismatches | 0 | **2** |
| PM mismatches | 0 | **2** |
| Event-on-court issues | 1 (E258 Harkless rebound) | **1** (E255 SUB: Harkless FOR Williams) |

Same pattern: correction shifted blocker to the sub event. **Rejected.**

**Minute audit:** All 19 players match official minutes within 0.002 minutes (rounding only).

---

### B3. `0021900487` — POR @ MEM, Period 2 (2020)

**Event-on-court issue row:**

| Event | Period | Clock | Event Class | Player | Player ID | Team | Status | Current Lineup |
|---|---|---|---|---|---|---|---|---|
| E246 | 2 | 6:40 | StatsRebound | Jaren Jackson Jr. | 1628991 | MEM | off_court_event_credit | [203937, 1628415, 1629630, 202685, 203524] |

Description: "Jackson Jr. REBOUND (Off:0 Def:6)".

**Scratch validation result (`_tmp_validate_0021900487_event_20260322_v1`):**

| Metric | Before | After |
|---|---|---|
| Minutes mismatches | 0 | **6** |
| PM mismatches | 0 | **4** |
| Event-on-court issues | 1 (E246 Jackson Jr.) | **1** (E239 SUB: Jones FOR Morant) |

This was the worst scratch result — the correction cleared the rebound event but left a completely different blocker (E239, a different player's sub) and introduced **6** minute mismatches across multiple players. **Rejected.**

**Minute audit (selected players with PM mismatches):**

| Player | Minutes Output | Minutes Official | PM Output | PM Official | PM Diff |
|---|---|---|---|---|---|
| Tyus Jones | 19.63 | 19.63 | -6.0 | -7.0 | +1.0 |
| Ja Morant | 28.37 | 28.37 | +19.0 | +20.0 | -1.0 |
| Miles Bridges | 26.16 | 26.16 | -5.0 | -6.0 | +1.0 |
| Cody Martin | 15.15 | 15.15 | -1.0 | 0.0 | -1.0 |

All minute outputs match official within 0.003 min. The PM mismatches are ±1 only — typical same-clock boundary differences.

---

### B4. `0021900920` — MEM @ NOP, Period 2 (2020)

**Event-on-court issue row:**

| Event | Period | Clock | Event Class | Player | Player ID | Team | Status | Current Lineup |
|---|---|---|---|---|---|---|---|---|
| E312 | 2 | 2:23 | StatsRebound | Anthony Tolliver | 201229 | MEM | off_court_event_credit | [203937, 1629001, 1628367, 202685, 1629630] |

Description: "Tolliver REBOUND (Off:0 Def:1)".

**Scratch validation results (two attempts):**

Event-only attempt: previously rejected (shifted blocker).

Widened window P2 E307-E312 (`_tmp_validate_0021900920_window_20260322_v2`):

| Metric | Before | After widened window |
|---|---|---|
| Minutes mismatches | 0 | **0** |
| PM mismatches | 4 | **4** (unchanged) |
| Event-on-court issues | 1 (E312 Tolliver) | **1** (E312 Tolliver — exact same!) |

The widened window was a **true no-op** — the exact same blocker survived with identical metrics. The window correction simply had no effect because the pipeline's lineup state was already locked at the cluster boundary. **Rejected.**

**Minute audit:** All 26 players match official minutes. The 4 PM mismatches are all ±2 (Chandler, Dinwiddie, Harris, Chiozza) — typical boundary differences.

---

### B5. `0041900155` — DAL @ LAC, Period 2 (2020 Playoffs)

**Event-on-court issue row:**

| Event | Period | Clock | Event Class | Player | Player ID | Team | Status | Current Lineup |
|---|---|---|---|---|---|---|---|---|
| E353 | 2 | 0:01.40 | StatsRebound | Montrezl Harrell | 1626149 | LAC | off_court_event_credit | [1629013, 202694, 101150, 1627826, 202704] |

Description: "Harrell REBOUND (Off:1 Def:3)". The rebound happens at 0:01.40 in Q2 — near the end of the half.

**Scratch validation results (two attempts):**

Event-only (`_tmp_validate_0041900155_event_20260322_v1`): traded E353 for E348 (`SUB: Harrell FOR Zubac`), introduced 2 minute mismatches.

Widened window P2 E348-E353 (`_tmp_validate_0041900155_window_20260322_v2`):

| Metric | Before | After widened window |
|---|---|---|
| Minutes mismatches | 0 | **2** (Harrell, Zubac) |
| PM mismatches | 0 | **2** |
| Event-on-court issues | 1 (E353 Harrell) | **1** (E348 SUB: Harrell FOR Zubac) |

Same pattern again: blocker shifted from rebound to sub event, minute mismatches introduced. **Rejected.**

**Minute audit (selected):**

| Player | Minutes Output | Minutes Official | PM Output | PM Official |
|---|---|---|---|---|
| Montrezl Harrell | 22.87 | 22.87 | +34.0 | +34.0 |
| Ivica Zubac | 23.61 | 23.61 | +10.0 | +10.0 |
| Kawhi Leonard | 29.83 | 29.83 | +25.0 | +24.0 |
| Landry Shamet | 22.47 | 22.47 | +21.0 | +22.0 |

All 28 players match on minutes. Only 2 PM mismatches in the entire game (Leonard ±1, Shamet ±1).

---

### Lane 2 Cross-Source Summary

| Game | Issue Event | Player | Scratch Result | Minutes After | New Blocker After |
|---|---|---|---|---|---|
| 0021900201 | P3 E398 rebound | Noel | 2 min + 2 PM mismatches | 2 new | E395 (sub event) |
| 0021900419 | P2 E258 rebound | Harkless | 2 min + 2 PM mismatches | 2 new | E255 (sub event) |
| 0021900487 | P2 E246 rebound | Jackson Jr. | **6 min + 4 PM mismatches** | 6 new | E239 (different sub!) |
| 0021900920 | P2 E312 rebound | Tolliver | True no-op | 0 new | E312 (same event!) |
| 0041900155 | P2 E353 rebound | Harrell | 2 min + 2 PM mismatches | 2 new | E348 (sub event) |

Every correction attempt fails the same way: the rebound and the substitution are at the same clock, and flipping which one is "first" just moves the off-court credit from the rebound event to the sub event.

---

## C. Period-Start Contradiction Cases (Lane 3)

### C1. `0020900189` — DEN @ MIN, Period 2 (2010, Block C)

**Event-on-court issue row:**

| Event | Period | Clock | Event Class | Player | Player ID | Team | Status | Current Lineup |
|---|---|---|---|---|---|---|---|---|
| E217 | 2 | 12:00 | StatsSubstitution | Chauncey Billups | 1497 | DEN | sub_out_player_missing_from_previous_lineup | [2546, 2365, 2747, 2403, 201951] |

The sub is "Lawson FOR Billups" but Billups is NOT in the previous lineup (which already has Lawson at 201951). This is the period-start contradiction: v6 says Lawson starts Q2, so the pipeline already has Lawson in the lineup when the "Lawson FOR Billups" sub event arrives.

**Source-by-source evidence:**

| Source | Who starts Denver Q2? | Evidence |
|---|---|---|
| `period_starters_v6` (gamerotation) | **Lawson** | Resolved Denver Q2 row has Lawson active at boundary |
| `full_pbp_new` (tpdev) | **Billups** | First Q2 possession at clock=720 has Billups on lineup; Lawson appears at clock=702 |
| Cached `pbpv3` (nba_raw.db) | **Ambiguous** | 12:00 cluster: Start of 2nd Period → technical foul → SUB: Lawson FOR Billups → technical FT — all at 12:00 |
| Pipeline (current) | **Lawson** | Follows v6, which puts Lawson in at the boundary |

**Minute audit for the key players:**

| Player | Minutes Output | Minutes Official | Diff | PM Output | PM Official | PM Diff |
|---|---|---|---|---|---|---|
| Chauncey Billups | 30.517 | 30.517 | 0.0 | -3.0 | -2.0 | **-1.0** |
| Ty Lawson | 21.865 | 21.867 | -0.002 | +25.0 | +24.0 | **+1.0** |

Minutes are effectively identical. The only impact is a ±1 PM swap between Billups and Lawson — the two players involved in the disputed Q2 start.

**Residual classification:** The event row is classified as `fixable_lineup_defect` with `is_blocking = True`. The PM rows are `candidate_boundary_difference` with `is_blocking = False`. The two PM rows perfectly offset: Billups -1, Lawson +1.

---

### C2. `0021300593` — MIA @ CHA, Period 2 (2014, Block D)

**Event-on-court issue rows:**

| Event | Period | Clock | Event Class | Player | Player ID | Team | Status | Current Lineup |
|---|---|---|---|---|---|---|---|---|
| E96 | 2 | 12:00 | StatsFoul | Norris Cole | 202708 | MIA | off_court_event_credit | [201596, 201563, 1740, 2427, 2617] |
| E99 | 2 | 12:00 | StatsSubstitution | Norris Cole | 202708 | MIA | sub_out_player_missing_from_previous_lineup | [201596, 201563, 1740, 2427, 2617] |

E96: Cole commits a foul at 12:00 Q2 start, but v6 says Mason Jr. (2427) starts Q2, so Cole is off-court.
E99: "SUB: Mason Jr. FOR Cole" but Cole isn't in the lineup (Mason already is, per v6).

**Source-by-source evidence:**

| Source | Who starts Miami Q2? | Evidence |
|---|---|---|
| `period_starters_v6` (gamerotation) | **Mason Jr.** | Resolved Miami Q2 row has Roger Mason Jr. (2427) active |
| `full_pbp_new` (tpdev) | **Cole** | First Q2 boundary row at clock=720, event_id=42 has Cole; next row at same clock, event_id=43 flips to Mason |
| Cached `pbpv3` | **Ambiguous** | 12:00 cluster: `Cole S.FOUL` → `SUB: Mason Jr. FOR Cole` — both at 12:00, FTs split around them |
| Pipeline (current) | **Mason Jr.** | Follows v6 |

**Minute audit for the key players:**

| Player | Minutes Output | Minutes Official | Diff | PM Output | PM Official | PM Diff |
|---|---|---|---|---|---|---|
| Norris Cole | 20.633 | 20.633 | 0.0 | -5.0 | -7.0 | **+2.0** |
| Roger Mason Jr. | 6.917 | 6.917 | 0.0 | -2.0 | 0.0 | **-2.0** |

Again: minutes are identical. The only impact is a ±2 PM swap between Cole and Mason — the two players involved in the disputed Q2 start. The PM rows perfectly offset.

**The ambiguity in detail:** Cole commits a foul at Q2 start, then is immediately subbed out. Were the FTs shot while Cole was on court (he committed the foul, was still there for the FTs) or after he was subbed out (the sub was at 12:00, before the FTs)? This is a genuinely unanswerable question from the available data.

---

## D. Remaining Minute-Impact Holds (Mixed Archetypes)

### D1. `0020400335` — NOH @ HOU, Period 2 (2005, Block B, `severe_minute_insufficient_local_context`)

**Event-on-court issue rows (5 total, all same player):**

| Event | Period | Clock | Event Class | Player | Player ID | Team | Status | Event Description |
|---|---|---|---|---|---|---|---|---|
| E162 | 2 | 6:31 | StatsFieldGoal | Al Harrington | 2454 | NOH | off_court_event_credit | Freije 22' Jump Shot (2 PTS) with Harrington assist |
| E181 | 2 | 4:47 | StatsFieldGoal | Al Harrington | 2454 | NOH | off_court_event_credit | Harrington Layup MISS / Massenburg BLOCK |
| E196 | 2 | 3:07 | StatsFieldGoal | Al Harrington | 2454 | NOH | off_court_event_credit | Harrington 19' Jump Shot (2 PTS) |
| E223 | 2 | 0:05.50 | StatsFoul | Al Harrington | 2454 | NOH | off_court_event_credit | Harrington S.FOUL |
| E226 | 2 | 0:00.50 | StatsFieldGoal | Al Harrington | 2454 | NOH | off_court_event_credit | Harrington Layup MISS / Duncan BLOCK |

Current lineup at events: changes across the stretch but never includes Harrington (2454).
- At E162: `[2365, 133, 2747, 2782, 136]`
- At E181-E226: `[1924, 133, 2747, 2782, 136]` (Nailon replaced Andersen)

Harrington is credited with 5 different events (assist, shot attempt, made shot, foul, shot attempt) spanning 6:31 minutes of game clock — a substantial stretch where the pipeline doesn't have him on court but the NBA PBP credits him with real basketball plays.

**Full minute audit for NOH players:**

| Player | Player ID | Minutes Output | Minutes Official | Diff | PM Output | PM Official | PM Diff | Outlier? |
|---|---|---|---|---|---|---|---|---|
| David Wesley | 133 | 39.70 | 39.70 | 0.0 | -6.0 | -6.0 | 0.0 | No |
| P.J. Brown | 136 | 41.58 | 41.58 | 0.0 | -8.0 | -8.0 | 0.0 | No |
| George Lynch | 248 | 13.95 | 13.95 | 0.0 | +2.0 | +2.0 | 0.0 | No |
| Lee Nailon | 1924 | 38.57 | 38.57 | 0.0 | -17.0 | -17.0 | 0.0 | No |
| Chris Andersen | 2365 | 18.07 | 18.07 | 0.0 | -13.0 | -13.0 | 0.0 | No |
| Dan Dickau | 2424 | 28.27 | 28.27 | 0.0 | -7.0 | -7.0 | 0.0 | No |
| **Al Harrington** | **2454** | **24.95** | **26.17** | **-1.22** | **-11.0** | **-11.0** | **0.0** | **YES** |
| JR Smith | 2747 | 12.51 | 12.52 | -0.003 | -11.0 | -11.0 | 0.0 | No |
| Matt Freije | 2782 | 18.08 | 18.08 | 0.0 | -5.0 | -5.0 | 0.0 | No |
| Lonny Baxter | 2437 | 4.32 | 4.32 | 0.0 | -4.0 | -4.0 | 0.0 | No |

Only Harrington has a minute mismatch. His PM is identical (+/- = 0.0) despite the minute gap — the pipeline and official agree on his plus-minus even though they disagree on his minutes.

**Cross-source minute comparison for Harrington (game total):**

| Source | Harrington Minutes |
|---|---|
| Pipeline output | 24.95 |
| BBR boxscore | ~24.95 (matches output) |
| Official NBA boxscore | 26.17 |
| tpdev_box | ~26.17 (matches official) |
| pbpstats_player_box | ~31.73 |
| tpdev_pbp (full_pbp_new) | ~32.70 |

**No two independent source families agree.** Output/BBR say ~25 min. Official/tpdev_box say ~26 min. pbpstats_box/tpdev_pbp say ~32 min. The 8-minute spread across sources makes it impossible to determine the "correct" answer.

**Why no override is possible:** The intraperiod candidate engine returns `insufficient_local_context` for both P2 deadball-window candidates (at 5:30 and 4:34). The first issue event (E162 at 6:31) predates both candidate windows — the pipeline's lineup is wrong *before* any identifiable repair point.

---

### D2. `0020000628` — NJN @ TOR, Period 2 (2001, Block B, `contradiction_mixed_source_case`)

**Event-on-court issue row:**

| Event | Period | Clock | Event Class | Player | Player ID | Team | Status | Current Lineup | Event Description |
|---|---|---|---|---|---|---|---|---|---|
| E227 | 2 | 2:23 | StatsFoul | Keith Van Horn | 1496 | NJN | off_court_event_credit | [950, 271, 2030, 446, 1425] | Van Horn S.FOUL (P2.PN) |

Van Horn commits a shooting foul at 2:23 Q2. The sub (Van Horn FOR Williams) is at E229, same clock. Pipeline processes sub first.

**Source-by-source evidence:**

| Source | Van Horn on court at 2:23 Q2? |
|---|---|
| Raw `playbyplayv2.parq` | **Yes** — foul event (E227) comes before sub (E229) in EVENTNUM order |
| Cached `pbpv3` | **Yes** — same event sequence |
| `full_pbp_new` (tpdev) | **Yes** — has Van Horn on the 2:23 possession lineup |
| Pipeline (current) | **No** — processes sub first, Van Horn off court when foul is credited |

**Scratch validation result (`_tmp_validate_0020000628_window_20260322_v1`):**

Tested window P2 E227-E230 with Van Horn on court:

| Metric | Before | After window |
|---|---|---|
| Minutes mismatches | 1 (Van Horn -0.25) | **1** (Van Horn -0.25 — unchanged) |
| PM mismatches | 0 | **2** (Van Horn -2, Aaron Williams +2) |
| Event-on-court issues | 1 (E227 Van Horn foul) | **1** (E229 SUB: Van Horn FOR Williams) |

The blocker just moved from the foul to the sub. The 0.25-minute tail stayed identical. Two new PM mismatches appeared. **Rejected.**

**Full minute audit (selected NJN players):**

| Player | Minutes Output | Minutes Official | Diff | PM Output | PM Official | PM Diff |
|---|---|---|---|---|---|---|
| Keith Van Horn | 32.73 | 32.98 | -0.25 | -12.0 | -14.0 | +2.0 |
| Aaron Williams | 33.67 | 33.67 | 0.0 | -18.0 | -16.0 | -2.0 |
| Stephon Marbury | 37.78 | 37.78 | 0.0 | -1.0 | -1.0 | 0.0 |
| Kenyon Martin | 30.57 | 30.57 | 0.0 | -2.0 | -2.0 | 0.0 |

Only Van Horn has a minute mismatch (0.25 min). The PM mismatches on Van Horn (+2) and Williams (-2) perfectly offset — this is a same-clock boundary attribution difference.

**Correction manifest entry:** Listed as `rejected` with note: "Rejected after scratch validation traded the original P2 E227 Van Horn foul blocker for P2 E229 and left the 0.25-minute Van Horn tail unchanged, while also introducing two small plus-minus mismatches."

---

## E. Block A Documented Holdouts (Lane 5)

### E1. `0029700159` — DEN @ VAN, Period 3 (1998, Block A)

**Event-on-court issue (from residual, after annotation):**

The single remaining event issue is P3 E349 — the broken "Lauderdale FOR Garrett" substitution — which has been annotated as `source_limited_upstream_error` and is no longer blocking.

**Source-limited annotation detail:**
- Annotation ID: `source_limited__0029700159__p3__t1610612743__player1051__e349__20260322`
- Evidence: "P3 E349 is the broken source substitution row 'Lauderdale FOR Garrett' even though Garrett was already removed earlier."
- Confidence: high

**Active lineup window overrides (2 windows):**

Window 1 (E349-366):
```json
{
  "period": 3, "team_id": 1610612743,
  "start_event_num": 349, "end_event_num": 366,
  "lineup_player_ids": [1517, 111, 271, 1504, 968]
}
```
Puts Lauderdale (968) in the prior Stith (179) slot. Bobby Jackson (1517), LaPhonso Ellis (111), Johnny Newman (271), Danny Fortson (1504) round out the five.

Window 2 (E367-372):
```json
{
  "period": 3, "team_id": 1610612743,
  "start_event_num": 367, "end_event_num": 372,
  "lineup_player_ids": [1517, 111, 271, 1504, 924]
}
```
Goldwire (924) replaces Lauderdale after the confirmed sub at 0:27. Confidence: high (0.93).

**Pre-window vs. post-window minute comparison (Denver players only):**

| Player | Pre-Window Minutes | Post-Window Minutes | Official | Pre Diff | Post Diff |
|---|---|---|---|---|---|
| Bryant Stith | 26.48 | 36.50 | 38.35 | **-11.87** | **-1.85** |
| Priest Lauderdale | 7.09 | 7.09 | 6.37 | +0.72 | +0.72 |
| Dean Garrett | 41.33 | 41.03 | 41.33 | 0.0 | -0.30 |
| Bobby Jackson | 40.42 | 40.72 | 40.72 | -0.30 | 0.0 |

The windows repair Stith's minutes from -11.87 to -1.85 (a huge improvement). Lauderdale and Garrett still have residual drift. The windows are clearly the least-bad state.

**Paired frontier-close comparison (live vs archived candidate state):**

The archived candidate state was recoverable exactly from the preserved scratch artifact and differs only in the first window boundary:

Live state:
```json
{
  "period": 3, "team_id": 1610612743,
  "start_event_num": 349, "end_event_num": 366,
  "lineup_player_ids": [1517, 111, 271, 1504, 968]
}
```

Archived candidate state:
```json
{
  "period": 3, "team_id": 1610612743,
  "start_event_num": 351, "end_event_num": 366,
  "lineup_player_ids": [1517, 111, 271, 1504, 968]
}
```

Shared trailing window:
```json
{
  "period": 3, "team_id": 1610612743,
  "start_event_num": 367, "end_event_num": 372,
  "lineup_player_ids": [1517, 111, 271, 1504, 924]
}
```

**Paired hardened rerun metrics (from `_tmp_phase6_0029700159_paired_compare_20260322_v1/summary.json`):**

| Metric | Live state | Archived candidate | Delta (candidate - live) |
|---|---:|---:|---:|
| Minutes mismatch rows | 5 | 5 | 0 |
| Minute outlier rows | 3 | 3 | 0 |
| PM mismatch rows | 4 | 4 | 0 |
| Max absolute minute diff | 11.8667 | 11.6667 | -0.2000 |
| Raw event issue rows | 1 | 2 | +1 |

**Conclusion:** recoverable, rerun cleanly, `tradeoff_or_worse`, keep the live state. The candidate only shaved `0.2` minutes off the broader raw rerun envelope while worsening raw issue rows from `1 -> 2`.

**Important blocker-count nuance:** the paired comparison above is a **raw rerun envelope** comparison. In the reviewed blocker inventory, the lone raw issue row (`P3 E349`) is already source-limited, so the current blocker state for `0029700159` is a **minute-only documented hold with 0 actionable blocker event rows**.

**Full current minute audit (all Denver players):**

| Player | ID | Minutes Output | Minutes Official | Diff | PM Output | PM Official | PM Diff | Outlier? |
|---|---|---|---|---|---|---|---|---|
| LaPhonso Ellis | 111 | 30.17 | 30.17 | 0.0 | -16.0 | -16.0 | 0.0 | No |
| Bryant Stith | 179 | 36.50 | 38.35 | **-1.85** | -3.0 | -5.0 | **+2.0** | **YES** |
| Johnny Newman | 271 | 35.38 | 35.38 | 0.0 | -1.0 | -1.0 | 0.0 | No |
| Joe Wolf | 341 | 17.38 | 17.38 | 0.0 | +3.0 | +3.0 | 0.0 | No |
| Anthony Goldwire | 924 | 7.75 | 7.65 | +0.10 | -5.0 | -2.0 | -3.0 | No |
| Priest Lauderdale | 968 | 7.09 | 6.37 | **+0.72** | +5.0 | +4.0 | **+1.0** | **YES** |
| Dean Garrett | 1051 | 41.03 | 41.33 | -0.30 | -2.0 | -2.0 | 0.0 | No |
| Danny Fortson | 1504 | 20.60 | 20.60 | 0.0 | 0.0 | 0.0 | 0.0 | No |
| Bobby Jackson | 1517 | 40.72 | 40.72 | 0.0 | -1.0 | -1.0 | 0.0 | No |
| Eric Washington | 1540 | 3.38 | 3.38 | 0.0 | +5.0 | +5.0 | 0.0 | No |

Vancouver's 10 players all match official minutes within 0.005 (rounding only). The entire minute residual is isolated to Denver's P3 Stith/Lauderdale/Garrett tradeoff.

---

### E2. `0029701075` — NYK @ BOS, Period 3 (1998, Block A)

**Event-on-court issue rows (13 total, all P3):**

| Event | Clock | Player | Player ID | Team | Event Description |
|---|---|---|---|---|---|
| E339 | 1:53 | Andrew DeClercq | 692 | BOS | DeClercq P.FOUL (P3.T4) |
| E342 | 1:45 | Terry Cummings | 187 | NYK | MISS Cummings 10' Jump Shot |
| E345 | 1:19 | Terry Cummings | 187 | NYK | Cummings 17' Jump Shot (19 PTS) (Childs 2 AST) |
| E345 | 1:19 | Chris Childs | 164 | NYK | (assist on same play) |
| E346 | 1:06 | Chris Childs | 164 | NYK | Childs P.FOUL (P2.PN) |
| E351 | 0:21.80 | Terry Cummings | 187 | NYK | Cummings 3' Layup (21 PTS) (Childs 3 AST) |
| E351 | 0:21.80 | Chris Childs | 164 | NYK | (assist on same play) |
| E399 | 3:39 | Chris Childs | 164 | NYK | Childs REBOUND (Off:0 Def:1) |
| E445 | 3:25 | Terry Cummings | 187 | NYK | Cummings 1' Driving Layup (17 PTS) (Childs 1 AST) |
| E445 | 3:25 | Chris Childs | 164 | NYK | (assist on same play) |
| E448 | 2:44 | Terry Cummings | 187 | NYK | Cummings REBOUND (Off:4 Def:4) |
| E473 | 2:23 | Tyus Edney | 721 | BOS | Edney Bad Pass Turnover (P3.T16) |
| E474 | 2:14 | Terry Cummings | 187 | NYK | MISS Cummings 5' Running Jump Shot |

All events show the same current NYK lineup: `[275, 891, 369, 317, 913]` (Houston, Oakley, Ward, Starks, L. Johnson). Childs and Cummings are NOT in this lineup despite being credited with shots, assists, rebounds, and fouls. BOS lineup: `[969, 952, 344, 1500, 65]` (Knight, Walker, Barros, Mercer, Minor). DeClercq and Edney are NOT in this lineup.

**Note the scrambled clock times:** Events E339-E351 go from 1:53 down to 0:21 (normal), then E399 jumps back to 3:39, E445 to 3:25, E448 to 2:44, E473 to 2:23, E474 to 2:14. This is the **time-reversal discontinuity** — the NBA PBP for this game has events placed out of chronological order in Q3.

**Source corroboration:**

| Source | Childs on court late Q3? | Cummings? | DeClercq? | Edney? |
|---|---|---|---|---|
| BBR PBP | **Yes** — plays at 3:39-0:21 | **Yes** — plays at 3:25-0:21 | **Yes** — on court, exits 7:19 | **Yes** — plays at 2:23 |
| tpdev (full_pbp_new) | **Yes** — on 4.2m-0.3m | **Yes** — on 4.2m-0.3m | **Yes** — on 12.0m-7.4m | **No** — off |
| Pipeline | **No** | **No** | **No** | **No** |

BBR resolves all 4. tpdev resolves 3 of 4 (Edney is only resolved by BBR). The NBA PBP simply never shows the substitution events that bring Childs and Cummings on court — they just start appearing in plays.

**Scratch correction attempts:**

1. BOS P3 starter correction: **No-op** — the proposed BOS P3 lineup already matched current inferred starters.
2. NYK window probe (tested multiple widths from E342-E351 to E399-E474): Each attempt improved Childs/Cummings minute accuracy but **worsened** Oakley and Larry Johnson by ~1 minute each, creating new severe outliers:

| Player | Without window | With NYK window | Official |
|---|---|---|---|
| Chris Childs | 14.73 | 15.80 | 15.68 |
| Terry Cummings | 17.37 | 18.43 | 18.40 |
| Charles Oakley | 32.45 | **31.38** | 32.45 |
| Larry Johnson | 35.78 | **34.72** | 35.78 |

Childs improves 0.95 → 0.12, Cummings improves 1.03 → 0.03, but Oakley worsens 0.0 → **-1.07** and Johnson worsens 0.0 → **-1.07**. Net: trades 2 severe outliers for 2 different severe outliers. **Rejected.**

7 different narrow window combinations were tested, all producing the same tradeoff pattern.

**Full current minute audit:**

| Player | ID | Team | Minutes Output | Minutes Official | Diff | PM Diff | Outlier? |
|---|---|---|---|---|---|---|---|
| Chris Childs | 164 | NYK | 14.73 | 15.68 | **-0.95** | 0.0 | **YES** |
| Terry Cummings | 187 | NYK | 17.37 | 18.40 | **-1.03** | 0.0 | **YES** |
| Andrew DeClercq | 692 | BOS | 12.62 | 12.93 | -0.32 | 0.0 | No |
| Tyus Edney | 721 | BOS | 9.99 | 10.17 | -0.18 | 0.0 | No |
| Allan Houston | 275 | NYK | 38.09 | 38.08 | +0.007 | 0.0 | No |
| Antoine Walker | 952 | BOS | 38.50 | 38.50 | 0.0 | 0.0 | No |
| Ron Mercer | 1500 | BOS | 44.78 | 44.78 | 0.0 | 0.0 | No |
| John Starks | 317 | NYK | 30.16 | 30.17 | -0.007 | 0.0 | No |
| Larry Johnson | 913 | NYK | 35.78 | 35.78 | 0.0 | 0.0 | No |

Childs and Cummings are the only severe outliers. All PM diffs are zero. The minute errors are pure lineup-tracking issues from the scrambled P3 event ordering.

---

## F. Same-Clock Accumulator Holdout (Lane 6)

### F1. `0021700394` — OKC @ CHA, full game (2018, Block E)

**No event-on-court issues.** This game has zero off-court event credits.

**Cluster-ledger summary (from `_tmp_trace_stints_0021700394_cluster_ledger_20260322_v1`):**

7 target same-clock clusters identified:

| Cluster | Period | Clock | Duration (sec) | OKC Affected | CHA Affected |
|---|---|---|---|---|---|
| P1 1:44 | 1 | 1:44 → 1:32 | 12.0 | Carmelo Anthony (out), Adams, Patterson, Huestis | Kemba Walker (out), Carter-Williams |
| P2 9:50 | 2 | 9:50 → 9:35 | 15.0 | Josh Huestis (out), Paul George (in) | — |
| P2 5:38 | 2 | 5:38 → 5:16 | 22.0 | Terrance Ferguson (out), Alex Abrines (in) | Treveon Graham (out), Jeremy Lamb |
| P2 0:57 | 2 | 0:57 → 0:48 | 9.0 | Nick Collison (out), Jerami Grant (in) | Dwight Howard (out), Frank Kaminsky (in), Treveon Graham (in) |
| P3 3:48 | 3 | 3:48 → 3:39 | 9.0 | Steven Adams (out), Josh Huestis | Dwight Howard (out), Frank Kaminsky (in) |
| P3 3:39 | 3 | 3:39 → 3:27 | 12.0 | — | Marvin Williams (out), Treveon Graham (in) |
| P3 3:27 | 3 | 3:27 → 3:13 | 14.0 | Alex Abrines (out), Terrance Ferguson | — |

**All 10 affected players and their minute drift:**

| Player | Player ID | Team | Minutes Output | Minutes Official | Diff (min) | Diff (sec) | In Cluster Map? |
|---|---|---|---|---|---|---|---|
| Carmelo Anthony | 2546 | OKC | 34.62 | 34.47 | +0.153 | +9.2 | Yes |
| Russell Westbrook | 201566 | OKC | 36.33 | 36.18 | +0.143 | +8.6 | **No** |
| Paul George | 202331 | OKC | 34.08 | 33.93 | +0.143 | +8.6 | Yes |
| Alex Abrines | 203518 | OKC | 26.33 | 26.18 | +0.150 | +9.0 | Yes |
| Jerami Grant | 203924 | OKC | 19.86 | 19.72 | +0.147 | +8.8 | Yes |
| Kemba Walker | 202689 | CHA | 38.97 | 38.82 | +0.150 | +9.0 | Yes |
| Marvin Williams | 101107 | CHA | 32.08 | 31.93 | +0.150 | +9.0 | Yes |
| Michael Kidd-Gilchrist | 203077 | CHA | 34.50 | 34.35 | +0.150 | +9.0 | **No** |
| Frank Kaminsky | 1626163 | CHA | 18.00 | 17.85 | +0.153 | +9.2 | Yes |
| Treveon Graham | 1626203 | CHA | 20.52 | 20.37 | +0.153 | +9.2 | Yes |

**Critical observation:** Russell Westbrook and Michael Kidd-Gilchrist have the same +9 second drift as the other 8 players, but they have **no direct stint boundary on any of the 7 target cluster clocks**. Their drift is an accumulator effect — the same-clock clusters cause a systematic ~9-second overcount that distributes across all players who were on court during any of those clusters, even if they didn't personally have a substitution event at the cluster boundary.

**Bounded four-source confirmation pass (from `_tmp_trace_stints_0021700394_source_compare_20260322_v1`):**

| Metric | Result |
|---|---|
| Target clusters reviewed | 7 |
| Candidate one-sided disagreements | **0** |
| Compared sources | `playbyplayv2`, cached `pbpv3`, `full_pbp_new`, `gamerotation_stints_v6` |
| Expected outcome | confirmation only |

**Sharper diagnosis from the bounded pass:**
- No single cluster produced a clearly one-sided bad-source diagnosis.
- `playbyplayv2`, cached `pbpv3`, and `gamerotation_stints_v6` align on the same-clock cluster ordering and substitution boundaries.
- In **6/7 informative clusters**, `full_pbp_new` is the coarsest source: it keeps the scoring or FT sequence in the possession row that ends at the sub clock and only flips to the post-sub lineup on the next possession row that starts at that same clock.
- That sharpens the diagnosis of a broader same-clock minute-accumulator / clock-attribution defect, but it does **not** justify a local window or a `source_limited_upstream_error` promotion.

**All PM diffs are exactly 0.0** for all 10 players. The pipeline's plus-minus is correct for every player despite the minute drift. This means the minute accumulation difference doesn't affect point-scoring attribution — it only affects the duration calculation.

**Why no fix is possible:**
- No single event range or period corrects the distributed drift
- The 7 clusters span P1-P3 and involve multiple different substitution pairs
- 2 of the 10 affected players aren't even involved in any substitution at the cluster boundaries
- A fix would require changing how the parser accumulates time across same-clock substitution-scoring events — a broad parser change that the project has explicitly frozen

---

## G. Summary: What the Cross-Source Evidence Shows

| Lane | Games | Event Rows | Minute Impact | PM Impact | Has a Fix Been Found? | Root Cause |
|---|---|---|---|---|---|---|
| Same-clock controls | 5 | 6 | Zero | ±1-2 per game | N/A (explicitly unfixed canaries) | Event-ordering convention |
| Rebound survivors | 5 | 5 | Zero | ±1-4 per game | 5 tested, all rejected | Same convention at rebound events |
| Period-start contradictions | 2 | 3 | Zero | ±1-2 per game | Sources genuinely disagree | Ambiguous 12:00 cluster ordering |
| Severe-minute insufficient-context holdout | 1 | 5 | 1.22 min | 0 | No safe local fix | Insufficient local context / split sources |
| Mixed-source contradiction case | 1 | 1 | 0.25 min | ±2 | 1 tested, rejected | Same-clock contradiction with small minute tail |
| Block A holdouts | 2 | 13 | 0.72-1.85 min | 0-3 per game | Multiple tested, all rejected/tradeoffs | Broken source data (scrambled PBP) |
| Accumulator holdout | 1 | 0 | 0.15 min × 10 players | Zero | No local fix possible | Distributed same-clock accumulator |

---

## H. Runtime Code Appendix

These verbatim code payloads were moved out of the main handoff to keep the main document focused on the current frontier and policy questions.

### Full Source: cautious_rerun.py (Season-Level Runner)

```python
from __future__ import annotations

import argparse
import importlib.util
import importlib.machinery
import json
import os
import sqlite3
import sys
import traceback
import types
import zlib
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

from boxscore_source_overrides import (
    apply_boxscore_response_overrides,
    load_boxscore_source_overrides,
    set_boxscore_source_overrides,
)

ROOT = Path(__file__).resolve().parent
NOTEBOOK_DUMP = ROOT / "0c2_build_tpdev_box_stats_version_v9b.py"
DEFAULT_DB = ROOT / "nba_raw.db"
DEFAULT_PARQUET = ROOT / "playbyplayv2.parq"
DEFAULT_OVERRIDES = ROOT / "validation_overrides.csv"
DEFAULT_BOXSCORE_SOURCE_OVERRIDES = ROOT / "boxscore_source_overrides.csv"
DEFAULT_FILE_DIRECTORY = ROOT
DEFAULT_PERIOD_STARTERS_PARQUETS = [
    ROOT / "period_starters_v6.parquet",
    ROOT / "period_starters_v5.parquet",
]

NOTEBOOK_LOCAL_IMPORT_PRELOADS = [
    "boxscore_source_overrides",
    "pbp_row_overrides",
    "pbp_stat_overrides",
    "player_id_normalization",
    "team_event_normalization",
    "boxscore_audit",
]


class _BoxscoreSourceLoader:
    def __init__(self, data: Dict[str, Any]):
        self._data = data

    def load_data(self, game_id: str | None = None) -> Dict[str, Any]:
        return self._data


def _ensure_local_pbpstats_importable() -> None:
    if importlib.util.find_spec("pbpstats") is not None:
        return
    candidates = []
    env_path = os.environ.get("PBPSTATS_REPO")
    if env_path:
        candidates.append(Path(env_path).expanduser())
    candidates.append(Path.home() / "Documents" / "GitHub" / "pbpstats")
    for candidate in candidates:
        if (candidate / "pbpstats").exists():
            sys.path.insert(0, str(candidate))
            if importlib.util.find_spec("pbpstats") is not None:
                return
    raise ModuleNotFoundError(
        "Could not import pbpstats. Set PBPSTATS_REPO or make the editable repo available."
    )


def _preload_local_module(module_name: str, module_path: Path) -> None:
    if module_name in sys.modules:
        return
    if module_path.suffix == ".pyc":
        loader = importlib.machinery.SourcelessFileLoader(module_name, str(module_path))
        spec = importlib.util.spec_from_loader(module_name, loader)
        if spec is None:
            raise ImportError(f"Could not build spec for sourceless module {module_name} at {module_path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        try:
            loader.exec_module(module)
        except Exception:
            sys.modules.pop(module_name, None)
            raise
        return
    module = types.ModuleType(module_name)
    module.__file__ = str(module_path)
    module.__package__ = ""
    sys.modules[module_name] = module
    try:
        source = module_path.read_text(encoding="utf-8")
        exec(compile(source, str(module_path), "exec"), module.__dict__)
    except Exception:
        sys.modules.pop(module_name, None)
        raise


def _load_raw_response(db_path: Path, game_id: str, endpoint: str) -> Dict[str, Any] | None:
    game_id = str(game_id).zfill(10)
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        row = conn.execute(
            "SELECT data FROM raw_responses WHERE game_id=? AND endpoint=? AND team_id IS NULL",
            (game_id, endpoint),
        ).fetchone()
        if not row:
            return None
        blob = row[0]
        try:
            data = json.loads(zlib.decompress(blob).decode())
        except (zlib.error, TypeError):
            if isinstance(blob, bytes):
                data = json.loads(blob.decode())
            else:
                data = json.loads(blob)
        if endpoint == "boxscore":
            return apply_boxscore_response_overrides(game_id, data)
        return data
    finally:
        conn.close()


def prepare_local_runtime_inputs(cache_dir, db_path=DEFAULT_DB, parquet_path=DEFAULT_PARQUET,
    overrides_path=DEFAULT_OVERRIDES, boxscore_source_overrides_path=DEFAULT_BOXSCORE_SOURCE_OVERRIDES,
    period_starter_parquet_paths=DEFAULT_PERIOD_STARTERS_PARQUETS,
    allow_unreadable_csv_fallback=False) -> Dict[str, Path]:
    """Hydrate runtime inputs to local per-run cache, returning resolved paths.
    As of the provenance hardening pass, defaults to fresh-copy mode:
    core inputs copied into per-run cache, overrides snapshotted per-run."""
    # Returns dict with keys: db_path, parquet_path, notebook_dump_path,
    #   preload_module_paths, overrides_path, period_starter_parquet_paths
    pass  # Full implementation in repo


def load_v9b_namespace(*, notebook_dump_path=NOTEBOOK_DUMP, preload_module_paths=None) -> Dict[str, Any]:
    """Load the notebook dump namespace. Preloads local modules, then exec's the
    safe prefix of the notebook dump (everything before the __main__ guard)."""
    _ensure_local_pbpstats_importable()
    module_paths = preload_module_paths or {
        module_name: ROOT / f"{module_name}.py"
        for module_name in NOTEBOOK_LOCAL_IMPORT_PRELOADS
    }
    for module_name in NOTEBOOK_LOCAL_IMPORT_PRELOADS:
        _preload_local_module(module_name, Path(module_paths[module_name]))
    source = notebook_dump_path.read_text(encoding="utf-8")
    marker = 'if __name__ == "__main__":\n    pass\n'
    prefix = source.split(marker, 1)[0] + marker
    namespace: Dict[str, Any] = {"__name__": "v9b_dump_safe", "__file__": str(notebook_dump_path)}
    exec(compile(prefix, str(notebook_dump_path), "exec"), namespace)
    return namespace


def install_local_boxscore_wrapper(namespace, db_path, file_directory=DEFAULT_FILE_DIRECTORY,
    allowed_seasons=None, allowed_game_ids=None, period_starter_parquet_paths=None) -> None:
    """Wrap namespace's get_possessions_from_df to inject local boxscore loader
    and period boxscore source loader."""
    from period_boxscore_source_loader import PeriodBoxscoreSourceLoader
    original_get_possessions = namespace["get_possessions_from_df"]
    period_boxscore_source_loader = PeriodBoxscoreSourceLoader(
        parquet_paths=period_starter_parquet_paths or DEFAULT_PERIOD_STARTERS_PARQUETS,
        allowed_seasons=allowed_seasons,
        allowed_game_ids=allowed_game_ids,
    )

    def wrapped_get_possessions(*args, **kwargs):
        pbp_df = args[0] if args else kwargs.get("pbp_df")
        loader = None
        if pbp_df is not None and not pbp_df.empty and "GAME_ID" in pbp_df.columns:
            game_id = str(pbp_df["GAME_ID"].iloc[0]).zfill(10)
            raw_boxscore = _load_raw_response(db_path, game_id, "boxscore")
            if raw_boxscore is not None:
                loader = _BoxscoreSourceLoader(raw_boxscore)
        kwargs.setdefault("boxscore_source_loader", loader)
        kwargs.setdefault("period_boxscore_source_loader", period_boxscore_source_loader)
        kwargs.setdefault("file_directory", str(file_directory.resolve()))
        return original_get_possessions(*args, **kwargs)

    namespace["get_possessions_from_df"] = wrapped_get_possessions


def run_lineup_audits(combined_df, season, output_dir, db_path, parquet_path,
    file_directory=DEFAULT_FILE_DIRECTORY) -> Dict[str, Any]:
    from audit_event_player_on_court import audit_event_player_on_court
    from audit_minutes_plus_minus import build_minutes_plus_minus_audit, summarize_minutes_plus_minus_audit

    minutes_audit_df = build_minutes_plus_minus_audit(combined_df, db_path=db_path)
    minutes_summary = summarize_minutes_plus_minus_audit(minutes_audit_df)
    problem_game_ids = sorted({
        str(game_id).zfill(10)
        for game_id in minutes_audit_df.loc[
            minutes_audit_df["has_minutes_mismatch"] | minutes_audit_df["has_plus_minus_mismatch"],
            "game_id",
        ].tolist()
    })
    issues_df, event_summary = audit_event_player_on_court(
        game_ids=problem_game_ids, parquet_path=parquet_path, db_path=db_path, file_directory=file_directory,
    )
    return {"minutes_plus_minus": minutes_summary, "problem_games": len(problem_game_ids), "event_on_court": event_summary}


def run_season(namespace, season, output_dir, parquet_path, db_path, file_directory,
    overrides_path, strict_mode, tolerance, max_workers, run_boxscore_audit_pass, run_lineup_audit_pass):
    """Run a single season through the pipeline. Returns (player_rows, failed_games, event_errors)."""
    namespace["DB_PATH"] = db_path
    namespace["clear_event_stats_errors"]()
    combined_df, error_df = namespace["process_season"](
        season=season, parquet_path=str(parquet_path), output_dir=".",
        validate=True, tolerance=tolerance, max_workers=max_workers,
        overrides_path=str(overrides_path), strict_mode=strict_mode,
        run_boxscore_audit=run_boxscore_audit_pass,
    )
    if run_lineup_audit_pass:
        lineup_audit_summary = run_lineup_audits(
            combined_df=combined_df, season=season, output_dir=output_dir,
            db_path=db_path, parquet_path=parquet_path, file_directory=file_directory,
        )
    return len(combined_df), len(error_df), len(namespace.get("_event_stats_errors", []))
```

### Full Source: StatsStartOfPeriod.get_period_starters() and key helper methods

This is the period starter resolution chain from the pbpstats fork — the single most important logic in the pipeline (~400 lines of the most critical paths).

```python
class StatsStartOfPeriod(StartOfPeriod, StatsEnhancedPbpItem):
    """stats.nba.com-specific start of period. Defines the resolution order."""

    def get_period_starters(self, file_directory=None):
        """Resolution order:
        1) Strict PBP inference (with overrides applied)
           - If result is internally impossible, treat as failure
        2) If strict succeeded AND v6 exists AND disagrees:
           - If manual override exists -> use strict
           - If disagreement matches opening-cluster pattern -> use strict
           - Otherwise -> use v6
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

        # Fallback chain
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


class StartOfPeriod(metaclass=abc.ABCMeta):
    """Base class. Contains ALL period-starter inference, override loading, fallback logic."""

    # ── Strict PBP Starter Inference ─────────────────────────────────

    def _get_players_who_started_period_with_team_map(self):
        """Walk period events to find starter candidates.
        For each event:
        - player1_id: if not seen as sub-in, they're a starter candidate
        - Substitution incoming_player_id: marked as subbed-in (not a starter)
        - Substitution outgoing_player_id: recorded as starter
        - Technical fouls/ejections excluded (player can get tech while on bench)
        Returns: (starters, player_team_map, player_first_seen_order, subbed_in_players)
        """
        starters = []
        subbed_in_players = []
        player_team_map = {}
        event = self
        event_order = 0

        while event is not None and not isinstance(event, EndOfPeriod):
            event_order += 1
            if (not isinstance(event, Timeout)
                and self._is_valid_starter_candidate(event.player1_id, known_team_ids)
                and hasattr(event, "team_id")):

                player_id = event.player1_id
                if not isinstance(event, JumpBall):
                    player_team_map[player_id] = event.team_id

                if isinstance(event, Substitution) and event.incoming_player_id is not None:
                    player_team_map[event.incoming_player_id] = event.team_id
                    if (event.incoming_player_id not in starters
                        and event.incoming_player_id not in subbed_in_players):
                        subbed_in_players.append(event.incoming_player_id)
                    if player_id not in starters and player_id not in subbed_in_players:
                        self._record_starter_candidate(player_id, starters, subbed_in_players, ...)

                is_technical_foul = isinstance(event, Foul) and (
                    event.is_technical or event.is_double_technical
                )
                if player_id not in starters and player_id not in subbed_in_players:
                    if not (is_technical_foul or isinstance(event, Ejection)):
                        self._record_starter_candidate(player_id, starters, subbed_in_players, ...)

            event = event.next_event

        # Fix: if a player's first explicit sub-in happens at same clock as first seen,
        # treat them as sub not starter
        for player_id, sub_secs in player_first_sub_in_seconds_remaining.items():
            first_seen_secs = player_first_seen_seconds_remaining.get(player_id)
            if first_seen_secs is None: continue
            if sub_secs + 0.001 >= first_seen_secs:
                starters = [s for s in starters if s != player_id]
                if player_id not in subbed_in_players:
                    subbed_in_players.append(player_id)

        return starters, player_team_map, player_first_seen_order, subbed_in_players

    # ── Previous Period Carryover ────────────────────────────────────

    def _fill_missing_starters_from_previous_period_end(self, starters_by_team):
        """When strict PBP finds <5 starters for a team, try to fill from
        previous period's ending lineup."""
        prev_lineups = getattr(self, "previous_period_end_lineups", None)
        if not isinstance(prev_lineups, dict): return starters_by_team
        for team_id, prev_players in prev_lineups.items():
            cur = starters_by_team.get(team_id, [])
            if len(cur) >= 5: continue
            team_subs = period_start_subs.get(team_id, {"in": set(), "out": set()})
            missing = [p for p in prev_players if p not in set(cur) and p not in team_subs["out"]]
            need = 5 - len(cur)
            starters_by_team[team_id] = cur + missing[:need]
        return starters_by_team

    # ── Override Loading ─────────────────────────────────────────────

    def _load_period_starter_overrides(self, file_directory):
        """Load from overrides/missing_period_starters.json and
        overrides/period_starters_overrides.json, merging both."""
        if file_directory is None: return {}
        override_files = [
            f"{file_directory}/overrides/missing_period_starters.json",
            f"{file_directory}/overrides/period_starters_overrides.json",
        ]
        merged = {}
        for path in override_files:
            if not os.path.isfile(path): continue
            with open(path) as f:
                data = json.loads(f.read(), cls=IntDecoder)
            for game_id, periods in data.items():
                merged.setdefault(game_id, {})
                for period, teams in periods.items():
                    merged[game_id].setdefault(period, {}).update(teams)
        return merged

    # ── V6 Conflict Resolution (Opening Cluster Logic) ───────────────

    def _should_prefer_strict_starters_over_exact_v6(self, strict_starters, local_boxscore_starters):
        """When strict PBP and v6 gamerotation disagree, prefer strict ONLY if
        the disagreement matches a period-start delayed-substitution cluster
        (technical/flagrant at period start where outgoing player is still on court).

        This is the opening-cluster carryover fix (March 20, 2026)."""
        if not self._is_exact_starter_map(strict_starters): return False
        if not self._is_exact_starter_map(local_boxscore_starters): return False
        start_seconds = self._get_period_start_seconds()
        saw_supported_difference = False
        for team_id, strict_players in strict_starters.items():
            local_players = local_boxscore_starters.get(team_id, [])
            if set(strict_players) == set(local_players): continue
            if not self._period_start_v6_diff_matches_delayed_sub_cluster(
                team_id, strict_players, local_players, start_seconds
            ):
                return False
            saw_supported_difference = True
        return saw_supported_difference

    # ── Strict Starter Validation ────────────────────────────────────

    def _strict_starters_are_impossible(self, starters_by_team):
        """Return True when a strict PBP starter map is internally contradictory.
        Checks: 2 teams, 5 per team, no duplicates, each classified as starter
        by substitution timing."""
        if not isinstance(starters_by_team, dict) or len(starters_by_team) != 2:
            return True
        sub_lookup = self._get_period_substitution_order_lookup()
        seen = set()
        for team_id, starters in starters_by_team.items():
            if not isinstance(starters, list) or len(starters) != 5: return True
            for pid in starters:
                if pid in seen: return True
                seen.add(pid)
                if self._classify_period_boxscore_candidate(team_id, pid, sub_lookup) is not True:
                    return True
        return False

    # ── Main Strict Pipeline ─────────────────────────────────────────

    def _get_period_starters_from_period_events(self, file_directory, ignore_missing_starters=False):
        """1. Walk events to find starters + team map
        2. Split by team
        3. Fill missing from previous period ending lineup
        4. Apply overrides
        5. Validate 5-per-team (unless ignore_missing_starters)"""
        starters, player_team_map, first_seen, subbed_in = (
            self._get_players_who_started_period_with_team_map()
        )
        starters = [pid for pid in starters if pid not in subbed_in]
        starters_by_team = self._split_up_starters_by_team(starters, player_team_map)
        starters_by_team = self._fill_missing_starters_from_previous_period_end(starters_by_team)
        starters_by_team = self._apply_period_starter_overrides(starters_by_team, file_directory)
        if not ignore_missing_starters:
            self._check_both_teams_have_5_starters(starters_by_team, file_directory)
        return starters_by_team
```
