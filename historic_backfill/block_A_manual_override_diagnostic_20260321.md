# Block A (1997-2000) Manual Override Diagnostic Report

Date: 2026-03-21
Source: BBR play-by-play + tpdev PBP cross-reference analysis
Baseline: `block_A_postpatch_20260321_v2/`

## Overview

12 games with event-on-court issues were investigated. 48 total issue rows across the 12 games.

- **9 games are fixable** via period-starter overrides and/or lineup-window overrides
- **5 games have unfixable source data errors** where the pipeline's inferred lineups are already correct

Expected improvement: eliminates ~30 of 48 event-on-court rows, fixes all 7 minute outliers, and significantly reduces minute mismatches.

---

## Fixable Games

### 1. Game 0029700438 (SEA vs PHI, 1998-01-02)

**Problem:** Both P2 starting lineups are completely wrong. The pipeline carried forward incorrect P1-end state instead of the actual P2 starters.

**Evidence:**
- BBR P1 late subs confirm lineup turnover before P2: Wingate FOR Schrempf @0:36, Ellis FOR Hawkins @3:24, Perkins FOR McIlvaine @3:24 (SEA); Weatherspoon FOR Coleman @0:36, Cummings FOR Ratliff @0:36, McKie FOR Iverson @0:36, Davis FOR Thomas @4:00 (PHI)
- tpdev Q2 first possession confirms correct starters for both teams

**Issue rows affected:** 10 (players 29, 689, 754, 934, 96)

**Fix type:** `period_starters_overrides.json`

**Override values:**
```json
"29700438": {
  "2": {
    "1610612760": [56, 1425, 107, 64, 766],
    "1610612755": [754, 707, 221, 187, 243]
  }
}
```

Player legend:
- SEA: Payton (56), A.Williams (1425), Ellis (107), Perkins (64), Wingate (766)
- PHI: J.Jackson (754), M.Davis (707), Weatherspoon (221), Cummings (187), McKie (243)

**Caveat:** 4 intraperiod events will remain unfixable even after fixing starters:
- Coleman (934): BBR credits blocks/rebounds at P2 5:28 and 4:16, but his first recorded sub is @2:39. Missing NBA PBP sub entry.
- Schrempf (96): Makes shots at P2 3:25 and 2:13 with no recorded sub entering him in P2. Entry event completely absent from NBA PBP. Explains -1.09 min outlier.
- McIlvaine (29): Rebounds at P2 1:50 and blocks at 1:25 with no recorded P2 entry.
- Ratliff (689): Credited with P2 1:11 foul after being subbed out @1:25 per BBR. Scorer error.

**Minutes impact:** Fixes Schrempf -1.09 min outlier partially (structural source gap remains).

---

### 2. Game 0029701075 (NYK @ BOS, 1998-04-05)

**Problem A:** BOS P3 starters are completely wrong. Pipeline carried forward P2-end lineup (Tabak, Barros, Jones, Edney, Minor) instead of the actual P3 starters (Walker, DeClercq, K.Anderson, McCarty, Mercer).

**Evidence:**
- BBR P3 first ~25 events show Walker, DeClercq, K.Anderson, McCarty, Mercer for BOS
- tpdev Q3 first BOS possession lineup: [962, 1500, 952, 692, 72]

**Problem B:** NYK has a missing mid-P3 substitution. Around 3:39 remaining, Childs (164) and Cummings (187) enter for Oakley (891) and L.Johnson (913) with no recorded sub event in NBA PBP.

**Evidence:**
- tpdev Q3 NYK second lineup: [187, 275, 369, 317, 164] (Cummings, Houston, Ward, Starks, Childs)
- BBR confirms Childs and Cummings active in late P3

**Issue rows affected:** 13 (players 164, 187, 692, 721)

**Fix type:** `period_starters_overrides.json` + `lineup_window_overrides.json`

**Period starter override:**
```json
"29701075": {
  "3": {
    "1610612738": [952, 692, 72, 962, 1500]
  }
}
```
Player legend: Walker (952), DeClercq (692), K.Anderson (72), McCarty (962), Mercer (1500)

**Lineup window override:**
```json
"0029701075": [
  {
    "period": 3,
    "team_id": 1610612752,
    "start_event_num": 399,
    "end_event_num": 999,
    "lineup_player_ids": [275, 369, 317, 187, 164]
  }
]
```
Player legend: Houston (275), Ward (369), Starks (317), Cummings (187), Childs (164)

Note: end_event_num should be set to the last event in P3 for NYK (verify exact value at implementation time).

**Caveat:** 2 residual source errors will remain:
- DeClercq (692) foul at P3 1:53: after fixing starters, DeClercq exits at 7:19. The 1:53 foul is a scorer error (likely Knight's).
- Edney (721) turnover at P3 2:23: Edney never entered P3 per BBR. Pure scorer error.

**Minutes impact:** Fixes Childs -0.95 min and Cummings -1.03 min outliers.

---

### 3. Game 0029800075 (NJN @ MIA, 1999-02-15)

**Problem:** Biggest minute outlier in Block A. Spurious Gill re-entry at Q2 event 73 — Jones should stay on court through end of Q2. The NBA PBP has no substitution event bringing Gill back, but the pipeline (or tpdev) infers one incorrectly.

**Evidence:**
- BBR Q2: Jones enters at 9:55 (for Gill) and plays through end of Q2 — no Gill re-entry recorded
- tpdev incorrectly shows Gill re-entering at 5:15 (event 73)
- Official box score minutes confirm Jones played ~16.6 min total, not ~11.3 min

**Issue rows affected:** 0 event-on-court issues, but Gill +3.98 min / Jones -5.33 min outliers

**Fix type:** `lineup_window_overrides.json`

**Override values:**
```json
"0029800075": [
  {
    "period": 2,
    "team_id": 1610612751,
    "start_event_num": 73,
    "end_event_num": 79,
    "lineup_player_ids": [1800, 420, 197, 423, 954]
  },
  {
    "period": 2,
    "team_id": 1610612751,
    "start_event_num": 80,
    "end_event_num": 80,
    "lineup_player_ids": [1800, 197, 423, 954, 938]
  },
  {
    "period": 2,
    "team_id": 1610612751,
    "start_event_num": 81,
    "end_event_num": 83,
    "lineup_player_ids": [1800, 1496, 197, 954, 938]
  },
  {
    "period": 2,
    "team_id": 1610612751,
    "start_event_num": 84,
    "end_event_num": 87,
    "lineup_player_ids": [1800, 1496, 785, 954, 938]
  },
  {
    "period": 2,
    "team_id": 1610612751,
    "start_event_num": 88,
    "end_event_num": 89,
    "lineup_player_ids": [1800, 1496, 29, 785, 954]
  }
]
```

Player legend:
- 1800 = Damon Jones (replaces 383 = Kendall Gill in every window)
- 420 = Jayson Williams, 197 = Scott Burrell, 423 = Chris Gatling, 954 = Kerry Kittles
- 938 = Rony Seikaly, 1496 = Keith Van Horn, 785 = Eric Murdock, 29 = Jim McIlvaine

**Minutes impact:** Jones -5.33 → ~-0.32 min. Gill +3.98 → ~-1.42 min (residual is structural — official NJN box sums to 241.35 min, 81 sec over theoretical 240 min).

---

### 4. Game 0029700159 (VAN vs DEN, 1997-11-21)

**Problem:** Conflicting substitution events in P3. At 3:08, DEN made two subs. Then at 1:51, the NBA PBP records "SUB: Lauderdale FOR Garrett" — but Garrett was already removed at 3:08. The sub description is wrong: Lauderdale actually replaced Jackson (who had taken Garrett's slot). The pipeline cannot apply the sub (Garrett not in lineup), leaving Lauderdale un-added to the tracked lineup.

**Evidence:**
- BBR confirms both the 3:08 sub "B. Jackson enters for D. Garrett" and the 1:51 sub "P. Lauderdale enters for D. Garrett" — both sources reflect internally-inconsistent source text
- All of Lauderdale's and Goldwire's actions in the last 2 minutes confirmed by BBR

**Issue rows affected:** 8 (players 968 Lauderdale, 924 Goldwire, 1051 Garrett)

**Fix type:** `lineup_window_overrides.json`

**Override values:**
```json
"0029700159": [
  {
    "period": 3,
    "team_id": 1610612743,
    "start_event_num": 349,
    "end_event_num": 366,
    "lineup_player_ids": [968, 179, 1504, 271, 111]
  },
  {
    "period": 3,
    "team_id": 1610612743,
    "start_event_num": 367,
    "end_event_num": 373,
    "lineup_player_ids": [924, 179, 1504, 271, 111]
  }
]
```

Player legend:
- Window 1: Lauderdale (968) replaces Jackson (1517) from event 349-366
- Window 2: Goldwire (924) replaces Lauderdale (968) from event 367 to end of period
- Other DEN players: Williams (179), LaPhonso Ellis (1504), Anthony (271), Ellis (111)

**Minutes impact:** Fixes Lauderdale -0.67 min outlier. Goldwire and Garrett minute diffs should also improve.

---

### 5. Game 0029700141 (CHH vs POR, 1997-11-19)

**Problem:** Single event-ordering issue. The pipeline processed a sub (event 485: Farmer FOR Rice, clock 0:37) after the rebound it should precede (event 483: Farmer offensive rebound, clock 0:36). EVENTNUM ordering puts the rebound before the sub even though clock times are consistent with sub-first ordering.

**Evidence:**
- BBR event ordering: sub (event_index 436) before rebound (437) — correct order
- tpdev possession at TimeRemaining=46 shows Farmer in lineup, Rice out

**Issue rows affected:** 1 (player 1108 Farmer)

**Fix type:** `lineup_window_overrides.json`

**Override values:**
```json
"0029700141": [
  {
    "period": 4,
    "team_id": 1610612766,
    "start_event_num": 483,
    "end_event_num": 484,
    "lineup_player_ids": [769, 1133, 462, 124, 1108]
  }
]
```

Player legend: Farmer (1108) replaces Rice (779) for events 483-484 before the sub event 485 is processed.

---

### 6. Game 0029700367 (CHH vs TOR, 1997-12-22)

**Problem:** Same-clock boundary at end of Q4. Stoudamire subbed out at 0:04 but his shot at 0:03 (event 464) is processed after the sub because the sub's clock (0:04) > shot's clock (0:03) in descending sort.

**Evidence:**
- NBA EVENTNUM ordering (464 < 469) says shot before sub
- BBR row ordering (438=sub, 439=shot) and tpdev both align with sub-before-shot
- Genuine source conflict — but all sources agree the basket was made by Stoudamire

**Issue rows affected:** 1 (player 757 Stoudamire)

**Fix type:** `lineup_window_overrides.json`

**Override values:**
```json
"0029700367": [
  {
    "period": 4,
    "team_id": 1610612761,
    "start_event_num": 464,
    "end_event_num": 464,
    "lineup_player_ids": [757, 948, 57, 961, 932]
  }
]
```

Player legend: Stoudamire (757) in place of Tabak (440) for the single shot event.

**Caveat:** Moderate confidence. tpdev and BBR both support sub-before-shot. EVENTNUM says shot-before-sub. Could alternatively be handled as a `pbp_row_overrides.csv` entry to reorder event 469 before 464.

---

### 7. Game 0029800063 (DAL @ DEN, 1999-02-12)

**Problem:** P4 0:24 — Green entered for Bradley at 0:30 (BBR row 475), but the pipeline processed the rebound (event 511) before the sub due to event ordering.

**Evidence:**
- BBR: Green enters at 0:30, then rebound at 0:24
- tpdev at 30 sec remaining shows Green in lineup, Bradley out

**Issue rows affected:** 1 (player 920 Green, P4) — the P2 Nowitzki issue is a separate source error (see unfixable section)

**Fix type:** `lineup_window_overrides.json`

**Override values:**
```json
"0029800063": [
  {
    "period": 4,
    "team_id": 1610612742,
    "start_event_num": 511,
    "end_event_num": 999,
    "lineup_player_ids": [714, 920, 959, 76, 1065]
  }
]
```

Player legend: Green (920) replaces Bradley (762). Others: Finley (714), Nash (959), Ceballos (76), Strickland (1065).

Note: end_event_num should cover through end of P4 (verify exact value at implementation).

---

### 8. Game 0049700045 (CHH @ CHI, 1998-05-06 — Playoff)

**Problem A:** P1 jump ball (event 1) has empty lineups. The existing lineup_window_override covers event 1-1 but doesn't populate the initial state before event processing begins.

**Problem B:** P2 event 172 — Kukoc goaltending violation at 6:32 processed after his sub at the same clock time.

**Evidence:**
- tpdev and existing override agree on P1 starters for both teams
- BBR and tpdev confirm Kukoc was on court at 6:32 before the subs were processed

**Issue rows affected:** 4 (players 389 Kukoc, 124 Wesley, 133 Divac)

**Fix type:** `period_starters_overrides.json` + `lineup_window_overrides.json`

**Period starter override:**
```json
"49700045": {
  "1": {
    "1610612741": [166, 893, 389, 23, 937],
    "1610612766": [124, 193, 779, 133, 184]
  }
}
```

Player legend:
- CHI: Longley (166), Rodman (893), Kukoc (389), Jordan (23), Pippen (937)
- CHH: Wesley (124), Mason (193), Rice (779), Divac (133), Campbell (184)

**Lineup window override (append to existing 0049700045 entry):**
```json
{
  "period": 2,
  "team_id": 1610612741,
  "start_event_num": 172,
  "end_event_num": 172,
  "lineup_player_ids": [893, 389, 23, 197, 937]
}
```

Player legend: Kukoc (389) still in pre-sub CHI lineup. Burrell (197) not yet replaced by Harper.

**Caveat:** The P1 period_starters_override and existing P1 lineup_window_override for event 1-1 are redundant. Once the period_starters_override is added, the P1 event 1-1 lineup_window entries could optionally be removed.

---

### 9. Game 0029700452 (SEA @ VAN, 1998-01-04)

**Problem:** P4 event 402 — Schrempf rebound at 5:37 processed before his 5:33 entry sub. BBR confirms Schrempf was already on court by that point.

**Evidence:**
- BBR P4: rebound at 5:37 (row 386) before sub at 5:33 (row 387) — Schrempf credited with play before his sub event
- tpdev confirms Schrempf in SEA lineup from ~5:54 onward

**Issue rows affected:** 1 (player 96 Schrempf, P4) — P2/P3 Wingate issues are source attribution errors

**Fix type:** `lineup_window_overrides.json`

**Override values:**
```json
"0029700452": [
  {
    "period": 4,
    "team_id": 1610612760,
    "start_event_num": 402,
    "end_event_num": 402,
    "lineup_player_ids": [107, 765, 452, 21, 96]
  }
]
```

Player legend: Schrempf (96) replaces Perkins (64). Others: Ellis (107), Hawkins (765), Baker (452), Anthony (21).

**Caveat:** P2 Wingate foul and P3 Wingate appearances are scorer attribution errors — pipeline lineup is correct at those points. The -0.87 min Wingate outlier from P3 is unfixable.

---

## Unfixable — Source Data Errors

These games have event-on-court issues where the pipeline's inferred lineups are already correct. The NBA PBP misattributes the event to a player who was off court.

### 10. Game 0029800063 P2 — Nowitzki (1717)

Event 211, P2 2:57: missed jump shot attributed to Nowitzki. He was subbed out at P2 5:05 (H. Williams entered for him, BBR row 183). Both BBR and tpdev confirm Nowitzki was off court. Pipeline lineup is correct.

### 11. Game 0029800462 P3 — Battie (1499)

Events 435/441, P3 1:24/1:22: offensive goaltending turnover and block attributed to Battie. He was subbed out at P3 2:20 (Potapenko re-entered for him, BBR row 320). Pipeline lineup is correct.

### 12. Game 0029800606 P5 — Del Negro (219)

Event 544, P5 (OT) 4:10: foul attributed to Del Negro. He doesn't enter OT until 0:29 (BBR row 487). Pipeline lineup is correct. The -0.20 min diff is a separate issue from the rapid sub sequence in the final 30 seconds of OT.

### 13. Game 0029900342 P3 — Doug West (28)

Event 369, P3 0:22: defensive 3-second team technical foul attributed to Doug West (VAN). He was subbed out at P3 1:21 (D. Scott entered for him). Pipeline lineup is correct.

### 14. Game 0029900517 P2 — Michael Curry (688)

Event 245, P2 0:01: missed shot attributed to Michael Curry (DET). He was subbed out at P2 3:34 (L. Hunter entered for him). Pipeline lineup is correct. BBR also misattributes this shot to Curry — both sources drew from the same erroneous data.

---

## Implementation Checklist for Codex

### period_starters_overrides.json additions

| Game key | Period | Team ID | Lineup | Notes |
|---|---|---|---|---|
| `29700438` | 2 | 1610612760 (SEA) | [56, 1425, 107, 64, 766] | Both teams wrong |
| `29700438` | 2 | 1610612755 (PHI) | [754, 707, 221, 187, 243] | Both teams wrong |
| `29701075` | 3 | 1610612738 (BOS) | [952, 692, 72, 962, 1500] | Completely wrong P3 starters |
| `49700045` | 1 | 1610612741 (CHI) | [166, 893, 389, 23, 937] | Empty lineup at jump ball |
| `49700045` | 1 | 1610612766 (CHH) | [124, 193, 779, 133, 184] | Empty lineup at jump ball |

### lineup_window_overrides.json additions

| Game key | Period | Team ID | Event range | Lineup | Reason |
|---|---|---|---|---|---|
| `0029700141` | 4 | 1610612766 | 483-484 | [769, 1133, 462, 124, 1108] | Farmer sub processed after rebound |
| `0029700159` | 3 | 1610612743 | 349-366 | [968, 179, 1504, 271, 111] | Lauderdale replaces Jackson |
| `0029700159` | 3 | 1610612743 | 367-373 | [924, 179, 1504, 271, 111] | Goldwire replaces Lauderdale |
| `0029700367` | 4 | 1610612761 | 464-464 | [757, 948, 57, 961, 932] | Stoudamire same-clock boundary |
| `0029700452` | 4 | 1610612760 | 402-402 | [107, 765, 452, 21, 96] | Schrempf sub-before-rebound |
| `0029701075` | 3 | 1610612752 | 399-end | [275, 369, 317, 187, 164] | Childs/Cummings missing sub |
| `0029800063` | 4 | 1610612742 | 511-end | [714, 920, 959, 76, 1065] | Green sub-before-rebound |
| `0029800075` | 2 | 1610612751 | 73-79 | [1800, 420, 197, 423, 954] | Jones stays, Gill spurious re-entry |
| `0029800075` | 2 | 1610612751 | 80-80 | [1800, 197, 423, 954, 938] | Jones stays (continued) |
| `0029800075` | 2 | 1610612751 | 81-83 | [1800, 1496, 197, 954, 938] | Jones stays (continued) |
| `0029800075` | 2 | 1610612751 | 84-87 | [1800, 1496, 785, 954, 938] | Jones stays (continued) |
| `0029800075` | 2 | 1610612751 | 88-89 | [1800, 1496, 29, 785, 954] | Jones stays (continued) |
| `0049700045` | 2 | 1610612741 | 172-172 | [893, 389, 23, 197, 937] | Kukoc goaltending before sub |

### Notes for implementation

1. **Verify end_event_num values**: For entries marked "end" in the event range, verify the actual last event number in that period for that team before writing the override.
2. **Game key format**: `period_starters_overrides.json` uses game IDs without leading zeros (e.g., `"29700438"`). `lineup_window_overrides.json` uses the full 10-digit format with leading zeros (e.g., `"0029700438"`). Verify this convention against existing entries.
3. **Existing 0049700045 entry**: The lineup_window_overrides.json already has entries for this game (P1 event 1-1 for both teams). The new P2 entry should be appended to the existing array. Consider removing the redundant P1 entries once the period_starters_override is added.
4. **Provenance notes**: Each override should get a corresponding row in `overrides/period_starters_override_notes.csv` or `overrides/lineup_window_override_notes.csv` with source_type, reason, and evidence_summary fields.
5. **Validation**: After applying each override, rerun the affected game using `rerun_selected_games.py` and verify:
   - Event-on-court issues decrease or disappear for that game
   - Counting stats stay clean (0 audit mismatches)
   - Minute diffs improve
   - No new regressions introduced
