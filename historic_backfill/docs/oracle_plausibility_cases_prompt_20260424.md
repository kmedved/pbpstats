We need an outside review of a set of NBA play-by-play lineup/parser hold cases. Please reason basketball-first and give a per-game recommendation. This is advisory only: do not write code. We will verify any suggested override or parser rule with scratch reruns before promoting it.

Project/context:

- Repo/workspace: `/Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev`.
- We are migrating/replacing historical tpdev/pbpstats-derived NBA lineup outputs.
- The current task is to classify and possibly repair remaining "open hold" games in a 1997-2020 audit frontier.
- A game can have event-on-court issues, player-game minute diffs, plus-minus reference diffs, or source/reference contradictions.
- We are using a "plausibility-first" rubric: when same-clock feed order conflicts with basic basketball causality, the coherent basketball story should be the primary signal. But we still need to be explicit about minute/PM tradeoffs and release-policy consequences.
- We are not asking you to trust NBA raw PBP, cached pbpv3, BBR PBP, tpdev possessions, or official box minutes as sacred. Compare them as evidence. If a source implies impossible basketball, call that out.
- Current date of this review is 2026-04-24.

Important current state:

- `0021700394` was recently fixed separately by a pbpstats period-start ordering rule and should not be treated as a remaining plausibility hold. The issue there was a missed shot at Q3 12:00 before the official start-of-period marker; moving the period marker before live action cleared it.
- `0020000628` was just promoted as a **partial production event-order correction**, not exact clearance. We added `E229 move_before E227` in `pbp_row_overrides.csv`. That clears the Van Horn event-on-court issue, but leaves a 0.25 minute and +/-2 PM source/reference residue. The reviewed policy keeps it non-release-blocking but research-open/documented-hold.

Attached context files:

- `plausibility_first_fix_candidates_20260424.md`: living note with detailed basketball-story reasoning for each case.
- `phase7_open_blocker_inventory_20260424_period_start_fix_v1.csv`: current frontier/inventory after the 0021700394 period-start fix and after the 0020000628 partial-promotion note was updated.
- `phase7_true_blocker_shortlist_20260424_period_start_fix_v1.csv`: current shortlist/action framing.
- `reviewed_frontier_policy_overlay_20260424_period_start_fix_v1.csv`: release-policy overlay. This is how raw-open leftovers become non-release-blocking documented holds or accepted boundary differences.
- `pbp_row_overrides.csv`: production row-order/drop overrides, including the newly promoted `0020000628` row.
- `open_game_pbp_bundle_20260423_v1/games/**/{README.md,*focus_windows.csv}`: focused PBP/source windows for the open-game cases. Some were produced before the very latest notes, so prefer the current prompt and current inventory when they conflict.
- `_tmp_residual_0020000628_row_move_promoted_v1/*` and `_tmp_rerun_0020000628_row_move_promoted_v1/*` selected files: focused validation artifacts for the promoted partial 0020000628 row move.

What I want from you:

For each individual remaining plausibility case below, return a table with:

1. Your recommended handling: promote local row override, insert/synthesize sub, suppress/reclassify sub, parser/systematic rule, documented hold, accepted boundary difference, source-limited, or defer/no-anchor.
2. The "what actually happened" basketball story in one or two sentences.
3. Whether my current provisional view is right, too aggressive, too conservative, or missing an important rule/source detail.
4. The expected tradeoff: event issues, minutes, PM, counting stats, source disagreement.
5. The exact validation gate you would require before promotion.
6. Any cases that should be grouped into one systematic rule rather than fixed one by one.

Please be concrete. If you think a row move is plausible, name the event/cluster and whether it should be before or after the anchor. If you think there is no safe anchor, say what evidence would be needed.

Current per-game facts and provisional view:

1. `0020000628` - Nets at SuperSonics, 2001-01-27
   - Failure before promotion: Van Horn had an off-court shooting foul at Q2 `2:23`, event `E227`; raw row `E229 SUB: Van Horn FOR Williams` came later at the same clock.
   - Scratch/prod row move: `E229 move_before E227`.
   - Result: event-on-court issues go to `0`; no new sub blocker; counting stats clean. Remaining residue: Van Horn `-0.25` minutes and `-2` PM, Aaron Williams `+2` PM.
   - Basketball story: Williams owns the Q2 `2:38` dunk/and-one; Van Horn enters at `2:23` and commits the foul. Van Horn cannot be moved back to `2:38` without contradicting Williams' scoring play and tpdev possession lineup.
   - My view: promote row move as event-order correction, keep remaining minute/PM residue as documented source/reference tradeoff.

2. `0020400335` - Spurs at Hornets, 2004-12-17
   - Failure: Junior Harrington has five Q2 off-court event credits (`6:31` assist, `4:47` missed layup, `3:07` made jumper, `0:05.50` shooting foul, `0:00.50` missed layup). Parser has J.R. Smith, not Harrington, in that slot.
   - BBR PBP reportedly has `J. Harrington enters for J. Smith` at Q2 `7:59`; tpdev possession lineups reportedly put Harrington on from `7:59`.
   - Official/tpdev box minutes and PM prefer a different Harrington/Smith split; BBR/event/possession story prefers Harrington.
   - My view: missing-sub repair at Q2 `7:59`, Harrington for J.R. Smith, accepting official box PM as less credible for this pair. Need validation/documented reference tradeoff.

3. `0020900189` - Bulls at Nuggets, 2009-11-21
   - Failure: Q2 `12:00 SUB: Lawson FOR Billups` is incompatible with early Q2 events where Lawson is active and Billups appears to enter later.
   - Basketball story: Lawson started Q2 or was already active; Billups was not the player Lawson replaced at `12:00`.
   - My view: stale/phantom period-start sub row; likely suppress/reclassify rather than force Billups into Q2 starter lineup.

4. `0021300593` - Heat at Bobcats, 2014-01-18
   - Failure: Q2 opening exact-clock cluster: Cole shooting foul, Sessions FT1, `SUB: Mason Jr. FOR Cole`, Sessions FT2, later Mason foul.
   - Basketball story: Cole starts Q2, commits foul, then Mason replaces him during the dead ball/free throws.
   - My view: foul-before-sub same-clock row-order/control rule. Minutes are exact; only small PM boundary split remains.

5. `0021700236` - Warriors at Nets, 2017-11-19
   - Failure: same-clock/instant-replay/control rows around P1 `4:03` and `2:47`.
   - Basketball story: Dinwiddie fouls first, then LeVert replaces him. At `2:47`, Green fouls, Warriors timeout, Casspi enters for Green, Zeller misses FT, Casspi rebounds.
   - My view: two narrow row/order repairs or one cluster-order rule: foul before sub-out for Dinwiddie; Casspi sub before rebound in that later cluster.

6. `0021700337` - Spurs at Thunder, 2017-12-03
   - Failure: Huestis fouls at Q3 `1:00`; after timeout/replay `SUB: Lauvergne FOR Anderson`; Lauvergne shoots FTs. Parser flags Lauvergne/lineup semantics.
   - Basketball story candidate A: Lauvergne was the fouled player and must be on before foul. Candidate B: Anderson was injured/replaced, and Lauvergne is a legitimate replacement FT shooter.
   - My current view after Claude critique: do not blindly row-reorder; handle replacement-FT-shooter eligibility as a cluster/systematic rule if evidence supports injury/replacement semantics.

7. `0021700377` - Lakers at Hornets, 2017-12-09
   - Failure: Howard technical foul; Clarkson shoots technical FT at Q3 `3:03`; same-clock `SUB: Clarkson FOR Caldwell-Pope`.
   - Basketball story: Clarkson may be an entering player/designated technical FT shooter; technical FTs are control events, weaker evidence than live shots/rebounds/fouls.
   - My view: technical-FT shooter eligibility/cluster-control handling, not a simple live-lineup row-order override.

8. `0021700514` - Jazz at Warriors, 2017-12-27
   - Failure: Royce O'Neale credited with Q2 `5:09` shooting foul on Jordan Bell; parser has Joe Ingles in his slot.
   - Basketball story: O'Neale commits the foul, then Ingles replaces him during the same dead-ball/free-throw cluster.
   - My view: strong foul-before-sub row-order candidate.

9. `0021700917` - Warriors at Wizards, 2018-02-28
   - Failure: end-Q1 `0:04.20` cluster: Curry makes three, Iguodala take foul, Livingston-for-Curry sub, Oubre/Brooks technicals, Curry technical FTs.
   - Basketball story: either Curry shot technicals before the sub became effective, or technical-FT shooter eligibility should be handled separately from live lineup state.
   - My view: cluster-control/technical FT eligibility rule; do not automatically move Livingston sub for live minutes.

10. `0021800484` - Mavericks at Warriors, 2018-12-22
    - Failure: Q3 `3:54` Barea made basket, same-clock Warriors subs including Livingston for Curry, then Curry shooting foul and Barea and-one FT.
    - Basketball story: Curry's foul is part of the made-basket and-one. Curry cannot be subbed out before committing it; Livingston replaces him afterward.
    - My view: airtight foul-before-sub row-order candidate.

11. `0021801067` - Celtics at 76ers, 2019-03-20
    - Failure: Q3 `11:06` raw order says Brown for Smart before Smart ejection/flagrant foul sequence.
    - Basketball story: Smart commits flagrant 2, is ejected, Brown replaces him because of that ejection/dead ball.
    - My view: airtight flagrant/ejection-before-sub row-order candidate.

12. `0029600070` - Nuggets at Cavaliers, 1996-11-10
    - Failure: Sarunas Marciulionis minute-only tail: output `17:38`, official `18:00`, diff `-0:22`; event-on-court issues `0`, PM matches.
    - Basketball story: he plays real visible stints; parser math matches visible PBP; no event-level contradiction.
    - My current view: defer/no-anchor; likely whole-minute old-era box/reference issue, not an accumulator fix.

13. `0029600171` - Kings at Cavaliers, 1996-11-24
    - Failure: Michael Smith has six late-Q3 event credits while parser has him off court; raw PBP has no Q3 Smith re-entry.
    - Basketball story: Smith definitely played a late-Q3 stint; incoming is Smith, outgoing is unknown from raw PBP alone.
    - My view: insert-sub candidate, but needs BBR/rotation-source corroboration for outgoing player before promotion.

14. `0029600175` - Timberwolves at Bullets, 1996-11-25
    - Failure: Chris Carr misses a Q3 `0:22` three while parser has him off; minute scale about `14` seconds.
    - Basketball story: Carr was on for the last possession, but outgoing player is unknown and scale is tiny.
    - My view: defer/no-anchor unless BBR gives a specific sub row; one-possession insert is probably not worth override machinery.

15. `0029600204` - Bulls at Mavericks, 1996-11-29
    - Failure: Ron Harper `SUB: Harper FOR Jordan` is clocked at Q3 `0:26`, but Harper commits a foul at `0:35` and later misses at `0:00`.
    - Basketball story: Harper entered before the `0:35` foul/free-throw cluster; the `0:26` clock/placement is not credible as written.
    - My view: strong row-order/clock-normalization candidate: move Harper sub into/before `0:35` cluster before Harper foul.

16. `0029600332` - Warriors at SuperSonics, 1996-12-17
    - Failure: huge box/PBP rotation contradiction: Mullin parser `29:32` vs official `42:00`; DeClercq parser `19:50` vs official `8:00`. No event-on-court rows; BBR PBP reportedly agrees with raw NBA PBP and also lacks Mullin P4 entry.
    - Basketball story: visible PBP supports parser stints; box minutes disagree with event feeds. Earlier reputation-based claim that Mullin "must" have 42 minutes is not reliable.
    - My view: rotation-rebuild/source reconstruction only; no local plausibility override available without external rotation source.

17. `0029600370` - Mavericks at SuperSonics, 1996-12-22
    - Failure: paired ~12-minute swaps: Harper/Mashburn on Dallas and Ehlo/Schrempf on Seattle. No direct event-on-court contradictions; PM mostly old-era/unreliable.
    - Basketball story: could be missing starter re-entries or bad period starters, but raw/BBR event evidence has enough Harper/Ehlo activity that fame-based assumptions are unsafe.
    - My view: rotation-rebuild only; needs external rotation source or period-starter verification, not local override.

18. `0029600585` - Knicks at Hornets, 1997-01-24
    - Failure: Dell Curry credited with `8:35` foul and `6:46` miss before visible `6:38 SUB: Curry FOR Smith`.
    - Basketball story: Curry was already on before `8:35`; visible `6:38` entry is late, duplicate, or wrong.
    - My view: strong missing/late-entry story, but moving Curry back may create bigger minute tradeoff than current `0.2700` tail; needs validation/source corroboration.

19. `0029600657` - Kings at Spurs, 1997-02-03
    - Failure: two `Maxwell FOR Del Negro` rows in Q2 (`5:44`, `4:54`), but Del Negro makes a layup at `5:27`.
    - Basketball story: first Maxwell-for-Del-Negro row is duplicate/premature; Del Negro stays through `5:27`; Maxwell's real entry is likely `4:54`.
    - My view: suppress first duplicate/premature sub or otherwise treat real entry as later; accept possible official-minute tradeoff if event evidence wins.

20. `0029601163` - Trail Blazers at Grizzlies, 1997-04-17
    - Failure: minute-only tail: Dudley short `~39s`, Robinson long `~27s`; no event issues; PM matches; not a clean pair.
    - Basketball story: no event-level contradiction. Could be boundary timestamp/rounding/reference artifact.
    - My view: defer/no-anchor; do not override locally.

21. `0049600063` - Rockets at SuperSonics, 1997-05-15
    - Failure: four foul rows where parser has fouler already subbed out: Willis, Threatt, Cummings, Elie. Distributed across P2/P4.
    - Basketball story: fouler commits foul, then substitution happens during same dead ball/free throws.
    - My view: this is not one local override but a cluster-order systematic family: foul-before-sub for same-clock sub-outs of the fouler, with broad canaries.

22. `0029700159` - Nuggets at Grizzlies, 1997-11-21
    - Failure: Q3 `1:51 SUB: Lauderdale FOR Garrett`, but Garrett was already gone. Incoming Lauderdale is credible; outgoing player is impossible/stale.
    - Basketball story: the row's outgoing player is wrong. Existing live windows are least-bad but leave Stith/Lauderdale/Garrett minute tradeoff.
    - My view: reclassify-sub/source-limited tradeoff; identify true outgoing from lineup/rotation source if possible; otherwise keep documented hold.

23. `0029701075` - Knicks at Celtics, 1998-04-05
    - Failure: P3 chronology scrambled; Childs/Cummings/DeClercq/Edney have coherent late-Q3 event evidence, but local windows that insert Childs/Cummings steal minutes from Oakley/LJ and just move damage.
    - Basketball story: several players clearly have real late-Q3 actions; the repair is broader than one swap. Could involve wrong Knicks P3 starters or multiple missing subs.
    - My view: full P3 rotation reconstruction from BBR/tpdev/event context; no narrow local override.

Output format requested:

- Start with a concise executive summary: which cases you would promote now, which need systematic parser rules, which are documented holds/defer, and which require external rotation/source lookup.
- Then provide a per-game table covering all 23 games above.
- Then list "high confidence canary patches" in priority order, if any.
- Then list "do not fix locally" cases and why.
- Call out any cases where my current view is wrong or missing a basketball rule, especially around technical FTs, replacement FT shooters, fouls before subs, ejections, period-start substitutions, or old-era box-minute rounding.
- If you recommend a systematic rule, describe the guardrails narrowly enough to test.
