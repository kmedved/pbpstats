# Plausibility-First Fix Candidates

Living note for hold cases where the raw event order is less credible than the basketball story implied by player credit, lineup state, and minute accounting.

This note now has two layers:

- the original exploratory reasoning, preserved below as evidence; and
- the forced-coverage production decision set from 2026-04-24, where every remaining hold gets a concrete chosen lane.

The forced-coverage pass supersedes earlier `defer-no-anchor` wording as a production decision. "Do nothing" is represented explicitly as `status_quo_chosen` or `policy_overlay_chosen`, not as an unresolved hold.

## Working Rule

When same-clock feed order conflicts with basic basketball plausibility, plausibility should be the primary signal.

Do not treat raw `playbyplayv2`, cached `pbpv3`, BBR order, or official minute/PM references as sacred when they imply an impossible basketball sequence. The right fix candidate is the order that lets the parser tell a coherent basketball story while preferably improving or preserving:

- event-on-court audit rows
- substitution integrity
- minute reconciliation
- plus-minus reconciliation
- counting-stat boxscore audit

If plausibility and reference reconciliation cannot both be satisfied, document that explicitly as a reference/source tradeoff instead of hiding the basketball contradiction.

## First-Principles Rubric for Each Hold

Every hold gets reasoned through the same lens:

1. **What actually happened on the court?** Collect the credited plays (assists, fouls, shots, rebounds) and construct the smallest coherent basketball sequence that could have produced those credits.
2. **Why does the parser disagree?** Identify the specific raw-feed artifact driving the disagreement - same-clock row order, missing substitution, duplicate or stale sub, instant replay, scrambled event numbers, or accumulator leakage.
3. **What is the concrete fix?** Map each hold to one of: row-reorder, insert-sub, reclassify-sub, suppress-sub, accumulator-systematic, cluster-order-systematic, rotation-rebuild, or defer-no-anchor.
4. **What reference disagreement should be expected?** Name the minutes/PM source that will move and accept that movement as the cost of coherence.

Plausibility has **no anchor** when the parser's math already matches the visible raw inputs and no credited play contradicts the resulting lineup state. Minute-tail-only and catastrophic rotation-break cases fall here; they are not plausibility-override candidates, they are source-reconstruction tasks.

## Forced Coverage Decision, 2026-04-24

Policy: use event-level basketball plausibility first, but only when the repair has a concrete anchor. Live credited actions beat row order and reference MIN/PM when the fix is anchored. Technical FTs and replacement FTs are control/eligibility events, not ordinary live-lineup proof. Synthetic substitutions require both IN and OUT to be anchored. When no event contradiction exists, keep visible PBP output and name that as a chosen production lane.

Production artifacts:

- `pbp_row_overrides.csv`
- `phase7_open_blocker_inventory_20260424_forced_coverage_v1.csv`
- `phase7_true_blocker_shortlist_20260424_forced_coverage_v1.csv`
- `reviewed_frontier_policy_overlay_20260424_forced_coverage_v1.csv`
- `H_1997-2020_20260424_forced_coverage_v1`
- `phase7_reviewed_frontier_inventory_20260424_forced_coverage_v1`
- `phase7_reviewed_pm_reference_report_1997_2020_20260424_forced_coverage_v1`
- `reviewed_release_quality_sidecar_20260424_forced_coverage_v1`

New production row overrides promoted in this pass:

```csv
0029600204,move_before,338,339,Normalize Ron Harper FOR Jordan before Harper's Q3 0:35 foul; accept residual Harper/Jordan clock and PM source split
0020900189,drop,217,,Drop stale exact-start Lawson FOR Billups phantom row; Lawson is already active in Q2 and Billups re-enters later
```

Previously promoted row overrides retained:

```csv
0020000628,move_before,229,227,Move Van Horn substitution ahead of his same-clock Q2 shooting foul; residual minute/PM source split remains documented
0021800484,move_before,437,433,Move Curry's and-one shooting foul before same-clock Warriors substitutions so the foul precedes the dead-ball substitution block
0021801067,move_before,374,369,Move Smart flagrant cause before Brown replacement sub in same-clock ejection cluster
0021801067,move_after,369,372,Move Brown replacement after Smart ejection and Embiid technical in same-clock ejection cluster
```

Forced decision table:

| Game | Chosen lane | Production decision |
|---|---|---|
| `0020000628` | `local_override_chosen` | Keep `E229 before E227`; accept Van Horn/Williams source-reference residue. |
| `0020400335` | `synthetic_sub_chosen` | Choose Q2 `7:59 Harrington FOR J.R. Smith`; accept official PM disagreement. |
| `0020900189` | `local_override_chosen` | Drop stale exact-start `E217 Lawson FOR Billups`; fresh selected rerun leaves boundary PM only. |
| `0021300593` | `systematic_rule_chosen` | Exact-period-start live event rule: Cole foul before Mason same-clock sub. |
| `0021700236` | `systematic_rule_chosen` | Same-clock cluster causality: Dinwiddie foul before LeVert, Casspi post-timeout sub before FT rebound. |
| `0021700337` | `systematic_rule_chosen` | Replacement-FT-shooter eligibility; do not move Lauvergne before Anderson foul. |
| `0021700377` | `systematic_rule_chosen` | Technical-FT eligibility for Clarkson; no live-minute row movement. |
| `0021700514` | `systematic_rule_chosen` | Same-clock live-foul-before-sub-out for O'Neale before Ingles. |
| `0021700917` | `systematic_rule_chosen` | Technical-FT eligibility for Curry; no Livingston/Curry live-boundary move from technical FTs alone. |
| `0021800484` | `local_override_chosen` | Keep `E437 before E433`; accept boundary PM residue. |
| `0021801067` | `local_override_chosen` | Keep Smart flagrant/ejection before Brown replacement; scratch was exact-clean. |
| `0029600070` | `status_quo_chosen` | Keep visible Marciulionis stints; accept old-era 22-second minute tail. |
| `0029600171` | `status_quo_chosen` | Smith IN is real, but OUT is unanchored; keep current output with accepted source defect. |
| `0029600175` | `status_quo_chosen` | Carr one-possession evidence is too small and OUT is unknown; keep current output. |
| `0029600204` | `local_override_chosen` | Promote `E338 before E339`; accept Ron Harper minute and Harper/Jordan PM residual. |
| `0029600332` | `status_quo_chosen` | Keep visible Mullin/DeClercq stints over unanchored box-minute reconstruction. |
| `0029600370` | `status_quo_chosen` | Keep visible Dallas/Seattle rotations over unanchored role/box-minute reconstruction. |
| `0029600585` | `status_quo_chosen` | Curry's true entry is missing, but the OUT/legal boundary is unanchored; keep current output. |
| `0029600657` | `status_quo_chosen` | Keep current Maxwell/Del Negro output because both tested suppressions are worse. |
| `0029601163` | `status_quo_chosen` | Keep current Dudley/Robinson output; minute-only old-era drift. |
| `0029700159` | `policy_overlay_chosen` | Lauderdale IN is credible but true OUT is unanchored; keep current Denver P3 state. |
| `0029701075` | `policy_overlay_chosen` | Scrambled P3; keep current output rather than partial fake reconstruction. |
| `0049600063` | `systematic_rule_chosen` | Same-clock live-foul-before-sub-out rule family for four playoff foul rows. |

Validation:

- Selected rerun `_tmp_rerun_forced_coverage_row_overrides_v2`: `0020900189` and `0029600204` completed with `0` failed games, `0` event stat errors, and clean counting audits.
- Selected residual `_tmp_residual_forced_coverage_row_overrides_v2`: `0020900189` is boundary-only after dropping `E217`; `0029600204` has `0` event rows after `E338 before E339` and only the accepted Ron Harper/Jordan residual.
- Reviewed residual `H_1997-2020_20260424_forced_coverage_v1`: `tier1_release_ready=true`, `tier2_frontier_closed=true`, `release_blocking_game_ids=[]`, `research_open_game_ids=[]`.
- Reviewed frontier `phase7_reviewed_frontier_inventory_20260424_forced_coverage_v1`: 23 reviewed games, `0` release blockers, `0` research-open games.
- PM report and sidecar were rebuilt under the same overlay; sidecar join smoke passed.
- Focused tests passed: `tests/test_pbp_row_overrides.py`, `tests/test_build_reviewed_frontier_policy_overlay.py`, `tests/test_build_reviewed_frontier_inventory.py`, `tests/test_build_lineup_residual_outputs.py`.

## `0020000628` - Nets at SuperSonics, 2001-01-27

Status: promoted partial repair - production row-order correction plus documented residual hold.

Current reviewed state:

- reviewed classification: `documented_hold`
- reason family: `mixed_source_boundary_tail`
- issue row: `P2 E227`, Keith Van Horn, `off_court_event_credit`
- current NJN lineup at `E227`: Stephon Marbury, Johnny Newman, Kenyon Martin, Lucious Harris, Aaron Williams
- missing player: Keith Van Horn
- minute tail: Keith Van Horn is short by about `0.25` minutes
- BBR match: `200101270SEA`

Basketball story:

At `2:23` in Q2, Keith Van Horn is credited with a shooting foul on Vin Baker. The parser lineup still has Aaron Williams on the floor and does not have Van Horn. At the exact same clock, the feed later records `SUB: Van Horn FOR Williams`.

The plausible story is that Van Horn was already on the floor for that possession/free-throw sequence. The sub row is logged late inside the same-clock cluster.

Raw/source sequence:

```text
E227 Van Horn S.FOUL
E228 Baker Free Throw 1 of 2
E229 SUB: Van Horn FOR Williams
E230 Baker Free Throw 2 of 2
```

Plausibility-first sequence tested in scratch:

```text
E229 SUB: Van Horn FOR Williams
E227 Van Horn S.FOUL
E228 Baker Free Throw 1 of 2
E230 Baker Free Throw 2 of 2
```

Scratch override tested:

```csv
game_id,action,event_num,anchor_event_num,notes
0020000628,move_before,229,227,Same-clock plausibility repair: Van Horn must enter before his credited Q2 2:23 shooting foul.
```

Why this differed from the earlier rejected scratch window:

The rejected March scratch forced a lineup window over `P2 E227-E230` with Van Horn already on court. That was not equivalent to a row-order correction. It made `E229 SUB: Van Horn FOR Williams` incoherent because, by the time the parser reached the sub row, Van Horn was already in and Williams was already out.

A true row-order correction lets the parser process the substitution naturally while Williams is still present, then process the foul with Van Horn on court. That part validated.

Scratch validation result, 2026-04-24:

- `event_player_on_court` issues went to `0`.
- No new substitution-row blocker appeared at `E229`.
- Counting-stat boxscore audit stayed clean.
- But the game does **not** become exact: Van Horn remains short by `0.25` minutes and carries a `-2` plus-minus delta; Aaron Williams carries the offsetting `+2` plus-minus delta.
- Residual output still classifies `0020000628` as raw-open because of the Van Horn material minute issue.

Why the row move is not enough:

After the row move, the parser already gives Van Horn the Q2 stint from `2:23` to `0:00` (`143` seconds). The remaining `15` second official/BBR gap would require Van Horn to enter around `2:38`. That is not basketball-plausible.

Cross-source check (verified 2026-04-24 against `full_pbp_new.parq`):

- raw NBA PBP: `E224 Williams Slam Dunk (2 PTS)` and `E226 Williams FT 1 of 1 (3 PTS)` both at `2:38`; `E229 SUB: Van Horn FOR Williams` at `2:23`
- BBR PBP: same scoring credits, same sub at `2:23`
- `tpdev` possession lineups: Williams on the `158`-remaining (`2:38`) possession (length `15s`, ending at `143`); Van Horn enters on the `143`-remaining (`2:23`) possession
- four independent event-driven sources concur: Williams plays through `2:23`, Van Horn enters at `2:23`

Only the NBA official box (and `tpdev_box`, which is downstream of it) gives Van Horn `158` seconds. That source is the outlier.

Conclusion:

Promote the `E229 before E227` row move to production as an event-order correction, not as full clearance. It fixes the parser's basketball state at the foul row. The remaining minute/PM reference disagreement cannot be repaired without contradicting the `2:38` Aaron Williams scoring play and the `tpdev` possession state, so it stays under the reviewed `documented_hold` / `mixed_source_boundary_tail` policy row.

Plausibility-first principle this case illustrates: when raw PBP, BBR, `tpdev` possessions, and explicit scoring credits all agree and only the box-minute reference disagrees, the box reference is the wrong source. Keep this game as a documented reference disagreement, not a parser failure.

## `0020400335` - Spurs at Hornets, 2004-12-17

Status: preferred synthetic missing-sub canary, not implemented.

Current reviewed state:

- reviewed classification: `documented_hold`
- reason family: `severe_minute_insufficient_local_context`
- issue rows: `P2 E162`, `E181`, `E196`, `E223`, `E226`
- credited player on all five issue rows: Junior Harrington
- current NOH parser lineup during the Q2 stretch: has J.R. Smith, not Harrington
- BBR match: `200412170NOH`

Basketball story:

Junior Harrington is credited with too many real basketball actions for this to be a harmless stat-credit quirk:

- `6:31` Q2: assist on a Matt Freije jumper
- `4:47` Q2: missed layup blocked by Tony Massenburg
- `3:07` Q2: made 19-foot jumper
- `0:05.50` Q2: shooting foul on Malik Rose
- `0:00.50` Q2: missed layup blocked by Tim Duncan

A player cannot accumulate an assist, shot attempts, a make, and a shooting foul across six-plus minutes while never being on the court. The plausible basketball story is that Harrington was on the floor for the Q2 segment, and the parser missed the substitution that put him there.

Most plausible missing action:

```text
Q2 7:59
SUB: Junior Harrington FOR J.R. Smith
```

This fits the dead-ball context:

```text
8:00  J.R. Smith misses 26-foot three
7:59  Spurs rebound
7:59  Lonny Baxter loose-ball foul
7:59  missing/implicit Hornets sub: Harrington for Smith
7:59  Spurs substitutions
7:42  Duncan misses
7:41  Baxter shooting foul / Massenburg FTs
6:31  Harrington assists Freije
```

Candidate repair shape to test later:

```text
Insert or synthesize a Hornets substitution at P2 7:59:
  Junior Harrington in
  J.R. Smith out

Then let the normal parser carry Harrington through the rest of Q2.
```

If the current override machinery cannot synthesize a missing substitution row, the test equivalent is a NOH lineup window beginning at the `7:59` dead-ball cluster and replacing J.R. Smith with Junior Harrington through the end of Q2, with the important caveat that this is logically a missing-sub repair, not merely a cosmetic event window.

Why this differs from the old conservative classification:

The old classification centered official minute/PM reconciliation and treated the source split as unresolved. A plausibility-first read should give more weight to on-court event reality:

- raw NBA PBP credits Harrington with the Q2 actions
- BBR PBP includes `J. Harrington enters the game for J. Smith` at `7:59`
- `tpdev` possession lineups put Harrington, not Smith, on the floor from the `7:59` Q2 possession onward
- the current parser has Smith in that slot and therefore flags every Harrington Q2 event

Known reference tradeoff:

This candidate will move the game away from official NBA/tpdev-box plus-minus for Harrington and J.R. Smith, but that reference appears less credible than the on-court story. The scratch stint trace shows current parser stints already match the current player-game output. Applying the missing Q2 Harrington-for-Smith substitution would likely:

- clear all five Harrington off-court event credits
- move the Q2 `7:59` to `0:00` segment from J.R. Smith to Harrington
- align the Q2 lineup with event/stat plausibility and `tpdev_pbp`
- create or enlarge minute/PM deltas versus the official box reference

That tradeoff is not evidence against the basketball story. For this case, use the plausibility story as preferred: Harrington played the Q2 segment, Smith did not, and the NBA/tpdev-box PM split should be documented as a reference disagreement.

Reference split:

| Source | Harrington MIN | Harrington PM | J.R. Smith MIN | J.R. Smith PM |
|---|---:|---:|---:|---:|
| BBR box | `24:57` | `-20` | `12:31` | `-2` |
| NBA official | `26:10` | `-11` | `12:31` | `-11` |
| tpdev box | `26:10` | `-11` | `12:31` | `-11` |
| tpdev PBP / possessions | about `32:42` | `-20` | about `4:31` | `-2` |

The candidate repair should prefer the event/lineup/BBR-PM story over the official/tpdev-box PM story.

Acceptance gate before implementation:

- all five Harrington event-on-court issues clear
- the substitution/window does not create a new impossible six-man or duplicate-player state
- player counting stats remain clean
- expected minute/PM deltas versus official are documented as reference/source disagreement, not mistaken for a new parser failure

## Remaining Hold Games - Plausibility Review, 2026-04-24

Scope: every current hold in `phase7_open_blocker_inventory_20260424_rebound_sub_v1.csv` that is not one of the two detailed cases above.

This section is documentation only. It is not an implementation plan and does not add overrides.

### First-Principles Summary with Concrete Fix Recommendations

Fix category legend:
- **row-reorder** - the raw event order contradicts the credited basketball sequence; move a row within a same-clock cluster. Gated on confirming the parser actually honors event_num ordering (not an alternate sort key) when building lineup state.
- **insert-sub** - the raw feed is missing a substitution that event evidence demands. Operationally distinct from row-reorder: the override machinery has to synthesize a sub row, not just move an existing one. May require different override-table semantics from a simple `move_before` action.
- **reclassify-sub** - an existing sub row's IN/OUT identifiers are wrong or stale.
- **suppress-sub** - an existing sub row is duplicate/phantom and should be dropped because surrounding live evidence proves it never happened.
- **accumulator-systematic** - not a local override; fixes elapsed-time accounting so same-clock clusters do not charge time more than once.
- **cluster-order-systematic** - not a local override; fixes same-clock causality and eligibility semantics such as foul-before-sub ordering and technical/replacement FT shooter handling.
- **rotation-rebuild** - parser logic is fine; raw data is too corrupt to fix locally.
- **defer-no-anchor** - no event-level anchor for plausibility; leave open.

Rule notes verified against the current NBA rulebook on 2026-04-24:

- Technical FTs must be attempted by a player in the game when the technical is assessed; a substitute who was already beckoned in or recognized by officials before the technical is eligible. This makes technical-FT rows weaker live-lineup evidence than shots, rebounds, assists, or personal fouls.
- Personal-foul FTs normally belong to the offended player. Injury/ejection exceptions exist, but the replacement-shooter constraints matter and should be validated from the row identities and later re-entry pattern.
- A substitute is considered in the game when beckoned or recognized by an official. Substitutes generally cannot enter after a successful field goal unless the ball is dead because of a foul, technical foul, timeout, violation, or other listed exception. That matters for "late visible sub" cases where the player must have been on before committing a foul.

| Game | Basketball story | Proposed fix |
|---|---|---|
| `0020000628` | Van Horn must be on for his Q2 2:23 foul, and `E229 before E227` clears the event issue, but the remaining 15s minute/PM gap would require contradicting Aaron Williams' 2:38 and-one | **row-reorder + documented hold**: promote `E229 before E227`; keep remaining minute/PM residue as reviewed source/reference tradeoff |
| `0020400335` | Harrington plays Q2 `7:59`-end; five Q2 events credited to him; BBR has `Harrington FOR J.R. Smith` at 7:59 | **insert-sub canary** at Q2 7:59: synthesize Harrington IN for J.R. Smith if scratch clears events and documents the official PM tradeoff |
| `0020900189` | Lawson is live from Q2 start; the `Lawson FOR Billups` row is a stale period-start phantom | **suppress-sub candidate (Rule 5)**, gated on stronger Q2-start validation showing Lawson live before any Billups event |
| `0021300593` | Cole commits the Q2 12:00 foul, then Mason replaces him during Sessions' FTs | **cluster-order-systematic (Rule 6 period-start)**: starter Cole owns the credited foul; sub takes effect after the foul's dead-ball resolution |
| `0021700236` | At 4:03: Dinwiddie fouls, then LeVert replaces him. At 2:47: Casspi enters during Warriors timeout, then rebounds. | **cluster-order-systematic canary**: foul before same-clock sub-out; post-timeout Casspi sub before the FT rebound |
| `0021700337` | Huestis fouls Anderson at `1:00`, Spurs timeout + replay, `SUB: Lauvergne FOR Anderson`, Lauvergne shoots FTs. Replacement-FT-shooter semantics may make the raw sequence legitimate. | **cluster-order-systematic (Rule 3 replacement-FT-shooter)**: validate via `PLAYER2_ID=Anderson`; do not row-reorder Lauvergne before the foul |
| `0021700377` | Dwight Howard T-foul; Clarkson shoots the technical FT at `3:03`, and `SUB: Clarkson FOR Caldwell-Pope` is in the same cluster | **cluster-order-systematic (Rule 2 technical-FT eligibility)**: recognize Clarkson FT eligibility if beckoned/recognized before technical; do not infer live lineup from the FT alone |
| `0021700514` | O'Neale fouls Bell at Q2 5:09 before Ingles replaces him | **row-reorder candidate** only after locating the exact `Ingles FOR O'Neale` anchor row |
| `0021700917` | End-of-Q1 0:04 cluster: Curry made a 3, Iguodala take foul, `SUB: Livingston FOR Curry`, Oubre T-foul, Curry shoots technical FTs. The sub may be logged early, or the FT row may need control-event eligibility handling. | **cluster-order-systematic (Rule 2 technical-FT eligibility)**: treat technical FT shooter eligibility as distinct from live-possession lineup validation; do not move Livingston-for-Curry around technical FTs |
| `0021800484` | Curry fouls Barea on made basket (and-one at 3:54); Warriors subs happen after the foul | **promoted row-reorder**: `E437` Curry foul now precedes both same-clock Warriors subs (`E433 Looney FOR Green`, `E434 Livingston FOR Curry`) |
| `0021801067` | Smart's flagrant causes his ejection; Brown replaces Smart because of the ejection | **promoted (Rule 4 flagrant/ejection canary)**: `E374` Smart flagrant before `E369 Brown FOR Smart`, then `E369` after `E372` |
| `0029600070` | Marciulionis plays real minutes across all periods; parser 17:38, official 18:00, short by 22s; visible stints add up cleanly to 17:38 | **defer-no-anchor** - looks like official whole-minute rounding/reference tolerance, not accumulator leakage. Sign is wrong for an overcrediting accumulator family. |
| `0029600171` | Smith plays a late-Q3 Kings stint; six credited events; raw PBP has no Q3 Smith re-entry | **insert-sub** at or before Q3 5:11: Smith IN; outgoing needs BBR/rotation-source corroboration |
| `0029600175` | Carr shoots a 3 at Q3 0:22; 14s scale; outgoing unidentified | **defer-no-anchor** given 14s scale - override machinery cost exceeds benefit unless BBR provides a specific sub row |
| `0029600204` | Ron Harper fouls at Q3 0:35 but `SUB: Harper FOR Jordan` is clocked at 0:26; visible row is too late, but the legal pre-foul entry point is not established | **scratch-only clock-normalization hypothesis**: test `E338` into/before the 0:35 cluster, but do not promote without a legal pre-foul entry anchor |
| `0029600332` | Parser math matches visible PBP: Mullin 29:32 visible, DeClercq 19:50 visible. BBR PBP agrees with raw NBA PBP (also shows no Mullin P4, also has DeClercq entering at P3 9:56). Box minutes disagree with both event feeds. | **rotation-rebuild** from external source only - not a local fix candidate. BBR PBP cannot resolve this because it agrees with raw. |
| `0029600370` | Paired rotation damage on both teams (Harper +12.38 / Mashburn -12.16; Ehlo +11.97 / Schrempf -11.15). PBPs (raw and BBR) have plenty of Harper/Ehlo event evidence - no single event contradicts the parser's lineup state. | **rotation-rebuild** from external source only - no event-level anchor. |
| `0029600585` | Dell Curry must be on for 8:35 foul and 6:46 miss; visible `SUB: Curry FOR Smith` at 6:38 is late | **source-limited missing/late-entry hold** until a legal earlier entry/outgoing player is identified |
| `0029600657` | Del Negro makes a layup at 5:27 after a `5:44 Maxwell FOR Del Negro`, but official minutes prefer some earlier Maxwell time | **rejected local suppress candidate**: dropping either duplicate row leaves a blocker; keep documented hold/source tradeoff |
| `0029601163` | Dudley -39s, Robinson +27s; not cleanly paired; no event issues; PM matches | **defer-no-anchor** - speculating about boundary timestamp artifact without event-level evidence |
| `0029700159` | `SUB: Lauderdale FOR Garrett` at P3 1:51 is broken - Garrett was already gone; incoming is Lauderdale, outgoing is someone else (possibly Stith) | **reclassify-sub**: accept Lauderdale IN; identify true outgoing from lineup state; Stith minute tradeoff remains |
| `0029701075` | P3 events scrambled; Childs/Cummings/DeClercq/Edney credited with coherent late-Q3 actions | **rotation-rebuild** for Knicks P3: full window reconstruction needed - local swaps just move minute damage to Oakley/LJ |
| `0049600063` | Four foul-before-sub boundary rows across P2 and P4 (Willis, Threatt, Cummings, Elie) | **cluster-order-systematic (Rule 1 foul-before-sub-out)**: broad rule canary — four distributed examples in one playoff game |

### Oracle Review Deltas, 2026-04-24

High-confidence scratch queue from the outside review:

1. `0021800484`: **validated/promoted 2026-04-24**. `E437 move_before E433` clears the Curry and-one foul blocker with clean counting stats, zero minute mismatches, and fewer PM reference rows.
2. `0021801067`: **validated/promoted 2026-04-24**. `E374 move_before E369` plus `E369 move_after E372` clears the Smart flagrant/ejection blocker exactly.
3. `0029600657`: **scratch rejected 2026-04-24**. Dropping the `5:44` row preserves plausibility but worsens the minute/PM split; dropping the `4:54` row improves reference reconciliation but creates a Del Negro off-court layup blocker.
4. `0020400335`: synthesize Q2 `7:59 Harrington FOR J.R. Smith` if scratch confirms all five Harrington rows clear and the expected official PM split is documented.
5. `0021300593`: handle the exact-start Cole/Mason cluster as a parser semantics problem, not a simple row override.
6. `0049600063`: use only as a broad foul-before-sub systematic canary after the clean local canaries succeed.

Do **not** locally fix without stronger anchors:

- `0029600204`: Ron Harper's `0:35` foul proves he was on the floor, but moving the visible `0:26 Harper FOR Jordan` row into the foul stoppage does not explain a legal pre-foul entry. Need an earlier dead-ball/substitution anchor or external rotation source.
- `0029600585`: Dell Curry was on before the visible `6:38` entry, but the true entry/outgoing player is not anchored. Moving him to the `8:35` foul stoppage has the same legal-entry problem as Harper.
- `0021700337`: treat Lauvergne's FTs as potential replacement-shooter semantics, not proof that Lauvergne was the fouled player.
- `0021700377` / `0021700917`: technical FT rows should be handled through technical-FT eligibility, not live-lineup row reordering.

Second-review delta:

A second external review argued that `0029600204` should be treated as a strong local **clock-normalization** candidate rather than discarded: raw EVENTNUM order already has `E338 Harper FOR Jordan` before `E339 Harper P.FOUL`, but the clock/action placement puts the sub at `0:26` after the `0:35` foul/FT cluster. That is a real distinction from a blind event-number move.

Current decision: keep it scratchable, but not high-confidence promotion. The source window has Jordan's made shot at `0:52`, no visible timeout/violation/dead-ball substitution opportunity before Harper's `0:35` foul, and current NBA substitution rules do not permit normal substitutions after a successful field goal without a dead-ball exception. A scratch test may normalize `E338` into/before the `0:35` cluster, but acceptance should require either a legal pre-foul entry anchor or an explicit source-limited explanation for why the legal anchor is missing.

### Systematic Rule Families with Guardrails

Promote each rule narrowly with tight pre-conditions and explicit anti-canaries. Do not let the rules cross-contaminate.

#### Rule 1: Same-clock live-foul-before-sub-out

Apply only when **all** conditions hold:
- The foul row and a substitution row share the same clock.
- The credited foul is by player `X`.
- The same-clock substitution removes `X` (`X` is the OUT player).
- The foul is a *live-play* foul (personal, shooting, loose-ball, offensive) — not a technical foul.
- The foul precedes resulting FTs/dead-ball continuation in basketball causality.
- No live action by the incoming player precedes the foul.

Anti-canaries (do **not** apply this rule when):
- The foul is a technical foul (use Rule 2 instead).
- The fouled player is replaced and the incoming player shoots the FTs (use Rule 3 instead).
- The foul is a flagrant 2 / disqualifying ejection (use Rule 4 instead).

Candidate games: `0049600063`, `0021800484` (already promoted), `0021700514`, parts of `0021700236`, `0021300593` (period-start variant).

#### Rule 2: Technical-FT shooter eligibility

Apply only to technical free throws. Do not treat a technical-FT shooter as ordinary live-lineup proof.

Pre-conditions:
- The FT row is explicitly a technical FT (event class / description marks it as such).
- A same-clock substitution involves the FT shooter, or the shooter is otherwise absent from the pre-cluster live lineup.
- No live shot/rebound/foul/assist by the shooter is being suppressed by this rule.

Anti-canaries:
- A live-play shot, rebound, foul, or assist is **never** suppressed under this rule. Only the technical-FT row's eligibility is reclassified.
- Do not move live-possession minutes around technical-FT clusters unless an independent live event requires it.

Candidate games: `0021700377`, `0021700917`.

#### Rule 3: Replacement-FT-shooter after injury/replay/sub

Apply only when:
- The foul row identifies the offended player `Y` via `PLAYER2_ID`.
- A same-clock timeout, instant replay, or dead-ball pause follows the foul.
- `Y` is subbed out at the same clock.
- The incoming player shoots the resulting FTs.
- No live action occurs between the foul and the FTs.

Anti-canaries:
- Do not infer the incoming player was the fouled player. `PLAYER2_ID` on the foul row is primary evidence.
- Do not row-reorder the sub before the foul. The raw sequence is legitimate under NBA Rule 9.

Candidate game: `0021700337`.

#### Rule 4: Flagrant/ejection-cause-before-replacement-sub

Apply only when:
- A player commits a flagrant 2, ejection, or disqualifying foul.
- A same-clock substitution removes that player.
- The replacement is causally a consequence of the disqualifying event (the player must leave because of the ejection/disqualification).
- No live action by the replacement player precedes the disqualifying event.

This rule is structurally distinct from Rule 1 because the substitution is *mandatory* under NBA rules once the player is ejected, not a routine rotational sub. The causality chain is: foul → ejection → mandatory replacement.

Candidate game: `0021801067`.

#### Rule 5: Duplicate/phantom sub suppression

Apply only when:
- A same-player pair has two incompatible substitution rows.
- A credited live event between the two rows proves the first sub is impossible (e.g., the supposedly-out player makes a basket after his "exit").
- Suppressing the first row does not create a duplicate-player or six-man state.

Candidate games: `0029600657` (Maxwell duplicate confirmed by Del Negro's `5:27` layup); possibly `0020900189` (Lawson phantom period-start) only after stronger Q2-start validation.

Anti-canaries:
- Do not suppress a sub row solely because it disagrees with a minute reference. There must be a *credited live event* contradicting the sub.
- Do not generalize this rule to "the parser doesn't like this row" cases without explicit live-event evidence.

#### Rule 6: Period-start exact-clock cluster control

Apply only when:
- A sub row at exact period-start clock (`12:00`, or `5:00` in OT) shares the cluster with a live event credited to a listed period-start starter.
- The sub is logged in the same `12:00` cluster as the starter's first credited action.

Resolution: the starter is on the floor for their credited action; the sub takes effect after the action's dead-ball resolution.

Candidate game: `0021300593` (Cole/Mason at Q2 12:00).

Note: this rule is closely related to but distinct from Rule 1. The difference is that period-start clusters carry initialization ambiguity that ordinary same-clock clusters don't.

### `0029600070` - Nuggets at Cavaliers, 1996-11-10

Status: `defer-no-anchor` - not an accumulator candidate (wrong sign).

Basketball story:

Marciulionis plays a real rotation: six substitution crossings (`P2 5:24 out`, `P3 1:46 in`, `P4 7:32 out`, `P4 5:46 in`, `P4 3:24 out`, `P4 2:26 in`), dozens of event credits. Event-on-court issues are zero, plus-minus matches official.

```text
output   17:38
official 18:00
diff     -0:22
```

First-principles read:

An earlier read placed this in an accumulator family, but the sign is wrong: Marciulionis is **short** (parser 17:38 vs official 18:00), not over-credited. Same-clock accumulator leakage would produce over-credit. The visible stints add up cleanly to 17:38, so the parser math matches its inputs. The 22-second gap is much more likely old-era whole-minute rounding in the official box (or a reference-tolerance band), not a parser defect.

Proposed fix:

`defer-no-anchor`. No event-level contradiction; no signed-match with the accumulator bucket. Do not invent a Marciulionis override or promote this into an accumulator fix - it would go the wrong direction.

### `0029600171` - Kings at Cavaliers, 1996-11-24

Status: clear missing-sub case; incoming player is Smith; outgoing is under-determined.

Basketball story:

Michael Smith is credited with six late-Q3 actions:

```text
5:11  Smith 5' hook shot
4:53  Smith shooting foul
3:14  Smith misses FT 1 of 2
3:14  Smith misses FT 2 of 2
1:18  Smith lost-ball turnover
0:11  Smith defensive rebound
```

A player cannot score, foul, shoot free throws, turn it over, and rebound while on the bench. Smith was on for a late-Q3 stint. He was subbed out at `P2 1:16 Polynice FOR Smith`. The raw NBA PBP has no Smith re-entry event in Q3. His events begin at `Q3 5:11`, so the missing entry must be between `Q2 0:00` (period break) and `Q3 5:11` - most plausibly a Q3 dead-ball cluster at a standard sub spot (e.g. a free-throw pause, timeout, or period start).

First-principles read:

The incoming player is definitively Smith. The outgoing player cannot be safely inferred from the NBA PBP alone because Polynice's minutes already match the reference - so "Smith FOR Polynice" would break Polynice's reference. Two possibilities:

1. BBR/game-rotation data has the sub and identifies the outgoing player (e.g., Polynice, Williamson, or another Kings big).
2. The Kings rotation at the time of Smith's events had someone else on (not Polynice), and the parser simply didn't process an earlier Polynice-out sub.

Proposed fix:

Requires a BBR or game-rotation source lookup. The candidate repair shape is: `insert Smith FOR [X] at P3 5:11 or the nearest preceding Kings dead ball`, where `X` is identified by cross-checking BBR. If the outgoing player is Polynice, accept that Polynice's minute reference will move. The plausibility principle dictates that if BBR and event evidence agree, trust them over the minute reference.

### `0029600175` - Timberwolves at Bullets, 1996-11-25

Status: `defer-no-anchor` - cost exceeds benefit.

Basketball story:

At `Q3 0:22`, Chris Carr misses a 25-foot three. The raw Q3 has one Minnesota sub (`Jackson FOR Cheaney at 1:19`) and then Carr's miss. Carr is short by ~14 seconds (`0.2333` min).

First-principles read:

Carr was on for the last possession (the miss is clear on-court evidence). But the outgoing player cannot be inferred from the NBA PBP alone, and the scale is tiny (14 seconds). A one-possession insert requires override machinery whose cost exceeds the benefit at this scale.

Proposed fix:

`defer-no-anchor`. Upgrade only if a future BBR pull provides a specific sub row identifying the outgoing player.

### `0029600204` - Bulls at Mavericks, 1996-11-29

Status: source-limited until a legal pre-foul entry anchor is found.

Basketball story:

At `0:35` in Q3, Ron Harper commits a personal foul. The feed also has `SUB: Harper FOR Jordan`, but it is clocked at `0:26`, after the foul clock. That is impossible as written: Harper had to be on the floor before the defensive possession that produced his `0:35` foul.

Raw/source sequence:

```text
0:52  Jordan jumper
0:26  SUB: Harper FOR Jordan
0:35  Harper P.FOUL
0:35  Jackson FT 1 of 2
0:35  Kukoc/Rodman and Kidd/McCloud subs
0:35  Jackson FT 2 of 2
0:26  Kerr 3PT jumper
0:00  Derek Harper misses 3PT
```

Most coherent story:

Ron Harper entered before the `0:35` foul/free-throw sequence. The `0:26` clock on the Harper-for-Jordan substitution is not credible as written. However, a simple move into the `0:35` foul stoppage is not enough: the substitution had to be legally effective **before** Harper committed the foul, not during the dead ball caused by that same foul.

Oracle review delta:

This is less airtight than the other foul-before-sub cases. Moving `Harper FOR Jordan` to just before `Harper P.FOUL` would make the event audit happy, but it would not explain how Harper legally entered the defensive possession. It needs an earlier dead-ball anchor, timeout, substitution opportunity, or external rotation source. Also, the `0:00` Harper miss in the focus window is Derek Harper, not Ron Harper, so it should not be used as evidence for Ron Harper's Bulls stint.

Proposed fix:

`scratch-only clock-normalization hypothesis`. A test candidate can normalize `E338 SUB: Harper FOR Jordan` into/before the `0:35` cluster so the parser has Harper on for `E339`, but promotion requires more than a cleared event audit. The scratch must also explain the legal entry point or explicitly document that the feed is missing the dead-ball anchor. Without that, keep this as source-limited rather than a production row override.

Scratch gate:

- Clear Ron Harper's `E339` off-court foul without creating a new Jordan/Harper substitution blocker.
- Preserve Jordan's `0:52` made jumper, Kerr's `0:26` three, and all counting stats.
- Limit Jordan/Harper minute movement to the expected clock-scale window.
- Identify a legal pre-foul dead-ball/substitution anchor; if none exists, do not promote beyond documented source-limited status.

### `0029600332` - Warriors at SuperSonics, 1996-12-17

Status: box/PBP contradiction; `rotation-rebuild` only, no local plausibility override available.

Basketball story:

Parser has Mullin at `29:32` min vs official `42:00`, DeClercq at `19:50` vs official `8:00` - a ~12-minute paired swap in the box. No event-on-court rows; many official plus-minus values are zero and unreliable.

Mullin visible subs: `P1 3:14 out`, `P2 9:53 in`, `P3 1:07 out`. Three crossings, total visible stint `8:46 + 20:46 = 29:32` = parser output exactly. No visible Mullin sub in P4.

DeClercq visible subs: `P1 0:57 in`, `P2 6:15 out`, `P3 9:56 in` (DeClercq FOR Smith), `P4 8:49 out` (Owes FOR DeClercq). Parser `~19:50`, matches visible inputs.

First-principles read:

Both raw NBA PBP and BBR PBP agree: no Mullin in P4, DeClercq enters at P3 9:56 and stays until P4 8:49. The box minutes disagree with **both** event feeds. This is not a "BBR has the missing row" case - BBR and raw concur.

An earlier pass speculated about a missing Mullin P4 entry (because Mullin was a star and 42 min feels right for a captain). That was role-intuition fudging a box number into a fix story without event-level evidence. The rubric explicitly disallows this.

Proposed fix:

`rotation-rebuild` from an **external** source (game-rotation feed, archived NBA stats with per-stint data, or official gametracker if available). Do not override from plausibility alone, and do not rely on BBR PBP - BBR agrees with raw.

### `0029600370` - Mavericks at SuperSonics, 1996-12-22

Status: box/PBP disagreement; `rotation-rebuild` only, no event-level anchor.

Basketball story:

Paired minute damage on both teams:
- Dallas: Derek Harper `+12.38`, Mashburn `-12.16`
- Seattle: Schrempf `-11.15`, Ehlo `+11.97`

No event-on-court issues; official PM is mostly zeros and unusable.

First-principles read:

The PBPs (raw NBA and BBR) have plenty of event evidence for all four players - Harper and Ehlo are credited with plays the parser correctly attributes, and no credited play contradicts the parser's lineup state anywhere. The disagreement lives only in the box-minute column.

An earlier pass reasoned from "Mashburn and Schrempf were scoring leads who should play 38-42 min" to "the parser has the workload upside-down." That is role-intuition, not event-level evidence. If the 1996-97 official box over-credits Mashburn/Schrempf (possible for this era), the parser could actually be closer to correct.

Proposed fix:

`rotation-rebuild` from external source. No plausibility-first override target: no single event contradiction anchors a row-level fix.

### `0029600585` - Knicks at Hornets, 1997-01-24

Status: source-limited missing/late-entry story; true entry/outgoing not anchored.

Basketball story:

Dell Curry is credited with:

```text
8:35  Curry personal foul
6:46  Curry missed layup
6:38  SUB: Curry FOR Smith
4:21  Curry defensive rebound
```

The first two Curry events happen before the visible Curry-for-Smith substitution, so the parser quite reasonably has Tony Smith, not Curry, on the floor for those rows. BBR preserves the same basketball contradiction: Curry commits the `8:35` foul and misses the `6:46` layup before the visible `6:38` entry.

Most coherent story:

Curry was already on the floor before the `8:35` foul and stayed on for the `6:46` layup. The visible `SUB: Curry FOR Smith` at `6:38` is therefore late, duplicate, or otherwise not the true entry point.

Candidate repair shape to test later:

```text
Move or reinterpret Curry's entry before the 8:35 Curry foul.
Then keep Curry on for the 6:46 layup and later 4:21 rebound.
```

Why this needs care:

Moving Curry back to `8:35` adds far more time than the visible `0.2700` Curry minute tail, so this is likely a reference/source tradeoff rather than a free local cleanup. Plausibility says Curry was in; the official-minute reconciliation may not like the full consequence.

Oracle review delta:

Like `0029600204`, this needs a legal pre-event entry anchor. Curry could not enter because of the dead ball created by his own `8:35` foul; he had to be on the floor before that defensive possession. Do not promote a local move unless BBR/rotation evidence identifies the true earlier Hornets sub and outgoing player.

### `0029600657` - Kings at Spurs, 1997-02-03

Status: rejected local suppress candidate; keep documented hold/source tradeoff.

Basketball story:

The Q2 feed has two visible `Maxwell FOR Del Negro` rows:

```text
6:15  Anderson layup assisted by Del Negro
5:44  SUB: Maxwell FOR Del Negro
5:27  Del Negro layup
4:54  SUB: Maxwell FOR Del Negro
3:50  Williams layup assisted by Maxwell
```

Del Negro cannot make the `5:27` layup if he really left at `5:44`.

Most coherent story:

The first `Maxwell FOR Del Negro` row at `5:44` is duplicate/premature. Del Negro stayed in through the `5:27` basket. Maxwell's real entry is the later `4:54` substitution, after which he appears in the live play.

Reference tradeoff:

The current residue has Maxwell short by about `0.75` minutes and Del Negro long by about `0.65`, with offsetting plus-minus deltas. Official minutes appear to prefer some of the earlier-sub time, but event plausibility prefers the later-sub story because Del Negro's `5:27` basket is hard evidence.

Scratch gate:

- Suppress or reclassify the first `5:44 Maxwell FOR Del Negro` row.
- Confirm Maxwell has no live credited action between `5:44` and `4:54`.
- Confirm Del Negro remains valid through the `5:27` layup.
- Keep counting stats clean and avoid creating a new substitution blocker.
- Document any remaining Maxwell/Del Negro minute or PM tradeoff as the cost of preferring the coherent event story.

Scratch validation result, 2026-04-24:

Two local suppressions were tested and both failed the acceptance gate.

`drop E161` (`5:44 Maxwell FOR Del Negro`) preserves the coherent basketball story - Del Negro remains on for his `5:27` layup and Maxwell enters at the `4:54` timeout - but it makes the reference split worse:

- `event_player_on_court` issues stay at `0`.
- Counting-stat boxscore audit stays clean.
- Maxwell becomes `-0.8333` minutes and `-4` PM versus official.
- Del Negro becomes `+0.7333` minutes and `+4` PM versus official.
- The game remains release-blocking/open under raw residual output.

`drop E160` (`4:54 Maxwell FOR Del Negro`) matches the official minutes/PM much better, but it creates the basketball impossibility:

- Maxwell minutes and PM become exact.
- Del Negro is only `-0.10` minutes and PM exact.
- But `P2 E156 Del Negro layup` becomes an `off_court_event_credit`.
- The game remains release-blocking/open under raw residual output.

Conclusion: do not promote a local suppression. The clean event story and the official minute/PM story choose different duplicate rows. Keep this as a documented source tradeoff unless an external rotation source identifies a third boundary or explains the duplicate pair.

### `0029601163` - Trail Blazers at Grizzlies, 1997-04-17

Status: `defer-no-anchor`.

Basketball story:

Event-on-court issues: 0. PM matches. Dudley short by `0:39`, Robinson long by `0:27`. Sub counts normal (Portland `1/4/2/5` per period; Vancouver `3/5/4/6`).

First-principles read:

Not cleanly paired in magnitude; no credited play contradicts the parser's lineup state for either player. Speculating about a boundary-timestamp or decimal-rounding artifact is *possible* but not anchored in event evidence, so it doesn't pass the rubric.

Proposed fix:

`defer-no-anchor`. No local override from plausibility.

### `0049600063` - Rockets at SuperSonics, 1997-05-15

Status: `cluster-order-systematic` - not a local override candidate.

Basketball story:

There are four foul rows where the credited fouler has already been removed in the parser's lineup:

```text
P2 6:22  Willis loose-ball foul; then Barkley FOR Willis
P4 5:43  Threatt shooting foul; then Maloney FOR Threatt
P4 4:55  Cummings personal foul; then Perkins FOR Cummings
P4 2:14  Elie personal foul; then Threatt FOR Elie
```

Most coherent story:

In each cluster, the player commits the foul first, then the substitution occurs during the dead ball before or between free throws. The parser is putting the substitution on the wrong side of the credited foul.

Repair category:

This belongs with systematic same-clock cluster ordering, not one-off row moves. Four examples across both teams and multiple periods imply a parser rule: credited live fouls should be evaluated before same-clock sub-outs of the fouler, then the post-foul dead-ball lineup should own the next live interval. Any implementation needs broad canaries because the minutes impact is tiny and the remaining residue is mostly plus-minus/reference boundary noise.

### `0029700159` - Nuggets at Grizzlies, 1997-11-21

Status: source-limited substitution row plus unresolved minute tradeoff.

Basketball story:

At `1:51` in Q3, the feed says:

```text
SUB: Lauderdale FOR Garrett
```

But Garrett has already been removed from the current Denver lineup. The current live window has Lauderdale entering into the Stith slot, not the Garrett slot. Later, at `0:27`, Goldwire replaces Lauderdale.

Most coherent story:

The incoming player is credible: Lauderdale came in. The outgoing player in the source row is not credible: it cannot be Garrett because Garrett is already gone. The row is a broken substitution row, not a normal lineup contradiction.

Remaining downside:

The existing live windows are the least-bad state found so far, but they leave the Denver P3 Stith/Lauderdale/Garrett minute tradeoff unresolved. Plausibility tells us the row's outgoing player is bad; it does not fully solve Stith's official-minute deficit.

### `0029701075` - Knicks at Celtics, 1998-04-05

Status: strong scrambled-P3 basketball story, but repair requires full P3 rotation reconstruction.

Basketball story:

The P3 feed is visibly scrambled: event numbers and clocks jump backward and forward. Meanwhile, the credited plays are coherent:

```text
3:39  Chris Childs rebound
3:25  Terry Cummings layup, Childs assist
2:44  Cummings rebound
2:23  Tyus Edney turnover
2:14  Cummings missed runner
1:53  Andrew DeClercq foul
1:45  Cummings miss
1:19  Cummings make, Childs assist
1:06  Childs foul
0:21  Cummings layup, Childs assist
```

A player cannot record that run of rebounds, shots, assists, and fouls from the bench. Childs and Cummings were on the floor for the Knicks late-Q3 stretch; DeClercq and Edney also have real Celtics-side event evidence.

Why the obvious window was rejected:

Putting Childs and Cummings into the visible late-Q3 Knicks window clears the basketball contradiction and improves their minutes, but the tested local windows take the time from Oakley and Larry Johnson, making those two wrong by about the same amount. That means the defect is broader than "swap two Knicks into this one window."

Plausibility result:

The basketball story is clear; the repair shape is not. This needs a full P3 reconstruction from BBR/tpdev/event context, not a narrow local window. In particular, verify the Knicks P3 opening lineup and missing P3 subs from a third source before concluding that Childs/Cummings should simply displace Oakley/Larry Johnson in the current parser window.

### `0020900189` - Bulls at Nuggets, 2009-11-21

Status: plausibility favors phantom/stale substitution row, not Billups-start repair.

Basketball story:

The blocker row is:

```text
12:00 Q2  SUB: Lawson FOR Billups
```

But the actual Q2 play sequence strongly supports Lawson already being in the game:

```text
11:52  Lawson foul
10:50  Anthony layup, Lawson assist
10:21  Lawson missed layup
9:00   Lawson free throws
7:53   SUB: Billups FOR Anthony
7:03   Billups misses
6:43   Billups 3PT, Lawson assist
```

Most coherent story:

Lawson started Q2 or was already active at the start. Billups was not the player Lawson replaced at `12:00`; Billups visibly enters later at `7:53`. The `Lawson FOR Billups` row is therefore more plausibly a stale/phantom period-start substitution than an instruction to force Billups into the Q2 starting lineup.

Reference tradeoff:

Older source comparison noted `full_pbp_new` had Billups on the first Q2 boundary row, but the event-level basketball story is stronger: Lawson is immediately active and Billups' real visible entry is at `7:53`.

### `0021300593` - Heat at Bobcats, 2014-01-18

Status: `cluster-order-systematic` - parser period-start cluster control fix, not a row move.

Basketball story:

The Q2 opening cluster:

```text
12:00  Cole shooting foul
12:00  Sessions FT 1 of 2
12:00  SUB: Mason Jr. FOR Cole
12:00  Sessions FT 2 of 2
11:07  Mason Jr. shooting foul
```

The raw event order is **already coherent**: Cole foul → FT1 → Mason-for-Cole sub → FT2. Cole committed the foul as the listed Q2 starter, then Mason replaced him during the FT sequence.

Why this isn't a row-reorder:

The defect is not feed chronology - the feed has it right. The defect is parser inference at exact-clock period start: when a sub event shares the period-start clock with a credited foul by a starter, the parser is treating the sub as the period-opener rather than treating the foul as evidence of the pre-sub starter lineup.

Proposed fix:

`cluster-order-systematic`. At an exact period-start clock, a credited live foul by a listed starter should anchor the pre-sub lineup; same-clock sub-out should apply after the foul credit. Minutes already exact; only a small Cole/Mason PM boundary split should move.

### `0021700236` - Warriors at Nets, 2017-11-19

Status: cluster-order systematic canary, not isolated hand patch.

Basketball story:

This P1 window is scrambled by same-clock and instant-replay rows:

```text
4:03  SUB: LeVert FOR Dinwiddie
...
4:03  Dinwiddie shooting foul

2:47  Green shooting foul
2:47  Warriors timeout
2:47  SUB: Casspi FOR Green
2:47  Zeller missed FT
2:47  Casspi rebound
```

Most coherent story:

Dinwiddie committed the `4:03` foul before LeVert replaced him. At `2:47`, Green fouled, Golden State called timeout, Casspi entered for Green, Zeller missed the free throw, and Casspi got the rebound.

Why this is promising:

Both issue rows have ordinary basketball explanations. The current raw order lets same-clock/instant-replay rows land on the wrong side of the substitutions. Prefer validating this as part of the same-clock cluster-order rule rather than as two unrelated per-game edits.

### `0021700337` - Spurs at Thunder, 2017-12-03

Status: `cluster-order-systematic` (replacement-FT-shooter rule); raw `PLAYER2_ID=Anderson` is primary evidence.

Basketball story:

The Q3 cluster: Huestis fouls at `1:00`, Spurs timeout, instant replay review, then `SUB: Lauvergne FOR Anderson`, then Lauvergne shoots 2 FTs. Raw `PLAYER2_ID` on the foul row identifies Anderson as the fouled player.

```text
1:00  Huestis S.FOUL  (PLAYER2_ID = Anderson)
1:00  Spurs Timeout
1:00  Instant Replay
1:00  SUB: Lauvergne FOR Anderson
1:00  Lauvergne FT 1 of 2
1:00  Lauvergne FT 2 of 2
```

The basketball story: Anderson was fouled by Huestis. During the replay review or after it, Anderson was substituted out (likely for injury - replacement-FT-shooter rule under NBA Rule 9 applies). Lauvergne entered and shot the two FTs as the replacement shooter selected by the opposing coach.

The earlier "Lauvergne must have been on the floor before the foul" framing was wrong. Raw evidence puts Anderson as the fouled player, and the replacement-shooter rule covers Lauvergne's FT credit.

Proposed fix:

`cluster-order-systematic`. Parser should validate FT shooter eligibility under NBA Rule 9 (offended-player FTs with injury/ejection exceptions): when a sub-out and FT shooter swap appear at the same clock as a foul, treat the replacement shooter as rule-valid for the FT rows without forcing them onto the pre-foul lineup. Do not row-reorder. Validate against `PLAYER2_ID` to ensure we are correctly identifying the fouled player as the substitution-out target.

Validation gate:

- Verify `PLAYER2_ID=Anderson` or equivalent source fields identify Anderson as the fouled player.
- Check whether Anderson re-enters later; if he immediately returns with no injury/replacement context, keep this as source-limited/control rather than rule-valid clearance.
- Confirm no live action requires Lauvergne to be on the floor before the foul.

### `0021700377` - Lakers at Hornets, 2017-12-09

Status: `cluster-order-systematic` - designated-FT-shooter handling, not a row-reorder.

Basketball story:

At `3:03` in Q3, Howard T-foul; Clarkson shoots the technical FT; same-clock `SUB: Clarkson FOR Caldwell-Pope`. The technical FT credit is a dead-ball/control event, not the same strength of on-court evidence as a live rebound, foul, assist, or shot.

Proposed fix:

`cluster-order-systematic`. Parser should handle same-clock technical-FT shooter eligibility separately from live possession lineup validity. Current NBA Rule 12 allows a technical FT shooter who was in the game when the technical was assessed, or a substitute already beckoned/recognized before the technical. Recognize Clarkson's FT as rule/cluster-valid only under that eligibility gate, without forcing a row-reorder solely because he is absent from the pre-sub lineup. This is a *rule-awareness* fix, not a chronology fix.

### `0021700514` - Jazz at Warriors, 2017-12-27

Status: row-reorder candidate, anchor row needs verification before promotion.

Basketball story:

At Q2 `5:09`, Royce O'Neale is credited with a shooting foul on Jordan Bell, then Bell shoots free throws. The parser has Joe Ingles in that Jazz slot instead.

Verified raw event order:

```text
E243 (5:09)  O'Neale S.FOUL on Bell
E247 (5:09)  Bell FT 1 of 2 (miss)
E248 (5:09)  Bell FT 2 of 2 (made)
E253 (5:09)  Warriors rebound
E356 (5:09)  SUB: Ingles FOR O'Neale
```

Note: EVENTNUMs in this game are non-monotonic with PCTIMESTRINGs (e.g., `E245` at `5:25` falls between `E243` and `E247` at `5:09`), suggesting post-hoc feed reordering or instant-replay artifacts. The `Ingles FOR O'Neale` sub has EVENTNUM `E356`, much later than the foul at `E243`.

Why this needs verification before promoting:

If the parser sorts by EVENTNUM the foul (`E243`) precedes the sub (`E356`) and the off-court-foul issue should not arise. The fact that it does arise suggests the parser uses a different sort key (perhaps a synthetic chronology that places the sub before the foul). Before promoting a row override, confirm which sort key the parser uses and what the canonical anchor row to move actually is.

Proposed fix:

`row-reorder candidate`, gated on confirming the parser sort key produces the observed off-court-foul state and locating the canonical anchor row. Do not promote a blind override.

### `0021700917` - Warriors at Wizards, 2018-02-28

Status: `cluster-order-systematic` - designated-FT-shooter handling.

Basketball story:

At `0:04.20` of Q1: Curry made a three, Iguodala take foul, `SUB: Livingston FOR Curry`, Oubre T-foul, Curry shoots technical FTs, Brooks T-foul.

Two plausible readings:
1. The sub was logged inside the cluster early, but Curry shot the technicals before actually leaving.
2. The technical FT row needs control-event eligibility handling rather than ordinary live-lineup validation.

Either reading makes the raw sequence explainable without treating the technical FTs as proof of the live lineup interval. Neither requires a row-reorder by itself.

Proposed fix:

`cluster-order-systematic`. Recognize technical-FT shooter eligibility around intervening same-clock sub-out rows. Current NBA Rule 12's "in game / beckoned or recognized" standard should drive validation. Do not force a row-reorder into live possession minutes - that risks misattributing the ~4 seconds of end-of-Q1 time.

### `0021800484` - Mavericks at Warriors, 2018-12-22

Status: promoted foul-before-sub row-order correction.

Basketball story:

At `3:54` in Q3, Barea makes a running jumper, Curry commits the shooting foul, Barea shoots the and-one free throw, and the same clock has `SUB: Livingston FOR Curry`.

Raw/source sequence:

```text
3:54  Barea running jumper
3:54  Looney FOR Green
3:54  Livingston FOR Curry
3:54  Curry shooting foul
3:54  Barea FT 1 of 1
```

Most coherent story:

The foul is part of the made-basket play. Curry cannot be subbed out before committing the and-one foul. Process the Barea shot and Curry foul as the possession event, then apply the Warriors substitutions during the dead ball.

Scratch gate:

Move/process Curry's `3:54` shooting foul before both same-clock Warriors substitutions (`Looney FOR Green` and `Livingston FOR Curry`), not only before Livingston. Rerun must clear the Curry event row, preserve Barea's made basket/free throw stats, avoid material minute movement, and leave the post-FT lineup with the substitutions applied.

Scratch validation result, 2026-04-24:

- Production row override promoted: `0021800484,move_before,437,433`.
- Reordered cluster: `Barea make -> Curry shooting foul -> Looney FOR Green -> Livingston FOR Curry -> Barea FT`.
- `event_player_on_court` issues went from `1` to `0`.
- No new substitution-row blocker appeared.
- Counting-stat boxscore audit stayed clean.
- Minutes stayed clean: `minutes_mismatches = 0`, max absolute minute diff stayed clock-scale at about `0.0067`.
- Plus-minus reference delta rows improved from `8` to `6`.
- Residual output without overlay classifies the game as `boundary_difference`, `release_blocking_game_count = 0`, `research_open_game_count = 0`.

### `0021801067` - Celtics at 76ers, 2019-03-20

Status: promoted flagrant/ejection-before-sub row-order correction.

Basketball story:

At `11:06` in Q3, the raw row order says:

```text
SUB: Brown FOR Smart
Smart ejection
Embiid technical
Irving technical FT
Smart flagrant foul type 2
Embiid flagrant FTs
```

Most coherent story:

Smart committed the flagrant, was ejected, and Brown replaced him because of that ejection/dead-ball sequence. Brown cannot replace Smart before the event that causes Smart to leave.

Candidate repair shape:

For ejection/flagrant same-clock clusters, process the credited foul/ejection cause with the offending player still on court, then apply the replacement substitution.

Scratch validation result, 2026-04-24:

- Production row overrides promoted: `0021801067,move_before,374,369` and `0021801067,move_after,369,372`.
- Reordered cluster: `Smart flagrant -> Smart ejection -> Embiid technical -> Brown FOR Smart -> Irving technical FT -> Embiid flagrant FTs`.
- This matches the `pbpv3` action-id chronology more closely than raw row order.
- `event_player_on_court` issues went from `1` to `0`.
- No new substitution-row blocker appeared.
- Counting-stat boxscore audit stayed clean.
- Minutes stayed clean: `minutes_mismatches = 0`.
- Plus-minus reference deltas went from `2` to `0`.

## Rule References

Verified 2026-04-24 against the current NBA Official rule pages:

- [NBA Rule No. 3: Players, Substitutes and Coaches](https://official.nba.com/rule-no-3-players-substitutes-and-coaches/)
- [NBA Rule No. 9: Free Throws and Penalties](https://official.nba.com/rule-no-9-free-throws-and-penalties/)
- [NBA Rule No. 12: Fouls and Penalties](https://official.nba.com/rule-no-12-fouls-and-penalties/)
