# Phase 6 Blocker-Policy Frontier

Historical note:

- this is the pre-adoption policy memo
- the reviewed March 22 release-policy decision was later applied in:
  - `/Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev/reviewed_release_policy_decision_20260322_v1.md`
  - `/Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev/phase6_reviewed_frontier_inventory_20260322_v1/summary.json`

Current reviewed frontier after the capped confirmation pass:

- `17` live open games total
- `15` open games outside Block A
- `20` actionable event rows outside Block A
- `10` games are already in reviewed non-local lanes:
  - `5` same-clock controls / guardrails
  - `5` rebound-credit survivors with rejected local-boundary scratches
- `7` games remain documented holds:
  - `0021700394`
  - `0029700159`
  - `0029701075`
  - `0020400335`
  - `0020000628`
  - `0020900189`
  - `0021300593`

Investigation outcomes that matter for policy:

- `0021700394` stayed a broader same-clock minute-accumulator defect after a bounded four-source comparison.
  - No single cluster or single source isolated a one-sided bad row.
  - `full_pbp_new` is systematically coarser / pre-sub on repeated same-clock FT-sub boundaries, but that sharpens the diagnosis rather than closing the game.
- `0029700159` live vs archived candidate was recoverable and rerun cleanly.
  - The archived candidate did not improve minute mismatch or outlier counts.
  - It worsened actionable event rows from `1` to `2`.
  - Keep the current live state if policy remains strict.

## Decision Options

| Option | Reclassified Games | Remaining Blockers | Risk |
| --- | ---: | ---: | --- |
| 1. Status quo | 0 | 17 | No policy risk, but the done-definition stays far away and the queue remains dominated by already-reviewed non-local lanes. |
| 2. Reclassify same-clock controls + rebound survivors | 10 | 7 | Low; these are already documented as non-override lanes with no material minute issue. |
| 3. Also reclassify contradiction cases | 12 | 5 | Medium; two genuine boundary contradictions stop counting as blockers even though they remain unresolved. |
| 4. Also reclassify `0020000628` and `0020400335` | 14 | 3 | Higher; one severe-minute holdout and one mixed-source comparison case stop blocking finalization. |

Option-to-game mapping:

- Option 2 adds:
  - `0021700337`
  - `0021700377`
  - `0021700514`
  - `0021801067`
  - `0021900333`
  - `0021900201`
  - `0021900419`
  - `0021900487`
  - `0021900920`
  - `0041900155`
- Option 3 additionally adds:
  - `0020900189`
  - `0021300593`
- Option 4 additionally adds:
  - `0020000628`
  - `0020400335`

Residual blockers by option:

- Option 1:
  - all `17`
- Option 2:
  - `0021700394`
  - `0029700159`
  - `0029701075`
  - `0020400335`
  - `0020000628`
  - `0020900189`
  - `0021300593`
- Option 3:
  - `0021700394`
  - `0029700159`
  - `0029701075`
  - `0020400335`
  - `0020000628`
- Option 4:
  - `0021700394`
  - `0029700159`
  - `0029701075`

## Infrastructure Debt

`0029800606` remains unresolved infrastructure debt.

- Golden Canary semantics are now truthful:
  - the case is explicitly tagged as `unstable_control`
  - the suite reports both all-case and stable-case pass states
- But the block-vs-single-game parity split is still unresolved, so the canary gate is not a perfect reliability signal for every historical artifact.

## Gate

Phase 7 scope depends on the user’s blocker-policy decision.

Do not begin final proving / finalization work until one of the numbered policy options above is chosen.
