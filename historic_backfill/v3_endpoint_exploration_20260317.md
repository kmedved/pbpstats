# V3 Endpoint Exploration - 2026-03-17

## Goal

Explore whether `stats.nba.com/stats/boxscoretraditionalv3` can be used as a more robust period-starter signal for historical games, especially in the known post-Q1 starter failure families:

- strict PBP returns a full but wrong 5
- strict PBP returns an incomplete lineup and needs rescue
- the tiny early-window V3 heuristic returns a false positive "resolved" answer

This note is exploratory only. Nothing here is wired into the repo runtime yet.

## Why This Matters

The March 17 rollback was the right safety move because it stopped bad parquet/V3 rows from overriding already-correct strict PBP answers. But the broader question remains open:

- is there a safer way to use the V3 endpoint as a period-starter signal
- can it rescue the real "strict succeeded but chose the wrong 5" games
- can it avoid the `0029700060`-style false positives that came from trusting one tiny early window

My current view is that this endpoint may still be a goldmine, but not in the original "single `first_event + 5s` probe" form.

## Scope Of This Exploration

I used the notebook-style live request setup:

- endpoint: `boxscoretraditionalv3`
- headers: same browser-like header set used in the notebook
- proxy: DataImpulse session proxy
- request style: single-threaded, rate-limited

Observed operational behavior:

- live probes were kept intentionally slow, roughly `1.4-1.5s` apart
- a couple transient `403` responses appeared during the wider sweep and recovered on retry
- total live probe count on March 17, 2026 was modest, not a broad scrape

This was enough to evaluate behavior on a focused canary set without leaning too hard on the endpoint.

## Core Idea That Looks Promising

The endpoint appears much more informative when treated as a sequence of snapshots instead of a single tiny start-window answer.

The useful question is not:

- "what does V3 say at `first_event + 5s`?"

The more useful question seems to be:

- "when does the V3 top-5 set stabilize?"

That matches the coarse snapshot-table idea: build a sparse timeline of windows, look for plateaus, and treat the early short-lived states as provisional.

## Main Canary Set

I focused on these periods:

- `0029700060` period `3`
- `0020401139` period `5`
- `0020700319` period `4`
- `0020100162` period `5`
- `0020400932` period `3`

These cover the three important failure shapes:

- false-positive tiny-window V3 answer
- strict PBP wrong-full-five rescue
- incomplete-lineup rescue

## Headline Findings

### 1. Tiny windows are brittle

The original tiny-window heuristic is the problem, not necessarily the endpoint itself.

The clearest example is `0029700060` period `3`:

- at `10s` and `12s`, V3 gives the bad pair:
  - `Jermaine O'Neal` over `Rasheed Wallace`
  - `Matt Bullard` over `Clyde Drexler`
- by `20s`, both teams flip to the correct top 5
- they then stay correct through `30s`, `40s`, `50s`, `60s`, and the wide pre-sub window

So the bad lineup was real as an endpoint response, but it was a short-lived early plateau, not a stable signal.

### 2. The helpful rescue cases stabilize early and stay stable

#### `0020401139` period `5`

- at `5s`, the tiny-window version is misleading
- by `10s`, V3 already has the good Spurs five with `Bruce Bowen` in and `Brent Barry` sixth
- it remains stable through `20s`, `30s`, and `40s`

#### `0020700319` period `4`

- already correct by `15s`
- remains correct through `19s`, `30s`, `60s`, and `80s`

#### `0020100162` period `5`

- at `10s`, the lineup is still ambiguous / early-shape
- by `20s`, it flips to the later stable set
- it stays there through `30s`, `40s`, `60s`, `120s`, and the pre-sub window

#### `0020400932` period `3`

- already correct-looking by `15s`
- stays stable all the way out

### 3. Exact `StartRange` boundary sensitivity is real

This is important.

Small tenths-level changes near period start can materially change the result:

- on `0029700060` period `3`, shifting `StartRange` by `+1` tenth immediately fixed the bad early-window result
- on `0020401139` period `5`, shifting by `+1` tenth at `5s` produced an empty player list

That means any method depending on a single exact period-start boundary is fragile.

The endpoint can still be useful, but the method needs to be robust to this start-range sensitivity.

### 4. Early PBP participation is useful as a veto, but not as a full solution

This also became clearer in the live probes.

For `0029700060` period `3`, early PBP before the first sub includes:

- `Clyde Drexler`
- `Rasheed Wallace`

and does not support the bad tiny-window pair:

- `Matt Bullard`
- `Jermaine O'Neal`

So early PBP is a good veto there.

But it is not universally sufficient:

- in `0020700319` period `4`, early PBP does not cleanly surface `Fred Jones`
- the V3 endpoint still provides the useful signal

So PBP-before-first-sub looks like a supporting check, not a standalone arbiter.

## Concrete Canary Results

### `0029700060` period `3`

Window ladder:

- `10s`: wrong
- `20s`: flips to correct
- `30s+`: stays correct

Interpretation:

- bad tiny-window answer
- good medium-window answer
- extremely strong evidence against trusting one early micro-window

### `0020401139` period `5`

Window ladder:

- `5s`: misleading
- `10s`: correct
- `20s+`: stable

Interpretation:

- V3 can rescue the strict-wrong-full-five family
- but only if we stop asking it the question too early

### `0020700319` period `4`

Window ladder:

- `15s`: correct
- `19s+`: stable

Interpretation:

- a genuinely helpful V3 case
- looks much healthier than the false-positive 1998 case

### `0020100162` period `5`

Window ladder:

- `10s`: early shape still wrong-ish / ambiguous
- `20s`: flips to stable useful set
- `30s+`: stable

Interpretation:

- another sign that a small ladder beats a single tiny window

### `0020400932` period `3`

Window ladder:

- `15s`: already right-looking
- `19s+`: stable

Interpretation:

- not every case needs a wide probe
- but the method should let the stable cases prove themselves instead of assuming all cases behave the same way

## Working Hypothesis

The V3 endpoint likely does contain real period-start lineup information on older data, but:

- the signal often matures over roughly `10-30` seconds
- the earliest tiny window can be misleading
- the exact period-start tenths boundary is noisy

That suggests the endpoint is not best modeled as:

- "authoritative answer at one magic timestamp"

It is better modeled as:

- "a time-evolving lineup signal that becomes reliable once it reaches a stable plateau"

## Most Promising Rule Shape To Explore Next

I would explore a stability-based rule, not a single-window rule.

Candidate shape:

1. Probe a small ladder of windows such as:
   - `10s`
   - `20s`
   - `30s`
   - `min(60s, first_sub - 1)`

2. Compare the top-5 set at each step.

3. Prefer the earliest window whose top-5 set matches a later window.

4. Treat a case as suspicious when:
   - the `#5 / #6` identity flips between `10s` and `20s` or `30s`
   - the only support comes from one tiny early window
   - the answer is highly sensitive to `StartRange +/- 1` tenths

5. Use early PBP participation only as a supporting check or veto:
   - helpful when V3 picks a player with no pre-sub evidence while the omitted player appears early
   - not strong enough to replace the V3 signal by itself

This rule shape fits the canaries much better than the current notebook heuristic.

## Binary Search Looks Useful, But For A Narrower Question

I also tested the idea of using binary search against the endpoint, not to pick the entire starter set directly, but to answer a more targeted question:

- when does a specific player first become non-zero in the V3 accumulation

That turns out to be a much better fit for binary search than "what is the correct 5-man unit?"

### Example: `0029700438`, period `2`, Seattle, `Detlef Schrempf` (`player_id=96`)

Using the notebook headers and proxy:

- 10-second-resolution binary search found first non-zero at about `520.0s`
- 5-second-resolution binary search refined that to about `515.0s`

That lines up well with the coarse snapshot scan.

The before/after snapshots were also informative:

- before entry, Seattle had:
  - `Dale Ellis 492`
  - `Sam Perkins 492`
  - `Greg Anthony 318`
  - `David Wingate 301`
  - `Vin Baker 257`
  - `Aaron Williams 235`
  - `Hersey Hawkins 191`
  - `Gary Payton 174`
- at entry, Seattle had:
  - `Dale Ellis 515`
  - `Sam Perkins 515`
  - `Greg Anthony 318`
  - `David Wingate 301`
  - `Vin Baker 280`
  - `Aaron Williams 235`
  - `Hersey Hawkins 214`
  - `Gary Payton 197`
  - `Detlef Schrempf 23`

The "minutes froze" outgoing-candidate idea surfaced:

- `Greg Anthony`
- `David Wingate`
- `Aaron Williams`

That is not enough by itself to prove the outgoing player, but it is useful evidence for narrowing the candidate set.

### What Binary Search Seems Good For

- localizing the first non-zero appearance of a disputed player
- narrowing the time range where a missing starter / silent sub-in appears
- identifying outgoing-player candidates whose accumulated seconds freeze when the new player appears
- reducing probe count once we already know which player we care about

### What Binary Search Is Not Good For

- choosing the full starter set from scratch
- replacing the ladder / plateau logic
- solving periods where the issue is not one specific disputed player

So the best use of binary search looks like:

1. use the ladder / plateau rule to identify a disputed period and candidate player(s)
2. binary-search the first non-zero window for the candidate player
3. inspect frozen-minute outgoing candidates and nearby PBP evidence

That makes it a follow-up diagnostic tool, not the primary classifier.

## Refinement To The Ladder Rule

After pressure-testing the canaries, I would refine the earlier candidate rule slightly.

The original version included:

- reject periods where the disputed `#5 / #6` identity flips between `10s` and `20/30s`

That helps on `0029700060`, but it may be too conservative as a hard reject rule.

Why:

- `0029700060` flips early and should indeed be treated as "do not trust the tiny window"
- but `0020100162` also changes between early and later windows, and the later stable answer looks useful

So I would change that rule from:

- hard reject if early window flips

to:

- treat early-window flips as a "needs stronger confirmation" signal

More concretely:

1. If `10s` differs from `20s` or `30s`, do not trust the early window.
2. Do not automatically throw the period away.
3. Instead, require a stronger later confirmation such as:
   - `30s == 60s`
   - or `30s == pre-sub`
4. Only then consider V3 informative.

That seems to preserve the useful late-stabilizing rescue cases while still protecting against the `0029700060` false-positive shape.

## Refined Candidate Rule To Explore

If I had to pick the next research rule to test, it would be:

1. Probe a ladder such as:
   - `10s`
   - `20s`
   - `30s`
   - `min(60s, first_sub - 1)`
   - optionally `pre-sub`
2. Use `30s -> 60s` or `30s -> pre-sub` agreement as the main stability anchor.
3. Treat `10s` disagreement as an instability flag, not an automatic reject.
4. Use early-PBP participation only as a supporting veto when it is strong and one-sided.
5. If the period is still interesting but ambiguous, binary-search the disputed player's first non-zero appearance.

This looks like the best combined path so far:

- the ladder handles the period-level classification
- the binary search handles the disputed-player localization

## Why The Snapshot-Matrix Idea Looks Good

The coarse scan idea appears valuable because the endpoint behaves like a piecewise-constant accumulation process:

- players appear in clusters
- seconds often grow in jumps
- early plateaus can be misleading
- later plateaus can be stable and informative

That means we probably do not need a brute-force per-second or per-10-second scan in production.

Instead, the scan can be used as research tooling to answer:

- how early does the stable plateau usually emerge
- which failure families stabilize by `20s`
- which ones need `30s` or `pre-sub`
- how often the early plateau is wrong but short-lived

## Open Questions

- Is `20s` already enough for most historical rescue cases, or is `30s` the safer anchor?
- Does using `30s` and `pre-sub` agreement get most of the benefit without over-querying?
- Is `StartRange + 1` sometimes a better normalization, or is offset consensus safer than choosing one offset?
- Are there seasons or eras where the V3 signal matures more slowly?
- Does overtime behave differently from regulation quarters in a systematic way?
- Can we classify periods into:
  - tiny-window false positive
  - early stable useful
  - late-stable useful
  - endpoint too noisy / inconclusive

## Recommended Next Steps

### 1. Build a research-only sparse scanner

Not for runtime yet. Just an exploration tool.

It should:

- probe a small fixed ladder like `10s`, `20s`, `30s`, `60s`, `pre-sub`
- store top-5 sets, sixth player, and gap
- record whether the set stabilized and when
- optionally record `StartRange -1`, `0`, `+1`

### 2. Expand the canary set

Use known classes:

- `strict 10 but wrong 5`
- incomplete-lineup rescue games
- known false-positive tiny-window V3 games
- a few clean control games by era

The goal is to measure how often stability-based V3 helps versus hurts.

### 3. Quantify "earliest stable window"

For each period, ask:

- what is the earliest window matching the later pre-sub answer
- what is the gap growth pattern
- does the stable answer also match external truth or downstream minute outcomes

### 4. Separate research from runtime

For now, keep this endpoint as a research tool and evidence source.

Do not promote a new runtime rule until:

- the canary set is broader
- the failure tradeoffs are quantified
- the plateau logic clearly beats both:
  - strict-only rollback
  - the old tiny-window V3 heuristic

### 5. Study offset robustness explicitly

Given the sensitivity to `StartRange +/- 1`, a follow-up study should test whether:

- consensus across offsets helps
- one offset is systematically better
- offset disagreement itself is a good "do not trust this period" signal

## Research Tooling Added

A research-only scanner now exists at:

- [explore_v3_stability.py](/Users/kmedved/Library/CloudStorage/OneDrive-Personal/github/darko/replace_tpdev/explore_v3_stability.py)

Current canary artifacts:

- [summary.json](/Users/kmedved/Library/CloudStorage/OneDrive-Personal/github/darko/replace_tpdev/v3_canary_research_20260317_v4/summary.json)
- [report.md](/Users/kmedved/Library/CloudStorage/OneDrive-Personal/github/darko/replace_tpdev/v3_canary_research_20260317_v4/report.md)

The scanner currently:

- probes a fixed ladder of `10s`, `20s`, `30s`, and `min(60s, first_sub - 1)`
- optionally includes `pre-sub`
- scores a stability rule using `20s -> 30s` and `30s -> later` agreement
- only treats a `10s` disagreement as meaningful if `10s` is after the first real event
- scores a separate future-action support rule from the selected early window up to that team's first substitution
- ignores substitution rows where the disputed "expected out" player appears only as the incoming player
- optionally binary-searches a disputed player's first non-zero appearance
- now includes a missing-sub inference pass for the `Detlef Schrempf` canary

### Current Starter Canary Read

Using the current three-part scoring framework:

- `stable_candidate` + future-action support:
  - `0020400932` period `3` -> `high`
- `stable_candidate` + no contradictory future signal:
  - `0020401139` period `5` -> `medium`
  - `0020700319` period `4` -> `medium`
- `stable_but_needs_confirmation` + strong future-action support:
  - `0029700060` period `3` -> `medium`
  - `0020100162` period `5` -> `medium`
  - `0020000576` period `5` -> `medium`

That feels more realistic than the cruder first pass because:

- the false-positive `0029700060` still stays in the cautious bucket rather than being promoted too aggressively
- the clearly useful rescue cases (`0020401139`, `0020700319`) now read as informative but not overclaimed
- `0020100162` improves from "late-stabilizing but suspicious" to "late-stabilizing with meaningful support"
- `0020400932` now looks genuinely strong because both the V3 plateau and the pre-sub future actions point the same way
- `0020000576` is now a formal example of a shared OT ghost plateau:
  - wrong through `20s`
  - flips at `22s`
  - selected formal anchor is `30s -> 60s`
  - Orlando and San Antonio both show the same ghost-starter pattern (`Garrity`, `Ferry`) freezing at `13s`

### Current Missing-Sub Read

The built-in missing-sub example is:

- `0029700438`, period `2`, Seattle, `Detlef Schrempf`

Current result:

- first non-zero localized to about `515.0s`
- early stable V3 period-start lineup:
  - `Aaron Williams`
  - `Dale Ellis`
  - `David Wingate`
  - `Gary Payton`
  - `Sam Perkins`
- logged lineup immediately before Schrempf's missing entry:
  - `Vin Baker`
  - `Dale Ellis`
  - `Hersey Hawkins`
  - `Gary Payton`
  - `Sam Perkins`
- future-action inference after Schrempf appears:
  - `Sam Perkins`: `0` events
  - `Dale Ellis`: `1` event
  - `Detlef Schrempf`: `3` events
  - `Hersey Hawkins`: `3` events
  - `Gary Payton`: `4` events
  - `Vin Baker`: `4` events
- current verdict:
  - `strong_best_fit`
  - best outgoing candidate: `Sam Perkins`

This is the first concrete example where the endpoint looks useful not because it names the whole starter set perfectly on one probe, but because it helps localize a missing silent entry and then lets later PBP actions narrow the outgoing player.

## Current Bottom Line

I do not think the right conclusion is:

- "the V3 endpoint is too noisy to use"

I think the better conclusion is:

- "the endpoint is informative, but the original question we asked it was too narrow and too early"

The canaries suggest there is real signal here, especially if we shift from:

- one tiny early probe

to:

- a small stability / plateau-based window ladder
- plus a separate future-action confirmation layer
- plus binary search / missing-sub inference for the truly weird cases

That looks like the most promising path for exploring this endpoint further without repeating the `0029700060` mistake.
