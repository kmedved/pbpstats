A package to scrape and parse NBA, WNBA and G-League play-by-play data.

# Features
* Adds lineup on floor for all events
* Adds detailed data for each possession including start time, end time, score margin, how the previous possession ended
* Shots, rebounds and assists broken down by shot zone
* Supports both stats.nba.com and data.nba.com endpoints
* Supports NBA, WNBA and G-League stats
* All stats on pbpstats.com are derived from these stats
* Fixes order of events for some common cases in which events are out of order
* Supports an opt-in NBA stats.nba.com `playbyplayv3` synthetic fallback that
  emits the existing playbyplayv2-shaped PBP contract

# Installation
Tested on Python 3.10-3.12
```
pip install pbpstats
```

# LLM Context
LLM-facing context artifacts live in `context/`.

- Start with `context/REPO_ARCHITECTURE.md`
- For guided context, add one `context/COMPRESSED_*.md`
- For oracle workflows, add `context/FILE_INDEX.md`
- For implementation tasks, add raw source for the touched files

Refresh checked-in context artifacts with `python scripts/generate_repo_architecture_sync.py`.
Refresh local bundles with `python scripts/build_context_bundle.py`.
Version policy: Policy B, so only shipped/runtime behavior changes require a version bump.

# stats.nba.com PBP Endpoint Strategy
For NBA `stats_nba` `Pbp`, `EnhancedPbp`, and `Possessions` resources, the
source loaders accept `endpoint_strategy`:

* `v2` - default compatibility mode. Uses true playbyplayv2 file/web data.
* `v3_synthetic` - uses playbyplayv3 and emits synthetic playbyplayv2-shaped rows.
* `auto` - tries v2 first and falls back to synthetic v3 only when v2 is missing
  or malformed.

True v2 files remain canonical under `/pbp`. Raw v3 cache files are written under
`/pbp_v3`, and synthetic rows are written under `/pbp_synthetic_v3`.

League coverage is intentionally conservative. The synthetic v3 PBP path is
validated for NBA games only. WNBA `shotchartdetail` uses the same shot-chart
schema and remains supported by the normal `Shots` and enhanced-coordinate path,
but sampled WNBA `playbyplayv3` payloads omit v2 participant roles such as
foul-drawn players and some complete jump-ball roles. G League synthetic v3 PBP
also remains unsupported until league-specific fixtures prove parity.

# Local Development
Using [poetry](https://python-poetry.org/) for package management. Install it first if it is not already installed on your system.

Clone the repo on the default `main` branch:

`git clone https://github.com/kmedved/pbpstats.git`

`cd pbpstats`

Install dependencies:

`poetry install`

Activate virtualenv:

`poetry shell`

Install pre-commit:

`pre-commit install`

# Historic Backfill Pipeline
This repo also hosts the in-tree historic backfill pipeline at `historic_backfill/`. It is the corpus-scale correction, audit, and release overlay for historic NBA games (mainly 1997-2020). It depends on `pbpstats` but is not part of the parser package; live/CDN-fed games go through `pbpstats` alone.

Quick links:
- `historic_backfill/README.md` for orientation and directory map
- `historic_backfill/RERUN.md` for the operational runbook
- `historic_backfill/releases/v4_1997_2020_20260424_mechanics_fullrun/` for the immutable v4 release record

Scoped validation entrypoints:

```
PYTHONPATH=. python -m historic_backfill.runners.validate --scope=core
PYTHONPATH=. python -m historic_backfill.runners.validate --scope=cross-source
PYTHONPATH=. python -m historic_backfill.runners.validate --scope=provenance
```

`--scope=core` is the NBA-only input/catalog preflight and never touches BBR/tpdev paths. `--scope=cross-source` reports skipped diagnostics when optional BBR/tpdev inputs are absent. `--scope=provenance` is stricter and fails clearly when evidence files needed for re-review are missing.

# Legacy `replace_tpdev` Compatibility Smoke
A legacy compatibility gate at `scripts/run_replace_tpdev_compatibility_smoke.py` runs the Golden Canary suite from the standalone `replace_tpdev` workspace (the migration source) against the current editable `pbpstats` checkout. It defaults to `../replace_tpdev` and remains usable as long as that backup workspace is present.

`python scripts/run_replace_tpdev_compatibility_smoke.py --replace-tpdev-root ../replace_tpdev`

Notes:
- The script pins `--pbpstats-repo` to the current editable checkout automatically.
- It exits non-zero if `failed_games`, `event_stats_errors`, or the Golden Canary suite pass flags are not clean.
- If `--output-dir` is omitted, it writes to a temporary directory and prints the resolved path.
- This is a local data-dependent gate; it is not part of GitHub Actions because CI does not have the sibling repo and historical runtime inputs.
- For new work, prefer the in-tree `historic_backfill.runners.validate` workflow above.
