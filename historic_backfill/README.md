# Historic Backfill

`historic_backfill/` is the corpus-scale correction, audit, and release overlay
for historic NBA games, mainly 1997-2020.

It depends on the parser package in `pbpstats/`, but it is not part of that
package. Live/CDN-fed games should be able to use `pbpstats` alone. Historic
reruns use `pbpstats` plus the committed catalogs here, then validate through
the NBA-only core audits.

This private fork targets Python 3.10+.

## Runtime Model

```text
Frozen production runtime = NBA data + pbpstats + committed catalogs.
Optional diagnostics = BBR/tpdev, skipped when absent.
Provenance re-review = may require BBR/tpdev and should fail clearly if requested evidence is missing.
```

BBR and tpdev are evidence sources. They explain why some rules and overrides
exist, but they are not required to apply the frozen historic backfill.

## Directory Map

| Path | Role |
| --- | --- |
| `catalogs/` | Active runtime catalogs and loaders. These are applied during historic reruns. |
| `catalogs/overrides/` | Active lineup correction manifests and note files. |
| `audits/core/` | NBA-only audits and release-gate builders. These must not import cross-source diagnostics. |
| `audits/cross_source/` | Optional BBR/tpdev diagnostics and evidence recheck tools. |
| `common/` | Shared NBA/PBP helpers used by runners and core audits. |
| `runners/` | Operational entrypoints, including `cautious_rerun.py` and `validate.py`. |
| `docs/` | Living notes and future analysis. |
| `provenance/` | Evidence indexes or per-rule citations when maintained separately from catalogs. |
| `releases/` | Immutable release records. The v4 record preserves the reviewed 1997-2020 gate. |
| `data/` | Local runtime inputs, gitignored except its README. |
| `runs/` | Generated outputs and scratch reruns, gitignored except its README. |
| `tests/` | Historic backfill tests. |

## Parser Boundary

Generic row override mechanics live in:

```text
pbpstats/offline/row_overrides.py
```

That parser module has no default path to `historic_backfill/catalogs/` and does
not import `historic_backfill`. Historic catalog defaults live in:

```text
historic_backfill/catalogs/loader.py
```

The intended dependency direction is:

```text
historic_backfill -> pbpstats
pbpstats -> no historic_backfill import
```

## Validation Scopes

Use the scoped validation runner:

```bash
python -m historic_backfill.runners.validate --scope=core
python -m historic_backfill.runners.validate --scope=cross-source
python -m historic_backfill.runners.validate --scope=provenance
```

`core` checks required NBA runtime inputs and committed runtime catalogs; when
the local NBA files exist, it opens `nba_raw.db`, checks required
`raw_responses` endpoints, and verifies the `playbyplayv2.parq` schema. It never
checks BBR/tpdev paths. It is an input/catalog preflight, not a full corpus
rerun. `cross-source` reports missing optional BBR/tpdev inputs as skipped
diagnostics. `provenance` is stricter and fails when evidence files needed for
re-review are missing.

See `RERUN.md` for operational commands.

## V4 Release Record

The integrated v4 release record is:

```text
historic_backfill/releases/v4_1997_2020_20260424_mechanics_fullrun/
```

It preserves the reviewed overlay, sidecar contract, inventories, original
summary JSONs, checksums, and the closure note. The manifest names the release
tag `historic-backfill-v4-1997-2020-20260424` rather than trying to embed the
SHA of the commit that contains the manifest.

The original summary JSONs are stored under `summaries/original/` and preserve
historical absolute paths from the source workspace intentionally.

## Future Cleanup

`cautious_rerun.py` remains the bulk orchestration layer for now. A future
simplification can route one-off or bulk games more directly through:

```python
from pbpstats.offline import get_possessions_from_df
from pbpstats.offline.row_overrides import apply_pbp_row_overrides
```

That refactor is intentionally deferred so this migration stays about structure,
catalogs, import boundaries, and release records.
