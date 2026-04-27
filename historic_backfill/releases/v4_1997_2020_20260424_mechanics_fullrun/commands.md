# V4 Historic Backfill Commands

This release record preserves the checked result for the 1997-2020 mechanics
fullrun. The JSON summaries in `summaries/original/` are exact originals from
the source workspace and may contain absolute historical paths.

## Validation

```bash
PYTHONPATH=. python -m pytest -q
PYTHONPATH=. python -m pytest -q historic_backfill/tests
PYTHONPATH=. python -m historic_backfill.runners.validate --scope=core
PYTHONPATH=. python -m historic_backfill.runners.validate --scope=cross-source
PYTHONPATH=. python -m historic_backfill.runners.validate --scope=provenance
```

`--scope=core` is the frozen-runtime gate: NBA data plus committed catalogs.
It may require the NBA runtime inputs under `historic_backfill/data/`, but it
must not require BBR or tpdev data. `--scope=cross-source` is optional and
skips missing BBR/tpdev sources. `--scope=provenance` is for re-review and
fails clearly when requested evidence sources are absent.

## Release Tag

After the integration commits and tests pass:

```bash
git tag -a historic-backfill-v4-1997-2020-20260424 \
  -m "V4 historic backfill release record"
```

The release manifest names this tag instead of embedding the commit SHA of the
commit that contains the manifest.
