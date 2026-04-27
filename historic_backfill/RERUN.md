# Historic Backfill Rerun Runbook

Run commands from the repo root:

```bash
cd /Users/konstantinmedvedovsky/migrate_tpdev/pbpstats
```

Use Python 3.10 or newer; `pyproject.toml` intentionally no longer advertises
pre-3.10 compatibility for this private fork.

## 1. Verify Imports And Unit Tests

```bash
PYTHONPATH=. python -m pytest -q
PYTHONPATH=. python -m pytest -q historic_backfill/tests
```

The full repo test command includes the parser tests and the historic backfill
tests because `pyproject.toml` sets both test paths.

## 2. Check Runtime Inputs

Core backfill uses NBA-only local data:

```text
historic_backfill/data/nba_raw.db
historic_backfill/data/playbyplayv2.parq
historic_backfill/data/playbyplayv3.parq
```

Optional evidence diagnostics use:

```text
historic_backfill/data/bbr/bbref_boxscores.db
historic_backfill/data/tpdev/full_pbp_new.parq
historic_backfill/data/tpdev/tpdev_box.parq
historic_backfill/data/tpdev/tpdev_box_new.parq
historic_backfill/data/tpdev/tpdev_box_cdn.parq
```

See `data/README.md` for the canonical local data layout.

## 3. Run Scoped Validation

Core validation is an input/catalog preflight. It should fail clearly when
required NBA data is missing, and it should validate committed catalogs such as
the PBP row override synthetic-sub canary. It should not require or inspect
BBR/tpdev paths. A successful preflight is not a full corpus rerun.

```bash
PYTHONPATH=. python -m historic_backfill.runners.validate --scope=core
```

Cross-source validation is optional. Missing BBR/tpdev files are reported as
skipped diagnostics, and the command still exits successfully.

```bash
PYTHONPATH=. python -m historic_backfill.runners.validate --scope=cross-source
```

Provenance validation is for evidence re-review. Missing BBR/tpdev evidence
files are treated as required and the command exits non-zero.

```bash
PYTHONPATH=. python -m historic_backfill.runners.validate --scope=provenance
```

## 4. Rerun A Historic Backfill

`historic_backfill/runners/cautious_rerun.py` remains the bulk orchestration
entrypoint. Its defaults resolve under `historic_backfill/data/` and
`historic_backfill/catalogs/`.
The runner snapshots `nba_raw.db`, `playbyplayv2.parq`, and
`playbyplayv3.parq` into each run cache; use `--pbp-v3-path` if the v3 source
lives somewhere else.

Check its current CLI before a full run:

```bash
PYTHONPATH=. python historic_backfill/runners/cautious_rerun.py --help
```

Use `historic_backfill/runs/` for generated outputs:

```text
historic_backfill/runs/full_history_1997_2020_YYYYMMDD_label/
historic_backfill/runs/smoke_0020400335_YYYYMMDD/
```

Generated output directories, DBs, parquets, notebooks, and scratch folders are
gitignored. Promote only small release contracts or new catalog decisions into
the repo.

## 5. Validate The V4 Release Record

The v4 manifest and sidecar contract can be checked without rerunning the full
corpus:

```bash
PYTHONPATH=. python -m historic_backfill.runners.validate_release_manifest \
  historic_backfill/releases/v4_1997_2020_20260424_mechanics_fullrun/release_manifest.json
```

The release tag for this integrated state is:

```bash
git tag -a historic-backfill-v4-1997-2020-20260424 \
  -m "V4 historic backfill release record"
```

Do not put the final commit SHA inside the committed manifest. The annotated tag
is the Git reference that records the final integrated commit.
