# Historic Backfill Migration Exclusions

The migration intentionally keeps the runnable pipeline, catalogs, tests, docs,
and small release contracts in git while excluding local data and generated run
outputs. The original standalone workspace remains available at:

`/Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev`

## Excluded Classes

| Pattern or file class | Old location | Reason |
| --- | --- | --- |
| `nba_raw.db` | `replace_tpdev/nba_raw.db` | Required local NBA input; too large for git. Documented under `historic_backfill/data/README.md`. |
| `playbyplayv2.parq` and v3 parquet inputs | `replace_tpdev/*.parq` | Local runtime data; too large for git. Documented under `historic_backfill/data/README.md`. |
| Full-history output dirs | `replace_tpdev/full_history_*` | Generated outputs; preserve summaries/checksums/release records instead of committing full output. |
| Scratch reruns | `replace_tpdev/_tmp*` | Temporary validation artifacts; not authoritative runtime source. |
| Historical residual output dirs | `replace_tpdev/phase7_raw_residuals_*`, `replace_tpdev/phase7_reviewed_residuals_*`, `replace_tpdev/phase7_reviewed_frontier_inventory_*`, `replace_tpdev/phase7_reviewed_pm_reference_report_*` | Generated artifacts; selected v4 summaries are committed in the release record. |
| Sidecar join smoke output dirs | `replace_tpdev/reviewed_release_quality_sidecar_join_smoke_*` | Generated smoke outputs; selected v4 sidecar contract files are committed. |
| Notebooks | `replace_tpdev/*.ipynb` | Exploratory notebooks are ignored; required runtime logic is committed as Python files. |
| `__pycache__`, `.pytest_cache`, `.DS_Store` | all paths | Local/cache files. |
| Large exploratory CSVs/parquets | `replace_tpdev/nbastats_*.csv`, `replace_tpdev/nbastatsv3_*.csv`, `replace_tpdev/orphan_*.csv`, generated `darko_*.parquet` | Data products or exploratory outputs, not runtime catalogs. |

## Archive Note

No external archive was created as part of this commit. The untouched original
workspace is the migration backup. If generated outputs need to be preserved
outside git later, archive them with SHA256 checksums and record the archive
location here.

