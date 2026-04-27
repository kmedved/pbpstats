# Historic Backfill Data Inputs

This directory is the canonical local home for runtime inputs used by
`historic_backfill`. The files here are intentionally gitignored because they
are large local data products.

## Required for NBA-Only Core Backfill

| File | Required for | Notes |
| --- | --- | --- |
| `nba_raw.db` | core backfill, NBA official boxscore lookups, and v3 `pbpv3` actions | Local SQLite cache of NBA stats endpoint payloads. The bulk runner's `fetch_pbp_v3` reads the `pbpv3` endpoint from this DB. |
| `playbyplayv2.parq` | v2 play-by-play DataFrame input | Historical NBA PBP source used by the backfill runner. |

Missing required NBA files should make core validation fail clearly.

## Optional NBA Companion Inputs

| File | Used by | Notes |
| --- | --- | --- |
| `playbyplayv3.parq` | parquet-direct experiments or one-off `fetch_pbp_v3_fn` workflows | Not required by `cautious_rerun.py`; the bulk runner gets v3 actions from `nba_raw.db`. |

## Optional Cross-Source Diagnostics

These files are evidence/provenance inputs. They are not required to run the
frozen production backfill.

| File | Used by | Behavior when absent |
| --- | --- | --- |
| `bbr/bbref_boxscores.db` | BBR boxscore/PBP comparison diagnostics | `validate --scope=cross-source` reports skipped diagnostics. |
| `tpdev/full_pbp_new.parq` | tpdev possession/PBP comparison diagnostics | `validate --scope=cross-source` reports skipped diagnostics. |
| `tpdev/tpdev_box.parq` | tpdev box comparison diagnostics | `validate --scope=cross-source` reports skipped diagnostics. |
| `tpdev/tpdev_box_new.parq` | tpdev box comparison diagnostics | `validate --scope=cross-source` reports skipped diagnostics. |
| `tpdev/tpdev_box_cdn.parq` | tpdev CDN box comparison diagnostics | `validate --scope=cross-source` reports skipped diagnostics. |

`validate --scope=provenance` is stricter: if a provenance re-review asks for a
specific evidence source, missing files should fail with a clear message.

## Local Placement

Expected shape:

```text
historic_backfill/data/
  nba_raw.db
  playbyplayv2.parq
  playbyplayv3.parq          # optional parquet-direct companion
  bbr/
    bbref_boxscores.db
  tpdev/
    full_pbp_new.parq
    tpdev_box.parq
    tpdev_box_new.parq
    tpdev_box_cdn.parq
```
