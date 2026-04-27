# Historic Backfill Migration Map

This file records the intentional path migration from the standalone
`/Users/konstantinmedvedovsky/migrate_tpdev/replace_tpdev` workspace into the
private `pbpstats` repo. The original workspace is left untouched as the
backup/source of truth during migration.

Starting `pbpstats` commit: `e53dab5ac6ad12e4d6fc59d634c4bbc89d6be6ee`.

| Old path | New path | Status |
| --- | --- | --- |
| `replace_tpdev/*.py` | `historic_backfill/` initially, then split across `runners/`, `audits/`, `common/`, `catalogs/` | Curated source import; reorganized in follow-up commits. |
| `replace_tpdev/tests/test_*.py` | `historic_backfill/tests/` | Curated test import; imports normalized later. |
| `replace_tpdev/pbp_row_overrides.py` | split between `pbpstats/offline/row_overrides.py` and `historic_backfill/catalogs/loader.py` | Generic parser mechanics move to `pbpstats`; catalog defaults stay in backfill. |
| `replace_tpdev/pbp_row_overrides.csv` | `historic_backfill/catalogs/pbp_row_overrides.csv` | Runtime catalog. |
| `replace_tpdev/validation_overrides.csv` | `historic_backfill/catalogs/validation_overrides.csv` | Runtime catalog. |
| `replace_tpdev/boxscore_audit_overrides.csv` | `historic_backfill/catalogs/boxscore_audit_overrides.csv` | Runtime catalog. |
| `replace_tpdev/boxscore_source_overrides.csv` | `historic_backfill/catalogs/boxscore_source_overrides.csv` | Runtime catalog. |
| `replace_tpdev/pbp_stat_overrides.csv` | `historic_backfill/catalogs/pbp_stat_overrides.csv` | Runtime catalog. |
| `replace_tpdev/manual_poss_fixes.json` | `historic_backfill/catalogs/manual_poss_fixes.json` | Runtime catalog. |
| `replace_tpdev/audit_event_player_on_court.py` | `historic_backfill/audits/core/event_player_on_court.py` | NBA-only core audit. |
| `replace_tpdev/audit_period_starters_against_tpdev.py` | `historic_backfill/audits/cross_source/period_starters.py` plus shared helpers in `historic_backfill/common/` | Cross-source diagnostic after helper extraction. |
| `replace_tpdev/audit_lineup_possession_starts_against_tpdev.py` | `historic_backfill/audits/cross_source/lineup_possession_starts.py` | Cross-source diagnostic. |
| `replace_tpdev/bbr_*.py` | `historic_backfill/audits/cross_source/` | Optional BBR diagnostics. |
| `replace_tpdev/cautious_rerun.py` | `historic_backfill/runners/cautious_rerun.py` | Bulk orchestration. |
| `replace_tpdev/0c2_build_tpdev_box_stats_version_v9b.py` | `historic_backfill/runners/build_tpdev_box_stats_v9b.py` | Runtime companion script renamed to an importable module name. |
| `replace_tpdev/phase7_mechanics_fullrun_v4_release_explainer_20260425.md` | `historic_backfill/releases/v4_1997_2020_20260424_mechanics_fullrun/closure_note.md` | Immutable v4 release note. |
| `replace_tpdev/reviewed_frontier_policy_overlay_20260424_mechanics_fullrun_v4.csv` | `historic_backfill/releases/v4_1997_2020_20260424_mechanics_fullrun/policy/` | Immutable v4 release policy. |
| `replace_tpdev/phase7_open_blocker_inventory_20260424_mechanics_fullrun_v4.csv` | `historic_backfill/releases/v4_1997_2020_20260424_mechanics_fullrun/inventories/` | Immutable v4 release inventory. |
| `replace_tpdev/phase7_true_blocker_shortlist_20260424_mechanics_fullrun_v4.csv` | `historic_backfill/releases/v4_1997_2020_20260424_mechanics_fullrun/inventories/` | Immutable v4 release inventory. |
