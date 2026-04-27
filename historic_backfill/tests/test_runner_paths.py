from pathlib import Path

import historic_backfill.catalogs.lineup_correction_manifest as lineup_manifest
import historic_backfill.common.game_context as game_context
import historic_backfill.runners.build_tpdev_box_stats_v9b as build_runner
import historic_backfill.runners.cautious_rerun as cautious_rerun


HISTORIC_ROOT = Path(__file__).resolve().parents[1]


def test_cautious_rerun_defaults_point_to_historic_backfill_data_and_catalogs():
    assert cautious_rerun.ROOT == HISTORIC_ROOT
    assert cautious_rerun.NOTEBOOK_DUMP == HISTORIC_ROOT / "runners" / "build_tpdev_box_stats_v9b.py"
    assert cautious_rerun.DEFAULT_DB == HISTORIC_ROOT / "data" / "nba_raw.db"
    assert cautious_rerun.DEFAULT_PARQUET == HISTORIC_ROOT / "data" / "playbyplayv2.parq"
    assert cautious_rerun.DEFAULT_PBP_V3 == HISTORIC_ROOT / "data" / "playbyplayv3.parq"
    assert cautious_rerun.DEFAULT_OVERRIDES == HISTORIC_ROOT / "catalogs" / "validation_overrides.csv"


def test_build_tpdev_box_stats_runner_is_importable_after_numeric_rename():
    assert build_runner.DB_PATH == HISTORIC_ROOT / "data" / "nba_raw.db"
    assert build_runner.DEFAULT_PARQUET_PATH == HISTORIC_ROOT / "data" / "playbyplayv2.parq"


def test_common_game_context_defaults_point_to_historic_backfill_data():
    assert game_context.DEFAULT_DB_PATH == HISTORIC_ROOT / "data" / "nba_raw.db"
    assert game_context.DEFAULT_PARQUET_PATH == HISTORIC_ROOT / "data" / "playbyplayv2.parq"
    assert game_context.DEFAULT_PBP_V3_PATH == HISTORIC_ROOT / "data" / "playbyplayv3.parq"
    assert game_context.DEFAULT_FILE_DIRECTORY == HISTORIC_ROOT / "data"


def test_lineup_correction_manifest_defaults_point_to_catalogs_and_data():
    assert lineup_manifest.DEFAULT_OVERRIDES_DIR == HISTORIC_ROOT / "catalogs" / "overrides"
    assert (
        lineup_manifest.DEFAULT_MANIFEST_PATH
        == HISTORIC_ROOT / "catalogs" / "overrides" / "correction_manifest.json"
    )
    assert lineup_manifest.DEFAULT_DB_PATH == HISTORIC_ROOT / "data" / "nba_raw.db"
    assert lineup_manifest.DEFAULT_PARQUET_PATH == HISTORIC_ROOT / "data" / "playbyplayv2.parq"
    assert lineup_manifest.DEFAULT_FILE_DIRECTORY == HISTORIC_ROOT / "data"
