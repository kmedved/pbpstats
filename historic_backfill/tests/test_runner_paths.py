from pathlib import Path

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
