import pandas as pd

from historic_backfill.audits.cross_source.build_override_provenance_report import _tpdev_basic_value


def test_tpdev_basic_value_reads_direct_stat():
    df = pd.DataFrame([{"FGA": 15, "OREB": 2, "DRB": 5}])

    assert _tpdev_basic_value(df, "FGA") == "15"


def test_tpdev_basic_value_derives_total_rebounds():
    df = pd.DataFrame([{"OREB": 2, "DRB": 5}])

    assert _tpdev_basic_value(df, "REB") == "7"


def test_tpdev_basic_value_handles_missing_rebound_parts():
    df = pd.DataFrame([{"OREB": 2}])

    assert _tpdev_basic_value(df, "REB") == ""
