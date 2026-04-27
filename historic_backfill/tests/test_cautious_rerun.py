from __future__ import annotations

from pathlib import Path

import pandas as pd

import historic_backfill.runners.cautious_rerun as runner
from pbpstats.offline.row_overrides import PBP_ROW_OVERRIDE_ACTION_COLUMN


def _write_runtime_sources(base_dir: Path) -> dict[str, Path]:
    live_dir = base_dir / "live"
    overrides_dir = live_dir / "overrides"
    overrides_dir.mkdir(parents=True)

    files = {
        "db_path": live_dir / "nba_raw.db",
        "parquet_path": live_dir / "playbyplayv2.parq",
        "overrides_path": live_dir / "validation_overrides.csv",
        "boxscore_source_overrides_path": live_dir / "boxscore_source_overrides.csv",
        "notebook_dump_path": live_dir / "build_tpdev_box_stats_v9b.py",
        "period_starters_v6": live_dir / "period_starters_v6.parquet",
        "period_starters_v5": live_dir / "period_starters_v5.parquet",
        "lineup_window_overrides": overrides_dir / "lineup_window_overrides.json",
        "period_starters_overrides": overrides_dir / "period_starters_overrides.json",
    }
    for path in files.values():
        path.write_text(path.name, encoding="utf-8")
    return files


def _fake_hydrate_runtime_input(source_path: Path, cache_dir: Path) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    target_path = cache_dir / Path(source_path).name
    target_path.write_text(f"copied:{Path(source_path).name}", encoding="utf-8")
    return target_path


def test_prepare_local_runtime_inputs_fresh_copy_ignores_latest_global_cache(
    tmp_path: Path,
    monkeypatch,
) -> None:
    files = _write_runtime_sources(tmp_path)
    cache_dir = tmp_path / "run" / "_local_runtime_cache"
    cached_dir = tmp_path / "other_run" / "_local_runtime_cache"
    cached_dir.mkdir(parents=True)
    for name in [
        "nba_raw.db",
        "playbyplayv2.parq",
        "build_tpdev_box_stats_v9b.py",
        "boxscore_source_overrides.csv",
        "period_starters_v6.parquet",
        "period_starters_v5.parquet",
    ]:
        (cached_dir / name).write_text(f"cached:{name}", encoding="utf-8")

    monkeypatch.setattr(runner, "NOTEBOOK_DUMP", files["notebook_dump_path"])
    monkeypatch.setattr(runner, "NOTEBOOK_LOCAL_IMPORT_PRELOADS", [])
    monkeypatch.setattr(runner, "_hydrate_runtime_input", _fake_hydrate_runtime_input)
    monkeypatch.setattr(
        runner,
        "_latest_cached_runtime_copy",
        lambda filename: cached_dir / filename,
    )
    monkeypatch.setattr(runner, "load_boxscore_source_overrides", lambda _path: {})
    monkeypatch.setattr(runner, "set_boxscore_source_overrides", lambda _overrides: None)

    runtime_inputs = runner.prepare_local_runtime_inputs(
        cache_dir,
        db_path=files["db_path"],
        parquet_path=files["parquet_path"],
        overrides_path=files["overrides_path"],
        boxscore_source_overrides_path=files["boxscore_source_overrides_path"],
        period_starter_parquet_paths=[files["period_starters_v6"], files["period_starters_v5"]],
        file_directory=files["db_path"].parent,
        runtime_input_cache_mode="fresh-copy",
    )

    assert runtime_inputs["db_path"] == cache_dir / "nba_raw.db"
    assert runtime_inputs["parquet_path"] == cache_dir / "playbyplayv2.parq"
    assert runtime_inputs["notebook_dump_path"] == cache_dir / "build_tpdev_box_stats_v9b.py"
    assert runtime_inputs["period_starter_parquet_paths"] == [
        cache_dir / "period_starters_v6.parquet",
        cache_dir / "period_starters_v5.parquet",
    ]
    assert runtime_inputs["runtime_input_provenance"]["inputs"]["db_path"]["resolution_kind"] == "copied_to_run_cache"
    assert runtime_inputs["file_directory"] != files["db_path"].parent
    assert (
        runtime_inputs["file_directory"] / "overrides" / "lineup_window_overrides.json"
    ).read_text(encoding="utf-8") == "lineup_window_overrides.json"


def test_prepare_local_runtime_inputs_can_reuse_global_cache_when_explicitly_requested(
    tmp_path: Path,
    monkeypatch,
) -> None:
    files = _write_runtime_sources(tmp_path)
    cache_dir = tmp_path / "run" / "_local_runtime_cache"
    cached_dir = tmp_path / "other_run" / "_local_runtime_cache"
    cached_dir.mkdir(parents=True)
    cached_paths = {}
    for name in [
        "nba_raw.db",
        "playbyplayv2.parq",
        "build_tpdev_box_stats_v9b.py",
        "boxscore_source_overrides.csv",
        "period_starters_v6.parquet",
        "period_starters_v5.parquet",
    ]:
        cached_path = cached_dir / name
        cached_path.write_text(f"cached:{name}", encoding="utf-8")
        cached_paths[name] = cached_path

    monkeypatch.setattr(runner, "NOTEBOOK_DUMP", files["notebook_dump_path"])
    monkeypatch.setattr(runner, "NOTEBOOK_LOCAL_IMPORT_PRELOADS", [])
    monkeypatch.setattr(runner, "_hydrate_runtime_input", _fake_hydrate_runtime_input)
    monkeypatch.setattr(
        runner,
        "_latest_cached_runtime_copy",
        lambda filename: cached_paths.get(filename),
    )
    monkeypatch.setattr(runner, "load_boxscore_source_overrides", lambda _path: {})
    monkeypatch.setattr(runner, "set_boxscore_source_overrides", lambda _overrides: None)

    runtime_inputs = runner.prepare_local_runtime_inputs(
        cache_dir,
        db_path=files["db_path"],
        parquet_path=files["parquet_path"],
        overrides_path=files["overrides_path"],
        boxscore_source_overrides_path=files["boxscore_source_overrides_path"],
        period_starter_parquet_paths=[files["period_starters_v6"], files["period_starters_v5"]],
        file_directory=files["db_path"].parent,
        runtime_input_cache_mode="reuse-latest-global-cache",
    )

    assert runtime_inputs["db_path"] == cached_paths["nba_raw.db"]
    assert runtime_inputs["parquet_path"] == cached_paths["playbyplayv2.parq"]
    assert runtime_inputs["notebook_dump_path"] == cached_paths["build_tpdev_box_stats_v9b.py"]
    assert runtime_inputs["period_starter_parquet_paths"] == [
        cached_paths["period_starters_v6.parquet"],
        cached_paths["period_starters_v5.parquet"],
    ]
    assert runtime_inputs["runtime_input_provenance"]["inputs"]["db_path"]["resolution_kind"] == "reused_global_cache"
    assert runtime_inputs["runtime_input_provenance"]["inputs"]["overrides_path"]["resolution_kind"] == "copied_to_run_cache"


def test_prepare_local_runtime_file_directory_snapshots_overrides(tmp_path: Path) -> None:
    live_dir = tmp_path / "live"
    overrides_dir = live_dir / "overrides"
    pbp_dir = live_dir / "pbp"
    overrides_dir.mkdir(parents=True)
    pbp_dir.mkdir(parents=True)
    (overrides_dir / "lineup_window_overrides.json").write_text('{"a": 1}\n', encoding="utf-8")
    (pbp_dir / "stats_foo.json").write_text("pbp", encoding="utf-8")

    info = runner.prepare_local_runtime_file_directory(
        tmp_path / "runtime_file_directory",
        live_file_directory=live_dir,
    )
    runtime_dir = Path(info["runtime_file_directory"]["path"])
    snapshot_path = runtime_dir / "overrides" / "lineup_window_overrides.json"

    assert snapshot_path.read_text(encoding="utf-8") == '{"a": 1}\n'
    (overrides_dir / "lineup_window_overrides.json").write_text('{"a": 2}\n', encoding="utf-8")
    assert snapshot_path.read_text(encoding="utf-8") == '{"a": 1}\n'
    assert (runtime_dir / "pbp").exists()


def test_prepare_local_runtime_inputs_reuse_validated_cache_reuses_matching_files(
    tmp_path: Path,
    monkeypatch,
) -> None:
    files = _write_runtime_sources(tmp_path)
    cache_dir = tmp_path / "run" / "_local_runtime_cache"

    monkeypatch.setattr(runner, "NOTEBOOK_DUMP", files["notebook_dump_path"])
    monkeypatch.setattr(runner, "NOTEBOOK_LOCAL_IMPORT_PRELOADS", [])
    monkeypatch.setattr(runner, "load_boxscore_source_overrides", lambda _path: {})
    monkeypatch.setattr(runner, "set_boxscore_source_overrides", lambda _overrides: None)

    first_runtime_inputs = runner.prepare_local_runtime_inputs(
        cache_dir,
        db_path=files["db_path"],
        parquet_path=files["parquet_path"],
        overrides_path=files["overrides_path"],
        boxscore_source_overrides_path=files["boxscore_source_overrides_path"],
        period_starter_parquet_paths=[files["period_starters_v6"], files["period_starters_v5"]],
        file_directory=files["db_path"].parent,
        runtime_input_cache_mode="reuse-validated-cache",
    )
    manifest_path = Path(
        first_runtime_inputs["runtime_input_provenance"]["validated_cache_manifest_path"]
    )
    assert manifest_path.exists()

    db_cached_path = cache_dir / "nba_raw.db"
    db_cached_path.write_text("manually mutated cache\n", encoding="utf-8")
    second_runtime_inputs = runner.prepare_local_runtime_inputs(
        cache_dir,
        db_path=files["db_path"],
        parquet_path=files["parquet_path"],
        overrides_path=files["overrides_path"],
        boxscore_source_overrides_path=files["boxscore_source_overrides_path"],
        period_starter_parquet_paths=[files["period_starters_v6"], files["period_starters_v5"]],
        file_directory=files["db_path"].parent,
        runtime_input_cache_mode="reuse-validated-cache",
    )

    assert second_runtime_inputs["db_path"] == db_cached_path
    assert db_cached_path.read_text(encoding="utf-8") == "manually mutated cache\n"
    assert (
        second_runtime_inputs["runtime_input_provenance"]["inputs"]["db_path"]["resolution_kind"]
        == "validated_run_cache_hit"
    )


def test_prepare_local_runtime_inputs_reuse_validated_cache_refreshes_stale_entries(
    tmp_path: Path,
    monkeypatch,
) -> None:
    files = _write_runtime_sources(tmp_path)
    cache_dir = tmp_path / "run" / "_local_runtime_cache"

    monkeypatch.setattr(runner, "NOTEBOOK_DUMP", files["notebook_dump_path"])
    monkeypatch.setattr(runner, "NOTEBOOK_LOCAL_IMPORT_PRELOADS", [])
    monkeypatch.setattr(runner, "load_boxscore_source_overrides", lambda _path: {})
    monkeypatch.setattr(runner, "set_boxscore_source_overrides", lambda _overrides: None)

    runner.prepare_local_runtime_inputs(
        cache_dir,
        db_path=files["db_path"],
        parquet_path=files["parquet_path"],
        overrides_path=files["overrides_path"],
        boxscore_source_overrides_path=files["boxscore_source_overrides_path"],
        period_starter_parquet_paths=[files["period_starters_v6"], files["period_starters_v5"]],
        file_directory=files["db_path"].parent,
        runtime_input_cache_mode="reuse-validated-cache",
    )

    files["db_path"].write_text("updated source db\n", encoding="utf-8")
    refreshed_runtime_inputs = runner.prepare_local_runtime_inputs(
        cache_dir,
        db_path=files["db_path"],
        parquet_path=files["parquet_path"],
        overrides_path=files["overrides_path"],
        boxscore_source_overrides_path=files["boxscore_source_overrides_path"],
        period_starter_parquet_paths=[files["period_starters_v6"], files["period_starters_v5"]],
        file_directory=files["db_path"].parent,
        runtime_input_cache_mode="reuse-validated-cache",
    )

    assert (cache_dir / "nba_raw.db").read_text(encoding="utf-8") == "updated source db\n"
    assert (
        refreshed_runtime_inputs["runtime_input_provenance"]["inputs"]["db_path"]["resolution_kind"]
        == "validated_run_cache_refresh"
    )


def test_v9b_namespace_uses_historic_pbp_row_override_catalog():
    namespace = runner.load_v9b_namespace()
    game_df = pd.DataFrame(
        {
            "GAME_ID": ["0020400335"] * 2,
            "EVENTNUM": [147, 149],
            "EVENTMSGTYPE": [6, 8],
            "EVENTMSGACTIONTYPE": [3, 0],
            "PERIOD": [2, 2],
            "PCTIMESTRING": ["7:59", "7:59"],
            "WCTIMESTRING": ["8:59 PM", "8:59 PM"],
            "HOMEDESCRIPTION": ["Baxter L.B.FOUL (P2.T2)", ""],
            "NEUTRALDESCRIPTION": ["", ""],
            "VISITORDESCRIPTION": ["", "SUB: Barry FOR Udrih"],
            "SCORE": ["", ""],
            "SCOREMARGIN": ["", ""],
            "PLAYER1_ID": [2437, 2757],
            "PLAYER1_NAME": ["Lonny Baxter", "Beno Udrih"],
            "PLAYER1_TEAM_ID": [1610612740, 1610612759],
            "PLAYER2_ID": [0, 699],
            "PLAYER2_NAME": ["", "Brent Barry"],
            "PLAYER2_TEAM_ID": ["", 1610612759],
            "PLAYER3_ID": [0, 0],
            "PLAYER3_NAME": ["", ""],
            "PLAYER3_TEAM_ID": ["", ""],
            "event_num": [147, 149],
            "period": [2, 2],
            "clock_seconds_remaining": [479.0, 479.0],
            "description": ["Baxter L.B.FOUL (P2.T2)", "SUB: Barry FOR Udrih"],
        }
    )

    result = namespace["apply_pbp_row_overrides"](game_df)
    inserted = result[result["EVENTNUM"] == 148].iloc[0]

    assert inserted[PBP_ROW_OVERRIDE_ACTION_COLUMN] == "insert_sub_before"
