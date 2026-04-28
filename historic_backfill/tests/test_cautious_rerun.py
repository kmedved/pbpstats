from __future__ import annotations

import json
import os
import sqlite3
import threading
import zlib
from pathlib import Path

import pandas as pd
import pytest

import historic_backfill.runners.cautious_rerun as runner
from historic_backfill.catalogs.boxscore_source_overrides import BOXSCORE_SOURCE_COLUMNS
from historic_backfill.common import game_context
from historic_backfill.common.period_boxscore_source_loader import STARTER_LOOKUP_COLUMNS
from historic_backfill.runners import build_tpdev_box_stats_v9b as v9b
from pbpstats.offline.row_overrides import PBP_ROW_OVERRIDE_ACTION_COLUMN


def _write_runtime_overrides_dir(overrides_dir: Path) -> None:
    overrides_dir.mkdir(parents=True, exist_ok=True)
    (overrides_dir / "correction_manifest.json").write_text(
        '{"manifest_version": "test", "corrections": [], "residual_annotations": []}\n',
        encoding="utf-8",
    )
    (overrides_dir / "lineup_window_overrides.json").write_text(
        "{}\n", encoding="utf-8"
    )
    (overrides_dir / "period_starters_overrides.json").write_text(
        "{}\n", encoding="utf-8"
    )


def _write_pbp_row_catalog(path: Path, extra_rows: str = "") -> None:
    path.write_text(
        "game_id,action,event_num,anchor_event_num,notes,period,pctimestring,wctimestring,description_side,"
        "player_out_id,player_out_name,player_out_team_id,player_in_id,player_in_name,player_in_team_id\n"
        "0020400335,insert_sub_before,148,149,canary,2,7:59,,home,"
        "2747,JR Smith,1610612740,2454,Junior Harrington,1610612740\n"
        f"{extra_rows}",
        encoding="utf-8",
    )


def _boxscore_row(
    game_id: str,
    team_id: int,
    player_id: int,
    player_name: str,
    start_position: str,
) -> list[object]:
    values: dict[str, object] = {
        "GAME_ID": game_id,
        "TEAM_ID": team_id,
        "TEAM_ABBREVIATION": "HOM" if team_id == 1610612740 else "AWY",
        "TEAM_CITY": "Home" if team_id == 1610612740 else "Away",
        "PLAYER_ID": player_id,
        "PLAYER_NAME": player_name,
        "NICKNAME": player_name.split()[0],
        "START_POSITION": start_position,
        "COMMENT": "",
        "MIN": "12:00",
        "FGM": 0,
        "FGA": 0,
        "FG_PCT": 0.0,
        "FG3M": 0,
        "FG3A": 0,
        "FG3_PCT": 0.0,
        "FTM": 0,
        "FTA": 0,
        "FT_PCT": 0.0,
        "OREB": 0,
        "DREB": 0,
        "REB": 0,
        "AST": 0,
        "STL": 0,
        "BLK": 0,
        "TO": 0,
        "PF": 0,
        "PTS": 0,
        "PLUS_MINUS": 0,
    }
    return [values[column] for column in BOXSCORE_SOURCE_COLUMNS]


def _write_valid_runtime_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.unlink(missing_ok=True)
    payloads = {
        "boxscore": {
            "resultSets": [
                {
                    "headers": BOXSCORE_SOURCE_COLUMNS,
                    "rowSet": [
                        _boxscore_row(
                            "0029700001",
                            1610612740,
                            123,
                            "Home Player",
                            "G",
                        ),
                        _boxscore_row(
                            "0029700001",
                            1610612741,
                            456,
                            "Away Player",
                            "F",
                        ),
                    ],
                }
            ]
        },
        "summary": {
            "resultSets": [
                {
                    "headers": ["GAME_ID", "HOME_TEAM_ID", "VISITOR_TEAM_ID"],
                    "rowSet": [["0029700001", 1610612740, 1610612741]],
                },
            ]
        },
        "pbpv3": {"game": {"actions": []}},
    }
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "CREATE TABLE raw_responses (game_id TEXT, endpoint TEXT, team_id INTEGER, data BLOB)"
        )
        for endpoint, payload in payloads.items():
            conn.execute(
                "INSERT INTO raw_responses (game_id, endpoint, team_id, data) VALUES (?, ?, NULL, ?)",
                ("0029700001", endpoint, json.dumps(payload).encode("utf-8")),
            )
        conn.commit()
    finally:
        conn.close()


def _write_valid_runtime_parquet(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "GAME_ID": ["0029700001"],
            "EVENTNUM": [1],
            "EVENTMSGTYPE": [12],
            "EVENTMSGACTIONTYPE": [0],
            "PERIOD": [1],
            "SEASON": [1998],
            "PCTIMESTRING": ["12:00"],
            "HOMEDESCRIPTION": ["Start Period"],
            "VISITORDESCRIPTION": [""],
            "PLAYER1_ID": [0],
            "PLAYER2_ID": [0],
            "PLAYER3_ID": [0],
            "PLAYER1_TEAM_ID": [0],
            "PLAYER2_TEAM_ID": [0],
            "PLAYER3_TEAM_ID": [0],
        }
    ).to_parquet(path)


def _write_valid_period_starters_parquet(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    row = {column: [1] for column in STARTER_LOOKUP_COLUMNS}
    row["game_id"] = ["0029700001"]
    row["period"] = [1]
    row["resolved"] = [True]
    row["away_team_id"] = [1610612740]
    row["home_team_id"] = [1610612741]
    for index in range(1, 6):
        row[f"away_player{index}"] = [100 + index]
        row[f"home_player{index}"] = [200 + index]
    pd.DataFrame(row).to_parquet(path)


@pytest.fixture(autouse=True)
def _skip_boxscore_catalog_validation_for_text_runtime_fixtures(monkeypatch):
    monkeypatch.setattr(
        runner, "validate_boxscore_source_overrides", lambda _path: None
    )


def _write_runtime_sources(base_dir: Path) -> dict[str, Path]:
    live_dir = base_dir / "live"
    overrides_dir = live_dir / "overrides"
    _write_runtime_overrides_dir(overrides_dir)

    files = {
        "db_path": live_dir / "nba_raw.db",
        "parquet_path": live_dir / "playbyplayv2.parq",
        "overrides_path": live_dir / "validation_overrides.csv",
        "boxscore_source_overrides_path": live_dir / "boxscore_source_overrides.csv",
        "notebook_dump_path": live_dir / "build_tpdev_box_stats_v9b.py",
        "period_starters_v6": live_dir / "period_starters_v6.parquet",
        "period_starters_v5": live_dir / "period_starters_v5.parquet",
        "correction_manifest": overrides_dir / "correction_manifest.json",
        "lineup_window_overrides": overrides_dir / "lineup_window_overrides.json",
        "period_starters_overrides": overrides_dir / "period_starters_overrides.json",
    }
    for key, path in files.items():
        if key in {
            "correction_manifest",
            "lineup_window_overrides",
            "period_starters_overrides",
        }:
            continue
        if key == "overrides_path":
            path.write_text("game_id,action,tolerance,notes\n", encoding="utf-8")
            continue
        if key == "db_path":
            _write_valid_runtime_db(path)
            continue
        if key == "parquet_path":
            _write_valid_runtime_parquet(path)
            continue
        if key in {"period_starters_v6", "period_starters_v5"}:
            _write_valid_period_starters_parquet(path)
            continue
        path.write_text(path.name, encoding="utf-8")
    return files


def _fake_hydrate_runtime_input(source_path: Path, cache_dir: Path) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    target_path = cache_dir / Path(source_path).name
    if Path(source_path).suffix in {".csv", ".db", ".parq", ".parquet"}:
        target_path.write_bytes(Path(source_path).read_bytes())
    else:
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
    monkeypatch.setattr(
        runner, "set_boxscore_source_overrides", lambda _overrides: None
    )

    runtime_inputs = runner.prepare_local_runtime_inputs(
        cache_dir,
        db_path=files["db_path"],
        parquet_path=files["parquet_path"],
        overrides_path=files["overrides_path"],
        boxscore_source_overrides_path=files["boxscore_source_overrides_path"],
        period_starter_parquet_paths=[
            files["period_starters_v6"],
            files["period_starters_v5"],
        ],
        file_directory=files["db_path"].parent,
        catalog_overrides_dir=files["db_path"].parent / "overrides",
        runtime_input_cache_mode="fresh-copy",
    )

    assert runtime_inputs["db_path"] == cache_dir / "nba_raw.db"
    assert runtime_inputs["parquet_path"] == cache_dir / "playbyplayv2.parq"
    assert (
        runtime_inputs["notebook_dump_path"]
        == cache_dir / "build_tpdev_box_stats_v9b.py"
    )
    assert runtime_inputs["period_starter_parquet_paths"] == [
        cache_dir / "period_starters_v6.parquet",
        cache_dir / "period_starters_v5.parquet",
    ]
    assert (
        runtime_inputs["runtime_input_provenance"]["inputs"]["db_path"][
            "resolution_kind"
        ]
        == "copied_to_run_cache"
    )
    assert "pbp_v3_path" not in runtime_inputs["runtime_input_provenance"]["inputs"]
    assert runtime_inputs["file_directory"] != files["db_path"].parent
    assert (
        runtime_inputs["file_directory"] / "overrides" / "lineup_window_overrides.json"
    ).read_text(encoding="utf-8") == "{}\n"
    assert runtime_inputs["pbp_row_overrides_path"].parent == cache_dir / "catalogs"
    assert runtime_inputs["pbp_stat_overrides_path"].parent == cache_dir / "catalogs"
    assert runtime_inputs["overrides_path"].parent == cache_dir / "catalogs"


def test_prepare_local_runtime_inputs_fails_early_for_missing_required_inputs(
    tmp_path: Path,
) -> None:
    files = _write_runtime_sources(tmp_path)
    files["db_path"].unlink()

    with pytest.raises(FileNotFoundError, match="nba_raw.db"):
        runner.prepare_local_runtime_inputs(
            tmp_path / "run" / "_local_runtime_cache",
            db_path=files["db_path"],
            parquet_path=files["parquet_path"],
            overrides_path=files["overrides_path"],
            boxscore_source_overrides_path=files["boxscore_source_overrides_path"],
            period_starter_parquet_paths=[
                files["period_starters_v6"],
                files["period_starters_v5"],
            ],
            file_directory=files["db_path"].parent,
            catalog_overrides_dir=files["db_path"].parent / "overrides",
        )


def test_prepare_local_runtime_inputs_fails_for_missing_period_starter_parquet(
    tmp_path: Path,
) -> None:
    files = _write_runtime_sources(tmp_path)
    files["period_starters_v6"].unlink()

    with pytest.raises(FileNotFoundError, match="period_starters_v6.parquet"):
        runner.prepare_local_runtime_inputs(
            tmp_path / "run" / "_local_runtime_cache",
            db_path=files["db_path"],
            parquet_path=files["parquet_path"],
            overrides_path=files["overrides_path"],
            boxscore_source_overrides_path=files["boxscore_source_overrides_path"],
            period_starter_parquet_paths=[
                files["period_starters_v6"],
                files["period_starters_v5"],
            ],
            file_directory=files["db_path"].parent,
            catalog_overrides_dir=files["db_path"].parent / "overrides",
        )


def test_prepare_local_runtime_inputs_validates_row_catalog_before_runtime_dir_replace(
    tmp_path: Path,
    monkeypatch,
) -> None:
    files = _write_runtime_sources(tmp_path)
    conflict_catalog = tmp_path / "pbp_row_overrides.csv"
    conflict_catalog.write_text(
        "game_id,action,event_num,anchor_event_num,notes\n"
        "0021900261,drop,367,,drop stranded row\n"
        "0021900261,move_before,367,368,stale move\n",
        encoding="utf-8",
    )
    cache_dir = tmp_path / "run" / "_local_runtime_cache"
    runtime_dir = cache_dir.parent / "_local_runtime_file_directory"
    runtime_dir.mkdir(parents=True)
    sentinel = runtime_dir / "previous_snapshot.txt"
    sentinel.write_text("keep me\n", encoding="utf-8")

    monkeypatch.setattr(runner, "DEFAULT_PBP_ROW_OVERRIDES", conflict_catalog)
    monkeypatch.setattr(runner, "load_boxscore_source_overrides", lambda _path: {})
    monkeypatch.setattr(
        runner, "set_boxscore_source_overrides", lambda _overrides: None
    )

    with pytest.raises(ValueError, match="conflicting drop and move|moved after"):
        runner.prepare_local_runtime_inputs(
            cache_dir,
            db_path=files["db_path"],
            parquet_path=files["parquet_path"],
            overrides_path=files["overrides_path"],
            boxscore_source_overrides_path=files["boxscore_source_overrides_path"],
            period_starter_parquet_paths=[
                files["period_starters_v6"],
                files["period_starters_v5"],
            ],
            file_directory=files["db_path"].parent,
            catalog_overrides_dir=files["db_path"].parent / "overrides",
        )

    assert sentinel.read_text(encoding="utf-8") == "keep me\n"


def test_prepare_local_runtime_inputs_preserves_runtime_dir_on_late_catalog_failure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    files = _write_runtime_sources(tmp_path)
    cache_dir = tmp_path / "run" / "_local_runtime_cache"
    runtime_dir = cache_dir.parent / "_local_runtime_file_directory"
    runtime_dir.mkdir(parents=True)
    sentinel = runtime_dir / "previous_snapshot.txt"
    sentinel.write_text("keep me\n", encoding="utf-8")

    def reject_boxscore_catalog(_path):
        raise ValueError("bad boxscore source catalog")

    monkeypatch.setattr(runner, "validate_boxscore_source_overrides", reject_boxscore_catalog)

    with pytest.raises(ValueError, match="bad boxscore source catalog"):
        runner.prepare_local_runtime_inputs(
            cache_dir,
            db_path=files["db_path"],
            parquet_path=files["parquet_path"],
            overrides_path=files["overrides_path"],
            boxscore_source_overrides_path=files["boxscore_source_overrides_path"],
            period_starter_parquet_paths=[
                files["period_starters_v6"],
                files["period_starters_v5"],
            ],
            file_directory=files["db_path"].parent,
            catalog_overrides_dir=files["db_path"].parent / "overrides",
        )

    assert sentinel.read_text(encoding="utf-8") == "keep me\n"


def test_prepare_local_runtime_inputs_validates_validation_overrides_before_runtime_dir_replace(
    tmp_path: Path,
) -> None:
    files = _write_runtime_sources(tmp_path)
    files["overrides_path"].write_text(
        "game_id,action,tolerance,notes\n"
        "0029700001,skip,0,skip is not release safe\n",
        encoding="utf-8",
    )
    cache_dir = tmp_path / "run" / "_local_runtime_cache"
    runtime_dir = cache_dir.parent / "_local_runtime_file_directory"
    runtime_dir.mkdir(parents=True)
    sentinel = runtime_dir / "previous_snapshot.txt"
    sentinel.write_text("keep me\n", encoding="utf-8")

    with pytest.raises(ValueError, match="invalid action for release"):
        runner.prepare_local_runtime_inputs(
            cache_dir,
            db_path=files["db_path"],
            parquet_path=files["parquet_path"],
            overrides_path=files["overrides_path"],
            boxscore_source_overrides_path=files["boxscore_source_overrides_path"],
            period_starter_parquet_paths=[
                files["period_starters_v6"],
                files["period_starters_v5"],
            ],
            file_directory=files["db_path"].parent,
            catalog_overrides_dir=files["db_path"].parent / "overrides",
        )

    assert sentinel.read_text(encoding="utf-8") == "keep me\n"


def test_prepare_local_runtime_inputs_validates_core_data_before_runtime_dir_replace(
    tmp_path: Path,
) -> None:
    files = _write_runtime_sources(tmp_path)
    files["period_starters_v6"].write_text("not parquet\n", encoding="utf-8")
    cache_dir = tmp_path / "run" / "_local_runtime_cache"
    runtime_dir = cache_dir.parent / "_local_runtime_file_directory"
    runtime_dir.mkdir(parents=True)
    sentinel = runtime_dir / "previous_snapshot.txt"
    sentinel.write_text("keep me\n", encoding="utf-8")

    with pytest.raises(ValueError, match="period_starters_v6.parquet"):
        runner.prepare_local_runtime_inputs(
            cache_dir,
            db_path=files["db_path"],
            parquet_path=files["parquet_path"],
            overrides_path=files["overrides_path"],
            boxscore_source_overrides_path=files["boxscore_source_overrides_path"],
            period_starter_parquet_paths=[
                files["period_starters_v6"],
                files["period_starters_v5"],
            ],
            file_directory=files["db_path"].parent,
            catalog_overrides_dir=files["db_path"].parent / "overrides",
        )

    assert sentinel.read_text(encoding="utf-8") == "keep me\n"


def test_prepare_local_runtime_inputs_csv_fallback_writes_valid_boxscore_header(
    tmp_path: Path,
    monkeypatch,
) -> None:
    files = _write_runtime_sources(tmp_path)
    cache_dir = tmp_path / "run" / "_local_runtime_cache"

    def fake_hydrate(source_path: Path, local_cache_dir: Path) -> Path:
        if Path(source_path).name == "boxscore_source_overrides.csv":
            raise OSError("permission denied")
        return _fake_hydrate_runtime_input(source_path, local_cache_dir)

    monkeypatch.setattr(runner, "NOTEBOOK_DUMP", files["notebook_dump_path"])
    monkeypatch.setattr(runner, "NOTEBOOK_LOCAL_IMPORT_PRELOADS", [])
    monkeypatch.setattr(runner, "_hydrate_runtime_input", fake_hydrate)
    monkeypatch.setattr(runner, "load_boxscore_source_overrides", lambda _path: {})
    monkeypatch.setattr(
        runner, "set_boxscore_source_overrides", lambda _overrides: None
    )

    runtime_inputs = runner.prepare_local_runtime_inputs(
        cache_dir,
        db_path=files["db_path"],
        parquet_path=files["parquet_path"],
        overrides_path=files["overrides_path"],
        boxscore_source_overrides_path=files["boxscore_source_overrides_path"],
        period_starter_parquet_paths=[
            files["period_starters_v6"],
            files["period_starters_v5"],
        ],
        allow_unreadable_csv_fallback=True,
        file_directory=files["db_path"].parent,
        catalog_overrides_dir=files["db_path"].parent / "overrides",
    )

    fallback_path = runtime_inputs["boxscore_source_overrides_path"]
    assert fallback_path.exists()
    assert fallback_path.read_text(encoding="utf-8").startswith("game_id,GAME_ID")


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
        if name == "nba_raw.db":
            _write_valid_runtime_db(cached_path)
        elif name == "playbyplayv2.parq":
            _write_valid_runtime_parquet(cached_path)
        elif name in {"period_starters_v6.parquet", "period_starters_v5.parquet"}:
            _write_valid_period_starters_parquet(cached_path)
        else:
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
    monkeypatch.setattr(
        runner, "set_boxscore_source_overrides", lambda _overrides: None
    )

    runtime_inputs = runner.prepare_local_runtime_inputs(
        cache_dir,
        db_path=files["db_path"],
        parquet_path=files["parquet_path"],
        overrides_path=files["overrides_path"],
        boxscore_source_overrides_path=files["boxscore_source_overrides_path"],
        period_starter_parquet_paths=[
            files["period_starters_v6"],
            files["period_starters_v5"],
        ],
        file_directory=files["db_path"].parent,
        catalog_overrides_dir=files["db_path"].parent / "overrides",
        runtime_input_cache_mode="reuse-latest-global-cache-unsafe",
    )

    assert runtime_inputs["db_path"] == cached_paths["nba_raw.db"]
    assert runtime_inputs["parquet_path"] == cached_paths["playbyplayv2.parq"]
    assert (
        runtime_inputs["notebook_dump_path"]
        == cached_paths["build_tpdev_box_stats_v9b.py"]
    )
    assert runtime_inputs["period_starter_parquet_paths"] == [
        cached_paths["period_starters_v6.parquet"],
        cached_paths["period_starters_v5.parquet"],
    ]
    assert (
        runtime_inputs["runtime_input_provenance"]["inputs"]["db_path"][
            "resolution_kind"
        ]
        == "reused_global_cache"
    )
    assert (
        runtime_inputs["runtime_input_provenance"]["inputs"]["overrides_path"][
            "resolution_kind"
        ]
        == "copied_to_run_cache"
    )


def test_prepare_local_runtime_file_directory_snapshots_overrides(
    tmp_path: Path,
) -> None:
    live_dir = tmp_path / "live"
    overrides_dir = live_dir / "overrides"
    pbp_dir = live_dir / "pbp"
    _write_runtime_overrides_dir(overrides_dir)
    pbp_dir.mkdir(parents=True)
    (pbp_dir / "stats_foo.json").write_text("pbp", encoding="utf-8")

    info = runner.prepare_local_runtime_file_directory(
        tmp_path / "runtime_file_directory",
        live_file_directory=live_dir,
        catalog_overrides_dir=overrides_dir,
    )
    runtime_dir = Path(info["runtime_file_directory"]["path"])
    snapshot_path = runtime_dir / "overrides" / "lineup_window_overrides.json"

    assert snapshot_path.read_text(encoding="utf-8") == "{}\n"
    (overrides_dir / "lineup_window_overrides.json").write_text(
        '{"a": 2}\n', encoding="utf-8"
    )
    assert snapshot_path.read_text(encoding="utf-8") == "{}\n"
    assert (runtime_dir / "pbp").exists()
    assert (runtime_dir / "pbp" / "stats_foo.json").read_text(
        encoding="utf-8"
    ) == "pbp"
    (pbp_dir / "stats_foo.json").write_text("mutated", encoding="utf-8")
    assert (runtime_dir / "pbp" / "stats_foo.json").read_text(
        encoding="utf-8"
    ) == "pbp"
    assert all(
        row["link_mode"] != "symlink" for row in info["linked_paths"]
    )


def test_prepare_local_runtime_file_directory_defaults_to_committed_catalog_overrides(
    tmp_path: Path,
) -> None:
    live_dir = tmp_path / "live"
    live_dir.mkdir()

    info = runner.prepare_local_runtime_file_directory(
        tmp_path / "runtime_file_directory",
        live_file_directory=live_dir,
    )
    runtime_dir = Path(info["runtime_file_directory"]["path"])

    assert info["overrides_snapshot"]["source_kind"] == "catalogs"
    assert (runtime_dir / "overrides" / "correction_manifest.json").exists()
    assert (runtime_dir / "overrides" / "period_starters_overrides.json").exists()
    assert (runtime_dir / "overrides" / "lineup_window_overrides.json").exists()


def test_prepare_local_runtime_file_directory_can_use_file_directory_overrides(
    tmp_path: Path,
) -> None:
    live_dir = tmp_path / "live"
    overrides_dir = live_dir / "overrides"
    _write_runtime_overrides_dir(overrides_dir)

    info = runner.prepare_local_runtime_file_directory(
        tmp_path / "runtime_file_directory",
        live_file_directory=live_dir,
        catalog_overrides_dir=None,
    )
    runtime_dir = Path(info["runtime_file_directory"]["path"])

    assert info["overrides_snapshot"]["source_kind"] == "file_directory"
    assert (runtime_dir / "overrides" / "lineup_window_overrides.json").read_text(
        encoding="utf-8"
    ) == "{}\n"


def test_prepare_local_runtime_file_directory_fails_for_missing_catalog_overrides(
    tmp_path: Path,
) -> None:
    live_dir = tmp_path / "live"
    live_dir.mkdir()

    with pytest.raises(FileNotFoundError, match="Runtime overrides directory"):
        runner.prepare_local_runtime_file_directory(
            tmp_path / "runtime_file_directory",
            live_file_directory=live_dir,
            catalog_overrides_dir=tmp_path / "missing_catalog_overrides",
        )


def test_prepare_local_runtime_file_directory_validates_source_before_replacing_target(
    tmp_path: Path,
) -> None:
    live_dir = tmp_path / "live"
    live_dir.mkdir()
    runtime_dir = tmp_path / "runtime_file_directory"
    runtime_dir.mkdir()
    sentinel = runtime_dir / "previous_snapshot.txt"
    sentinel.write_text("keep me\n", encoding="utf-8")

    with pytest.raises(FileNotFoundError, match="Runtime overrides directory"):
        runner.prepare_local_runtime_file_directory(
            runtime_dir,
            live_file_directory=live_dir,
            catalog_overrides_dir=tmp_path / "missing_catalog_overrides",
        )

    assert sentinel.read_text(encoding="utf-8") == "keep me\n"


def test_prepare_local_runtime_file_directory_preserves_existing_target_on_copy_failure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    live_dir = tmp_path / "live"
    overrides_dir = live_dir / "overrides"
    pbp_dir = live_dir / "pbp"
    _write_runtime_overrides_dir(overrides_dir)
    pbp_dir.mkdir(parents=True)
    (pbp_dir / "stats_foo.json").write_text("pbp", encoding="utf-8")
    runtime_dir = tmp_path / "runtime_file_directory"
    runtime_dir.mkdir()
    sentinel = runtime_dir / "previous_snapshot.txt"
    sentinel.write_text("keep me\n", encoding="utf-8")

    def fail_copy(_source_path, _target_path):
        raise OSError("copy failed")

    monkeypatch.setattr(runner, "_link_or_copy_path", fail_copy)

    with pytest.raises(OSError, match="copy failed"):
        runner.prepare_local_runtime_file_directory(
            runtime_dir,
            live_file_directory=live_dir,
            catalog_overrides_dir=overrides_dir,
        )

    assert sentinel.read_text(encoding="utf-8") == "keep me\n"


def test_prepare_local_runtime_file_directory_fails_for_missing_file_directory_overrides(
    tmp_path: Path,
) -> None:
    live_dir = tmp_path / "live"
    live_dir.mkdir()

    with pytest.raises(FileNotFoundError, match="Runtime overrides directory"):
        runner.prepare_local_runtime_file_directory(
            tmp_path / "runtime_file_directory",
            live_file_directory=live_dir,
            catalog_overrides_dir=None,
        )


def test_parse_args_exposes_boxscore_and_override_directory_controls(
    tmp_path: Path,
) -> None:
    args = runner.parse_args(
        [
            "--seasons",
            "1997",
            "--output-dir",
            str(tmp_path / "out"),
            "--boxscore-source-overrides-path",
            str(tmp_path / "boxscore_source_overrides.csv"),
            "--catalog-overrides-dir",
            str(tmp_path / "catalog_overrides"),
        ]
    )

    assert args.boxscore_source_overrides_path == (
        tmp_path / "boxscore_source_overrides.csv"
    )
    assert args.catalog_overrides_dir == tmp_path / "catalog_overrides"
    assert args.use_file_directory_overrides is False


def test_parse_args_can_prefer_file_directory_overrides(tmp_path: Path) -> None:
    args = runner.parse_args(
        [
            "--seasons",
            "1997",
            "--output-dir",
            str(tmp_path / "out"),
            "--use-file-directory-overrides",
        ]
    )

    assert args.use_file_directory_overrides is True


def test_prepare_local_runtime_inputs_reuse_validated_cache_reuses_matching_files(
    tmp_path: Path,
    monkeypatch,
) -> None:
    files = _write_runtime_sources(tmp_path)
    cache_dir = tmp_path / "run" / "_local_runtime_cache"

    monkeypatch.setattr(runner, "NOTEBOOK_DUMP", files["notebook_dump_path"])
    monkeypatch.setattr(runner, "NOTEBOOK_LOCAL_IMPORT_PRELOADS", [])
    monkeypatch.setattr(runner, "load_boxscore_source_overrides", lambda _path: {})
    monkeypatch.setattr(
        runner, "set_boxscore_source_overrides", lambda _overrides: None
    )

    first_runtime_inputs = runner.prepare_local_runtime_inputs(
        cache_dir,
        db_path=files["db_path"],
        parquet_path=files["parquet_path"],
        overrides_path=files["overrides_path"],
        boxscore_source_overrides_path=files["boxscore_source_overrides_path"],
        period_starter_parquet_paths=[
            files["period_starters_v6"],
            files["period_starters_v5"],
        ],
        file_directory=files["db_path"].parent,
        catalog_overrides_dir=files["db_path"].parent / "overrides",
        runtime_input_cache_mode="reuse-validated-cache",
    )
    manifest_path = Path(
        first_runtime_inputs["runtime_input_provenance"][
            "validated_cache_manifest_path"
        ]
    )
    assert manifest_path.exists()

    second_runtime_inputs = runner.prepare_local_runtime_inputs(
        cache_dir,
        db_path=files["db_path"],
        parquet_path=files["parquet_path"],
        overrides_path=files["overrides_path"],
        boxscore_source_overrides_path=files["boxscore_source_overrides_path"],
        period_starter_parquet_paths=[
            files["period_starters_v6"],
            files["period_starters_v5"],
        ],
        file_directory=files["db_path"].parent,
        catalog_overrides_dir=files["db_path"].parent / "overrides",
        runtime_input_cache_mode="reuse-validated-cache",
    )

    db_cached_path = cache_dir / "nba_raw.db"
    assert second_runtime_inputs["db_path"] == db_cached_path
    assert db_cached_path.read_bytes() == files["db_path"].read_bytes()
    assert (
        second_runtime_inputs["runtime_input_provenance"]["inputs"]["db_path"][
            "resolution_kind"
        ]
        == "validated_run_cache_hit"
    )


def test_prepare_local_runtime_inputs_reuse_validated_cache_refreshes_mutated_cache(
    tmp_path: Path,
    monkeypatch,
) -> None:
    files = _write_runtime_sources(tmp_path)
    cache_dir = tmp_path / "run" / "_local_runtime_cache"

    monkeypatch.setattr(runner, "NOTEBOOK_DUMP", files["notebook_dump_path"])
    monkeypatch.setattr(runner, "NOTEBOOK_LOCAL_IMPORT_PRELOADS", [])
    monkeypatch.setattr(runner, "load_boxscore_source_overrides", lambda _path: {})
    monkeypatch.setattr(
        runner, "set_boxscore_source_overrides", lambda _overrides: None
    )

    runner.prepare_local_runtime_inputs(
        cache_dir,
        db_path=files["db_path"],
        parquet_path=files["parquet_path"],
        overrides_path=files["overrides_path"],
        boxscore_source_overrides_path=files["boxscore_source_overrides_path"],
        period_starter_parquet_paths=[
            files["period_starters_v6"],
            files["period_starters_v5"],
        ],
        file_directory=files["db_path"].parent,
        catalog_overrides_dir=files["db_path"].parent / "overrides",
        runtime_input_cache_mode="reuse-validated-cache",
    )

    db_cached_path = cache_dir / "nba_raw.db"
    db_cached_path.write_text("manually mutated cache\n", encoding="utf-8")

    refreshed_runtime_inputs = runner.prepare_local_runtime_inputs(
        cache_dir,
        db_path=files["db_path"],
        parquet_path=files["parquet_path"],
        overrides_path=files["overrides_path"],
        boxscore_source_overrides_path=files["boxscore_source_overrides_path"],
        period_starter_parquet_paths=[
            files["period_starters_v6"],
            files["period_starters_v5"],
        ],
        file_directory=files["db_path"].parent,
        catalog_overrides_dir=files["db_path"].parent / "overrides",
        runtime_input_cache_mode="reuse-validated-cache",
    )

    assert db_cached_path.read_bytes() == files["db_path"].read_bytes()
    assert (
        refreshed_runtime_inputs["runtime_input_provenance"]["inputs"]["db_path"][
            "resolution_kind"
        ]
        == "validated_run_cache_refresh"
    )


def test_prepare_local_runtime_inputs_reuse_validated_cache_hashes_same_size_mutations(
    tmp_path: Path,
    monkeypatch,
) -> None:
    files = _write_runtime_sources(tmp_path)
    cache_dir = tmp_path / "run" / "_local_runtime_cache"

    monkeypatch.setattr(runner, "NOTEBOOK_DUMP", files["notebook_dump_path"])
    monkeypatch.setattr(runner, "NOTEBOOK_LOCAL_IMPORT_PRELOADS", [])
    monkeypatch.setattr(runner, "load_boxscore_source_overrides", lambda _path: {})
    monkeypatch.setattr(
        runner, "set_boxscore_source_overrides", lambda _overrides: None
    )

    runner.prepare_local_runtime_inputs(
        cache_dir,
        db_path=files["db_path"],
        parquet_path=files["parquet_path"],
        overrides_path=files["overrides_path"],
        boxscore_source_overrides_path=files["boxscore_source_overrides_path"],
        period_starter_parquet_paths=[
            files["period_starters_v6"],
            files["period_starters_v5"],
        ],
        file_directory=files["db_path"].parent,
        catalog_overrides_dir=files["db_path"].parent / "overrides",
        runtime_input_cache_mode="reuse-validated-cache",
    )

    db_cached_path = cache_dir / "nba_raw.db"
    stat = db_cached_path.stat()
    original = bytearray(db_cached_path.read_bytes())
    original[0] = (original[0] + 1) % 256
    db_cached_path.write_bytes(bytes(original))
    os.utime(db_cached_path, ns=(stat.st_atime_ns, stat.st_mtime_ns))

    refreshed_runtime_inputs = runner.prepare_local_runtime_inputs(
        cache_dir,
        db_path=files["db_path"],
        parquet_path=files["parquet_path"],
        overrides_path=files["overrides_path"],
        boxscore_source_overrides_path=files["boxscore_source_overrides_path"],
        period_starter_parquet_paths=[
            files["period_starters_v6"],
            files["period_starters_v5"],
        ],
        file_directory=files["db_path"].parent,
        catalog_overrides_dir=files["db_path"].parent / "overrides",
        runtime_input_cache_mode="reuse-validated-cache",
    )

    assert db_cached_path.read_bytes() == files["db_path"].read_bytes()
    assert (
        refreshed_runtime_inputs["runtime_input_provenance"]["inputs"]["db_path"][
            "resolution_kind"
        ]
        == "validated_run_cache_refresh"
    )


def test_prepare_local_runtime_inputs_reuse_validated_cache_refreshes_stale_sources(
    tmp_path: Path,
    monkeypatch,
) -> None:
    files = _write_runtime_sources(tmp_path)
    cache_dir = tmp_path / "run" / "_local_runtime_cache"

    monkeypatch.setattr(runner, "NOTEBOOK_DUMP", files["notebook_dump_path"])
    monkeypatch.setattr(runner, "NOTEBOOK_LOCAL_IMPORT_PRELOADS", [])
    monkeypatch.setattr(runner, "load_boxscore_source_overrides", lambda _path: {})
    monkeypatch.setattr(
        runner, "set_boxscore_source_overrides", lambda _overrides: None
    )

    runner.prepare_local_runtime_inputs(
        cache_dir,
        db_path=files["db_path"],
        parquet_path=files["parquet_path"],
        overrides_path=files["overrides_path"],
        boxscore_source_overrides_path=files["boxscore_source_overrides_path"],
        period_starter_parquet_paths=[
            files["period_starters_v6"],
            files["period_starters_v5"],
        ],
        file_directory=files["db_path"].parent,
        catalog_overrides_dir=files["db_path"].parent / "overrides",
        runtime_input_cache_mode="reuse-validated-cache",
    )

    _write_valid_runtime_db(files["db_path"])
    conn = sqlite3.connect(files["db_path"])
    try:
        conn.execute(
            "INSERT INTO raw_responses (game_id, endpoint, team_id, data) VALUES (?, ?, NULL, ?)",
            (
                "0099999999",
                "boxscore",
                json.dumps(
                    {
                        "resultSets": [
                            {
                                "headers": BOXSCORE_SOURCE_COLUMNS,
                                "rowSet": [
                                    _boxscore_row(
                                        "0099999999",
                                        1610612740,
                                        999,
                                        "Extra Home",
                                        "G",
                                    ),
                                    _boxscore_row(
                                        "0099999999",
                                        1610612741,
                                        998,
                                        "Extra Away",
                                        "F",
                                    ),
                                ],
                            }
                        ]
                    }
                ).encode("utf-8"),
            ),
        )
        conn.commit()
    finally:
        conn.close()
    refreshed_runtime_inputs = runner.prepare_local_runtime_inputs(
        cache_dir,
        db_path=files["db_path"],
        parquet_path=files["parquet_path"],
        overrides_path=files["overrides_path"],
        boxscore_source_overrides_path=files["boxscore_source_overrides_path"],
        period_starter_parquet_paths=[
            files["period_starters_v6"],
            files["period_starters_v5"],
        ],
        file_directory=files["db_path"].parent,
        catalog_overrides_dir=files["db_path"].parent / "overrides",
        runtime_input_cache_mode="reuse-validated-cache",
    )

    assert (cache_dir / "nba_raw.db").read_bytes() == files["db_path"].read_bytes()
    assert (
        refreshed_runtime_inputs["runtime_input_provenance"]["inputs"]["db_path"][
            "resolution_kind"
        ]
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


def test_runtime_catalog_wrappers_use_snapshot_catalogs(tmp_path: Path) -> None:
    row_catalog = tmp_path / "pbp_row_overrides.csv"
    _write_pbp_row_catalog(
        row_catalog,
        "0029700001,drop,2,,drop from snapshot,,,,,,,,,,\n",
    )
    stat_catalog = tmp_path / "pbp_stat_overrides.csv"
    stat_catalog.write_text(
        "game_id,team_id,player_id,stat_key,stat_value,notes\n"
        "0029700001,1610612740,123,PTS,5,stat from snapshot\n",
        encoding="utf-8",
    )
    namespace: dict[str, object] = {}

    runner.install_runtime_catalog_wrappers(
        namespace,
        pbp_row_overrides_path=row_catalog,
        pbp_stat_overrides_path=stat_catalog,
    )

    row_result = namespace["apply_pbp_row_overrides"](
        pd.DataFrame(
            {
                "GAME_ID": ["0029700001", "0029700001"],
                "EVENTNUM": [1, 2],
                "EVENTMSGTYPE": [1, 4],
                "PERIOD": [1, 1],
                "PCTIMESTRING": ["12:00", "11:59"],
            }
        )
    )
    stat_result = namespace["apply_pbp_stat_overrides"]("0029700001", [])

    assert row_result["EVENTNUM"].tolist() == [1]
    assert stat_result == [
        {
            "player_id": 123,
            "team_id": 1610612740,
            "stat_key": "PTS",
            "stat_value": 5.0,
        }
    ]


def test_runtime_row_catalog_wrapper_accepts_explicit_empty_overrides(
    tmp_path: Path,
) -> None:
    row_catalog = tmp_path / "pbp_row_overrides.csv"
    _write_pbp_row_catalog(
        row_catalog,
        "0029700001,drop,2,,drop from snapshot,,,,,,,,,,\n",
    )
    stat_catalog = tmp_path / "pbp_stat_overrides.csv"
    stat_catalog.write_text(
        "game_id,team_id,player_id,stat_key,stat_value,notes\n",
        encoding="utf-8",
    )
    namespace: dict[str, object] = {}

    runner.install_runtime_catalog_wrappers(
        namespace,
        pbp_row_overrides_path=row_catalog,
        pbp_stat_overrides_path=stat_catalog,
    )

    game_df = pd.DataFrame(
        {
            "GAME_ID": ["0029700001", "0029700001"],
            "EVENTNUM": [1, 2],
            "EVENTMSGTYPE": [1, 4],
            "PERIOD": [1, 1],
            "PCTIMESTRING": ["12:00", "11:59"],
        }
    )

    row_result = namespace["apply_pbp_row_overrides"](game_df, overrides={})

    assert row_result["EVENTNUM"].tolist() == [1, 2]


def test_runtime_catalog_wrappers_reject_conflicting_row_catalog(tmp_path: Path) -> None:
    row_catalog = tmp_path / "pbp_row_overrides.csv"
    row_catalog.write_text(
        "game_id,action,event_num,anchor_event_num,notes\n"
        "0021900261,drop,367,,drop stranded row\n"
        "0021900261,move_before,367,368,stale move\n",
        encoding="utf-8",
    )
    stat_catalog = tmp_path / "pbp_stat_overrides.csv"
    stat_catalog.write_text(
        "game_id,team_id,player_id,stat_key,stat_value,notes\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="conflicting drop and move|moved after"):
        runner.install_runtime_catalog_wrappers(
            {},
            pbp_row_overrides_path=row_catalog,
            pbp_stat_overrides_path=stat_catalog,
        )


def test_installed_runtime_catalog_worker_accepts_explicit_stat_overrides(
    tmp_path: Path,
) -> None:
    namespace = runner.load_v9b_namespace()
    row_catalog = tmp_path / "pbp_row_overrides.csv"
    _write_pbp_row_catalog(row_catalog)
    stat_catalog = tmp_path / "pbp_stat_overrides.csv"
    stat_catalog.write_text(
        "game_id,team_id,player_id,stat_key,stat_value,notes\n"
        "0029700001,1610612740,123,PTS,5,snapshot\n",
        encoding="utf-8",
    )
    runner.install_runtime_catalog_wrappers(
        namespace,
        pbp_row_overrides_path=row_catalog,
        pbp_stat_overrides_path=stat_catalog,
    )
    db_path = tmp_path / "nba_raw.db"
    sqlite3.connect(db_path).close()
    observed: dict[str, list[dict]] = {}

    class _Possessions:
        player_stats = []
        manual_player_stats = []

    def fake_generate(
        game_id,
        game_df,
        fetch_boxscore,
        fetch_summary,
        fetch_pbp_v3,
        errors,
        rebound_deletions,
    ):
        observed["stats"] = namespace["apply_pbp_stat_overrides"](game_id, [])
        return pd.DataFrame({"GAME_ID": [game_id]}), _Possessions()

    namespace["_generate_darko_hybrid_with_fetchers"] = fake_generate

    result = namespace["_process_single_game_worker"](
        "0029700001",
        pd.DataFrame({"GAME_ID": ["0029700001"], "EVENTNUM": [1]}),
        str(db_path),
        validate=False,
        pbp_stat_overrides={
            "0029700001": [
                {
                    "team_id": 1610612740,
                    "player_id": 456,
                    "stat_key": "AST",
                    "stat_value": 1.0,
                    "notes": "explicit",
                }
            ]
        },
    )

    assert result[2] is None
    assert observed["stats"] == [
        {
            "player_id": 456,
            "team_id": 1610612740,
            "stat_key": "AST",
            "stat_value": 1.0,
        }
    ]


def test_direct_worker_normalizes_float_like_game_df_ids(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "nba_raw.db"
    sqlite3.connect(db_path).close()
    observed: dict[str, list[str]] = {}

    class _Possessions:
        player_stats = []
        manual_player_stats = []

    def fake_generate(
        game_id,
        game_df,
        fetch_boxscore,
        fetch_summary,
        fetch_pbp_v3,
        errors,
        rebound_deletions,
    ):
        observed["game_ids"] = game_df["GAME_ID"].tolist()
        return pd.DataFrame({"GAME_ID": [game_id]}), _Possessions()

    monkeypatch.setattr(v9b, "_generate_darko_hybrid_with_fetchers", fake_generate)

    result = v9b._process_single_game_worker(
        "0029700001",
        pd.DataFrame({"GAME_ID": ["29700001.0"], "EVENTNUM": [1]}),
        str(db_path),
        validate=False,
    )

    assert result[2] is None
    assert observed["game_ids"] == ["0029700001"]


def test_v9b_validation_overrides_reject_release_unsafe_skip(tmp_path: Path) -> None:
    overrides_path = tmp_path / "validation_overrides.csv"
    overrides_path.write_text(
        "game_id,action,tolerance,notes\n"
        "0029700001,skip,0,release unsafe\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="invalid action for release"):
        v9b.load_validation_overrides(str(overrides_path))


def test_v9b_assert_team_totals_explicit_empty_overrides_suppresses_global(
    monkeypatch,
) -> None:
    class _Possessions:
        items = []

    monkeypatch.setattr(
        v9b,
        "fetch_boxscore_stats",
        lambda _game_id: pd.DataFrame(
            {
                "PLAYER_ID": [123],
                "TEAM_ID": [1610612740],
                "PTS": [10],
            }
        ),
    )
    v9b.set_validation_overrides(
        {
            "0029700001": {
                "action": "allow",
                "tolerance": 999,
                "notes": "global should not apply",
            }
        }
    )

    try:
        with pytest.raises(AssertionError, match="VALIDATION FAILED"):
            v9b.assert_team_totals_match(
                "0029700001",
                pd.DataFrame(),
                _Possessions(),
                tolerance=0,
                overrides={},
            )
    finally:
        v9b.set_validation_overrides({})


def test_local_boxscore_wrapper_uses_runtime_boxscore_source_snapshot(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "nba_raw.db"
    payload = {
        "resultSets": [
            {
                "headers": ["GAME_ID", "TEAM_ID", "PLAYER_ID", "PTS"],
                "rowSet": [["0029700001", 1610612740, 123, 7]],
            }
        ]
    }
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE raw_responses (game_id TEXT, endpoint TEXT, team_id INTEGER, data BLOB)"
    )
    conn.execute(
        "INSERT INTO raw_responses (game_id, endpoint, team_id, data) VALUES (?, ?, NULL, ?)",
        ("0029700001", "boxscore", json.dumps(payload).encode("utf-8")),
    )
    conn.commit()
    conn.close()
    observed: dict[str, object] = {}

    def fake_get_possessions_from_df(*args, **kwargs):
        loader = kwargs["boxscore_source_loader"]
        observed["boxscore"] = loader.load_data("0029700001")
        return object()

    namespace = {"get_possessions_from_df": fake_get_possessions_from_df}
    runner.install_local_boxscore_wrapper(
        namespace,
        db_path,
        boxscore_source_overrides=pd.DataFrame(
            {
                "game_id": ["0029700001"],
                "GAME_ID": ["0029700001"],
                "TEAM_ID": [1610612740],
                "PLAYER_ID": [123],
                "PTS": [42],
            }
        ),
    )

    namespace["get_possessions_from_df"](
        pd.DataFrame({"GAME_ID": ["0029700001"], "EVENTNUM": [1]})
    )
    row = observed["boxscore"]["resultSets"][0]["rowSet"][0]

    assert dict(zip(["GAME_ID", "TEAM_ID", "PLAYER_ID", "PTS"], row)) == {
        "GAME_ID": "0029700001",
        "TEAM_ID": 1610612740,
        "PLAYER_ID": 123,
        "PTS": 42,
    }


def test_patched_parallel_passes_boxscore_source_overrides_to_worker():
    captured = []
    marker_overrides = pd.DataFrame({"game_id": ["0029700001"]})

    class FakeParallel:
        def __init__(self, *args, **kwargs):
            pass

        def __call__(self, jobs):
            return [job() for job in jobs]

    def fake_delayed(fn):
        def build_job(*args, **kwargs):
            return lambda: fn(*args, **kwargs)

        return build_job

    def fake_worker(
        game_id,
        game_df,
        db_path,
        validate,
        tolerance,
        overrides,
        strict_mode,
        run_boxscore_audit,
        boxscore_source_overrides=None,
        pbp_row_overrides=None,
        pbp_stat_overrides=None,
    ):
        captured.append(boxscore_source_overrides)
        return (
            game_id,
            pd.DataFrame({"player_rows": [1]}),
            None,
            [],
            [],
            {"team_rows": [], "player_rows": [], "audit_errors": []},
        )

    namespace = {
        "_process_single_game_worker": fake_worker,
        "DB_PATH": "nba_raw.db",
        "Parallel": FakeParallel,
        "delayed": fake_delayed,
        "pd": pd,
        "_event_stats_errors": [],
        "_rebound_fallback_lock": threading.Lock(),
        "_rebound_fallback_deletions": [],
        "TEAM_AUDIT_COLUMNS": [],
        "PLAYER_MISMATCH_COLUMNS": [],
        "AUDIT_ERROR_COLUMNS": [],
    }
    runner._patch_v9b_runtime_namespace(namespace)

    namespace["process_games_parallel"](
        ["0029700001"],
        pd.DataFrame({"GAME_ID": ["0029700001"], "EVENTNUM": [1]}),
        max_workers=1,
        boxscore_source_overrides=marker_overrides,
    )

    assert len(captured) == 1
    assert captured[0] is marker_overrides


def test_direct_process_season_forwards_runtime_catalog_snapshots(
    monkeypatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}
    season_df = pd.DataFrame({"GAME_ID": ["0029700001"], "EVENTNUM": [1]})
    boxscore_overrides = pd.DataFrame({"game_id": ["0029700001"]})
    row_overrides = {"0029700001": []}
    stat_overrides = {"0029700001": []}

    def fake_process_games_parallel(game_ids, season_pbp_df, **kwargs):
        captured["game_ids"] = game_ids
        captured["season_pbp_df"] = season_pbp_df
        captured.update(kwargs)
        return (
            pd.DataFrame({"Game_SingleGame": ["0029700001"]}),
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
        )

    monkeypatch.setattr(v9b, "load_validation_overrides", lambda _path: {})
    monkeypatch.setattr(v9b, "clear_rebound_fallback_deletions", lambda: None)
    monkeypatch.setattr(v9b, "load_pbp_from_parquet", lambda *_args, **_kwargs: season_df)
    monkeypatch.setattr(v9b, "process_games_parallel", fake_process_games_parallel)
    monkeypatch.setattr(v9b, "export_rebound_fallback_deletions", lambda _path: None)

    combined_df, error_df = v9b.process_season(
        1997,
        parquet_path=str(tmp_path / "playbyplayv2.parq"),
        output_dir=str(tmp_path),
        boxscore_source_overrides=boxscore_overrides,
        pbp_row_overrides=row_overrides,
        pbp_stat_overrides=stat_overrides,
    )

    assert not combined_df.empty
    assert error_df.empty
    assert captured["boxscore_source_overrides"] is boxscore_overrides
    assert captured["pbp_row_overrides"] is row_overrides
    assert captured["pbp_stat_overrides"] is stat_overrides


def test_patched_parallel_normalizes_float_like_game_ids():
    captured: dict[str, pd.DataFrame] = {}

    class FakeParallel:
        def __init__(self, *args, **kwargs):
            pass

        def __call__(self, jobs):
            return [job() for job in jobs]

    def fake_delayed(fn):
        def build_job(*args, **kwargs):
            return lambda: fn(*args, **kwargs)

        return build_job

    def fake_worker(
        game_id,
        game_df,
        db_path,
        validate,
        tolerance,
        overrides,
        strict_mode,
        run_boxscore_audit,
        boxscore_source_overrides=None,
        pbp_row_overrides=None,
        pbp_stat_overrides=None,
    ):
        captured["game_df"] = game_df
        return (
            game_id,
            pd.DataFrame({"player_rows": [1]}),
            None,
            [],
            [],
            {"team_rows": [], "player_rows": [], "audit_errors": []},
        )

    namespace = {
        "_process_single_game_worker": fake_worker,
        "DB_PATH": "nba_raw.db",
        "Parallel": FakeParallel,
        "delayed": fake_delayed,
        "pd": pd,
        "_event_stats_errors": [],
        "_rebound_fallback_lock": threading.Lock(),
        "_rebound_fallback_deletions": [],
        "TEAM_AUDIT_COLUMNS": [],
        "PLAYER_MISMATCH_COLUMNS": [],
        "AUDIT_ERROR_COLUMNS": [],
    }
    runner._patch_v9b_runtime_namespace(namespace)

    namespace["process_games_parallel"](
        ["0029700001"],
        pd.DataFrame({"GAME_ID": ["29700001.0"], "EVENTNUM": [1]}),
        max_workers=1,
    )

    assert captured["game_df"]["EVENTNUM"].tolist() == [1]


def test_load_pbp_from_parquet_filters_lowercase_season_and_normalizes_game_id(
    monkeypatch,
):
    namespace = runner.load_v9b_namespace()

    def fake_read_parquet(_path, filters=None):
        if filters is not None:
            raise RuntimeError("predicate pushdown unavailable")
        return pd.DataFrame(
            {
                "season": [1997, 1998],
                "game_id": [29700001.0, "0029800002"],
                "eventnum": [1, 2],
                "eventmsgtype": [12, 1],
                "eventmsgactiontype": [0, 0],
                "period": [1, 1],
            }
        )

    monkeypatch.setattr(namespace["pd"], "read_parquet", fake_read_parquet)

    result = namespace["load_pbp_from_parquet"]("fake.parq", season=1997)

    assert len(result) == 1
    assert result.iloc[0]["GAME_ID"] == "0029700001"


@pytest.mark.parametrize(
    "summary",
    [
        {"failed_games": 1, "event_stats_errors": 0, "seasons": []},
        {"failed_games": 0, "event_stats_errors": 2, "seasons": []},
        {
            "failed_games": 0,
            "event_stats_errors": 0,
            "seasons": [{"season": 1997, "player_rows": 0}],
        },
        {
            "failed_games": 0,
            "event_stats_errors": 0,
            "run_boxscore_audit": True,
            "seasons": [
                {
                    "season": 1997,
                    "player_rows": 1,
                    "boxscore_audit": {"audit_failures": 1},
                }
            ],
        },
        {
            "failed_games": 0,
            "event_stats_errors": 0,
            "run_boxscore_audit": True,
            "seasons": [{"season": 1997, "player_rows": 1, "boxscore_audit": None}],
        },
    ],
)
def test_runner_failure_reasons_fail_failed_or_empty_outputs(summary):
    assert runner._runner_failure_reasons(summary)


def test_runner_failure_reasons_allows_successful_nonempty_seasons():
    assert (
        runner._runner_failure_reasons(
            {
                "failed_games": 0,
                "event_stats_errors": 0,
                "seasons": [{"season": 1997, "player_rows": 1}],
            }
        )
        == []
    )


def test_worker_decodes_compressed_boxscore_blob(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "nba_raw.db"
    payload = {
        "resultSets": [
            {
                "headers": ["TEAM_ID", "PLAYER_ID", "PTS"],
                "rowSet": [[1610612740, 123, 7]],
            }
        ]
    }
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE raw_responses (game_id TEXT, endpoint TEXT, team_id INTEGER, data BLOB)"
    )
    conn.execute(
        "INSERT INTO raw_responses (game_id, endpoint, team_id, data) VALUES (?, ?, NULL, ?)",
        (
            "0020000001",
            "boxscore",
            zlib.compress(json.dumps(payload).encode("utf-8")),
        ),
    )
    conn.commit()
    conn.close()

    observed: dict[str, pd.DataFrame] = {}

    class _Possessions:
        manual_player_stats = {}
        player_stats = {}

    def fake_generate(
        game_id,
        game_df,
        fetch_boxscore,
        fetch_summary,
        fetch_pbp_v3,
        errors,
        rebound_deletions,
    ):
        observed["boxscore"] = fetch_boxscore(game_id)
        observed["row_override_result"] = v9b.apply_pbp_row_overrides(game_df)
        observed["stat_override_result"] = v9b.apply_pbp_stat_overrides(game_id, [])
        return pd.DataFrame({"GAME_ID": [game_id]}), _Possessions()

    monkeypatch.setattr(v9b, "_generate_darko_hybrid_with_fetchers", fake_generate)

    result = v9b._process_single_game_worker(
        "0020000001",
        pd.DataFrame(
            {
                "GAME_ID": ["0020000001", "0020000001"],
                "EVENTNUM": [1, 2],
                "EVENTMSGTYPE": [1, 4],
                "PERIOD": [1, 1],
                "PCTIMESTRING": ["12:00", "11:59"],
            }
        ),
        str(db_path),
        validate=False,
        boxscore_source_overrides=pd.DataFrame(
            {
                "game_id": ["0020000001"],
                "GAME_ID": ["0020000001"],
                "TEAM_ID": [1610612740],
                "PLAYER_ID": [123],
                "PTS": [42],
            }
        ),
        pbp_row_overrides={
            "0020000001": [
                {
                    "action": "drop",
                    "event_num": 2,
                    "anchor_event_num": None,
                    "notes": "drop via explicit worker catalog",
                }
            ]
        },
        pbp_stat_overrides={
            "0020000001": [
                {
                    "team_id": 1610612740,
                    "player_id": 123,
                    "stat_key": "PTS",
                    "stat_value": 5.0,
                    "notes": "stat via explicit worker catalog",
                }
            ]
        },
    )

    assert result[2] is None
    assert observed["boxscore"].to_dict("records") == [
        {"TEAM_ID": 1610612740, "PLAYER_ID": 123, "PTS": 42}
    ]
    assert observed["row_override_result"]["EVENTNUM"].tolist() == [1]
    assert observed["stat_override_result"] == [
        {
            "player_id": 123,
            "team_id": 1610612740,
            "stat_key": "PTS",
            "stat_value": 5.0,
        }
    ]


def test_v9b_process_games_parallel_rejects_threading_with_runtime_catalog_overrides() -> None:
    with pytest.raises(ValueError, match="process-based joblib backend"):
        v9b.process_games_parallel(
            ["0020000001"],
            pd.DataFrame(),
            backend="threading",
            validate=False,
            pbp_row_overrides={"0020000001": []},
        )


def test_patched_process_games_parallel_rejects_threading_with_runtime_catalog_overrides() -> None:
    namespace = runner.load_v9b_namespace()

    with pytest.raises(ValueError, match="process-based joblib backend"):
        namespace["process_games_parallel"](
            ["0020000001"],
            pd.DataFrame(),
            backend="threading",
            validate=False,
            pbp_stat_overrides={"0020000001": []},
        )


def test_game_context_installs_runtime_catalog_snapshots(monkeypatch, tmp_path: Path):
    game_context._GAME_CONTEXT_NAMESPACE_CACHE.clear()
    game_context._GAME_CONTEXT_SEASON_PBP_CACHE.clear()

    paths = {
        "parquet": tmp_path / "playbyplayv2.parq",
        "db": tmp_path / "nba_raw.db",
        "file_directory": tmp_path / "file_directory",
        "row": tmp_path / "pbp_row_overrides.csv",
        "stat": tmp_path / "pbp_stat_overrides.csv",
        "boxscore": tmp_path / "boxscore_source_overrides.csv",
        "period": tmp_path / "period_starters_v6.parquet",
    }
    for path in paths.values():
        if path.suffix:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("", encoding="utf-8")
        else:
            path.mkdir(parents=True, exist_ok=True)

    observed: dict[str, object] = {}

    def fake_load_v9b_namespace():
        return {
            "load_pbp_from_parquet": lambda *_args, **_kwargs: pd.DataFrame(
                {"GAME_ID": ["0029700001"]}
            ),
            "generate_darko_hybrid": lambda *_args, **_kwargs: (
                pd.DataFrame({"unused": [1]}),
                object(),
            ),
        }

    def fake_install_runtime_catalog_wrappers(
        namespace,
        *,
        pbp_row_overrides_path,
        pbp_stat_overrides_path,
    ):
        observed["row_path"] = Path(pbp_row_overrides_path)
        observed["stat_path"] = Path(pbp_stat_overrides_path)

    def fake_install_local_boxscore_wrapper(
        namespace,
        db_path,
        *,
        file_directory,
        allowed_seasons,
        period_starter_parquet_paths=None,
        boxscore_source_overrides=None,
        **_kwargs,
    ):
        observed["db_path"] = Path(db_path)
        observed["file_directory"] = Path(file_directory)
        observed["allowed_seasons"] = list(allowed_seasons)
        observed["period_starter_parquet_paths"] = tuple(period_starter_parquet_paths)
        observed["boxscore_source_overrides"] = boxscore_source_overrides

    monkeypatch.setattr(runner, "load_v9b_namespace", fake_load_v9b_namespace)
    monkeypatch.setattr(
        runner, "install_runtime_catalog_wrappers", fake_install_runtime_catalog_wrappers
    )
    monkeypatch.setattr(
        runner, "install_local_boxscore_wrapper", fake_install_local_boxscore_wrapper
    )
    monkeypatch.setattr(
        "historic_backfill.catalogs.boxscore_source_overrides.load_boxscore_source_overrides",
        lambda path: pd.DataFrame({"source_path": [str(path)]}),
    )
    monkeypatch.setattr(
        "historic_backfill.catalogs.boxscore_source_overrides.validate_boxscore_source_overrides",
        lambda _path: None,
    )
    monkeypatch.setattr(
        game_context,
        "_prepare_darko_df",
        lambda _df: pd.DataFrame({"player_id": [123], "player_name": ["Player"]}),
    )

    game_context._load_game_context(
        "0029700001",
        parquet_path=paths["parquet"],
        db_path=paths["db"],
        file_directory=paths["file_directory"],
        pbp_row_overrides_path=paths["row"],
        pbp_stat_overrides_path=paths["stat"],
        boxscore_source_overrides_path=paths["boxscore"],
        period_starter_parquet_paths=[paths["period"]],
    )

    assert observed["row_path"] == paths["row"].resolve()
    assert observed["stat_path"] == paths["stat"].resolve()
    assert observed["db_path"] == paths["db"].resolve()
    assert observed["file_directory"] == paths["file_directory"].resolve()
    assert observed["allowed_seasons"] == [1998]
    assert observed["period_starter_parquet_paths"] == (paths["period"].resolve(),)
    assert observed["boxscore_source_overrides"].to_dict("records") == [
        {"source_path": [str(paths["boxscore"].resolve())][0]}
    ]


def test_game_context_load_response_uses_explicit_boxscore_snapshot(
    monkeypatch,
    tmp_path: Path,
):
    game_context._GAME_CONTEXT_NAMESPACE_CACHE.clear()
    game_context._GAME_CONTEXT_SEASON_PBP_CACHE.clear()

    boxscore_path = tmp_path / "boxscore_source_overrides.csv"
    boxscore_path.write_text("game_id,GAME_ID\n", encoding="utf-8")
    observed: dict[str, object] = {}
    namespace: dict[str, object] = {}

    def fake_generate_darko_hybrid(game_id, _season_df):
        namespace["load_response"](game_id, "boxscore")
        return pd.DataFrame({"unused": [1]}), object()

    namespace.update(
        {
            "load_pbp_from_parquet": lambda *_args, **_kwargs: pd.DataFrame(
                {"GAME_ID": ["0029700001"]}
            ),
            "generate_darko_hybrid": fake_generate_darko_hybrid,
        }
    )

    monkeypatch.setattr(runner, "load_v9b_namespace", lambda: namespace)
    monkeypatch.setattr(
        runner,
        "install_local_boxscore_wrapper",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "historic_backfill.catalogs.boxscore_source_overrides.load_boxscore_source_overrides",
        lambda path: pd.DataFrame({"source_path": [str(path)]}),
    )
    monkeypatch.setattr(
        "historic_backfill.catalogs.boxscore_source_overrides.validate_boxscore_source_overrides",
        lambda _path: None,
    )
    monkeypatch.setattr(
        runner,
        "_load_raw_response",
        lambda _db_path, _game_id, _endpoint, *, boxscore_source_overrides=None: observed.setdefault(
            "boxscore_source_overrides",
            boxscore_source_overrides,
        ),
    )
    monkeypatch.setattr(
        game_context,
        "_prepare_darko_df",
        lambda _df: pd.DataFrame({"player_id": [123], "player_name": ["Player"]}),
    )

    game_context._load_game_context(
        "0029700001",
        parquet_path=tmp_path / "playbyplayv2.parq",
        db_path=tmp_path / "nba_raw.db",
        boxscore_source_overrides_path=boxscore_path,
    )

    assert observed["boxscore_source_overrides"].to_dict("records") == [
        {"source_path": str(boxscore_path.resolve())}
    ]


def test_game_context_rejects_partial_runtime_catalog_paths(tmp_path: Path):
    game_context._GAME_CONTEXT_NAMESPACE_CACHE.clear()
    with pytest.raises(ValueError, match="must be provided together"):
        game_context._load_game_context(
            "0029700001",
            parquet_path=tmp_path / "playbyplayv2.parq",
            db_path=tmp_path / "nba_raw.db",
            pbp_row_overrides_path=tmp_path / "pbp_row_overrides.csv",
        )


def test_game_context_rejects_missing_explicit_snapshot_paths(tmp_path: Path):
    existing_row = tmp_path / "pbp_row_overrides.csv"
    existing_stat = tmp_path / "pbp_stat_overrides.csv"
    existing_row.write_text("", encoding="utf-8")
    existing_stat.write_text("", encoding="utf-8")

    with pytest.raises(FileNotFoundError, match="Boxscore source overrides"):
        game_context._load_game_context(
            "0029700001",
            parquet_path=tmp_path / "playbyplayv2.parq",
            db_path=tmp_path / "nba_raw.db",
            pbp_row_overrides_path=existing_row,
            pbp_stat_overrides_path=existing_stat,
            boxscore_source_overrides_path=tmp_path / "missing_boxscore.csv",
        )

    with pytest.raises(FileNotFoundError, match="Period starter parquet"):
        game_context._load_game_context(
            "0029700001",
            parquet_path=tmp_path / "playbyplayv2.parq",
            db_path=tmp_path / "nba_raw.db",
            period_starter_parquet_paths=[tmp_path / "missing_period.parquet"],
        )


def test_run_lineup_audits_passes_runtime_catalog_paths(monkeypatch, tmp_path: Path):
    import historic_backfill.audits.core.event_player_on_court as event_audit
    import historic_backfill.audits.core.minutes_plus_minus as minutes_audit

    observed: dict[str, object] = {}

    def fake_build_minutes_plus_minus_audit(*_args, **kwargs):
        observed["minutes_kwargs"] = kwargs
        return pd.DataFrame(
            {
                "game_id": ["0029700001"],
                "has_minutes_mismatch": [True],
                "has_plus_minus_mismatch": [False],
            }
        )

    monkeypatch.setattr(
        minutes_audit,
        "build_minutes_plus_minus_audit",
        fake_build_minutes_plus_minus_audit,
    )
    monkeypatch.setattr(
        minutes_audit,
        "summarize_minutes_plus_minus_audit",
        lambda _df: {
            "minutes_mismatches": 1,
            "minutes_outliers": 0,
            "plus_minus_mismatches": 0,
        },
    )

    def fake_audit_event_player_on_court(**kwargs):
        observed.update(kwargs)
        return pd.DataFrame(), {"issue_games": 0}

    monkeypatch.setattr(
        event_audit, "audit_event_player_on_court", fake_audit_event_player_on_court
    )

    paths = {
        "db": tmp_path / "nba_raw.db",
        "parquet": tmp_path / "playbyplayv2.parq",
        "file_directory": tmp_path / "file_directory",
        "row": tmp_path / "pbp_row_overrides.csv",
        "stat": tmp_path / "pbp_stat_overrides.csv",
        "boxscore": tmp_path / "boxscore_source_overrides.csv",
        "period": tmp_path / "period_starters_v6.parquet",
    }
    paths["file_directory"].mkdir()

    runner.run_lineup_audits(
        combined_df=pd.DataFrame(),
        season=1997,
        output_dir=tmp_path,
        db_path=paths["db"],
        parquet_path=paths["parquet"],
        file_directory=paths["file_directory"],
        pbp_row_overrides_path=paths["row"],
        pbp_stat_overrides_path=paths["stat"],
        boxscore_source_overrides_path=paths["boxscore"],
        period_starter_parquet_paths=[paths["period"]],
    )

    assert observed["game_ids"] == ["0029700001"]
    assert observed["db_path"] == paths["db"]
    assert observed["parquet_path"] == paths["parquet"]
    assert observed["file_directory"] == paths["file_directory"]
    assert observed["pbp_row_overrides_path"] == paths["row"]
    assert observed["pbp_stat_overrides_path"] == paths["stat"]
    assert observed["boxscore_source_overrides_path"] == paths["boxscore"]
    assert observed["period_starter_parquet_paths"] == [paths["period"]]
    assert observed["minutes_kwargs"]["boxscore_source_overrides_path"] == paths["boxscore"]


def test_direct_v9b_process_games_parallel_merges_mixed_game_id_styles(monkeypatch):
    observed: dict[str, pd.DataFrame] = {}

    def fake_worker(
        game_id,
        game_df,
        *_args,
        **_kwargs,
    ):
        observed["game_id"] = game_id
        observed["game_df"] = game_df
        return (game_id, pd.DataFrame({"GAME_ID": [game_id]}), None, [], [], {})

    monkeypatch.setattr(v9b, "_process_single_game_worker", fake_worker)
    monkeypatch.setattr(
        v9b,
        "Parallel",
        lambda **_kwargs: lambda calls: [call() for call in calls],
    )
    monkeypatch.setattr(v9b, "delayed", lambda fn: lambda *args, **kwargs: lambda: fn(*args, **kwargs))

    combined_df, error_df, *_rest = v9b.process_games_parallel(
        ["0029700001"],
        pd.DataFrame(
            {
                "GAME_ID": ["0029700001", "29700001.0"],
                "EVENTNUM": [1, 2],
            }
        ),
        validate=False,
    )

    assert error_df.empty
    assert combined_df["GAME_ID"].tolist() == ["0029700001"]
    assert observed["game_id"] == "0029700001"
    assert observed["game_df"]["EVENTNUM"].tolist() == [1, 2]
