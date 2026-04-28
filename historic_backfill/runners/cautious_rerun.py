from __future__ import annotations

import argparse
import hashlib
import importlib.util
import importlib.machinery
import json
import os
import resource
import shutil
import sqlite3
import sys
import time
import traceback
import types
import zlib
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

from historic_backfill.catalogs.boxscore_source_overrides import (
    apply_boxscore_response_overrides,
    load_boxscore_source_overrides,
    set_boxscore_source_overrides,
)

ROOT = Path(__file__).resolve().parents[1]
RUNNERS_ROOT = ROOT / "runners"
CATALOGS_ROOT = ROOT / "catalogs"
DATA_ROOT = ROOT / "data"
NOTEBOOK_DUMP = RUNNERS_ROOT / "build_tpdev_box_stats_v9b.py"
DEFAULT_DB = DATA_ROOT / "nba_raw.db"
DEFAULT_PARQUET = DATA_ROOT / "playbyplayv2.parq"
DEFAULT_OVERRIDES = CATALOGS_ROOT / "validation_overrides.csv"
DEFAULT_BOXSCORE_SOURCE_OVERRIDES = CATALOGS_ROOT / "boxscore_source_overrides.csv"
DEFAULT_FILE_DIRECTORY = DATA_ROOT
DEFAULT_RUNTIME_CATALOG_OVERRIDES_DIR = CATALOGS_ROOT / "overrides"
DEFAULT_RUNTIME_INPUT_CACHE_MODE = "fresh-copy"
DEFAULT_AUDIT_PROFILE = "full"
RUNTIME_INPUT_CACHE_MODES = {
    "fresh-copy",
    "reuse-latest-global-cache",
    "reuse-validated-cache",
}
AUDIT_PROFILES = {"full", "counting_only"}
VALIDATED_CACHE_MANIFEST_NAME = "validated_runtime_input_manifest.json"
SMALL_RUNTIME_INPUT_HASH_LIMIT_BYTES = 1024 * 1024
# Runtime starter precedence is now gamerotation-backed v6, then v5 as fallback.
DEFAULT_PERIOD_STARTERS_PARQUETS = [
    DATA_ROOT / "period_starters_v6.parquet",
    DATA_ROOT / "period_starters_v5.parquet",
]
RUNTIME_FILE_DIRECTORY_LINK_NAMES = [
    "game_details",
    "pbp",
    "pbp_v3",
    "raw_responses",
    "schedule",
]

NOTEBOOK_LOCAL_IMPORT_PRELOADS: list[str] = []


class _BoxscoreSourceLoader:
    def __init__(self, data: Dict[str, Any]):
        self._data = data

    def load_data(self, game_id: str | None = None) -> Dict[str, Any]:
        return self._data


def _current_peak_rss_mb() -> float | None:
    try:
        raw_value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except Exception:
        return None
    bytes_value = raw_value if sys.platform == "darwin" else raw_value * 1024
    return round(bytes_value / (1024 * 1024), 3)


def _ensure_local_pbpstats_importable() -> None:
    if importlib.util.find_spec("pbpstats") is not None:
        return

    candidates = []
    env_path = os.environ.get("PBPSTATS_REPO")
    if env_path:
        candidates.append(Path(env_path).expanduser())
    candidates.append(Path.home() / "Documents" / "GitHub" / "pbpstats")

    for candidate in candidates:
        if (candidate / "pbpstats").exists():
            sys.path.insert(0, str(candidate))
            if importlib.util.find_spec("pbpstats") is not None:
                return

    raise ModuleNotFoundError(
        "Could not import pbpstats. Set PBPSTATS_REPO or make the editable repo available."
    )


def _preload_local_module(module_name: str, module_path: Path) -> None:
    if module_name in sys.modules:
        return

    if module_path.suffix == ".pyc":
        loader = importlib.machinery.SourcelessFileLoader(module_name, str(module_path))
        spec = importlib.util.spec_from_loader(module_name, loader)
        if spec is None:
            raise ImportError(
                f"Could not build spec for sourceless module {module_name} at {module_path}"
            )
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        try:
            loader.exec_module(module)
        except Exception:
            sys.modules.pop(module_name, None)
            raise
        return

    # Reading the module source directly avoids an importlib path that can hang
    # under cloud-backed workspaces while the notebook dump is being exec'd.
    module = types.ModuleType(module_name)
    module.__file__ = str(module_path)
    module.__package__ = ""
    sys.modules[module_name] = module
    try:
        source = module_path.read_text(encoding="utf-8")
        exec(compile(source, str(module_path), "exec"), module.__dict__)
    except Exception:
        sys.modules.pop(module_name, None)
        raise


def _load_raw_response(
    db_path: Path, game_id: str, endpoint: str
) -> Dict[str, Any] | None:
    game_id = str(game_id).zfill(10)
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        row = conn.execute(
            "SELECT data FROM raw_responses WHERE game_id=? AND endpoint=? AND team_id IS NULL",
            (game_id, endpoint),
        ).fetchone()
        if not row:
            return None
        blob = row[0]
        try:
            data = json.loads(zlib.decompress(blob).decode())
        except (zlib.error, TypeError):
            if isinstance(blob, bytes):
                data = json.loads(blob.decode())
            else:
                data = json.loads(blob)
        if endpoint == "boxscore":
            return apply_boxscore_response_overrides(game_id, data)
        return data
    finally:
        conn.close()


def _hydrate_runtime_input(source_path: Path, cache_dir: Path) -> Path:
    source_path = source_path.resolve()
    cache_dir.mkdir(parents=True, exist_ok=True)
    cached_path = cache_dir / source_path.name
    if source_path.exists():
        with source_path.open("rb") as src, cached_path.open("wb") as dst:
            while True:
                chunk = src.read(1024 * 1024)
                if not chunk:
                    break
                dst.write(chunk)
        return cached_path
    return source_path


def _validated_cache_manifest_path(cache_dir: Path) -> Path:
    return cache_dir / VALIDATED_CACHE_MANIFEST_NAME


def _load_validated_cache_manifest(cache_dir: Path) -> dict[str, Any]:
    manifest_path = _validated_cache_manifest_path(cache_dir)
    if not manifest_path.exists():
        return {"entries": {}}
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return {"entries": {}}
    entries = payload.get("entries")
    if not isinstance(entries, dict):
        return {"entries": {}}
    return {"entries": entries}


def _write_validated_cache_manifest(cache_dir: Path, manifest: dict[str, Any]) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = _validated_cache_manifest_path(cache_dir)
    payload = {
        "entries": manifest.get("entries", {}),
    }
    manifest_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return manifest_path


def _source_fingerprint(path: Path) -> dict[str, Any]:
    resolved = path.resolve()
    if not resolved.exists():
        return {
            "source_path": str(resolved),
            "exists": False,
        }
    stat = resolved.stat()
    return {
        "source_path": str(resolved),
        "exists": True,
        "size_bytes": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
    }


def _cached_fingerprint(path: Path) -> dict[str, Any]:
    resolved = path.resolve()
    if not resolved.exists():
        return {"exists": False}
    stat = resolved.stat()
    fingerprint: dict[str, Any] = {
        "exists": True,
        "size_bytes": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
    }
    if stat.st_size <= SMALL_RUNTIME_INPUT_HASH_LIMIT_BYTES:
        fingerprint["sha256"] = hashlib.sha256(resolved.read_bytes()).hexdigest()
    return fingerprint


def _validated_cache_entry_key(source_path: Path) -> str:
    return str(source_path.resolve())


def _validated_cache_entry_matches(
    *,
    manifest: dict[str, Any],
    source_path: Path,
    cached_path: Path,
) -> bool:
    entry = manifest.get("entries", {}).get(_validated_cache_entry_key(source_path))
    if not isinstance(entry, dict):
        return False
    if str(cached_path) != str(entry.get("cached_path") or ""):
        return False
    if not cached_path.exists():
        return False
    return entry.get("source") == _source_fingerprint(source_path) and entry.get(
        "cached"
    ) == _cached_fingerprint(cached_path)


def _record_validated_cache_entry(
    *,
    manifest: dict[str, Any],
    source_path: Path,
    cached_path: Path,
) -> None:
    manifest.setdefault("entries", {})[_validated_cache_entry_key(source_path)] = {
        "source": _source_fingerprint(source_path),
        "cached": _cached_fingerprint(cached_path),
        "cached_path": str(cached_path),
    }


def _latest_cached_runtime_copy(filename: str) -> Path | None:
    cached_matches = sorted(
        ROOT.glob(f"**/_local_runtime_cache/{filename}"),
        key=lambda path: path.stat().st_mtime if path.exists() else 0,
        reverse=True,
    )
    return cached_matches[0] if cached_matches else None


def _path_metadata(path: Path) -> Dict[str, Any]:
    info: Dict[str, Any] = {
        "path": str(path),
        "exists": path.exists(),
    }
    if not path.exists():
        return info
    stat = path.stat()
    info.update(
        {
            "resolved_path": str(path.resolve()),
            "is_dir": path.is_dir(),
            "is_symlink": path.is_symlink(),
            "size_bytes": int(stat.st_size),
            "mtime": float(stat.st_mtime),
        }
    )
    return info


def _build_runtime_input_record(
    *,
    source_path: Path,
    resolved_path: Path,
    resolution_kind: str,
    error_message: str | None = None,
) -> Dict[str, Any]:
    record: Dict[str, Any] = {
        "resolution_kind": resolution_kind,
        "source": _path_metadata(source_path),
        "resolved": _path_metadata(resolved_path),
    }
    if error_message is not None:
        record["error_message"] = error_message
    return record


def _remove_existing_path(path: Path) -> None:
    if path.is_symlink() or path.is_file():
        path.unlink(missing_ok=True)
    elif path.exists():
        shutil.rmtree(path)


def _link_or_copy_path(source_path: Path, target_path: Path) -> str:
    try:
        target_path.symlink_to(source_path, target_is_directory=source_path.is_dir())
        return "symlink"
    except OSError:
        if source_path.is_dir():
            shutil.copytree(source_path, target_path)
            return "copytree"
        shutil.copy2(source_path, target_path)
        return "copy2"


def prepare_local_runtime_file_directory(
    runtime_file_directory: Path,
    *,
    live_file_directory: Path = DEFAULT_FILE_DIRECTORY,
    catalog_overrides_dir: Path | None = DEFAULT_RUNTIME_CATALOG_OVERRIDES_DIR,
) -> Dict[str, Any]:
    live_file_directory = live_file_directory.resolve()
    runtime_file_directory = runtime_file_directory.resolve()
    catalog_overrides_dir = (
        Path(catalog_overrides_dir).resolve()
        if catalog_overrides_dir is not None
        else None
    )
    _remove_existing_path(runtime_file_directory)
    runtime_file_directory.mkdir(parents=True, exist_ok=True)

    linked_paths = []
    for name in RUNTIME_FILE_DIRECTORY_LINK_NAMES:
        source_path = live_file_directory / name
        if not source_path.exists():
            continue
        target_path = runtime_file_directory / name
        link_mode = _link_or_copy_path(source_path, target_path)
        linked_paths.append(
            {
                "name": name,
                "link_mode": link_mode,
                "source": _path_metadata(source_path),
                "target": _path_metadata(target_path),
            }
        )

    live_overrides_source = live_file_directory / "overrides"
    overrides_source_kind = "empty"
    if catalog_overrides_dir is not None and catalog_overrides_dir.exists():
        overrides_source = catalog_overrides_dir
        overrides_source_kind = "catalogs"
    elif live_overrides_source.exists():
        overrides_source = live_overrides_source
        overrides_source_kind = "file_directory"
    else:
        overrides_source = live_overrides_source
    overrides_target = runtime_file_directory / "overrides"
    if overrides_source.exists():
        shutil.copytree(overrides_source, overrides_target)
    else:
        overrides_target.mkdir(parents=True, exist_ok=True)

    return {
        "live_file_directory": _path_metadata(live_file_directory),
        "runtime_file_directory": _path_metadata(runtime_file_directory),
        "linked_paths": linked_paths,
        "overrides_snapshot": {
            "source_kind": overrides_source_kind,
            "source": _path_metadata(overrides_source),
            "target": _path_metadata(overrides_target),
            "files": [
                _path_metadata(path)
                for path in sorted(overrides_target.rglob("*"))
                if path.is_file()
            ],
        },
    }


def prepare_local_runtime_inputs(
    cache_dir: Path,
    db_path: Path = DEFAULT_DB,
    parquet_path: Path = DEFAULT_PARQUET,
    overrides_path: Path = DEFAULT_OVERRIDES,
    boxscore_source_overrides_path: Path = DEFAULT_BOXSCORE_SOURCE_OVERRIDES,
    period_starter_parquet_paths: Iterable[Path] = DEFAULT_PERIOD_STARTERS_PARQUETS,
    allow_unreadable_csv_fallback: bool = False,
    file_directory: Path = DEFAULT_FILE_DIRECTORY,
    catalog_overrides_dir: Path | None = DEFAULT_RUNTIME_CATALOG_OVERRIDES_DIR,
    runtime_input_cache_mode: str = DEFAULT_RUNTIME_INPUT_CACHE_MODE,
) -> Dict[str, Any]:
    if runtime_input_cache_mode not in RUNTIME_INPUT_CACHE_MODES:
        raise ValueError(
            f"Unsupported runtime_input_cache_mode={runtime_input_cache_mode!r}; expected one of "
            f"{sorted(RUNTIME_INPUT_CACHE_MODES)}"
        )

    reuse_cached_names = {
        "build_tpdev_box_stats_v9b.py",
        "nba_raw.db",
        "playbyplayv2.parq",
        "boxscore_source_overrides.csv",
        "period_starters_v6.parquet",
        "period_starters_v5.parquet",
    }
    runtime_input_provenance: Dict[str, Any] = {
        "runtime_input_cache_mode": runtime_input_cache_mode,
        "inputs": {},
        "preload_modules": {},
        "period_starter_parquet_inputs": [],
    }
    validated_cache_manifest = (
        _load_validated_cache_manifest(cache_dir)
        if runtime_input_cache_mode == "reuse-validated-cache"
        else {"entries": {}}
    )

    def _hydrate_or_fallback(
        source_path: Path,
        *,
        allow_empty_fallback: bool,
        required: bool = True,
    ) -> tuple[Path, Dict[str, Any]]:
        source_path = Path(source_path).resolve()
        cached_path = cache_dir / source_path.name
        if required and not source_path.exists():
            raise FileNotFoundError(f"Required runtime input not found: {source_path}")
        if (
            runtime_input_cache_mode == "reuse-latest-global-cache"
            and source_path.name in reuse_cached_names
        ):
            cached_copy = _latest_cached_runtime_copy(source_path.name)
            if cached_copy is not None:
                return cached_copy, _build_runtime_input_record(
                    source_path=source_path,
                    resolved_path=cached_copy,
                    resolution_kind="reused_global_cache",
                )
        if runtime_input_cache_mode == "reuse-validated-cache":
            if _validated_cache_entry_matches(
                manifest=validated_cache_manifest,
                source_path=source_path,
                cached_path=cached_path,
            ):
                return cached_path, _build_runtime_input_record(
                    source_path=source_path,
                    resolved_path=cached_path,
                    resolution_kind="validated_run_cache_hit",
                )
        try:
            had_cached_copy = cached_path.exists()
            hydrated_path = _hydrate_runtime_input(source_path, cache_dir)
            if (
                runtime_input_cache_mode == "reuse-validated-cache"
                and hydrated_path != source_path
            ):
                _record_validated_cache_entry(
                    manifest=validated_cache_manifest,
                    source_path=source_path,
                    cached_path=hydrated_path,
                )
                resolution_kind = (
                    "validated_run_cache_refresh"
                    if had_cached_copy
                    else "copied_to_run_cache"
                )
            else:
                resolution_kind = (
                    "copied_to_run_cache"
                    if hydrated_path != source_path
                    else "source_direct"
                )
            return hydrated_path, _build_runtime_input_record(
                source_path=source_path,
                resolved_path=hydrated_path,
                resolution_kind=resolution_kind,
            )
        except OSError as exc:
            if not allow_empty_fallback or not allow_unreadable_csv_fallback:
                raise
            fallback_path = cache_dir / source_path.name
            fallback_path.unlink(missing_ok=True)
            print(
                f"[RUNNER] WARNING: using empty fallback for unreadable runtime input {source_path}: {exc}"
            )
            return fallback_path, _build_runtime_input_record(
                source_path=source_path,
                resolved_path=fallback_path,
                resolution_kind="empty_fallback_for_unreadable_csv",
                error_message=str(exc),
            )

    hydrated_db_path, runtime_input_provenance["inputs"]["db_path"] = (
        _hydrate_or_fallback(
            db_path,
            allow_empty_fallback=False,
        )
    )
    hydrated_parquet_path, runtime_input_provenance["inputs"]["parquet_path"] = (
        _hydrate_or_fallback(
            parquet_path,
            allow_empty_fallback=False,
        )
    )
    (
        hydrated_notebook_dump_path,
        runtime_input_provenance["inputs"]["notebook_dump_path"],
    ) = _hydrate_or_fallback(
        NOTEBOOK_DUMP,
        allow_empty_fallback=False,
    )
    hydrated_overrides_path, runtime_input_provenance["inputs"]["overrides_path"] = (
        _hydrate_or_fallback(
            overrides_path,
            allow_empty_fallback=True,
        )
    )
    (
        hydrated_boxscore_source_path,
        runtime_input_provenance["inputs"]["boxscore_source_overrides_path"],
    ) = _hydrate_or_fallback(
        boxscore_source_overrides_path,
        allow_empty_fallback=True,
    )
    hydrated_preload_module_paths: Dict[str, Path] = {}
    for module_name in NOTEBOOK_LOCAL_IMPORT_PRELOADS:
        pyc_path = (
            ROOT
            / "__pycache__"
            / f"{module_name}.cpython-{sys.version_info.major}{sys.version_info.minor}.pyc"
        )
        if module_name == "pbp_row_overrides" and pyc_path.exists():
            hydrated_preload_module_paths[module_name] = pyc_path
            runtime_input_provenance["preload_modules"][module_name] = (
                _build_runtime_input_record(
                    source_path=ROOT / f"{module_name}.py",
                    resolved_path=pyc_path,
                    resolution_kind="local_pyc_direct",
                )
            )
            continue
        hydrated_path, record = _hydrate_or_fallback(
            RUNNERS_ROOT / f"{module_name}.py",
            allow_empty_fallback=False,
        )
        hydrated_preload_module_paths[module_name] = hydrated_path
        runtime_input_provenance["preload_modules"][module_name] = record

    hydrated_period_starter_paths: list[Path] = []
    for path in period_starter_parquet_paths:
        hydrated_path, record = _hydrate_or_fallback(
            Path(path),
            allow_empty_fallback=False,
            required=True,
        )
        hydrated_period_starter_paths.append(hydrated_path)
        runtime_input_provenance["period_starter_parquet_inputs"].append(record)

    file_directory_provenance = prepare_local_runtime_file_directory(
        cache_dir.parent / "_local_runtime_file_directory",
        live_file_directory=file_directory,
        catalog_overrides_dir=catalog_overrides_dir,
    )
    runtime_input_provenance["file_directory"] = file_directory_provenance
    if runtime_input_cache_mode == "reuse-validated-cache":
        runtime_input_provenance["validated_cache_manifest_path"] = str(
            _write_validated_cache_manifest(
                cache_dir, validated_cache_manifest
            ).resolve()
        )
    set_boxscore_source_overrides(
        load_boxscore_source_overrides(hydrated_boxscore_source_path)
    )
    return {
        "db_path": hydrated_db_path,
        "parquet_path": hydrated_parquet_path,
        "notebook_dump_path": hydrated_notebook_dump_path,
        "preload_module_paths": hydrated_preload_module_paths,
        "overrides_path": hydrated_overrides_path,
        "boxscore_source_overrides_path": hydrated_boxscore_source_path,
        "period_starter_parquet_paths": hydrated_period_starter_paths,
        "file_directory": Path(
            file_directory_provenance["runtime_file_directory"]["path"]
        ),
        "runtime_input_provenance": runtime_input_provenance,
    }


def _patch_v9b_runtime_namespace(namespace: Dict[str, Any]) -> None:
    original_process_single_game_worker = namespace["_process_single_game_worker"]

    def process_games_parallel_patched(
        game_ids: list[str],
        season_pbp_df: Any,
        max_workers: int = -1,
        validate: bool = True,
        tolerance: int = 2,
        backend: str = "loky",
        overrides: Dict[str, Dict] | None = None,
        strict_mode: bool | None = None,
        run_boxscore_audit: bool = False,
    ) -> Tuple[Any, Any, Any, Any, Any]:
        db_path_str = str(namespace["DB_PATH"])
        normalized_game_ids = [str(gid).zfill(10) for gid in game_ids]
        print(
            f"[PREP] Building lazy row slices for {len(normalized_game_ids)} games..."
        )

        row_positions_by_game: dict[str, list[int]] = {}
        for row_index, raw_game_id in enumerate(season_pbp_df["GAME_ID"].tolist()):
            normalized = str(raw_game_id).zfill(10)
            row_positions_by_game.setdefault(normalized, []).append(row_index)

        missing_game_ids = sorted(
            gid for gid in normalized_game_ids if gid not in row_positions_by_game
        )
        if missing_game_ids:
            raise ValueError(
                f"Requested games missing from season dataframe: {missing_game_ids}"
            )

        print(f"[RUN] Processing with {max_workers} workers (backend={backend})...")
        results_list = namespace["Parallel"](
            n_jobs=max_workers, backend=backend, verbose=10
        )(
            namespace["delayed"](original_process_single_game_worker)(
                gid,
                season_pbp_df.iloc[row_positions_by_game[gid]].copy(),
                db_path_str,
                validate,
                tolerance,
                overrides,
                strict_mode,
                run_boxscore_audit,
            )
            for gid in normalized_game_ids
        )

        results: list[Any] = []
        errors: list[dict[str, Any]] = []
        all_event_errors: list[dict[str, Any]] = []
        all_rebound_deletions: list[dict[str, Any]] = []
        all_team_audit_rows: list[dict[str, Any]] = []
        all_player_mismatch_rows: list[dict[str, Any]] = []
        all_audit_error_rows: list[dict[str, Any]] = []

        for (
            game_id,
            df,
            error,
            event_errors,
            rebound_deletions,
            audit_payload,
        ) in results_list:
            all_event_errors.extend(event_errors)
            all_rebound_deletions.extend(rebound_deletions)
            if run_boxscore_audit:
                all_team_audit_rows.extend(audit_payload.get("team_rows", []))
                all_player_mismatch_rows.extend(audit_payload.get("player_rows", []))
                all_audit_error_rows.extend(audit_payload.get("audit_errors", []))

            if error is None:
                results.append(df)
            else:
                errors.append({"game_id": game_id, "error": error})
                print(f"[FAILED] {game_id}: {error}")

        namespace["_event_stats_errors"].extend(all_event_errors)
        with namespace["_rebound_fallback_lock"]:
            namespace["_rebound_fallback_deletions"].extend(all_rebound_deletions)

        combined_df = (
            namespace["pd"].concat(results, ignore_index=True)
            if results
            else namespace["pd"].DataFrame()
        )
        error_df = namespace["pd"].DataFrame(errors)
        team_audit_df = namespace["pd"].DataFrame(
            all_team_audit_rows,
            columns=namespace["TEAM_AUDIT_COLUMNS"],
        )
        player_mismatch_df = namespace["pd"].DataFrame(
            all_player_mismatch_rows,
            columns=namespace["PLAYER_MISMATCH_COLUMNS"],
        )
        audit_error_df = namespace["pd"].DataFrame(
            all_audit_error_rows,
            columns=namespace["AUDIT_ERROR_COLUMNS"],
        )

        print(f"[DONE] {len(results)} succeeded, {len(errors)} failed")
        return combined_df, error_df, team_audit_df, player_mismatch_df, audit_error_df

    def process_season_patched(
        season: int,
        parquet_path: str = "playbyplayv2.parq",
        output_dir: str = ".",
        validate: bool = True,
        tolerance: int = 2,
        max_workers: int = -1,
        overrides_path: str = "validation_overrides.csv",
        strict_mode: bool | None = None,
        run_boxscore_audit: bool = False,
    ) -> Tuple[Any, Any]:
        timings: dict[str, float] = {}
        audit_summary: dict[str, Any] | None = None

        overall_start = time.perf_counter()
        overrides = namespace["load_validation_overrides"](overrides_path)
        namespace["clear_rebound_fallback_deletions"]()

        if strict_mode is None:
            strict_mode = True
        print(f"[CONFIG] Season {season}: REBOUND_STRICT_MODE={strict_mode}")

        pbp_load_start = time.perf_counter()
        season_df = namespace["load_pbp_from_parquet"](parquet_path, season=season)
        timings["pbp_load_seconds"] = round(time.perf_counter() - pbp_load_start, 6)

        if season_df.empty:
            print(f"[ERROR] No data found for season {season}")
            timings["game_processing_seconds"] = 0.0
            timings["parquet_write_seconds"] = 0.0
            timings["error_write_seconds"] = 0.0
            timings["rebound_export_seconds"] = 0.0
            timings["boxscore_audit_seconds"] = 0.0
            timings["total_wall_seconds"] = round(
                time.perf_counter() - overall_start, 6
            )
            namespace["_last_process_season_timing"] = timings
            namespace["_last_process_season_resource_usage"] = {
                "peak_rss_mb": _current_peak_rss_mb()
            }
            namespace["_last_process_season_audit_summary"] = None
            return namespace["pd"].DataFrame(), namespace["pd"].DataFrame()

        game_ids = season_df["GAME_ID"].unique().tolist()
        print(
            f"Processing {len(game_ids)} games for season {season} with {max_workers} workers..."
        )

        game_processing_start = time.perf_counter()
        (
            combined_df,
            error_df,
            team_audit_df,
            player_mismatch_df,
            audit_error_df,
        ) = namespace["process_games_parallel"](
            game_ids,
            season_df,
            max_workers=max_workers,
            validate=validate,
            tolerance=tolerance,
            overrides=overrides,
            strict_mode=strict_mode,
            run_boxscore_audit=run_boxscore_audit,
        )
        timings["game_processing_seconds"] = round(
            time.perf_counter() - game_processing_start, 6
        )

        parquet_write_start = time.perf_counter()
        if not combined_df.empty:
            output_file = f"{output_dir}/darko_{season}.parquet"
            combined_df.to_parquet(output_file, index=False)
            print(f"[OUTPUT] Saved {len(combined_df)} rows to {output_file}")
        timings["parquet_write_seconds"] = round(
            time.perf_counter() - parquet_write_start, 6
        )

        error_write_start = time.perf_counter()
        if not error_df.empty:
            error_file = f"{output_dir}/errors_{season}.csv"
            error_df.to_csv(error_file, index=False)
            print(f"[ERRORS] {len(error_df)} game errors saved to {error_file}")
        timings["error_write_seconds"] = round(
            time.perf_counter() - error_write_start, 6
        )

        rebound_export_start = time.perf_counter()
        namespace["export_rebound_fallback_deletions"](
            f"rebound_fallback_deletions_{season}.csv"
        )
        timings["rebound_export_seconds"] = round(
            time.perf_counter() - rebound_export_start, 6
        )

        if run_boxscore_audit:
            audit_start = time.perf_counter()
            audit_summary = namespace["write_boxscore_audit_outputs"](
                team_audit=team_audit_df,
                player_mismatches=player_mismatch_df,
                audit_errors=audit_error_df,
                season=season,
                output_dir=Path(output_dir),
                games_requested=len(game_ids),
            )
            timings["boxscore_audit_seconds"] = round(
                time.perf_counter() - audit_start, 6
            )
            print(
                f"[AUDIT] Saved season {season}: games_with_team_mismatch={audit_summary['games_with_team_mismatch']} "
                f"player_rows_with_mismatch={audit_summary['player_rows_with_mismatch']} "
                f"audit_failures={audit_summary['audit_failures']}"
            )
        else:
            timings["boxscore_audit_seconds"] = 0.0

        timings["total_wall_seconds"] = round(time.perf_counter() - overall_start, 6)
        namespace["_last_process_season_timing"] = timings
        namespace["_last_process_season_resource_usage"] = {
            "peak_rss_mb": _current_peak_rss_mb()
        }
        namespace["_last_process_season_audit_summary"] = audit_summary
        return combined_df, error_df

    namespace["process_games_parallel"] = process_games_parallel_patched
    namespace["process_season"] = process_season_patched


def load_v9b_namespace(
    *,
    notebook_dump_path: Path = NOTEBOOK_DUMP,
    preload_module_paths: Dict[str, Path] | None = None,
) -> Dict[str, Any]:
    _ensure_local_pbpstats_importable()
    module_paths = preload_module_paths or {
        module_name: ROOT / f"{module_name}.py"
        for module_name in NOTEBOOK_LOCAL_IMPORT_PRELOADS
    }
    for module_name in NOTEBOOK_LOCAL_IMPORT_PRELOADS:
        _preload_local_module(module_name, Path(module_paths[module_name]))
    source = notebook_dump_path.read_text(encoding="utf-8")
    marker = 'if __name__ == "__main__":\n    pass\n'
    if marker not in source:
        raise RuntimeError(f"Could not find safe split marker in {notebook_dump_path}")
    prefix = source.split(marker, 1)[0] + marker
    namespace: Dict[str, Any] = {
        "__name__": "v9b_dump_safe",
        "__file__": str(notebook_dump_path),
    }
    exec(compile(prefix, str(notebook_dump_path), "exec"), namespace)
    _patch_v9b_runtime_namespace(namespace)
    return namespace


def install_local_boxscore_wrapper(
    namespace: Dict[str, Any],
    db_path: Path,
    file_directory: Path = DEFAULT_FILE_DIRECTORY,
    allowed_seasons: Iterable[int] | None = None,
    allowed_game_ids: Iterable[str | int] | None = None,
    period_starter_parquet_paths: Iterable[Path] | None = None,
) -> None:
    from historic_backfill.common.period_boxscore_source_loader import (
        PeriodBoxscoreSourceLoader,
    )

    original_get_possessions = namespace["get_possessions_from_df"]
    parquet_paths = [
        Path(path).resolve()
        for path in (
            period_starter_parquet_paths
            if period_starter_parquet_paths is not None
            else DEFAULT_PERIOD_STARTERS_PARQUETS
        )
    ]
    period_boxscore_source_loader = PeriodBoxscoreSourceLoader(
        parquet_paths=parquet_paths,
        allowed_seasons=allowed_seasons,
        allowed_game_ids=allowed_game_ids,
    )

    def wrapped_get_possessions(*args: Any, **kwargs: Any) -> Any:
        pbp_df = args[0] if args else kwargs.get("pbp_df")
        loader = None
        if pbp_df is not None and not pbp_df.empty and "GAME_ID" in pbp_df.columns:
            game_id = str(pbp_df["GAME_ID"].iloc[0]).zfill(10)
            raw_boxscore = _load_raw_response(db_path, game_id, "boxscore")
            if raw_boxscore is not None:
                loader = _BoxscoreSourceLoader(raw_boxscore)

        kwargs.setdefault("boxscore_source_loader", loader)
        kwargs.setdefault(
            "period_boxscore_source_loader", period_boxscore_source_loader
        )
        kwargs.setdefault("file_directory", str(file_directory.resolve()))
        return original_get_possessions(*args, **kwargs)

    namespace["get_possessions_from_df"] = wrapped_get_possessions


def run_lineup_audits(
    combined_df: Any,
    season: int,
    output_dir: Path,
    db_path: Path,
    parquet_path: Path,
    file_directory: Path = DEFAULT_FILE_DIRECTORY,
) -> Dict[str, Any]:
    from historic_backfill.audits.core.event_player_on_court import (
        audit_event_player_on_court,
    )
    from historic_backfill.audits.core.minutes_plus_minus import (
        build_minutes_plus_minus_audit,
        summarize_minutes_plus_minus_audit,
    )

    overall_start = time.perf_counter()

    minutes_audit_start = time.perf_counter()
    minutes_audit_df = build_minutes_plus_minus_audit(combined_df, db_path=db_path)
    minutes_summary = summarize_minutes_plus_minus_audit(minutes_audit_df)
    minutes_audit_seconds = round(time.perf_counter() - minutes_audit_start, 6)
    minutes_audit_df.to_csv(
        output_dir / f"minutes_plus_minus_audit_{season}.csv", index=False
    )
    (output_dir / f"minutes_plus_minus_summary_{season}.json").write_text(
        json.dumps(minutes_summary, indent=2),
        encoding="utf-8",
    )

    problem_game_ids = sorted(
        {
            str(game_id).zfill(10)
            for game_id in minutes_audit_df.loc[
                minutes_audit_df["has_minutes_mismatch"]
                | minutes_audit_df["has_plus_minus_mismatch"],
                "game_id",
            ].tolist()
        }
    )
    (output_dir / f"lineup_problem_games_{season}.txt").write_text(
        "\n".join(problem_game_ids) + ("\n" if problem_game_ids else ""),
        encoding="utf-8",
    )

    event_on_court_start = time.perf_counter()
    issues_df, event_summary = audit_event_player_on_court(
        game_ids=problem_game_ids,
        parquet_path=parquet_path,
        db_path=db_path,
        file_directory=file_directory,
    )
    event_on_court_seconds = round(time.perf_counter() - event_on_court_start, 6)
    issues_df.to_csv(
        output_dir / f"event_player_on_court_issues_{season}.csv", index=False
    )
    (output_dir / f"event_on_court_summary_{season}.json").write_text(
        json.dumps(event_summary, indent=2),
        encoding="utf-8",
    )

    return {
        "minutes_plus_minus": minutes_summary,
        "problem_games": int(len(problem_game_ids)),
        "event_on_court": event_summary,
        "timings": {
            "minutes_audit_seconds": minutes_audit_seconds,
            "event_on_court_seconds": event_on_court_seconds,
            "total_wall_seconds": round(time.perf_counter() - overall_start, 6),
        },
        "resource_usage": {
            "peak_rss_mb": _current_peak_rss_mb(),
        },
    }


def run_season(
    namespace: Dict[str, Any],
    season: int,
    output_dir: Path,
    parquet_path: Path,
    db_path: Path,
    file_directory: Path,
    overrides_path: Path,
    strict_mode: bool,
    tolerance: int,
    max_workers: int,
    run_boxscore_audit_pass: bool,
    run_lineup_audit_pass: bool,
) -> Dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    namespace["DB_PATH"] = db_path
    namespace["clear_event_stats_errors"]()
    overall_start = time.perf_counter()

    old_cwd = Path.cwd()
    os.chdir(output_dir)
    try:
        combined_df, error_df = namespace["process_season"](
            season=season,
            parquet_path=str(parquet_path),
            output_dir=".",
            validate=True,
            tolerance=tolerance,
            max_workers=max_workers,
            overrides_path=str(overrides_path),
            strict_mode=strict_mode,
            run_boxscore_audit=run_boxscore_audit_pass,
        )
        if namespace.get("_event_stats_errors"):
            namespace["export_event_stats_errors"](f"event_stats_errors_{season}.csv")
        player_rows = len(combined_df)
        failed_games = len(error_df)
        event_errors = len(namespace.get("_event_stats_errors", []))
        audit_summary = namespace.get("_last_process_season_audit_summary")
        lineup_audit_summary = None
        if run_boxscore_audit_pass and audit_summary is None:
            audit_summary_path = output_dir / f"boxscore_audit_summary_{season}.json"
            if audit_summary_path.exists():
                audit_summary = json.loads(
                    audit_summary_path.read_text(encoding="utf-8")
                )
            else:
                print(
                    f"[AUDIT] No integrated audit summary was written for season {season}"
                )
        if run_lineup_audit_pass:
            lineup_audit_summary = run_lineup_audits(
                combined_df=combined_df,
                season=season,
                output_dir=output_dir,
                db_path=db_path,
                parquet_path=parquet_path,
                file_directory=file_directory,
            )
            print(
                f"[LINEUP AUDIT] Finished season {season}: "
                f"minutes_mismatches={lineup_audit_summary['minutes_plus_minus']['minutes_mismatches']} "
                f"minutes_outliers={lineup_audit_summary['minutes_plus_minus']['minutes_outliers']} "
                f"plus_minus_mismatches={lineup_audit_summary['minutes_plus_minus']['plus_minus_mismatches']} "
                f"event_on_court_issue_games={lineup_audit_summary['event_on_court']['issue_games']}"
            )
        process_timings = dict(namespace.get("_last_process_season_timing", {}))
        process_timings["lineup_audit_seconds"] = (
            float(
                lineup_audit_summary.get("timings", {}).get("total_wall_seconds", 0.0)
            )
            if lineup_audit_summary is not None
            else 0.0
        )
        process_timings["total_wall_seconds"] = round(
            time.perf_counter() - overall_start, 6
        )
        summary = {
            "season": season,
            "player_rows": player_rows,
            "failed_games": failed_games,
            "event_stats_errors": event_errors,
            "strict_mode": strict_mode,
            "tolerance": tolerance,
            "boxscore_audit": audit_summary,
            "lineup_audit": lineup_audit_summary,
            "timings": process_timings,
            "resource_usage": {
                "peak_rss_mb": _current_peak_rss_mb(),
                **dict(namespace.get("_last_process_season_resource_usage", {})),
            },
        }
        (output_dir / f"summary_{season}.json").write_text(
            json.dumps(summary, indent=2), encoding="utf-8"
        )
        return summary
    finally:
        os.chdir(old_cwd)


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cautious offline rerun for replace_tpdev seasons"
    )
    parser.add_argument("--seasons", nargs="+", type=int, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--parquet-path", type=Path, default=DEFAULT_PARQUET)
    parser.add_argument("--overrides-path", type=Path, default=DEFAULT_OVERRIDES)
    parser.add_argument(
        "--boxscore-source-overrides-path",
        type=Path,
        default=DEFAULT_BOXSCORE_SOURCE_OVERRIDES,
    )
    parser.add_argument("--file-directory", type=Path, default=DEFAULT_FILE_DIRECTORY)
    override_group = parser.add_mutually_exclusive_group()
    override_group.add_argument(
        "--catalog-overrides-dir",
        type=Path,
        default=DEFAULT_RUNTIME_CATALOG_OVERRIDES_DIR,
        help=(
            "Directory of committed runtime override JSONs to snapshot into the run "
            "file directory."
        ),
    )
    override_group.add_argument(
        "--use-file-directory-overrides",
        action="store_true",
        default=False,
        help=(
            "Snapshot overrides from --file-directory/overrides instead of the "
            "committed catalog override directory."
        ),
    )
    parser.add_argument("--strict-mode", action="store_true", default=False)
    parser.add_argument("--tolerance", type=int, default=2)
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument("--run-boxscore-audit", action="store_true", default=False)
    parser.add_argument("--skip-lineup-audit", action="store_true", default=False)
    parser.add_argument(
        "--audit-profile", choices=sorted(AUDIT_PROFILES), default=DEFAULT_AUDIT_PROFILE
    )
    parser.add_argument(
        "--allow-unreadable-csv-fallback", action="store_true", default=False
    )
    parser.add_argument(
        "--runtime-input-cache-mode",
        choices=sorted(RUNTIME_INPUT_CACHE_MODES),
        default=DEFAULT_RUNTIME_INPUT_CACHE_MODE,
    )
    return parser.parse_args(argv)


def _runner_failure_reasons(final_summary: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    failed_games = int(final_summary.get("failed_games", 0) or 0)
    event_stats_errors = int(final_summary.get("event_stats_errors", 0) or 0)
    if failed_games > 0:
        reasons.append(f"failed_games={failed_games}")
    if event_stats_errors > 0:
        reasons.append(f"event_stats_errors={event_stats_errors}")

    zero_row_seasons = [
        int(summary.get("season"))
        for summary in final_summary.get("seasons", [])
        if int(summary.get("player_rows", 0) or 0) == 0
    ]
    if zero_row_seasons:
        reasons.append(f"zero_player_row_seasons={zero_row_seasons}")

    if final_summary.get("run_boxscore_audit"):
        audit_failure_seasons = []
        for summary in final_summary.get("seasons", []):
            audit_summary = summary.get("boxscore_audit") or {}
            if int(audit_summary.get("audit_failures", 0) or 0) > 0:
                audit_failure_seasons.append(int(summary.get("season")))
        if audit_failure_seasons:
            reasons.append(f"boxscore_audit_failure_seasons={audit_failure_seasons}")
    return reasons


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    catalog_overrides_dir = (
        None
        if args.use_file_directory_overrides
        else args.catalog_overrides_dir.resolve()
    )
    overall_start = time.perf_counter()
    runtime_input_start = time.perf_counter()
    runtime_inputs = prepare_local_runtime_inputs(
        output_dir / "_local_runtime_cache",
        db_path=args.db_path.resolve(),
        parquet_path=args.parquet_path.resolve(),
        overrides_path=args.overrides_path.resolve(),
        boxscore_source_overrides_path=args.boxscore_source_overrides_path.resolve(),
        allow_unreadable_csv_fallback=args.allow_unreadable_csv_fallback,
        file_directory=args.file_directory.resolve(),
        catalog_overrides_dir=catalog_overrides_dir,
        runtime_input_cache_mode=args.runtime_input_cache_mode,
    )
    (output_dir / "runtime_input_provenance.json").write_text(
        json.dumps(runtime_inputs["runtime_input_provenance"], indent=2),
        encoding="utf-8",
    )
    runtime_input_seconds = round(time.perf_counter() - runtime_input_start, 6)

    namespace_load_start = time.perf_counter()
    namespace = load_v9b_namespace(
        notebook_dump_path=runtime_inputs["notebook_dump_path"],
        preload_module_paths=runtime_inputs["preload_module_paths"],
    )
    install_local_boxscore_wrapper(
        namespace,
        runtime_inputs["db_path"],
        file_directory=runtime_inputs["file_directory"],
        allowed_seasons=args.seasons,
        period_starter_parquet_paths=runtime_inputs["period_starter_parquet_paths"],
    )
    namespace_load_seconds = round(time.perf_counter() - namespace_load_start, 6)

    print(f"[RUNNER] Output dir: {output_dir}")
    print(f"[RUNNER] Seasons: {args.seasons}")
    print(f"[RUNNER] Strict mode: {args.strict_mode}")
    print(f"[RUNNER] Tolerance: {args.tolerance}")
    print(f"[RUNNER] max_workers: {args.max_workers}")
    print(f"[RUNNER] boxscore_audit: {args.run_boxscore_audit}")
    print(f"[RUNNER] audit_profile: {args.audit_profile}")
    print(
        f"[RUNNER] lineup_audit: {args.audit_profile == 'full' and not args.skip_lineup_audit}"
    )
    print(f"[RUNNER] runtime_input_cache_mode: {args.runtime_input_cache_mode}")
    print(f"[RUNNER] live_file_directory: {args.file_directory.resolve()}")
    print(f"[RUNNER] runtime_file_directory: {runtime_inputs['file_directory']}")
    print(
        "[RUNNER] overrides_snapshot: "
        f"{runtime_inputs['runtime_input_provenance']['file_directory']['overrides_snapshot']['source_kind']} "
        f"from {runtime_inputs['runtime_input_provenance']['file_directory']['overrides_snapshot']['source']['path']}"
    )
    print(
        "[RUNNER] period_starters_sources: "
        + ", ".join(
            str(Path(path).name)
            for path in runtime_inputs["period_starter_parquet_paths"]
            if Path(path).exists()
        )
    )

    season_summaries: list[dict[str, Any]] = []
    for season in args.seasons:
        print(f"[RUNNER] Starting season {season}")
        try:
            season_summary = run_season(
                namespace=namespace,
                season=season,
                output_dir=output_dir,
                parquet_path=runtime_inputs["parquet_path"],
                db_path=runtime_inputs["db_path"],
                file_directory=runtime_inputs["file_directory"],
                overrides_path=runtime_inputs["overrides_path"],
                strict_mode=args.strict_mode,
                tolerance=args.tolerance,
                max_workers=args.max_workers,
                run_boxscore_audit_pass=args.run_boxscore_audit,
                run_lineup_audit_pass=(
                    args.audit_profile == "full" and not args.skip_lineup_audit
                ),
            )
            season_summaries.append(season_summary)
            print(
                f"[RUNNER] Finished season {season}: "
                f"player_rows={season_summary['player_rows']} "
                f"failed_games={season_summary['failed_games']} "
                f"event_stats_errors={season_summary['event_stats_errors']}"
            )
        except Exception:
            print(f"[RUNNER] Season {season} failed with an unhandled exception")
            traceback.print_exc()
            return 1

    final_summary = {
        "seasons_requested": [int(season) for season in args.seasons],
        "seasons_completed": [int(summary["season"]) for summary in season_summaries],
        "player_rows": int(
            sum(int(summary.get("player_rows", 0) or 0) for summary in season_summaries)
        ),
        "failed_games": int(
            sum(
                int(summary.get("failed_games", 0) or 0) for summary in season_summaries
            )
        ),
        "event_stats_errors": int(
            sum(
                int(summary.get("event_stats_errors", 0) or 0)
                for summary in season_summaries
            )
        ),
        "strict_mode": bool(args.strict_mode),
        "tolerance": int(args.tolerance),
        "max_workers": int(args.max_workers),
        "run_boxscore_audit": bool(args.run_boxscore_audit),
        "audit_profile": str(args.audit_profile),
        "runtime_input_cache_mode": str(args.runtime_input_cache_mode),
        "runtime_input_provenance_path": str(
            (output_dir / "runtime_input_provenance.json").resolve()
        ),
        "runtime_file_directory": str(runtime_inputs["file_directory"]),
        "timings": {
            "runtime_input_prep_seconds": runtime_input_seconds,
            "namespace_load_seconds": namespace_load_seconds,
            "total_wall_seconds": round(time.perf_counter() - overall_start, 6),
        },
        "resource_usage": {
            "peak_rss_mb": _current_peak_rss_mb(),
        },
        "seasons": season_summaries,
    }
    failure_reasons = _runner_failure_reasons(final_summary)
    final_summary["ok"] = not failure_reasons
    final_summary["failure_reasons"] = failure_reasons
    (output_dir / "summary.json").write_text(
        json.dumps(final_summary, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(final_summary, indent=2))
    if failure_reasons:
        print(f"[RUNNER] Completed with failures: {failure_reasons}")
        return 1
    print("[RUNNER] All requested seasons complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
