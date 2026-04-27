from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Iterable

from historic_backfill.runners.cautious_rerun import AUDIT_PROFILES, DEFAULT_AUDIT_PROFILE, RUNTIME_INPUT_CACHE_MODES
from historic_backfill.catalogs.lineup_correction_manifest import (
    DEFAULT_DB_PATH,
    DEFAULT_FILE_DIRECTORY,
    DEFAULT_MANIFEST_PATH,
    DEFAULT_OVERRIDES_DIR,
    DEFAULT_PARQUET_PATH,
    compile_runtime_views,
    load_manifest,
)


ROOT = Path(__file__).resolve().parent
DEFAULT_PBPSTATS_REPO = ROOT.parent / "pbpstats"
DEFAULT_ORCHESTRATOR_RUNTIME_INPUT_CACHE_MODE = "reuse-validated-cache"


def _normalize_game_id(value: str | int) -> str:
    return str(int(value)).zfill(10)


def _expand_tokens(values: Iterable[str]) -> list[str]:
    expanded: list[str] = []
    for value in values:
        expanded.extend(token.strip() for token in str(value).split(",") if token.strip())
    return expanded


def _load_game_ids(args: argparse.Namespace) -> list[str]:
    raw_ids: list[str] = []
    if args.game_ids:
        raw_ids.extend(_expand_tokens(args.game_ids))
    if args.game_ids_file is not None:
        raw_ids.extend(
            _expand_tokens(
                line.strip()
                for line in args.game_ids_file.read_text(encoding="utf-8").splitlines()
                if line.strip()
            )
        )
    if not raw_ids:
        raise ValueError("Provide --game-ids and/or --game-ids-file")
    return sorted({_normalize_game_id(game_id) for game_id in raw_ids})


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a scratch-only targeted rerun with selected corrections activated in a temp manifest "
            "and compiled to a temp file_directory."
        )
    )
    parser.add_argument("--game-ids", nargs="*")
    parser.add_argument("--game-ids-file", type=Path)
    parser.add_argument("--manifest-path", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--live-file-directory", type=Path, default=DEFAULT_FILE_DIRECTORY)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--parquet-path", type=Path, default=DEFAULT_PARQUET_PATH)
    parser.add_argument(
        "--validation-overrides-path",
        type=Path,
        default=ROOT / "validation_overrides.csv",
    )
    parser.add_argument("--activate-correction-id", action="append", default=[])
    parser.add_argument("--activate-episode-id", action="append", default=[])
    parser.add_argument("--deactivate-correction-id", action="append", default=[])
    parser.add_argument("--deactivate-episode-id", action="append", default=[])
    parser.add_argument("--strict-mode", action="store_true", default=False)
    parser.add_argument("--tolerance", type=int, default=2)
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument("--run-boxscore-audit", action="store_true", default=False)
    parser.add_argument("--skip-lineup-audit", action="store_true", default=False)
    parser.add_argument(
        "--runtime-input-cache-mode",
        choices=sorted(RUNTIME_INPUT_CACHE_MODES),
        default=DEFAULT_ORCHESTRATOR_RUNTIME_INPUT_CACHE_MODE,
    )
    parser.add_argument("--audit-profile", choices=sorted(AUDIT_PROFILES), default=DEFAULT_AUDIT_PROFILE)
    parser.add_argument("--allow-unreadable-csv-fallback", action="store_true", default=False)
    parser.add_argument("--compile-only", action="store_true", default=False)
    return parser.parse_args(argv)


def _set_correction_statuses(
    manifest: dict,
    *,
    activate_correction_ids: set[str],
    activate_episode_ids: set[str],
    deactivate_correction_ids: set[str],
    deactivate_episode_ids: set[str],
) -> dict[str, list[str]]:
    seen_ids = {str(correction["correction_id"]) for correction in manifest.get("corrections", [])}
    seen_episode_ids = {str(correction["episode_id"]) for correction in manifest.get("corrections", [])}

    missing_correction_ids = sorted(
        (activate_correction_ids | deactivate_correction_ids) - seen_ids
    )
    missing_episode_ids = sorted((activate_episode_ids | deactivate_episode_ids) - seen_episode_ids)
    if missing_correction_ids:
        raise ValueError(f"Unknown correction ids: {missing_correction_ids}")
    if missing_episode_ids:
        raise ValueError(f"Unknown episode ids: {missing_episode_ids}")

    activated: list[str] = []
    deactivated: list[str] = []
    for correction in manifest.get("corrections", []):
        correction_id = str(correction["correction_id"])
        episode_id = str(correction["episode_id"])
        if correction_id in activate_correction_ids or episode_id in activate_episode_ids:
            if correction.get("status") != "active":
                correction["status"] = "active"
                activated.append(correction_id)
        if correction_id in deactivate_correction_ids or episode_id in deactivate_episode_ids:
            if correction.get("status") == "active":
                correction["status"] = "retired"
                deactivated.append(correction_id)
    return {
        "activated_correction_ids": sorted(activated),
        "deactivated_correction_ids": sorted(deactivated),
    }


def _copy_live_overrides(live_file_directory: Path, scratch_file_directory: Path) -> None:
    live_overrides_dir = live_file_directory.resolve() / "overrides"
    scratch_overrides_dir = scratch_file_directory.resolve() / "overrides"
    scratch_overrides_dir.mkdir(parents=True, exist_ok=True)
    if not live_overrides_dir.exists():
        return
    for source_path in sorted(live_overrides_dir.iterdir()):
        target_path = scratch_overrides_dir / source_path.name
        if source_path.is_dir():
            shutil.copytree(source_path, target_path, dirs_exist_ok=True)
        else:
            shutil.copy2(source_path, target_path)


def _build_rerun_command(
    *,
    game_ids: list[str],
    output_dir: Path,
    db_path: Path,
    parquet_path: Path,
    validation_overrides_path: Path,
    file_directory: Path,
    strict_mode: bool,
    tolerance: int,
    max_workers: int,
    run_boxscore_audit: bool,
    skip_lineup_audit: bool,
    runtime_input_cache_mode: str,
    audit_profile: str,
    allow_unreadable_csv_fallback: bool,
) -> list[str]:
    command = [
        sys.executable,
        str(ROOT / "rerun_selected_games.py"),
        "--output-dir",
        str(output_dir),
        "--db-path",
        str(db_path),
        "--parquet-path",
        str(parquet_path),
        "--overrides-path",
        str(validation_overrides_path),
        "--file-directory",
        str(file_directory),
        "--tolerance",
        str(tolerance),
        "--max-workers",
        str(max_workers),
        "--runtime-input-cache-mode",
        str(runtime_input_cache_mode),
        "--audit-profile",
        str(audit_profile),
    ]
    if strict_mode:
        command.append("--strict-mode")
    if run_boxscore_audit:
        command.append("--run-boxscore-audit")
    if skip_lineup_audit:
        command.append("--skip-lineup-audit")
    if allow_unreadable_csv_fallback:
        command.append("--allow-unreadable-csv-fallback")
    command.extend(["--game-ids", *game_ids])
    return command


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    game_ids = _load_game_ids(args)
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest = load_manifest(args.manifest_path.resolve())
    status_delta = _set_correction_statuses(
        manifest,
        activate_correction_ids=set(args.activate_correction_id),
        activate_episode_ids=set(args.activate_episode_id),
        deactivate_correction_ids=set(args.deactivate_correction_id),
        deactivate_episode_ids=set(args.deactivate_episode_id),
    )

    scratch_file_directory = output_dir / "file_directory"
    _copy_live_overrides(args.live_file_directory.resolve(), scratch_file_directory)
    scratch_manifest_path = scratch_file_directory / "overrides" / "correction_manifest.json"
    scratch_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    scratch_manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    compile_summary = compile_runtime_views(
        manifest,
        output_dir=scratch_file_directory / "overrides",
        db_path=args.db_path.resolve(),
        parquet_path=args.parquet_path.resolve(),
        file_directory=scratch_file_directory,
    )

    rerun_output_dir = output_dir / "rerun"
    rerun_output_dir.mkdir(parents=True, exist_ok=True)
    command = _build_rerun_command(
        game_ids=game_ids,
        output_dir=rerun_output_dir,
        db_path=args.db_path.resolve(),
        parquet_path=args.parquet_path.resolve(),
        validation_overrides_path=args.validation_overrides_path.resolve(),
        file_directory=scratch_file_directory,
        strict_mode=args.strict_mode,
        tolerance=args.tolerance,
        max_workers=args.max_workers,
        run_boxscore_audit=args.run_boxscore_audit,
        skip_lineup_audit=args.skip_lineup_audit,
        runtime_input_cache_mode=args.runtime_input_cache_mode,
        audit_profile=args.audit_profile,
        allow_unreadable_csv_fallback=args.allow_unreadable_csv_fallback,
    )

    probe_summary = {
        "manifest_path": str(args.manifest_path.resolve()),
        "scratch_manifest_path": str(scratch_manifest_path),
        "scratch_file_directory": str(scratch_file_directory),
        "compile_summary": compile_summary,
        "game_ids": game_ids,
        **status_delta,
        "rerun_command": command,
        "compile_only": bool(args.compile_only),
    }
    (output_dir / "probe_summary.json").write_text(
        json.dumps(probe_summary, indent=2) + "\n",
        encoding="utf-8",
    )

    if args.compile_only:
        return 0

    log_path = output_dir / "rerun.log"
    env = os.environ.copy()
    if "PBPSTATS_REPO" not in env and DEFAULT_PBPSTATS_REPO.exists():
        env["PBPSTATS_REPO"] = str(DEFAULT_PBPSTATS_REPO.resolve())
    with log_path.open("w", encoding="utf-8") as log_file:
        process = subprocess.run(
            command,
            check=False,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            env=env,
        )
    probe_summary["rerun_exit_code"] = int(process.returncode)
    (output_dir / "probe_summary.json").write_text(
        json.dumps(probe_summary, indent=2) + "\n",
        encoding="utf-8",
    )
    return int(process.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
