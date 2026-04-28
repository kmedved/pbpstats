"""Scoped validation entrypoint for historic backfill workflows."""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

from historic_backfill.catalogs.lineup_correction_manifest import (
    validate_compiled_runtime_views,
    validate_manifest_schema,
)
from historic_backfill.catalogs.loader import validate_historic_pbp_row_override_catalog


ROOT = Path(__file__).resolve().parents[1]

CORE_INPUTS = (
    "data/nba_raw.db",
    "data/playbyplayv2.parq",
    "data/period_starters_v6.parquet",
    "data/period_starters_v5.parquet",
)
OPTIONAL_CROSS_SOURCE_INPUTS = (
    "data/bbr/bbref_boxscores.db",
    "data/tpdev/full_pbp_new.parq",
    "data/tpdev/tpdev_box.parq",
    "data/tpdev/tpdev_box_new.parq",
    "data/tpdev/tpdev_box_cdn.parq",
)
CORE_CATALOG_INPUTS = (
    "catalogs/pbp_row_overrides.csv",
    "catalogs/pbp_stat_overrides.csv",
    "catalogs/validation_overrides.csv",
    "catalogs/overrides/correction_manifest.json",
    "catalogs/overrides/period_starters_overrides.json",
    "catalogs/overrides/lineup_window_overrides.json",
)
REQUIRED_RAW_RESPONSE_ENDPOINTS = {"boxscore", "summary", "pbpv3"}
PBP_STAT_OVERRIDE_REQUIRED_COLUMNS = {
    "game_id",
    "team_id",
    "player_id",
    "stat_key",
    "stat_value",
    "notes",
}
VALIDATION_OVERRIDE_REQUIRED_COLUMNS = {"game_id", "action", "tolerance", "notes"}
VALIDATION_OVERRIDE_ACTIONS = {"allow"}
CORRECTION_MANIFEST_REQUIRED_KEYS = {
    "manifest_version",
    "corrections",
    "residual_annotations",
}


@dataclass
class ValidationResult:
    scope: str
    ok: bool
    validation_level: str
    missing_required: list[str] = field(default_factory=list)
    skipped_optional: list[str] = field(default_factory=list)
    validation_errors: list[str] = field(default_factory=list)
    message: str = ""

    def to_dict(self) -> dict[str, object]:
        return {
            "scope": self.scope,
            "ok": self.ok,
            "validation_level": self.validation_level,
            "missing_required": self.missing_required,
            "skipped_optional": self.skipped_optional,
            "validation_errors": self.validation_errors,
            "message": self.message,
        }


def _missing(root: Path, paths: Iterable[str]) -> list[str]:
    return [path for path in paths if not (root / path).exists()]


def _validate_csv_columns(path: Path, required_columns: set[str]) -> None:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        try:
            header = next(reader)
        except StopIteration as exc:
            raise ValueError(f"{path} is empty") from exc
    missing_columns = required_columns - set(header)
    if missing_columns:
        raise ValueError(f"{path} missing columns: {sorted(missing_columns)}")


def _read_csv_dicts(path: Path, required_columns: set[str]) -> list[dict[str, str]]:
    _validate_csv_columns(path, required_columns)
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _parse_numeric_field(
    value: str,
    *,
    path: Path,
    row_number: int,
    field: str,
    integer: bool,
) -> float | int:
    text = str(value if value is not None else "").strip()
    if not text:
        raise ValueError(f"{path} row {row_number} missing required field {field}")
    try:
        parsed = float(text)
    except ValueError as exc:
        raise ValueError(
            f"{path} row {row_number} has invalid numeric {field}: {text!r}"
        ) from exc
    if integer:
        if not parsed.is_integer():
            raise ValueError(
                f"{path} row {row_number} has non-integer {field}: {text!r}"
            )
        return int(parsed)
    return parsed


def _validate_pbp_stat_overrides(path: Path) -> None:
    for row_number, row in enumerate(
        _read_csv_dicts(path, PBP_STAT_OVERRIDE_REQUIRED_COLUMNS),
        start=2,
    ):
        _parse_numeric_field(
            row.get("game_id", ""),
            path=path,
            row_number=row_number,
            field="game_id",
            integer=True,
        )
        team_id = _parse_numeric_field(
            row.get("team_id", ""),
            path=path,
            row_number=row_number,
            field="team_id",
            integer=True,
        )
        player_id = _parse_numeric_field(
            row.get("player_id", ""),
            path=path,
            row_number=row_number,
            field="player_id",
            integer=True,
        )
        stat_value = _parse_numeric_field(
            row.get("stat_value", ""),
            path=path,
            row_number=row_number,
            field="stat_value",
            integer=False,
        )
        if team_id <= 0:
            raise ValueError(f"{path} row {row_number} team_id must be positive")
        if player_id <= 0:
            raise ValueError(f"{path} row {row_number} player_id must be positive")
        if float(stat_value) == 0.0:
            raise ValueError(f"{path} row {row_number} stat_value must be non-zero")
        if not str(row.get("stat_key", "")).strip():
            raise ValueError(f"{path} row {row_number} missing required field stat_key")


def _validate_validation_overrides(path: Path) -> None:
    for row_number, row in enumerate(
        _read_csv_dicts(path, VALIDATION_OVERRIDE_REQUIRED_COLUMNS),
        start=2,
    ):
        _parse_numeric_field(
            row.get("game_id", ""),
            path=path,
            row_number=row_number,
            field="game_id",
            integer=True,
        )
        action = str(row.get("action", "")).strip().lower()
        if action not in VALIDATION_OVERRIDE_ACTIONS:
            raise ValueError(f"{path} row {row_number} has invalid action: {action!r}")
        tolerance = _parse_numeric_field(
            row.get("tolerance", ""),
            path=path,
            row_number=row_number,
            field="tolerance",
            integer=False,
        )
        if tolerance < 0:
            raise ValueError(f"{path} row {row_number} tolerance must be non-negative")


def _validate_json_keys(path: Path, required_keys: set[str]) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    missing_keys = required_keys - set(payload)
    if missing_keys:
        raise ValueError(f"{path} missing keys: {sorted(missing_keys)}")


def _validate_nba_raw_db(path: Path) -> None:
    if not path.exists():
        return
    try:
        conn = sqlite3.connect(path)
    except sqlite3.Error as exc:
        raise ValueError(f"{path} is not a readable SQLite database: {exc}") from exc
    try:
        table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='raw_responses'"
        ).fetchone()
        if table is None:
            raise ValueError(f"{path} missing required table raw_responses")
        columns = {row[1] for row in conn.execute("PRAGMA table_info(raw_responses)")}
        required_columns = {"game_id", "endpoint", "team_id", "data"}
        missing_columns = required_columns - columns
        if missing_columns:
            raise ValueError(
                f"{path} raw_responses missing columns: {sorted(missing_columns)}"
            )
        observed = {
            str(row[0])
            for row in conn.execute(
                "SELECT DISTINCT endpoint FROM raw_responses WHERE endpoint IS NOT NULL"
            )
        }
        missing_endpoints = REQUIRED_RAW_RESPONSE_ENDPOINTS - observed
        if missing_endpoints:
            raise ValueError(
                f"{path} raw_responses missing required endpoints: {sorted(missing_endpoints)}"
            )
    except sqlite3.Error as exc:
        raise ValueError(f"{path} failed raw_responses validation: {exc}") from exc
    finally:
        conn.close()


def _validate_core_nba_inputs(root: Path) -> list[str]:
    errors: list[str] = []
    db_path = root / "data" / "nba_raw.db"
    if db_path.exists():
        try:
            _validate_nba_raw_db(db_path)
        except Exception as exc:  # noqa: BLE001 - report plainly in CLI output.
            errors.append(str(exc))
    return errors


def _validate_core_catalogs(root: Path) -> list[str]:
    validators = [
        (
            root / "catalogs" / "pbp_row_overrides.csv",
            lambda path: validate_historic_pbp_row_override_catalog(path),
        ),
        (
            root / "catalogs" / "pbp_stat_overrides.csv",
            _validate_pbp_stat_overrides,
        ),
        (
            root / "catalogs" / "validation_overrides.csv",
            _validate_validation_overrides,
        ),
        (
            root / "catalogs" / "overrides" / "correction_manifest.json",
            lambda path: (
                _validate_json_keys(path, CORRECTION_MANIFEST_REQUIRED_KEYS),
                validate_manifest_schema(path),
                validate_compiled_runtime_views(path, path.parent),
            ),
        ),
        (
            root / "catalogs" / "overrides" / "period_starters_overrides.json",
            lambda path: json.loads(path.read_text(encoding="utf-8")),
        ),
        (
            root / "catalogs" / "overrides" / "lineup_window_overrides.json",
            lambda path: json.loads(path.read_text(encoding="utf-8")),
        ),
    ]
    errors: list[str] = []
    for path, validator in validators:
        if not path.exists():
            continue
        try:
            validator(path)
        except (
            Exception
        ) as exc:  # noqa: BLE001 - CLI preflight should report catalog errors plainly.
            errors.append(str(exc))
    return errors


def validate_scope(scope: str, root: Path = ROOT) -> ValidationResult:
    root = root.resolve()
    if scope == "core":
        missing_required = _missing(root, (*CORE_INPUTS, *CORE_CATALOG_INPUTS))
        validation_errors = [
            *_validate_core_nba_inputs(root),
            *_validate_core_catalogs(root),
        ]
        return ValidationResult(
            scope=scope,
            ok=not missing_required and not validation_errors,
            validation_level="input_preflight",
            missing_required=missing_required,
            validation_errors=validation_errors,
            message=(
                "core input/catalog preflight passed"
                if not missing_required and not validation_errors
                else "missing or invalid NBA-only runtime inputs/catalogs"
            ),
        )

    if scope == "cross-source":
        skipped_optional = _missing(root, OPTIONAL_CROSS_SOURCE_INPUTS)
        return ValidationResult(
            scope=scope,
            ok=True,
            validation_level="optional_diagnostic_preflight",
            skipped_optional=skipped_optional,
            message=(
                "cross-source inputs present"
                if not skipped_optional
                else "missing optional BBR/tpdev inputs skipped"
            ),
        )

    if scope == "provenance":
        missing_required = _missing(root, OPTIONAL_CROSS_SOURCE_INPUTS)
        return ValidationResult(
            scope=scope,
            ok=not missing_required,
            validation_level="provenance_evidence_preflight",
            missing_required=missing_required,
            message=(
                "provenance inputs present"
                if not missing_required
                else "missing evidence inputs required for provenance re-review"
            ),
        )

    raise ValueError(f"Unknown validation scope: {scope}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scope",
        choices=("core", "cross-source", "provenance"),
        required=True,
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=ROOT,
        help="Historic backfill root. Defaults to the package root.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = validate_scope(args.scope, args.root)
    print(json.dumps(result.to_dict(), indent=2))
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
