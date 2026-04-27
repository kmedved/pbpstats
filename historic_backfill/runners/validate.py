"""Scoped validation entrypoint for historic backfill workflows."""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

from historic_backfill.catalogs.loader import validate_historic_pbp_row_override_catalog


ROOT = Path(__file__).resolve().parents[1]

CORE_INPUTS = (
    "data/nba_raw.db",
    "data/playbyplayv2.parq",
    "data/playbyplayv3.parq",
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
)
PBP_STAT_OVERRIDE_REQUIRED_COLUMNS = {
    "game_id",
    "team_id",
    "player_id",
    "stat_key",
    "stat_value",
    "notes",
}
VALIDATION_OVERRIDE_REQUIRED_COLUMNS = {"game_id", "action", "tolerance", "notes"}
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


def _validate_json_keys(path: Path, required_keys: set[str]) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    missing_keys = required_keys - set(payload)
    if missing_keys:
        raise ValueError(f"{path} missing keys: {sorted(missing_keys)}")


def _validate_core_catalogs(root: Path) -> list[str]:
    validators = [
        lambda: validate_historic_pbp_row_override_catalog(root / "catalogs" / "pbp_row_overrides.csv"),
        lambda: _validate_csv_columns(
            root / "catalogs" / "pbp_stat_overrides.csv",
            PBP_STAT_OVERRIDE_REQUIRED_COLUMNS,
        ),
        lambda: _validate_csv_columns(
            root / "catalogs" / "validation_overrides.csv",
            VALIDATION_OVERRIDE_REQUIRED_COLUMNS,
        ),
        lambda: _validate_json_keys(
            root / "catalogs" / "overrides" / "correction_manifest.json",
            CORRECTION_MANIFEST_REQUIRED_KEYS,
        ),
    ]
    errors: list[str] = []
    for validator in validators:
        try:
            validator()
        except Exception as exc:  # noqa: BLE001 - CLI preflight should report catalog errors plainly.
            errors.append(str(exc))
    return errors


def validate_scope(scope: str, root: Path = ROOT) -> ValidationResult:
    root = root.resolve()
    if scope == "core":
        missing_required = _missing(root, (*CORE_INPUTS, *CORE_CATALOG_INPUTS))
        validation_errors = [] if missing_required else _validate_core_catalogs(root)
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
