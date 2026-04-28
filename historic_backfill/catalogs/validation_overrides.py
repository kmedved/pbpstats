from __future__ import annotations

import csv
import math
from pathlib import Path
from typing import Any, Dict

from pbpstats.offline.row_overrides import normalize_game_id


DEFAULT_VALIDATION_OVERRIDES_PATH = (
    Path(__file__).resolve().parent / "validation_overrides.csv"
)
VALIDATION_OVERRIDE_COLUMNS = {"game_id", "action", "tolerance", "notes"}
RELEASE_SAFE_ACTIONS = {"allow"}


def _parse_tolerance(value: object, *, path: Path, row_number: int) -> int | float:
    text = str(value if value is not None else "").strip()
    if not text:
        raise ValueError(f"{path} row {row_number} missing tolerance")
    try:
        parsed = float(text)
    except ValueError as exc:
        raise ValueError(
            f"{path} row {row_number} has invalid tolerance: {text!r}"
        ) from exc
    if not math.isfinite(parsed) or parsed < 0:
        raise ValueError(f"{path} row {row_number} invalid tolerance={text!r}")
    if parsed.is_integer():
        return int(parsed)
    return parsed


def _read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        missing = VALIDATION_OVERRIDE_COLUMNS - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"{path} missing columns: {sorted(missing)}")
        return list(reader)


def load_validation_overrides(
    filepath: str | Path = DEFAULT_VALIDATION_OVERRIDES_PATH,
    *,
    release_safe: bool = True,
) -> Dict[str, Dict[str, Any]]:
    path = Path(filepath)
    if not path.exists():
        return {}

    overrides: Dict[str, Dict[str, Any]] = {}
    for row_number, row in enumerate(_read_rows(path), start=2):
        try:
            game_id = normalize_game_id(row.get("game_id"))
        except ValueError as exc:
            raise ValueError(
                f"{path} row {row_number} has invalid game_id: {row.get('game_id')!r}"
            ) from exc
        if game_id in overrides:
            raise ValueError(f"{path} row {row_number} duplicates game_id={game_id}")

        action = str(row.get("action", "")).strip().lower()
        if release_safe and action not in RELEASE_SAFE_ACTIONS:
            raise ValueError(
                f"{path} row {row_number} has invalid action for release: {action!r}"
            )
        if not release_safe and action not in RELEASE_SAFE_ACTIONS | {"skip"}:
            raise ValueError(f"{path} row {row_number} has invalid action: {action!r}")

        overrides[game_id] = {
            "action": action,
            "tolerance": _parse_tolerance(
                row.get("tolerance"),
                path=path,
                row_number=row_number,
            ),
            "notes": str(row.get("notes", "")).strip(),
        }
    return overrides


def validate_validation_overrides(
    filepath: str | Path = DEFAULT_VALIDATION_OVERRIDES_PATH,
    *,
    release_safe: bool = True,
) -> None:
    load_validation_overrides(filepath, release_safe=release_safe)
