"""Scoped validation entrypoint for historic backfill workflows."""

from __future__ import annotations

import argparse
import csv
import json
import math
import sqlite3
import zlib
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

import pyarrow.parquet as pq

from historic_backfill.catalogs.boxscore_source_overrides import (
    BOXSCORE_SOURCE_COLUMNS,
    BOXSCORE_SOURCE_OVERRIDE_COLUMNS,
    FLOAT_COLUMNS as BOXSCORE_FLOAT_COLUMNS,
    INT_COLUMNS as BOXSCORE_INT_COLUMNS,
    validate_boxscore_source_overrides,
)
from historic_backfill.catalogs.lineup_correction_manifest import (
    validate_compiled_runtime_views,
    validate_manifest_schema,
)
from historic_backfill.catalogs.loader import validate_historic_pbp_row_override_catalog
from historic_backfill.catalogs.pbp_stat_overrides import load_pbp_stat_overrides
from historic_backfill.catalogs.validation_overrides import (
    validate_validation_overrides,
)
from historic_backfill.common.period_boxscore_source_loader import (
    STARTER_LOOKUP_COLUMNS,
)
from pbpstats.offline.row_overrides import normalize_game_id


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
    "catalogs/boxscore_source_overrides.csv",
    "catalogs/overrides/correction_manifest.json",
    "catalogs/overrides/period_starters_overrides.json",
    "catalogs/overrides/lineup_window_overrides.json",
)
REQUIRED_RAW_RESPONSE_ENDPOINTS = {"boxscore", "summary", "pbpv3"}
PLAYBYPLAY_V2_REQUIRED_COLUMNS = {
    "GAME_ID",
    "EVENTNUM",
    "EVENTMSGTYPE",
    "EVENTMSGACTIONTYPE",
    "PERIOD",
    "SEASON",
    "PCTIMESTRING",
    "HOMEDESCRIPTION",
    "VISITORDESCRIPTION",
    "PLAYER1_ID",
    "PLAYER2_ID",
    "PLAYER3_ID",
    "PLAYER1_TEAM_ID",
    "PLAYER2_TEAM_ID",
    "PLAYER3_TEAM_ID",
}
PLAYBYPLAY_V2_INTEGER_COLUMNS = {
    "EVENTMSGACTIONTYPE",
    "EVENTMSGTYPE",
    "EVENTNUM",
    "PERIOD",
    "SEASON",
}
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
    if not math.isfinite(parsed):
        raise ValueError(
            f"{path} row {row_number} has non-finite numeric {field}: {text!r}"
        )
    if integer:
        if not parsed.is_integer():
            raise ValueError(
                f"{path} row {row_number} has non-integer {field}: {text!r}"
            )
        return int(parsed)
    return parsed


def _season_from_game_id(game_id: str) -> int:
    yy = int(game_id[3:5])
    return 1901 + yy if yy >= 50 else 2001 + yy


def _parse_parquet_integer_field(
    value: object,
    *,
    path: Path,
    row_number: int,
    field: str,
) -> int:
    if isinstance(value, str):
        raise ValueError(f"{path} row {row_number} has string {field}: {value!r}")
    return int(
        _parse_numeric_field(
            value,
            path=path,
            row_number=row_number,
            field=field,
            integer=True,
        )
    )


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
                """
                SELECT DISTINCT endpoint
                FROM raw_responses
                WHERE endpoint IS NOT NULL AND team_id IS NULL
                """
            )
        }
        missing_endpoints = REQUIRED_RAW_RESPONSE_ENDPOINTS - observed
        if missing_endpoints:
            raise ValueError(
                f"{path} raw_responses missing team_id IS NULL rows for required "
                f"endpoints: {sorted(missing_endpoints)}"
            )
        for endpoint in sorted(REQUIRED_RAW_RESPONSE_ENDPOINTS):
            bad_rows: list[str] = []
            row_count = 0
            rows = conn.execute(
                """
                SELECT game_id, data
                FROM raw_responses
                WHERE endpoint=? AND team_id IS NULL
                """,
                (endpoint,),
            )
            for raw_game_id, blob in rows:
                row_count += 1
                try:
                    game_id = _canonical_raw_response_game_id(
                        raw_game_id,
                        path=path,
                        endpoint=endpoint,
                    )
                    payload = _decode_raw_response_blob(blob)
                    _validate_raw_response_payload_shape(
                        payload,
                        endpoint=endpoint,
                        game_id=game_id,
                        path=path,
                    )
                except Exception as exc:  # noqa: BLE001 - report plainly in CLI output.
                    try:
                        game_id = normalize_game_id(raw_game_id)
                    except ValueError:
                        game_id = str(raw_game_id)
                    bad_rows.append(f"{game_id}: {exc}")
                    if len(bad_rows) >= 10:
                        break
            if row_count == 0:
                raise ValueError(
                    f"{path} raw_responses has no team_id IS NULL rows for endpoint {endpoint}"
                )
            if bad_rows:
                raise ValueError(
                    f"{path} raw_responses has invalid {endpoint} blobs; "
                    f"examples={bad_rows}"
                )
    except sqlite3.Error as exc:
        raise ValueError(f"{path} failed raw_responses validation: {exc}") from exc
    finally:
        conn.close()


def _canonical_raw_response_game_id(
    raw_game_id: object,
    *,
    path: Path,
    endpoint: str,
) -> str:
    raw_text = str(raw_game_id if raw_game_id is not None else "")
    if raw_text != raw_text.strip():
        raise ValueError(
            f"{path} raw_responses endpoint={endpoint} has non-canonical "
            f"game_id with surrounding whitespace: {raw_text!r}"
        )
    try:
        normalized = normalize_game_id(raw_text)
    except ValueError as exc:
        raise ValueError(
            f"{path} raw_responses endpoint={endpoint} has invalid game_id: "
            f"{raw_game_id!r}"
        ) from exc

    if raw_text != normalized:
        raise ValueError(
            f"{path} raw_responses endpoint={endpoint} has non-canonical "
            f"game_id {raw_text!r}; expected {normalized!r}"
        )
    return normalized


def _decode_raw_response_blob(blob: object) -> object:
    try:
        return json.loads(zlib.decompress(blob).decode())
    except (zlib.error, TypeError):
        if isinstance(blob, bytes):
            return json.loads(blob.decode())
        return json.loads(str(blob))


def _validate_result_set_table(
    result_set: object,
    *,
    endpoint: str,
    game_id: str,
    path: Path,
    require_nonempty_rows: bool,
) -> tuple[list[str], list[list[object]]]:
    if not isinstance(result_set, dict):
        raise ValueError(f"{path} {endpoint} {game_id} first resultSet is invalid")

    headers = result_set.get("headers")
    rowset = result_set.get("rowSet")
    if not isinstance(headers, list) or not all(
        isinstance(header, str) for header in headers
    ):
        raise ValueError(
            f"{path} {endpoint} {game_id} headers must be a list of strings"
        )
    if not isinstance(rowset, list):
        raise ValueError(f"{path} {endpoint} {game_id} rowSet must be a list")
    if require_nonempty_rows and not rowset:
        raise ValueError(f"{path} {endpoint} {game_id} rowSet is empty")

    for row_number, row in enumerate(rowset, start=1):
        if not isinstance(row, list):
            raise ValueError(
                f"{path} {endpoint} {game_id} row {row_number} is not a list"
            )
        if len(row) != len(headers):
            raise ValueError(
                f"{path} {endpoint} {game_id} row {row_number} length "
                f"{len(row)} does not match headers length {len(headers)}"
            )
    return headers, rowset


def _validate_raw_response_payload_shape(
    payload: object,
    *,
    endpoint: str,
    game_id: str,
    path: Path,
) -> None:
    if not isinstance(payload, dict):
        raise ValueError(f"{path} {endpoint} {game_id} payload is not a JSON object")

    if endpoint == "boxscore":
        result_sets = payload.get("resultSets")
        if not isinstance(result_sets, list) or not result_sets:
            raise ValueError(f"{path} boxscore {game_id} missing resultSets")
        headers, rowset = _validate_result_set_table(
            result_sets[0],
            endpoint=endpoint,
            game_id=game_id,
            path=path,
            require_nonempty_rows=True,
        )
        required_headers = set(BOXSCORE_SOURCE_COLUMNS)
        missing_headers = required_headers - set(headers)
        if missing_headers:
            raise ValueError(
                f"{path} boxscore {game_id} missing required headers: "
                f"{sorted(missing_headers)}"
            )
        header_index = {header: index for index, header in enumerate(headers)}
        has_player_row = False
        team_ids: set[int] = set()
        for row_number, row in enumerate(rowset, start=1):
            try:
                parsed_ints = {
                    field: _parse_numeric_field(
                        row[header_index[field]],
                        path=path,
                        row_number=row_number,
                        field=field,
                        integer=True,
                    )
                    for field in sorted(BOXSCORE_INT_COLUMNS)
                }
                for field in sorted(BOXSCORE_FLOAT_COLUMNS):
                    _parse_numeric_field(
                        row[header_index[field]],
                        path=path,
                        row_number=row_number,
                        field=field,
                        integer=False,
                    )
            except ValueError as exc:
                raise ValueError(f"{path} boxscore {game_id}: {exc}") from exc
            team_id = int(parsed_ints["TEAM_ID"])
            player_id = int(parsed_ints["PLAYER_ID"])
            if int(team_id) > 0 and int(player_id) > 0:
                has_player_row = True
                team_ids.add(int(team_id))
        if not has_player_row:
            raise ValueError(
                f"{path} boxscore {game_id} has no positive PLAYER_ID rows"
            )
        if len(team_ids) < 2:
            raise ValueError(
                f"{path} boxscore {game_id} has fewer than two positive TEAM_ID values"
            )
        return

    if endpoint == "summary":
        result_sets = payload.get("resultSets")
        if not isinstance(result_sets, list) or not result_sets:
            raise ValueError(f"{path} summary {game_id} missing resultSets")
        headers, rowset = _validate_result_set_table(
            result_sets[0],
            endpoint=endpoint,
            game_id=game_id,
            path=path,
            require_nonempty_rows=True,
        )
        required_headers = {"HOME_TEAM_ID", "VISITOR_TEAM_ID"}
        missing_headers = required_headers - set(headers)
        if missing_headers:
            raise ValueError(
                f"{path} summary {game_id} missing required headers: "
                f"{sorted(missing_headers)}"
            )
        header_index = {header: index for index, header in enumerate(headers)}
        first_row = rowset[0]
        if "GAME_ID" in header_index:
            row_game_id = normalize_game_id(first_row[header_index["GAME_ID"]])
            if row_game_id != game_id:
                raise ValueError(
                    f"{path} summary {game_id} GAME_ID {row_game_id!r} "
                    f"does not match raw response game_id"
                )
        parsed_team_ids: dict[str, int] = {}
        for field in sorted(required_headers):
            parsed_team_ids[field] = int(
                _parse_numeric_field(
                    first_row[header_index[field]],
                    path=path,
                    row_number=1,
                    field=field,
                    integer=True,
                )
            )
            if parsed_team_ids[field] <= 0:
                raise ValueError(
                    f"{path} summary {game_id} {field} must be positive"
                )
        if parsed_team_ids["HOME_TEAM_ID"] == parsed_team_ids["VISITOR_TEAM_ID"]:
            raise ValueError(
                f"{path} summary {game_id} HOME_TEAM_ID and VISITOR_TEAM_ID must differ"
            )
        return

    if endpoint == "pbpv3":
        game = payload.get("game")
        if not isinstance(game, dict) or not isinstance(game.get("actions"), list):
            raise ValueError(f"{path} pbpv3 {game_id} missing game.actions")


def _payload_boxscore_team_ids(payload: object) -> set[int]:
    if not isinstance(payload, dict):
        return set()
    result_sets = payload.get("resultSets")
    if not isinstance(result_sets, list) or not result_sets:
        return set()
    first = result_sets[0]
    if not isinstance(first, dict):
        return set()
    headers = first.get("headers")
    rowset = first.get("rowSet")
    if not isinstance(headers, list) or not isinstance(rowset, list):
        return set()
    if "TEAM_ID" not in headers or "PLAYER_ID" not in headers:
        return set()
    team_index = headers.index("TEAM_ID")
    player_index = headers.index("PLAYER_ID")
    team_ids: set[int] = set()
    for row in rowset:
        if not isinstance(row, list) or len(row) <= max(team_index, player_index):
            continue
        try:
            team_id = int(float(str(row[team_index]).strip()))
            player_id = int(float(str(row[player_index]).strip()))
        except ValueError:
            continue
        if team_id > 0 and player_id > 0:
            team_ids.add(team_id)
    return team_ids


def _payload_summary_team_ids(payload: object) -> set[int]:
    if not isinstance(payload, dict):
        return set()
    result_sets = payload.get("resultSets")
    if not isinstance(result_sets, list) or not result_sets:
        return set()
    first = result_sets[0]
    if not isinstance(first, dict):
        return set()
    headers = first.get("headers")
    rowset = first.get("rowSet")
    if not isinstance(headers, list) or not isinstance(rowset, list) or not rowset:
        return set()
    required = {"HOME_TEAM_ID", "VISITOR_TEAM_ID"}
    if not required.issubset(set(headers)):
        return set()
    first_row = rowset[0]
    if not isinstance(first_row, list):
        return set()
    team_ids: set[int] = set()
    for field in sorted(required):
        index = headers.index(field)
        if len(first_row) <= index:
            return set()
        try:
            team_id = int(float(str(first_row[index]).strip()))
        except ValueError:
            return set()
        team_ids.add(team_id)
    return team_ids


def _read_playbyplay_game_ids(path: Path) -> set[str]:
    schema = pq.read_schema(path)
    columns_by_upper = {str(column).upper(): str(column) for column in schema.names}
    game_id_column = columns_by_upper.get("GAME_ID")
    if game_id_column is None:
        return set()
    table = pq.read_table(path, columns=[game_id_column])
    values = table.column(game_id_column).to_pylist()
    return {normalize_game_id(value) for value in values if value is not None}


def _validate_raw_response_game_coverage(db_path: Path, parquet_path: Path) -> None:
    if not db_path.exists() or not parquet_path.exists():
        return
    game_ids = _read_playbyplay_game_ids(parquet_path)
    if not game_ids:
        return
    conn = sqlite3.connect(db_path)
    try:
        validated_payloads: dict[str, dict[str, object]] = defaultdict(dict)
        for endpoint in sorted(REQUIRED_RAW_RESPONSE_ENDPOINTS):
            rows_by_game_id: dict[str, list[object]] = defaultdict(list)
            for raw_game_id, blob in conn.execute(
                """
                SELECT game_id, data
                FROM raw_responses
                WHERE endpoint=? AND team_id IS NULL
                """,
                (endpoint,),
            ):
                game_id = _canonical_raw_response_game_id(
                    raw_game_id,
                    path=db_path,
                    endpoint=endpoint,
                )
                rows_by_game_id[game_id].append(blob)

            missing_game_ids = sorted(game_ids - set(rows_by_game_id))
            if missing_game_ids:
                raise ValueError(
                    f"{db_path} missing {endpoint} responses for "
                    f"{len(missing_game_ids)} playbyplayv2 games; "
                    f"examples={missing_game_ids[:10]}"
                )
            duplicate_game_ids = sorted(
                game_id
                for game_id in game_ids
                if len(rows_by_game_id.get(game_id, [])) > 1
            )
            if duplicate_game_ids:
                raise ValueError(
                    f"{db_path} has duplicate team_id IS NULL {endpoint} "
                    f"responses; examples={duplicate_game_ids[:10]}"
                )
            bad_rows: list[str] = []
            for game_id in sorted(game_ids):
                try:
                    payload = _decode_raw_response_blob(rows_by_game_id[game_id][0])
                    _validate_raw_response_payload_shape(
                        payload,
                        endpoint=endpoint,
                        game_id=game_id,
                        path=db_path,
                    )
                    validated_payloads[endpoint][game_id] = payload
                except Exception as exc:  # noqa: BLE001 - report plainly in CLI output.
                    bad_rows.append(f"{game_id}: {exc}")
                    if len(bad_rows) >= 10:
                        break
            if bad_rows:
                raise ValueError(
                    f"{db_path} has invalid {endpoint} blobs for playbyplayv2 "
                    f"games; examples={bad_rows}"
                )
        team_mismatches: list[str] = []
        for game_id in sorted(game_ids):
            summary_team_ids = _payload_summary_team_ids(
                validated_payloads.get("summary", {}).get(game_id)
            )
            boxscore_team_ids = _payload_boxscore_team_ids(
                validated_payloads.get("boxscore", {}).get(game_id)
            )
            if summary_team_ids and boxscore_team_ids and summary_team_ids != boxscore_team_ids:
                team_mismatches.append(
                    f"{game_id}: summary teams {sorted(summary_team_ids)} "
                    f"do not equal boxscore teams {sorted(boxscore_team_ids)}"
                )
                if len(team_mismatches) >= 10:
                    break
        if team_mismatches:
            raise ValueError(
                f"{db_path} summary team ids do not match boxscore team ids; "
                f"examples={team_mismatches}"
            )
    finally:
        conn.close()


def _validate_playbyplay_v2_parquet(path: Path) -> None:
    if not path.exists():
        return
    try:
        schema = pq.read_schema(path)
    except Exception as exc:  # noqa: BLE001 - report plainly in CLI output.
        raise ValueError(f"{path} is not a readable parquet file: {exc}") from exc
    observed_columns = {str(column).upper() for column in schema.names}
    missing_columns = PLAYBYPLAY_V2_REQUIRED_COLUMNS - observed_columns
    if missing_columns:
        raise ValueError(
            f"{path} missing required playbyplayv2 columns: {sorted(missing_columns)}"
        )
    columns_by_upper = {str(column).upper(): str(column) for column in schema.names}
    table = pq.read_table(
        path,
        columns=[
            columns_by_upper["GAME_ID"],
            columns_by_upper["SEASON"],
            *[
                columns_by_upper[column]
                for column in sorted(PLAYBYPLAY_V2_INTEGER_COLUMNS - {"SEASON"})
            ],
        ],
    )
    if table.num_rows == 0:
        raise ValueError(f"{path} contains no playbyplayv2 rows")
    game_ids = table.column(columns_by_upper["GAME_ID"]).to_pylist()
    table_columns = table.to_pydict()
    seasons = table_columns[columns_by_upper["SEASON"]]
    if not any(value is not None for value in seasons):
        raise ValueError(f"{path} contains no non-null SEASON values")
    bad_rows: list[str] = []
    for row_number, value in enumerate(game_ids, start=1):
        if value is None:
            continue
        try:
            game_id = normalize_game_id(value)
        except ValueError as exc:
            bad_rows.append(f"row {row_number} GAME_ID {value!r}: {exc}")
            if len(bad_rows) >= 10:
                break
            continue
        try:
            parsed_season = _parse_parquet_integer_field(
                seasons[row_number - 1],
                path=path,
                row_number=row_number,
                field="SEASON",
            )
            expected_season = _season_from_game_id(game_id)
            if parsed_season != expected_season:
                raise ValueError(
                    f"{path} row {row_number} SEASON {parsed_season} "
                    f"does not match GAME_ID-derived season {expected_season}"
                )
            for field in sorted(PLAYBYPLAY_V2_INTEGER_COLUMNS - {"SEASON"}):
                _parse_parquet_integer_field(
                    table_columns[columns_by_upper[field]][row_number - 1],
                    path=path,
                    row_number=row_number,
                    field=field,
                )
        except ValueError as exc:
            bad_rows.append(str(exc))
            if len(bad_rows) >= 10:
                break
    if bad_rows:
        raise ValueError(
            f"{path} contains invalid playbyplayv2 rows; examples={bad_rows}"
        )


def _validate_period_starters_parquet(path: Path) -> None:
    if not path.exists():
        return
    try:
        schema = pq.read_schema(path)
    except Exception as exc:  # noqa: BLE001 - report plainly in CLI output.
        raise ValueError(f"{path} is not a readable parquet file: {exc}") from exc
    missing_columns = set(STARTER_LOOKUP_COLUMNS) - set(schema.names)
    if missing_columns:
        raise ValueError(
            f"{path} missing required period starter columns: {sorted(missing_columns)}"
        )
    table = pq.read_table(path, columns=STARTER_LOOKUP_COLUMNS)
    if table.num_rows == 0:
        raise ValueError(f"{path} contains no period starter rows")
    df = table.to_pandas()
    resolved = df[df["resolved"] == True].copy()  # noqa: E712
    if resolved.empty:
        raise ValueError(f"{path} contains no resolved period starter rows")

    int_fields = [
        "period",
        "away_team_id",
        "home_team_id",
        *[f"away_player{i}" for i in range(1, 6)],
        *[f"home_player{i}" for i in range(1, 6)],
    ]
    seen_starter_keys: set[tuple[str, int]] = set()
    for row_number, row in enumerate(resolved.to_dict(orient="records"), start=2):
        try:
            game_id = normalize_game_id(row.get("game_id"))
        except ValueError as exc:
            raise ValueError(
                f"{path} row {row_number} has invalid game_id: {row.get('game_id')!r}"
            ) from exc
        parsed_values: dict[str, int] = {}
        for field in int_fields:
            value = _parse_numeric_field(
                row.get(field),
                path=path,
                row_number=row_number,
                field=field,
                integer=True,
            )
            if int(value) <= 0:
                raise ValueError(
                    f"{path} row {row_number} {field} must be positive"
                )
            parsed_values[field] = int(value)
        key = (game_id, parsed_values["period"])
        if key in seen_starter_keys:
            raise ValueError(f"{path} row {row_number} duplicates starter key {key}")
        seen_starter_keys.add(key)

        for side in ("away", "home"):
            players = [parsed_values[f"{side}_player{i}"] for i in range(1, 6)]
            if len(set(players)) != 5:
                raise ValueError(
                    f"{path} row {row_number} has duplicate {side} starter ids"
                )


def _validate_core_nba_inputs(root: Path) -> list[str]:
    errors: list[str] = []
    validators = [
        (root / "data" / "nba_raw.db", _validate_nba_raw_db),
        (root / "data" / "playbyplayv2.parq", _validate_playbyplay_v2_parquet),
        (
            root / "data" / "period_starters_v6.parquet",
            _validate_period_starters_parquet,
        ),
        (
            root / "data" / "period_starters_v5.parquet",
            _validate_period_starters_parquet,
        ),
    ]
    for path, validator in validators:
        if not path.exists():
            continue
        try:
            validator(path)
        except Exception as exc:  # noqa: BLE001 - report plainly in CLI output.
            errors.append(str(exc))
    if (root / "data" / "nba_raw.db").exists() and (
        root / "data" / "playbyplayv2.parq"
    ).exists():
        try:
            _validate_raw_response_game_coverage(
                root / "data" / "nba_raw.db",
                root / "data" / "playbyplayv2.parq",
            )
        except Exception as exc:  # noqa: BLE001 - report plainly in CLI output.
            errors.append(str(exc))
    return errors


def validate_core_runtime_data_inputs(
    *,
    db_path: Path,
    parquet_path: Path,
    period_starter_parquet_paths: Iterable[Path],
) -> None:
    errors: list[str] = []
    validators = [
        (Path(db_path), _validate_nba_raw_db),
        (Path(parquet_path), _validate_playbyplay_v2_parquet),
        *[
            (Path(path), _validate_period_starters_parquet)
            for path in period_starter_parquet_paths
        ],
    ]
    for path, validator in validators:
        try:
            validator(path)
        except Exception as exc:  # noqa: BLE001 - report all input failures together.
            errors.append(str(exc))
    try:
        _validate_raw_response_game_coverage(Path(db_path), Path(parquet_path))
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    if errors:
        raise ValueError("; ".join(errors))


def _validate_core_catalogs(root: Path) -> list[str]:
    validators = [
        (
            root / "catalogs" / "pbp_row_overrides.csv",
            lambda path: validate_historic_pbp_row_override_catalog(path),
        ),
        (
            root / "catalogs" / "pbp_stat_overrides.csv",
            lambda path: load_pbp_stat_overrides(path, strict=True),
        ),
        (
            root / "catalogs" / "validation_overrides.csv",
            validate_validation_overrides,
        ),
        (
            root / "catalogs" / "boxscore_source_overrides.csv",
            lambda path: (
                _validate_csv_columns(path, set(BOXSCORE_SOURCE_OVERRIDE_COLUMNS)),
                validate_boxscore_source_overrides(path),
            ),
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
