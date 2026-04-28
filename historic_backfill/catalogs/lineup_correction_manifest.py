from __future__ import annotations

import csv
import json
import sqlite3
import zlib
from pathlib import Path
from typing import Any

from historic_backfill.common.game_context import _load_game_context
from historic_backfill.common.lineups import _collect_game_events, _normalize_lineups


ROOT = Path(__file__).resolve().parents[1]
CATALOGS_ROOT = ROOT / "catalogs"
DATA_ROOT = ROOT / "data"
DEFAULT_OVERRIDES_DIR = CATALOGS_ROOT / "overrides"
DEFAULT_MANIFEST_PATH = DEFAULT_OVERRIDES_DIR / "correction_manifest.json"
DEFAULT_DB_PATH = DATA_ROOT / "nba_raw.db"
DEFAULT_PARQUET_PATH = DATA_ROOT / "playbyplayv2.parq"
DEFAULT_FILE_DIRECTORY = DATA_ROOT

PERIOD_STARTERS_JSON = "period_starters_overrides.json"
PERIOD_STARTERS_NOTES_CSV = "period_starters_override_notes.csv"
LINEUP_WINDOWS_JSON = "lineup_window_overrides.json"
LINEUP_WINDOWS_NOTES_CSV = "lineup_window_override_notes.csv"

COMPILE_SUMMARY_JSON = "correction_manifest_compile_summary.json"

CORRECTION_STATUS_VALUES = {"proposed", "active", "retired", "rejected"}
CORRECTION_SCOPE_VALUES = {"period_start", "window", "event"}
AUTHORING_MODE_VALUES = {"explicit", "delta"}
SOURCE_VALUES = {
    "raw_pbp",
    "v6",
    "v5",
    "bbr",
    "tpdev",
    "boxscore_start_position",
    "period_boxscore_v3",
    "manual_trace",
    "unknown",
}
CONFIDENCE_VALUES = {"low", "medium", "high", "legacy"}
RESIDUAL_CLASS_VALUES = {
    "fixable_lineup_defect",
    "candidate_systematic_defect",
    "source_limited_upstream_error",
    "candidate_boundary_difference",
    "boundary_difference",
    "unknown",
}
RESIDUAL_STATUS_VALUES = {"proposed", "accepted", "rejected", "needs_review"}


class ManifestValidationError(ValueError):
    pass


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _write_csv_rows(
    path: Path, fieldnames: list[str], rows: list[dict[str, Any]]
) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _correction_sort_key(correction: dict[str, Any]) -> tuple[Any, ...]:
    scope_rank = {"period_start": 0, "window": 1, "event": 2}
    return (
        str(correction.get("game_id") or ""),
        int(correction.get("period") or 0),
        int(correction.get("team_id") or 0),
        scope_rank.get(str(correction.get("scope_type") or ""), 99),
        int(correction.get("start_event_num") or 0),
        int(correction.get("end_event_num") or 0),
        str(correction.get("correction_id") or ""),
    )


def _normalize_runtime_game_id(value: str | int) -> str:
    return str(int(value)).zfill(10)


def _normalize_source(value: str | None) -> str:
    raw = (value or "").strip().lower()
    if raw in {
        "v5",
        "v6",
        "bbr",
        "tpdev",
        "raw_pbp",
        "boxscore_start_position",
        "period_boxscore_v3",
    }:
        return raw
    if raw in {"manual_local_review", "manual_trace"}:
        return "manual_trace"
    return "unknown"


def _confidence_from_scores(
    local_confidence_score: str | None,
    external_alignment_score: str | None = None,
) -> str:
    for candidate in [local_confidence_score, external_alignment_score]:
        if candidate in (None, ""):
            continue
        try:
            value = float(candidate)
        except (TypeError, ValueError):
            continue
        if value >= 0.85:
            return "high"
        if value >= 0.60:
            return "medium"
        return "low"
    return "legacy"


def _starter_correction_id(game_id: str, period: str, team_id: str) -> str:
    return f"starter__{game_id}__p{period}__t{team_id}"


def _window_correction_id(
    game_id: str,
    period: str,
    team_id: str,
    start_event_num: str,
    end_event_num: str,
) -> str:
    return (
        f"window__{game_id}__p{period}__t{team_id}__e{start_event_num}_{end_event_num}"
    )


def _build_episode_id(
    game_id: str, scope_type: str, period: str, team_id: str, reason_code: str
) -> str:
    safe_reason = (reason_code or "unspecified").replace(" ", "_")
    return f"{scope_type}__{game_id}__p{period}__t{team_id}__{safe_reason}"


def seed_manifest_from_runtime(
    *,
    overrides_dir: Path = DEFAULT_OVERRIDES_DIR,
    manifest_path: Path | None = None,
) -> dict[str, Any]:
    overrides_dir = overrides_dir.resolve()
    starter_overrides = _read_json(overrides_dir / PERIOD_STARTERS_JSON)
    starter_notes = _read_csv_rows(overrides_dir / PERIOD_STARTERS_NOTES_CSV)
    window_overrides = _read_json(overrides_dir / LINEUP_WINDOWS_JSON)
    window_notes = _read_csv_rows(overrides_dir / LINEUP_WINDOWS_NOTES_CSV)

    corrections: list[dict[str, Any]] = []

    for row in starter_notes:
        game_id = row["game_id"]
        period = row["period"]
        team_id = row["team_id"]
        lineup = starter_overrides[game_id][period][team_id]
        preferred_source = _normalize_source(row.get("preferred_source"))
        correction_id = _starter_correction_id(game_id, period, team_id)
        corrections.append(
            {
                "correction_id": correction_id,
                "episode_id": _build_episode_id(
                    game_id, "period_start", period, team_id, row.get("reason", "")
                ),
                "status": "active",
                "domain": "lineup",
                "scope_type": "period_start",
                "authoring_mode": "explicit",
                "game_id": game_id,
                "period": int(period),
                "team_id": int(team_id),
                "lineup_player_ids": [int(player_id) for player_id in lineup],
                "source_type": row.get("source_type", ""),
                "reason_code": row.get("reason", ""),
                "evidence_summary": row.get("evidence_summary", ""),
                "source_primary": preferred_source,
                "source_secondary": (
                    "raw_pbp" if preferred_source != "raw_pbp" else "unknown"
                ),
                "preferred_source": preferred_source,
                "confidence": "legacy",
                "validation_artifacts": [],
                "supersedes": [],
                "date_added": row.get("date_added", ""),
                "notes": row.get("notes", ""),
            }
        )

    window_lookup = {
        (
            game_id,
            str(window["period"]),
            str(window["team_id"]),
            str(window["start_event_num"]),
            str(window["end_event_num"]),
        ): window
        for game_id, windows in window_overrides.items()
        for window in windows
    }
    for row in window_notes:
        key = (
            row["game_id"],
            row["period"],
            row["team_id"],
            row["start_event_num"],
            row["end_event_num"],
        )
        window = window_lookup[key]
        correction_id = _window_correction_id(*key)
        scope_type = (
            "event" if row["start_event_num"] == row["end_event_num"] else "window"
        )
        confidence = _confidence_from_scores(
            row.get("local_confidence_score"),
            row.get("external_alignment_score"),
        )
        corrections.append(
            {
                "correction_id": correction_id,
                "episode_id": _build_episode_id(
                    row["game_id"],
                    scope_type,
                    row["period"],
                    row["team_id"],
                    row.get("reason", ""),
                ),
                "status": "active",
                "domain": "lineup",
                "scope_type": scope_type,
                "authoring_mode": "explicit",
                "game_id": row["game_id"],
                "period": int(row["period"]),
                "team_id": int(row["team_id"]),
                "start_event_num": int(row["start_event_num"]),
                "end_event_num": int(row["end_event_num"]),
                "lineup_player_ids": [
                    int(player_id) for player_id in window["lineup_player_ids"]
                ],
                "source_type": row.get("source_type", ""),
                "reason_code": row.get("reason", ""),
                "evidence_summary": row.get("evidence_summary", ""),
                "source_primary": "manual_trace",
                "source_secondary": _normalize_source(row.get("source_type")),
                "preferred_source": "manual_trace",
                "confidence": confidence,
                "local_confidence_score": row.get("local_confidence_score", ""),
                "external_alignment_score": row.get("external_alignment_score", ""),
                "validation_artifacts": [],
                "supersedes": [],
                "date_added": row.get("date_added", ""),
                "notes": row.get("notes", ""),
            }
        )

    manifest = {
        "manifest_version": "20260321_v1",
        "description": "Canonical lineup correction manifest seeded from live runtime override artifacts.",
        "corrections": corrections,
        "residual_annotations": [],
    }
    if manifest_path is not None:
        write_manifest(manifest, manifest_path)
    return manifest


def load_manifest(path: Path = DEFAULT_MANIFEST_PATH) -> dict[str, Any]:
    return _read_json(path.resolve())


def write_manifest(
    manifest: dict[str, Any], path: Path = DEFAULT_MANIFEST_PATH
) -> None:
    _write_json(path.resolve(), manifest)


def validate_manifest_schema(path: Path = DEFAULT_MANIFEST_PATH) -> None:
    """Validate correction manifest structure without loading NBA runtime data."""
    manifest = load_manifest(path)
    build_explicit_runtime_views(manifest)
    _validate_residual_annotations(manifest)


def build_explicit_runtime_views(
    manifest: dict[str, Any],
) -> tuple[dict[str, dict[str, dict[str, list[int]]]], dict[str, list[dict[str, Any]]]]:
    """Build runtime override JSON views from explicit active corrections only."""
    seen_ids: set[str] = set()
    seen_period_keys: set[tuple[str, int, int]] = set()
    seen_window_ranges: dict[tuple[str, int, int], list[tuple[int, int, str]]] = {}
    period_overrides: dict[str, dict[str, dict[str, list[int]]]] = {}
    lineup_windows: dict[str, list[dict[str, Any]]] = {}

    for correction in sorted(manifest.get("corrections", []), key=_correction_sort_key):
        _validate_correction_record(correction, seen_ids)
        if correction.get("status") != "active":
            continue

        correction_id = str(correction["correction_id"])
        game_id = str(correction["game_id"])
        period = int(correction["period"])
        team_id = int(correction["team_id"])
        scope_type = str(correction["scope_type"])

        if correction.get("authoring_mode") != "explicit":
            raise ManifestValidationError(
                f"Correction {correction_id} requires NBA data to compile; "
                "core preflight only supports explicit active corrections"
            )
        lineup = correction.get("lineup_player_ids")
        if not isinstance(lineup, list):
            raise ManifestValidationError(
                f"Correction {correction_id} is explicit but missing lineup_player_ids"
            )
        try:
            parsed_lineup = [int(player_id) for player_id in lineup]
        except (TypeError, ValueError) as exc:
            raise ManifestValidationError(
                f"Correction {correction_id} has non-integer lineup_player_ids"
            ) from exc
        _validate_lineup_shape(parsed_lineup, correction_id)

        if scope_type == "period_start":
            period_key = (game_id, period, team_id)
            if period_key in seen_period_keys:
                raise ManifestValidationError(
                    f"Duplicate active period_start correction for {game_id} P{period} T{team_id}"
                )
            seen_period_keys.add(period_key)
            period_overrides.setdefault(game_id, {}).setdefault(str(period), {})[
                str(team_id)
            ] = parsed_lineup
            continue

        if scope_type in {"window", "event"}:
            start_event_num = int(correction["start_event_num"])
            end_event_num = int(correction["end_event_num"])
            if end_event_num < start_event_num:
                raise ManifestValidationError(
                    f"Correction {correction_id} has end_event_num < start_event_num"
                )
            range_key = (game_id, period, team_id)
            for prior_start, prior_end, prior_id in seen_window_ranges.get(
                range_key, []
            ):
                if max(prior_start, start_event_num) <= min(prior_end, end_event_num):
                    raise ManifestValidationError(
                        f"Correction {correction_id} overlaps active correction {prior_id} "
                        f"for {game_id} P{period} T{team_id}"
                    )
            seen_window_ranges.setdefault(range_key, []).append(
                (start_event_num, end_event_num, correction_id)
            )
            lineup_windows.setdefault(game_id, []).append(
                {
                    "period": period,
                    "team_id": team_id,
                    "start_event_num": start_event_num,
                    "end_event_num": end_event_num,
                    "lineup_player_ids": parsed_lineup,
                }
            )

    period_overrides = {
        game_id: {
            period: dict(sorted(team_map.items(), key=lambda item: int(item[0])))
            for period, team_map in sorted(
                period_map.items(), key=lambda item: int(item[0])
            )
        }
        for game_id, period_map in sorted(period_overrides.items())
    }
    lineup_windows = {
        game_id: sorted(
            windows,
            key=lambda row: (
                int(row["period"]),
                int(row["team_id"]),
                int(row["start_event_num"]),
                int(row["end_event_num"]),
            ),
        )
        for game_id, windows in sorted(lineup_windows.items())
    }
    return period_overrides, lineup_windows


def validate_compiled_runtime_views(
    manifest_path: Path = DEFAULT_MANIFEST_PATH,
    overrides_dir: Path = DEFAULT_OVERRIDES_DIR,
) -> None:
    """Assert committed runtime override JSONs match the manifest."""
    manifest = load_manifest(manifest_path)
    expected_period_overrides, expected_lineup_windows = build_explicit_runtime_views(
        manifest
    )
    _validate_residual_annotations(manifest)

    overrides_dir = overrides_dir.resolve()
    observed_period_overrides = _read_json(overrides_dir / PERIOD_STARTERS_JSON)
    observed_lineup_windows = _read_json(overrides_dir / LINEUP_WINDOWS_JSON)
    if observed_period_overrides != expected_period_overrides:
        raise ManifestValidationError(
            f"{PERIOD_STARTERS_JSON} does not match active explicit corrections in {manifest_path}"
        )
    if observed_lineup_windows != expected_lineup_windows:
        raise ManifestValidationError(
            f"{LINEUP_WINDOWS_JSON} does not match active explicit corrections in {manifest_path}"
        )


def _load_raw_boxscore_response(db_path: Path, game_id: str) -> dict[str, Any]:
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT data FROM raw_responses WHERE game_id=? AND endpoint='boxscore' AND team_id IS NULL",
            (_normalize_runtime_game_id(game_id),),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        raise ManifestValidationError(f"No boxscore response found for game {game_id}")
    blob = row[0]
    try:
        return json.loads(zlib.decompress(blob).decode())
    except (zlib.error, TypeError):
        if isinstance(blob, bytes):
            return json.loads(blob.decode())
        return json.loads(blob)


def _load_game_rosters(
    game_id: str, db_path: Path, cache: dict[str, dict[int, set[int]]]
) -> dict[int, set[int]]:
    normalized = _normalize_runtime_game_id(game_id)
    if normalized in cache:
        return cache[normalized]
    response = _load_raw_boxscore_response(db_path, normalized)
    player_stats = None
    for result_set in response.get("resultSets", []):
        if result_set.get("name") == "PlayerStats":
            player_stats = result_set
            break
    if player_stats is None:
        raise ManifestValidationError(
            f"PlayerStats missing from boxscore response for {game_id}"
        )
    headers = player_stats.get("headers") or []
    rowset = player_stats.get("rowSet") or []
    try:
        team_id_index = headers.index("TEAM_ID")
        player_id_index = headers.index("PLAYER_ID")
    except ValueError as exc:
        raise ManifestValidationError(
            f"Boxscore PlayerStats headers missing TEAM_ID/PLAYER_ID for {game_id}"
        ) from exc
    rosters: dict[int, set[int]] = {}
    for row in rowset:
        team_id = int(row[team_id_index])
        player_id = int(row[player_id_index])
        rosters.setdefault(team_id, set()).add(player_id)
    cache[normalized] = rosters
    return rosters


def _event_index_lookup(events: list[object]) -> dict[tuple[int, int], list[int]]:
    lookup: dict[tuple[int, int], list[int]] = {}
    for index, event in enumerate(events):
        try:
            key = (int(getattr(event, "period")), int(getattr(event, "event_num")))
        except (AttributeError, TypeError, ValueError):
            continue
        lookup.setdefault(key, []).append(index)
    return lookup


def _lineup_at_event(events: list[object], index: int, team_id: int) -> list[int]:
    if index < 0 or index >= len(events):
        return []
    current = _normalize_lineups(getattr(events[index], "current_players", {})).get(
        int(team_id), []
    )
    if current:
        return list(current)
    previous_event = getattr(events[index], "previous_event", None)
    if previous_event is None:
        return []
    return list(
        _normalize_lineups(getattr(previous_event, "current_players", {})).get(
            int(team_id), []
        )
    )


def _resolve_base_lineup(
    correction: dict[str, Any],
    *,
    parquet_path: Path,
    db_path: Path,
    file_directory: Path,
    game_cache: dict[str, tuple[list[object], dict[tuple[int, int], list[int]]]],
) -> list[int]:
    runtime_game_id = _normalize_runtime_game_id(correction["game_id"])
    if runtime_game_id not in game_cache:
        _, possessions, _ = _load_game_context(
            runtime_game_id,
            parquet_path=parquet_path.resolve(),
            db_path=db_path.resolve(),
            file_directory=file_directory.resolve(),
        )
        events = _collect_game_events(possessions)
        game_cache[runtime_game_id] = (events, _event_index_lookup(events))
    events, lookup = game_cache[runtime_game_id]
    period = int(correction["period"])
    team_id = int(correction["team_id"])
    scope_type = str(correction["scope_type"])

    if scope_type == "period_start":
        for index, event in enumerate(events):
            try:
                event_period = int(getattr(event, "period"))
            except (AttributeError, TypeError, ValueError):
                continue
            if event_period != period:
                continue
            lineup = _lineup_at_event(events, index, team_id)
            if lineup:
                return lineup
        raise ManifestValidationError(
            f"Could not resolve inferred period-start lineup for {correction['game_id']} P{period} T{team_id}"
        )

    start_event_num = int(correction["start_event_num"])
    indices = lookup.get((period, start_event_num), [])
    if not indices:
        raise ManifestValidationError(
            f"Could not resolve start event {start_event_num} for {correction['game_id']} P{period}"
        )
    lineup = _lineup_at_event(events, indices[0], team_id)
    if not lineup:
        raise ManifestValidationError(
            f"Could not resolve inferred lineup at event {start_event_num} for {correction['game_id']} P{period} T{team_id}"
        )
    return lineup


def _resolve_lineup_for_correction(
    correction: dict[str, Any],
    *,
    parquet_path: Path,
    db_path: Path,
    file_directory: Path,
    game_cache: dict[str, tuple[list[object], dict[tuple[int, int], list[int]]]],
) -> list[int]:
    authoring_mode = str(correction.get("authoring_mode") or "")
    if authoring_mode not in AUTHORING_MODE_VALUES:
        raise ManifestValidationError(
            f"Correction {correction.get('correction_id')} has invalid authoring_mode {authoring_mode}"
        )
    if authoring_mode == "explicit":
        lineup = correction.get("lineup_player_ids")
        if not isinstance(lineup, list):
            raise ManifestValidationError(
                f"Correction {correction.get('correction_id')} is explicit but missing lineup_player_ids"
            )
        try:
            return [int(player_id) for player_id in lineup]
        except (TypeError, ValueError) as exc:
            raise ManifestValidationError(
                f"Correction {correction.get('correction_id')} has non-integer lineup_player_ids"
            ) from exc

    base_lineup = _resolve_base_lineup(
        correction,
        parquet_path=parquet_path,
        db_path=db_path,
        file_directory=file_directory,
        game_cache=game_cache,
    )
    try:
        swap_out = int(correction["swap_out_player_id"])
        swap_in = int(correction["swap_in_player_id"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ManifestValidationError(
            f"Delta correction {correction.get('correction_id')} is missing swap_out_player_id/swap_in_player_id"
        ) from exc
    if swap_out not in base_lineup:
        raise ManifestValidationError(
            f"Delta correction {correction.get('correction_id')} swap_out {swap_out} not in base lineup {base_lineup}"
        )
    if swap_in in base_lineup:
        raise ManifestValidationError(
            f"Delta correction {correction.get('correction_id')} swap_in {swap_in} already present in base lineup {base_lineup}"
        )
    return [
        swap_in if player_id == swap_out else int(player_id)
        for player_id in base_lineup
    ]


def _validate_lineup_shape(lineup: list[int], correction_id: str) -> None:
    if len(lineup) != 5:
        raise ManifestValidationError(
            f"Correction {correction_id} does not resolve to 5 players: {lineup}"
        )
    if len(set(lineup)) != 5:
        raise ManifestValidationError(
            f"Correction {correction_id} does not resolve to 5 unique players: {lineup}"
        )


def _validate_correction_record(correction: dict[str, Any], seen_ids: set[str]) -> None:
    correction_id = str(correction.get("correction_id") or "").strip()
    if not correction_id:
        raise ManifestValidationError("Every correction must have a correction_id")
    if correction_id in seen_ids:
        raise ManifestValidationError(f"Duplicate correction_id: {correction_id}")
    seen_ids.add(correction_id)

    status = str(correction.get("status") or "")
    if status not in CORRECTION_STATUS_VALUES:
        raise ManifestValidationError(
            f"Correction {correction_id} has invalid status {status}"
        )
    if status != "active":
        return

    domain = str(correction.get("domain") or "")
    if domain != "lineup":
        raise ManifestValidationError(
            f"v1 compiler only supports active lineup corrections; {correction_id} has domain {domain}"
        )
    scope_type = str(correction.get("scope_type") or "")
    if scope_type not in CORRECTION_SCOPE_VALUES:
        raise ManifestValidationError(
            f"Correction {correction_id} has invalid scope_type {scope_type}"
        )
    confidence = str(correction.get("confidence") or "")
    if confidence and confidence not in CONFIDENCE_VALUES:
        raise ManifestValidationError(
            f"Correction {correction_id} has invalid confidence {confidence}"
        )
    for field in [
        "game_id",
        "period",
        "team_id",
        "authoring_mode",
        "reason_code",
        "evidence_summary",
        "source_primary",
        "preferred_source",
    ]:
        if correction.get(field) in (None, ""):
            raise ManifestValidationError(
                f"Correction {correction_id} is missing required field {field}"
            )
    authoring_mode = str(correction.get("authoring_mode") or "")
    if authoring_mode not in AUTHORING_MODE_VALUES:
        raise ManifestValidationError(
            f"Correction {correction_id} has invalid authoring_mode {authoring_mode}"
        )
    if authoring_mode == "delta":
        for player_field in ("swap_out_player_id", "swap_in_player_id"):
            try:
                int(correction[player_field])
            except (KeyError, TypeError, ValueError) as exc:
                raise ManifestValidationError(
                    f"Delta correction {correction_id} is missing or has invalid {player_field}"
                ) from exc
    for source_field in ["source_primary", "source_secondary", "preferred_source"]:
        source_value = correction.get(source_field)
        if source_value in (None, ""):
            continue
        if source_value not in SOURCE_VALUES:
            raise ManifestValidationError(
                f"Correction {correction_id} has invalid {source_field}={source_value}"
            )
    if scope_type in {"window", "event"}:
        if "start_event_num" not in correction or "end_event_num" not in correction:
            raise ManifestValidationError(
                f"Correction {correction_id} requires start_event_num/end_event_num"
            )


def _validate_residual_annotations(manifest: dict[str, Any]) -> None:
    seen_ids: set[str] = set()
    for annotation in manifest.get("residual_annotations", []):
        annotation_id = str(annotation.get("annotation_id") or "").strip()
        if not annotation_id:
            raise ManifestValidationError(
                "Every residual annotation must have an annotation_id"
            )
        if annotation_id in seen_ids:
            raise ManifestValidationError(f"Duplicate annotation_id: {annotation_id}")
        seen_ids.add(annotation_id)
        residual_class = str(annotation.get("residual_class") or "")
        if residual_class not in RESIDUAL_CLASS_VALUES:
            raise ManifestValidationError(
                f"Residual annotation {annotation_id} has invalid residual_class {residual_class}"
            )
        status = str(annotation.get("status") or "")
        if status not in RESIDUAL_STATUS_VALUES:
            raise ManifestValidationError(
                f"Residual annotation {annotation_id} has invalid status {status}"
            )


def compile_runtime_views(
    manifest: dict[str, Any],
    *,
    output_dir: Path = DEFAULT_OVERRIDES_DIR,
    db_path: Path = DEFAULT_DB_PATH,
    parquet_path: Path = DEFAULT_PARQUET_PATH,
    file_directory: Path = DEFAULT_FILE_DIRECTORY,
) -> dict[str, Any]:
    output_dir = output_dir.resolve()
    seen_ids: set[str] = set()
    for correction in sorted(manifest.get("corrections", []), key=_correction_sort_key):
        _validate_correction_record(correction, seen_ids)
    _validate_residual_annotations(manifest)

    roster_cache: dict[str, dict[int, set[int]]] = {}
    game_cache: dict[str, tuple[list[object], dict[tuple[int, int], list[int]]]] = {}
    period_overrides: dict[str, dict[str, dict[str, list[int]]]] = {}
    lineup_windows: dict[str, list[dict[str, Any]]] = {}
    starter_rows: list[dict[str, Any]] = []
    window_rows: list[dict[str, Any]] = []
    seen_period_keys: set[tuple[str, int, int]] = set()
    seen_window_ranges: dict[tuple[str, int, int], list[tuple[int, int, str]]] = {}

    for correction in sorted(manifest.get("corrections", []), key=_correction_sort_key):
        if correction.get("status") != "active":
            continue

        correction_id = str(correction["correction_id"])
        game_id = str(correction["game_id"])
        period = int(correction["period"])
        team_id = int(correction["team_id"])
        lineup = _resolve_lineup_for_correction(
            correction,
            parquet_path=parquet_path,
            db_path=db_path,
            file_directory=file_directory,
            game_cache=game_cache,
        )
        _validate_lineup_shape(lineup, correction_id)

        rosters = _load_game_rosters(game_id, db_path, roster_cache)
        if team_id not in rosters:
            raise ManifestValidationError(
                f"Correction {correction_id} uses team_id {team_id} that is not in game {game_id}"
            )
        invalid_players = [
            player_id for player_id in lineup if player_id not in rosters[team_id]
        ]
        if invalid_players:
            raise ManifestValidationError(
                f"Correction {correction_id} includes players not on team {team_id} roster for game {game_id}: {invalid_players}"
            )

        scope_type = str(correction["scope_type"])
        if scope_type == "period_start":
            period_key = (game_id, period, team_id)
            if period_key in seen_period_keys:
                raise ManifestValidationError(
                    f"Duplicate active period_start correction for {game_id} P{period} T{team_id}"
                )
            seen_period_keys.add(period_key)
            period_overrides.setdefault(game_id, {}).setdefault(str(period), {})[
                str(team_id)
            ] = lineup
            starter_rows.append(
                {
                    "game_id": game_id,
                    "period": period,
                    "team_id": team_id,
                    "source_type": correction.get("source_type", ""),
                    "reason": correction.get("reason_code", ""),
                    "evidence_summary": correction.get("evidence_summary", ""),
                    "preferred_source": correction.get("preferred_source", ""),
                    "date_added": correction.get("date_added", ""),
                    "notes": correction.get("notes", ""),
                }
            )
            continue

        start_event_num = int(correction["start_event_num"])
        end_event_num = int(correction["end_event_num"])
        if end_event_num < start_event_num:
            raise ManifestValidationError(
                f"Correction {correction_id} has end_event_num < start_event_num"
            )
        range_key = (game_id, period, team_id)
        for prior_start, prior_end, prior_id in seen_window_ranges.get(range_key, []):
            if max(prior_start, start_event_num) <= min(prior_end, end_event_num):
                raise ManifestValidationError(
                    f"Correction {correction_id} overlaps active correction {prior_id} for {game_id} P{period} T{team_id}"
                )
        seen_window_ranges.setdefault(range_key, []).append(
            (start_event_num, end_event_num, correction_id)
        )
        lineup_windows.setdefault(game_id, []).append(
            {
                "period": period,
                "team_id": team_id,
                "start_event_num": start_event_num,
                "end_event_num": end_event_num,
                "lineup_player_ids": lineup,
            }
        )
        window_rows.append(
            {
                "game_id": game_id,
                "period": period,
                "team_id": team_id,
                "start_event_num": start_event_num,
                "end_event_num": end_event_num,
                "source_type": correction.get("source_type", ""),
                "reason": correction.get("reason_code", ""),
                "evidence_summary": correction.get("evidence_summary", ""),
                "local_confidence_score": (
                    correction.get("local_confidence_score", "")
                    if correction.get("local_confidence_score") is not None
                    else ""
                ),
                "external_alignment_score": (
                    correction.get("external_alignment_score", "")
                    if correction.get("external_alignment_score") is not None
                    else ""
                ),
                "date_added": correction.get("date_added", ""),
                "notes": correction.get("notes", ""),
            }
        )

    starter_rows = sorted(
        starter_rows,
        key=lambda row: (str(row["game_id"]), int(row["period"]), int(row["team_id"])),
    )
    window_rows = sorted(
        window_rows,
        key=lambda row: (
            str(row["game_id"]),
            int(row["period"]),
            int(row["team_id"]),
            int(row["start_event_num"]),
            int(row["end_event_num"]),
        ),
    )
    lineup_windows = {
        game_id: sorted(
            windows,
            key=lambda row: (
                int(row["period"]),
                int(row["team_id"]),
                int(row["start_event_num"]),
                int(row["end_event_num"]),
            ),
        )
        for game_id, windows in sorted(lineup_windows.items())
    }
    period_overrides = {
        game_id: {
            period: dict(sorted(team_map.items(), key=lambda item: int(item[0])))
            for period, team_map in sorted(
                period_map.items(), key=lambda item: int(item[0])
            )
        }
        for game_id, period_map in sorted(period_overrides.items())
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    _write_json(output_dir / PERIOD_STARTERS_JSON, period_overrides)
    _write_json(output_dir / LINEUP_WINDOWS_JSON, lineup_windows)
    _write_csv_rows(
        output_dir / PERIOD_STARTERS_NOTES_CSV,
        [
            "game_id",
            "period",
            "team_id",
            "source_type",
            "reason",
            "evidence_summary",
            "preferred_source",
            "date_added",
            "notes",
        ],
        starter_rows,
    )
    _write_csv_rows(
        output_dir / LINEUP_WINDOWS_NOTES_CSV,
        [
            "game_id",
            "period",
            "team_id",
            "start_event_num",
            "end_event_num",
            "source_type",
            "reason",
            "evidence_summary",
            "local_confidence_score",
            "external_alignment_score",
            "date_added",
            "notes",
        ],
        window_rows,
    )

    summary = {
        "manifest_version": manifest.get("manifest_version"),
        "active_corrections": len(starter_rows) + len(window_rows),
        "active_period_start_corrections": len(starter_rows),
        "active_window_corrections": len(window_rows),
        "active_domains": ["lineup"],
        "output_dir": str(output_dir),
    }
    _write_json(output_dir / COMPILE_SUMMARY_JSON, summary)
    return summary
