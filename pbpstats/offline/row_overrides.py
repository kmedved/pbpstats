"""Generic play-by-play row override mechanics.

This module provides the parser-side primitives for applying evidence-based
corrections to a stats.nba.com play-by-play DataFrame before possession
parsing. It supports five override action verbs:

* ``move_before`` / ``move_after`` -- relocate an existing event row relative
  to an anchor event without changing event content.
* ``drop`` -- delete an existing event row.
* ``insert_sub_before`` / ``insert_sub_after`` -- inject a synthetic SUB row
  (``EVENTMSGTYPE = 8``) anchored to an existing event when raw NBA data is
  missing a substitution that the box score requires.

Inserted synthetic rows are tagged via ``PBP_ROW_OVERRIDE_ACTION_COLUMN`` so
that downstream v3 dedupe (``pbpstats.offline.ordering.dedupe_with_v3``) can
preserve them rather than treating them as v2-only noise.

This module intentionally has **no dependency on any catalog location**. It
takes parsed override dicts as input and never reads a default CSV from disk.
The historic backfill pipeline lives in a separate ``historic_backfill``
folder and provides its own catalog loader; ``pbpstats`` itself must not
import from that folder. See ``historic_backfill/README.md`` for the
canonical catalog of overrides used in 1997-2020 NBA reruns.

Public API:

* :func:`load_pbp_row_overrides` -- read and validate an override CSV at an explicit path.
* :func:`apply_pbp_row_overrides` -- apply parsed overrides to a single-game DataFrame.
* :func:`normalize_game_id` -- coerce mixed game-id stylings to a 10-digit string.
* :data:`PBP_ROW_OVERRIDE_ACTION_COLUMN` -- column name marking override-touched rows.
* :data:`PBP_ROW_OVERRIDE_NOTES_COLUMN` -- column name carrying override provenance notes.
* :data:`VALID_PBP_ROW_OVERRIDE_ACTIONS` -- set of recognized action verbs.

The loader and applier raise ``ValueError`` (or ``FileNotFoundError`` for a
missing catalog) on malformed input rather than silently dropping work, so
override mistakes surface during validation rather than after a full
historic backfill.
"""

from __future__ import annotations

from os import PathLike
from typing import Dict, List

import pandas as pd


PBP_ROW_OVERRIDE_ACTION_COLUMN = "PBP_ROW_OVERRIDE_ACTION"
"""Column name written onto override-touched rows.

``dedupe_with_v3`` in ``pbpstats.offline.ordering`` checks for non-empty
values in this column and preserves those rows even if the v3 stream lacks a
matching event.
"""

PBP_ROW_OVERRIDE_NOTES_COLUMN = "PBP_ROW_OVERRIDE_NOTES"
"""Column name carrying free-text provenance notes attached to an override row."""

VALID_PBP_ROW_OVERRIDE_ACTIONS = {
    "drop",
    "move_before",
    "move_after",
    "insert_sub_before",
    "insert_sub_after",
}


_OPTIONAL_INSERT_FIELDS = {
    "period",
    "pctimestring",
    "wctimestring",
    "description_side",
    "player_out_id",
    "player_out_name",
    "player_out_team_id",
    "player_in_id",
    "player_in_name",
    "player_in_team_id",
}


def _coerce_optional_int(value: object) -> int | None:
    raw = str(value if value is not None else "").strip()
    if not raw:
        return None
    try:
        return int(float(raw))
    except ValueError:
        return None


def normalize_game_id(value: object) -> str:
    """Normalize NBA game IDs from string/int/float-like source values."""
    raw = str(value if value is not None else "").strip()
    if not raw:
        raise ValueError("game_id is blank")
    return str(int(float(raw))).zfill(10)


def _clock_seconds_from_pctimestring(value: str) -> float | None:
    parts = str(value).strip().split(":")
    if len(parts) != 2:
        return None
    try:
        return float(int(parts[0]) * 60 + float(parts[1]))
    except ValueError:
        return None


def _required_string(row: dict, field: str, row_number: int, strict: bool) -> str:
    value = str(row.get(field, "")).strip()
    if strict and not value:
        raise ValueError(f"Row {row_number} is missing required field {field}")
    return value


def _parse_int(value: str, field: str, row_number: int, strict: bool, *, required: bool) -> int | None:
    if not value:
        if strict and required:
            raise ValueError(f"Row {row_number} is missing required field {field}")
        return None
    try:
        return int(float(value))
    except ValueError:
        if strict:
            raise ValueError(f"Row {row_number} has invalid integer field {field}: {value!r}")
        return None


def _validate_parsed_override(row: dict, row_number: int, strict: bool) -> bool:
    if not strict:
        return True

    action = row["action"]
    if action not in VALID_PBP_ROW_OVERRIDE_ACTIONS:
        raise ValueError(f"Row {row_number} has unknown override action: {action!r}")

    if action in {"move_before", "move_after", "insert_sub_before", "insert_sub_after"}:
        if row.get("anchor_event_num") is None:
            raise ValueError(f"Row {row_number} action {action} requires anchor_event_num")
        if int(row["event_num"]) == int(row["anchor_event_num"]):
            raise ValueError(f"Row {row_number} action {action} cannot anchor to itself")

    if action in {"insert_sub_before", "insert_sub_after"}:
        for field in ("player_out_id", "player_out_name", "player_in_id", "player_in_name"):
            if not str(row.get(field, "")).strip():
                raise ValueError(f"Row {row_number} action {action} requires {field}")

    return True


def load_pbp_row_overrides(
    path: str | PathLike[str],
    *,
    missing_ok: bool = False,
    strict: bool = True,
) -> Dict[str, List[dict]]:
    """Load play-by-play row overrides from a CSV file.

    The CSV must contain at minimum the columns ``game_id``, ``action``, and
    ``event_num``. ``anchor_event_num`` is required for ``move_before``,
    ``move_after``, ``insert_sub_before``, and ``insert_sub_after`` actions.
    Optional synthetic-sub fields (``period``, ``pctimestring``,
    ``wctimestring``, ``description_side``, ``player_out_id``,
    ``player_out_name``, ``player_out_team_id``, ``player_in_id``,
    ``player_in_name``, ``player_in_team_id``) are read when present and may
    be left blank for non-insert actions.

    Game IDs are zero-padded to 10 digits so that callers can mix the two
    common NBA stylings (``"0020400335"`` and ``"20300257"``) in the same
    catalog.

    Parameters
    ----------
    path :
        Filesystem path to the override CSV. Missing files raise by default
        because callers supplied an explicit catalog path.
    missing_ok :
        Return an empty mapping for a missing path. This is intended only for
        optional ad hoc use, not the historic runtime catalog.
    strict :
        Validate required columns and action-specific fields while loading.

    Returns
    -------
    dict
        Mapping from ten-digit ``game_id`` to a list of parsed override dicts
        in catalog order. Each dict contains the parsed ``action``,
        ``event_num``, ``anchor_event_num`` (or ``None``), ``notes``, and the
        optional synthetic-sub fields as strings.

    Notes
    -----
    Catalog discovery is the caller's responsibility. ``pbpstats`` itself
    does not ship a default override CSV; the historic backfill catalog
    lives at ``historic_backfill/catalogs/pbp_row_overrides.csv`` in
    consumer repos.
    """

    from pathlib import Path

    override_path = Path(path)
    if not override_path.exists():
        if missing_ok:
            return {}
        raise FileNotFoundError(f"PBP row override catalog not found: {override_path}")

    df = pd.read_csv(
        override_path,
        dtype={
            "game_id": str,
            "action": str,
            "event_num": str,
            "anchor_event_num": str,
            "notes": str,
            **{field: str for field in _OPTIONAL_INSERT_FIELDS},
        },
    ).fillna("")

    overrides: Dict[str, List[dict]] = {}
    seen_rows: set[tuple[str, str, int, int | None]] = set()
    for row_number, row in enumerate(df.to_dict(orient="records"), start=2):
        raw_gid = _required_string(row, "game_id", row_number, strict)
        raw_action = _required_string(row, "action", row_number, strict).lower()
        raw_event = _required_string(row, "event_num", row_number, strict)
        raw_anchor = str(row.get("anchor_event_num", "")).strip()
        if not raw_gid or not raw_action or not raw_event:
            continue
        try:
            game_id = normalize_game_id(raw_gid)
        except ValueError as exc:
            if strict:
                raise ValueError(f"Row {row_number} has invalid game_id: {raw_gid!r}") from exc
            continue
        event_num = _parse_int(raw_event, "event_num", row_number, strict, required=True)
        anchor_event_num = _parse_int(raw_anchor, "anchor_event_num", row_number, strict, required=False)
        if event_num is None:
            continue
        parsed_row = {
            "action": raw_action,
            "event_num": event_num,
            "anchor_event_num": anchor_event_num,
            "notes": str(row.get("notes", "")).strip(),
        }
        for field in _OPTIONAL_INSERT_FIELDS:
            parsed_row[field] = str(row.get(field, "")).strip()
        if not _validate_parsed_override(parsed_row, row_number, strict):
            continue
        duplicate_key = (game_id, raw_action, event_num, anchor_event_num)
        if duplicate_key in seen_rows:
            raise ValueError(f"Row {row_number} duplicates override {duplicate_key}")
        seen_rows.add(duplicate_key)
        overrides.setdefault(game_id, []).append(parsed_row)
    return overrides


def _infer_sub_description_column(
    df: pd.DataFrame,
    player_out_team_id: int | None,
    requested_side: str,
) -> str:
    side = requested_side.strip().lower()
    side_to_column = {
        "home": "HOMEDESCRIPTION",
        "visitor": "VISITORDESCRIPTION",
        "neutral": "NEUTRALDESCRIPTION",
    }
    if side in side_to_column and side_to_column[side] in df.columns:
        return side_to_column[side]

    if player_out_team_id is not None and {"PLAYER1_TEAM_ID", "EVENTMSGTYPE"}.issubset(df.columns):
        team_ids = pd.to_numeric(df.get("PLAYER1_TEAM_ID"), errors="coerce")
        event_types = pd.to_numeric(df.get("EVENTMSGTYPE"), errors="coerce")
        sub_rows = df[(event_types == 8) & (team_ids == player_out_team_id)]
        for column in ("HOMEDESCRIPTION", "VISITORDESCRIPTION", "NEUTRALDESCRIPTION"):
            if column not in sub_rows.columns:
                continue
            descriptions = sub_rows[column].fillna("").astype(str).str.strip()
            if descriptions.str.startswith("SUB:").any():
                return column

    for column in ("HOMEDESCRIPTION", "VISITORDESCRIPTION", "NEUTRALDESCRIPTION"):
        if column in df.columns:
            return column
    return "HOMEDESCRIPTION"


def _build_synthetic_sub_row(
    df: pd.DataFrame,
    anchor_row: pd.Series,
    override: dict,
    game_id: str,
) -> pd.DataFrame:
    event_num = override["event_num"]
    anchor_event_num = override.get("anchor_event_num")
    player_out_id = _coerce_optional_int(override.get("player_out_id"))
    player_in_id = _coerce_optional_int(override.get("player_in_id"))
    player_out_team_id = _coerce_optional_int(override.get("player_out_team_id"))
    player_in_team_id = _coerce_optional_int(override.get("player_in_team_id"))
    if player_out_id is None or player_in_id is None:
        raise ValueError("Synthetic substitution override requires player_out_id and player_in_id")

    player_out_name = str(override.get("player_out_name", "")).strip()
    player_in_name = str(override.get("player_in_name", "")).strip()
    if not player_out_name or not player_in_name:
        raise ValueError("Synthetic substitution override requires player_out_name and player_in_name")

    period = _coerce_optional_int(override.get("period"))
    if period is None:
        period = _coerce_optional_int(anchor_row.get("PERIOD"))
    pctimestring = str(override.get("pctimestring", "")).strip() or str(anchor_row.get("PCTIMESTRING", "")).strip()
    wctimestring = str(override.get("wctimestring", "")).strip() or str(anchor_row.get("WCTIMESTRING", "")).strip()
    description = f"SUB: {player_in_name} FOR {player_out_name}"

    row = anchor_row.copy()
    assignments = {
        "GAME_ID": game_id,
        "EVENTNUM": event_num,
        "EVENTMSGTYPE": 8,
        "EVENTMSGACTIONTYPE": 0,
        "PERIOD": period,
        "PCTIMESTRING": pctimestring,
        "WCTIMESTRING": wctimestring,
        "SCORE": "",
        "SCOREMARGIN": "",
        "PLAYER1_ID": player_out_id,
        "PLAYER1_NAME": player_out_name,
        "PLAYER1_TEAM_ID": player_out_team_id or "",
        "PLAYER2_ID": player_in_id,
        "PLAYER2_NAME": player_in_name,
        "PLAYER2_TEAM_ID": player_in_team_id or player_out_team_id or "",
        "PLAYER3_ID": 0,
        "PLAYER3_NAME": "",
        "PLAYER3_TEAM_ID": "",
        "event_num": event_num,
        "period": period,
        "description": description,
        PBP_ROW_OVERRIDE_ACTION_COLUMN: override.get("action", ""),
        PBP_ROW_OVERRIDE_NOTES_COLUMN: override.get("notes", ""),
    }
    clock_seconds = _clock_seconds_from_pctimestring(pctimestring)
    if clock_seconds is not None:
        assignments["clock_seconds_remaining"] = clock_seconds

    for column in ("HOMEDESCRIPTION", "VISITORDESCRIPTION", "NEUTRALDESCRIPTION"):
        if column in row.index:
            row[column] = ""
    description_column = _infer_sub_description_column(
        df,
        player_out_team_id,
        str(override.get("description_side", "")),
    )
    assignments[description_column] = description

    for column, value in assignments.items():
        if column in row.index:
            row[column] = value

    return pd.DataFrame([row], columns=df.columns)


def apply_pbp_row_overrides(
    game_df: pd.DataFrame,
    overrides: Dict[str, List[dict]],
) -> pd.DataFrame:
    """Apply parsed row overrides to a single game's play-by-play DataFrame.

    ``game_df`` must contain rows for exactly one game (after game-ID
    normalization); a multi-game frame raises ``ValueError``. Only overrides
    keyed to that game's normalized 10-digit ID are applied; an empty
    DataFrame is returned unchanged. Each override runs in catalog order:

    * ``move_before`` / ``move_after`` -- the row whose ``EVENTNUM`` matches
      ``event_num`` is relocated to immediately before or after the row whose
      ``EVENTNUM`` matches ``anchor_event_num``. The moved row's content is
      unchanged. Ambiguous or missing lookups are skipped silently.
    * ``drop`` -- the row whose ``EVENTNUM`` matches ``event_num`` is removed.
      Ambiguous or missing lookups are skipped silently.
    * ``insert_sub_before`` / ``insert_sub_after`` -- a synthetic SUB row
      (``EVENTMSGTYPE = 8``) with ``EVENTNUM = event_num`` is inserted
      immediately before or after the anchor row. Both
      ``player_out_id`` / ``player_out_name`` and
      ``player_in_id`` / ``player_in_name`` are required; missing fields
      raise ``ValueError`` from the synthetic row builder. The synthetic row
      inherits ``PERIOD``, ``PCTIMESTRING``, and ``WCTIMESTRING`` from the
      override when supplied, otherwise from the anchor row. The
      ``PBP_ROW_OVERRIDE_ACTION`` and ``PBP_ROW_OVERRIDE_NOTES`` columns are
      populated so the synthetic row survives downstream v3 dedupe.

    Unknown actions and missing-anchor errors raise ``ValueError`` rather
    than silently dropping work; override-catalog mistakes surface during
    application instead of after a full historic backfill.

    Parameters
    ----------
    game_df :
        Per-game play-by-play DataFrame in stats.nba.com schema. May be
        empty, in which case it is returned unchanged.
    overrides :
        Mapping returned by :func:`load_pbp_row_overrides`.

    Returns
    -------
    pandas.DataFrame
        A new DataFrame with overrides applied. The original ``game_df`` is
        not mutated.

    Raises
    ------
    ValueError
        If ``game_df`` mixes multiple game IDs, if an override uses an
        unknown action, or if a ``move_before`` / ``move_after`` /
        ``insert_sub_*`` override is missing ``anchor_event_num``.
    """

    if game_df.empty:
        return game_df

    normalized_game_ids = (
        game_df["GAME_ID"].dropna().map(normalize_game_id).drop_duplicates().tolist()
    )
    if len(normalized_game_ids) != 1:
        raise ValueError("apply_pbp_row_overrides expects a single-game DataFrame")
    game_id = normalized_game_ids[0]
    applicable = overrides.get(game_id)
    if not applicable:
        return game_df

    df = game_df.copy().reset_index(drop=True)
    event_nums = pd.to_numeric(df["EVENTNUM"], errors="coerce")

    for override in applicable:
        action = override["action"]
        if action not in VALID_PBP_ROW_OVERRIDE_ACTIONS:
            raise ValueError(f"Unknown PBP row override action: {action!r}")
        event_num = override["event_num"]
        anchor_event_num = override.get("anchor_event_num")

        if action in {"insert_sub_before", "insert_sub_after"}:
            if anchor_event_num is None:
                raise ValueError(f"{action} override for event {event_num} requires anchor_event_num")
            if len(event_nums.index[event_nums == event_num]) != 0:
                continue
            for column in (PBP_ROW_OVERRIDE_ACTION_COLUMN, PBP_ROW_OVERRIDE_NOTES_COLUMN):
                if column not in df.columns:
                    df[column] = ""
            anchor_idx = event_nums.index[event_nums == anchor_event_num]
            if len(anchor_idx) != 1:
                continue
            anchor_idx = int(anchor_idx[0])
            row = _build_synthetic_sub_row(df, df.iloc[anchor_idx], override, game_id)
            insert_at = anchor_idx if action == "insert_sub_before" else anchor_idx + 1
            df = pd.concat([df.iloc[:insert_at], row, df.iloc[insert_at:]], ignore_index=True)
            event_nums = pd.to_numeric(df["EVENTNUM"], errors="coerce")
            continue

        match_idx = event_nums.index[event_nums == event_num]
        if len(match_idx) != 1:
            continue
        row_idx = int(match_idx[0])

        if action == "drop":
            df = df.drop(index=row_idx).reset_index(drop=True)
            event_nums = pd.to_numeric(df["EVENTNUM"], errors="coerce")
            continue

        if action not in {"move_before", "move_after"}:
            continue
        if anchor_event_num is None:
            raise ValueError(f"{action} override for event {event_num} requires anchor_event_num")

        anchor_idx = event_nums.index[event_nums == anchor_event_num]
        if len(anchor_idx) != 1:
            continue
        anchor_idx = int(anchor_idx[0])

        row = df.iloc[[row_idx]].copy()
        df = df.drop(index=row_idx).reset_index(drop=True)
        event_nums = pd.to_numeric(df["EVENTNUM"], errors="coerce")

        anchor_idx = int(event_nums.index[event_nums == anchor_event_num][0])
        insert_at = anchor_idx if action == "move_before" else anchor_idx + 1

        df = pd.concat([df.iloc[:insert_at], row, df.iloc[insert_at:]], ignore_index=True)
        event_nums = pd.to_numeric(df["EVENTNUM"], errors="coerce")

    return df.reset_index(drop=True)
