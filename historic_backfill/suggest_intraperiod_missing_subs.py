from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

import pandas as pd

from audit_minutes_plus_minus import _prepare_darko_df, load_official_boxscore_df
from bbr_boxscore_loader import (
    DEFAULT_BBR_DB_PATH,
    DEFAULT_PLAYER_CROSSWALK_PATH,
    load_bbr_boxscore_df,
)
from intraperiod_missing_sub_repair import build_intraperiod_missing_sub_candidates
from minute_reference_sources import (
    DEFAULT_PBPSTATS_PLAYER_BOX_PATH,
    DEFAULT_TPDEV_PBP_PATH,
    load_pbpstats_player_box_frame,
    load_tpdev_pbp_minutes_frame,
)
from trace_player_stints_game import (
    DEFAULT_DB_PATH,
    DEFAULT_PARQUET_PATH,
    _collect_game_events,
    _load_game_context,
)


def _normalize_game_id(value: str | int) -> str:
    return str(int(value)).zfill(10)


def _load_game_ids(args: argparse.Namespace) -> list[str]:
    raw_ids: list[str] = []
    if args.game_ids:
        raw_ids.extend(args.game_ids)
    if args.game_ids_file is not None:
        raw_ids.extend(
            line.strip()
            for line in args.game_ids_file.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    if not raw_ids:
        raise ValueError("Provide --game-ids and/or --game-ids-file")
    return sorted({_normalize_game_id(game_id) for game_id in raw_ids})


def _seconds_map(frame: pd.DataFrame, column_name: str) -> dict[tuple[str, int], float]:
    if frame.empty or column_name not in frame.columns:
        return {}
    normalized = frame.copy()
    normalized["game_id"] = normalized["game_id"].map(_normalize_game_id)
    normalized["player_id"] = pd.to_numeric(
        normalized["player_id"], errors="coerce"
    ).fillna(0).astype(int)
    normalized[column_name] = pd.to_numeric(
        normalized[column_name], errors="coerce"
    ).fillna(0.0)
    return {
        (row.game_id, int(row.player_id)): float(getattr(row, column_name))
        for row in normalized.itertuples(index=False)
        if int(row.player_id) > 0
    }


def _external_improvement(
    *,
    game_id: str,
    candidate: dict,
    output_seconds_map: dict[tuple[str, int], float],
    reference_seconds_map: dict[tuple[str, int], float],
) -> float | None:
    if not reference_seconds_map:
        return None
    player_in = int(candidate["player_in_id"])
    player_out = int(candidate["player_out_id"] or 0)
    if player_out <= 0:
        return None
    window_seconds = float(candidate.get("approx_window_seconds") or 0.0)
    if window_seconds <= 0:
        return None

    in_key = (game_id, player_in)
    out_key = (game_id, player_out)
    if in_key not in output_seconds_map or out_key not in output_seconds_map:
        return None
    if in_key not in reference_seconds_map or out_key not in reference_seconds_map:
        return None

    current_gap = abs(output_seconds_map[in_key] - reference_seconds_map[in_key]) + abs(
        output_seconds_map[out_key] - reference_seconds_map[out_key]
    )
    projected_gap = abs(
        (output_seconds_map[in_key] + window_seconds) - reference_seconds_map[in_key]
    ) + abs(
        (output_seconds_map[out_key] - window_seconds) - reference_seconds_map[out_key]
    )
    return current_gap - projected_gap


def _build_override_proposals(candidates_df: pd.DataFrame) -> dict[str, list[dict]]:
    proposals: dict[str, list[dict]] = {}
    if candidates_df.empty:
        return proposals
    proposal_df = candidates_df.loc[
        candidates_df["promotion_decision"].isin(
            ["ambiguous_runner_up", "low_local_confidence", "introduces_new_contradiction"]
        )
        & candidates_df["lineup_player_ids_json"].notna()
    ].copy()
    for row in proposal_df.itertuples(index=False):
        proposals.setdefault(str(row.game_id), []).append(
            {
                "period": int(row.period),
                "team_id": int(row.team_id),
                "start_event_num": int(row.start_event_num),
                "end_event_num": int(row.end_event_num),
                "lineup_player_ids": json.loads(row.lineup_player_ids_json),
            }
        )
    return proposals


def _build_note_proposals(candidates_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
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
    ]
    if candidates_df.empty:
        return pd.DataFrame(columns=columns)

    proposals = candidates_df.loc[
        candidates_df["promotion_decision"].isin(
            ["ambiguous_runner_up", "low_local_confidence", "introduces_new_contradiction"]
        )
    ].copy()
    if proposals.empty:
        return pd.DataFrame(columns=columns)

    proposals["source_type"] = "intraperiod_missing_sub_suggester"
    proposals["reason"] = proposals["promotion_decision"].astype(str)
    proposals["evidence_summary"] = proposals.apply(
        lambda row: (
            f"player_in={int(row['player_in_id'])}, "
            f"player_out={int(row['player_out_id']) if pd.notna(row['player_out_id']) else 0}, "
            f"contradictions_removed={int(row['contradictions_removed'])}, "
            f"new_contradictions_introduced={int(row['new_contradictions_introduced'])}"
        ),
        axis=1,
    )
    proposals["date_added"] = pd.Timestamp.now(tz="America/New_York").date().isoformat()
    proposals["notes"] = ""
    return proposals[columns].reset_index(drop=True)


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Suggest local intraperiod missing-sub repairs without runtime tpdev dependence."
    )
    parser.add_argument("--game-ids", nargs="*")
    parser.add_argument("--game-ids-file", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--parquet-path", type=Path, default=DEFAULT_PARQUET_PATH)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--tpdev-pbp-path", type=Path, default=DEFAULT_TPDEV_PBP_PATH)
    parser.add_argument(
        "--pbpstats-player-box-path",
        type=Path,
        default=DEFAULT_PBPSTATS_PLAYER_BOX_PATH,
    )
    parser.add_argument("--bbr-db-path", type=Path, default=DEFAULT_BBR_DB_PATH)
    parser.add_argument(
        "--player-crosswalk-path", type=Path, default=DEFAULT_PLAYER_CROSSWALK_PATH
    )
    parser.add_argument("--emit-override-proposals", action="store_true", default=False)
    parser.add_argument("--emit-override-note-proposals", action="store_true", default=False)
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    game_ids = _load_game_ids(args)
    tpdev_pbp_map = _seconds_map(
        load_tpdev_pbp_minutes_frame(args.tpdev_pbp_path.resolve(), game_ids),
        "tpdev_pbp_seconds",
    )
    pbpstats_box_map = _seconds_map(
        load_pbpstats_player_box_frame(args.pbpstats_player_box_path.resolve(), game_ids),
        "pbpstats_box_seconds",
    )

    candidate_rows: list[dict] = []
    for game_id in game_ids:
        darko_df, possessions, _ = _load_game_context(
            game_id,
            parquet_path=args.parquet_path.resolve(),
            db_path=args.db_path.resolve(),
        )
        prepared_darko = _prepare_darko_df(darko_df)
        output_seconds_map = {
            (str(row.game_id).zfill(10), int(row.player_id)): float(row.Minutes_output) * 60.0
            for row in prepared_darko.itertuples(index=False)
        }
        official_map = _seconds_map(
            load_official_boxscore_df(args.db_path.resolve(), game_id).assign(
                official_seconds=lambda frame: frame["Minutes_official"] * 60.0
            ),
            "official_seconds",
        )
        bbr_map = _seconds_map(
            load_bbr_boxscore_df(
                game_id,
                nba_raw_db_path=args.db_path.resolve(),
                bbr_db_path=args.bbr_db_path.resolve(),
                crosswalk_path=args.player_crosswalk_path.resolve(),
            ).assign(bbr_seconds=lambda frame: frame["Minutes_bbr_box"] * 60.0),
            "bbr_seconds",
        )
        events = _collect_game_events(possessions)
        candidates = build_intraperiod_missing_sub_candidates(events, game_id=game_id)
        for candidate in candidates:
            row = dict(candidate)
            lineup_ids = row.pop("override_lineup_player_ids", None)
            row["lineup_player_ids_json"] = (
                json.dumps(lineup_ids) if isinstance(lineup_ids, list) else None
            )
            row["override_event_indices_json"] = json.dumps(
                row.pop("override_event_indices", [])
            )
            row["tpdev_alignment_score"] = _external_improvement(
                game_id=game_id,
                candidate=candidate,
                output_seconds_map=output_seconds_map,
                reference_seconds_map=tpdev_pbp_map,
            )
            row["pbpstats_alignment_score"] = _external_improvement(
                game_id=game_id,
                candidate=candidate,
                output_seconds_map=output_seconds_map,
                reference_seconds_map=pbpstats_box_map,
            )
            row["official_alignment_score"] = _external_improvement(
                game_id=game_id,
                candidate=candidate,
                output_seconds_map=output_seconds_map,
                reference_seconds_map=official_map,
            )
            row["bbr_alignment_score"] = _external_improvement(
                game_id=game_id,
                candidate=candidate,
                output_seconds_map=output_seconds_map,
                reference_seconds_map=bbr_map,
            )
            alignment_values = [
                value
                for value in (
                    row["tpdev_alignment_score"],
                    row["pbpstats_alignment_score"],
                    row["official_alignment_score"],
                    row["bbr_alignment_score"],
                )
                if value is not None
            ]
            row["external_alignment_score"] = (
                float(sum(alignment_values) / len(alignment_values))
                if alignment_values
                else None
            )
            candidate_rows.append(row)

    candidates_df = pd.DataFrame(candidate_rows)
    candidates_df.to_csv(
        output_dir / "intraperiod_missing_sub_candidates.csv",
        index=False,
    )

    summary = {
        "games": len(game_ids),
        "candidate_rows": int(len(candidates_df)),
        "auto_apply_candidates": int(candidates_df["auto_apply"].sum()) if not candidates_df.empty else 0,
        "promotion_decision_counts": (
            candidates_df["promotion_decision"].value_counts().sort_index().to_dict()
            if not candidates_df.empty
            else {}
        ),
        "deadball_choice_kind_counts": (
            candidates_df["deadball_choice_kind"].fillna("missing").value_counts().sort_index().to_dict()
            if not candidates_df.empty and "deadball_choice_kind" in candidates_df.columns
            else {}
        ),
        "not_nearest_auto_apply_candidates": (
            int(
                candidates_df.loc[
                    candidates_df["auto_apply"]
                    & candidates_df["deadball_choice_kind"].isin(["earliest_winning", "middle_winning"])
                ].shape[0]
            )
            if not candidates_df.empty and "deadball_choice_kind" in candidates_df.columns
            else 0
        ),
    }
    (output_dir / "intraperiod_missing_sub_summary.json").write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )

    if args.emit_override_proposals:
        proposals = _build_override_proposals(candidates_df)
        (output_dir / "lineup_window_override_proposals.json").write_text(
            json.dumps(proposals, indent=2),
            encoding="utf-8",
        )

    if args.emit_override_note_proposals:
        notes_df = _build_note_proposals(candidates_df)
        notes_df.to_csv(output_dir / "lineup_window_override_note_proposals.csv", index=False)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
