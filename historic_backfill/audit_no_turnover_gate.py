from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
from contextlib import contextmanager
from pathlib import Path

import pandas as pd
import pyarrow.dataset as ds

ROOT = Path(__file__).resolve().parent
BUNDLE_ROOT = ROOT.parent
DEFAULT_PBPSTATS_REPO = BUNDLE_ROOT / "pbpstats"


def _ensure_local_pbpstats_importable() -> None:
    if importlib.util.find_spec("pbpstats") is not None:
        return

    candidates: list[Path] = []
    env_path = os.environ.get("PBPSTATS_REPO")
    if env_path:
        candidates.append(Path(env_path).expanduser())
    candidates.append(DEFAULT_PBPSTATS_REPO)

    for candidate in candidates:
        if (candidate / "pbpstats").exists() and str(candidate) not in sys.path:
            sys.path.insert(0, str(candidate))
            if importlib.util.find_spec("pbpstats") is not None:
                return

    raise ModuleNotFoundError(
        "Could not import pbpstats. Set PBPSTATS_REPO or place the fork at ../pbpstats."
    )


_ensure_local_pbpstats_importable()

import pbpstats
from pbpstats.resources.enhanced_pbp import Substitution
from pbpstats.resources.enhanced_pbp.turnover import Turnover

from override_necessity_utils import (
    DEFAULT_VALIDATION_OVERRIDES_PATH,
    compare_boxes,
    diff_pipeline_metrics,
    load_namespace_for_necessity,
    load_single_game_df,
    run_game_variant,
)
from cautious_rerun import DEFAULT_DB, DEFAULT_PARQUET

DEFAULT_OUTPUT_DIR = ROOT / "audit_no_turnover_gate_20260315_v1"
DEFAULT_SEASON_START = 1997
DEFAULT_SEASON_STOP = 2020


def _normalize_game_id(value: str | int) -> str:
    return str(int(value)).zfill(10)


def _season_from_game_id(game_id: str | int) -> int:
    gid = _normalize_game_id(game_id)
    yy = int(gid[3:5])
    return 1901 + yy if yy >= 50 else 2001 + yy


def _description_contains_no_turnover(home: str, visitor: str) -> bool:
    return "no turnover" in f"{home} {visitor}".lower()


def _discover_impacted_games(parquet_path: Path, *, season_start: int, season_stop: int) -> pd.DataFrame:
    dataset = ds.dataset(parquet_path)
    table = dataset.to_table(
        columns=[
            "GAME_ID",
            "EVENTNUM",
            "EVENTMSGTYPE",
            "EVENTMSGACTIONTYPE",
            "PLAYER1_ID",
            "PLAYER1_TEAM_ID",
            "HOMEDESCRIPTION",
            "VISITORDESCRIPTION",
        ],
        filter=(ds.field("EVENTMSGTYPE") == "5") & (ds.field("EVENTMSGACTIONTYPE") == "0"),
    )
    df = table.to_pandas()
    df["GAME_ID"] = df["GAME_ID"].astype(str).str.zfill(10)
    df["season"] = df["GAME_ID"].map(_season_from_game_id)
    df = df[(df["season"] >= season_start) & (df["season"] <= season_stop)].copy()
    df = df[
        df.apply(
            lambda row: _description_contains_no_turnover(
                str(row.get("HOMEDESCRIPTION", "") or ""),
                str(row.get("VISITORDESCRIPTION", "") or ""),
            ),
            axis=1,
        )
    ]
    df = df[
        df["PLAYER1_ID"].fillna("0").astype(str).ne("0")
        & df["PLAYER1_TEAM_ID"].fillna("0").astype(str).ne("0")
    ].copy()
    return df.sort_values(["GAME_ID", "EVENTNUM"]).reset_index(drop=True)


def _variant_turnover_event_stats(self, *, count_deadball_no_turnover: str):
    stats = []
    second_chance_stats = []
    no_turnover_with_steal = self.is_no_turnover and self.is_steal
    valid_no_turnover_committer = self.is_no_turnover and getattr(
        self, "player1_id", 0
    ) not in [0, None, "0"] and getattr(self, "team_id", 0) not in [0, None, "0"]

    if count_deadball_no_turnover == "always":
        countable_no_turnover = valid_no_turnover_committer
    elif count_deadball_no_turnover == "never":
        countable_no_turnover = False
    else:
        raise ValueError(f"Unsupported count_deadball_no_turnover mode: {count_deadball_no_turnover}")

    if not self.is_no_turnover or no_turnover_with_steal or countable_no_turnover:
        team_ids = list(self.current_players.keys())
        opponent_team_id = team_ids[0] if self.team_id == team_ids[1] else team_ids[1]
        lineup_ids = self.lineup_ids
        if self.is_steal:
            turnover_key = (
                pbpstats.LOST_BALL_TURNOVER_STRING
                if self.is_lost_ball or self.is_no_turnover
                else pbpstats.BAD_PASS_TURNOVER_STRING
            )
            steal_team_id = opponent_team_id
            if self.player3_id in self.current_players.get(self.team_id, []):
                steal_team_id = self.team_id
            stats.append(
                {
                    "player_id": self.player1_id,
                    "team_id": self.team_id,
                    "stat_key": turnover_key,
                    "stat_value": 1,
                }
            )
            steal_key = (
                pbpstats.LOST_BALL_STEAL_STRING
                if self.is_lost_ball or self.is_no_turnover
                else pbpstats.BAD_PASS_STEAL_STRING
            )
            stats.append(
                {
                    "player_id": self.player3_id,
                    "team_id": steal_team_id,
                    "stat_key": steal_key,
                    "stat_value": 1,
                }
            )
        else:
            stats.append(
                {
                    "player_id": self.player1_id,
                    "team_id": self.team_id,
                    "stat_key": pbpstats.DEADBALL_TURNOVERS_STRING,
                    "stat_value": 1,
                }
            )
            if self.is_travel:
                stats.append(
                    {
                        "player_id": self.player1_id,
                        "team_id": self.team_id,
                        "stat_key": pbpstats.TRAVELS_STRING,
                        "stat_value": 1,
                    }
                )
            elif self.is_3_second_violation:
                stats.append(
                    {
                        "player_id": self.player1_id,
                        "team_id": self.team_id,
                        "stat_key": pbpstats.THREE_SECOND_VIOLATION_TURNOVER_STRING,
                        "stat_value": 1,
                    }
                )
            elif self.is_step_out_of_bounds:
                stats.append(
                    {
                        "player_id": self.player1_id,
                        "team_id": self.team_id,
                        "stat_key": pbpstats.STEP_OUT_OF_BOUNDS_TURNOVER_STRING,
                        "stat_value": 1,
                    }
                )
            elif self.is_offensive_goaltending:
                stats.append(
                    {
                        "player_id": self.player1_id,
                        "team_id": self.team_id,
                        "stat_key": pbpstats.OFFENSIVE_GOALTENDING_STRING,
                        "stat_value": 1,
                    }
                )
            elif self.is_lost_ball_out_of_bounds:
                stats.append(
                    {
                        "player_id": self.player1_id,
                        "team_id": self.team_id,
                        "stat_key": pbpstats.LOST_BALL_OUT_OF_BOUNDS_TURNOVER_STRING,
                        "stat_value": 1,
                    }
                )
            elif self.is_bad_pass_out_of_bounds:
                stats.append(
                    {
                        "player_id": self.player1_id,
                        "team_id": self.team_id,
                        "stat_key": pbpstats.BAD_PASS_OUT_OF_BOUNDS_TURNOVER_STRING,
                        "stat_value": 1,
                    }
                )
            elif self.is_shot_clock_violation:
                stats.append(
                    {
                        "player_id": self.player1_id,
                        "team_id": self.team_id,
                        "stat_key": pbpstats.SHOT_CLOCK_VIOLATION_TURNOVER_STRING,
                        "stat_value": 1,
                    }
                )

            events_to_check = self.get_all_events_at_current_time()
            if self.player1_id != 0 and str(self.player1_id) not in lineup_ids[self.team_id].split("-"):
                for event in events_to_check:
                    if isinstance(event, Substitution) and event.outgoing_player_id == self.player1_id:
                        fixed_lineup_id = lineup_ids[self.team_id].replace(
                            str(event.incoming_player_id),
                            str(event.outgoing_player_id),
                        )
                        lineup_ids[self.team_id] = fixed_lineup_id

        for stat in stats:
            opponent_team_id = team_ids[0] if stat["team_id"] == team_ids[1] else team_ids[1]
            stat["lineup_id"] = lineup_ids[stat["team_id"]]
            stat["opponent_team_id"] = opponent_team_id
            stat["opponent_lineup_id"] = lineup_ids[opponent_team_id]
            if self.is_second_chance_event():
                second_chance_stats.append(
                    {
                        key: value if key != "stat_key" else f"{pbpstats.SECOND_CHANCE_STRING}{value}"
                        for key, value in stat.items()
                    }
                )
            if self.is_penalty_event():
                second_chance_stats.append(
                    {
                        key: value if key != "stat_key" else f"{pbpstats.PENALTY_STRING}{value}"
                        for key, value in stat.items()
                    }
                )
    return self.base_stats + stats + second_chance_stats


@contextmanager
def _patched_turnover_gate(mode: str):
    original_property = Turnover.event_stats

    def _patched(self):
        return _variant_turnover_event_stats(self, count_deadball_no_turnover=mode)

    Turnover.event_stats = property(_patched)
    try:
        yield
    finally:
        Turnover.event_stats = original_property


def _metrics_score(metrics) -> tuple[int, int, int, int, int, int, int]:
    return (
        1 if metrics.error else 0,
        metrics.event_stats_errors,
        metrics.rebound_deletions,
        metrics.audit_errors,
        metrics.audit_team_rows,
        metrics.audit_player_rows,
        -metrics.darko_rows,
    )


def _comparison_label(current_metrics, variant_metrics) -> str:
    current_score = _metrics_score(current_metrics)
    variant_score = _metrics_score(variant_metrics)
    if variant_score < current_score:
        return "variant_better"
    if variant_score > current_score:
        return "variant_worse"
    return "same"


def _modes_for_season(season: int) -> list[str]:
    return ["never"] if season >= 2017 else ["always"]


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit the historical dead-ball 'No Turnover' season gate against impacted games.")
    parser.add_argument("--parquet-path", type=Path, default=DEFAULT_PARQUET)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--validation-overrides-path", type=Path, default=DEFAULT_VALIDATION_OVERRIDES_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--tolerance", type=int, default=2)
    parser.add_argument("--season-start", type=int, default=DEFAULT_SEASON_START)
    parser.add_argument("--season-stop", type=int, default=DEFAULT_SEASON_STOP)
    parser.add_argument("--limit-games", type=int)
    args = parser.parse_args()

    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    impacted_rows = _discover_impacted_games(
        args.parquet_path.resolve(),
        season_start=args.season_start,
        season_stop=args.season_stop,
    )
    impacted_rows.to_csv(output_dir / "impacted_no_turnover_rows.csv", index=False)
    impacted_games = sorted(impacted_rows["GAME_ID"].drop_duplicates().tolist())
    if args.limit_games is not None:
        impacted_games = impacted_games[: int(args.limit_games)]
        impacted_rows = impacted_rows[impacted_rows["GAME_ID"].isin(impacted_games)].reset_index(drop=True)

    namespace_current, validation_overrides = load_namespace_for_necessity(
        db_path=args.db_path.resolve(),
        validation_overrides_path=args.validation_overrides_path.resolve(),
    )
    namespace_variant, _ = load_namespace_for_necessity(
        db_path=args.db_path.resolve(),
        validation_overrides_path=args.validation_overrides_path.resolve(),
    )

    rows: list[dict] = []
    for game_id in impacted_games:
        season = _season_from_game_id(game_id)
        game_df = load_single_game_df(args.parquet_path.resolve(), game_id)
        current_metrics, current_box = run_game_variant(
            namespace_current,
            game_id,
            game_df,
            validation_overrides=validation_overrides,
            tolerance=args.tolerance,
        )

        for mode in _modes_for_season(season):
            with _patched_turnover_gate(mode):
                variant_metrics, variant_box = run_game_variant(
                    namespace_variant,
                    game_id,
                    game_df,
                    validation_overrides=validation_overrides,
                    tolerance=args.tolerance,
                )

            changed_players = 0
            changed_cells = 0
            if current_box is not None and variant_box is not None:
                changed_players, changed_cells = compare_boxes(current_box, variant_box)

            rows.append(
                {
                    "game_id": game_id,
                    "season": season,
                    "variant": mode,
                    "comparison": _comparison_label(current_metrics, variant_metrics),
                    "changed_players": changed_players,
                    "changed_cells": changed_cells,
                    "changed_pipeline_metrics": "|".join(diff_pipeline_metrics(current_metrics, variant_metrics)),
                    "current_error": current_metrics.error,
                    "variant_error": variant_metrics.error,
                    "current_event_stats_errors": current_metrics.event_stats_errors,
                    "variant_event_stats_errors": variant_metrics.event_stats_errors,
                    "current_rebound_deletions": current_metrics.rebound_deletions,
                    "variant_rebound_deletions": variant_metrics.rebound_deletions,
                    "current_audit_team_rows": current_metrics.audit_team_rows,
                    "variant_audit_team_rows": variant_metrics.audit_team_rows,
                    "current_audit_player_rows": current_metrics.audit_player_rows,
                    "variant_audit_player_rows": variant_metrics.audit_player_rows,
                    "current_darko_rows": current_metrics.darko_rows,
                    "variant_darko_rows": variant_metrics.darko_rows,
                }
            )

    report = pd.DataFrame(rows).sort_values(["variant", "comparison", "game_id"]).reset_index(drop=True)
    report.to_csv(output_dir / "no_turnover_gate_audit.csv", index=False)

    summary = {
        "impacted_rows": int(len(impacted_rows)),
        "impacted_games": int(len(impacted_games)),
        "season_range": [args.season_start, args.season_stop],
        "season_counts": impacted_rows["GAME_ID"].map(_season_from_game_id).value_counts().sort_index().to_dict(),
        "variant_comparison_counts": {
            mode: report[report["variant"] == mode]["comparison"].value_counts(dropna=False).to_dict()
            for mode in sorted(report["variant"].unique())
        },
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
