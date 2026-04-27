from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from boxscore_audit import _load_single_game_pbp, build_pbp_boxscore_from_stat_rows
from cautious_rerun import DEFAULT_DB, DEFAULT_PARQUET, install_local_boxscore_wrapper, load_v9b_namespace


ROOT = Path(__file__).resolve().parent
DEFAULT_VALIDATION_OVERRIDES_PATH = ROOT / "validation_overrides.csv"
NECESSITY_COMPARE_STATS = [
    "PTS",
    "AST",
    "STL",
    "BLK",
    "TOV",
    "PF",
    "FGA",
    "FGM",
    "3PA",
    "3PM",
    "FTA",
    "FTM",
    "OREB",
    "DRB",
    "REB",
]


@dataclass(frozen=True)
class GameVariantMetrics:
    error: str = ""
    darko_rows: int = 0
    event_stats_errors: int = 0
    rebound_deletions: int = 0
    audit_team_rows: int = 0
    audit_player_rows: int = 0
    audit_errors: int = 0


def prepare_single_game_df(game_df: pd.DataFrame) -> pd.DataFrame:
    df = game_df.copy()
    df.columns = [c.upper() for c in df.columns]
    if "WCTIMESTRING" not in df.columns:
        df["WCTIMESTRING"] = "00:00 AM"

    for col in [
        "HOMEDESCRIPTION",
        "VISITORDESCRIPTION",
        "NEUTRALSITEDESCRIPTION",
        "PLAYER1_NAME",
        "PLAYER2_NAME",
        "PLAYER3_NAME",
    ]:
        if col in df.columns:
            df[col] = df[col].fillna("")

    if "GAME_ID" in df.columns:
        df["GAME_ID"] = df["GAME_ID"].astype(str).str.zfill(10)

    for col in ["EVENTNUM", "EVENTMSGTYPE", "EVENTMSGACTIONTYPE", "PERIOD"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    for col in [
        "PLAYER1_ID",
        "PLAYER2_ID",
        "PLAYER3_ID",
        "PLAYER1_TEAM_ID",
        "PLAYER2_TEAM_ID",
        "PLAYER3_TEAM_ID",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    return df


def load_namespace_for_necessity(
    *,
    db_path: Path = DEFAULT_DB,
    validation_overrides_path: Path = DEFAULT_VALIDATION_OVERRIDES_PATH,
) -> tuple[dict, dict]:
    namespace = load_v9b_namespace()
    install_local_boxscore_wrapper(namespace, db_path.resolve())
    namespace["DB_PATH"] = db_path.resolve()
    validation_overrides = namespace["load_validation_overrides"](str(validation_overrides_path.resolve()))
    namespace["set_validation_overrides"](validation_overrides)
    return namespace, validation_overrides


def load_single_game_df(parquet_path: Path, game_id: str) -> pd.DataFrame:
    return prepare_single_game_df(_load_single_game_pbp(parquet_path.resolve(), game_id))


def ensure_box_columns(box: pd.DataFrame) -> pd.DataFrame:
    result = box.copy()
    for stat in NECESSITY_COMPARE_STATS:
        if stat not in result.columns:
            result[stat] = 0
        result[stat] = pd.to_numeric(result[stat], errors="coerce").fillna(0).astype(int)
    if "player_id" in result.columns:
        result["player_id"] = pd.to_numeric(result["player_id"], errors="coerce").fillna(0).astype(int)
    if "team_id" in result.columns:
        result["team_id"] = pd.to_numeric(result["team_id"], errors="coerce").fillna(0).astype(int)
    return result


def compare_boxes(box_with: pd.DataFrame, box_without: pd.DataFrame) -> tuple[int, int]:
    mismatch_players = 0
    mismatch_cells = 0
    merged = box_with.merge(
        box_without.rename(columns={stat: f"WITHOUT_{stat}" for stat in NECESSITY_COMPARE_STATS}),
        on=["player_id", "team_id"],
        how="outer",
    ).fillna(0)

    for _, player_row in merged.iterrows():
        player_mismatch = False
        for stat in NECESSITY_COMPARE_STATS:
            with_value = int(player_row.get(stat, 0))
            without_value = int(player_row.get(f"WITHOUT_{stat}", 0))
            if with_value != without_value:
                mismatch_cells += 1
                player_mismatch = True
        if player_mismatch:
            mismatch_players += 1

    return mismatch_players, mismatch_cells


def diff_pipeline_metrics(with_metrics: GameVariantMetrics, without_metrics: GameVariantMetrics) -> list[str]:
    diffs: list[str] = []
    if with_metrics.error != without_metrics.error:
        diffs.append(f"error:{without_metrics.error or 'none'}->{with_metrics.error or 'none'}")
    for field in [
        "darko_rows",
        "event_stats_errors",
        "rebound_deletions",
        "audit_team_rows",
        "audit_player_rows",
        "audit_errors",
    ]:
        with_value = getattr(with_metrics, field)
        without_value = getattr(without_metrics, field)
        if with_value != without_value:
            diffs.append(f"{field}:{without_value}->{with_value}")
    return diffs


def run_game_variant(
    namespace: dict,
    game_id: str,
    game_df: pd.DataFrame,
    *,
    validation_overrides: dict,
    tolerance: int = 2,
    run_boxscore_audit: bool = True,
) -> tuple[GameVariantMetrics, pd.DataFrame | None]:
    namespace["clear_event_stats_errors"]()
    namespace["clear_rebound_fallback_deletions"]()

    error = ""
    darko_rows = 0
    audit_team_rows = 0
    audit_player_rows = 0
    audit_errors = 0
    box = None
    possessions = None
    darko_df = None

    try:
        darko_df, possessions = namespace["generate_darko_hybrid"](game_id, game_df)
        darko_rows = len(darko_df)
        stat_rows = getattr(possessions, "manual_player_stats", possessions.player_stats)
        box = ensure_box_columns(build_pbp_boxscore_from_stat_rows(stat_rows))
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"

    if possessions is not None and darko_df is not None:
        try:
            namespace["assert_team_totals_match"](
                game_id,
                darko_df,
                possessions,
                tolerance=tolerance,
                overrides=validation_overrides,
            )
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"

        if run_boxscore_audit:
            try:
                official_box = namespace["fetch_boxscore_stats"](game_id)
                team_rows, player_rows, audit_error_rows = namespace["_build_game_boxscore_audit_rows"](
                    game_id,
                    getattr(possessions, "manual_player_stats", possessions.player_stats),
                    official_box,
                )
                audit_team_rows = len(team_rows)
                audit_player_rows = len(player_rows)
                audit_errors = len(audit_error_rows)
            except Exception:
                audit_errors += 1

    metrics = GameVariantMetrics(
        error=error,
        darko_rows=darko_rows,
        event_stats_errors=len(namespace.get("_event_stats_errors", [])),
        rebound_deletions=len(namespace.get("_rebound_fallback_deletions", [])),
        audit_team_rows=audit_team_rows,
        audit_player_rows=audit_player_rows,
        audit_errors=audit_errors,
    )
    return metrics, box
