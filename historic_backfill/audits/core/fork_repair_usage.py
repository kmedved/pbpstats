from __future__ import annotations

import argparse
import inspect
import json
import textwrap
from pathlib import Path
from typing import Callable

import pandas as pd

from historic_backfill.runners.cautious_rerun import DEFAULT_DB, DEFAULT_PARQUET
from historic_backfill.common.override_necessity_utils import load_namespace_for_necessity, load_single_game_df, run_game_variant


ROOT = Path(__file__).resolve().parent
DEFAULT_ROW_OVERRIDES_PATH = ROOT / "pbp_row_overrides.csv"
DEFAULT_OUTPUT_DIR = ROOT / "fork_repair_usage_20260315_v1"


def _normalize_game_id(value: str | int) -> str:
    return str(int(float(value))).zfill(10)


def _load_row_override_games(path: Path = DEFAULT_ROW_OVERRIDES_PATH) -> list[str]:
    df = pd.read_csv(path, dtype=str).fillna("")
    return sorted({_normalize_game_id(game_id) for game_id in df["game_id"]})


def _load_game_ids_from_file(path: Path) -> list[str]:
    return sorted(
        {
            _normalize_game_id(line.strip())
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        }
    )


def _pattern_rule_id(comment_line: str) -> str:
    label = comment_line.strip().replace("# --- ", "").replace(" ---", "")
    raw_id = (
        label.lower()
        .replace("pattern ", "")
        .replace(": ", "_")
        .replace(" ", "_")
        .replace("/", "_")
        .replace("+", "plus")
        .replace("-", "m")
        .replace(".", "_")
        .replace(",", "")
    )
    return f"processor.{raw_id}"


def _instrument_fix_event_order_source(func: Callable) -> Callable:
    source = textwrap.dedent(inspect.getsource(func))
    lines = source.splitlines()
    output: list[str] = []
    current_rule: str | None = None

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("# --- PATTERN"):
            current_rule = _pattern_rule_id(stripped)
        elif stripped.startswith("# --- FALLBACK"):
            current_rule = "processor.fallback.delete_orphan_rebound"

        if current_rule and stripped == "return":
            indent = line[: len(line) - len(line.lstrip())]
            prev_nonempty = next((candidate.strip() for candidate in reversed(output) if candidate.strip()), "")
            if "_record_repair(" not in prev_nonempty:
                output.append(f"{indent}self._record_repair({current_rule!r})")

        output.append(line)

    namespace = dict(func.__globals__)
    local_ns: dict[str, object] = {}
    exec("\n".join(output), namespace, local_ns)
    return local_ns[func.__name__]


def _instrument_silent_ft_source(func: Callable) -> Callable:
    source = textwrap.dedent(inspect.getsource(func))
    lines = source.splitlines()
    output: list[str] = []
    current_rule: str | None = None

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("# Reversed and-one / 1-of-1 block:"):
            current_rule = "processor.silent_ft.reversed_andone_block"
        elif stripped.startswith("# Reversed two-shot FT block with a real player rebound stranded after"):
            current_rule = "processor.silent_ft.reversed_two_shot_block"

        if current_rule and stripped == "changed = True":
            indent = line[: len(line) - len(line.lstrip())]
            prev_nonempty = next((candidate.strip() for candidate in reversed(output) if candidate.strip()), "")
            if "_record_repair(" not in prev_nonempty:
                output.append(f"{indent}self._record_repair({current_rule!r})")

        output.append(line)

    namespace = dict(func.__globals__)
    local_ns: dict[str, object] = {}
    exec("\n".join(output), namespace, local_ns)
    return local_ns[func.__name__]


def install_repair_usage_instrumentation():
    import pbpstats.offline.processor as processor_module

    if getattr(processor_module, "_CODEX_REPAIR_USAGE_INSTALLED", False):
        return processor_module

    processor_module._CURRENT_REPAIR_LOG = None
    cls = processor_module.PbpProcessor

    original_init = cls.__init__

    def instrumented_init(self, *args, **kwargs):
        self._repair_log = getattr(processor_module, "_CURRENT_REPAIR_LOG", None)
        return original_init(self, *args, **kwargs)

    def _record_repair(self, rule_id: str) -> None:
        repair_log = getattr(self, "_repair_log", None)
        if repair_log is None:
            return
        repair_log.append(
            {
                "game_id": getattr(self, "game_id", ""),
                "rule_id": rule_id,
            }
        )

    cls.__init__ = instrumented_init
    cls._record_repair = _record_repair
    cls._fix_event_order = _instrument_fix_event_order_source(cls._fix_event_order)
    cls._repair_silent_ft_rebound_windows = _instrument_silent_ft_source(cls._repair_silent_ft_rebound_windows)
    processor_module._CODEX_REPAIR_USAGE_INSTALLED = True
    return processor_module


def _set_row_override_mode(namespace: dict, disable_row_overrides: bool) -> None:
    if disable_row_overrides:
        namespace["apply_pbp_row_overrides"] = lambda game_df: game_df


def audit_games(
    *,
    game_ids: list[str],
    parquet_path: Path,
    db_path: Path,
    disable_row_overrides: bool,
    tolerance: int = 2,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    processor_module = install_repair_usage_instrumentation()
    namespace, validation_overrides = load_namespace_for_necessity(db_path=db_path)
    _set_row_override_mode(namespace, disable_row_overrides)

    mode = "raw_no_row_overrides" if disable_row_overrides else "current_production"
    game_rows: list[dict] = []
    hit_rows: list[dict] = []

    for game_id in game_ids:
        game_df = load_single_game_df(parquet_path, game_id)
        current_log: list[dict] = []
        processor_module._CURRENT_REPAIR_LOG = current_log
        try:
            metrics, _ = run_game_variant(
                namespace,
                game_id,
                game_df,
                validation_overrides=validation_overrides,
                tolerance=tolerance,
                run_boxscore_audit=True,
            )
        finally:
            processor_module._CURRENT_REPAIR_LOG = None

        unique_rules = sorted({row["rule_id"] for row in current_log})
        game_rows.append(
            {
                "mode": mode,
                "game_id": game_id,
                "repair_hit_count": len(current_log),
                "unique_rule_count": len(unique_rules),
                "repair_rules": "|".join(unique_rules),
                "error": metrics.error,
                "darko_rows": metrics.darko_rows,
                "event_stats_errors": metrics.event_stats_errors,
                "rebound_deletions": metrics.rebound_deletions,
                "audit_team_rows": metrics.audit_team_rows,
                "audit_player_rows": metrics.audit_player_rows,
                "audit_errors": metrics.audit_errors,
            }
        )
        for hit_index, hit in enumerate(current_log, start=1):
            hit_rows.append(
                {
                    "mode": mode,
                    "game_id": game_id,
                    "hit_index": hit_index,
                    "rule_id": hit["rule_id"],
                    "error": metrics.error,
                    "rebound_deletions": metrics.rebound_deletions,
                    "audit_team_rows": metrics.audit_team_rows,
                    "audit_player_rows": metrics.audit_player_rows,
                }
            )

    return pd.DataFrame(game_rows), pd.DataFrame(hit_rows)


def summarize_rule_hits(hit_df: pd.DataFrame) -> pd.DataFrame:
    if hit_df.empty:
        return pd.DataFrame(
            columns=[
                "mode",
                "rule_id",
                "games_hit",
                "total_hits",
                "games_with_errors",
                "games_with_rebound_deletions",
                "games_with_audit_rows",
                "sample_games",
            ]
        )

    working = hit_df.copy()
    for col in ["audit_team_rows", "audit_player_rows", "rebound_deletions", "error"]:
        if col not in working.columns:
            working[col] = 0 if col != "error" else ""

    grouped_rows: list[dict] = []
    for (mode, rule_id), group in working.groupby(["mode", "rule_id"], dropna=False):
        game_ids = sorted(group["game_id"].astype(str).unique())
        grouped_rows.append(
            {
                "mode": mode,
                "rule_id": rule_id,
                "games_hit": len(game_ids),
                "total_hits": int(len(group)),
                "games_with_errors": int(group.loc[group["error"].astype(str) != "", "game_id"].nunique()),
                "games_with_rebound_deletions": int(group.loc[pd.to_numeric(group["rebound_deletions"], errors="coerce").fillna(0) > 0, "game_id"].nunique()),
                "games_with_audit_rows": int(
                    group.loc[
                        (pd.to_numeric(group["audit_team_rows"], errors="coerce").fillna(0) > 0)
                        | (pd.to_numeric(group["audit_player_rows"], errors="coerce").fillna(0) > 0),
                        "game_id",
                    ].nunique()
                ),
                "sample_games": "|".join(game_ids[:10]),
            }
        )

    return pd.DataFrame(grouped_rows).sort_values(["mode", "games_hit", "total_hits", "rule_id"], ascending=[True, False, False, True]).reset_index(drop=True)


def summarize_rule_comparison(summary_df: pd.DataFrame) -> pd.DataFrame:
    if summary_df.empty:
        return pd.DataFrame()

    pivot = summary_df.pivot_table(
        index="rule_id",
        columns="mode",
        values="games_hit",
        aggfunc="first",
        fill_value=0,
    ).reset_index()
    for mode in ["current_production", "raw_no_row_overrides"]:
        if mode not in pivot.columns:
            pivot[mode] = 0

    def classify(row: pd.Series) -> str:
        current_hits = int(row["current_production"])
        raw_hits = int(row["raw_no_row_overrides"])
        if current_hits > 0:
            return "active_with_current_overrides"
        if raw_hits > 0:
            return "only_active_without_row_overrides"
        return "not_observed"

    pivot["usage_class"] = pivot.apply(classify, axis=1)
    return pivot.sort_values(["usage_class", "current_production", "raw_no_row_overrides", "rule_id"], ascending=[True, False, False, True]).reset_index(drop=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit which pbpstats fork repair families actually fire on historical games")
    parser.add_argument("--parquet-path", type=Path, default=DEFAULT_PARQUET)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--row-overrides-path", type=Path, default=DEFAULT_ROW_OVERRIDES_PATH)
    parser.add_argument("--game-ids-file", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--mode", choices=["both", "current", "raw"], default="both")
    parser.add_argument("--limit-games", type=int, default=0)
    parser.add_argument("--tolerance", type=int, default=2)
    args = parser.parse_args()

    if args.game_ids_file is not None:
        game_ids = _load_game_ids_from_file(args.game_ids_file.resolve())
    else:
        game_ids = _load_row_override_games(args.row_overrides_path)
    if args.limit_games > 0:
        game_ids = game_ids[: args.limit_games]

    frames_games: list[pd.DataFrame] = []
    frames_hits: list[pd.DataFrame] = []
    current_games = pd.DataFrame()
    raw_games = pd.DataFrame()

    if args.mode in ("both", "current"):
        current_games, current_hits = audit_games(
            game_ids=game_ids,
            parquet_path=args.parquet_path.resolve(),
            db_path=args.db_path.resolve(),
            disable_row_overrides=False,
            tolerance=args.tolerance,
        )
        frames_games.append(current_games)
        frames_hits.append(current_hits)

    if args.mode in ("both", "raw"):
        raw_games, raw_hits = audit_games(
            game_ids=game_ids,
            parquet_path=args.parquet_path.resolve(),
            db_path=args.db_path.resolve(),
            disable_row_overrides=True,
            tolerance=args.tolerance,
        )
        frames_games.append(raw_games)
        frames_hits.append(raw_hits)

    game_results = pd.concat(frames_games, ignore_index=True) if frames_games else pd.DataFrame()
    hit_results = pd.concat(frames_hits, ignore_index=True) if frames_hits else pd.DataFrame()
    rule_summary = summarize_rule_hits(hit_results)
    rule_comparison = summarize_rule_comparison(rule_summary)

    summary = {
        "mode": args.mode,
        "games_audited": len(game_ids),
        "current_production_games_with_repairs": int((pd.to_numeric(current_games["repair_hit_count"], errors="coerce").fillna(0) > 0).sum()) if not current_games.empty else 0,
        "raw_no_row_games_with_repairs": int((pd.to_numeric(raw_games["repair_hit_count"], errors="coerce").fillna(0) > 0).sum()) if not raw_games.empty else 0,
        "current_production_rules_active": int((rule_comparison["current_production"] > 0).sum()) if not rule_comparison.empty else 0,
        "raw_no_row_rules_active": int((rule_comparison["raw_no_row_overrides"] > 0).sum()) if not rule_comparison.empty else 0,
        "usage_class_counts": rule_comparison["usage_class"].value_counts(dropna=False).to_dict() if not rule_comparison.empty else {},
    }

    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    game_results.to_csv(output_dir / "fork_repair_usage_game_results.csv", index=False)
    hit_results.to_csv(output_dir / "fork_repair_usage_hit_log.csv", index=False)
    rule_summary.to_csv(output_dir / "fork_repair_usage_rule_summary.csv", index=False)
    rule_comparison.to_csv(output_dir / "fork_repair_usage_rule_comparison.csv", index=False)
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
