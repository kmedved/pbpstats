import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from historic_backfill.runners.scrape_period_starters_v5 import (
    FAILURE_COLS,
    FAILURES_PATH,
    RESOLVED_COLS,
    RESOLVED_PATH,
    UNRESOLVED_COLS,
    UNRESOLVED_PATH,
    build_jobs,
    period_start_tenths,
    seconds_to_tenths,
)


DATA_DIR = Path(".")
V4_RESOLVED_PATH = DATA_DIR / "period_starters_v4.parquet"
V4_UNRESOLVED_PATH = DATA_DIR / "period_starters_unresolved_v4.parquet"
V4_FAILURES_PATH = DATA_DIR / "period_starters_failures_v4.parquet"

QUEUE_PATH = DATA_DIR / "period_starters_v5_rescrape_queue.parquet"
SUMMARY_PATH = DATA_DIR / "period_starters_v5_migration_summary.json"

MIGRATED_RESOLVER_MODE = "migrated_from_v4_same_window"


def load_unique_periods(path):
    if not path.exists():
        return set()
    df = pd.read_parquet(path, columns=["game_id", "period"])
    df["game_id"] = df["game_id"].astype(str).str.zfill(10)
    return set(zip(df["game_id"], df["period"]))


def assert_outputs_absent():
    existing = [path for path in [RESOLVED_PATH, UNRESOLVED_PATH, FAILURES_PATH, QUEUE_PATH, SUMMARY_PATH] if path.exists()]
    if existing:
        joined = ", ".join(str(path) for path in existing)
        raise FileExistsError(f"Refusing to overwrite existing v5 migration artifacts: {joined}")


def build_rescrape_reason(row):
    reasons = []
    if bool(row["was_v4_resolved"]) and not bool(row["carried_forward"]):
        reasons.append("window_changed")
    if bool(row["was_v4_unresolved"]):
        reasons.append("v4_unresolved")
    if bool(row["was_v4_failure"]):
        reasons.append("v4_failure")
    return ",".join(reasons)


def main():
    assert_outputs_absent()

    jobs = build_jobs()
    jobs = jobs[
        [
            "game_id",
            "period",
            "first_event_elapsed",
            "first_nonzero_event_elapsed",
            "anchor_elapsed",
            "first_sub_elapsed",
            "requested_window_seconds",
            "window_seconds",
            "window_capped_by_sub",
        ]
    ].copy()

    v4_resolved = pd.read_parquet(V4_RESOLVED_PATH)
    v4_resolved["game_id"] = v4_resolved["game_id"].astype(str).str.zfill(10)

    merged = v4_resolved.merge(
        jobs.rename(
            columns={
                "first_event_elapsed": "v5_first_event_elapsed",
                "first_nonzero_event_elapsed": "v5_first_nonzero_event_elapsed",
                "anchor_elapsed": "v5_anchor_elapsed",
                "first_sub_elapsed": "v5_first_sub_elapsed",
                "requested_window_seconds": "v5_requested_window_seconds",
                "window_seconds": "v5_window_seconds",
                "window_capped_by_sub": "v5_window_capped_by_sub",
            }
        ),
        on=["game_id", "period"],
        how="left",
    )

    same_window = merged["window_seconds"].round(6) == merged["v5_window_seconds"].round(6)
    carried = merged[same_window].copy()

    carried["start_range"] = carried["period"].map(period_start_tenths)
    carried["end_range"] = carried["start_range"] + carried["v5_window_seconds"].map(seconds_to_tenths)
    carried["window_seconds"] = carried["v5_window_seconds"]
    carried["requested_window_seconds"] = carried["v5_requested_window_seconds"]
    carried["first_event_elapsed"] = carried["v5_first_event_elapsed"]
    carried["first_nonzero_event_elapsed"] = carried["v5_first_nonzero_event_elapsed"]
    carried["anchor_elapsed"] = carried["v5_anchor_elapsed"]
    carried["first_sub_elapsed"] = carried["v5_first_sub_elapsed"]
    carried["window_capped_by_sub"] = carried["v5_window_capped_by_sub"]
    carried["resolver_mode"] = MIGRATED_RESOLVER_MODE
    carried["resolved"] = True
    carried = carried[RESOLVED_COLS].copy()

    unresolved_keys = load_unique_periods(V4_UNRESOLVED_PATH)
    failure_keys = load_unique_periods(V4_FAILURES_PATH)
    carried_keys = set(zip(carried["game_id"], carried["period"]))

    v4_resolved_meta = (
        v4_resolved[["game_id", "period", "window_seconds", "resolver_mode"]]
        .rename(
            columns={
                "window_seconds": "v4_window_seconds",
                "resolver_mode": "v4_resolver_mode",
            }
        )
        .copy()
    )

    queue = jobs.merge(v4_resolved_meta, on=["game_id", "period"], how="left")
    queue["was_v4_resolved"] = queue["v4_window_seconds"].notna()
    queue["carried_forward"] = [
        (game_id, period) in carried_keys
        for game_id, period in zip(queue["game_id"], queue["period"])
    ]
    queue["was_v4_unresolved"] = [
        (game_id, period) in unresolved_keys
        for game_id, period in zip(queue["game_id"], queue["period"])
    ]
    queue["was_v4_failure"] = [
        (game_id, period) in failure_keys
        for game_id, period in zip(queue["game_id"], queue["period"])
    ]
    queue["rescrape_reason"] = queue.apply(build_rescrape_reason, axis=1)
    queue = queue[queue["rescrape_reason"] != ""].copy()

    queue = queue[
        [
            "game_id",
            "period",
            "first_event_elapsed",
            "first_nonzero_event_elapsed",
            "anchor_elapsed",
            "first_sub_elapsed",
            "requested_window_seconds",
            "window_seconds",
            "window_capped_by_sub",
            "v4_window_seconds",
            "v4_resolver_mode",
            "rescrape_reason",
            "was_v4_resolved",
            "was_v4_unresolved",
            "was_v4_failure",
            "carried_forward",
        ]
    ].sort_values(["game_id", "period"]).reset_index(drop=True)

    carried.to_parquet(RESOLVED_PATH, index=False)
    pd.DataFrame(columns=UNRESOLVED_COLS).to_parquet(UNRESOLVED_PATH, index=False)
    pd.DataFrame(columns=FAILURE_COLS).to_parquet(FAILURES_PATH, index=False)
    queue.to_parquet(QUEUE_PATH, index=False)

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "v4_resolved_periods": int(v4_resolved.drop_duplicates(["game_id", "period"]).shape[0]),
        "v4_unresolved_periods": int(len(unresolved_keys)),
        "v4_failure_periods": int(len(failure_keys)),
        "v5_carried_forward_periods": int(len(carried)),
        "v5_rescrape_queue_periods": int(len(queue)),
        "rescrape_reason_counts": queue["rescrape_reason"].value_counts().to_dict(),
    }
    SUMMARY_PATH.write_text(json.dumps(summary, indent=2))

    print(f"Seeded v5 resolved parquet: {RESOLVED_PATH}")
    print(f"Created empty v5 unresolved parquet: {UNRESOLVED_PATH}")
    print(f"Created empty v5 failures parquet: {FAILURES_PATH}")
    print(f"Created v5 rescrape queue: {QUEUE_PATH}")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
