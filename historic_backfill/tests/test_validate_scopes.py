from pathlib import Path
import sqlite3

import pandas as pd
import pytest

from historic_backfill.runners.validate import validate_scope


def _touch(root: Path, rel_path: str) -> None:
    path = root / rel_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("fixture\n", encoding="utf-8")


def _write_valid_nba_db(
    root: Path,
    endpoints: tuple[str, ...] = ("boxscore", "summary", "pbpv3"),
) -> None:
    path = root / "data" / "nba_raw.db"
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "CREATE TABLE raw_responses (game_id TEXT, endpoint TEXT, team_id INTEGER, data BLOB)"
        )
        for endpoint in endpoints:
            conn.execute(
                "INSERT INTO raw_responses (game_id, endpoint, team_id, data) VALUES (?, ?, NULL, ?)",
                ("0029700001", endpoint, b"{}"),
            )
        conn.commit()
    finally:
        conn.close()


def _write_core_inputs(root: Path) -> None:
    _write_valid_nba_db(root)
    parquet_path = root / "data" / "playbyplayv2.parq"
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "GAME_ID": ["0029700001"],
            "EVENTNUM": [1],
            "EVENTMSGTYPE": [12],
            "PERIOD": [1],
            "SEASON": [1997],
        }
    ).to_parquet(parquet_path)
    for rel_path in (
        "data/period_starters_v6.parquet",
        "data/period_starters_v5.parquet",
    ):
        _touch(root, rel_path)


def _write_core_catalogs(root: Path) -> None:
    catalogs = root / "catalogs"
    (catalogs / "overrides").mkdir(parents=True, exist_ok=True)
    (catalogs / "pbp_row_overrides.csv").write_text(
        "game_id,action,event_num,anchor_event_num,notes,period,pctimestring,wctimestring,description_side,"
        "player_out_id,player_out_name,player_out_team_id,player_in_id,player_in_name,player_in_team_id\n"
        "0020400335,insert_sub_before,148,149,canary,2,7:59,,home,"
        "2747,JR Smith,1610612740,2454,Junior Harrington,1610612740\n",
        encoding="utf-8",
    )
    (catalogs / "pbp_stat_overrides.csv").write_text(
        "game_id,team_id,player_id,stat_key,stat_value,notes\n",
        encoding="utf-8",
    )
    (catalogs / "validation_overrides.csv").write_text(
        "game_id,action,tolerance,notes\n",
        encoding="utf-8",
    )
    (catalogs / "boxscore_source_overrides.csv").write_text(
        "game_id,GAME_ID,TEAM_ID,TEAM_ABBREVIATION,TEAM_CITY,PLAYER_ID,PLAYER_NAME,"
        "NICKNAME,START_POSITION,COMMENT,MIN,FGM,FGA,FG_PCT,FG3M,FG3A,FG3_PCT,"
        "FTM,FTA,FT_PCT,OREB,DREB,REB,AST,STL,BLK,TO,PF,PTS,PLUS_MINUS,notes\n",
        encoding="utf-8",
    )
    (catalogs / "overrides" / "correction_manifest.json").write_text(
        '{"manifest_version": "test", "corrections": [], "residual_annotations": []}\n',
        encoding="utf-8",
    )
    (catalogs / "overrides" / "period_starters_overrides.json").write_text(
        "{}\n",
        encoding="utf-8",
    )
    (catalogs / "overrides" / "lineup_window_overrides.json").write_text(
        "{}\n",
        encoding="utf-8",
    )


def test_core_validation_requires_nba_inputs_without_checking_cross_source(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)

    result = validate_scope("core", root=tmp_path)

    assert result.ok is True
    assert result.validation_level == "input_preflight"
    assert result.missing_required == []
    assert result.skipped_optional == []
    assert result.validation_errors == []


def test_core_validation_reports_invalid_catalogs(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    (tmp_path / "catalogs" / "pbp_row_overrides.csv").write_text(
        "game_id,action,event_num,anchor_event_num,notes\n"
        "0020400335,teleport,148,149,bad action\n",
        encoding="utf-8",
    )

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert result.validation_errors


def test_core_validation_reports_invalid_catalogs_even_when_nba_inputs_are_missing(
    tmp_path,
):
    _write_core_catalogs(tmp_path)
    (tmp_path / "catalogs" / "validation_overrides.csv").write_text(
        "game_id\n",
        encoding="utf-8",
    )

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert "data/nba_raw.db" in result.missing_required
    assert result.validation_errors
    assert any(
        "validation_overrides.csv" in error for error in result.validation_errors
    )


def test_core_validation_reports_invalid_nba_raw_db_contents(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    (tmp_path / "data" / "nba_raw.db").unlink()
    _write_valid_nba_db(tmp_path, endpoints=("boxscore", "summary"))

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any("pbpv3" in error for error in result.validation_errors)


def test_core_validation_reports_invalid_playbyplay_v2_parquet(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    (tmp_path / "data" / "playbyplayv2.parq").write_text(
        "not parquet\n",
        encoding="utf-8",
    )

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any("playbyplayv2.parq" in error for error in result.validation_errors)


def test_core_validation_reports_missing_playbyplay_v2_columns(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    pd.DataFrame(
        {
            "game_id": ["0029700001"],
            "eventnum": [1],
            "eventmsgtype": [12],
            "period": [1],
        }
    ).to_parquet(tmp_path / "data" / "playbyplayv2.parq")

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any("SEASON" in error for error in result.validation_errors)


@pytest.mark.parametrize(
    ("rel_path", "bad_contents"),
    [
        ("catalogs/pbp_stat_overrides.csv", "game_id\n"),
        (
            "catalogs/pbp_stat_overrides.csv",
            "game_id,team_id,player_id,stat_key,stat_value,notes\n"
            "0029600332,team,769,UnknownDistance2ptDefRebounds,1,bad team\n",
        ),
        ("catalogs/validation_overrides.csv", "game_id\n"),
        (
            "catalogs/validation_overrides.csv",
            "game_id,action,tolerance,notes\n" "29600370,teleport,5,bad action\n",
        ),
        (
            "catalogs/validation_overrides.csv",
            "game_id,action,tolerance,notes\n" "29600370,allow,-1,bad tolerance\n",
        ),
        ("catalogs/boxscore_source_overrides.csv", "game_id\n"),
        ("catalogs/overrides/correction_manifest.json", "{not json\n"),
        (
            "catalogs/overrides/correction_manifest.json",
            '{"manifest_version": "test"}\n',
        ),
        ("catalogs/overrides/period_starters_overrides.json", "{not json\n"),
        ("catalogs/overrides/lineup_window_overrides.json", "{not json\n"),
    ],
)
def test_core_validation_reports_invalid_non_pbp_row_catalogs(
    tmp_path, rel_path, bad_contents
):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    (tmp_path / rel_path).write_text(bad_contents, encoding="utf-8")

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert result.validation_errors


def test_core_validation_reports_invalid_correction_manifest_semantics(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    (tmp_path / "catalogs" / "overrides" / "correction_manifest.json").write_text(
        """
{
  "manifest_version": "test",
  "corrections": [
    {
      "correction_id": "bad_lineup",
      "status": "active",
      "domain": "lineup",
      "scope_type": "period_start",
      "authoring_mode": "explicit",
      "game_id": "0029700001",
      "period": 1,
      "team_id": 1610612740,
      "lineup_player_ids": [1, 2, 3, 4],
      "reason_code": "test",
      "evidence_summary": "test",
      "source_primary": "manual_trace",
      "preferred_source": "manual_trace"
    }
  ],
  "residual_annotations": []
}
""",
        encoding="utf-8",
    )

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any(
        "does not resolve to 5 players" in error for error in result.validation_errors
    )


def test_core_validation_reports_invalid_delta_correction_schema(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    (tmp_path / "catalogs" / "overrides" / "correction_manifest.json").write_text(
        """
{
  "manifest_version": "test",
  "corrections": [
    {
      "correction_id": "bad_delta",
      "status": "active",
      "domain": "lineup",
      "scope_type": "period_start",
      "authoring_mode": "delta",
      "game_id": "0029700001",
      "period": 1,
      "team_id": 1610612740,
      "swap_out_player_id": "abc",
      "reason_code": "test",
      "evidence_summary": "test",
      "source_primary": "manual_trace",
      "preferred_source": "manual_trace"
    }
  ],
  "residual_annotations": []
}
""",
        encoding="utf-8",
    )

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any("swap_out_player_id" in error for error in result.validation_errors)


def test_core_validation_reports_stale_compiled_runtime_views(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    (tmp_path / "catalogs" / "overrides" / "correction_manifest.json").write_text(
        """
{
  "manifest_version": "test",
  "corrections": [
    {
      "correction_id": "period_start_fix",
      "status": "active",
      "domain": "lineup",
      "scope_type": "period_start",
      "authoring_mode": "explicit",
      "game_id": "0029700001",
      "period": 1,
      "team_id": 1610612740,
      "lineup_player_ids": [1, 2, 3, 4, 5],
      "reason_code": "test",
      "evidence_summary": "test",
      "source_primary": "manual_trace",
      "preferred_source": "manual_trace"
    }
  ],
  "residual_annotations": []
}
""",
        encoding="utf-8",
    )

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any(
        "period_starters_overrides.json does not match" in error
        for error in result.validation_errors
    )


def test_cross_source_validation_skips_missing_optional_inputs(tmp_path):
    result = validate_scope("cross-source", root=tmp_path)

    assert result.ok is True
    assert result.validation_level == "optional_diagnostic_preflight"
    assert "data/bbr/bbref_boxscores.db" in result.skipped_optional
    assert "data/tpdev/full_pbp_new.parq" in result.skipped_optional


def test_provenance_validation_fails_when_evidence_inputs_are_missing(tmp_path):
    result = validate_scope("provenance", root=tmp_path)

    assert result.ok is False
    assert result.validation_level == "provenance_evidence_preflight"
    assert "data/bbr/bbref_boxscores.db" in result.missing_required
    assert "data/tpdev/tpdev_box.parq" in result.missing_required
