from pathlib import Path

from audit_period_starters_against_tpdev import build_period_starter_audit


def test_build_period_starter_audit_handles_missing_tpdev_rows(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(
        "audit_period_starters_against_tpdev.load_v9b_namespace",
        lambda: {},
    )
    monkeypatch.setattr(
        "audit_period_starters_against_tpdev.install_local_boxscore_wrapper",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "audit_period_starters_against_tpdev._load_current_game_possessions",
        lambda _namespace, _game_id, _parquet_path: (object(), {11: "Starter One", 22: "Starter Two"}),
    )
    monkeypatch.setattr(
        "audit_period_starters_against_tpdev._extract_current_period_starters",
        lambda _possessions, _name_map: [
            {
                "game_id": "0029600001",
                "period": 1,
                "team_id": 1610612737,
                "current_starter_ids": [11, 22],
                "current_starter_names": ["Starter One", "Starter Two"],
            }
        ],
    )
    monkeypatch.setattr(
        "audit_period_starters_against_tpdev._load_tpdev_period_starters",
        lambda _tpdev_pbp_path, _game_id, _name_map: [],
    )

    audit_df = build_period_starter_audit(
        game_ids=["0029600001"],
        parquet_path=tmp_path / "fake.parq",
        db_path=tmp_path / "fake.db",
        tpdev_pbp_path=tmp_path / "fake_tpdev.parq",
    )

    assert len(audit_df) == 1
    row = audit_df.iloc[0]
    assert row["game_id"] == "0029600001"
    assert row["period"] == 1
    assert row["team_id"] == 1610612737
    assert row["current_starter_ids"] == [11, 22]
    assert row["tpdev_starter_ids"] == []
    assert bool(row["starter_sets_match"]) is False
    assert row["extra_in_current_ids"] == [11, 22]
