import builtins
import importlib

import pandas as pd
import pytest

import pbpstats

adapter = importlib.import_module("pbpstats.offline.nba_on_court")


def test_load_nba_on_court_pbp_loads_nbastats_and_normalizes_game_ids(monkeypatch):
    captured = {}

    def fake_load_nba_data(**kwargs):
        captured.update(kwargs)
        return pd.DataFrame(
            [
                {"GAME_ID": 22300001, "EVENTNUM": 1},
                {"GAME_ID": "0022300002", "EVENTNUM": 2},
            ]
        )

    monkeypatch.setattr(
        adapter,
        "_import_load_nba_data",
        lambda: fake_load_nba_data,
    )

    game_df = adapter.load_nba_on_court_pbp(
        2023,
        league=pbpstats.NBA_STRING,
        season_type="rg",
        path="cache-dir",
    )

    assert captured == {
        "path": "cache-dir",
        "seasons": 2023,
        "data": "nbastats",
        "seasontype": "rg",
        "league": pbpstats.NBA_STRING,
        "in_memory": True,
        "use_pandas": True,
    }
    assert game_df["GAME_ID"].tolist() == ["0022300001", "0022300002"]


def test_get_possessions_from_nba_on_court_filters_nba_game_and_forwards_options(
    monkeypatch,
):
    captured = {}
    sentinel = object()
    fetch_pbp_v3_fn = object()
    rebound_deletions_list = []
    boxscore_source_loader = object()
    period_boxscore_source_loader = object()

    def fake_load_nba_on_court_pbp(season, league, season_type, path):
        captured["load_args"] = {
            "season": season,
            "league": league,
            "season_type": season_type,
            "path": path,
        }
        return pd.DataFrame(
            [
                {"GAME_ID": 22300001, "EVENTNUM": 1},
                {"GAME_ID": 22300002, "EVENTNUM": 2},
            ]
        )

    def fake_get_possessions_from_df(game_df, **kwargs):
        captured["game_df"] = game_df
        captured["processor_kwargs"] = kwargs
        return sentinel

    monkeypatch.setattr(
        adapter,
        "load_nba_on_court_pbp",
        fake_load_nba_on_court_pbp,
    )
    monkeypatch.setattr(
        adapter,
        "get_possessions_from_df",
        fake_get_possessions_from_df,
    )

    result = adapter.get_possessions_from_nba_on_court(
        2023,
        22300001,
        league=pbpstats.NBA_STRING,
        season_type="po",
        path="cache-dir",
        fetch_pbp_v3_fn=fetch_pbp_v3_fn,
        rebound_deletions_list=rebound_deletions_list,
        boxscore_source_loader=boxscore_source_loader,
        period_boxscore_source_loader=period_boxscore_source_loader,
        file_directory="pbpstats-data",
    )

    assert result is sentinel
    assert captured["load_args"] == {
        "season": 2023,
        "league": pbpstats.NBA_STRING,
        "season_type": "po",
        "path": "cache-dir",
    }
    assert captured["game_df"]["GAME_ID"].tolist() == ["0022300001"]
    assert captured["processor_kwargs"] == {
        "fetch_pbp_v3_fn": fetch_pbp_v3_fn,
        "rebound_deletions_list": rebound_deletions_list,
        "boxscore_source_loader": boxscore_source_loader,
        "period_boxscore_source_loader": period_boxscore_source_loader,
        "file_directory": "pbpstats-data",
        "league": pbpstats.NBA_STRING,
    }


def test_get_possessions_from_nba_on_court_normalizes_wnba_game_ids(monkeypatch):
    captured = {}
    sentinel = object()

    monkeypatch.setattr(
        adapter,
        "load_nba_on_court_pbp",
        lambda *_args, **_kwargs: pd.DataFrame(
            [
                {"GAME_ID": 1022500234.0, "EVENTNUM": 1},
                {"GAME_ID": "22500235", "EVENTNUM": 2},
            ]
        ),
    )

    def fake_get_possessions_from_df(game_df, **kwargs):
        captured["game_df"] = game_df
        captured["league"] = kwargs["league"]
        return sentinel

    monkeypatch.setattr(
        adapter,
        "get_possessions_from_df",
        fake_get_possessions_from_df,
    )

    result = adapter.get_possessions_from_nba_on_court(
        2025,
        "22500234",
        league=pbpstats.WNBA_STRING,
    )

    assert result is sentinel
    assert captured["game_df"]["GAME_ID"].tolist() == ["1022500234"]
    assert captured["league"] == pbpstats.WNBA_STRING


def test_get_possessions_from_nba_on_court_raises_when_game_not_found(monkeypatch):
    monkeypatch.setattr(
        adapter,
        "load_nba_on_court_pbp",
        lambda *_args, **_kwargs: pd.DataFrame(
            [{"GAME_ID": "0022300002", "EVENTNUM": 2}]
        ),
    )

    with pytest.raises(ValueError, match="No nba-on-court PBP rows found"):
        adapter.get_possessions_from_nba_on_court(2023, "0022300001")


def test_load_nba_on_court_pbp_reports_missing_optional_dependency(monkeypatch):
    real_import = builtins.__import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "nba_on_court.nba_on_court":
            raise ImportError("missing nba-on-court")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    with pytest.raises(ImportError) as exc_info:
        adapter._import_load_nba_data()

    message = str(exc_info.value)
    assert "nba-on-court is required" in message
    assert "pip install git+https://github.com/shufinskiy/nba-on-court.git" in message


def test_nba_on_court_helpers_are_exported_from_offline_package():
    from pbpstats.offline import (
        get_possessions_from_nba_on_court,
        load_nba_on_court_pbp,
    )

    assert (
        get_possessions_from_nba_on_court is adapter.get_possessions_from_nba_on_court
    )
    assert load_nba_on_court_pbp is adapter.load_nba_on_court_pbp
