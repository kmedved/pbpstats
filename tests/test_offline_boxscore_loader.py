import pandas as pd
import pytest

import pbpstats
from pbpstats.offline import processor as processor_module
from pbpstats.resources.enhanced_pbp import StartOfPeriod


class DummyBoxscoreLoader:
    def load_data(self):
        return {"resultSets": []}


class DummyPeriodBoxscoreLoader:
    def load_data(self, game_id, period, mode):
        return {"boxScoreTraditional": {}}


class DummyStartOfPeriod(StartOfPeriod):
    def __init__(self, data, order):
        self.data = data
        self.order = order
        self.game_id = data.get("GAME_ID")

    def get_period_starters(self, file_directory=None):
        return {}


class DummyNonStartEvent:
    def __init__(self, data, order):
        self.data = data
        self.order = order


class SlottedNonStartEvent:
    __slots__ = ("data", "order")

    def __init__(self, data, order):
        self.data = data
        self.order = order


def test_pbp_processor_attaches_boxscore_loader_to_start_of_period_items(monkeypatch):
    class DummyFactory:
        def get_event_class(self, event_type):
            return DummyStartOfPeriod if int(event_type) == 12 else DummyNonStartEvent

    def fake_process(self, max_retries):
        self._build_items_from_data()
        self.possessions = []

    monkeypatch.setattr(processor_module, "StatsNbaEnhancedPbpFactory", DummyFactory)
    monkeypatch.setattr(
        processor_module.PbpProcessor, "_process_with_retries", fake_process
    )

    loader = DummyBoxscoreLoader()
    processor = processor_module.PbpProcessor(
        "0021900001",
        [
            {"EVENTMSGTYPE": 12, "EVENTNUM": 0},
            {"EVENTMSGTYPE": 1, "EVENTNUM": 1},
        ],
        boxscore_source_loader=loader,
    )

    assert processor.items[0].boxscore_source_loader is loader
    assert not hasattr(processor.items[1], "boxscore_source_loader")


def test_pbp_processor_attaches_period_boxscore_loader_to_start_of_period_items(
    monkeypatch,
):
    class DummyFactory:
        def get_event_class(self, event_type):
            return DummyStartOfPeriod if int(event_type) == 12 else DummyNonStartEvent

    def fake_process(self, max_retries):
        self._build_items_from_data()
        self.possessions = []

    monkeypatch.setattr(processor_module, "StatsNbaEnhancedPbpFactory", DummyFactory)
    monkeypatch.setattr(
        processor_module.PbpProcessor, "_process_with_retries", fake_process
    )

    loader = DummyPeriodBoxscoreLoader()
    processor = processor_module.PbpProcessor(
        "0021900001",
        [
            {"EVENTMSGTYPE": 12, "EVENTNUM": 0},
            {"EVENTMSGTYPE": 1, "EVENTNUM": 1},
        ],
        period_boxscore_source_loader=loader,
    )

    assert processor.items[0].period_boxscore_source_loader is loader
    assert not hasattr(processor.items[1], "period_boxscore_source_loader")


def test_pbp_processor_infers_wnba_league_from_game_id(monkeypatch):
    def fake_process(self, max_retries):
        self.possessions = []

    monkeypatch.setattr(
        processor_module.PbpProcessor, "_process_with_retries", fake_process
    )

    processor = processor_module.PbpProcessor("1022500234", [])

    assert processor.league == pbpstats.WNBA_STRING


def test_pbp_processor_honors_explicit_league_override(monkeypatch):
    def fake_process(self, max_retries):
        self.possessions = []

    monkeypatch.setattr(
        processor_module.PbpProcessor, "_process_with_retries", fake_process
    )

    processor = processor_module.PbpProcessor(
        "0022500234",
        [],
        league=pbpstats.WNBA_STRING,
    )

    assert processor.league == pbpstats.WNBA_STRING


def test_pbp_processor_normalizes_short_wnba_game_ids_and_raw_rows(monkeypatch):
    def fake_process(self, max_retries):
        self.possessions = []

    monkeypatch.setattr(
        processor_module.PbpProcessor, "_process_with_retries", fake_process
    )

    processor = processor_module.PbpProcessor(
        "22500234",
        [{"EVENTMSGTYPE": 12, "EVENTNUM": 0, "GAME_ID": "22500234"}],
        league=pbpstats.WNBA_STRING,
    )

    assert processor.game_id == "1022500234"
    assert processor.data[0]["GAME_ID"] == "1022500234"


def test_pbp_processor_coerces_raw_int_fields_and_uses_processor_game_id(
    monkeypatch,
):
    def fake_process(self, max_retries):
        self.possessions = []

    monkeypatch.setattr(
        processor_module.PbpProcessor, "_process_with_retries", fake_process
    )

    processor = processor_module.PbpProcessor(
        "22500234",
        [
            {
                "GAME_ID": float("nan"),
                "EVENTMSGTYPE": "12",
                "EVENTNUM": "0.0",
                "EVENTMSGACTIONTYPE": "0",
                "PERIOD": "1",
            }
        ],
        league=pbpstats.WNBA_STRING,
    )

    assert processor.data[0]["GAME_ID"] == "1022500234"
    assert processor.data[0]["EVENTMSGTYPE"] == 12
    assert processor.data[0]["EVENTNUM"] == 0
    assert processor.data[0]["EVENTMSGACTIONTYPE"] == 0
    assert processor.data[0]["PERIOD"] == 1


def test_pbp_processor_propagates_league_override_to_start_items(monkeypatch):
    class DummyFactory:
        def get_event_class(self, event_type):
            return DummyStartOfPeriod if int(event_type) == 12 else DummyNonStartEvent

    def fake_process(self, max_retries):
        self._build_items_from_data()
        self.possessions = []

    monkeypatch.setattr(processor_module, "StatsNbaEnhancedPbpFactory", DummyFactory)
    monkeypatch.setattr(
        processor_module.PbpProcessor, "_process_with_retries", fake_process
    )

    processor = processor_module.PbpProcessor(
        "0022500234",
        [
            {"EVENTMSGTYPE": 12, "EVENTNUM": 0},
        ],
        league=pbpstats.WNBA_STRING,
    )

    assert processor.items[0].loader_league == pbpstats.WNBA_STRING
    assert processor.items[0].league == pbpstats.WNBA_STRING
    assert processor.items[0].game_id == "1022500234"


def test_pbp_processor_does_not_require_loader_league_attr_on_non_start_items(
    monkeypatch,
):
    class DummyFactory:
        def get_event_class(self, event_type):
            return SlottedNonStartEvent

    def fake_process(self, max_retries):
        self._build_items_from_data()
        self.possessions = []

    monkeypatch.setattr(processor_module, "StatsNbaEnhancedPbpFactory", DummyFactory)
    monkeypatch.setattr(
        processor_module.PbpProcessor, "_process_with_retries", fake_process
    )

    processor = processor_module.PbpProcessor(
        "0021900001",
        [{"EVENTMSGTYPE": 1, "EVENTNUM": 1}],
    )

    assert isinstance(processor.items[0], SlottedNonStartEvent)


def test_get_possessions_from_df_forwards_boxscore_loader(monkeypatch):
    captured = {}

    class DummyProcessor:
        def __init__(
            self,
            game_id,
            raw_data_dicts,
            rebound_deletions_list=None,
            boxscore_source_loader=None,
            period_boxscore_source_loader=None,
            file_directory=None,
        ):
            captured["game_id"] = game_id
            captured["raw_data_dicts"] = raw_data_dicts
            captured["rebound_deletions_list"] = rebound_deletions_list
            captured["boxscore_source_loader"] = boxscore_source_loader
            captured["period_boxscore_source_loader"] = period_boxscore_source_loader
            captured["file_directory"] = file_directory
            self.possessions = []

    monkeypatch.setattr(processor_module, "dedupe_with_v3", lambda df, *_args: df)
    monkeypatch.setattr(
        processor_module, "patch_start_of_periods", lambda df, *_args: df
    )
    monkeypatch.setattr(
        processor_module, "preserve_order_after_v3_repairs", lambda df: df
    )
    monkeypatch.setattr(processor_module, "_ensure_eventnum_int", lambda df: df)
    monkeypatch.setattr(
        processor_module,
        "create_raw_dicts_from_df",
        lambda df: [{"EVENTNUM": int(df.iloc[0]["EVENTNUM"])}],
    )
    monkeypatch.setattr(processor_module, "PbpProcessor", DummyProcessor)

    loader = DummyBoxscoreLoader()
    game_df = pd.DataFrame([{"GAME_ID": "0021900001", "EVENTNUM": 1}])
    possessions = processor_module.get_possessions_from_df(
        game_df,
        boxscore_source_loader=loader,
    )

    assert captured["game_id"] == "0021900001"
    assert captured["raw_data_dicts"] == [{"EVENTNUM": 1}]
    assert captured["boxscore_source_loader"] is loader
    assert captured["period_boxscore_source_loader"] is None
    assert len(possessions.items) == 0


def test_get_possessions_from_df_forwards_explicit_league(monkeypatch):
    captured = {}

    class DummyProcessor:
        def __init__(
            self,
            game_id,
            raw_data_dicts,
            rebound_deletions_list=None,
            boxscore_source_loader=None,
            period_boxscore_source_loader=None,
            file_directory=None,
            league=None,
        ):
            captured["league"] = league
            self.possessions = []

    monkeypatch.setattr(processor_module, "dedupe_with_v3", lambda df, *_args: df)
    monkeypatch.setattr(
        processor_module, "patch_start_of_periods", lambda df, *_args, **_kwargs: df
    )
    monkeypatch.setattr(processor_module, "_ensure_eventnum_int", lambda df: df)
    monkeypatch.setattr(processor_module, "create_raw_dicts_from_df", lambda df: [])
    monkeypatch.setattr(processor_module, "PbpProcessor", DummyProcessor)

    game_df = pd.DataFrame([{"GAME_ID": "1022500234", "EVENTNUM": 1}])
    processor_module.get_possessions_from_df(
        game_df,
        fetch_pbp_v3_fn=None,
        league=pbpstats.WNBA_STRING,
    )

    assert captured["league"] == pbpstats.WNBA_STRING


def test_get_possessions_from_df_normalizes_short_wnba_id_before_fetch_and_raw_dicts(
    monkeypatch,
):
    captured = {}
    fetched_game_ids = []

    class DummyProcessor:
        def __init__(
            self,
            game_id,
            raw_data_dicts,
            rebound_deletions_list=None,
            boxscore_source_loader=None,
            period_boxscore_source_loader=None,
            file_directory=None,
            league=None,
        ):
            captured["game_id"] = game_id
            captured["raw_data_dicts"] = raw_data_dicts
            captured["league"] = league
            self.possessions = []

    def fetch_v3(game_id):
        fetched_game_ids.append(game_id)
        return pd.DataFrame([{"actionNumber": 1, "clock": "PT09M59.00S"}])

    monkeypatch.setattr(processor_module, "PbpProcessor", DummyProcessor)

    game_df = pd.DataFrame(
        [
            {
                "GAME_ID": "22500234",
                "EVENTNUM": 1,
                "PERIOD": 1,
                "EVENTMSGTYPE": 1,
                "EVENTMSGACTIONTYPE": 1,
                "PCTIMESTRING": "9:59",
                "PLAYER1_ID": 1,
                "PLAYER1_TEAM_ID": 100,
                "PLAYER2_ID": 0,
                "PLAYER2_TEAM_ID": 0,
                "PLAYER3_ID": 0,
                "PLAYER3_TEAM_ID": 0,
            }
        ]
    )

    processor_module.get_possessions_from_df(
        game_df,
        fetch_pbp_v3_fn=fetch_v3,
        league=pbpstats.WNBA_STRING,
    )

    assert fetched_game_ids == ["1022500234", "1022500234"]
    assert captured["game_id"] == "1022500234"
    assert {row["GAME_ID"] for row in captured["raw_data_dicts"]} == {"1022500234"}
    assert captured["league"] == pbpstats.WNBA_STRING


def test_get_possessions_from_df_normalizes_game_id_column_before_raw_dicts(
    monkeypatch,
):
    captured = {}

    class DummyProcessor:
        def __init__(
            self,
            game_id,
            raw_data_dicts,
            rebound_deletions_list=None,
            boxscore_source_loader=None,
            period_boxscore_source_loader=None,
            file_directory=None,
        ):
            captured["game_id"] = game_id
            captured["raw_data_dicts"] = raw_data_dicts
            self.possessions = []

    monkeypatch.setattr(processor_module, "dedupe_with_v3", lambda df, *_args: df)
    monkeypatch.setattr(
        processor_module, "patch_start_of_periods", lambda df, *_args: df
    )
    monkeypatch.setattr(processor_module, "_ensure_eventnum_int", lambda df: df)
    monkeypatch.setattr(
        processor_module,
        "create_raw_dicts_from_df",
        lambda df: [{"GAME_ID": df.iloc[0]["GAME_ID"]}],
    )
    monkeypatch.setattr(processor_module, "PbpProcessor", DummyProcessor)

    for game_id_value, expected in [
        (1022500234, "1022500234"),
        (1022500234.0, "1022500234"),
        (" 1022500234 ", "1022500234"),
        ("1022500234.0", "1022500234"),
        (21900001.0, "0021900001"),
    ]:
        captured.clear()
        game_df = pd.DataFrame([{"GAME_ID": game_id_value, "EVENTNUM": 1}])
        processor_module.get_possessions_from_df(game_df, fetch_pbp_v3_fn=None)

        assert captured["game_id"] == expected
        assert captured["raw_data_dicts"] == [{"GAME_ID": expected}]


def test_get_possessions_from_df_rejects_mixed_game_ids():
    game_df = pd.DataFrame(
        [
            {"GAME_ID": "0021900001", "EVENTNUM": 1},
            {"GAME_ID": "0021900002", "EVENTNUM": 2},
        ]
    )

    with pytest.raises(ValueError, match="expects a single-game DataFrame"):
        processor_module.get_possessions_from_df(game_df, fetch_pbp_v3_fn=None)


def test_get_possessions_from_df_coerces_eventmsgtype_before_raw_dicts(
    monkeypatch,
):
    captured = {}

    class DummyProcessor:
        def __init__(
            self,
            game_id,
            raw_data_dicts,
            rebound_deletions_list=None,
            boxscore_source_loader=None,
            period_boxscore_source_loader=None,
            file_directory=None,
        ):
            captured["raw_data_dicts"] = raw_data_dicts
            self.possessions = []

    monkeypatch.setattr(processor_module, "PbpProcessor", DummyProcessor)

    game_df = pd.DataFrame(
        [{"GAME_ID": "0021900001", "EVENTNUM": "1", "EVENTMSGTYPE": "1"}]
    )

    processor_module.get_possessions_from_df(game_df, fetch_pbp_v3_fn=None)

    assert captured["raw_data_dicts"][0]["EVENTMSGTYPE"] == 1


def test_get_possessions_from_df_defaults_boxscore_loader_to_none(monkeypatch):
    captured = {}

    class DummyProcessor:
        def __init__(
            self,
            game_id,
            raw_data_dicts,
            rebound_deletions_list=None,
            boxscore_source_loader=None,
            period_boxscore_source_loader=None,
            file_directory=None,
        ):
            captured["boxscore_source_loader"] = boxscore_source_loader
            captured["period_boxscore_source_loader"] = period_boxscore_source_loader
            self.possessions = []

    monkeypatch.setattr(processor_module, "dedupe_with_v3", lambda df, *_args: df)
    monkeypatch.setattr(
        processor_module, "patch_start_of_periods", lambda df, *_args: df
    )
    monkeypatch.setattr(processor_module, "_ensure_eventnum_int", lambda df: df)
    monkeypatch.setattr(processor_module, "create_raw_dicts_from_df", lambda df: [])
    monkeypatch.setattr(processor_module, "PbpProcessor", DummyProcessor)

    game_df = pd.DataFrame([{"GAME_ID": "0021900001", "EVENTNUM": 1}])
    processor_module.get_possessions_from_df(game_df, fetch_pbp_v3_fn=None)

    assert captured["boxscore_source_loader"] is None
    assert captured["period_boxscore_source_loader"] is None


def test_get_possessions_from_df_forwards_period_boxscore_loader(monkeypatch):
    captured = {}

    class DummyProcessor:
        def __init__(
            self,
            game_id,
            raw_data_dicts,
            rebound_deletions_list=None,
            boxscore_source_loader=None,
            period_boxscore_source_loader=None,
            file_directory=None,
        ):
            captured["period_boxscore_source_loader"] = period_boxscore_source_loader
            self.possessions = []

    monkeypatch.setattr(processor_module, "dedupe_with_v3", lambda df, *_args: df)
    monkeypatch.setattr(
        processor_module, "patch_start_of_periods", lambda df, *_args: df
    )
    monkeypatch.setattr(processor_module, "_ensure_eventnum_int", lambda df: df)
    monkeypatch.setattr(processor_module, "create_raw_dicts_from_df", lambda df: [])
    monkeypatch.setattr(processor_module, "PbpProcessor", DummyProcessor)

    loader = DummyPeriodBoxscoreLoader()
    game_df = pd.DataFrame([{"GAME_ID": "0021900001", "EVENTNUM": 1}])
    processor_module.get_possessions_from_df(
        game_df,
        fetch_pbp_v3_fn=None,
        period_boxscore_source_loader=loader,
    )

    assert captured["period_boxscore_source_loader"] is loader
