import pandas as pd

from pbpstats.offline import processor as processor_module
from pbpstats.resources.enhanced_pbp import StartOfPeriod


class DummyBoxscoreLoader:
    def load_data(self):
        return {"resultSets": []}


class DummyStartOfPeriod(StartOfPeriod):
    def __init__(self, data, order):
        self.data = data
        self.order = order

    def get_period_starters(self, file_directory=None):
        return {}


class DummyNonStartEvent:
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

    monkeypatch.setattr(processor_module, 'StatsNbaEnhancedPbpFactory', DummyFactory)
    monkeypatch.setattr(processor_module.PbpProcessor, '_process_with_retries', fake_process)

    loader = DummyBoxscoreLoader()
    processor = processor_module.PbpProcessor(
        '0021900001',
        [
            {'EVENTMSGTYPE': 12, 'EVENTNUM': 0},
            {'EVENTMSGTYPE': 1, 'EVENTNUM': 1},
        ],
        boxscore_source_loader=loader,
    )

    assert processor.items[0].boxscore_source_loader is loader
    assert not hasattr(processor.items[1], 'boxscore_source_loader')


def test_get_possessions_from_df_forwards_boxscore_loader(monkeypatch):
    captured = {}

    class DummyProcessor:
        def __init__(self, game_id, raw_data_dicts, rebound_deletions_list=None, boxscore_source_loader=None):
            captured['game_id'] = game_id
            captured['raw_data_dicts'] = raw_data_dicts
            captured['rebound_deletions_list'] = rebound_deletions_list
            captured['boxscore_source_loader'] = boxscore_source_loader
            self.possessions = []

    monkeypatch.setattr(processor_module, 'dedupe_with_v3', lambda df, *_args: df)
    monkeypatch.setattr(processor_module, 'patch_start_of_periods', lambda df, *_args: df)
    monkeypatch.setattr(processor_module, 'reorder_with_v3', lambda df, *_args: df)
    monkeypatch.setattr(processor_module, '_ensure_eventnum_int', lambda df: df)
    monkeypatch.setattr(processor_module, 'create_raw_dicts_from_df', lambda df: [{'EVENTNUM': int(df.iloc[0]['EVENTNUM'])}])
    monkeypatch.setattr(processor_module, 'PbpProcessor', DummyProcessor)

    loader = DummyBoxscoreLoader()
    game_df = pd.DataFrame([{'GAME_ID': '0021900001', 'EVENTNUM': 1}])
    possessions = processor_module.get_possessions_from_df(
        game_df,
        boxscore_source_loader=loader,
    )

    assert captured['game_id'] == '0021900001'
    assert captured['raw_data_dicts'] == [{'EVENTNUM': 1}]
    assert captured['boxscore_source_loader'] is loader
    assert len(possessions.items) == 0


def test_get_possessions_from_df_defaults_boxscore_loader_to_none(monkeypatch):
    captured = {}

    class DummyProcessor:
        def __init__(self, game_id, raw_data_dicts, rebound_deletions_list=None, boxscore_source_loader=None):
            captured['boxscore_source_loader'] = boxscore_source_loader
            self.possessions = []

    monkeypatch.setattr(processor_module, 'dedupe_with_v3', lambda df, *_args: df)
    monkeypatch.setattr(processor_module, 'patch_start_of_periods', lambda df, *_args: df)
    monkeypatch.setattr(processor_module, '_ensure_eventnum_int', lambda df: df)
    monkeypatch.setattr(processor_module, 'create_raw_dicts_from_df', lambda df: [])
    monkeypatch.setattr(processor_module, 'PbpProcessor', DummyProcessor)

    game_df = pd.DataFrame([{'GAME_ID': '0021900001', 'EVENTNUM': 1}])
    processor_module.get_possessions_from_df(game_df, fetch_pbp_v3_fn=None)

    assert captured['boxscore_source_loader'] is None
