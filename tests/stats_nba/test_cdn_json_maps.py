# -*- coding: utf-8 -*-
import json
import os
import tempfile
from pathlib import Path

import pytest
import pbpstats.data_loader.stats_nba.pbp.cdn_adapter as cdn_adapter
from pbpstats.data_loader.stats_nba.pbp.cdn_adapter import (
    map_eventmsgactiontype,
    map_eventmsgtype,
)


def _load_case_matrix():
    base_dir = Path(__file__).resolve().parents[1] / "data" / "cdn_map_cases"
    cases = []
    if base_dir.is_dir():
        for case_file in sorted(base_dir.glob("*.json")):
            with case_file.open(encoding="utf-8") as file_obj:
                payload = json.load(file_obj)
            for entry in payload.get("cases", []):
                entry.setdefault("source", case_file.name)
                cases.append(entry)
    return cases


JSON_CASES = _load_case_matrix()


def test_overlay_adds_new_turnover_subtype_and_canonicalizes(monkeypatch):
    overlay = {
        "TOV_MAP": {
            "Mystery": 99,
            "Double Dribble": 42,
        }
    }

    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, "overlay.json")
            with open(path, "w", encoding="utf-8") as file_obj:
                json.dump(overlay, file_obj)

            monkeypatch.setenv("PBPSTATS_CDN_MAPS", path)
            cdn_adapter.reload_cdn_maps()

            action_new = {"actionType": "Turnover", "subType": "Mystery"}
            evt = map_eventmsgtype(action_new)
            assert evt == 5
            assert map_eventmsgactiontype(action_new, evt) == 99

            action_override = {"actionType": "Turnover", "subType": "Double Dribble"}
            evt2 = map_eventmsgtype(action_override)
            assert evt2 == 5
            assert map_eventmsgactiontype(action_override, evt2) == 42
    finally:
        cdn_adapter.reload_cdn_maps(paths="")


def test_overlay_multiple_files_merge_order(monkeypatch):
    overlay_one = {"VIOL_MAP": {"Delay of Game": 8}}
    overlay_two = {"VIOL_MAP": {"Delay-of-Game": 3}}

    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            path_one = os.path.join(temp_dir, "overlay_one.json")
            path_two = os.path.join(temp_dir, "overlay_two.json")
            with open(path_one, "w", encoding="utf-8") as file_obj:
                json.dump(overlay_one, file_obj)
            with open(path_two, "w", encoding="utf-8") as file_obj:
                json.dump(overlay_two, file_obj)

            joined = os.pathsep.join([path_one, path_two])
            monkeypatch.setenv("PBPSTATS_CDN_MAPS", joined)
            cdn_adapter.reload_cdn_maps()

            action = {"actionType": "Violation", "subType": "Delay of Game"}
            evt = map_eventmsgtype(action)
            assert evt == 7
            assert map_eventmsgactiontype(action, evt) == 3
    finally:
        cdn_adapter.reload_cdn_maps(paths="")


@pytest.mark.parametrize("case", JSON_CASES, ids=lambda c: c.get("name") or c.get("source"))
def test_json_defined_synonym_cases(case):
    action = case["action"].copy()
    expected_event_type = case["expected_event_type"]
    expected_action_type = case["expected_action_type"]

    evt_type = map_eventmsgtype(action)
    assert evt_type == expected_event_type
    assert map_eventmsgactiontype(action, evt_type) == expected_action_type
