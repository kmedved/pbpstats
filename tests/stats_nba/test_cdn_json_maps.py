# -*- coding: utf-8 -*-
import json
import os
import tempfile

import pbpstats.data_loader.stats_nba.pbp.cdn_adapter as cdn_adapter
from pbpstats.data_loader.stats_nba.pbp.cdn_adapter import (
    map_eventmsgactiontype,
    map_eventmsgtype,
)


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
