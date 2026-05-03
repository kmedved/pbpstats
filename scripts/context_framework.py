from __future__ import annotations

import ast
import inspect
import json
import re
import textwrap
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import tiktoken

REPO_ROOT = Path(__file__).resolve().parents[1]
CONTEXT_DIR = REPO_ROOT / "context"

PRIMARY_ARCHETYPE = "Library / SDK"
SECONDARY_ARCHETYPES = ["Data / Workflow / Pipeline"]
REPO_TOPOLOGY = "single package / single app"
VERSION_POLICY = "Policy B: only shipped/runtime behavior changes bump version."

BUNDLE_CONTRACT = {
    "default_bundle": "COMPRESSED_core.md",
    "bundles": [
        {
            "name": "COMPRESSED_core.md",
            "purpose": "package constants, client wiring, objects, overrides, and offline orchestration",
        },
        {
            "name": "COMPRESSED_loaders.md",
            "purpose": "data loader factory, provider-specific file/web loaders, and endpoint routing",
        },
        {
            "name": "COMPRESSED_resources_core.md",
            "purpose": "resource wrappers, possession containers, boxscore/game/pbp/shots models",
        },
        {
            "name": "COMPRESSED_resources_events.md",
            "purpose": "enhanced play-by-play event classes, factories, start-of-period logic, and shot clock rules",
        },
        {
            "name": "COMPRESSED_tests.md",
            "purpose": "fixture-backed behavioral coverage for loaders, possessions, shot clock, and event edge cases",
        },
    ],
}

BUNDLE_FILE_GROUPS = {
    "COMPRESSED_core.md": [
        "pbpstats/__init__.py",
        "pbpstats/client.py",
        "pbpstats/overrides.py",
        "pbpstats/objects",
        "pbpstats/offline",
    ],
    "COMPRESSED_loaders.md": [
        "pbpstats/data_loader",
    ],
    "COMPRESSED_resources_core.md": [
        "pbpstats/resources/__init__.py",
        "pbpstats/resources/base.py",
        "pbpstats/resources/boxscore",
        "pbpstats/resources/games",
        "pbpstats/resources/pbp",
        "pbpstats/resources/possessions",
        "pbpstats/resources/shots",
    ],
    "COMPRESSED_resources_events.md": [
        "pbpstats/resources/enhanced_pbp",
    ],
    "COMPRESSED_tests.md": [
        "tests",
    ],
}

ALLOWED_ROOT_FILES = {
    ".gitignore",
    ".repomixignore",
    "AGENTS.md",
    "LICENSE",
    "README.md",
    "pyproject.toml",
    "repomix.config.json",
    "tox.ini",
}

ALLOWED_DOC_FILES = {
    ".github/workflows/ci.yaml",
    "docs/conf.py",
    "docs/index.rst",
    "docs/quickstart.rst",
}

IGNORE_PATH_PARTS = {
    ".git",
    ".pytest_cache",
    "__pycache__",
    "build",
    "dist",
    "env",
    "node_modules",
    "pbpstats.egg-info",
}

IGNORE_FILE_SUFFIXES = {
    ".DS_Store",
    ".ipynb",
    ".pyc",
}

CHECKED_IN_CONTEXT_FILES = [
    "context/START_HERE.md",
    "context/REPO_ARCHITECTURE.md",
    "context/REPO_ARCHITECTURE_SYNC.json",
    "context/FILE_INDEX.md",
]

SUBSYSTEM_ORDER = [
    "package.constants",
    "client",
    "objects",
    "resources.core",
    "resources.enhanced_pbp",
    "data_loader.core",
    "data_loader.stats_nba",
    "data_loader.data_nba",
    "data_loader.live",
    "offline",
]


def _encoding():
    try:
        return tiktoken.get_encoding("o200k_base")
    except ValueError:
        return tiktoken.get_encoding("cl100k_base")


def count_tokens(text: str) -> int:
    return len(_encoding().encode(text))


def read_version() -> str:
    text = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    match = re.search(
        r'^\[tool\.poetry\]\s.*?^version = "([^"]+)"',
        text,
        flags=re.MULTILINE | re.DOTALL,
    )
    if match is None:
        raise ValueError("Unable to find Poetry version in pyproject.toml")
    return match.group(1)


def _is_generated_context_path(relative_path: str) -> bool:
    if not relative_path.startswith("context/"):
        return False
    name = Path(relative_path).name
    return name.startswith("COMPRESSED_") or name.startswith("CONTEXT_BUDGET")


def _is_accidental_duplicate(relative_path: str) -> bool:
    return " 2" in relative_path


def _is_included_source(relative_path: str) -> bool:
    path = Path(relative_path)
    parts = set(path.parts)
    if parts & IGNORE_PATH_PARTS:
        return False
    if any(relative_path.endswith(suffix) for suffix in IGNORE_FILE_SUFFIXES):
        return False
    if _is_generated_context_path(relative_path):
        return False
    if _is_accidental_duplicate(relative_path):
        return False
    if relative_path.startswith("tests/data/"):
        return False
    if relative_path.startswith("docs/"):
        return relative_path in ALLOWED_DOC_FILES
    if relative_path.startswith("pbpstats/"):
        return relative_path.endswith(".py")
    if relative_path.startswith("tests/"):
        return relative_path.endswith(".py")
    if relative_path.startswith("scripts/"):
        return relative_path.endswith(".py")
    if relative_path.startswith(".github/"):
        return relative_path in ALLOWED_DOC_FILES
    return relative_path in ALLOWED_ROOT_FILES


def collect_file_inventory() -> List[str]:
    files = []
    for path in sorted(REPO_ROOT.rglob("*")):
        if not path.is_file():
            continue
        relative_path = path.relative_to(REPO_ROOT).as_posix()
        if _is_included_source(relative_path):
            files.append(relative_path)
    return files


def _module_name_from_path(path: Path) -> Optional[str]:
    if path.suffix != ".py":
        return None
    relative = path.relative_to(REPO_ROOT).with_suffix("")
    parts = list(relative.parts)
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def _public_properties(cls) -> List[str]:
    properties = []
    for name, value in cls.__dict__.items():
        if name.startswith("_"):
            continue
        if isinstance(value, property):
            properties.append(name)
    return sorted(properties)


def _signature_string(callable_obj) -> str:
    return str(inspect.signature(callable_obj))


def _collect_loader_routes() -> Dict[str, Dict[str, List[Dict[str, str]]]]:
    from pbpstats.data_loader.factory import DataLoaderFactory

    factory = DataLoaderFactory()
    routes: Dict[str, Dict[str, List[Dict[str, str]]]] = {}
    for resource in sorted(factory.loaders):
        routes[resource] = {}
        for provider in sorted(factory.loaders[resource]):
            entries = []
            for loader in sorted(
                factory.loaders[resource][provider],
                key=lambda item: item["loader"].__name__,
            ):
                entries.append(
                    {
                        "loader": loader["loader"].__name__,
                        "file_source": loader["file_source"].__name__,
                        "web_source": loader["web_source"].__name__,
                        "parent_object": loader["loader"].parent_object,
                    }
                )
            routes[resource][provider] = entries
    return routes


def _collect_parent_object_routes(
    loader_routes: Dict[str, Dict[str, List[Dict[str, str]]]]
) -> Dict[str, List[str]]:
    routes = defaultdict(set)
    for resource, provider_map in loader_routes.items():
        for provider, entries in provider_map.items():
            for entry in entries:
                label = "%s (%s)" % (resource, provider)
                routes[entry["parent_object"]].add(label)
    return {
        parent: sorted(values)
        for parent, values in sorted(routes.items(), key=lambda item: item[0])
    }


def _collect_factory_dispatch() -> Dict[str, List[Dict[str, str]]]:
    from pbpstats.resources.enhanced_pbp.data_nba.enhanced_pbp_factory import (
        DataNbaEnhancedPbpFactory,
    )
    from pbpstats.resources.enhanced_pbp.live.enhanced_pbp_factory import (
        LiveEnhancedPbpFactory,
    )
    from pbpstats.resources.enhanced_pbp.stats_nba.enhanced_pbp_factory import (
        StatsNbaEnhancedPbpFactory,
    )

    def summarize(factory) -> List[Dict[str, str]]:
        values = []
        for key, cls in sorted(factory.event_classes.items(), key=lambda item: str(item[0])):
            values.append({"key": str(key), "class": cls.__name__})
        return values

    return {
        "stats_nba": summarize(StatsNbaEnhancedPbpFactory()),
        "data_nba": summarize(DataNbaEnhancedPbpFactory()),
        "live": summarize(LiveEnhancedPbpFactory()),
    }


def collect_behavior_snapshot() -> Dict[str, object]:
    loader_routes = _collect_loader_routes()
    return {
        "archetype": {
            "primary": PRIMARY_ARCHETYPE,
            "secondary": SECONDARY_ARCHETYPES,
            "topology": REPO_TOPOLOGY,
        },
        "supported_sources": ["file", "web"],
        "supported_leagues": ["nba", "wnba", "gleague"],
        "supported_season_types": ["Regular Season", "Playoffs", "PlayIn"],
        "resource_provider_routes": loader_routes,
        "parent_object_routes": _collect_parent_object_routes(loader_routes),
        "enhanced_pbp_factory_dispatch": _collect_factory_dispatch(),
        "override_files": [
            "overrides/bad_pbp_possessions.json",
            "overrides/missing_period_starters.json",
            "overrides/non_possession_changing_event_overrides.json",
            "overrides/possession_change_event_overrides.json",
        ],
        "offline_pipeline": [
            "_ensure_eventnum_int",
            "dedupe_with_v3",
            "patch_start_of_periods",
            "preserve_order_after_v3_repairs",
            "create_raw_dicts_from_df",
            "PbpProcessor",
            "Possessions",
        ],
    }


def _parse_python_module(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=path.as_posix())


def _render_ast_signature(node: ast.FunctionDef) -> str:
    args = []
    positional = list(node.args.args)
    defaults = [None] * (len(positional) - len(node.args.defaults)) + list(
        node.args.defaults
    )
    for arg, default in zip(positional, defaults):
        if default is None:
            args.append(arg.arg)
        else:
            args.append("%s=..." % arg.arg)
    if node.args.vararg is not None:
        args.append("*%s" % node.args.vararg.arg)
    elif node.args.kwonlyargs:
        args.append("*")
    for kwarg, default in zip(node.args.kwonlyargs, node.args.kw_defaults):
        if default is None:
            args.append(kwarg.arg)
        else:
            args.append("%s=..." % kwarg.arg)
    if node.args.kwarg is not None:
        args.append("**%s" % node.args.kwarg.arg)
    return "(%s)" % ", ".join(args)


def _extract_offline_contracts() -> Dict[str, Dict[str, object]]:
    processor_path = REPO_ROOT / "pbpstats/offline/processor.py"
    module = _parse_python_module(processor_path)
    contracts: Dict[str, Dict[str, object]] = {
        "PbpProcessor": {"signature": None, "properties": []},
        "get_possessions_from_df": {"signature": None},
        "set_rebound_strict_mode": {"signature": None},
    }
    for node in module.body:
        if isinstance(node, ast.ClassDef) and node.name == "PbpProcessor":
            for class_child in node.body:
                if isinstance(class_child, ast.FunctionDef) and class_child.name == "__init__":
                    contracts["PbpProcessor"]["signature"] = _render_ast_signature(
                        class_child
                    )
        if isinstance(node, ast.FunctionDef) and node.name in contracts:
            contracts[node.name]["signature"] = _render_ast_signature(node)
    return contracts


def collect_public_contracts() -> Dict[str, object]:
    from pbpstats.client import Client
    from pbpstats.objects.day import Day
    from pbpstats.objects.game import Game
    from pbpstats.objects.season import Season
    from pbpstats.resources.boxscore.boxscore import Boxscore
    from pbpstats.resources.enhanced_pbp.enhanced_pbp import EnhancedPbp
    from pbpstats.resources.games.games import Games
    from pbpstats.resources.pbp.pbp import Pbp
    from pbpstats.resources.possessions.possessions import Possessions
    from pbpstats.resources.shots.shots import Shots

    loader_routes = _collect_loader_routes()
    resources_contract = {}
    for cls in [Boxscore, EnhancedPbp, Games, Pbp, Possessions, Shots]:
        resources_contract[cls.__name__] = {
            "signature": _signature_string(cls.__init__),
            "properties": _public_properties(cls),
        }

    settings_shape = {}
    for resource, provider_map in loader_routes.items():
        settings_shape[resource] = {
            "required_keys": ["source", "data_provider"],
            "sources": ["file", "web"],
            "data_providers": sorted(provider_map),
        }

    return {
        "client": {
            "Client": {
                "signature": _signature_string(Client.__init__),
                "settings_shape": settings_shape,
            }
        },
        "objects": {
            "Day": {"signature": _signature_string(Day.__init__)},
            "Game": {"signature": _signature_string(Game.__init__)},
            "Season": {"signature": _signature_string(Season.__init__)},
        },
        "resources": resources_contract,
        "offline": _extract_offline_contracts(),
    }


def _map_module_to_node(module_name: str) -> Optional[str]:
    if module_name == "pbpstats":
        return "package.constants"
    if module_name.startswith("pbpstats.client"):
        return "client"
    if module_name.startswith("pbpstats.objects"):
        return "objects"
    if module_name.startswith("pbpstats.resources.enhanced_pbp"):
        return "resources.enhanced_pbp"
    if module_name.startswith("pbpstats.resources"):
        return "resources.core"
    if module_name.startswith("pbpstats.data_loader.stats_nba"):
        return "data_loader.stats_nba"
    if module_name.startswith("pbpstats.data_loader.data_nba"):
        return "data_loader.data_nba"
    if module_name.startswith("pbpstats.data_loader.live"):
        return "data_loader.live"
    if module_name.startswith("pbpstats.data_loader"):
        return "data_loader.core"
    if module_name.startswith("pbpstats.offline"):
        return "offline"
    return None


def _resolve_from_import(
    current_module: str,
    imported_module: Optional[str],
    level: int,
) -> Optional[str]:
    if level == 0:
        return imported_module
    module_parts = current_module.split(".")
    package_parts = module_parts[:-1]
    if current_module.endswith("__init__"):
        package_parts = module_parts
    trim = level - 1
    if trim:
        package_parts = package_parts[:-trim]
    if imported_module:
        return ".".join(package_parts + imported_module.split("."))
    return ".".join(package_parts)


def _collect_internal_import_nodes(path: Path) -> List[str]:
    module_name = _module_name_from_path(path)
    if module_name is None:
        return []
    tree = _parse_python_module(path)
    modules = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith("pbpstats"):
                    modules.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            resolved = _resolve_from_import(module_name, node.module, node.level)
            if resolved and resolved.startswith("pbpstats"):
                modules.append(resolved)
    return modules


def collect_module_dependencies() -> Dict[str, List[str]]:
    adjacency = {node: set() for node in SUBSYSTEM_ORDER}
    for path in sorted((REPO_ROOT / "pbpstats").rglob("*.py")):
        relative_path = path.relative_to(REPO_ROOT).as_posix()
        if _is_accidental_duplicate(relative_path):
            continue
        current_module = _module_name_from_path(path)
        if current_module is None:
            continue
        current_node = _map_module_to_node(current_module)
        if current_node is None:
            continue
        for imported_module in _collect_internal_import_nodes(path):
            target_node = _map_module_to_node(imported_module)
            if target_node and target_node != current_node:
                adjacency[current_node].add(target_node)
    return {
        node: sorted(adjacency[node])
        for node in SUBSYSTEM_ORDER
        if adjacency[node]
    }


def build_sync_data() -> Dict[str, object]:
    return {
        "arch_version": read_version(),
        "repo_archetype": {
            "primary": PRIMARY_ARCHETYPE,
            "secondary": SECONDARY_ARCHETYPES,
            "topology": REPO_TOPOLOGY,
            "version_policy": VERSION_POLICY,
        },
        "file_inventory": collect_file_inventory(),
        "behavior_snapshot": collect_behavior_snapshot(),
        "module_dependencies": collect_module_dependencies(),
        "public_contracts": collect_public_contracts(),
        "bundle_contract": BUNDLE_CONTRACT,
    }


def render_sync_json(sync_data: Dict[str, object]) -> str:
    return json.dumps(sync_data, indent=2, sort_keys=True) + "\n"


def _group_paths_for_index(paths: Sequence[str]) -> List[Tuple[str, List[str]]]:
    groups: Dict[str, List[str]] = defaultdict(list)
    for path in paths:
        parts = path.split("/")
        if len(parts) == 1:
            group = "Root"
        elif parts[0] == ".github":
            group = ".github/workflows"
        elif parts[0] == "pbpstats":
            group = "/".join(parts[:2]) if len(parts) > 2 else "pbpstats"
        elif parts[0] == "tests":
            group = "/".join(parts[:2]) if len(parts) > 2 else "tests"
        elif parts[0] == "scripts":
            group = "scripts"
        else:
            group = parts[0]
        groups[group].append(path)
    return sorted((group, sorted(values)) for group, values in groups.items())


def render_file_index(file_inventory: Sequence[str]) -> str:
    lines = [
        "Use this for oracle workflows when you want the model to request exact files by path.",
        "Pair with `context/REPO_ARCHITECTURE.md`, not instead of it.",
        "For implementation work, still provide raw source of the files you expect to edit.",
        "",
        "# File Index",
        "",
    ]
    for group, paths in _group_paths_for_index(file_inventory):
        lines.append("## %s" % group)
        for path in paths:
            lines.append("- `%s`" % path)
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _render_contract_rows(public_contracts: Dict[str, object]) -> List[str]:
    client_contract = public_contracts["client"]["Client"]
    rows = [
        "| Surface | Signature | Notes |",
        "|---|---|---|",
        "| `Client` | `%s` | Settings keys mirror resource class names; each resource config requires `source` and `data_provider`. |"
        % client_contract["signature"],
    ]

    for name in ["Day", "Game", "Season"]:
        rows.append(
            "| `%s` | `%s` | Object wrapper that instantiates bound resources based on client wiring. |"
            % (name, public_contracts["objects"][name]["signature"])
        )

    for name in ["Boxscore", "EnhancedPbp", "Games", "Pbp", "Possessions", "Shots"]:
        notes = ", ".join(public_contracts["resources"][name]["properties"])
        rows.append(
            "| `%s` | `%s` | Properties: `%s`. |"
            % (name, public_contracts["resources"][name]["signature"], notes)
        )

    rows.append(
        "| `PbpProcessor` | `%s` | Offline bridge that turns stats-style event dicts into enhanced events and possessions. |"
        % public_contracts["offline"]["PbpProcessor"]["signature"]
    )
    rows.append(
        "| `get_possessions_from_df` | `%s` | Offline convenience entrypoint for pandas workflows. |"
        % public_contracts["offline"]["get_possessions_from_df"]["signature"]
    )
    return rows


def _render_dependency_lines(module_dependencies: Dict[str, List[str]]) -> List[str]:
    return [
        "- `%s` -> %s"
        % (node, ", ".join("`%s`" % dependency for dependency in dependencies))
        for node, dependencies in module_dependencies.items()
    ]


def _render_bundle_picker(bundle_contract: Dict[str, object]) -> List[str]:
    bundle_map = {
        "COMPRESSED_core.md": "Client wiring, objects, overrides, offline entrypoints",
        "COMPRESSED_loaders.md": "Loader factory, file/web sources, provider routing",
        "COMPRESSED_resources_core.md": "Resource wrappers, possessions, boxscore/game/pbp/shots models",
        "COMPRESSED_resources_events.md": "Enhanced event classes, shot clock, start-of-period logic",
        "COMPRESSED_tests.md": "Behavioral tests, fixtures, regression coverage",
    }
    rows = ["| Task area | Bundle |", "|---|---|"]
    for bundle in bundle_contract["bundles"]:
        rows.append(
            "| %s | `%s` |" % (bundle_map[bundle["name"]], bundle["name"])
        )
    rows.append("")
    rows.append(
        "Default: `context/REPO_ARCHITECTURE.md` + `context/%s` + your question."
        % bundle_contract["default_bundle"]
    )
    rows.append(
        "For implementation tasks, also paste raw source of the files you're editing."
    )
    return rows


def render_architecture_markdown(sync_data: Dict[str, object]) -> str:
    contracts = sync_data["public_contracts"]
    module_dependencies = sync_data["module_dependencies"]
    bundle_contract = sync_data["bundle_contract"]
    lines = [
        "Paste this first.",
        "Pair with one `COMPRESSED_*.md` bundle for guided context, or with `FILE_INDEX.md` for oracle workflows.",
        "For implementation tasks, also paste raw source of the files you expect to edit.",
        "",
        "Architecture sync version: %s" % sync_data["arch_version"],
        "",
        "## TL;DR",
        (
            "pbpstats is a single-package Python library for loading NBA, WNBA, and G-League play-by-play data from "
            "`stats_nba`, `data_nba`, and `live` providers, then normalizing it into resource wrappers (`Game`, `Day`, "
            "`Season`, `Boxscore`, `EnhancedPbp`, `Possessions`, and related item types). The core idea is metadata-driven "
            "routing: `Client(settings)` discovers loader classes from package exports, binds them to object classes by "
            "`parent_object`, and lets resource loaders compose richer state like lineups, starters, shot clocks, "
            "possession splits, and offline repair flows without changing the public object/resource surface."
        ),
        "",
        "## Behavior / Routing Matrix",
        "| Surface | Inputs | Route | Result |",
        "|---|---|---|---|",
        "| `Client(settings)` | Resource name + `source` + `data_provider` + supported source-loader options | `pbpstats.resources.__all__` + `DataLoaderFactory.loaders` | Binds `*DataLoaderClass`, `*DataSource`, supported `*DataSourceOptions`, and resource class onto `Game` / `Day` / `Season`. |",
        "| `Game(...)` resources | `Boxscore`, `Pbp`, `EnhancedPbp`, `Possessions`, `Shots` | Loader metadata with `parent_object = \"Game\"` | Instantiates snake-case attributes like `game.boxscore`, `game.enhanced_pbp`, `game.possessions`. |",
        "| stats PBP web/file source | v2 cache/endpoint or `endpoint_strategy` = `v3_synthetic` / `auto` | `StatsNbaPbp*Loader` + private v3 synthetic transformer | Returns v2-shaped rows for validated NBA v3 payloads and WNBA v3 payloads with a validated true-v2 role supplement; true v2 cache stays under `/pbp`, raw v3 under `/pbp_v3`, synthetic rows under `/pbp_synthetic_v3`. |",
        "| `Day(...)` games | `Games` + `stats_nba` scoreboard route | `StatsNbaScoreboardLoader` | Returns same `Games` resource surface, but day-scoped. |",
        "| `Season(...)` games | `Games` + provider season schedule route | `StatsNbaLeagueGameLogLoader`, `DataNbaScheduleLoader`, `LiveScheduleLoader` | Returns season-scoped `games`. |",
        "| stats enhanced PBP | raw stats rows + optional shots/v3/boxscore sources | `StatsNbaEnhancedPbpFactory(EVENTMSGTYPE)` -> enrich -> rebound order repair -> shot XY | Produces linked enhanced events with lineups, fouls-to-give, score, starters, and shot clock. |",
        "| data/live enhanced PBP | provider-specific raw event payloads | Provider factory -> shared enrichment | Produces the same high-level event surface with provider-specific parsing. |",
        "| possessions | ordered enhanced events | `_split_events_by_possession()` + `Possession(...)` + alternation checks | Derives possession start/end, offense team, start type, team/player/lineup aggregations. |",
        "| offline dataframe path | pandas frame + optional v3 fetcher | normalize -> dedupe -> patch period starts -> reorder -> `PbpProcessor` | Returns a `Possessions` resource without going through on-disk web/file loaders. |",
        "",
        "### Critical Invariants",
        "- Loader discovery only sees classes exported from `pbpstats.data_loader` that define `resource`, `data_provider`, and `parent_object`; breaking those attrs silently removes a route. (`pbpstats/data_loader/factory.py`, `tests/test_client.py`)",
        "- `stats_nba` synthetic v3 PBP must emit the existing playbyplayv2 row contract (`EVENTMSGTYPE`, `EVENTMSGACTIONTYPE`, `PLAYER1_*`, `PLAYER2_*`, `PLAYER3_*`) so the PBP, enhanced PBP, and possession stacks stay unchanged. NBA synthetic rows can be built from v3 alone. WNBA synthetic rows require a validated true-v2 role supplement because WNBA playbyplayv3 is not role-complete enough for foul-drawn and jump-ball role parity. WNBA shotchartdetail remains a shots/enhanced-coordinate source, not a participant-role source. (`pbpstats/data_loader/stats_nba/pbp/v3_synthetic.py`, `tests/data_loaders/test_stats_v3_synthetic.py`)",
        "- True v2 file/cache data remains canonical; synthetic v3 rows use `/pbp_synthetic_v3`, raw v3 uses `/pbp_v3`, and `auto` fallback only triggers for missing or malformed v2 source data. (`pbpstats/data_loader/stats_nba/pbp/file.py`, `pbpstats/data_loader/stats_nba/pbp/web.py`)",
        "- Event linking and enrichment happen before possession logic or shot-clock annotation; `previous_event` / `next_event`, score, foul state, and override flags must exist first. (`pbpstats/data_loader/nba_enhanced_pbp_loader.py`)",
        "- `StatsNbaPossessionLoader` expects possessions to alternate offensive teams unless a known bad-PBP override or flagrant-foul exception applies. (`pbpstats/data_loader/stats_nba/possessions/loader.py`)",
        "- Start-of-period handling must resolve five starters per team, optionally using overrides or previous-period ending lineups to fill gaps. (`pbpstats/resources/enhanced_pbp/start_of_period.py`, `tests/test_period_starters_carryover.py`)",
        "- Shot-clock annotation uses loader season or string/numeric event/game-id fallback for league thresholds: NBA rim-retention short reset starts in 2018-19, WNBA/G-League/D-League in 2016, and retained defensive stops, including stats-style kicked balls, use same-or-14 for supported leagues. (`pbpstats/resources/enhanced_pbp/shot_clock.py`, `pbpstats/data_loader/nba_enhanced_pbp_loader.py`, `tests/test_shot_clock.py`)",
        "- Live enhanced PBP computes shot clock after defensive rebound team-id normalization; moving that pass before normalization changes live possession semantics. (`pbpstats/data_loader/live/enhanced_pbp/loader.py`, `tests/test_shot_clock.py`)",
        "- Team and lineup possession aggregations divide selected keys by five because they are assembled from player-level rows. (`pbpstats/resources/possessions/possessions.py`, `tests/test_team_on_court_stats.py`)",
        "- Context docs are part of the repo contract: changes to exposed routes, contracts, invariants, or module boundaries must regenerate `context/REPO_ARCHITECTURE_SYNC.json` and the checked-in markdown artifacts in the same change. (`scripts/generate_repo_architecture_sync.py`, `tests/test_context_framework.py`)",
        "",
        "### Conventions",
        "- Resource configuration keys are CamelCase class names (`Boxscore`, `Possessions`, `Games`), but bound instance attributes are snake_case (`boxscore`, `possessions`, `games`).",
        "- `endpoint_strategy` is a supported source-loader option only for `stats_nba` `Pbp`, `EnhancedPbp`, and `Possessions`; supported values are `v2`, `v3_synthetic`, and `auto`, with `v2` as the compatibility default. It is ignored for `data_nba` and `live`; `v3_synthetic` supports NBA directly and WNBA only with a validated true-v2 role supplement. G League remains unsupported until league-specific fixtures prove parity.",
        "- Providers are named `stats_nba`, `data_nba`, and `live`; every routed loader pairs a `loader` with matching `file_source` and `web_source` classes.",
        "- Override files live under `<data dir>/overrides/` and should degrade to empty maps when absent.",
        "- `stats_nba` is the richest provider: it owns shot coordinates, v3 reorder repair, scoreboard/game-log splits for `Games`, and most regression coverage.",
        "- Tests rely on fixture JSON under `tests/data/` and typically validate derived behavior rather than mocking deep event internals.",
        "",
        "## Public Contract Snapshot",
    ]
    lines.extend(_render_contract_rows(contracts))
    lines.extend(
        [
            "",
            "## Core Abstractions",
            "- `Client`: metadata-driven binder that exposes objects and resources from a settings dict.",
            "- `Game` / `Day` / `Season`: object entrypoints whose constructors trigger bound loader/resource pairs.",
            "- `DataLoaderFactory`: registry that maps resource + provider to loader/file/web triplets.",
            "- `EnhancedPbp` and provider factories: shared event surface with provider-specific dispatch keys.",
            "- `StartOfPeriod`: period-boundary abstraction responsible for starters and opening-possession state.",
            "- `Possession` / `Possessions`: possession segmentation plus team/player/lineup aggregation surfaces.",
            "- `PbpProcessor`: offline orchestration layer that repairs event order and emits possessions from dataframes.",
            "",
            "## Module Dependency Map",
        ]
    )
    lines.extend(_render_dependency_lines(module_dependencies))
    lines.extend(
        [
            "",
            "## Where To Edit",
            "| Task | Start here | Also touch | Primary bundle |",
            "|---|---|---|---|",
            "| Add or change client wiring for a resource | `pbpstats/client.py` | `pbpstats/resources/__init__.py`, `pbpstats/data_loader/factory.py` | `COMPRESSED_core.md` |",
            "| Add a new provider/resource route | `pbpstats/data_loader/factory.py` | matching provider loader/file/web modules, `pbpstats/data_loader/__init__.py` | `COMPRESSED_loaders.md` |",
            "| Change stats.nba game/boxscore/pbp endpoint behavior | `pbpstats/data_loader/stats_nba/` | related file/web loaders and tests under `tests/data_loaders/` | `COMPRESSED_loaders.md` |",
            "| Change data/live schedule or game payload parsing | `pbpstats/data_loader/data_nba/` or `pbpstats/data_loader/live/` | matching resource item classes and loader tests | `COMPRESSED_loaders.md` |",
            "| Change enhanced event classification or provider factory dispatch | `pbpstats/resources/enhanced_pbp/*/enhanced_pbp_factory.py` | provider event subclasses, enhanced loader tests | `COMPRESSED_resources_events.md` |",
            "| Change period-starter inference or opening-possession rules | `pbpstats/resources/enhanced_pbp/start_of_period.py` | provider-specific start-of-period classes, `tests/test_period_starters_carryover.py` | `COMPRESSED_resources_events.md` |",
            "| Change shot clock annotation | `pbpstats/resources/enhanced_pbp/shot_clock.py` | enhanced loaders, shot-clock tests | `COMPRESSED_resources_events.md` |",
            "| Change possession splitting, offense-team logic, or aggregations | `pbpstats/resources/possessions/possession.py` | `pbpstats/resources/possessions/possessions.py`, `pbpstats/data_loader/stats_nba/possessions/loader.py` | `COMPRESSED_resources_core.md` |",
            "| Change offline dataframe repair flow | `pbpstats/offline/ordering.py` | `pbpstats/offline/processor.py`, possession regression tests | `COMPRESSED_core.md` |",
            "| Change resource wrapper convenience properties | `pbpstats/resources/boxscore/boxscore.py`, `games.py`, `pbp.py`, `shots.py`, or `possessions.py` | client/object tests and any downstream docs | `COMPRESSED_resources_core.md` |",
            "| Update regression expectations or add coverage for a bug fix | matching file under `tests/` | fixture JSON under `tests/data/` when needed | `COMPRESSED_tests.md` |",
            "",
            "## Bundle Picker",
        ]
    )
    lines.extend(_render_bundle_picker(bundle_contract))
    lines.extend(
        [
            "",
            "## Validation Surface",
            "- `tests/data_loaders/`: provider/file/web loader behavior against captured responses.",
            "- `tests/resources/`: event-level and possession-level rules, including jump balls, free throws, shot clock, and live game-end edge cases.",
            "- `tests/test_team_on_court_stats.py` and `tests/resources/test_full_game_possessions.py`: high-value regression checks on aggregated possession outputs.",
            "- `tests/test_context_framework.py`: keeps the context layer in sync with versioning, inventory, contract extraction, bundle routing, and token budget.",
            "",
            "## Grouped Module Catalog",
            "- `pbpstats/__init__.py`, `pbpstats/client.py`, `pbpstats/objects/`: package constants plus the dynamic object/resource binding layer.",
            "- `pbpstats/data_loader/`: provider-specific source loaders, shared base classes, and the factory registry that expose all supported routes.",
            "- `pbpstats/resources/boxscore`, `games`, `pbp`, `shots`, `possessions`: resource wrappers and item models that define the library's convenient read surface.",
            "- `pbpstats/resources/enhanced_pbp/`: the highest-risk subsystem; event classes, start-of-period/starter inference, rebound order assumptions, and shot clock annotation all converge here.",
            "- `pbpstats/offline/`: dataframe-oriented repair/orchestration path that reuses core event and possession logic outside the file/web loader flow.",
            "- `tests/`: fixture-backed contract suite for loaders, resources, possessions, and recent edge-case fixes.",
            "- `scripts/` and `context/`: repo-only context/governance tooling; these do not change runtime behavior but are part of the maintenance contract.",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def render_start_here() -> str:
    default_bundle = BUNDLE_CONTRACT["default_bundle"]
    return textwrap.dedent(
        """\
        # Start Here

        Default:
        1. Paste `context/REPO_ARCHITECTURE.md`
        2. Paste one matching `context/COMPRESSED_*.md`
        3. Ask your question

        Oracle:
        1. Paste `context/REPO_ARCHITECTURE.md`
        2. Paste `context/FILE_INDEX.md`
        3. Let the model request files by path

        If the task changes behavior, logic, math, routing, retries, serialization, or solver internals, also paste raw source of the files you expect to edit.

        If unsure which bundle to use, start with `context/%s`.
        """
        % default_bundle
    )


def render_checked_in_artifacts(sync_data: Optional[Dict[str, object]] = None) -> Dict[str, str]:
    sync_data = sync_data or build_sync_data()
    file_index = render_file_index(sync_data["file_inventory"])
    architecture = render_architecture_markdown(sync_data)
    return {
        "context/REPO_ARCHITECTURE_SYNC.json": render_sync_json(sync_data),
        "context/FILE_INDEX.md": file_index,
        "context/START_HERE.md": render_start_here(),
        "context/REPO_ARCHITECTURE.md": architecture,
    }


def _short_docstring(docstring: Optional[str]) -> Optional[str]:
    if not docstring:
        return None
    first = docstring.strip().split("\n\n", 1)[0].replace("\n", " ").strip()
    return re.sub(r"\s+", " ", first)


def _value_to_literal(node: ast.AST) -> str:
    if isinstance(node, ast.Constant):
        return repr(node.value)
    return "..."


def _public_methods_and_properties(class_node: ast.ClassDef) -> Tuple[List[str], List[str]]:
    methods = []
    properties = []
    for child in class_node.body:
        if not isinstance(child, ast.FunctionDef):
            continue
        if child.name.startswith("_"):
            continue
        decorator_names = []
        for decorator in child.decorator_list:
            if isinstance(decorator, ast.Name):
                decorator_names.append(decorator.id)
            elif isinstance(decorator, ast.Attribute):
                decorator_names.append(decorator.attr)
        if "property" in decorator_names or "abstractproperty" in decorator_names:
            properties.append(child.name)
        else:
            methods.append("%s%s" % (child.name, _render_ast_signature(child)))
    return methods, properties


def render_compressed_python_file(path: Path) -> str:
    tree = _parse_python_module(path)
    module_doc = _short_docstring(ast.get_docstring(tree))
    lines = ["## `%s`" % path.relative_to(REPO_ROOT).as_posix()]
    if module_doc:
        lines.append("Purpose: %s" % module_doc)

    constants = []
    classes = []
    functions = []
    for node in tree.body:
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = []
            value = node.value if isinstance(node, ast.AnnAssign) else node.value
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id.isupper():
                        targets.append(target.id)
            elif isinstance(node.target, ast.Name) and node.target.id.isupper():
                targets.append(node.target.id)
            for target in targets:
                constants.append("%s = %s" % (target, _value_to_literal(value)))
        elif isinstance(node, ast.ClassDef):
            init_signature = None
            for child in node.body:
                if isinstance(child, ast.FunctionDef) and child.name == "__init__":
                    init_signature = _render_ast_signature(child)
                    break
            methods, properties = _public_methods_and_properties(node)
            class_line = "- `%s%s`" % (node.name, init_signature or "()")
            doc = _short_docstring(ast.get_docstring(node))
            if doc:
                class_line += ": %s" % doc
            if properties:
                class_line += " Properties: %s." % ", ".join("`%s`" % name for name in properties[:8])
            if methods:
                class_line += " Methods: %s." % ", ".join("`%s`" % name for name in methods[:8])
            classes.append(class_line)
        elif isinstance(node, ast.FunctionDef) and not node.name.startswith("_"):
            function_line = "- `%s%s`" % (node.name, _render_ast_signature(node))
            doc = _short_docstring(ast.get_docstring(node))
            if doc:
                function_line += ": %s" % doc
            functions.append(function_line)

    if constants:
        lines.append("Constants:")
        lines.extend("- `%s`" % constant for constant in constants[:10])
    if classes:
        lines.append("Classes:")
        lines.extend(classes)
    if functions:
        lines.append("Functions:")
        lines.extend(functions)
    return "\n".join(lines)


def _bundle_paths(bundle_name: str) -> List[Path]:
    results = []
    for item in BUNDLE_FILE_GROUPS[bundle_name]:
        target = REPO_ROOT / item
        if target.is_dir():
            for path in sorted(target.rglob("*.py")):
                relative_path = path.relative_to(REPO_ROOT).as_posix()
                if _is_accidental_duplicate(relative_path):
                    continue
                if relative_path.startswith("tests/data/"):
                    continue
                results.append(path)
        elif target.is_file():
            results.append(target)
    return results


def render_bundle(bundle_name: str, purpose: str) -> str:
    lines = [
        "Use this as a navigation bundle for `%s`." % purpose,
        "Pair it with `context/REPO_ARCHITECTURE.md`.",
        "For implementation tasks, still paste raw source of the files you plan to edit.",
        "",
        "# %s" % bundle_name.replace(".md", "").replace("_", " "),
        "",
    ]
    for path in _bundle_paths(bundle_name):
        lines.append(render_compressed_python_file(path))
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def render_context_budget(
    architecture_text: str, bundle_texts: Dict[str, str], include_src: bool = False
) -> str:
    lines = [
        "# Context Budget",
        "",
        "Generated locally from the repository context builder.",
        "Use `context/REPO_ARCHITECTURE.md` first, then add exactly one bundle unless the task is very local.",
        "For implementation tasks, add raw source of the touched files.",
        "",
        "| Artifact | Tokens (`o200k_base`) |",
        "|---|---:|",
        "| `REPO_ARCHITECTURE.md` | %s |" % count_tokens(architecture_text),
    ]
    for bundle_name, text in sorted(bundle_texts.items()):
        lines.append("| `%s` | %s |" % (bundle_name, count_tokens(text)))
    lines.extend(
        [
            "",
            "## Combined Guided Context",
            "",
            "| Bundle | Combined tokens with architecture doc | Band |",
            "|---|---:|---|",
        ]
    )
    arch_tokens = count_tokens(architecture_text)
    for bundle_name, text in sorted(bundle_texts.items()):
        combined = arch_tokens + count_tokens(text)
        if combined <= 15000:
            band = "Green"
        elif combined <= 18000:
            band = "Yellow"
        else:
            band = "Red"
        lines.append("| `%s` | %s | %s |" % (bundle_name, combined, band))
    lines.extend(
        [
            "",
            "Default guided bundle: `context/%s`."
            % BUNDLE_CONTRACT["default_bundle"],
            "Full `COMPRESSED_SRC.md` was not generated because split bundles are the default and keep the prompt budget healthier."
            if not include_src
            else "A full `COMPRESSED_SRC.md` bundle was generated as an optional reference only.",
            "",
            "If a task changes runtime behavior, routing, aggregation semantics, or public contracts, do not rely on these bundles alone; add raw source.",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def write_artifacts(artifacts: Dict[str, str]) -> None:
    for relative_path, contents in artifacts.items():
        path = REPO_ROOT / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents, encoding="utf-8")
