Use this as the repo-level map when it helps.
Pair with one `COMPRESSED_*.md` bundle for guided context, or with `FILE_INDEX.md` for oracle workflows.
For implementation tasks, prefer raw source of the files you expect to edit.

Architecture sync version: 1.5.2

## TL;DR
pbpstats is a single-package Python library for loading NBA, WNBA, and G-League play-by-play data from `stats_nba`, `data_nba`, and `live` providers, then normalizing it into resource wrappers (`Game`, `Day`, `Season`, `Boxscore`, `EnhancedPbp`, `Possessions`, and related item types). The core idea is metadata-driven routing: `Client(settings)` discovers loader classes from package exports, binds them to object classes by `parent_object`, and lets resource loaders compose richer state like lineups, starters, shot clocks, possession splits, and offline repair flows without changing the public object/resource surface.

## Behavior / Routing Matrix
| Surface | Inputs | Route | Result |
|---|---|---|---|
| `Client(settings)` | Resource name + `source` + `data_provider` + supported source-loader options | `pbpstats.resources.__all__` + `DataLoaderFactory.loaders` | Binds `*DataLoaderClass`, `*DataSource`, supported `*DataSourceOptions`, and resource class onto `Game` / `Day` / `Season`. |
| `Game(...)` resources | `Boxscore`, `Pbp`, `EnhancedPbp`, `Possessions`, `Shots` | Loader metadata with `parent_object = "Game"` | Instantiates snake-case attributes like `game.boxscore`, `game.enhanced_pbp`, `game.possessions`. |
| stats PBP web/file source | v2 cache/endpoint or `endpoint_strategy` = `v3_synthetic` / `auto` | `StatsNbaPbp*Loader` + private v3 synthetic transformer | Returns v2-shaped rows for validated NBA v3 payloads and WNBA v3 payloads with a validated true-v2 role supplement; true v2 cache stays under `/pbp`, raw v3 under `/pbp_v3`, synthetic rows under `/pbp_synthetic_v3`. |
| `Day(...)` games | `Games` + `stats_nba` scoreboard route | `StatsNbaScoreboardLoader` | Returns same `Games` resource surface, but day-scoped. |
| `Season(...)` games | `Games` + provider season schedule route | `StatsNbaLeagueGameLogLoader`, `DataNbaScheduleLoader`, `LiveScheduleLoader` | Returns season-scoped `games`. |
| stats enhanced PBP | raw stats rows + optional shots/v3/boxscore sources | `StatsNbaEnhancedPbpFactory(EVENTMSGTYPE)` -> enrich -> rebound order repair -> shot XY | Produces linked enhanced events with lineups, fouls-to-give, score, starters, and shot clock. |
| data/live enhanced PBP | provider-specific raw event payloads | Provider factory -> shared enrichment | Produces the same high-level event surface with provider-specific parsing. |
| possessions | ordered enhanced events | `_split_events_by_possession()` + `Possession(...)` + alternation checks | Derives possession start/end, offense team, start type, team/player/lineup aggregations. |
| offline dataframe path | pandas frame + optional v3 fetcher + optional/inferred league | normalize -> dedupe -> patch period starts -> reorder -> `PbpProcessor` | Returns a `Possessions` resource without going through on-disk web/file loaders. |
| offline nba-on-court path | season + game id + NBA/WNBA league | optional `nba_on_court.load_nba_data` -> `get_possessions_from_df` | Convenience nba_data adapter; nba-on-court is optional. |

### Critical Invariants
- Loader discovery only sees classes exported from `pbpstats.data_loader` that define `resource`, `data_provider`, and `parent_object`; breaking those attrs silently removes a route. (`pbpstats/data_loader/factory.py`, `tests/test_client.py`)
- `stats_nba` synthetic v3 PBP must emit the existing playbyplayv2 row contract (`EVENTMSGTYPE`, `EVENTMSGACTIONTYPE`, `PLAYER1_*`, `PLAYER2_*`, `PLAYER3_*`) so the PBP, enhanced PBP, and possession stacks stay unchanged. NBA synthetic rows can be built from v3 alone. WNBA synthetic rows require a validated true-v2 role supplement because WNBA playbyplayv3 is not role-complete enough for foul-drawn and jump-ball role parity. WNBA shotchartdetail remains a shots/enhanced-coordinate source, not a participant-role source. (`pbpstats/data_loader/stats_nba/pbp/v3_synthetic.py`, `tests/data_loaders/test_stats_v3_synthetic.py`)
- True v2 file/cache data remains canonical; synthetic v3 rows use `/pbp_synthetic_v3`, raw v3 uses `/pbp_v3`, and `auto` fallback only triggers for missing or malformed v2 source data. (`pbpstats/data_loader/stats_nba/pbp/file.py`, `pbpstats/data_loader/stats_nba/pbp/web.py`)
- Event linking and enrichment happen before possession logic or shot-clock annotation; `previous_event` / `next_event`, score, foul state, and override flags must exist first. (`pbpstats/data_loader/nba_enhanced_pbp_loader.py`)
- Offline processing normalizes the single-game `GAME_ID` column to a 10-character string before raw dict conversion; league-sensitive behavior uses an explicit `league` override when provided, otherwise game-id prefix inference with NBA fallback; WNBA synthetic period starts must use 20:00 halves before 2006, 10:00 quarters starting in 2006, and 5:00 overtime. (`pbpstats/offline/ordering.py`, `pbpstats/offline/processor.py`, `tests/test_offline_ordering.py`, `tests/test_offline_boxscore_loader.py`)
- `StatsNbaPossessionLoader` expects possessions to alternate offensive teams unless a known bad-PBP override or flagrant-foul exception applies. (`pbpstats/data_loader/stats_nba/possessions/loader.py`)
- Start-of-period handling must resolve five starters per team, optionally using overrides or previous-period ending lineups to fill gaps; WNBA period-start windows follow the historical 2006 switch from 20-minute halves to 10-minute quarters, with pre-2006 WNBA period 3+ treated as overtime for opening jump balls, foul-state reset, and non-negative elapsed-time guards. (`pbpstats/resources/enhanced_pbp/start_of_period.py`, `pbpstats/resources/enhanced_pbp/enhanced_pbp_item.py`, `pbpstats/data_loader/nba_enhanced_pbp_loader.py`, `tests/test_period_starters_carryover.py`, `tests/resources/test_enhanced_pbp_item.py`)
- Shot-clock annotation uses loader season or string/numeric event/game-id fallback for league thresholds: NBA rim-retention short reset starts in 2018-19, WNBA/G-League/D-League in 2016, and retained defensive stops, including stats-style kicked balls, use same-or-14 for supported leagues. (`pbpstats/resources/enhanced_pbp/shot_clock.py`, `pbpstats/data_loader/nba_enhanced_pbp_loader.py`, `tests/test_shot_clock.py`)
- Live enhanced PBP computes shot clock after defensive rebound team-id normalization; moving that pass before normalization changes live possession semantics. (`pbpstats/data_loader/live/enhanced_pbp/loader.py`, `tests/test_shot_clock.py`)
- Team and lineup possession aggregations divide selected keys by five because they are assembled from player-level rows. (`pbpstats/resources/possessions/possessions.py`, `tests/test_team_on_court_stats.py`)

### Conventions
- Resource configuration keys are CamelCase class names (`Boxscore`, `Possessions`, `Games`), but bound instance attributes are snake_case (`boxscore`, `possessions`, `games`).
- `endpoint_strategy` is a supported source-loader option only for `stats_nba` `Pbp`, `EnhancedPbp`, and `Possessions`; supported values are `v2`, `v3_synthetic`, and `auto`, with `v2` as the compatibility default. It is ignored for `data_nba` and `live`; `v3_synthetic` supports NBA directly and WNBA only with a validated true-v2 role supplement. G League remains unsupported until league-specific fixtures prove parity.
- Providers are named `stats_nba`, `data_nba`, and `live`; every routed loader pairs a `loader` with matching `file_source` and `web_source` classes.
- Offline `league` overrides are lower-case league strings such as `wnba`; when absent, `GAME_ID` prefixes (`00`, `10`, `20`) drive league-sensitive period-start and shot-clock behavior.
- The offline nba-on-court adapter supports NBA/WNBA, imports on use, and delegates to the dataframe path.
- Override files live under `<data dir>/overrides/` and should degrade to empty maps when absent.
- `stats_nba` is the richest provider: it owns shot coordinates, v3 reorder repair, scoreboard/game-log splits for `Games`, and most regression coverage.
- Tests rely on fixture JSON under `tests/data/` and typically validate derived behavior rather than mocking deep event internals.

## Public Contract Snapshot
| Surface | Signature | Notes |
|---|---|---|
| `Client` | `(self, settings)` | Settings keys mirror resource class names; each resource config requires `source` and `data_provider`. |
| `Day` | `(self, date, league)` | Object wrapper that instantiates bound resources based on client wiring. |
| `Game` | `(self, game_id)` | Object wrapper that instantiates bound resources based on client wiring. |
| `Season` | `(self, league, season, season_type)` | Object wrapper that instantiates bound resources based on client wiring. |
| `Boxscore` | `(self, items)` | Properties: `data, player_items, player_name_map, player_team_map, team_items`. |
| `EnhancedPbp` | `(self, items)` | Properties: `data, fgas, fgms, ftas, rebounds, turnovers`. |
| `Games` | `(self, items)` | Properties: `data, final_games`. |
| `Pbp` | `(self, items)` | Properties: `data`. |
| `Possessions` | `(self, items)` | Properties: `data, lineup_opponent_stats, lineup_stats, opponent_stats, player_stats, team_stats`. |
| `Shots` | `(self, items)` | Properties: `data`. |
| `PbpProcessor` | `(self, game_id, raw_data_dicts, rebound_deletions_list=..., boxscore_source_loader=..., period_boxscore_source_loader=..., file_directory=..., league=...)` | Offline bridge that turns stats-style event dicts into enhanced events and possessions; `league` overrides game-id inference. |
| `get_possessions_from_df` | `(game_df, fetch_pbp_v3_fn=..., rebound_deletions_list=..., boxscore_source_loader=..., period_boxscore_source_loader=..., file_directory=..., league=...)` | Offline convenience entrypoint for pandas workflows; `league` is optional and otherwise inferred from `GAME_ID`. |
| `load_nba_on_court_pbp` | `(season, league=..., season_type=..., path=...)` | Optional nba-on-court season PBP loader. |
| `get_possessions_from_nba_on_court` | `(season, game_id, league=..., season_type=..., path=..., fetch_pbp_v3_fn=..., rebound_deletions_list=..., boxscore_source_loader=..., period_boxscore_source_loader=..., file_directory=...)` | Optional nba-on-court game-to-possessions helper. |

## Core Abstractions
- `Client`: metadata-driven binder that exposes objects and resources from a settings dict.
- `Game` / `Day` / `Season`: object entrypoints whose constructors trigger bound loader/resource pairs.
- `DataLoaderFactory`: registry that maps resource + provider to loader/file/web triplets.
- `EnhancedPbp` and provider factories: shared event surface with provider-specific dispatch keys.
- `StartOfPeriod`: period-boundary abstraction responsible for starters and opening-possession state.
- `Possession` / `Possessions`: possession segmentation plus team/player/lineup aggregation surfaces.
- `PbpProcessor`: offline orchestration layer that repairs event order and emits possessions from dataframes.

## Module Dependency Map
- `client` -> `data_loader.core`, `objects`, `resources.core`
- `objects` -> `client`
- `resources.core` -> `package.constants`, `resources.enhanced_pbp`
- `resources.enhanced_pbp` -> `data_loader.stats_nba`, `package.constants`, `resources.core`
- `data_loader.core` -> `data_loader.data_nba`, `data_loader.live`, `data_loader.stats_nba`, `package.constants`, `resources.enhanced_pbp`
- `data_loader.stats_nba` -> `data_loader.core`, `data_loader.data_nba`, `package.constants`, `resources.core`, `resources.enhanced_pbp`
- `data_loader.data_nba` -> `data_loader.core`, `package.constants`, `resources.core`, `resources.enhanced_pbp`
- `data_loader.live` -> `data_loader.core`, `package.constants`, `resources.core`, `resources.enhanced_pbp`
- `offline` -> `data_loader.core`, `package.constants`, `resources.core`, `resources.enhanced_pbp`

## Where To Edit
| Task | Start here | Also touch | Primary bundle |
|---|---|---|---|
| Add or change client wiring for a resource | `pbpstats/client.py` | `pbpstats/resources/__init__.py`, `pbpstats/data_loader/factory.py` | `COMPRESSED_core.md` |
| Add a new provider/resource route | `pbpstats/data_loader/factory.py` | matching provider loader/file/web modules, `pbpstats/data_loader/__init__.py` | `COMPRESSED_loaders.md` |
| Change stats.nba game/boxscore/pbp endpoint behavior | `pbpstats/data_loader/stats_nba/` | related file/web loaders and tests under `tests/data_loaders/` | `COMPRESSED_loaders.md` |
| Change data/live schedule or game payload parsing | `pbpstats/data_loader/data_nba/` or `pbpstats/data_loader/live/` | matching resource item classes and loader tests | `COMPRESSED_loaders.md` |
| Change enhanced event classification or provider factory dispatch | `pbpstats/resources/enhanced_pbp/*/enhanced_pbp_factory.py` | provider event subclasses, enhanced loader tests | `COMPRESSED_resources_events.md` |
| Change period-starter inference or opening-possession rules | `pbpstats/resources/enhanced_pbp/start_of_period.py` | provider-specific start-of-period classes, `tests/test_period_starters_carryover.py` | `COMPRESSED_resources_events.md` |
| Change shot clock annotation | `pbpstats/resources/enhanced_pbp/shot_clock.py` | enhanced loaders, shot-clock tests | `COMPRESSED_resources_events.md` |
| Change possession splitting, offense-team logic, or aggregations | `pbpstats/resources/possessions/possession.py` | `pbpstats/resources/possessions/possessions.py`, `pbpstats/data_loader/stats_nba/possessions/loader.py` | `COMPRESSED_resources_core.md` |
| Change offline dataframe repair flow or source adapters | `pbpstats/offline/ordering.py` | `pbpstats/offline/processor.py`, `pbpstats/offline/nba_on_court.py`, possession regression tests | `COMPRESSED_core.md` |
| Change resource wrapper convenience properties | `pbpstats/resources/boxscore/boxscore.py`, `games.py`, `pbp.py`, `shots.py`, or `possessions.py` | client/object tests and any downstream docs | `COMPRESSED_resources_core.md` |
| Update regression expectations or add coverage for a bug fix | matching file under `tests/` | fixture JSON under `tests/data/` when needed | `COMPRESSED_tests.md` |

## Bundle Picker
| Task area | Bundle |
|---|---|
| Client wiring, objects, overrides, offline entrypoints | `COMPRESSED_core.md` |
| Loader factory, file/web sources, provider routing | `COMPRESSED_loaders.md` |
| Resource wrappers, possessions, boxscore/game/pbp/shots models | `COMPRESSED_resources_core.md` |
| Enhanced event classes, shot clock, start-of-period logic | `COMPRESSED_resources_events.md` |
| Behavioral tests, fixtures, regression coverage | `COMPRESSED_tests.md` |

Default: `context/REPO_ARCHITECTURE.md` + `context/COMPRESSED_core.md` + your question.
For implementation tasks, prefer raw source of the files you're editing.

## Validation Surface
- `tests/data_loaders/`: provider/file/web loader behavior against captured responses.
- `tests/resources/`: event-level and possession-level rules, including jump balls, free throws, shot clock, and live game-end edge cases.
- `tests/test_team_on_court_stats.py` and `tests/resources/test_full_game_possessions.py`: high-value regression checks on aggregated possession outputs.
- `tests/test_context_framework.py`: validates the optional context renderers, contract extraction, bundle routing, and token budget without requiring checked-in context refreshes for runtime changes.

## Grouped Module Catalog
- `pbpstats/__init__.py`, `pbpstats/client.py`, `pbpstats/objects/`: package constants plus the dynamic object/resource binding layer.
- `pbpstats/data_loader/`: provider-specific source loaders, shared base classes, and the factory registry that expose all supported routes.
- `pbpstats/resources/boxscore`, `games`, `pbp`, `shots`, `possessions`: resource wrappers and item models that define the library's convenient read surface.
- `pbpstats/resources/enhanced_pbp/`: the highest-risk subsystem; event classes, start-of-period/starter inference, rebound order assumptions, and shot clock annotation all converge here.
- `pbpstats/offline/`: dataframe-oriented repair/orchestration path that reuses core event and possession logic outside the file/web loader flow.
- `tests/`: fixture-backed contract suite for loaders, resources, possessions, and recent edge-case fixes.
- `scripts/` and `context/`: repo-only context/navigation tooling; these do not change runtime behavior.
