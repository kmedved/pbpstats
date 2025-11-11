import json
import os
import requests

from pbpstats import G_LEAGUE_STRING, NBA_STRING
from pbpstats.data_loader.stats_nba.web_loader import StatsNbaWebLoader
from pbpstats.net.cdn_client import get_pbp_actions
from pbpstats.data_loader.stats_nba.pbp.cdn_adapter import cdn_to_stats_row


class StatsNbaPbpWebLoader(StatsNbaWebLoader):
    """
    A ``StatsNbaPbpWebLoader`` object should be instantiated and passed into ``StatsNbaPbpLoader`` when loading data directly from the NBA Stats API

    :param str file_directory: (optional, use it if you want to store the response data on disk)
        Directory in which data should be either stored.
        The specific file location will be `stats_<game_id>.json` in the `/pbp` subdirectory.
        If not provided response data will not be saved on disk.
    """

    def __init__(self, file_directory=None):
        self.file_directory = file_directory

    def load_data(self, game_id):
        self.game_id = game_id

        # Only use CDN for NBA games (not G-League or WNBA)
        if self.league == NBA_STRING:
            return self._load_from_cdn()
        else:
            # Fallback to legacy playbyplayv2 for non-NBA leagues
            return self._load_from_legacy_api()

    def _load_from_cdn(self):
        """Load data from CDN and convert to v2 format"""
        try:
            # Fetch from CDN
            cdn_data = get_pbp_actions(self.game_id)
            actions = cdn_data["game"].get("actions") or []

            # Stable ordering: prefer orderNumber, fallback actionNumber
            actions.sort(
                key=lambda a: (a.get("orderNumber") or 0, a.get("actionNumber") or 0)
            )

            # Convert to v2-style rows
            rows = [cdn_to_stats_row(a, self.game_id) for a in actions]

            # Dedupe based on EVENTNUM and timeActual (if present)
            # Keep latest edited version
            seen = {}
            deduped_rows = []
            for row in rows:
                key = (row["EVENTNUM"], row.get("timeActual"))
                if key not in seen or row.get("edited"):
                    seen[key] = True
                    # Remove from previous position if it was there
                    deduped_rows = [r for r in deduped_rows if (r["EVENTNUM"], r.get("timeActual")) != key]
                    deduped_rows.append(row)

            # Format in v2 resultSets structure expected by base class
            # Extract headers from first row (all rows have same keys)
            if deduped_rows:
                # Use a consistent header order matching v2 API
                headers = [
                    "GAME_ID",
                    "EVENTNUM",
                    "EVENTMSGTYPE",
                    "EVENTMSGACTIONTYPE",
                    "PERIOD",
                    "WCTIMESTRING",
                    "PCTIMESTRING",
                    "HOMEDESCRIPTION",
                    "NEUTRALDESCRIPTION",
                    "VISITORDESCRIPTION",
                    "SCORE",
                    "SCOREMARGIN",
                    "PERSON1TYPE",
                    "PLAYER1_ID",
                    "PLAYER1_NAME",
                    "PLAYER1_TEAM_ID",
                    "PLAYER1_TEAM_CITY",
                    "PLAYER1_TEAM_NICKNAME",
                    "PLAYER1_TEAM_ABBREVIATION",
                    "PERSON2TYPE",
                    "PLAYER2_ID",
                    "PLAYER2_NAME",
                    "PLAYER2_TEAM_ID",
                    "PLAYER2_TEAM_CITY",
                    "PLAYER2_TEAM_NICKNAME",
                    "PLAYER2_TEAM_ABBREVIATION",
                    "PERSON3TYPE",
                    "PLAYER3_ID",
                    "PLAYER3_NAME",
                    "PLAYER3_TEAM_ID",
                    "PLAYER3_TEAM_CITY",
                    "PLAYER3_TEAM_NICKNAME",
                    "PLAYER3_TEAM_ABBREVIATION",
                    "VIDEO_AVAILABLE_FLAG",
                ]

                # Convert rows to arrays matching header order
                # Keep extra fields in the dict but only serialize standard headers
                row_arrays = []
                for row in deduped_rows:
                    row_array = [row.get(h) for h in headers]
                    row_arrays.append(row_array)
            else:
                headers = []
                row_arrays = []

            self.source_data = {
                "resource": "playbyplay",
                "parameters": {"GameId": self.game_id},
                "resultSets": [
                    {"name": "PlayByPlay", "headers": headers, "rowSet": row_arrays}
                ],
            }

            self._save_data_to_file()
            return self.source_data

        except (requests.HTTPError, ValueError) as e:
            # If CDN fails, could optionally fallback to legacy API
            # For now, re-raise the error
            raise

    def _load_from_legacy_api(self):
        """Fallback to legacy playbyplayv2 API"""
        league_url_part = (
            f"{G_LEAGUE_STRING}.{NBA_STRING}"
            if self.league == G_LEAGUE_STRING
            else self.league
        )
        self.base_url = f"https://stats.{league_url_part}.com/stats/playbyplayv2"
        self.parameters = {
            "GameId": self.game_id,
            "StartPeriod": 0,
            "EndPeriod": 10,
            "RangeType": 2,
            "StartRange": 0,
            "EndRange": 55800,
        }
        return self._load_request_data()

    def _save_data_to_file(self):
        if self.file_directory is not None and os.path.isdir(self.file_directory):
            file_path = f"{self.file_directory}/pbp/stats_{self.game_id}.json"
            with open(file_path, "w") as outfile:
                json.dump(self.source_data, outfile)
