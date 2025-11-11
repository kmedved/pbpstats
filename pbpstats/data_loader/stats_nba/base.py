from pbpstats import (
    G_LEAGUE_GAME_ID_PREFIX,
    G_LEAGUE_STRING,
    NBA_GAME_ID_PREFIX,
    NBA_STRING,
    PLAY_IN_STRING,
    PLAYOFFS_STRING,
    REGULAR_SEASON_STRING,
    WNBA_GAME_ID_PREFIX,
    WNBA_STRING,
)


class StatsNbaLoaderBase(object):
    """
    Base Class for all stats.nba.com data loaders

    This class should not be instantiated directly
    """

    @staticmethod
    def transform_v3_to_v2_format(v3_response):
        """
        Transforms playbyplayv3 response format to playbyplayv2 format for backward compatibility

        :param dict v3_response: Response from playbyplayv3 endpoint
        :returns: dict in playbyplayv2 format
        """
        # Check if already in v2 format by looking at the headers
        if "resultSets" in v3_response:
            # Check if it's v2 format (headers in UPPERCASE) or v3 format (headers in camelCase)
            pbp_result_set = next((rs for rs in v3_response["resultSets"] if rs["name"] == "PlayByPlay"), None)
            if pbp_result_set and "GAME_ID" in pbp_result_set["headers"]:
                # Already in v2 format
                return v3_response
            # Otherwise, it's v3 format with resultSets structure - continue with transformation

        # V2 field structure
        v2_headers = [
            "GAME_ID", "EVENTNUM", "EVENTMSGTYPE", "EVENTMSGACTIONTYPE", "PERIOD",
            "WCTIMESTRING", "PCTIMESTRING", "HOMEDESCRIPTION", "NEUTRALDESCRIPTION",
            "VISITORDESCRIPTION", "SCORE", "SCOREMARGIN", "PERSON1TYPE", "PLAYER1_ID",
            "PLAYER1_NAME", "PLAYER1_TEAM_ID", "PLAYER1_TEAM_CITY", "PLAYER1_TEAM_NICKNAME",
            "PLAYER1_TEAM_ABBREVIATION", "PERSON2TYPE", "PLAYER2_ID", "PLAYER2_NAME",
            "PLAYER2_TEAM_ID", "PLAYER2_TEAM_CITY", "PLAYER2_TEAM_NICKNAME",
            "PLAYER2_TEAM_ABBREVIATION", "PERSON3TYPE", "PLAYER3_ID", "PLAYER3_NAME",
            "PLAYER3_TEAM_ID", "PLAYER3_TEAM_CITY", "PLAYER3_TEAM_NICKNAME",
            "PLAYER3_TEAM_ABBREVIATION", "VIDEO_AVAILABLE_FLAG"
        ]

        v2_rows = []

        # Get play by play actions from v3 format
        # V3 might use resultSets structure or a different structure
        if "resultSets" in v3_response:
            pbp_result_set = next((rs for rs in v3_response["resultSets"] if rs["name"] == "PlayByPlay"), None)
            if pbp_result_set:
                headers = pbp_result_set["headers"]
                rows = pbp_result_set["rowSet"]
                # Convert rows to list of dicts for easier processing
                actions = [dict(zip(headers, row)) for row in rows]
            else:
                actions = []
        else:
            play_by_play_data = v3_response.get("playByPlay", {})
            actions = play_by_play_data.get("actions", [])

        for action in actions:
            # Build score and score margin
            score_home = action.get("scoreHome")
            score_away = action.get("scoreAway")
            if score_home is not None and score_away is not None:
                score = f"{score_away} - {score_home}"
                score_margin = score_home - score_away
                if score_margin > 0:
                    score_margin = f"+{score_margin}"
                elif score_margin < 0:
                    score_margin = str(score_margin)
                else:
                    score_margin = "TIE"
            else:
                score = None
                score_margin = None

            # Parse description to determine home/visitor/neutral
            description = action.get("description", "")
            team_id = action.get("teamId")
            # We'll put all descriptions in NEUTRALDESCRIPTION for now
            # A more sophisticated approach would determine home vs visitor based on teamId
            home_desc = None
            visitor_desc = None
            neutral_desc = description if description else None

            # Map actionType to EVENTMSGTYPE
            # This requires understanding the mapping between v3 actionType and v2 EVENTMSGTYPE
            action_type = action.get("actionType")
            sub_type = action.get("subType")

            # Create v2 row
            v2_row = [
                action.get("gameId"),  # GAME_ID
                action.get("actionNumber"),  # EVENTNUM
                action_type,  # EVENTMSGTYPE (actionType in v3)
                sub_type,  # EVENTMSGACTIONTYPE (subType in v3)
                action.get("period"),  # PERIOD
                None,  # WCTIMESTRING (not available in v3)
                action.get("clock"),  # PCTIMESTRING
                home_desc,  # HOMEDESCRIPTION
                neutral_desc,  # NEUTRALDESCRIPTION
                visitor_desc,  # VISITORDESCRIPTION
                score,  # SCORE
                score_margin,  # SCOREMARGIN
                1 if action.get("personId") and action.get("personId") != 0 else 0,  # PERSON1TYPE
                action.get("personId", 0),  # PLAYER1_ID
                action.get("playerName"),  # PLAYER1_NAME
                action.get("teamId"),  # PLAYER1_TEAM_ID
                None,  # PLAYER1_TEAM_CITY (not available in v3)
                None,  # PLAYER1_TEAM_NICKNAME (not available in v3)
                action.get("teamTricode"),  # PLAYER1_TEAM_ABBREVIATION
                0,  # PERSON2TYPE
                0,  # PLAYER2_ID
                None,  # PLAYER2_NAME
                None,  # PLAYER2_TEAM_ID
                None,  # PLAYER2_TEAM_CITY
                None,  # PLAYER2_TEAM_NICKNAME
                None,  # PLAYER2_TEAM_ABBREVIATION
                0,  # PERSON3TYPE
                0,  # PLAYER3_ID
                None,  # PLAYER3_NAME
                None,  # PLAYER3_TEAM_ID
                None,  # PLAYER3_TEAM_CITY
                None,  # PLAYER3_TEAM_NICKNAME
                None,  # PLAYER3_TEAM_ABBREVIATION
                action.get("videoAvailable", 0),  # VIDEO_AVAILABLE_FLAG
            ]
            v2_rows.append(v2_row)

        # Build v2 format response
        v2_response = {
            "resource": "playbyplay",
            "parameters": v3_response.get("parameters", {}),
            "resultSets": [
                {
                    "name": "PlayByPlay",
                    "headers": v2_headers,
                    "rowSet": v2_rows
                },
                {
                    "name": "AvailableVideo",
                    "headers": ["VIDEO_AVAILABLE_FLAG"],
                    "rowSet": [[1] if any(action.get("videoAvailable") for action in actions) else [0]]
                }
            ]
        }

        return v2_response

    def make_list_of_dicts(self, results_set_index=0):
        """
        Creates list of dicts from source data

        :param int results_set_index: Index results are in. Default is 0
        :returns: list of dicts with data for results
        """
        headers = self.source_data["resultSets"][results_set_index]["headers"]
        rows = self.source_data["resultSets"][results_set_index]["rowSet"]
        deduped_rows = self.dedupe_events_row_set(rows)
        return [dict(zip(headers, row)) for row in deduped_rows]

    @staticmethod
    def dedupe_events_row_set(events_row_set):
        """
        Dedupes list of results while preserving order

        Used to dedupe events rowSets pbp response because some games have duplicate events

        :param list events_row_set: List of results from API Response
        :returns: deduped list of results
        """
        deduped_events_row_set = []
        for sublist in events_row_set:
            if sublist not in deduped_events_row_set:
                deduped_events_row_set.append(sublist)
        return deduped_events_row_set

    @property
    def data(self):
        """
        returns data from response JSON as a list of dicts
        """
        return self.make_list_of_dicts()

    @property
    def league(self):
        """
        Returns League for game id.

        First 2 in game id represent league - 00 for nba, 10 for wnba, 20 for g-league
        """
        if self.game_id[0:2] == NBA_GAME_ID_PREFIX:
            return NBA_STRING
        elif self.game_id[0:2] == G_LEAGUE_GAME_ID_PREFIX:
            return G_LEAGUE_STRING
        elif self.game_id[0:2] == WNBA_GAME_ID_PREFIX:
            return WNBA_STRING

    @property
    def season(self):
        """
        Returns season for game id

        4th and 5th characters in game id represent season year
        ex. for 2016-17 season 4th and 5th characters would be 16 and season should return 2016-17
        For WNBA just returns season year
        """
        digit4 = int(self.game_id[3])
        digit5 = int(self.game_id[4])
        if digit4 == 9:
            if digit5 == 9:
                return "1999" if self.league == WNBA_STRING else "1999-00"
            else:
                return (
                    f"19{digit4}{digit5}"
                    if self.league == WNBA_STRING
                    else f"19{digit4}{digit5}-{digit4}{digit5 + 1}"
                )
        elif digit5 == 9:
            return (
                f"20{digit4}{digit5}"
                if self.league == WNBA_STRING
                else f"20{digit4}{digit5}-{digit4 + 1}0"
            )
        else:
            return (
                f"20{digit4}{digit5}"
                if self.league == WNBA_STRING
                else f"20{digit4}{digit5}-{digit4}{digit5 + 1}"
            )

    @property
    def season_type(self):
        """
        Returns season type for game id

        3rd character in game id represent season type - 2 for reg season, 4 for playoffs, 5 for play in
        """
        if self.game_id[2] == "4":
            return PLAYOFFS_STRING
        elif self.game_id[2] == "2":
            return REGULAR_SEASON_STRING
        elif self.game_id[2] == "5":
            return PLAY_IN_STRING

    @property
    def league_id(self):
        """
        Returns League Id for league.

        00 for nba, 10 for wnba, 20 for g-league
        """
        if self.league_string == NBA_STRING:
            return NBA_GAME_ID_PREFIX
        elif self.league_string == WNBA_STRING:
            return WNBA_GAME_ID_PREFIX
        elif self.league_string == G_LEAGUE_STRING:
            return G_LEAGUE_GAME_ID_PREFIX
