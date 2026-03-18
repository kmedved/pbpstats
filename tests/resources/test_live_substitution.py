from pbpstats.resources.enhanced_pbp.live.enhanced_pbp_item import LiveEnhancedPbpItem
from pbpstats.resources.enhanced_pbp.live.substitution import LiveSubstitution

TEAM_A = 1610612737
TEAM_B = 1610612738


class SeedEvent:
    def __init__(self, current_players):
        self._players = {
            team_id: [player_id for player_id in player_ids]
            for team_id, player_ids in current_players.items()
        }

    @property
    def current_players(self):
        return {
            team_id: [player_id for player_id in player_ids]
            for team_id, player_ids in self._players.items()
        }

    @property
    def _raw_current_players(self):
        return self.current_players


def build_live_substitution(*, team_id, player_id, sub_type, previous_event, order):
    item = {
        "actionType": "substitution",
        "subType": sub_type,
        "clock": "PT11M00.00S",
        "period": 1,
        "teamId": team_id,
        "personId": player_id,
        "possession": TEAM_A,
        "orderNumber": order,
    }
    event = LiveSubstitution(item, "0020000001")
    event.previous_event = previous_event
    event.next_event = None
    return event


def build_live_event(*, previous_event, order):
    item = {
        "actionType": "2pt",
        "subType": "Jump Shot",
        "clock": "PT10M59.00S",
        "period": 1,
        "teamId": TEAM_B,
        "personId": 999,
        "possession": TEAM_B,
        "orderNumber": order,
        "description": "MISS Test Shot",
    }
    event = LiveEnhancedPbpItem(item, "0020000001")
    event.previous_event = previous_event
    event.next_event = None
    return event


def test_live_substitution_raw_current_players_handles_individual_in_out_actions():
    seed = SeedEvent(
        {
            TEAM_A: [1, 2, 3, 4, 5],
            TEAM_B: [11, 12, 13, 14, 15],
        }
    )

    sub_out = build_live_substitution(
        team_id=TEAM_A,
        player_id=1,
        sub_type="out",
        previous_event=seed,
        order=10,
    )
    sub_in = build_live_substitution(
        team_id=TEAM_A,
        player_id=6,
        sub_type="in",
        previous_event=sub_out,
        order=11,
    )
    following = build_live_event(previous_event=sub_in, order=12)

    assert None not in sub_out._raw_current_players[TEAM_A]
    assert sub_out._raw_current_players[TEAM_A] == [2, 3, 4, 5]
    assert sub_in._raw_current_players[TEAM_A] == [2, 3, 4, 5, 6]
    assert following.current_players[TEAM_A] == [2, 3, 4, 5, 6]
