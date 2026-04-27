from historic_backfill.audits.cross_source.trace_plus_minus_game import _collect_same_clock_window, _serialize_lineups


class DummyEvent:
    def __init__(self, *, period, clock, event_num):
        self.period = period
        self.clock = clock
        self.event_num = event_num


def test_collect_same_clock_window_returns_contiguous_same_clock_events():
    events = [
        DummyEvent(period=1, clock="10:00", event_num=1),
        DummyEvent(period=1, clock="9:59", event_num=2),
        DummyEvent(period=1, clock="9:59", event_num=3),
        DummyEvent(period=1, clock="9:59", event_num=4),
        DummyEvent(period=1, clock="9:58", event_num=5),
    ]

    window = _collect_same_clock_window(events, 2)

    assert [event.event_num for event in window] == [2, 3, 4]


def test_serialize_lineups_preserves_ids_and_names():
    lineups = {100: [1, 2], 200: [3, 4]}
    name_map = {1: "One", 2: "Two", 3: "Three", 4: "Four"}

    serialized = _serialize_lineups(lineups, name_map)

    assert serialized == {
        "100": {"ids": [1, 2], "names": ["One", "Two"]},
        "200": {"ids": [3, 4], "names": ["Three", "Four"]},
    }
