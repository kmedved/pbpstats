import time

from nba_stats_parser.nba_stats_parser import Game

GAME_ID = "0022200001"


def main():
    start = time.time()
    Game(GAME_ID).possessions
    duration = time.time() - start
    # Fail if slower than 6 seconds (approx 1.2x 5s baseline)
    assert duration < 6.0, f"Parsing took {duration:.2f}s"
    print(f"parsed in {duration:.2f}s")


if __name__ == "__main__":
    main()
