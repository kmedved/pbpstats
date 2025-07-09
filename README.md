[![PyPI version](https://badge.fury.io/py/pbpstats.svg)](https://badge.fury.io/py/pbpstats)

A package to scrape and parse NBA, WNBA and G-League play-by-play data.

# Features
* Adds lineup on floor for all events
* Adds detailed data for each possession including start time, end time, score margin, how the previous possession ended
* Shots, rebounds and assists broken down by shot zone
* Supports both stats.nba.com and data.nba.com endpoints
* Supports NBA, WNBA and G-League stats
* All stats on pbpstats.com are derived from these stats
* Fixes order of events for some common cases in which events are out of order
* Smarter inference of made/missed free-throws when the play-by-play feed omits "MISS"

# Installation
Tested on Python >=3.8
```
pip install pbpstats
```

# Resources
[Documentation](https://pbpstats.readthedocs.io/en/latest/)

For a high level tour of the library APIs and an example showing how to
export possession level data for RAPM see ``docs/usage_overview.rst``.

# Local Development
Using [poetry](https://python-poetry.org/) for package managment. Install it first if it is not install on your system.

`git clone https://github.com/dblackrun/pbpstats.git`

`cd pbpstats`

Develop using `develop` branch:
`git checkout develop`

Install dependencies:

`poetry install`

Activate virtualenv:

`poetry shell`

Install pre-commit:

`pre-commit install`
