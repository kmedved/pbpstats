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

# Installation
Tested on Python >=3.8
```
pip install pbpstats
```

# Resources
[Documentation](https://pbpstats.readthedocs.io/en/latest/)

# LLM Context
LLM-facing context artifacts live in `context/`.

- Start with `context/REPO_ARCHITECTURE.md`
- For guided context, add one `context/COMPRESSED_*.md`
- For oracle workflows, add `context/FILE_INDEX.md`
- For implementation tasks, add raw source for the touched files

Refresh checked-in context artifacts with `python scripts/generate_repo_architecture_sync.py`.
Refresh local bundles with `python scripts/build_context_bundle.py`.
Version policy: Policy B, so only shipped/runtime behavior changes require a version bump.

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
