# Scripts (Project 3)

## Table of Contents

## Setup

Dependencies are managed with [uv](https://docs.astral.sh/uv/).

### Project files

| File              | Purpose                                                                                                     | Edit by hand?                 |
| ----------------- | ----------------------------------------------------------------------------------------------------------- | ----------------------------- |
| `pyproject.toml`  | Project metadata and the list of direct dependencies (replaces `requirements.txt`).                         | Rarely; use `uv add`/`remove` |
| `uv.lock`         | Exact resolved versions of every package (including sub-dependencies) so installs are identical everywhere. | No; uv manages it             |
| `.python-version` | The Python version uv uses for this project's `.venv` (currently `3.11`).                                   | Only to change Python         |

All three should be committed to git; `.venv/` should not.

### First-time setup

```sh
uv sync    # creates .venv and installs the exact versions from uv.lock
```

### Installing packages

```sh
uv add <package>               # e.g. uv add pandas
uv add "<package>==<version>"  # pin a specific version
uv remove <package>            # uninstall and remove from pyproject.toml
```

`uv add` installs the package into `.venv` and updates both `pyproject.toml` and `uv.lock`. There is no need to use `pip install`.

### Running scripts

```sh
uv run python <path/to/script.py>
# e.g.
uv run python 04_adx/python/sync_device_daily_usage_to_clickhouse/v1/sync_device_daily_usage_to_clickhouse.py
```

`uv run` runs the command inside the project's `.venv` (syncing it first if needed), so you don't have to activate the venv. Run it from the `project_03` folder.
