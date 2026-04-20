# Project 05

## Install `uv`

- Using `pip`
  ```sh
    pip install uv
  ```

## Set Up Project

```sh
    # Navigate to the project
    cd project_05

    # Initialize a new uv project (creates pyproject.toml)
    uv init

    # Create venv with custom name
    uv venv .venv-project05

    # Activate it (Git Bash):
    source .venv-project05/Scripts/activate
```

## Install Packages

```sh
    # For PostgreSQL + data analysis
    uv pip install psycopg2-binary pymysql pandas numpy matplotlib seaborn sqlalchemy

    # Use this to add new packages:
    uv add package-name
```

## Verify Installation

```sh
    # Check installed packages
    uv pip list

    # Or freeze to requirements.txt
    uv pip freeze > requirements.txt
```

- Remarks
  - `uv pip install` = works like regular pip (no pyproject.toml update)
  - `uv add` = modern way (updates pyproject.toml automatically)

# Resources
