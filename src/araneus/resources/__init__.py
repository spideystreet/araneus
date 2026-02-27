import os
from dagster_dbt import DbtCliResource
from pathlib import Path

dbt_project_dir = Path(__file__).joinpath("..", "..", "..", "..", "dbt").resolve()

dbt_resource = DbtCliResource(
    project_dir=os.fspath(dbt_project_dir),
    profiles_dir=os.fspath(dbt_project_dir),
)
