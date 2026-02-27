import os
from dagster import AssetExecutionContext
from dagster_dbt import DagsterDbtTranslator, dbt_assets, DbtCliResource
from pathlib import Path
import json

dbt_project_dir = Path(__file__).joinpath("..", "..", "..", "..", "dbt").resolve()
dbt_manifest_path = dbt_project_dir.joinpath("target", "manifest.json")

class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    pass

@dbt_assets(
    manifest=dbt_manifest_path,
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
)
def araneus_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
