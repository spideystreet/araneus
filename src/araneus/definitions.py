import os
from dagster import Definitions, load_assets_from_modules
from .assets import dbt_assets, raw_data
from .resources import dbt_resource
from dotenv import load_dotenv

load_dotenv()

all_assets = load_assets_from_modules([dbt_assets, raw_data])

defs = Definitions(
    assets=all_assets,
    resources={
        "dbt": dbt_resource,
    },
)
