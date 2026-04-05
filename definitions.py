from dagster import Definitions, load_assets_from_modules, AssetKey
from dagster_dbt import dbt_assets, DbtCliResource, DagsterDbtTranslator
from dagster_snowflake import SnowflakeResource
from pathlib import Path
import os
import assets  # Make sure assets.py wahi hai jisme naya Snowflake logic hai

# 1. dbt Project Path
dbt_project_dir = Path(__file__).parent.joinpath("analytics").resolve()

# 2. Magic Translator: Python Asset aur dbt Model ko jodne ke liye
class MyCustomTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        # Jab dbt 'raw_prices' source dhundhega, toh wo Dagster ke 'snowflake_load' asset se connect hoga
        if dbt_resource_props.get("name") == "raw_prices":
            return AssetKey(["snowflake_load"])
        return super().get_asset_key(dbt_resource_props)

# 3. Load dbt Assets
@dbt_assets(
    manifest=dbt_project_dir.joinpath("target", "manifest.json"),
    dagster_dbt_translator=MyCustomTranslator()
)
def my_dbt_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

# 4. Load Python Assets (Jo assets.py mein hain)
all_python_assets = load_assets_from_modules([assets])

# 5. Final Definitions (Yahan humne Snowflake Resource add kiya hai)
defs = Definitions(
    assets=[*all_python_assets, my_dbt_assets],
    resources={
        "dbt": DbtCliResource(project_dir=str(dbt_project_dir)),
        "snowflake": SnowflakeResource(
            account="uoxcthr-vy59918",
            user="jayesh",
            password="My_pass", # <--- Apna asali password yahan dalo
            warehouse="CRYPTO_WH",
            database="CRYPTO_DB",
            schema="MAIN",
        ),
    },
)