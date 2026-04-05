from dagster import Definitions, load_assets_from_modules, AssetKey
from dagster_dbt import dbt_assets, DbtCliResource, DagsterDbtTranslator
from pathlib import Path
import os
import assets

# 1. dbt Project Path
dbt_project_dir = Path(__file__).joinpath("..", "analytics").resolve()

# 2. Magic Translator: Jo dbt aur Python ke beech arrow banayega
class MyCustomTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        # Agar dbt source ka naam 'raw_prices' hai, toh usse Python asset se connect karo
        if dbt_resource_props.get("name") == "raw_prices":
            return AssetKey(["clean_crypto_data"])
        return super().get_asset_key(dbt_resource_props)

# 3. Load dbt Assets with the Translator
@dbt_assets(
    manifest=dbt_project_dir.joinpath("target", "manifest.json"),
    dagster_dbt_translator=MyCustomTranslator()
)
def my_dbt_assets(context, dbt: DbtCliResource):
    # .stream() use karo, ye naye Dagster ka tareeka hai
    yield from dbt.cli(["build"], context=context).stream()

# 4. Load Python Assets
all_python_assets = load_assets_from_modules([assets])

# 5. Final Definitions
defs = Definitions(
    assets=[*all_python_assets, my_dbt_assets],
    resources={
        "dbt": DbtCliResource(project_dir=str(dbt_project_dir)),
    },
)