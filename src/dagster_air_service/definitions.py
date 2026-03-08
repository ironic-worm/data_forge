import dagster as dg
from dagster_dbt import DbtCliResource

from dagster_air_service.defs.assets.assets import (
    LoadResource,
    air_service_dbt_assets,
    load_data_air_service,
)
DBT_PROJECT_DIR = "/Users/yakov_shepelev/Учеба/Институт/Диплом/dagster_air_service/src/analytics"
DBT_PROFILES_DIR = "/Users/yakov_shepelev/.dbt"

defs = dg.Definitions(
    assets=[load_data_air_service, air_service_dbt_assets],
    resources={
        "demo_db_loader": LoadResource(),
        "dbt": DbtCliResource(
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROFILES_DIR,
        ),
    },
)