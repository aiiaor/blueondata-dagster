from dagster import Definitions
from ecommerce.resources import postgres_resource
from ecommerce.assets import orders_file_raw, orders_file_cleaned, check_orders_file_cleaned, orders_table

defs = Definitions(
    assets=[orders_file_raw, orders_file_cleaned, orders_table],
    asset_checks=[check_orders_file_cleaned],
    resources={
        "postgres": postgres_resource
    }
)