from dagster import Definitions, load_assets_from_modules, fs_io_manager, ResourceDefinition, with_resources, repository

from dagster_aws.s3 import s3_resource, s3_pickle_io_manager

from dagster_etl_enrich_demo.assets import my_assets
from dagster_etl_enrich_demo.utils.load_aws_secrets import load_aws_secret

from dagster_etl_enrich_demo.resources.files import local_fs, s3_fs, csv_io, excel_io
from dagster_etl_enrich_demo.resources.api import data_api

from dagster_snowflake_pandas import snowflake_pandas_io_manager
from dagster_snowflake import snowflake_resource


import os

#  This function will load environment variables from 
#  AWS secrets if they are not already set
#  if the secret is KEY=VALUE 
#  the result is an environment variable KEY set to VALUE
load_aws_secret("aws-snowflake-password")

# if the secret is just a VALUE 
# the result is an environment variable SNOWFLAKE_USER set to VALUE
load_aws_secret("SNOWFLAKE_USER")



def get_env():
    if os.getenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT", "") == "1":
        return "BRANCH"
    if os.getenv("DAGSTER_CLOUD_DEPLOYMENT_NAME", "") == "data-eng-prod":
        return "PROD"
    return "LOCAL"


#customers_csv = csv_io.configured({"filename": "customers.csv"})
#products_excel = excel_io.configured({"filename": "products.xlsx"})

s3_bucket_config = {"s3_bucket": "hooli-demo"}
s3_region_config = {"region_name": "us-west-1"}

snowflake_config ={
    'account': {'env': 'SNOWFLAKE_ACCOUNT'},
    'user':  {'env': 'SNOWFLAKE_USER'},
    'password': {'env': 'SNOWFLAKE_PASSWORD'},
    'warehouse': 'TINY_WAREHOUSE'

}

resources = {
    "LOCAL": {
        "fs": local_fs.configured({"base_dir": "."}),
        "csv_io": csv_io, #customers_csv,
        "excel_io": excel_io, #products_excel, 
        "pickle_io": fs_io_manager,
        "s3": ResourceDefinition.none_resource(), 
        "warehouse": snowflake_resource.configured({
            **snowflake_config, 
            "database": "demo_db2"
        }), 
        "warehouse_io": snowflake_pandas_io_manager.configured({
            **snowflake_config, 
            "database": "demo_db2"
        }), 
        "api": data_api
    }, 

    "PROD": {
        "fs": s3_fs.configured({**s3_region_config, **s3_bucket_config}),
        "csv_io": csv_io, #customers_csv,
        "excel_io": excel_io, #products_excel, 
        "s3": s3_resource.configured(s3_region_config), 
        "pickle_io": s3_pickle_io_manager.configured(s3_bucket_config), 
        "warehouse": snowflake_resource.configured({
            **snowflake_config, 
            "database": "demo_db2"
        }), 
        "warehouse_io": snowflake_pandas_io_manager.configured({
            **snowflake_config, 
            "database": "demo_db2"
        }), 
        "api": data_api
    }    
}


defs = Definitions(
    assets = load_assets_from_modules([my_assets]),
    schedules = [],
    sensors = [],
    jobs = [],
    resources = resources[get_env()]
)

# @repository 
# def my_repo():
#     return [
#         with_resources(
#             definitions = load_assets_from_modules([my_assets]), 
#             resource_defs = resources[get_env()]
#         )
#     ]