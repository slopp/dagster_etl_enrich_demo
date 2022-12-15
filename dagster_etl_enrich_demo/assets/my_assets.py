from dagster import asset, observable_source_asset, LogicalVersion, FreshnessPolicy, Field, Output, Field, SourceAsset, AssetKey, AssetIn, AssetsDefinition, graph, DynamicOut, op, Out, DynamicOutput
import pandas as pd
import numpy as np
import json 
# @observable_source_asset(
#     name = "customers_csv",
#     description = "raw csv data with customers",
#     io_manager_key= "csv_io",
#     #required_resource_keys = {"fs"}, 
#     #config_schema = {"filename": Field(str, default_value="customers.csv")}   
# )
# def observe_csv(context):
#     filename = "customers.csv"
#     mtime = context.resources.fs.get_mtime(filename)
#     return LogicalVersion(mtime)

# @observable_source_asset(
#     name = "products_excel",
#     description = "raw excel data with products",
#     io_manager_key= "excel_io",
#     #required_resource_keys = {"fs"}, 
#     #config_schema = {"filename": Field(str, default_value="products.xlsx")}   
# )
# def observe_excel(context):
#     filename = "products.xlsx"
#     mtime = context.resources.fs.get_mtime(filename)
#     return LogicalVersion(mtime)

# @observable_source_asset(
#     name = "orders", 
#     description = "sql events table with orders",
#     io_manager_key = "warehouse_io",
#     #required_resource_keys = {"warehouse"},
#     key_prefix="RAW_DATA"
# )
# def observe_sql(context):
#     max_row_id = context.resources.warehouse.execute_query(
#         "SELECT MAX(DT) FROM RAW_DATA.orders"
#     )
#     return LogicalVersion(max_row_id)

customers_csv = SourceAsset(
    key = ["customers_csv"], 
    io_manager_key="csv_io"
)

products_excel = SourceAsset(
    key = AssetKey(["products_excel"]), 
    io_manager_key="excel_io"
)

orders = SourceAsset(
    key = ["RAW_DATA", "orders"],
    io_manager_key="warehouse_io"
)


@asset(
    freshness_policy = FreshnessPolicy(maximum_lag_minutes=5), 
    io_manager_key = "pickle_io", 
    required_resource_keys = {"fs"},
)
def merged_sources(customers_csv, products_excel, orders: pd.DataFrame):
    """ Merge orders, customers, and products """
    orders_customers = pd.merge(orders, customers_csv, left_on="user_id", right_on = "ID", how = "left", suffixes=[None, "_customer"])
    orders_customers_products = pd.merge(orders_customers, products_excel, left_on="sku", right_on = "SKU", how = "left", suffixes=[None, "_product"])
    orders_customers_products = orders_customers_products[['user_id', 'dt', 'order_id', 'quantity', 'purchase_price', 'sku']]
    return Output(orders_customers_products, metadata={"num_rows": len(orders_customers_products), "schema": str(orders_customers_products.columns)})

@op(out=DynamicOut())
def split_rows(merged_sources):
    chunks = np.array_split(merged_sources, min(10, len(merged_sources)))
    r = 0
    for c in chunks:
        r = r +1
        yield DynamicOutput(c, mapping_key = str(r))
@op(
    required_resource_keys = {"api"}
)
def process_chunk(context, chunk) -> pd.DataFrame:
    chunk['order_center'] = chunk.apply(lambda row: get_order_center(row["order_id"], context.resources.api), axis = 1)
    return chunk 

def get_order_center(order_id, api):
    response = api.get_order_details(order_id)
    response_data = json.loads(response.json())
    return response_data['order_center']

@op(out=Out(asset_key=AssetKey(["ANALYTICS", "enriched_data"]),  io_manager_key = "warehouse_io"))
def concat_chunk_list(chunks) -> pd.DataFrame:
    """ Enriched order data from an API"""
    return pd.concat(chunks)


@graph
def enriched_data(merged_sources) -> pd.DataFrame:
    """ Enriched order data from an API"""
    chunks = split_rows(merged_sources)
    chunks_mapped = chunks.map(process_chunk)
    enriched_chunks = chunks_mapped.collect()
    return concat_chunk_list(enriched_chunks)

enriched_data = AssetsDefinition.from_graph(enriched_data, 
    key_prefix="ANALYTICS"
)

@asset(
     required_resource_keys = {"warehouse"},
     non_argument_deps={AssetKey(["ANALYTICS", "enriched_data"])}
)
def processed_data(context):
    """ Execute a stored snowflake procedure or SQL statement"""
    #query = "CALL process_data()"
    query = "SELECT * FROM ANALYTICS.enriched_data LIMIT 10"
    context.resources.warehouse.execute_query(query)
    return Output(None, metadata = {"sql": query})