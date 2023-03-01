import json

import numpy as np
import pandas as pd
from dagster import (
    AssetIn,
    AssetKey,
    AssetsDefinition,
    DynamicOut,
    DynamicOutput,
    Field,
    FreshnessPolicy,
    LogicalVersion,
    Out,
    Output,
    SourceAsset,
    asset,
    graph,
    observable_source_asset,
    op,
    graph_asset
)

# @observable_source_asset(
#     name = "customers_csv",
#     description = "raw csv data with customers",
#     io_manager_key= "csv_io",
#     #required_resource_keys = {"fs"},
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

customers_csv = SourceAsset(key="customers_csv", io_manager_key="csv_io")

products_excel = SourceAsset(key="products_excel", io_manager_key="excel_io")

orders = SourceAsset(key=["RAW_DATA", "orders"], io_manager_key="warehouse_io")


@asset(
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=5),
    io_manager_key="pickle_io",
    required_resource_keys={"fs"},
)
def merged_sources(customers_csv, products_excel, orders: pd.DataFrame):
    """Merge orders, customers, and products"""
    orders_customers = pd.merge(
        orders, customers_csv, left_on="user_id", right_on="ID", how="left"
    )
    orders_customers_products = pd.merge(
        orders_customers, products_excel, left_on="sku", right_on="SKU", how="left"
    )
    orders_customers_products = orders_customers_products[
        ["user_id", "dt", "order_id", "quantity", "purchase_price", "sku"]
    ]
    return Output(
        orders_customers_products,
        metadata={
            "num_rows": len(orders_customers_products),
            "schema": str(orders_customers_products.columns),
        },
    )


# -- Begin Enrichment Asset Graph
@op(out=DynamicOut(), config_schema={"n_chunks": Field(int, default_value=10)})
def split_rows(context, merged_sources):
    """
    Split a data frame into chunks of rows:
       the larger the number of chunks the more
       parallel runs will be submitted
    """
    n_chunks = context.op_config["n_chunks"]
    chunks = np.array_split(merged_sources, min(n_chunks, len(merged_sources)))
    r = 0
    for c in chunks:
        r = r + 1
        yield DynamicOutput(c, mapping_key=str(r))


@op(required_resource_keys={"api"})
def process_chunk(context, chunk) -> pd.DataFrame:
    """
    Process rows in each chunk by calling the enrichment API
        within a chunk processing is sequential
        but it could be parallelized with regular python techniques
    """
    chunk["order_center"] = chunk.apply(
        lambda row: get_order_center(row["order_id"], context.resources.api), axis=1
    )
    return chunk


def get_order_center(order_id, api):
    """Given an order id call the enrichment API to get a order center"""
    response = api.get_order_details(order_id)
    response_data = json.loads(response.json())
    return response_data["order_center"]


@op(
    out = Out(
        io_manager_key="warehouse_io"
    )
)
def concat_chunk_list(chunks) -> pd.DataFrame:
    """Enriched data from an API"""
    return pd.concat(chunks)


@graph_asset(
    key_prefix="ANALYTICS"
)
def enriched_data(merged_sources) -> pd.DataFrame:
    """Full enrichment process"""
    chunks = split_rows(merged_sources)
    chunks_mapped = chunks.map(process_chunk)
    enriched_chunks = chunks_mapped.collect()
    return concat_chunk_list(enriched_chunks)



#enriched_data = AssetsDefinition.from_graph(enriched_data, key_prefix="ANALYTICS")

# --- End Enrichment Asset Graph


@asset(
    required_resource_keys={"warehouse"},
    non_argument_deps={AssetKey(["ANALYTICS", "enriched_data"])},
)
def processed_data(context):
    """Execute a stored snowflake procedure or SQL statement"""
    # query = "CALL process_data()"
    query = "SELECT * FROM ANALYTICS.enriched_data LIMIT 10"
    context.resources.warehouse.execute_query(query)
    return Output(None, metadata={"sql": query})
