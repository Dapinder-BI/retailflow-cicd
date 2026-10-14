from pyspark import pipelines as dp
from pyspark.sql import functions as F


def calculate_customer_sales(customers_df, orders_df):

    return (
        customers_df.alias("c")
        .join(
            orders_df.alias("o"),
            on="customer_id",
            how="left"
        )
        .groupBy(
            "customer_id",
            "customer_name"
        )
        .agg(
            F.count("order_id").alias("total_orders"),
            F.coalesce(
                F.sum("order_amount"),
                F.lit(0.0)
            ).alias("total_spending")
        )
    )


@dp.materialized_view(
    name=f"{spark.conf.get('target_catalog')}.gold.customer_sales_summary",
    comment="Customer-level sales summary for business reporting"
)
@dp.expect_or_fail(
    "valid_customer_id",
    "customer_id IS NOT NULL"
)
@dp.expect_or_fail(
    "non_negative_spending",
    "total_spending >= 0"
)
def customer_sales_summary():

    customers_df = spark.read.table("customers_clean")
    orders_df = spark.read.table("orders_clean")

    return calculate_customer_sales(customers_df, orders_df)