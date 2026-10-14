from pyspark import pipelines as dp
from pyspark.sql import functions as F


def clean_orders(df):
    return (
        df.withColumn(
            "order_status",
            F.initcap(F.trim(F.col("order_status")))
        )
        .dropDuplicates(["order_id"])
    )


@dp.materialized_view(
    name="orders_clean",
    comment="Cleaned order records for the Silver layer"
)
@dp.expect_or_drop(
    "valid_order_id",
    "order_id IS NOT NULL"
)
def orders_clean():

    source_table = spark.conf.get("source_orders_table")

    df = spark.read.table(source_table)

    return clean_orders(df)