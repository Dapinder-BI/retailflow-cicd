from pyspark import pipelines as dp
from pyspark.sql import functions as F


def clean_customers(df):
    return (
        df.withColumn(
            "customer_name",
            F.initcap(F.trim(F.col("customer_name")))
        )
        .withColumn(
            "city",
            F.initcap(F.trim(F.col("city")))
        )
        .dropDuplicates(["customer_id"])
    )


@dp.materialized_view(
    name="customers_clean",
    comment="Cleaned customer records for the Silver layer"
)
@dp.expect_or_drop(
    "valid_customer_id",
    "customer_id IS NOT NULL"
)
def customers_clean():
    source_table = spark.conf.get("source_customers_table")

    df = spark.read.table(source_table)

    return clean_customers(df)