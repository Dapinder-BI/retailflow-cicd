import pytest
from pyspark.pipelines.testing import TestPipeline, test_spark

test_pipeline = TestPipeline.active()
print(test_pipeline)


@pytest.fixture(autouse=True)
def set_pipeline_configs(test_spark):
    """Set pipeline configuration values so decorators and functions can resolve them."""
    test_spark.conf.set("source_customers_table", "retailflow_dev.bronze.dev_customers")
    test_spark.conf.set("source_orders_table", "retailflow_dev.bronze.dev_orders")
    test_spark.conf.set("target_catalog", "retailflow_dev")


def test_customers_clean_normalizes_and_dedupes(test_spark):
    """customers_clean trims/initcaps name and city, drops null IDs, and removes duplicate customer_ids."""
    test_spark.sql("""
        CREATE OR REPLACE TABLE retailflow_dev.bronze.dev_customers (
            customer_id LONG, customer_name STRING, city STRING,
            email STRING, signup_date DATE
        )
    """)
    test_spark.sql("""
        INSERT INTO retailflow_dev.bronze.dev_customers VALUES
        (1, '  john doe  ', '  toronto  ', 'john@example.com', '2026-01-01'),
        (2, 'JANE SMITH', 'NEW YORK', 'jane@example.com', '2026-01-02'),
        (1, 'john doe', 'toronto', 'john@example.com', '2026-01-01'),
        (NULL, 'bob null', 'boston', 'bob@example.com', '2026-01-03')
    """)

    status = test_pipeline.run(
        test_spark, set(["retailflow_dev.silver.customers_clean"])
    )
    assert status.is_success, f"Pipeline run failed: {status.error_message}"

    result = test_spark.sql(
        "SELECT * FROM retailflow_dev.silver.customers_clean ORDER BY customer_id"
    )
    rows = result.collect()

    # Null customer_id dropped by expectation; duplicate customer_id removed
    assert len(rows) == 2

    row1 = rows[0]
    assert row1["customer_id"] == 1
    assert row1["customer_name"] == "John Doe"
    assert row1["city"] == "Toronto"

    row2 = rows[1]
    assert row2["customer_id"] == 2
    assert row2["customer_name"] == "Jane Smith"
    assert row2["city"] == "New York"


def test_orders_clean_normalizes_and_dedupes(test_spark):
    """orders_clean trims/initcaps order_status, drops null order_ids, and removes duplicate order_ids."""
    test_spark.sql("""
        CREATE OR REPLACE TABLE retailflow_dev.bronze.dev_orders (
            order_id LONG, customer_id LONG, order_date DATE,
            order_amount DOUBLE, order_status STRING
        )
    """)
    test_spark.sql("""
        INSERT INTO retailflow_dev.bronze.dev_orders VALUES
        (101, 1, '2026-07-01', 100.50, '  pending  '),
        (102, 2, '2026-07-02', 200.00, 'COMPLETED'),
        (101, 1, '2026-07-01', 100.50, 'pending'),
        (NULL, 3, '2026-07-03', 50.00, 'shipped')
    """)

    status = test_pipeline.run(
        test_spark, set(["retailflow_dev.silver.orders_clean"])
    )
    assert status.is_success, f"Pipeline run failed: {status.error_message}"

    result = test_spark.sql(
        "SELECT * FROM retailflow_dev.silver.orders_clean ORDER BY order_id"
    )
    rows = result.collect()

    # Null order_id dropped by expectation; duplicate order_id removed
    assert len(rows) == 2

    row1 = rows[0]
    assert row1["order_id"] == 101
    assert row1["order_status"] == "Pending"

    row2 = rows[1]
    assert row2["order_id"] == 102
    assert row2["order_status"] == "Completed"


def test_customer_sales_summary_aggregates_correctly(test_spark):
    """customer_sales_summary left-joins customers to orders and aggregates total_orders and total_spending."""
    test_spark.sql("""
        CREATE OR REPLACE TABLE retailflow_dev.bronze.dev_customers (
            customer_id LONG, customer_name STRING, city STRING,
            email STRING, signup_date DATE
        )
    """)
    test_spark.sql("""
        INSERT INTO retailflow_dev.bronze.dev_customers VALUES
        (1, 'Alice', 'Toronto', 'alice@example.com', '2026-01-01'),
        (2, 'Bob', 'Ottawa', 'bob@example.com', '2026-01-02')
    """)

    test_spark.sql("""
        CREATE OR REPLACE TABLE retailflow_dev.bronze.dev_orders (
            order_id LONG, customer_id LONG, order_date DATE,
            order_amount DOUBLE, order_status STRING
        )
    """)
    test_spark.sql("""
        INSERT INTO retailflow_dev.bronze.dev_orders VALUES
        (101, 1, '2026-07-01', 100.00, 'completed'),
        (102, 1, '2026-07-02', 200.00, 'pending'),
        (103, 2, '2026-07-03', 50.00, 'completed'),
        (104, 999, '2026-07-04', 999.00, 'completed')
    """)

    # Run full pipeline — gold depends on both silver tables
    status = test_pipeline.run(test_spark)
    assert status.is_success, f"Pipeline run failed: {status.error_message}"

    result = test_spark.sql(
        "SELECT * FROM retailflow_dev.gold.customer_sales_summary ORDER BY customer_id"
    )
    rows = result.collect()

    # Two customers; order 104 (customer 999) has no matching customer
    assert len(rows) == 2

    row1 = rows[0]
    assert row1["customer_id"] == 1
    assert row1["customer_name"] == "Alice"
    assert row1["total_orders"] == 2
    assert row1["total_spending"] == 300.00

    row2 = rows[1]
    assert row2["customer_id"] == 2
    assert row2["customer_name"] == "Bob"
    assert row2["total_orders"] == 1
    assert row2["total_spending"] == 50.00


def test_customer_sales_summary_customer_with_no_orders(test_spark):
    """A customer with no matching orders should get total_orders=0 and total_spending=0.0."""
    test_spark.sql("""
        CREATE OR REPLACE TABLE retailflow_dev.bronze.dev_customers (
            customer_id LONG, customer_name STRING, city STRING,
            email STRING, signup_date DATE
        )
    """)
    test_spark.sql("""
        INSERT INTO retailflow_dev.bronze.dev_customers VALUES
        (1, 'Alice', 'Toronto', 'alice@example.com', '2026-01-01'),
        (2, 'Bob', 'Ottawa', 'bob@example.com', '2026-01-02')
    """)

    test_spark.sql("""
        CREATE OR REPLACE TABLE retailflow_dev.bronze.dev_orders (
            order_id LONG, customer_id LONG, order_date DATE,
            order_amount DOUBLE, order_status STRING
        )
    """)
    test_spark.sql("""
        INSERT INTO retailflow_dev.bronze.dev_orders VALUES
        (101, 1, '2026-07-01', 100.00, 'completed')
    """)

    status = test_pipeline.run(test_spark)
    assert status.is_success, f"Pipeline run failed: {status.error_message}"

    result = test_spark.sql(
        "SELECT * FROM retailflow_dev.gold.customer_sales_summary ORDER BY customer_id"
    )
    rows = result.collect()

    assert len(rows) == 2

    bob = [r for r in rows if r["customer_id"] == 2][0]
    assert bob["total_orders"] == 0
    assert bob["total_spending"] == 0.0
