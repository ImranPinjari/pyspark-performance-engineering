from pyspark.sql import SparkSession

from business.ingestion import (
    ingest_customers,
    ingest_orders,
)

from business.validation import (
    validate_customers,
    validate_orders,
)

from business.transformations import transform_data


def run_baseline(
    spark: SparkSession,
    customers_path: str,
    orders_path: str,
) -> dict:
    """
    Execute the standard e-commerce ETL pipeline.

    Steps:
    1. Ingest customer and order datasets.
    2. Validate customer records.
    3. Validate order records and customer references.
    4. Enrich valid orders with customer details.
    5. Aggregate customer-level sales.
    """

    # Step 1: Ingest datasets
    customers = ingest_customers(spark, customers_path)
    orders = ingest_orders(spark, orders_path)

    # Step 2: Validate customers
    valid_customers, rejected_customers = validate_customers(
        customers
    )

    # Step 3: Validate orders
    valid_orders, rejected_orders = validate_orders(
        orders,
        valid_customers,
    )

    # Step 4: Transform valid records
    enriched_orders, customer_summary = transform_data(
        valid_orders,
        valid_customers,
    )

    return {
        "valid_customers": valid_customers,
        "rejected_customers": rejected_customers,
        "valid_orders": valid_orders,
        "rejected_orders": rejected_orders,
        "enriched_orders": enriched_orders,
        "customer_summary": customer_summary,
    }
