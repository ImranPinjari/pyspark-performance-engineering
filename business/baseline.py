
from pyspark.sql import SparkSession

from business.ingestion import (
    ingest_customers,
    ingest_orders,
)

from business.data_quality import validate_pipeline_data

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
    2. Detect duplicate customer IDs.
    3. Validate customer registration dates.
    4. Validate customer records.
    5. Validate order dates and business rules.
    6. Enrich valid orders with customer details.
    7. Aggregate customer-level sales.
    """

    # Step 1: Ingest datasets
    customers = ingest_customers(spark, customers_path)
    orders = ingest_orders(spark, orders_path)

    # Step 2–5: Apply shared data quality checks
    (
        valid_customers,
        rejected_customers,
        valid_orders,
        rejected_orders,
    ) = validate_pipeline_data(customers, orders)

    # Step 6: Transform valid records
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
