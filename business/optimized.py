from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

from business.ingestion import (
    ingest_customers,
    ingest_orders,
)

from business.validation import (
    validate_customers,
    validate_orders,
)

from business.transformations import aggregate_customer_sales


def run_optimized(
    spark: SparkSession,
    customers_path: str,
    orders_path: str,
) -> dict:
    """
    Execute the optimized e-commerce ETL pipeline.

    Uses a broadcast join for customer enrichment to reduce
    shuffle overhead when the customer dimension is small.
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

    # Step 4: Prepare customer dimension for broadcast join
    customer_details = broadcast(
        valid_customers.select(
            "customer_id",
            "customer_name",
            "city",
        )
    )

    # Step 5: Enrich orders using broadcast join
    enriched_orders = valid_orders.join(
        customer_details,
        on="customer_id",
        how="inner",
    )

    # Step 6: Aggregate customer sales
    customer_summary = aggregate_customer_sales(
        enriched_orders
    )

    return {
        "valid_customers": valid_customers,
        "rejected_customers": rejected_customers,
        "valid_orders": valid_orders,
        "rejected_orders": rejected_orders,
        "enriched_orders": enriched_orders,
        "customer_summary": customer_summary,
    }
