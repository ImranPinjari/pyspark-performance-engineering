from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    count,
    sum as spark_sum,
    when,
)


def join_orders_with_customers(
    orders: DataFrame,
    customers: DataFrame,
) -> DataFrame:
    """Enrich orders with customer details."""

    customer_details = customers.select(
        "customer_id",
        "customer_name",
        "city",
    )

    return orders.join(
        customer_details,
        on="customer_id",
        how="inner",
    )


def aggregate_customer_sales(
    enriched_orders: DataFrame,
) -> DataFrame:
    """Calculate customer-level sales and order-status metrics."""

    return (
        enriched_orders
        .groupBy("customer_id", "customer_name", "city")
        .agg(
            count("order_id").alias("total_orders"),
            spark_sum("amount").alias("total_amount"),
            avg("amount").alias("average_order_amount"),
            spark_sum(
                when(enriched_orders.status == "COMPLETED", 1).otherwise(0)
            ).alias("completed_orders"),
            spark_sum(
                when(enriched_orders.status == "PENDING", 1).otherwise(0)
            ).alias("pending_orders"),
            spark_sum(
                when(enriched_orders.status == "CANCELLED", 1).otherwise(0)
            ).alias("cancelled_orders"),
        )
    )


def transform_data(
    orders: DataFrame,
    customers: DataFrame,
):
    """Run enrichment and customer-level aggregation."""

    enriched_orders = join_orders_with_customers(
        orders,
        customers,
    )

    customer_summary = aggregate_customer_sales(enriched_orders)

    return enriched_orders, customer_summary
