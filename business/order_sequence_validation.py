from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, to_date, lit


def separate_out_of_order_orders(orders: DataFrame):
    """
    Detect orders whose date is earlier than the preceding
    order for the same customer, ordered by order_id.
    """

    window_spec = (
        Window
        .partitionBy("customer_id")
        .orderBy("order_id")
    )

    checked = orders.withColumn(
        "_previous_order_date",
        lag(to_date(col("order_date"))).over(window_spec)
    )

    out_of_order = (
        checked
        .filter(
            col("_previous_order_date").isNotNull()
            & (
                to_date(col("order_date"))
                < col("_previous_order_date")
            )
        )
        .drop("_previous_order_date")
        .withColumn(
            "reject_reason",
            lit("OUT_OF_ORDER_DATE")
        )
    )

    valid = (
        checked
        .join(
            out_of_order.select("order_id"),
            on="order_id",
            how="left_anti"
        )
        .drop("_previous_order_date")
    )

    return valid, out_of_order
