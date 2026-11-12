
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, when


def validate_customers(customers: DataFrame):
    """Separate valid customers from records that fail required-field checks."""

    invalid_condition = (
        col("customer_id").isNull()
        | (col("customer_id") <= 0)
        | col("customer_name").isNull()
        | (trim(col("customer_name")) == "")
        | col("email").isNull()
        | (trim(col("email")) == "")
        | (~col("email").contains("@"))
        | col("registration_date").isNull()
    )

    valid = customers.filter(~invalid_condition)

    rejected = (
        customers.filter(invalid_condition)
        .withColumn(
            "reject_reason",
            when(col("customer_id").isNull(), "NULL_CUSTOMER_ID")
            .when(col("customer_id") <= 0, "INVALID_CUSTOMER_ID")
            .when(col("customer_name").isNull(), "NULL_CUSTOMER_NAME")
            .when(trim(col("customer_name")) == "", "EMPTY_CUSTOMER_NAME")
            .when(col("email").isNull(), "NULL_EMAIL")
            .when(trim(col("email")) == "", "EMPTY_EMAIL")
            .when(~col("email").contains("@"), "INVALID_EMAIL")
            .otherwise("INVALID_REGISTRATION_DATE")
        )
    )

    return valid, rejected


def validate_orders(orders: DataFrame, customers: DataFrame):
    """
    Validate orders and quarantine every record belonging to a duplicate
    order ID. Also check required fields and customer references.
    """

    # 1. Find duplicate, non-null order IDs.
    duplicate_ids = (
        orders.groupBy("order_id")
        .count()
        .filter(
            (col("count") > 1) & col("order_id").isNotNull()
        )
        .select("order_id")
    )

    # 2. Quarantine ALL rows with duplicate order IDs.
    duplicate_records = (
        orders.join(duplicate_ids, on="order_id", how="left_semi")
        .withColumn("reject_reason", when(
            col("order_id").isNotNull(), "DUPLICATE_ORDER_ID"
        ))
    )

    # 3. Continue validation only with non-duplicate records.
    non_duplicate_orders = orders.join(
        duplicate_ids,
        on="order_id",
        how="left_anti"
    )

    # 4. Validate required fields and business rules.
    invalid_condition = (
        col("order_id").isNull()
        | (col("order_id") <= 0)
        | col("customer_id").isNull()
        | col("order_date").isNull()
        | col("amount").isNull()
        | (col("amount") <= 0)
        | col("status").isNull()
        | (~col("status").isin("COMPLETED", "PENDING", "CANCELLED"))
    )

    invalid_records = (
        non_duplicate_orders.filter(invalid_condition)
        .withColumn(
            "reject_reason",
            when(col("order_id").isNull(), "NULL_ORDER_ID")
            .when(col("order_id") <= 0, "INVALID_ORDER_ID")
            .when(col("customer_id").isNull(), "NULL_CUSTOMER_ID")
            .when(col("order_date").isNull(), "NULL_ORDER_DATE")
            .when(col("amount").isNull(), "NULL_AMOUNT")
            .when(col("amount") <= 0, "INVALID_AMOUNT")
            .when(col("status").isNull(), "NULL_STATUS")
            .otherwise("INVALID_STATUS")
        )
    )

    basic_valid = non_duplicate_orders.filter(~invalid_condition)

    # 5. Check customer foreign-key references.
    customer_keys = customers.select("customer_id").dropDuplicates()

    valid_orders = basic_valid.join(
        customer_keys,
        on="customer_id",
        how="left_semi"
    )

    invalid_references = (
        basic_valid.join(
            customer_keys,
            on="customer_id",
            how="left_anti"
        )
        .withColumn("reject_reason", when(
            col("customer_id").isNotNull(),
            "INVALID_CUSTOMER_REFERENCE"
        ))
    )

    # 6. Combine all rejected records.
    rejected_orders = (
        duplicate_records
        .unionByName(invalid_records)
        .unionByName(invalid_references)
    )

    return valid_orders, rejected_orders
