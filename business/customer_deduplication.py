from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, lit


def separate_duplicate_customers(customers: DataFrame):
    """
    Reject all records sharing a duplicate, non-null customer ID.
    Null customer IDs remain for the existing validation function.
    """

    duplicate_ids = (
        customers
        .filter(col("customer_id").isNotNull())
        .groupBy("customer_id")
        .agg(count("*").alias("record_count"))
        .filter(col("record_count") > 1)
        .select("customer_id")
    )

    rejected = (
        customers
        .join(duplicate_ids, on="customer_id", how="left_semi")
        .withColumn("reject_reason", lit("DUPLICATE_CUSTOMER_ID"))
    )

    unique = customers.join(
        duplicate_ids,
        on="customer_id",
        how="left_anti"
    )

    return unique, rejected
