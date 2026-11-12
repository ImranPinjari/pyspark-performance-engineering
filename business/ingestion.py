
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)


CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("registration_date", StringType(), True),
])


ORDER_SCHEMA = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True),
])


def create_spark_session() -> SparkSession:
    """Create a local Spark session."""

    return (
        SparkSession.builder
        .appName("EcommerceETLPipeline")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def ingest_customers(
    spark: SparkSession,
    file_path: str,
) -> DataFrame:
    """Read customer CSV using an explicit schema."""

    if not Path(file_path).is_file():
        raise FileNotFoundError(f"Customer file not found: {file_path}")

    return (
        spark.read
        .option("header", True)
        .schema(CUSTOMER_SCHEMA)
        .csv(file_path)
        .withColumn("source_file", 
                    __import__("pyspark").sql.functions.lit(
                        Path(file_path).name
                    ))
    )


def ingest_orders(
    spark: SparkSession,
    file_path: str,
) -> DataFrame:
    """Read order CSV using an explicit schema."""

    if not Path(file_path).is_file():
        raise FileNotFoundError(f"Order file not found: {file_path}")

    from pyspark.sql.functions import lit

    return (
        spark.read
        .option("header", True)
        .schema(ORDER_SCHEMA)
        .csv(file_path)
        .withColumn("source_file", lit(Path(file_path).name))
    )
