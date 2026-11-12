from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from business.order_sequence_validation import separate_out_of_order_orders
from business.ingestion import create_spark_session


def test_out_of_order_orders_are_quarantined(tmp_path):
    spark = create_spark_session()

    try:
        file_path = tmp_path / "order_sequence.csv"

        file_path.write_text(
            "order_id,customer_id,order_date\n"
            "1,10,2025-01-03\n"
            "2,10,2025-01-02\n"
            "3,10,2025-01-04\n"
            "4,20,2025-01-01\n",
            encoding="utf-8",
        )

        schema = StructType([
            StructField("order_id", IntegerType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("order_date", StringType(), True),
        ])

        orders = (
            spark.read
            .option("header", "true")
            .schema(schema)
            .csv(str(file_path))
        )

        valid, rejected = separate_out_of_order_orders(orders)

        assert valid.count() == 3
        assert rejected.count() == 1
        assert rejected.select("order_id").first()[0] == 2
        assert rejected.select("reject_reason").first()[0] == "OUT_OF_ORDER_DATE"

    finally:
        spark.stop()
