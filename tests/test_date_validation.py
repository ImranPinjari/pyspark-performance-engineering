import pytest
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from business.date_validation import separate_invalid_dates
from business.ingestion import create_spark_session


@pytest.fixture(scope="module")
def spark():
    session = create_spark_session()
    yield session
    session.stop()


@pytest.mark.parametrize(
    "date_column",
    ["registration_date", "order_date"],
)
def test_strict_date_validation(spark, tmp_path, date_column):
    file_path = tmp_path / "dates.csv"

    file_path.write_text(
        f"record_id,{date_column}\n"
        "1,2025-01-15\n"
        "2,2024-02-29\n"
        "3,15-01-2025\n"
        "4,2025-02-30\n"
        "5,\n",
        encoding="utf-8",
    )

    schema = StructType([
        StructField("record_id", IntegerType(), True),
        StructField(date_column, StringType(), True),
    ])

    dataframe = (
        spark.read
        .option("header", "true")
        .schema(schema)
        .csv(str(file_path))
    )

    valid, rejected = separate_invalid_dates(dataframe, date_column)

    assert valid.count() == 2
    assert rejected.count() == 3

    reasons = {
        row["reject_reason"]: row["count"]
        for row in rejected.groupBy("reject_reason").count().collect()
    }

    assert reasons["INVALID_DATE_FORMAT"] == 3
