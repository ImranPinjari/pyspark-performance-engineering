import pytest

from business.ingestion import (
    create_spark_session,
    ingest_customers,
    ingest_orders,
)
from business.validation import validate_customers, validate_orders
from business.transformations import transform_data


@pytest.fixture(scope="module")
def pipeline_data():
    spark = create_spark_session()
    try:
        customers = ingest_customers(spark, "data/input/customers.csv")
        orders = ingest_orders(spark, "data/input/orders.csv")

        valid_customers, rejected_customers = validate_customers(customers)
        valid_orders, rejected_orders = validate_orders(
            orders, valid_customers
        )
        enriched, summary = transform_data(valid_orders, valid_customers)

        yield {
            "valid_customers": valid_customers,
            "rejected_customers": rejected_customers,
            "valid_orders": valid_orders,
            "rejected_orders": rejected_orders,
            "enriched": enriched,
            "summary": summary,
        }
    finally:
        spark.stop()


def test_customer_validation(pipeline_data):
    assert pipeline_data["valid_customers"].count() == 99
    assert pipeline_data["rejected_customers"].count() == 1


def test_order_validation_and_quarantine(pipeline_data):
    assert pipeline_data["valid_orders"].count() == 484
    assert pipeline_data["rejected_orders"].count() == 17

    reasons = {
        row["reject_reason"]: row["count"]
        for row in pipeline_data["rejected_orders"]
        .groupBy("reject_reason").count().collect()
    }

    assert reasons["DUPLICATE_ORDER_ID"] == 2
    assert reasons["INVALID_AMOUNT"] == 1
    assert reasons["NULL_ORDER_DATE"] == 1
    assert reasons["INVALID_CUSTOMER_REFERENCE"] == 13


def test_enrichment_and_aggregation(pipeline_data):
    assert pipeline_data["enriched"].count() == 484
    assert pipeline_data["summary"].count() == 98

    assert (
        pipeline_data["summary"]
        .filter("total_orders <= 0")
        .count()
        == 0
    )
