
import pytest

from business.ingestion import (
    create_spark_session,
    CUSTOMER_SCHEMA,
    ORDER_SCHEMA,
)
from business.validation import validate_customers, validate_orders
from business.customer_deduplication import separate_duplicate_customers


@pytest.fixture(scope="module")
def spark():
    session = create_spark_session()
    yield session
    session.stop()


def read_csv(spark, path, schema):
    return (
        spark.read
        .option("header", "true")
        .schema(schema)
        .csv(str(path))
    )


def test_empty_datasets(spark, tmp_path):
    customers_file = tmp_path / "customers.csv"
    orders_file = tmp_path / "orders.csv"

    customers_file.write_text(
        "customer_id,customer_name,email,city,registration_date\n",
        encoding="utf-8",
    )
    orders_file.write_text(
        "order_id,customer_id,order_date,amount,status\n",
        encoding="utf-8",
    )

    customers = read_csv(spark, customers_file, CUSTOMER_SCHEMA)
    orders = read_csv(spark, orders_file, ORDER_SCHEMA)

    valid_customers, rejected_customers = validate_customers(customers)
    valid_orders, rejected_orders = validate_orders(
        orders, valid_customers
    )

    assert valid_customers.count() == 0
    assert rejected_customers.count() == 0
    assert valid_orders.count() == 0
    assert rejected_orders.count() == 0


def test_customer_required_field_validation(spark, tmp_path):
    customers_file = tmp_path / "customers.csv"
    customers_file.write_text(
        "customer_id,customer_name,email,city,registration_date\n"
        "1,Valid Customer,valid@example.com,Pune,2025-01-01\n"
        "2,   ,blank@example.com,Mumbai,2025-01-01\n"
        "3,Invalid Email,invalid-email,Pune,2025-01-01\n"
        "4,Missing Date,date@example.com,Pune,\n",
        encoding="utf-8",
    )

    customers = read_csv(spark, customers_file, CUSTOMER_SCHEMA)
    valid, rejected = validate_customers(customers)

    assert valid.count() == 1
    assert rejected.count() == 3

    reasons = {
        row["reject_reason"]: row["count"]
        for row in rejected.groupBy("reject_reason").count().collect()
    }

    assert reasons["EMPTY_CUSTOMER_NAME"] == 1
    assert reasons["INVALID_EMAIL"] == 1
    assert reasons["INVALID_REGISTRATION_DATE"] == 1


def test_order_duplicate_business_rules_and_references(spark, tmp_path):
    customers_file = tmp_path / "customers.csv"
    orders_file = tmp_path / "orders.csv"

    customers_file.write_text(
        "customer_id,customer_name,email,city,registration_date\n"
        "1,Customer One,one@example.com,Pune,2025-01-01\n",
        encoding="utf-8",
    )

    orders_file.write_text(
        "order_id,customer_id,order_date,amount,status\n"
        "101,1,2025-01-01,100.0,COMPLETED\n"
        "102,1,2025-01-02,-10.0,PENDING\n"
        "103,1,2025-01-03,50.0,UNKNOWN\n"
        "104,999,2025-01-04,75.0,PENDING\n"
        ",1,2025-01-05,25.0,PENDING\n"
        "106,,2025-01-06,25.0,PENDING\n"
        "107,1,,25.0,PENDING\n"
        "108,1,2025-01-08,,PENDING\n"
        "109,1,2025-01-09,25.0,\n"
        "200,1,2025-01-10,50.0,COMPLETED\n"
        "200,1,2025-01-11,60.0,COMPLETED\n",
        encoding="utf-8",
    )

    customers = read_csv(spark, customers_file, CUSTOMER_SCHEMA)
    orders = read_csv(spark, orders_file, ORDER_SCHEMA)

    valid_customers, _ = validate_customers(customers)
    valid, rejected = validate_orders(orders, valid_customers)

    assert valid.count() == 1
    assert rejected.count() == 10

    reasons = {
        row["reject_reason"]: row["count"]
        for row in rejected.groupBy("reject_reason").count().collect()
    }

    assert reasons["DUPLICATE_ORDER_ID"] == 2
    assert reasons["INVALID_AMOUNT"] == 1
    assert reasons["INVALID_STATUS"] == 1
    assert reasons["INVALID_CUSTOMER_REFERENCE"] == 1
    assert reasons["NULL_ORDER_ID"] == 1
    assert reasons["NULL_CUSTOMER_ID"] == 1
    assert reasons["NULL_ORDER_DATE"] == 1
    assert reasons["NULL_AMOUNT"] == 1
    assert reasons["NULL_STATUS"] == 1


def test_duplicate_customer_ids_are_detected(spark, tmp_path):
    customers_file = tmp_path / "duplicate_customers.csv"

    customers_file.write_text(
        "customer_id,customer_name,email,city,registration_date\n"
        "1,Customer One,one@example.com,Pune,2025-01-01\n"
        "1,Customer One Duplicate,duplicate@example.com,Mumbai,2025-01-02\n"
        "2,Customer Two,two@example.com,Nashik,2025-01-03\n",
        encoding="utf-8",
    )

    customers = read_csv(spark, customers_file, CUSTOMER_SCHEMA)

    unique, rejected = separate_duplicate_customers(customers)

    assert unique.count() == 1
    assert rejected.count() == 2

    reasons = {
        row["reject_reason"]: row["count"]
        for row in rejected.groupBy("reject_reason").count().collect()
    }

    assert reasons["DUPLICATE_CUSTOMER_ID"] == 2
    assert unique.select("customer_id").first()[0] == 2
