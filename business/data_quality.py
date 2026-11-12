from business.customer_deduplication import separate_duplicate_customers
from business.date_validation import separate_invalid_dates
from business.order_sequence_validation import separate_out_of_order_orders
from business.validation import validate_customers, validate_orders


def validate_pipeline_data(customers, orders):
    # ---------------------------------
    # 1. Customer quality checks
    # ---------------------------------

    unique_customers, duplicate_customers = (
        separate_duplicate_customers(customers)
    )

    date_valid_customers, invalid_customer_dates = (
        separate_invalid_dates(
            unique_customers,
            "registration_date"
        )
    )

    valid_customers, invalid_customer_fields = (
        validate_customers(date_valid_customers)
    )

    rejected_customers = (
        duplicate_customers
        .unionByName(invalid_customer_dates)
        .unionByName(invalid_customer_fields)
    )

    # ---------------------------------
    # 2. Order quality checks
    # ---------------------------------

    date_valid_orders, invalid_order_dates = (
        separate_invalid_dates(
            orders,
            "order_date"
        )
    )

    valid_orders, invalid_order_fields = validate_orders(
        date_valid_orders,
        valid_customers
    )

    # ---------------------------------
    # 3. Out-of-order detection
    # ---------------------------------

    valid_orders, out_of_order_orders = (
        separate_out_of_order_orders(valid_orders)
    )

    rejected_orders = (
        invalid_order_dates
        .unionByName(invalid_order_fields)
        .unionByName(out_of_order_orders)
    )

    # ---------------------------------
    # 4. Return validated datasets
    # ---------------------------------

    return (
        valid_customers,
        rejected_customers,
        valid_orders,
        rejected_orders
    )