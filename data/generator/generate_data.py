
import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path


def generate_data(
    output_dir: str = "data/input",
    customer_count: int = 1000,
    order_count: int = 5000,
    seed: int = 42,
):
    """Generate reproducible synthetic customers and orders."""

    random.seed(seed)

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    customers_file = output_path / "customers.csv"
    orders_file = output_path / "orders.csv"

    cities = ["Pune", "Mumbai", "Bengaluru", "Hyderabad", "Delhi"]
    statuses = ["COMPLETED", "PENDING", "CANCELLED"]

    # Generate customers
    customers = []

    for customer_id in range(1, customer_count + 1):
        customers.append({
            "customer_id": customer_id,
            "customer_name": f"Customer_{customer_id}",
            "email": f"customer{customer_id}@example.com",
            "city": random.choice(cities),
            "registration_date": (
                datetime(2024, 1, 1)
                + timedelta(days=random.randint(0, 365))
            ).date().isoformat(),
        })

    # Add deliberate data-quality issues
    if customer_count >= 10:
        customers[5]["email"] = None
        customers[6]["city"] = None

    # Generate orders
    orders = []

    for order_id in range(1, order_count + 1):
        orders.append({
            "order_id": order_id,
            "customer_id": random.randint(1, customer_count),
            "order_date": (
                datetime(2025, 1, 1)
                + timedelta(days=random.randint(0, 365))
            ).date().isoformat(),
            "amount": round(random.uniform(100, 10000), 2),
            "status": random.choice(statuses),
        })

    # Add deliberate invalid records
    if order_count >= 10:
        orders[5]["amount"] = -500
        orders[6]["customer_id"] = customer_count + 999
        orders[7]["order_date"] = None

    # Add duplicate order
    if order_count >= 20:
        duplicate = orders[9].copy()
        orders.append(duplicate)

    # Write CSV files
    def write_csv(file_path, records):
        with open(file_path, "w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=records[0].keys())
            writer.writeheader()
            writer.writerows(records)

    write_csv(customers_file, customers)
    write_csv(orders_file, orders)

    print("Synthetic data generation completed!")
    print(f"Customers generated: {len(customers)}")
    print(f"Orders generated: {len(orders)}")
    print(f"Output directory: {output_path.resolve()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate synthetic e-commerce datasets"
    )

    parser.add_argument("--customers", type=int, default=1000)
    parser.add_argument("--orders", type=int, default=5000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", default="data/input")

    args = parser.parse_args()

    generate_data(
        output_dir=args.output,
        customer_count=args.customers,
        order_count=args.orders,
        seed=args.seed,
    )
