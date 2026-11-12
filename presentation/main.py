from datetime import datetime, timezone

from business.ingestion import create_spark_session
from business.baseline import run_baseline
from business.optimized import run_optimized
from business.metrics import measure_pipeline, compare_performance
from business.publishing import publish_pipeline_results
from presentation.config import CUSTOMERS_PATH, ORDERS_PATH, OUTPUT_DIR
from presentation.report import write_execution_report


def main():
    started_at = datetime.now(timezone.utc).isoformat()
    spark = create_spark_session()

    try:
        customers_path = str(CUSTOMERS_PATH)
        orders_path = str(ORDERS_PATH)

        print("\n=== BASELINE PIPELINE ===")
        baseline = measure_pipeline(
            run_baseline, spark, customers_path, orders_path
        )

        print("\n=== OPTIMIZED PIPELINE ===")
        optimized = measure_pipeline(
            run_optimized, spark, customers_path, orders_path
        )

        comparison = compare_performance(
            baseline["metrics"], optimized["metrics"]
        )

        print("\n=== PERFORMANCE COMPARISON ===")
        print(comparison)

        print("\n=== ROW COUNT RECONCILIATION ===")
        for name, baseline_count in baseline["metrics"]["record_counts"].items():
            optimized_count = optimized["metrics"]["record_counts"][name]
            print(f"{name}: baseline={baseline_count}, optimized={optimized_count}")
            if baseline_count != optimized_count:
                raise AssertionError(f"Row count mismatch for {name}")

        print("\n=== PUBLISHING OPTIMIZED RESULTS ===")
        publish_pipeline_results(
            optimized["results"], str(OUTPUT_DIR / "optimized")
        )

        write_execution_report(
            str(OUTPUT_DIR / "reports" / "execution_report.json"),
            baseline["metrics"],
            optimized["metrics"],
            comparison,
            started_at=started_at,
        )

        print("\nPIPELINE COMPLETED SUCCESSFULLY")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
