from datetime import datetime, timezone
import uuid

from business.ingestion import create_spark_session
from business.baseline import run_baseline
from business.optimized import run_optimized
from business.metrics import measure_pipeline, compare_performance
from business.publishing import publish_pipeline_results

from presentation.config import CUSTOMERS_PATH, ORDERS_PATH, OUTPUT_DIR
from presentation.report import write_execution_report
from presentation.audit_report import write_audit_report


def main():
    started_at = datetime.now(timezone.utc).isoformat()

    run_id = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        + "_"
        + uuid.uuid4().hex[:8]
    )

    spark = None

    try:
        spark = create_spark_session()

        customers_path = str(CUSTOMERS_PATH)
        orders_path = str(ORDERS_PATH)

        print(f"\n=== PIPELINE RUN: {run_id} ===")

        # 1. Run baseline pipeline
        print("\n=== BASELINE PIPELINE ===")

        baseline = measure_pipeline(
            run_baseline,
            spark,
            customers_path,
            orders_path
        )

        # 2. Run optimized pipeline
        print("\n=== OPTIMIZED PIPELINE ===")

        optimized = measure_pipeline(
            run_optimized,
            spark,
            customers_path,
            orders_path
        )

        # 3. Compare performance
        comparison = compare_performance(
            baseline["metrics"],
            optimized["metrics"]
        )

        print("\n=== PERFORMANCE COMPARISON ===")
        print(comparison)

        # 4. Reconcile row counts
        print("\n=== ROW COUNT RECONCILIATION ===")

        for name, baseline_count in baseline["metrics"]["record_counts"].items():

            optimized_count = optimized["metrics"]["record_counts"][name]

            print(
                f"{name}: baseline={baseline_count}, "
                f"optimized={optimized_count}"
            )

            if baseline_count != optimized_count:
                raise AssertionError(
                    f"Row count mismatch for {name}"
                )

        # 5. Preserve historical output for every run
        print("\n=== PUBLISHING OPTIMIZED RESULTS ===")

        run_output_dir = OUTPUT_DIR / "runs" / run_id

        publish_pipeline_results(
            optimized["results"],
            str(run_output_dir / "optimized")
        )

        # 6. Write execution report
        write_execution_report(
            str(OUTPUT_DIR / "reports" / "execution_report.json"),
            baseline["metrics"],
            optimized["metrics"],
            comparison,
            status="SUCCESS",
            started_at=started_at
        )

        # 7. Write audit report
        write_audit_report(
            str(OUTPUT_DIR / "reports" / f"audit_{run_id}.json"),
            optimized["results"],
            run_id=run_id,
            started_at=started_at,
            status="SUCCESS"
        )

        print(f"\nPIPELINE RUN ID: {run_id}")
        print(f"OUTPUT DIRECTORY: {run_output_dir}")
        print("\nPIPELINE COMPLETED SUCCESSFULLY")

    except Exception as exc:

        print(f"\nPIPELINE FAILED: {exc}")

        # Write failed execution report
        write_execution_report(
            str(OUTPUT_DIR / "reports" / "execution_report.json"),
            {},
            {},
            {},
            status="FAILED",
            error=str(exc),
            started_at=started_at
        )

        # Write failed audit report
        write_audit_report(
            str(OUTPUT_DIR / "reports" / f"audit_{run_id}.json"),
            {},
            run_id=run_id,
            started_at=started_at,
            status="FAILED",
            error=str(exc)
        )

        raise

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()