import time


def measure_pipeline(
    pipeline_function,
    spark,
    customers_path: str,
    orders_path: str,
) -> dict:
    """
    Measure pipeline wall-clock time, including Spark actions,
    and collect record counts for all returned DataFrames.
    """

    start_time = time.perf_counter()

    results = pipeline_function(
        spark,
        customers_path,
        orders_path,
    )

    record_counts = {}

    # Trigger Spark execution and collect output counts.
    for name, dataframe in results.items():
        record_counts[name] = dataframe.count()

    execution_seconds = time.perf_counter() - start_time

    return {
        "results": results,
        "metrics": {
            "execution_seconds": round(execution_seconds, 4),
            "record_counts": record_counts,
        },
    }


def compare_performance(
    baseline_metrics: dict,
    optimized_metrics: dict,
) -> dict:
    """Compare baseline and optimized pipeline execution times."""

    baseline_time = baseline_metrics["execution_seconds"]
    optimized_time = optimized_metrics["execution_seconds"]

    if baseline_time <= 0 or optimized_time <= 0:
        raise ValueError("Execution times must be greater than zero.")

    speedup = baseline_time / optimized_time

    improvement_percentage = (
        (baseline_time - optimized_time) / baseline_time
    ) * 100

    return {
        "baseline_seconds": baseline_time,
        "optimized_seconds": optimized_time,
        "speedup": round(speedup, 2),
        "improvement_percentage": round(
            improvement_percentage, 2
        ),
    }
