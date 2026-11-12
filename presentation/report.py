import json
from datetime import datetime, timezone
from pathlib import Path


def write_execution_report(
    output_path: str,
    baseline_metrics: dict,
    optimized_metrics: dict,
    comparison: dict,
    status: str = "SUCCESS",
    error: str = None,
    started_at: str = None,
) -> None:
    report = {
        "run_status": status,
        "started_at": started_at,
        "ended_at": datetime.now(timezone.utc).isoformat(),
        "error": error,
        "baseline": baseline_metrics,
        "optimized": optimized_metrics,
        "performance_comparison": comparison,
    }

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as file:
        json.dump(report, file, indent=2, default=str)

    print(f"Execution report saved: {path}")
