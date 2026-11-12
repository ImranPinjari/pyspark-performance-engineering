import json
from datetime import datetime, timezone
from pathlib import Path


def write_audit_report(
    output_path: str,
    results: dict,
    run_id: str,
    started_at: str,
    status: str = "SUCCESS",
    error: str = None,
) -> None:

    audit = {
        "run_id": run_id,
        "run_status": status,
        "started_at": started_at,
        "ended_at": datetime.now(timezone.utc).isoformat(),
        "error": error,
        "datasets": {},
    }

    for dataset_name, dataframe in results.items():
        dataset_info = {
            "record_count": dataframe.count()
        }

        if "reject_reason" in dataframe.columns:
            rejection_counts = (
                dataframe
                .groupBy("reject_reason")
                .count()
                .collect()
            )

            dataset_info["rejection_breakdown"] = {
                row["reject_reason"]: row["count"]
                for row in rejection_counts
            }

        audit["datasets"][dataset_name] = dataset_info

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as file:
        json.dump(audit, file, indent=2, default=str)

    print(f"Audit report saved: {path}")
