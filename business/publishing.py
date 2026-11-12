from pathlib import Path
from pyspark.sql import DataFrame


def publish_dataframe(
    dataframe: DataFrame,
    output_path: str,
    mode: str = "overwrite",
    partition_by: list = None,
) -> None:
    """Publish a DataFrame as Parquet."""

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    writer = dataframe.write.mode(mode)

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.parquet(str(path))


def publish_pipeline_results(
    results: dict,
    output_dir: str = "storage/output",
) -> None:
    """Publish all pipeline DataFrames as separate Parquet datasets."""

    for name, dataframe in results.items():
        output_path = str(Path(output_dir) / name)

        publish_dataframe(
            dataframe,
            output_path,
            mode="overwrite",
        )

        print(f"Published {name} to {output_path}")
