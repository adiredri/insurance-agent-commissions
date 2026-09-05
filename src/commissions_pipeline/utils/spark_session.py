"""Spark session factory.

One function builds the SparkSession everywhere — local pytest runs, the
Docker container, and Databricks jobs — so environment differences live here
instead of being duplicated across every job entrypoint. On Databricks, the
cluster already provides a SparkSession with Delta configured, so
`get_spark()` simply returns the active session there.
"""

from __future__ import annotations

from pyspark.sql import SparkSession


def _running_on_databricks() -> bool:
    import os

    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark(app_name: str = "commissions-pipeline") -> SparkSession:
    if _running_on_databricks():
        return SparkSession.builder.getOrCreate()

    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.session.timeZone", "UTC")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
