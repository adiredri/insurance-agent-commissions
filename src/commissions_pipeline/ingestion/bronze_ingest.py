"""Bronze layer: land raw CSV extracts into Delta, unchanged, with lineage columns.

Bronze is intentionally "dumb" — no dedup, no type coercion beyond what CSV
inference gives us, no business rules. That's what makes it replayable: if a
downstream bug is found next month, Silver can be rebuilt from Bronze without
re-pulling from source systems. All cleaning happens in the Silver layer.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from commissions_pipeline.config import PATHS
from commissions_pipeline.utils.delta_helpers import write_delta
from commissions_pipeline.utils.spark_session import get_spark

REFERENCE_TABLES = ["agencies", "agents", "agent_tier_history", "products", "commission_plans"]
INCREMENTAL_TABLES = ["policies", "commission_transactions", "chargebacks", "payments"]


def _with_lineage(df: DataFrame) -> DataFrame:
    return df.withColumn("_ingested_at", F.current_timestamp()).withColumn(
        "_source_file", F.input_file_name()
    )


def ingest_reference_table(spark: SparkSession, raw_dir: str, name: str, out_dir: str) -> int:
    """Reference/dimension source dump — read every full_load_date partition landed
    so far but keep only the most recent one as this run's Bronze truth (a real
    agency-management extract is a full snapshot each time, not incremental)."""
    path = f"{raw_dir}/reference/{name}"
    df = spark.read.option("header", True).option("inferSchema", True).csv(path)
    df = df.withColumn(
        "_full_load_date", F.regexp_extract(F.input_file_name(), r"full_load_date=([\d-]+)", 1)
    )
    latest = df.agg(F.max("_full_load_date")).collect()[0][0]
    df = df.filter(F.col("_full_load_date") == latest).drop("_full_load_date")
    df = _with_lineage(df)
    write_delta(df, f"{out_dir}/{name}", mode="overwrite")
    return df.count()


def ingest_incremental_table(spark: SparkSession, raw_dir: str, name: str, out_dir: str) -> int:
    path = f"{raw_dir}/{name}"
    df = spark.read.option("header", True).option("inferSchema", True).csv(path)
    df = _with_lineage(df)
    write_delta(df, f"{out_dir}/{name}", mode="overwrite")
    return df.count()


def run(
    spark: SparkSession, raw_dir: str = PATHS.raw, out_dir: str | None = None
) -> dict[str, int]:
    out_dir = out_dir or PATHS.bronze
    counts: dict[str, int] = {}
    for name in REFERENCE_TABLES:
        counts[name] = ingest_reference_table(spark, raw_dir, name, out_dir)
    for name in INCREMENTAL_TABLES:
        counts[name] = ingest_incremental_table(spark, raw_dir, name, out_dir)
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", default=PATHS.raw, help="directory of raw CSV landing files")
    parser.add_argument(
        "--out-dir", default=PATHS.bronze, help="Delta bronze table output directory"
    )
    args = parser.parse_args()

    spark = get_spark("bronze-ingest")
    counts = run(spark, args.raw_dir, args.out_dir)
    for name, n in counts.items():
        print(f"{name:>24s}: {n:>8,} rows -> {args.out_dir}/{name}")


if __name__ == "__main__":
    main()
