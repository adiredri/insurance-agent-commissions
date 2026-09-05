"""Silver dim_agency — reference data, cleaned and deduped."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from commissions_pipeline.config import PATHS
from commissions_pipeline.dq.expectations import Check, not_null, run_checks, unique
from commissions_pipeline.utils.delta_helpers import write_delta


def transform(spark: SparkSession, bronze_dir: str = PATHS.bronze) -> DataFrame:
    df = spark.read.format("delta").load(f"{bronze_dir}/agencies")
    return df.drop("_ingested_at", "_source_file").dropDuplicates(["agency_id"])


def run(
    spark: SparkSession, bronze_dir: str = PATHS.bronze, silver_dir: str = PATHS.silver
) -> DataFrame:
    df = transform(spark, bronze_dir)
    run_checks(
        spark,
        df,
        [
            Check("agency_id_not_null", "agency_id must never be null", not_null("agency_id")),
            Check("agency_id_unique", "agency_id must be unique", unique("agency_id")),
        ],
        table_name="dim_agency",
        dq_log_path=f"{silver_dir}/_dq_log",
    )
    write_delta(df, f"{silver_dir}/dim_agency", mode="overwrite")
    return df
