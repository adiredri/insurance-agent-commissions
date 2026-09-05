"""Silver fact_payments: cleaned agent payment-run records from the finance system."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from commissions_pipeline.config import PATHS, PAYMENT_STATUSES
from commissions_pipeline.dq.expectations import Check, in_set, not_null, run_checks, unique
from commissions_pipeline.utils.delta_helpers import write_delta


def transform(spark: SparkSession, bronze_dir: str = PATHS.bronze) -> DataFrame:
    df = spark.read.format("delta").load(f"{bronze_dir}/payments")
    df = df.withColumn("payment_date", F.to_date("payment_date")).drop(
        "_ingested_at", "_source_file"
    )
    return df


def run(
    spark: SparkSession, bronze_dir: str = PATHS.bronze, silver_dir: str = PATHS.silver
) -> DataFrame:
    df = transform(spark, bronze_dir)

    checks = [
        Check("payment_id_not_null", "payment_id must never be null", not_null("payment_id")),
        Check("payment_id_unique", "payment_id must be unique", unique("payment_id")),
        Check(
            "payment_status_valid",
            "payment_status must be one of the known enum values",
            in_set("payment_status", set(PAYMENT_STATUSES)),
        ),
    ]
    run_checks(spark, df, checks, table_name="fact_payments", dq_log_path=f"{silver_dir}/_dq_log")

    write_delta(df, f"{silver_dir}/fact_payments", mode="overwrite", partition_by=["pay_period"])
    return df
