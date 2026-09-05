"""Silver fact_chargebacks: commission clawbacks from early policy cancellations."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from commissions_pipeline.config import PATHS
from commissions_pipeline.dq.expectations import Check, not_null, run_checks, unique
from commissions_pipeline.utils.delta_helpers import write_delta


def transform(spark: SparkSession, bronze_dir: str = PATHS.bronze) -> DataFrame:
    df = spark.read.format("delta").load(f"{bronze_dir}/chargebacks")
    df = df.withColumn("chargeback_date", F.to_date("chargeback_date")).drop(
        "_ingested_at", "_source_file"
    )
    df = df.withColumn("pay_period", F.date_format("chargeback_date", "yyyy-MM"))
    return df


def run(
    spark: SparkSession, bronze_dir: str = PATHS.bronze, silver_dir: str = PATHS.silver
) -> DataFrame:
    df = transform(spark, bronze_dir)

    checks = [
        Check(
            "chargeback_id_not_null", "chargeback_id must never be null", not_null("chargeback_id")
        ),
        Check("chargeback_id_unique", "chargeback_id must be unique", unique("chargeback_id")),
        Check(
            "chargeback_amount_negative",
            "a chargeback must reduce commission, so amount must be negative",
            lambda d: d.filter(F.col("chargeback_amount") >= 0),
        ),
    ]
    run_checks(
        spark, df, checks, table_name="fact_chargebacks", dq_log_path=f"{silver_dir}/_dq_log"
    )

    write_delta(df, f"{silver_dir}/fact_chargebacks", mode="overwrite", partition_by=["pay_period"])
    return df
