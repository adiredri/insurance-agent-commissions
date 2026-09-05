"""Silver dim_policy: deduped, typed policy records.

Bronze contains a couple of late-arriving duplicate policy rows (simulating a
source-system correction) and a handful of rows with a null premium (a real
data-entry gap upstream). Duplicates are resolved by keeping the most
recently ingested version; null-premium rows can't support a commission
calculation, so they're quarantined — written out separately rather than
silently dropped, so they're auditable instead of just vanishing.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from commissions_pipeline.config import PATHS
from commissions_pipeline.dq.expectations import Check, not_null, positive, run_checks, unique
from commissions_pipeline.utils.delta_helpers import write_delta


def transform(spark: SparkSession, bronze_dir: str = PATHS.bronze) -> tuple[DataFrame, DataFrame]:
    df = spark.read.format("delta").load(f"{bronze_dir}/policies")
    df = (
        df.withColumn("issue_date", F.to_date("issue_date"))
        .withColumn("policy_start_date", F.to_date("policy_start_date"))
        .withColumn("cancel_date", F.to_date("cancel_date"))
    )

    w = Window.partitionBy("policy_id").orderBy(F.col("_ingested_at").desc())
    deduped = (
        df.withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .drop("_rn", "_ingested_at", "_source_file")
    )

    clean = deduped.filter(F.col("annual_premium").isNotNull())
    quarantined = deduped.filter(F.col("annual_premium").isNull())
    return clean, quarantined


def run(
    spark: SparkSession, bronze_dir: str = PATHS.bronze, silver_dir: str = PATHS.silver
) -> DataFrame:
    clean, quarantined = transform(spark, bronze_dir)

    checks = [
        Check("policy_id_not_null", "policy_id must never be null", not_null("policy_id")),
        Check("policy_id_unique", "one row per policy after dedup", unique("policy_id")),
        Check("premium_positive", "annual_premium must be positive", positive("annual_premium")),
    ]
    run_checks(spark, clean, checks, table_name="dim_policy", dq_log_path=f"{silver_dir}/_dq_log")

    write_delta(clean, f"{silver_dir}/dim_policy", mode="overwrite", partition_by=["policy_status"])
    if quarantined.take(1):
        write_delta(quarantined, f"{silver_dir}/_quarantine/dim_policy", mode="overwrite")
    return clean
