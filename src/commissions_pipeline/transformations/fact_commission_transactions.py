"""Silver fact_commission_transactions: cleaned, typed, quality-checked commission events.

New Business, Renewal, and Override transactions should always carry a
positive commission_amount — a negative value here is a fat-fingered sign
error at the source, not a business chargeback (those live in their own
fact table). Rows that fail that check are quarantined rather than silently
corrected, since flipping a sign on financial data without confirmation
would be the wrong call for a real payments pipeline.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from commissions_pipeline.config import PATHS
from commissions_pipeline.dq.expectations import (
    Check,
    in_set,
    not_null,
    positive,
    run_checks,
    unique,
)
from commissions_pipeline.utils.delta_helpers import write_delta

VALID_TYPES = {"New Business", "Renewal", "Override"}


def transform(spark: SparkSession, bronze_dir: str = PATHS.bronze) -> tuple[DataFrame, DataFrame]:
    df = spark.read.format("delta").load(f"{bronze_dir}/commission_transactions")
    df = df.withColumn("transaction_date", F.to_date("transaction_date")).drop(
        "_ingested_at", "_source_file"
    )
    df = df.withColumn("pay_period", F.date_format("transaction_date", "yyyy-MM"))

    clean = df.filter(F.col("commission_amount") > 0)
    quarantined = df.filter((F.col("commission_amount") <= 0) | F.col("commission_amount").isNull())
    return clean, quarantined


def run(
    spark: SparkSession, bronze_dir: str = PATHS.bronze, silver_dir: str = PATHS.silver
) -> DataFrame:
    clean, quarantined = transform(spark, bronze_dir)

    checks = [
        Check(
            "transaction_id_not_null",
            "transaction_id must never be null",
            not_null("transaction_id"),
        ),
        Check("transaction_id_unique", "transaction_id must be unique", unique("transaction_id")),
        Check("agent_id_not_null", "agent_id must never be null", not_null("agent_id")),
        Check(
            "commission_amount_positive",
            "commission_amount must be positive",
            positive("commission_amount"),
        ),
        Check(
            "transaction_type_valid",
            "transaction_type must be one of the known enum values",
            in_set("transaction_type", VALID_TYPES),
        ),
    ]
    run_checks(
        spark,
        clean,
        checks,
        table_name="fact_commission_transactions",
        dq_log_path=f"{silver_dir}/_dq_log",
    )

    write_delta(
        clean,
        f"{silver_dir}/fact_commission_transactions",
        mode="overwrite",
        partition_by=["pay_period"],
    )
    if quarantined.take(1):
        write_delta(
            quarantined, f"{silver_dir}/_quarantine/fact_commission_transactions", mode="overwrite"
        )
    return clean
