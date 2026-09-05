"""Gold agent_commission_summary: the core commission mart, one row per agent per pay period.

This is the table a payments team would actually run payroll off of: gross
commission broken out by transaction type, chargeback deductions, and the
resulting net commission owed to the agent for that period.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from commissions_pipeline.config import PATHS
from commissions_pipeline.utils.delta_helpers import write_delta


def transform(spark: SparkSession, silver_dir: str = PATHS.silver) -> DataFrame:
    txns = spark.read.format("delta").load(f"{silver_dir}/fact_commission_transactions")
    chargebacks = spark.read.format("delta").load(f"{silver_dir}/fact_chargebacks")
    agents = spark.read.format("delta").load(f"{silver_dir}/dim_agent").filter("is_current = true")

    by_type = (
        txns.groupBy("agent_id", "pay_period")
        .pivot("transaction_type", ["New Business", "Renewal", "Override"])
        .agg(F.sum("commission_amount"))
        .withColumnRenamed("New Business", "new_business_commission")
        .withColumnRenamed("Renewal", "renewal_commission")
        .withColumnRenamed("Override", "override_commission")
        .fillna(
            0.0, subset=["new_business_commission", "renewal_commission", "override_commission"]
        )
    )
    gross = by_type.withColumn(
        "gross_commission",
        F.col("new_business_commission")
        + F.col("renewal_commission")
        + F.col("override_commission"),
    )

    cb = chargebacks.groupBy("agent_id", "pay_period").agg(
        F.sum("chargeback_amount").alias("chargeback_deductions")
    )

    summary = gross.join(cb, on=["agent_id", "pay_period"], how="left").fillna(
        0.0, subset=["chargeback_deductions"]
    )
    summary = summary.withColumn(
        "net_commission", F.col("gross_commission") + F.col("chargeback_deductions")
    )

    # Enrich with current agent attributes for readability. This is a deliberate SCD1-style
    # simplification for a reporting mart — dim_agent itself still preserves full SCD2 tier
    # history for anyone who needs to analyze commission as-of a historical tier.
    enriched = summary.join(
        agents.select(
            "agent_id",
            F.concat_ws(" ", "first_name", "last_name").alias("agent_name"),
            "agency_id",
            "tier",
            "agent_status",
        ),
        on="agent_id",
        how="left",
    )
    return enriched


def run(
    spark: SparkSession, silver_dir: str = PATHS.silver, gold_dir: str = PATHS.gold
) -> DataFrame:
    df = transform(spark, silver_dir)
    write_delta(
        df, f"{gold_dir}/agent_commission_summary", mode="overwrite", partition_by=["pay_period"]
    )
    return df
