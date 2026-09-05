"""Gold chargeback_exposure: flags agents whose chargeback rate signals risk.

A high chargeback rate (lots of early policy cancellations relative to new
business written) is a leading indicator of churny or even fraudulent
business — this is the kind of table a commissions/compliance team actually
watches.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from commissions_pipeline.config import PATHS
from commissions_pipeline.utils.delta_helpers import write_delta

HIGH_RISK_THRESHOLD = 0.25


def transform(spark: SparkSession, silver_dir: str = PATHS.silver) -> DataFrame:
    txns = spark.read.format("delta").load(f"{silver_dir}/fact_commission_transactions")
    chargebacks = spark.read.format("delta").load(f"{silver_dir}/fact_chargebacks")
    agents = spark.read.format("delta").load(f"{silver_dir}/dim_agent").filter("is_current = true")

    nb = (
        txns.filter("transaction_type = 'New Business'")
        .groupBy("agent_id")
        .agg(
            F.sum("commission_amount").alias("new_business_commission"),
            F.count("transaction_id").alias("new_business_count"),
        )
    )
    cb = chargebacks.groupBy("agent_id").agg(
        F.sum(F.abs("chargeback_amount")).alias("total_chargeback_amount"),
        F.count("chargeback_id").alias("chargeback_count"),
    )

    exposure = (
        nb.join(cb, on="agent_id", how="left")
        .fillna(0.0, subset=["total_chargeback_amount"])
        .fillna(0, subset=["chargeback_count"])
    )

    exposure = exposure.withColumn(
        "chargeback_rate",
        F.when(
            F.col("new_business_commission") > 0,
            F.col("total_chargeback_amount") / F.col("new_business_commission"),
        ).otherwise(0.0),
    ).withColumn("is_high_risk", F.col("chargeback_rate") > HIGH_RISK_THRESHOLD)

    return exposure.join(
        agents.select(
            "agent_id",
            F.concat_ws(" ", "first_name", "last_name").alias("agent_name"),
            "agency_id",
            "tier",
        ),
        on="agent_id",
        how="left",
    ).orderBy(F.col("chargeback_rate").desc())


def run(
    spark: SparkSession, silver_dir: str = PATHS.silver, gold_dir: str = PATHS.gold
) -> DataFrame:
    df = transform(spark, silver_dir)
    write_delta(df, f"{gold_dir}/chargeback_exposure", mode="overwrite")
    return df
