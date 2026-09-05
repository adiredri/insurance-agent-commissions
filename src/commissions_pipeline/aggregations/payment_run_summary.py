"""Gold payment_run_summary: finance-facing rollup of each payment run by region/agency."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from commissions_pipeline.config import PATHS
from commissions_pipeline.utils.delta_helpers import write_delta


def transform(spark: SparkSession, silver_dir: str = PATHS.silver) -> DataFrame:
    payments = spark.read.format("delta").load(f"{silver_dir}/fact_payments")
    agents = spark.read.format("delta").load(f"{silver_dir}/dim_agent").filter("is_current = true")
    agencies = spark.read.format("delta").load(f"{silver_dir}/dim_agency")

    enriched = payments.join(
        agents.select("agent_id", "agency_id"), on="agent_id", how="left"
    ).join(agencies.select("agency_id", "agency_name", "region"), on="agency_id", how="left")

    return (
        enriched.groupBy("pay_period", "agency_id", "agency_name", "region")
        .agg(
            F.count("payment_id").alias("num_payments"),
            F.sum("gross_commission").alias("total_gross_commission"),
            F.sum("chargeback_deductions").alias("total_chargeback_deductions"),
            F.sum("net_payment_amount").alias("total_net_paid"),
            F.sum(F.when(F.col("payment_status") == "Held", 1).otherwise(0)).alias("num_held"),
            F.sum(F.when(F.col("payment_status") == "Failed", 1).otherwise(0)).alias("num_failed"),
        )
        .orderBy("pay_period", "agency_id")
    )


def run(
    spark: SparkSession, silver_dir: str = PATHS.silver, gold_dir: str = PATHS.gold
) -> DataFrame:
    df = transform(spark, silver_dir)
    write_delta(
        df, f"{gold_dir}/payment_run_summary", mode="overwrite", partition_by=["pay_period"]
    )
    return df
