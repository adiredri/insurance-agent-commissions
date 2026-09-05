"""Gold agent_leaderboard: trailing-12-month agent ranking by net commission."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from commissions_pipeline.config import PATHS
from commissions_pipeline.utils.delta_helpers import write_delta


def transform(spark: SparkSession, gold_dir: str = PATHS.gold) -> DataFrame:
    summary = spark.read.format("delta").load(f"{gold_dir}/agent_commission_summary")

    max_period = summary.agg(F.max("pay_period")).collect()[0][0]
    trailing_12 = summary.filter(
        F.col("pay_period")
        >= F.date_format(F.add_months(F.to_date(F.lit(max_period + "-01")), -11), "yyyy-MM")
    )

    totals = trailing_12.groupBy("agent_id", "agent_name", "agency_id", "tier").agg(
        F.sum("gross_commission").alias("trailing_12mo_gross_commission"),
        F.sum("net_commission").alias("trailing_12mo_net_commission"),
        F.sum("chargeback_deductions").alias("trailing_12mo_chargebacks"),
    )

    w = Window.orderBy(F.col("trailing_12mo_net_commission").desc())
    return totals.withColumn("rank", F.rank().over(w))


def run(spark: SparkSession, gold_dir: str = PATHS.gold) -> DataFrame:
    df = transform(spark, gold_dir)
    write_delta(df, f"{gold_dir}/agent_leaderboard", mode="overwrite")
    return df
