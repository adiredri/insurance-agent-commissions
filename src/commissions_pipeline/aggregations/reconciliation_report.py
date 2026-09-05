"""Gold reconciliation_report: commission engine vs. finance payment system.

Two independent systems compute the same number two different ways: this
pipeline calculates what an agent is *owed* from raw transactions
(agent_commission_summary), while the finance system's payment run records
what was *actually paid* (fact_payments). In any real commissions platform
these should reconcile to the penny every period — this report is the
control that catches it when they don't, before an agent notices a payment
is wrong or finance overpays and can't get the money back.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from commissions_pipeline.config import PATHS
from commissions_pipeline.utils.delta_helpers import write_delta

BREAK_TOLERANCE = 1.00  # dollars — differences within a rounding tolerance aren't flagged


def transform(
    spark: SparkSession, silver_dir: str = PATHS.silver, gold_dir: str = PATHS.gold
) -> DataFrame:
    calculated = (
        spark.read.format("delta")
        .load(f"{gold_dir}/agent_commission_summary")
        .select(
            "agent_id",
            "agent_name",
            "pay_period",
            F.col("net_commission").alias("calculated_net_commission"),
        )
    )
    paid = (
        spark.read.format("delta")
        .load(f"{silver_dir}/fact_payments")
        .select(
            "agent_id",
            "pay_period",
            F.col("net_payment_amount").alias("recorded_net_payment"),
            "payment_status",
        )
    )

    recon = calculated.join(paid, on=["agent_id", "pay_period"], how="full_outer")
    recon = recon.withColumn(
        "variance",
        F.round(
            F.coalesce("recorded_net_payment", F.lit(0.0))
            - F.coalesce("calculated_net_commission", F.lit(0.0)),
            2,
        ),
    )
    recon = recon.withColumn(
        "reconciliation_status",
        F.when(F.col("calculated_net_commission").isNull(), "PAID_WITHOUT_CALCULATION")
        .when(F.col("recorded_net_payment").isNull(), "CALCULATED_BUT_NOT_PAID")
        .when(F.abs(F.col("variance")) > BREAK_TOLERANCE, "BREAK")
        .otherwise("MATCHED"),
    )
    return recon.orderBy(F.abs(F.col("variance")).desc())


def run(
    spark: SparkSession, silver_dir: str = PATHS.silver, gold_dir: str = PATHS.gold
) -> DataFrame:
    df = transform(spark, silver_dir, gold_dir)
    write_delta(df, f"{gold_dir}/reconciliation_report", mode="overwrite")

    breaks = df.filter("reconciliation_status != 'MATCHED'").count()
    total = df.count()
    print(
        f"Reconciliation: {total - breaks:,}/{total:,} periods matched, {breaks:,} breaks flagged for review"
    )
    return df
