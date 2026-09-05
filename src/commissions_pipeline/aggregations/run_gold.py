"""Orchestrates the full Silver -> Gold aggregation layer."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from commissions_pipeline.aggregations import (
    agent_commission_summary,
    agent_leaderboard,
    chargeback_exposure,
    payment_run_summary,
    reconciliation_report,
)
from commissions_pipeline.config import PATHS
from commissions_pipeline.utils.spark_session import get_spark


def run(spark: SparkSession, silver_dir: str = PATHS.silver, gold_dir: str = PATHS.gold) -> None:
    agent_commission_summary.run(spark, silver_dir, gold_dir)
    payment_run_summary.run(spark, silver_dir, gold_dir)
    chargeback_exposure.run(spark, silver_dir, gold_dir)
    agent_leaderboard.run(spark, gold_dir)
    reconciliation_report.run(spark, silver_dir, gold_dir)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-dir", default=PATHS.silver)
    parser.add_argument("--gold-dir", default=PATHS.gold)
    args = parser.parse_args()

    spark = get_spark("gold-aggregate")
    run(spark, args.silver_dir, args.gold_dir)
    print(f"Gold layer written to {args.gold_dir}")


if __name__ == "__main__":
    main()
