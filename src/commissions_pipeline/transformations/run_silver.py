"""Orchestrates the full Bronze -> Silver transformation layer."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from commissions_pipeline.config import PATHS
from commissions_pipeline.transformations import (
    dim_agency,
    dim_agent,
    dim_policy,
    dim_product,
    fact_chargebacks,
    fact_commission_transactions,
    fact_payments,
)
from commissions_pipeline.utils.spark_session import get_spark


def run(
    spark: SparkSession, bronze_dir: str = PATHS.bronze, silver_dir: str = PATHS.silver
) -> None:
    dim_agency.run(spark, bronze_dir, silver_dir)
    dim_agent.run(spark, bronze_dir, silver_dir)
    dim_product.run(spark, bronze_dir, silver_dir)
    dim_policy.run(spark, bronze_dir, silver_dir)
    fact_commission_transactions.run(spark, bronze_dir, silver_dir)
    fact_chargebacks.run(spark, bronze_dir, silver_dir)
    fact_payments.run(spark, bronze_dir, silver_dir)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze-dir", default=PATHS.bronze)
    parser.add_argument("--silver-dir", default=PATHS.silver)
    args = parser.parse_args()

    spark = get_spark("silver-transform")
    run(spark, args.bronze_dir, args.silver_dir)
    print(f"Silver layer written to {args.silver_dir}")


if __name__ == "__main__":
    main()
