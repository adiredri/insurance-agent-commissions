"""Silver dim_product / dim_commission_plan — reference data, cleaned and deduped."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from commissions_pipeline.config import PATHS
from commissions_pipeline.dq.expectations import Check, not_null, run_checks, unique
from commissions_pipeline.utils.delta_helpers import write_delta


def transform_products(spark: SparkSession, bronze_dir: str = PATHS.bronze) -> DataFrame:
    df = spark.read.format("delta").load(f"{bronze_dir}/products")
    return df.drop("_ingested_at", "_source_file").dropDuplicates(["product_id"])


def transform_commission_plans(spark: SparkSession, bronze_dir: str = PATHS.bronze) -> DataFrame:
    df = spark.read.format("delta").load(f"{bronze_dir}/commission_plans")
    df = df.withColumn("effective_start_date", F.to_date("effective_start_date")).withColumn(
        "effective_end_date", F.to_date("effective_end_date")
    )
    return df.drop("_ingested_at", "_source_file").dropDuplicates(["plan_id"])


def run(
    spark: SparkSession, bronze_dir: str = PATHS.bronze, silver_dir: str = PATHS.silver
) -> None:
    products = transform_products(spark, bronze_dir)
    run_checks(
        spark,
        products,
        [
            Check("product_id_not_null", "product_id must never be null", not_null("product_id")),
            Check("product_id_unique", "product_id must be unique", unique("product_id")),
        ],
        table_name="dim_product",
        dq_log_path=f"{silver_dir}/_dq_log",
    )
    write_delta(products, f"{silver_dir}/dim_product", mode="overwrite")

    plans = transform_commission_plans(spark, bronze_dir)
    run_checks(
        spark,
        plans,
        [Check("plan_id_not_null", "plan_id must never be null", not_null("plan_id"))],
        table_name="dim_commission_plan",
        dq_log_path=f"{silver_dir}/_dq_log",
    )
    write_delta(plans, f"{silver_dir}/dim_commission_plan", mode="overwrite")
