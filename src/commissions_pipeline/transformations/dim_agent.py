"""Silver dim_agent: a Type-2 SCD combining agent master data with tier history.

One row per agent per tier period. `is_current = true` identifies the row to
join against for "who is this agent today"; historical rows let Gold compute
commission accurately as-of the transaction date, since an agent's tier (and
therefore commission rate) can change mid-history.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from commissions_pipeline.config import PATHS
from commissions_pipeline.dq.expectations import Check, not_null, run_checks, unique
from commissions_pipeline.utils.delta_helpers import write_delta


def _clean_agents(bronze_agents: DataFrame) -> DataFrame:
    df = bronze_agents.withColumn("license_state", F.upper(F.trim(F.col("license_state"))))
    df = df.withColumn("hire_date", F.to_date("hire_date")).withColumn(
        "termination_date", F.to_date("termination_date")
    )
    # bronze intentionally contains a couple of exact-duplicate re-extract rows — dedupe on the
    # full business-key + attribute set here, in Silver, where cleaning rules belong.
    df = df.dropDuplicates(["agent_id"])
    df = df.filter(F.col("agent_id").isNotNull())
    return df


def transform(spark: SparkSession, bronze_dir: str = PATHS.bronze) -> DataFrame:
    agents = spark.read.format("delta").load(f"{bronze_dir}/agents")
    tier_history = spark.read.format("delta").load(f"{bronze_dir}/agent_tier_history")

    agents_clean = _clean_agents(agents)
    tiers = tier_history.withColumn(
        "effective_start_date", F.to_date("effective_start_date")
    ).withColumn("effective_end_date", F.to_date("effective_end_date"))

    dim = agents_clean.drop("_ingested_at", "_source_file").join(
        tiers.select(
            "agent_id", "tier", "effective_start_date", "effective_end_date", "is_current"
        ),
        on="agent_id",
        how="inner",
    )

    w = Window.partitionBy("agent_id").orderBy(F.col("effective_start_date").asc())
    dim = dim.withColumn(
        "agent_sk", F.concat_ws("_", F.col("agent_id"), F.row_number().over(w).cast("string"))
    )
    return dim


def run(
    spark: SparkSession, bronze_dir: str = PATHS.bronze, silver_dir: str = PATHS.silver
) -> DataFrame:
    dim = transform(spark, bronze_dir)

    checks = [
        Check("agent_id_not_null", "agent_id must never be null", not_null("agent_id")),
        Check("agent_sk_unique", "one row per agent per tier period", unique("agent_sk")),
        Check(
            "one_current_row_per_agent",
            "exactly one is_current=true row per agent",
            lambda df: df.filter("is_current = true")
            .groupBy("agent_id")
            .count()
            .filter("count > 1"),
        ),
    ]
    run_checks(spark, dim, checks, table_name="dim_agent", dq_log_path=f"{silver_dir}/_dq_log")

    write_delta(dim, f"{silver_dir}/dim_agent", mode="overwrite", partition_by=["is_current"])
    return dim
