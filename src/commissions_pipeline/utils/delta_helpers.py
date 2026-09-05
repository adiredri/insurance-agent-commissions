"""Reusable Delta Lake patterns: idempotent writes, upserts, and SCD2 merges."""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def table_exists(spark: SparkSession, path: str) -> bool:
    try:
        DeltaTable.forPath(spark, path)
        return True
    except Exception:
        return False


def write_delta(
    df: DataFrame, path: str, mode: str = "overwrite", partition_by: list[str] | None = None
) -> None:
    writer = (
        df.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true" if mode == "overwrite" else "false")
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def upsert_delta(spark: SparkSession, df: DataFrame, path: str, key_cols: list[str]) -> None:
    """Merge-by-key upsert — used for dimensions/facts that are re-emitted in full on each run."""
    if not table_exists(spark, path):
        write_delta(df, path, mode="overwrite")
        return

    target = DeltaTable.forPath(spark, path)
    condition = " AND ".join(f"target.{c} = source.{c}" for c in key_cols)
    (
        target.alias("target")
        .merge(df.alias("source"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def scd2_merge(
    spark: SparkSession,
    updates: DataFrame,
    path: str,
    key_col: str,
    tracked_cols: list[str],
    effective_start_col: str = "effective_start_date",
    effective_end_col: str = "effective_end_date",
    is_current_col: str = "is_current",
) -> None:
    """Type-2 slowly changing dimension merge.

    `updates` must contain one row per key representing its *current* state
    plus `effective_start_col`. Rows whose tracked columns changed get their
    old version closed out (effective_end + is_current=false) and a new
    current version inserted — the standard SCD2 pattern for dimensions like
    dim_agent where tier/status changes need to be queryable as-of any date.
    """
    if not table_exists(spark, path):
        from pyspark.sql.functions import lit

        seeded = updates.withColumn(is_current_col, lit(True)).withColumn(
            effective_end_col, lit(None).cast("date")
        )
        write_delta(seeded, path, mode="overwrite")
        return

    target = DeltaTable.forPath(spark, path)
    change_condition = " OR ".join(f"target.{c} <> source.{c}" for c in tracked_cols)

    current = target.toDF().filter(f"{is_current_col} = true")
    joined = current.alias("target").join(updates.alias("source"), key_col, "inner")
    changed_keys = [
        r[key_col] for r in joined.filter(change_condition).select(key_col).distinct().collect()
    ]

    if changed_keys:
        (
            target.alias("target").update(
                condition=f"target.{key_col} IN ({','.join(repr(k) for k in changed_keys)}) AND target.{is_current_col} = true",
                set={
                    is_current_col: "false",
                    effective_end_col: "current_date()",
                },
            )
        )
        from pyspark.sql.functions import lit

        new_versions = (
            updates.filter(updates[key_col].isin(changed_keys))
            .withColumn(is_current_col, lit(True))
            .withColumn(effective_end_col, lit(None).cast("date"))
        )
        new_versions.write.format("delta").mode("append").save(path)

    existing_keys = [r[key_col] for r in current.select(key_col).distinct().collect()]
    from pyspark.sql.functions import lit

    brand_new = (
        updates.filter(~updates[key_col].isin(existing_keys))
        .withColumn(is_current_col, lit(True))
        .withColumn(effective_end_col, lit(None).cast("date"))
    )
    if brand_new.take(1):
        brand_new.write.format("delta").mode("append").save(path)
