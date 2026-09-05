from commissions_pipeline.utils.delta_helpers import scd2_merge, upsert_delta


def test_upsert_delta_creates_table_if_not_exists(spark, tmp_path):
    path = f"{tmp_path}/dim_test"
    df = spark.createDataFrame([("A", 1), ("B", 2)], ["id", "value"])
    upsert_delta(spark, df, path, key_cols=["id"])

    result = spark.read.format("delta").load(path)
    assert result.count() == 2


def test_upsert_delta_merges_on_key_without_duplicating(spark, tmp_path):
    path = f"{tmp_path}/dim_test"
    upsert_delta(spark, spark.createDataFrame([("A", 1), ("B", 2)], ["id", "value"]), path, ["id"])
    upsert_delta(spark, spark.createDataFrame([("A", 99), ("C", 3)], ["id", "value"]), path, ["id"])

    result = {r["id"]: r["value"] for r in spark.read.format("delta").load(path).collect()}
    assert result == {"A": 99, "B": 2, "C": 3}


def test_scd2_merge_seeds_initial_table_as_current(spark, tmp_path):
    path = f"{tmp_path}/dim_agent_scd2"
    initial = spark.createDataFrame(
        [("AGT001", "Gold", "2024-01-01"), ("AGT002", "Silver", "2024-01-01")],
        ["agent_id", "tier", "effective_start_date"],
    )
    scd2_merge(spark, initial, path, key_col="agent_id", tracked_cols=["tier"])

    result = spark.read.format("delta").load(path).collect()
    assert len(result) == 2
    assert all(r["is_current"] for r in result)
    assert all(r["effective_end_date"] is None for r in result)


def test_scd2_merge_closes_old_version_when_tracked_column_changes(spark, tmp_path):
    path = f"{tmp_path}/dim_agent_scd2"
    initial = spark.createDataFrame(
        [("AGT001", "Gold", "2024-01-01"), ("AGT002", "Silver", "2024-01-01")],
        ["agent_id", "tier", "effective_start_date"],
    )
    scd2_merge(spark, initial, path, key_col="agent_id", tracked_cols=["tier"])

    # AGT001 gets promoted to Platinum; AGT002 is unchanged
    update = spark.createDataFrame(
        [("AGT001", "Platinum", "2025-06-01"), ("AGT002", "Silver", "2024-01-01")],
        ["agent_id", "tier", "effective_start_date"],
    )
    scd2_merge(spark, update, path, key_col="agent_id", tracked_cols=["tier"])

    result = spark.read.format("delta").load(path)

    agt001_rows = result.filter("agent_id = 'AGT001'").collect()
    assert len(agt001_rows) == 2  # old Gold version + new Platinum version
    current = [r for r in agt001_rows if r["is_current"]]
    assert len(current) == 1
    assert current[0]["tier"] == "Platinum"
    closed = [r for r in agt001_rows if not r["is_current"]]
    assert closed[0]["effective_end_date"] is not None

    agt002_rows = result.filter("agent_id = 'AGT002'").collect()
    assert len(agt002_rows) == 1  # unchanged — no new version created
    assert agt002_rows[0]["is_current"]


def test_scd2_merge_inserts_brand_new_keys(spark, tmp_path):
    path = f"{tmp_path}/dim_agent_scd2"
    scd2_merge(
        spark,
        spark.createDataFrame(
            [("AGT001", "Gold", "2024-01-01")], ["agent_id", "tier", "effective_start_date"]
        ),
        path,
        key_col="agent_id",
        tracked_cols=["tier"],
    )
    scd2_merge(
        spark,
        spark.createDataFrame(
            [("AGT001", "Gold", "2024-01-01"), ("AGT003", "Bronze", "2025-01-01")],
            ["agent_id", "tier", "effective_start_date"],
        ),
        path,
        key_col="agent_id",
        tracked_cols=["tier"],
    )

    result = spark.read.format("delta").load(path)
    assert result.filter("agent_id = 'AGT003'").count() == 1
    assert result.filter("agent_id = 'AGT003'").collect()[0]["is_current"]
