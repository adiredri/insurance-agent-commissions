from commissions_pipeline.transformations import fact_commission_transactions


def _write_bronze_transactions(spark, tmp_path):
    rows = [
        ("TXN001", "POL001", "AGT001", "AUTO", "New Business", "2025-01-15", 1000.0, 0.12, 120.0),
        (
            "TXN002",
            "POL002",
            "AGT001",
            "AUTO",
            "New Business",
            "2025-01-20",
            1000.0,
            0.12,
            -120.0,
        ),  # fat-finger sign
        ("TXN003", "POL003", "AGT002", "HOME", "Renewal", "2025-02-01", 2000.0, 0.06, 120.0),
    ]
    cols = [
        "transaction_id",
        "policy_id",
        "agent_id",
        "product_id",
        "transaction_type",
        "transaction_date",
        "premium_amount",
        "commission_rate",
        "commission_amount",
    ]
    df = spark.createDataFrame(rows, cols).withColumn(
        "_ingested_at", spark.sql("select current_timestamp()").collect()[0][0]
    )
    path = f"{tmp_path}/bronze/commission_transactions"
    df.write.format("delta").mode("overwrite").save(path)
    return f"{tmp_path}/bronze"


def test_negative_commission_amount_is_quarantined_not_dropped_silently(spark, tmp_path):
    bronze_dir = _write_bronze_transactions(spark, tmp_path)
    clean, quarantined = fact_commission_transactions.transform(spark, bronze_dir)

    assert clean.count() == 2
    assert quarantined.count() == 1
    assert quarantined.collect()[0]["transaction_id"] == "TXN002"


def test_pay_period_is_derived_from_transaction_date(spark, tmp_path):
    bronze_dir = _write_bronze_transactions(spark, tmp_path)
    clean, _ = fact_commission_transactions.transform(spark, bronze_dir)

    periods = {r["transaction_id"]: r["pay_period"] for r in clean.collect()}
    assert periods["TXN001"] == "2025-01"
    assert periods["TXN003"] == "2025-02"
