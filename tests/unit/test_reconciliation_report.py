from commissions_pipeline.aggregations import reconciliation_report


def _seed(spark, tmp_path):
    gold_dir = f"{tmp_path}/gold"
    silver_dir = f"{tmp_path}/silver"

    calculated = spark.createDataFrame(
        [
            ("AGT001", "Alice Agent", "2025-01", 500.0),  # matches payment exactly
            ("AGT002", "Bob Agent", "2025-01", 300.0),  # off by more than tolerance -> BREAK
            ("AGT003", "Cara Agent", "2025-01", 200.0),  # calculated but never paid
        ],
        ["agent_id", "agent_name", "pay_period", "net_commission"],
    )
    calculated.write.format("delta").mode("overwrite").save(f"{gold_dir}/agent_commission_summary")

    paid = spark.createDataFrame(
        [
            ("AGT001", "2025-01", 500.0, "Paid"),
            ("AGT002", "2025-01", 350.0, "Paid"),  # $50 discrepancy vs. calculated 300.0
            ("AGT004", "2025-01", 100.0, "Paid"),  # paid but never calculated
        ],
        ["agent_id", "pay_period", "net_payment_amount", "payment_status"],
    )
    paid.write.format("delta").mode("overwrite").save(f"{silver_dir}/fact_payments")

    return silver_dir, gold_dir


def test_reconciliation_flags_breaks_and_orphans(spark, tmp_path):
    silver_dir, gold_dir = _seed(spark, tmp_path)
    df = reconciliation_report.transform(spark, silver_dir, gold_dir)

    statuses = {r["agent_id"]: r["reconciliation_status"] for r in df.collect()}
    assert statuses["AGT001"] == "MATCHED"
    assert statuses["AGT002"] == "BREAK"
    assert statuses["AGT003"] == "CALCULATED_BUT_NOT_PAID"
    assert statuses["AGT004"] == "PAID_WITHOUT_CALCULATION"


def test_reconciliation_variance_is_paid_minus_calculated(spark, tmp_path):
    silver_dir, gold_dir = _seed(spark, tmp_path)
    df = reconciliation_report.transform(spark, silver_dir, gold_dir)

    row = df.filter("agent_id = 'AGT002'").collect()[0]
    assert round(row["variance"], 2) == 50.0
