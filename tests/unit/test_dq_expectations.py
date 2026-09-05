import pytest

from commissions_pipeline.dq.expectations import (
    Check,
    DataQualityError,
    in_set,
    not_null,
    positive,
    run_checks,
    unique,
)


def test_not_null_catches_nulls(spark):
    df = spark.createDataFrame([(1, "a"), (2, None), (3, "c")], ["id", "val"])
    result = run_checks(
        spark,
        df,
        [Check("val_not_null", "val must not be null", not_null("val"), severity="warn")],
        table_name="t",
        fail_on_error=False,
    )
    assert result[0].failing_rows == 1
    assert not result[0].passed


def test_unique_catches_duplicates(spark):
    df = spark.createDataFrame([(1,), (1,), (2,)], ["id"])
    result = run_checks(
        spark,
        df,
        [Check("id_unique", "id must be unique", unique("id"), severity="warn")],
        table_name="t",
        fail_on_error=False,
    )
    assert result[0].failing_rows == 2  # both rows sharing the duplicate key are flagged


def test_positive_catches_zero_and_negative(spark):
    df = spark.createDataFrame([(1, 10.0), (2, 0.0), (3, -5.0)], ["id", "amount"])
    result = run_checks(
        spark,
        df,
        [Check("amount_positive", "amount must be positive", positive("amount"), severity="warn")],
        table_name="t",
        fail_on_error=False,
    )
    assert result[0].failing_rows == 2


def test_in_set_catches_invalid_enum_values(spark):
    df = spark.createDataFrame([(1, "Paid"), (2, "Bogus")], ["id", "status"])
    result = run_checks(
        spark,
        df,
        [
            Check(
                "status_valid",
                "status must be valid",
                in_set("status", {"Paid", "Held"}),
                severity="warn",
            )
        ],
        table_name="t",
        fail_on_error=False,
    )
    assert result[0].failing_rows == 1


def test_error_severity_check_raises_by_default(spark):
    df = spark.createDataFrame([(1,), (None,)], ["id"])
    with pytest.raises(DataQualityError):
        run_checks(
            spark,
            df,
            [Check("id_not_null", "id must not be null", not_null("id"), severity="error")],
            table_name="t",
        )


def test_warn_severity_check_does_not_raise(spark):
    df = spark.createDataFrame([(1,), (None,)], ["id"])
    results = run_checks(
        spark,
        df,
        [Check("id_not_null", "id must not be null", not_null("id"), severity="warn")],
        table_name="t",
    )
    assert not results[0].passed  # recorded as failed, but didn't raise
