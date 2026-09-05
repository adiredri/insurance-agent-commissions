"""Lightweight data-quality framework.

Deliberately dependency-free (no Great Expectations / DLT) so it runs
identically in local pytest, Docker, and Databricks. Each `Check` is a named
predicate over a DataFrame; `run_checks` evaluates them all, writes a
DQ result log (useful as a Gold table you can dashboard), and raises if any
`error`-severity check fails — which is what should stop a pipeline in CI/CD.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Literal

from pyspark.sql import DataFrame, SparkSession

Severity = Literal["error", "warn"]


@dataclass
class Check:
    name: str
    description: str
    predicate: Callable[[DataFrame], DataFrame]  # returns the FAILING rows
    severity: Severity = "error"


@dataclass
class CheckResult:
    check_name: str
    description: str
    severity: Severity
    failing_rows: int
    total_rows: int
    passed: bool


class DataQualityError(Exception):
    pass


def not_null(col: str) -> Callable[[DataFrame], DataFrame]:
    return lambda df: df.filter(df[col].isNull())


def unique(col: str) -> Callable[[DataFrame], DataFrame]:
    def _f(df: DataFrame) -> DataFrame:
        dupes = df.groupBy(col).count().filter("count > 1").select(col)
        return df.join(dupes, on=col, how="inner")

    return _f


def positive(col: str) -> Callable[[DataFrame], DataFrame]:
    return lambda df: df.filter((df[col].isNotNull()) & (df[col] <= 0))


def in_set(col: str, allowed: set[str]) -> Callable[[DataFrame], DataFrame]:
    return lambda df: df.filter(~df[col].isin(list(allowed)) & df[col].isNotNull())


def run_checks(
    spark: SparkSession,
    df: DataFrame,
    checks: list[Check],
    table_name: str,
    dq_log_path: str | None = None,
    fail_on_error: bool = True,
) -> list[CheckResult]:
    total = df.count()
    results: list[CheckResult] = []

    for check in checks:
        failing = check.predicate(df)
        failing_count = failing.count()
        results.append(
            CheckResult(
                check_name=check.name,
                description=check.description,
                severity=check.severity,
                failing_rows=failing_count,
                total_rows=total,
                passed=failing_count == 0,
            )
        )

    if dq_log_path:
        run_ts = datetime.now(timezone.utc).isoformat()
        rows = [
            (
                table_name,
                r.check_name,
                r.description,
                r.severity,
                r.failing_rows,
                r.total_rows,
                r.passed,
                run_ts,
            )
            for r in results
        ]
        log_df = spark.createDataFrame(
            rows,
            schema="table_name string, check_name string, description string, severity string, "
            "failing_rows long, total_rows long, passed boolean, run_timestamp string",
        )
        log_df.write.format("delta").mode("append").save(dq_log_path)

    failed_errors = [r for r in results if not r.passed and r.severity == "error"]
    if fail_on_error and failed_errors:
        summary = "; ".join(
            f"{r.check_name} ({r.failing_rows}/{r.total_rows} rows failed)" for r in failed_errors
        )
        raise DataQualityError(f"Data quality checks failed for '{table_name}': {summary}")

    return results
