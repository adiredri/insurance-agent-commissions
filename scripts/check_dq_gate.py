"""CI gate: fail the build if any error-severity DQ check failed in the last Silver run.

Usage: python scripts/check_dq_gate.py [--dq-log-path data/silver/_dq_log]
"""

from __future__ import annotations

import argparse
import sys

from commissions_pipeline.utils.spark_session import get_spark


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dq-log-path", default="data/silver/_dq_log")
    args = parser.parse_args()

    spark = get_spark("ci-dq-gate")
    df = spark.read.format("delta").load(args.dq_log_path)
    failed = df.filter("passed = false and severity = 'error'")
    count = failed.count()

    if count > 0:
        failed.show(truncate=False)
        print(f"{count} error-severity DQ check(s) failed", file=sys.stderr)
        sys.exit(1)

    print("All error-severity DQ checks passed")


if __name__ == "__main__":
    main()
