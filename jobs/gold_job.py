"""Databricks job entrypoint for the Gold aggregation task."""

from commissions_pipeline.aggregations.run_gold import main

if __name__ == "__main__":
    main()
