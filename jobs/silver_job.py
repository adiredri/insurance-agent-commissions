"""Databricks job entrypoint for the Silver transformation task."""

from commissions_pipeline.transformations.run_silver import main

if __name__ == "__main__":
    main()
