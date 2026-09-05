"""Databricks job entrypoint — thin wrapper so notebooks/jobs don't import internals directly."""

from commissions_pipeline.ingestion.generate_synthetic_data import main

if __name__ == "__main__":
    main()
