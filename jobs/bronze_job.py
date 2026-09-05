"""Databricks job entrypoint for the Bronze ingestion task."""

from commissions_pipeline.ingestion.bronze_ingest import main

if __name__ == "__main__":
    main()
