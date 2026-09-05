# Architecture

## Medallion pipeline

```mermaid
flowchart LR
    subgraph Source["Simulated source systems"]
        AMS["Agency Mgmt System\n(agents, agencies)"]
        PAS["Policy Admin System\n(products, policies)"]
        CE["Commission Engine\n(plans, transactions, chargebacks)"]
        FIN["Finance System\n(payments)"]
    end

    AMS --> RAW[("Raw landing\nCSV, data/raw or ADLS raw/")]
    PAS --> RAW
    CE --> RAW
    FIN --> RAW

    RAW --> BRONZE[("Bronze\nDelta, 1:1 with source + lineage cols")]
    BRONZE --> SILVER[("Silver\ncleaned, deduped, typed, SCD2, DQ-checked")]
    SILVER --> GOLD[("Gold\nbusiness marts")]

    GOLD --> M1[agent_commission_summary]
    GOLD --> M2[payment_run_summary]
    GOLD --> M3[chargeback_exposure]
    GOLD --> M4[agent_leaderboard]
    GOLD --> M5[reconciliation_report]

    M1 & M2 & M3 & M4 & M5 --> BI["BI / Databricks SQL / Power BI"]
```

## Why medallion + Delta

- **Bronze is dumb on purpose.** No cleaning, no dedup — just raw source data landed with lineage columns (`_ingested_at`, `_source_file`). If a bug is found in a downstream transformation next month, Silver can be rebuilt from Bronze without re-pulling from source systems.
- **Silver is where trust is built.** Deduplication, type coercion, enum validation, and the SCD2 `dim_agent` all live here, gated by the data-quality framework in [`src/commissions_pipeline/dq`](../src/commissions_pipeline/dq). Rows that fail a hard check are quarantined to `silver/_quarantine/<table>` rather than silently dropped or silently "fixed."
- **Gold is business logic, not more cleaning.** Aggregations, the agent leaderboard, chargeback-risk scoring, and the finance reconciliation report all read only from Silver.
- **Delta Lake** gives ACID writes, time travel, and schema enforcement — the same format whether you're running this on a laptop, in Docker, or on Databricks with Unity Catalog.

## Cloud deployment target (Azure)

```mermaid
flowchart TB
    subgraph Azure["Azure Subscription"]
        subgraph RG["Resource Group"]
            ADLS[("ADLS Gen2\nHierarchical namespace\nraw + lakehouse containers")]
            KV["Key Vault\nsecrets, service principal creds"]
            DBX["Azure Databricks Workspace\n(Premium — Unity Catalog)"]
        end
    end

    GHA["GitHub Actions"] -- "OIDC (no stored secrets)" --> Azure
    GHA -- "Databricks Asset Bundle deploy" --> DBX
    DBX -- "Storage Blob Data Contributor\n(managed identity)" --> ADLS
    DBX -. "secret scope backed by" .-> KV
```

Infra is defined once in [`infra/terraform`](../infra/terraform) and deployed identically to dev/staging/prod by varying `-var-file`. The Databricks *jobs* (the actual bronze/silver/gold tasks and their schedule) are defined separately as a [Databricks Asset Bundle](../databricks.yml) — infra and workload are deployed independently, which is the standard split on a real data platform team.

## Local development

Same code, same Delta format, zero cloud dependency:

```mermaid
flowchart LR
    Gen["generate_synthetic_data.py"] --> Raw["data/raw (CSV)"]
    Raw --> BronzeJob["bronze_ingest.py"] --> Bronze["data/bronze (Delta)"]
    Bronze --> SilverJob["run_silver.py"] --> Silver["data/silver (Delta)"]
    Silver --> GoldJob["run_gold.py"] --> Gold["data/gold (Delta)"]
```

Setting `LAKEHOUSE_ROOT=abfss://lakehouse@<storage>.dfs.core.windows.net` (or a Unity Catalog volume path) points the exact same code at Azure — see [`config.py`](../src/commissions_pipeline/config.py).
