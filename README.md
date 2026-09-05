# Insurance Agent Commissions & Payments Platform

A production-shaped lakehouse pipeline that simulates and processes commission
payments for an insurance company's independent sales agents — new business
and renewal commissions, upline overrides, chargebacks on early policy
cancellations, and reconciliation between the commission engine and the
finance payment system.

Built as an end-to-end senior data engineering portfolio project: synthetic
data generation → medallion architecture (Bronze/Silver/Gold) on PySpark +
Delta Lake → data quality gates → CI/CD → infrastructure as code for Azure +
Databricks.

## Why this domain

Insurance commissions are a genuinely hard data problem, not a toy dataset:
tiered rates that change over time, an agent hierarchy that pays overrides
upstream, money that has to be *clawed back* when a policy cancels early, and
two independent systems (the commission engine and finance) that need to
reconcile to the penny every pay period. That last part — [`reconciliation_report`](docs/data_dictionary.md#goldreconciliation_report)
— is the table a real commissions team would actually watch.

## Architecture

See [`docs/architecture.md`](docs/architecture.md) for the full diagram. In short:

```
raw source CSVs → Bronze (Delta, as-is + lineage) → Silver (cleaned, deduped, SCD2, DQ-gated) → Gold (business marts)
```

The same code runs identically on a laptop (writing to `data/`) or on
Databricks against ADLS (writing to `abfss://...`) — see `LAKEHOUSE_ROOT` in
[`src/commissions_pipeline/config.py`](src/commissions_pipeline/config.py).

## Tech stack

| Layer | Tool |
|---|---|
| Processing | PySpark 3.5, Delta Lake |
| Orchestration (cloud) | Databricks Jobs, deployed via Databricks Asset Bundles |
| Cloud platform | Azure (ADLS Gen2, Azure Databricks, Key Vault) |
| Infrastructure as Code | Terraform |
| CI/CD | GitHub Actions (lint, test, DQ gate, bundle deploy, Terraform plan/apply) |
| Data quality | Custom lightweight expectations framework (see [`src/commissions_pipeline/dq`](src/commissions_pipeline/dq)) |
| Testing | pytest, local Spark |
| Local dev | Docker / docker-compose |

## Repo structure

```
src/commissions_pipeline/
  config.py                  # paths + business rule constants, single source of truth
  ingestion/                 # synthetic data generator + Bronze ingest
  transformations/           # Silver: cleaning, dedup, SCD2, DQ checks
  aggregations/               # Gold: commission summary, leaderboard, reconciliation, ...
  dq/                         # dependency-free data-quality check framework
  utils/                      # Spark session factory, Delta upsert/SCD2 helpers
jobs/                         # thin Databricks job entrypoints
resources/, databricks.yml    # Databricks Asset Bundle (job + schedule definitions)
infra/terraform/              # Azure resources: ADLS Gen2, Databricks workspace, Key Vault
docker/                       # local dev environment (avoids Windows/Spark friction entirely)
tests/unit/                   # pytest — generator business rules + transformation logic
docs/                         # architecture, ERD, data dictionary
.github/workflows/            # CI, CD-Databricks, CD-Terraform
```

## Getting started

### Option A — Docker (recommended, especially on Windows)

PySpark on native Windows needs `winutils.exe`/Hadoop native libraries to
behave; Docker sidesteps that entirely.

```bash
docker compose -f docker/docker-compose.yml run --rm pipeline bash
```

Then inside the container:

```bash
python -m commissions_pipeline.ingestion.generate_synthetic_data --num-agents 300 --num-policies 15000
python -m commissions_pipeline.ingestion.bronze_ingest
python -m commissions_pipeline.transformations.run_silver
python -m commissions_pipeline.aggregations.run_gold
pytest tests/unit -v
```

Or launch Jupyter for exploration: `docker compose -f docker/docker-compose.yml up jupyter` → http://localhost:8888

### Option B — native Python (Linux/macOS, or Windows with a JDK + Hadoop winutils already configured)

```bash
python -m venv .venv && source .venv/bin/activate   # .venv\Scripts\activate on Windows
pip install -r requirements-dev.txt
export PYTHONPATH=src   # $env:PYTHONPATH="src" on PowerShell

python -m commissions_pipeline.ingestion.generate_synthetic_data
python -m commissions_pipeline.ingestion.bronze_ingest
python -m commissions_pipeline.transformations.run_silver
python -m commissions_pipeline.aggregations.run_gold
```

Each stage is a separate, idempotent command — re-run any of them independently
once its upstream input exists. Or run all four in one shot with
`scripts/run_pipeline.sh` (bash) / `scripts/run_pipeline.ps1` (PowerShell).

## Data quality

Every Silver transformation runs a set of `Check`s (not-null, uniqueness,
positive-value, enum-membership) via [`dq/expectations.py`](src/commissions_pipeline/dq/expectations.py).
`error`-severity failures raise and stop the pipeline (this is what fails CI);
`warn`-severity failures are logged but don't block. All results — pass or
fail — are appended to `silver/_dq_log` for trend monitoring. Rows that fail
a hard check are written to `silver/_quarantine/<table>` instead of being
silently dropped.

The synthetic generator deliberately injects realistic messiness — duplicate
re-extracted rows, a few null premiums, a couple of sign-flipped commission
amounts, and a handful of payment/commission reconciliation breaks — so these
checks (and the reconciliation report) have real problems to catch, not a
clean toy dataset.

## Testing & CI

`pytest tests/unit` covers the generator's business rules (pure pandas, no
Spark needed) and the Silver/Gold transformation logic (local Spark + Delta,
using small in-memory fixtures). GitHub Actions (`.github/workflows/ci.yml`)
runs lint → unit tests → a full end-to-end smoke test on a small synthetic
dataset → a DQ gate that fails the build on any error-severity check failure.

## Cloud deployment (optional)

This runs entirely for free locally. To also deploy to Azure for a portfolio
demo:

1. **Provision infra**: `cd infra/terraform && terraform init && terraform apply -var-file=environments/dev.tfvars` (see [`infra/terraform`](infra/terraform)). Uses OIDC auth in CI — no stored Azure secrets.
2. **Deploy the pipeline**: `databricks bundle deploy -t dev` (see [`databricks.yml`](databricks.yml)) — creates the scheduled Databricks Job with bronze/silver/gold tasks.
3. **Tear down** when you're done demoing: `terraform destroy -var-file=environments/dev.tfvars` — avoid ongoing Azure charges.

`.github/workflows/cd-infra.yml` and `cd-databricks.yml` automate both, with a
required-reviewer GitHub Environment gating anything hitting `prod`.

## License

MIT — this is a portfolio/learning project, not insurance advice or a real payments system.
