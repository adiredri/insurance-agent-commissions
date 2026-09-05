# Runs the full generate -> bronze -> silver -> gold pipeline locally.
# Usage: .\scripts\run_pipeline.ps1 [numAgents] [numPolicies]
param(
    [int]$NumAgents = 300,
    [int]$NumPolicies = 15000
)

$ErrorActionPreference = "Stop"
$env:PYTHONPATH = "src"

Write-Host "== Generating synthetic data ($NumAgents agents, $NumPolicies policies) =="
python -m commissions_pipeline.ingestion.generate_synthetic_data `
    --num-agents $NumAgents --num-policies $NumPolicies --years 2 --seed 42 --output-dir data/raw

Write-Host "== Bronze ingest =="
python -m commissions_pipeline.ingestion.bronze_ingest --raw-dir data/raw --out-dir data/bronze

Write-Host "== Silver transform =="
python -m commissions_pipeline.transformations.run_silver --bronze-dir data/bronze --silver-dir data/silver

Write-Host "== Gold aggregate =="
python -m commissions_pipeline.aggregations.run_gold --silver-dir data/silver --gold-dir data/gold

Write-Host "== Done. Gold tables written to data/gold/ =="
