#!/usr/bin/env bash
# Runs the full generate -> bronze -> silver -> gold pipeline locally.
# Usage: scripts/run_pipeline.sh [num_agents] [num_policies]
set -euo pipefail

NUM_AGENTS="${1:-300}"
NUM_POLICIES="${2:-15000}"

export PYTHONPATH=src

echo "== Generating synthetic data ($NUM_AGENTS agents, $NUM_POLICIES policies) =="
python -m commissions_pipeline.ingestion.generate_synthetic_data \
    --num-agents "$NUM_AGENTS" --num-policies "$NUM_POLICIES" --years 2 --seed 42 --output-dir data/raw

echo "== Bronze ingest =="
python -m commissions_pipeline.ingestion.bronze_ingest --raw-dir data/raw --out-dir data/bronze

echo "== Silver transform =="
python -m commissions_pipeline.transformations.run_silver --bronze-dir data/bronze --silver-dir data/silver

echo "== Gold aggregate =="
python -m commissions_pipeline.aggregations.run_gold --silver-dir data/silver --gold-dir data/gold

echo "== Done. Gold tables written to data/gold/ =="
