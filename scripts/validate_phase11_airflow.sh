#!/usr/bin/env bash
set -euo pipefail

project_root="${DWH_PROJECT_ROOT:-/opt/airflow/project}"

cd "${project_root}/dwh_dbt"
dbt run \
  --profiles-dir . \
  --threads 1 \
  --select fact_phase6_sales mart_phase6_daily_sales

cd "${project_root}"
python scripts/verify_phase11_airflow_metadata.py
python scripts/run_data_quality.py --id phase6_sales
python scripts/reconcile_phase6_sales.py
