#!/usr/bin/env bash
set -euo pipefail

project_root="${DWH_PROJECT_ROOT:-/opt/airflow/project}"
conf_file="${project_root}/config/airflow_phase11_run.json"

airflow dags unpause -y el_csv_pipeline || true
airflow dags trigger \
  --conf "$(cat "${conf_file}")" \
  el_csv_pipeline
