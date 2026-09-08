#!/usr/bin/env bash
set -euo pipefail

project_root="${DWH_PROJECT_ROOT:-/opt/airflow/project}"
mode="${1:-success}"

case "${mode}" in
  success|recovery)
    conf_file="${project_root}/config/airflow_phase12_${mode}.json"
    ;;
  *)
    echo "Usage: $0 [success|recovery]" >&2
    exit 2
    ;;
esac

airflow dags unpause -y phase12_sales_e2e || true
airflow dags trigger \
  --conf "$(cat "${conf_file}")" \
  phase12_sales_e2e
