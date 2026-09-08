"""Airflow end-to-end vertical slice for the local phase6_sales source."""

import os
import sys
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag, get_current_context, task


PROJECT_ROOT = os.environ.get("DWH_PROJECT_ROOT", "/opt/airflow/project")
DBT_DIR = os.path.join(PROJECT_ROOT, "dwh_dbt")
sys.path.insert(0, PROJECT_ROOT)


@dag(
    dag_id="phase12_sales_e2e",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(hours=1),
    default_args={
        "owner": "data-team",
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
        "execution_timeout": timedelta(minutes=30),
    },
    params={
        "force": Param(default=True, type="boolean", title="Buộc tạo batch mới"),
        "simulate_failure_once": Param(
            default=False,
            type="boolean",
            title="Mô phỏng lỗi một lần để kiểm tra retry",
        ),
    },
    tags=["phase12", "e2e", "csv", "data-quality"],
)
def phase12_sales_e2e():
    @task
    def get_source() -> dict:
        os.chdir(PROJECT_ROOT)
        from src.config_loader import get_source_by_id

        return get_source_by_id("phase6_sales")

    @task
    def extract(source_config: dict) -> dict:
        os.chdir(PROJECT_ROOT)
        from src.tasks.extract_task import run_extract

        context = get_current_context()
        result = run_extract(
            source_config,
            force=bool(context["params"].get("force", True)),
        )
        if result is None:
            raise RuntimeError(
                "Phase 12 cần một batch mới; hãy trigger với force=true."
            )
        return {"source_config": source_config, "extract_result": result}

    @task(retries=1, retry_delay=timedelta(seconds=10))
    def failure_gate(payload: dict) -> dict:
        context = get_current_context()
        should_fail = bool(
            context["params"].get("simulate_failure_once", False)
        )
        if should_fail and context["ti"].try_number == 1:
            raise RuntimeError("Phase 12 simulated transient failure")
        return payload

    @task
    def load(payload: dict) -> dict:
        os.chdir(PROJECT_ROOT)
        from src.tasks.load_task import run_load

        source_config = payload["source_config"]
        extract_result = payload["extract_result"]
        row_count = run_load(
            source_config,
            extract_result["batch_id"],
            extract_result["minio_path"],
            load_mode=extract_result.get("load_mode_override"),
        )
        return {
            "source_id": source_config["source_id"],
            "target_table": source_config["target_staging_table"],
            "batch_id": extract_result["batch_id"],
            "minio_path": extract_result["minio_path"],
            "row_count": row_count,
            "status": "loaded",
        }

    @task
    def data_quality(load_result: dict) -> dict:
        os.chdir(PROJECT_ROOT)
        from src.tasks.data_quality_task import run_data_quality

        context = get_current_context()
        return run_data_quality(
            load_result=load_result,
            airflow_dag_id=context["dag_run"].dag_id,
            airflow_dag_run_id=context["dag_run"].run_id,
        )

    @task
    def reconcile() -> dict:
        os.chdir(PROJECT_ROOT)
        from scripts.reconcile_phase6_sales import main

        if main():
            raise RuntimeError("Phase 12 reconciliation failed")
        return {"status": "passed", "checks": 15}

    source = get_source()
    extracted = extract(source)
    recovered = failure_gate(extracted)
    loaded = load(recovered)
    quality_result = data_quality(loaded)
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt build --profiles-dir . --threads 1 "
            "--select fact_phase6_sales mart_phase6_daily_sales"
        ),
        env={**os.environ},
    )
    reconciled = reconcile()
    quality_result >> dbt_build >> reconciled


phase12_sales_e2e()
