import argparse
import json
import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(
    os.environ.get(
        "DWH_PROJECT_ROOT",
        Path(__file__).resolve().parents[1],
    )
)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

from sqlalchemy import select, text

from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.models.xcom import XComModel
from airflow.utils.session import create_session


DAG_ID = "phase12_sales_e2e"
EXPECTED_TASKS = {
    "get_source",
    "extract",
    "failure_gate",
    "load",
    "data_quality",
    "dbt_build",
    "reconcile",
}


def _get_xcom(session, run_id: str, task_id: str):
    row = session.scalars(
        XComModel.get_many(
            run_id=run_id,
            key="return_value",
            task_ids=task_id,
            dag_ids=DAG_ID,
            map_indexes=-1,
            limit=1,
        )
    ).first()
    return XComModel.deserialize_value(row) if row is not None else None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--expect-retry", action="store_true")
    args = parser.parse_args()

    with create_session() as session:
        dag_run = session.scalars(
            select(DagRun)
            .where(
                DagRun.dag_id == DAG_ID,
                DagRun.run_id.like("manual__%"),
            )
            .order_by(DagRun.run_after.desc())
        ).first()
        if dag_run is None:
            print("FAIL | Không tìm thấy manual DAG run")
            return 1

        task_instances = session.scalars(
            select(TaskInstance)
            .where(
                TaskInstance.dag_id == DAG_ID,
                TaskInstance.run_id == dag_run.run_id,
            )
            .order_by(TaskInstance.task_id)
        ).all()
        load_payload = _get_xcom(session, dag_run.run_id, "load")
        dq_payload = _get_xcom(session, dag_run.run_id, "data_quality")
        reconcile_payload = _get_xcom(session, dag_run.run_id, "reconcile")

    task_states = {item.task_id: item.state for item in task_instances}
    task_tries = {item.task_id: item.try_number for item in task_instances}
    batch_id = (load_payload or {}).get("batch_id")

    from src.connections import get_connection, get_sqlalchemy_uri
    from sqlalchemy import create_engine

    engine = create_engine(get_sqlalchemy_uri(get_connection("dwh_postgres")))
    with engine.connect() as connection:
        dq_row = connection.execute(
            text("""
                SELECT status, total_rules, passed_rules, failed_rules, batch_id
                FROM meta.dq_validation_runs
                WHERE airflow_dag_id = :dag_id
                  AND airflow_dag_run_id = :run_id
                ORDER BY started_at DESC
                LIMIT 1
            """),
            {"dag_id": DAG_ID, "run_id": dag_run.run_id},
        ).mappings().first()
        registry_status = connection.execute(
            text("""
                SELECT status
                FROM meta._source_registry
                WHERE batch_id = :batch_id
            """),
            {"batch_id": batch_id},
        ).scalar_one_or_none()

    print("AIRFLOW PHASE 12 METADATA")
    print("Run ID =", dag_run.run_id)
    print("Run state =", dag_run.state)
    for item in task_instances:
        print(
            "Task =", item.task_id,
            "| state =", item.state,
            "| try_number =", item.try_number,
        )
    print("Load XCom =", json.dumps(load_payload, ensure_ascii=False, default=str))
    print("DQ XCom =", json.dumps(dq_payload, ensure_ascii=False, default=str))
    print(
        "Reconcile XCom =",
        json.dumps(reconcile_payload, ensure_ascii=False, default=str),
    )
    print("DQ metadata =", dict(dq_row) if dq_row else None)
    print("Registry status =", registry_status)

    checks = {
        "DAG run succeeded": dag_run.state == "success",
        "All expected tasks present": EXPECTED_TASKS.issubset(task_states),
        "All tasks succeeded": all(
            task_states.get(task_id) == "success" for task_id in EXPECTED_TASKS
        ),
        "Load XCom has batch_id": bool(batch_id),
        "Load XCom has minio_path": bool((load_payload or {}).get("minio_path")),
        "DQ XCom passed 9/9": (
            (dq_payload or {}).get("status") == "passed"
            and (dq_payload or {}).get("passed_rules") == 9
            and (dq_payload or {}).get("failed_rules") == 0
        ),
        "Reconciliation passed 15/15": (
            reconcile_payload == {"status": "passed", "checks": 15}
        ),
        "DQ metadata linked to batch": (
            dq_row is not None
            and dq_row["status"] == "passed"
            and dq_row["passed_rules"] == 9
            and dq_row["batch_id"] == batch_id
        ),
        "Registry batch loaded": registry_status == "loaded",
        "Failure gate retry observed": (
            task_tries.get("failure_gate", 0) >= (2 if args.expect_retry else 1)
        ),
    }
    for name, passed in checks.items():
        print(("PASS" if passed else "FAIL"), "|", name)

    success = all(checks.values())
    print("Final status =", "PASSED" if success else "FAILED")
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
