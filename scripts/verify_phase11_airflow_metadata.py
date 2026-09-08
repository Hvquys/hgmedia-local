import json
import sys

from sqlalchemy import select

from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.models.xcom import XComModel
from airflow.utils.session import create_session


DAG_ID = "el_csv_pipeline"
EXPECTED_TASKS = {"get_sources", "extract", "load"}


def main() -> int:
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
            .order_by(TaskInstance.task_id, TaskInstance.map_index)
        ).all()

        print("AIRFLOW PHASE 11 METADATA")
        print("Run ID =", dag_run.run_id)
        print("Run state =", dag_run.state)

        task_ids = set()
        all_tasks_succeeded = True
        for task_instance in task_instances:
            task_ids.add(task_instance.task_id)
            all_tasks_succeeded &= task_instance.state == "success"
            print(
                "Task =",
                task_instance.task_id,
                "| map_index =",
                task_instance.map_index,
                "| state =",
                task_instance.state,
                "| try_number =",
                task_instance.try_number,
            )

        extract_xcom = session.scalars(
            XComModel.get_many(
                run_id=dag_run.run_id,
                key="return_value",
                task_ids="extract",
                dag_ids=DAG_ID,
                map_indexes=0,
                limit=1,
            )
        ).first()
        extract_payload = (
            XComModel.deserialize_value(extract_xcom)
            if extract_xcom is not None
            else None
        )

    payload_text = json.dumps(
        extract_payload,
        ensure_ascii=False,
        default=str,
    )
    has_batch_id = "batch_id" in payload_text
    has_minio_path = "minio_path" in payload_text
    tasks_complete = EXPECTED_TASKS.issubset(task_ids)

    print("XCom extract =", payload_text)
    print("Expected tasks present =", tasks_complete)
    print("All task instances succeeded =", all_tasks_succeeded)
    print("XCom has batch_id =", has_batch_id)
    print("XCom has minio_path =", has_minio_path)

    success = all(
        [
            dag_run.state == "success",
            tasks_complete,
            all_tasks_succeeded,
            has_batch_id,
            has_minio_path,
        ]
    )
    print("Final status =", "PASSED" if success else "FAILED")
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
