import json
from datetime import datetime, timezone

from sqlalchemy import create_engine, text

from src.connections import get_connection, get_sqlalchemy_uri


class DataQualityRepository:
    def __init__(self):
        conn_cfg = get_connection("dwh_postgres")
        self.engine = create_engine(get_sqlalchemy_uri(conn_cfg))

    def create_run(
        self,
        validation_run_id: str,
        airflow_dag_id: str,
        airflow_dag_run_id: str,
        source_id: str,
        target_table: str,
        batch_id: str | None,
        status: str,
        config_hash: str | None,
    ) -> None:
        query = text("""
            INSERT INTO meta.dq_validation_runs (
                validation_run_id,
                airflow_dag_id,
                airflow_dag_run_id,
                source_id,
                target_table,
                batch_id,
                status,
                started_at,
                config_hash
            )
            VALUES (
                CAST(:validation_run_id AS uuid),
                :airflow_dag_id,
                :airflow_dag_run_id,
                :source_id,
                :target_table,
                :batch_id,
                :status,
                :started_at,
                :config_hash
            )
        """)

        with self.engine.begin() as conn:
            conn.execute(query, {
                "validation_run_id": validation_run_id,
                "airflow_dag_id": airflow_dag_id,
                "airflow_dag_run_id": airflow_dag_run_id,
                "source_id": source_id,
                "target_table": target_table,
                "batch_id": batch_id,
                "status": status,
                "started_at": datetime.now(timezone.utc).replace(tzinfo=None),
                "config_hash": config_hash,
            })

    def save_rule_results(
        self,
        validation_run_id: str,
        source_id: str,
        results: list[dict],
    ) -> None:
        if not results:
            return

        query = text("""
            INSERT INTO meta.dq_validation_results (
                validation_run_id,
                source_id,
                rule_id,
                expectation_type,
                column_name,
                severity,
                success,
                observed_value,
                unexpected_count,
                unexpected_percent,
                result_json
            )
            VALUES (
                CAST(:validation_run_id AS uuid),
                :source_id,
                :rule_id,
                :expectation_type,
                :column_name,
                :severity,
                :success,
                :observed_value,
                :unexpected_count,
                :unexpected_percent,
                CAST(:result_json AS jsonb)
            )
        """)

        payload = []
        for result in results:
            payload.append({
                "validation_run_id": validation_run_id,
                "source_id": source_id,
                "rule_id": result["rule_id"],
                "expectation_type": result["expectation_type"],
                "column_name": result.get("column_name"),
                "severity": result["severity"],
                "success": result["success"],
                "observed_value": result.get("observed_value"),
                "unexpected_count": result.get("unexpected_count"),
                "unexpected_percent": result.get("unexpected_percent"),
                "result_json": json.dumps(
                    result.get("raw_result", {}),
                    ensure_ascii=False,
                    default=str,
                ),
            })

        with self.engine.begin() as conn:
            conn.execute(query, payload)

    def finish_run(
        self,
        validation_run_id: str,
        status: str,
        total_rules: int,
        passed_rules: int,
        failed_rules: int,
        error_message: str | None = None,
    ) -> None:
        success_percent = (
            round((passed_rules / total_rules) * 100, 2)
            if total_rules
            else 0
        )

        query = text("""
            UPDATE meta.dq_validation_runs
            SET
                status = :status,
                total_rules = :total_rules,
                passed_rules = :passed_rules,
                failed_rules = :failed_rules,
                success_percent = :success_percent,
                finished_at = :finished_at,
                error_message = :error_message
            WHERE validation_run_id = CAST(:validation_run_id AS uuid)
        """)

        with self.engine.begin() as conn:
            conn.execute(query, {
                "validation_run_id": validation_run_id,
                "status": status,
                "total_rules": total_rules,
                "passed_rules": passed_rules,
                "failed_rules": failed_rules,
                "success_percent": success_percent,
                "finished_at": datetime.now(timezone.utc).replace(tzinfo=None),
                "error_message": error_message,
            })
