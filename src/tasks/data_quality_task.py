import hashlib
import json
import logging
from uuid import uuid4

from src.data_quality.gx_validator import load_staging_dataframe, validate_dataframe
from src.data_quality.result_repository import DataQualityRepository
from src.data_quality.rule_loader import get_source_rules


logger = logging.getLogger(__name__)


class DataQualityError(RuntimeError):
    """Raised when a blocking data-quality rule fails."""


def _config_hash(source_rules: dict) -> str:
    payload = json.dumps(
        source_rules,
        ensure_ascii=False,
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def run_data_quality(
    load_result: dict,
    airflow_dag_id: str,
    airflow_dag_run_id: str,
) -> dict:
    source_id = load_result["source_id"]
    target_table = load_result["target_table"]
    batch_id = load_result.get("batch_id")
    validation_run_id = str(uuid4())
    repository = DataQualityRepository()

    if load_result["status"] != "loaded":
        repository.create_run(
            validation_run_id=validation_run_id,
            airflow_dag_id=airflow_dag_id,
            airflow_dag_run_id=airflow_dag_run_id,
            source_id=source_id,
            target_table=target_table,
            batch_id=batch_id,
            status="skipped",
            config_hash=None,
        )
        repository.finish_run(
            validation_run_id=validation_run_id,
            status="skipped",
            total_rules=0,
            passed_rules=0,
            failed_rules=0,
        )
        return {
            "source_id": source_id,
            "batch_id": batch_id,
            "status": "skipped",
            "validation_run_id": validation_run_id,
        }

    source_rules = get_source_rules(source_id)
    if not source_rules.get("enabled", True):
        repository.create_run(
            validation_run_id=validation_run_id,
            airflow_dag_id=airflow_dag_id,
            airflow_dag_run_id=airflow_dag_run_id,
            source_id=source_id,
            target_table=target_table,
            batch_id=batch_id,
            status="disabled",
            config_hash=None,
        )
        repository.finish_run(
            validation_run_id=validation_run_id,
            status="disabled",
            total_rules=0,
            passed_rules=0,
            failed_rules=0,
        )
        return {
            "source_id": source_id,
            "batch_id": batch_id,
            "status": "disabled",
            "validation_run_id": validation_run_id,
        }

    configured_table = source_rules.get("target_table")
    if configured_table != target_table:
        raise ValueError(
            f"[{source_id}] target_table không khớp: "
            f"DAG={target_table}, DQ config={configured_table}"
        )

    rules = source_rules.get("rules", [])
    config_hash = _config_hash(source_rules)
    repository.create_run(
        validation_run_id=validation_run_id,
        airflow_dag_id=airflow_dag_id,
        airflow_dag_run_id=airflow_dag_run_id,
        source_id=source_id,
        target_table=target_table,
        batch_id=batch_id,
        status="running",
        config_hash=config_hash,
    )

    try:
        dataframe = load_staging_dataframe(target_table)
        results = validate_dataframe(
            dataframe=dataframe,
            source_id=source_id,
            rules=rules,
        )
        repository.save_rule_results(
            validation_run_id=validation_run_id,
            source_id=source_id,
            results=results,
        )
    except Exception as error:
        repository.finish_run(
            validation_run_id=validation_run_id,
            status="error",
            total_rules=0,
            passed_rules=0,
            failed_rules=0,
            error_message=str(error),
        )
        raise

    total_rules = len(results)
    passed_rules = sum(1 for item in results if item["success"])
    failed_results = [item for item in results if not item["success"]]
    failed_rules = len(failed_results)
    fail_severities = {
        severity.lower()
        for severity in source_rules.get("fail_pipeline_on", ["critical"])
    }
    blocking_failures = [
        item
        for item in failed_results
        if item["severity"].lower() in fail_severities
    ]
    status = "failed" if blocking_failures else (
        "passed" if failed_rules == 0 else "passed_with_warnings"
    )
    repository.finish_run(
        validation_run_id=validation_run_id,
        status=status,
        total_rules=total_rules,
        passed_rules=passed_rules,
        failed_rules=failed_rules,
    )

    message = (
        f"[{source_id}] DQ {status}: "
        f"{passed_rules}/{total_rules} rule passed"
    )
    logger.info(message)
    if blocking_failures:
        failed_rule_ids = ", ".join(
            item["rule_id"] for item in blocking_failures
        )
        raise DataQualityError(
            f"{message}. Blocking rule failed: {failed_rule_ids}"
        )

    return {
        "source_id": source_id,
        "batch_id": batch_id,
        "status": status,
        "validation_run_id": validation_run_id,
        "total_rules": total_rules,
        "passed_rules": passed_rules,
        "failed_rules": failed_rules,
    }
