import argparse
import hashlib
import json
import sys
import uuid
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from src.data_quality.gx_validator import (
    load_staging_dataframe,
    validate_dataframe,
)
from src.data_quality.result_repository import DataQualityRepository
from src.data_quality.rule_loader import get_source_rules


def make_config_hash(config: dict) -> str:
    serialized = json.dumps(
        config,
        ensure_ascii=False,
        sort_keys=True,
        default=str,
    )

    return hashlib.sha256(
        serialized.encode("utf-8")
    ).hexdigest()


def get_batch_id(dataframe) -> str | None:
    if "_batch_id" not in dataframe.columns:
        return None

    batch_values = (
        dataframe["_batch_id"]
        .dropna()
        .astype(str)
        .unique()
        .tolist()
    )

    if len(batch_values) == 1:
        return batch_values[0]

    if "_loaded_at" in dataframe.columns:
        loaded_at = pd.to_datetime(
            dataframe["_loaded_at"],
            errors="coerce",
        )
        latest_loaded_at = loaded_at.max()

        if not pd.isna(latest_loaded_at):
            latest_batches = (
                dataframe.loc[
                    loaded_at == latest_loaded_at,
                    "_batch_id",
                ]
                .dropna()
                .astype(str)
                .unique()
                .tolist()
            )

            if len(latest_batches) == 1:
                return latest_batches[0]

    return None


def print_results(results: list[dict]) -> None:
    print()
    print("DATA QUALITY RESULTS")
    print("-" * 100)

    for result in results:
        status = "PASS" if result["success"] else "FAIL"

        print(
            f"{status:4} | "
            f"{result['severity']:8} | "
            f"{result['rule_id']:40} | "
            f"unexpected={result.get('unexpected_count')}"
        )

        if not result["success"]:
            error = result["raw_result"].get(
                "exception_message"
            )

            if error:
                print("       Error:", error)

    print("-" * 100)


def run_data_quality(source_id: str) -> int:
    config = get_source_rules(source_id)

    if not config.get("enabled", True):
        print(f"[{source_id}] DQ đang bị tắt.")
        return 0

    target_table = config["target_table"]
    rules = config.get("rules", [])
    fail_pipeline_on = {
        severity.lower()
        for severity in config.get(
            "fail_pipeline_on",
            ["critical"],
        )
    }

    dataframe = load_staging_dataframe(target_table)
    batch_id = get_batch_id(dataframe)

    validation_run_id = str(uuid.uuid4())
    config_hash = make_config_hash(config)

    repository = DataQualityRepository()

    repository.create_run(
        validation_run_id=validation_run_id,
        airflow_dag_id=None,
        airflow_dag_run_id=None,
        source_id=source_id,
        target_table=target_table,
        batch_id=batch_id,
        status="running",
        config_hash=config_hash,
    )

    try:
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

        passed_rules = sum(
            result["success"]
            for result in results
        )
        failed_rules = len(results) - passed_rules

        blocking_failures = [
            result
            for result in results
            if (
                not result["success"]
                and result["severity"].lower()
                in fail_pipeline_on
            )
        ]

        run_status = (
            "failed"
            if blocking_failures
            else "passed"
        )

        repository.finish_run(
            validation_run_id=validation_run_id,
            status=run_status,
            total_rules=len(results),
            passed_rules=passed_rules,
            failed_rules=failed_rules,
        )

        print_results(results)

        print("Validation run =", validation_run_id)
        print("Source         =", source_id)
        print("Target         =", target_table)
        print("Batch          =", batch_id)
        print("Status         =", run_status)
        print("Total rules    =", len(results))
        print("Passed rules   =", passed_rules)
        print("Failed rules   =", failed_rules)

        return 1 if blocking_failures else 0

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


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Chạy Data Quality cho một source"
    )
    parser.add_argument(
        "--id",
        required=True,
        help="Source ID trong data_quality_rules.yaml",
    )

    args = parser.parse_args()
    return run_data_quality(args.id)


if __name__ == "__main__":
    sys.exit(main())
