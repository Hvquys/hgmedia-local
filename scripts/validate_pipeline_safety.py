"""Integration checks for atomic staging replacement on local PostgreSQL."""

import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.loaders.staging_loader import StagingLoader  # noqa: E402


TABLE = "staging._pipeline_safety_check"


def _rows(connection):
    return connection.execute(
        text(
            'SELECT "id", "value" '
            'FROM staging."_pipeline_safety_check" ORDER BY "id"'
        )
    ).all()


def main() -> int:
    loader = StagingLoader()
    engine = loader.engine
    try:
        loader.load(
            pd.DataFrame({"id": [1], "value": ["original"]}),
            TABLE,
            "safety_initial",
            load_mode="truncate",
        )
        with engine.begin() as connection:
            connection.exec_driver_sql(
                'ALTER TABLE staging."_pipeline_safety_check" '
                'ADD CONSTRAINT "ck_pipeline_safety_id" CHECK ("id" > 0)'
            )
            connection.exec_driver_sql(
                'CREATE INDEX "ix_pipeline_safety_value" '
                'ON staging."_pipeline_safety_check" ("value")'
            )
            original_oid = connection.execute(
                text("SELECT 'staging._pipeline_safety_check'::regclass::oid")
            ).scalar_one()

        loader.load(
            pd.DataFrame({"id": [2], "value": ["replacement"]}),
            TABLE,
            "safety_replacement",
            load_mode="truncate",
        )
        with engine.connect() as connection:
            current_oid = connection.execute(
                text("SELECT 'staging._pipeline_safety_check'::regclass::oid")
            ).scalar_one()
            rows_after_success = _rows(connection)
            constraint_exists = connection.execute(
                text(
                    "SELECT EXISTS ("
                    "SELECT 1 FROM pg_constraint "
                    "WHERE conname = 'ck_pipeline_safety_id')"
                )
            ).scalar_one()
            index_exists = connection.execute(
                text(
                    "SELECT to_regclass("
                    "'staging.ix_pipeline_safety_value') IS NOT NULL"
                )
            ).scalar_one()

        failure_observed = False
        try:
            loader.load(
                pd.DataFrame(
                    {"id": [3], "value": ["must_rollback"], "unexpected": [1]}
                ),
                TABLE,
                "safety_failure",
                load_mode="truncate",
            )
        except Exception as error:
            failure_observed = True
            print(f"Expected load failure = {type(error).__name__}")

        with engine.connect() as connection:
            rows_after_failure = _rows(connection)

        checks = {
            "table identity preserved": original_oid == current_oid,
            "replacement rows committed": rows_after_success == [(2, "replacement")],
            "constraint preserved": bool(constraint_exists),
            "index preserved": bool(index_exists),
            "invalid replacement failed": failure_observed,
            "failed replacement rolled back": rows_after_failure
            == [(2, "replacement")],
        }
        for label, passed in checks.items():
            print(f"{'PASS' if passed else 'FAIL'} | {label}")
        final_status = all(checks.values())
        print(f"Final status = {'PASSED' if final_status else 'FAILED'}")
        return 0 if final_status else 1
    finally:
        with engine.begin() as connection:
            connection.exec_driver_sql(
                'DROP TABLE IF EXISTS staging."_pipeline_safety_check"'
            )
        engine.dispose()


if __name__ == "__main__":
    sys.exit(main())
