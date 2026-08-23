import json
import re
from typing import Any

import great_expectations as gx
import pandas as pd
from sqlalchemy import create_engine

from src.connections import get_connection, get_sqlalchemy_uri


TABLE_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*$")


def _to_dict(value: Any) -> dict:
    if hasattr(value, "to_json_dict"):
        return value.to_json_dict()

    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")

    if isinstance(value, dict):
        return value

    return {"value": str(value)}


def _to_text(value: Any) -> str | None:
    if value is None:
        return None

    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, default=str)

    return str(value)


def load_staging_dataframe(target_table: str) -> pd.DataFrame:
    if not TABLE_NAME_PATTERN.match(target_table):
        raise ValueError(f"Tên bảng không hợp lệ: {target_table}")

    conn_cfg = get_connection("dwh_postgres")
    engine = create_engine(get_sqlalchemy_uri(conn_cfg))

    return pd.read_sql(f"SELECT * FROM {target_table}", engine)


def validate_dataframe(
    dataframe: pd.DataFrame,
    source_id: str,
    rules: list[dict],
) -> list[dict]:
    context = gx.get_context(mode="ephemeral")

    datasource = context.data_sources.add_pandas(
        name=f"dq_pandas_{source_id}"
    )
    asset = datasource.add_dataframe_asset(
        name=f"dq_asset_{source_id}"
    )
    batch_definition = asset.add_batch_definition_whole_dataframe(
        name=f"dq_batch_{source_id}"
    )
    batch = batch_definition.get_batch(
        batch_parameters={"dataframe": dataframe}
    )

    normalized_results = []

    for rule in rules:
        if not rule.get("enabled", True):
            continue

        rule_id = rule["id"]
        expectation_name = rule["expectation"]
        severity = rule.get("severity", "warning").lower()
        kwargs = rule.get("kwargs", {})
        column_name = kwargs.get("column")

        try:
            expectation_class = getattr(gx.expectations, expectation_name)
            expectation = expectation_class(**kwargs)
            gx_result = batch.validate(expectation)
            raw_result = _to_dict(gx_result)

            result_details = raw_result.get("result", {})
            success = bool(raw_result.get("success", False))

            normalized_results.append({
                "rule_id": rule_id,
                "expectation_type": expectation_name,
                "column_name": column_name,
                "severity": severity,
                "success": success,
                "observed_value": _to_text(
                    result_details.get("observed_value")
                ),
                "unexpected_count": result_details.get("unexpected_count"),
                "unexpected_percent": result_details.get(
                    "unexpected_percent"
                ),
                "raw_result": raw_result,
            })

        except Exception as error:
            normalized_results.append({
                "rule_id": rule_id,
                "expectation_type": expectation_name,
                "column_name": column_name,
                "severity": severity,
                "success": False,
                "observed_value": None,
                "unexpected_count": None,
                "unexpected_percent": None,
                "raw_result": {
                    "success": False,
                    "exception_message": str(error),
                },
            })

    return normalized_results