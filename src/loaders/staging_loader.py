"""Load Bronze dataframes into PostgreSQL staging tables."""

import hashlib
import json
import logging
from datetime import datetime, timezone
from uuid import uuid4

import pandas as pd
from sqlalchemy import create_engine, text

from src.connections import get_connection, get_sqlalchemy_uri
from src.sql_utils import quote_dataframe_column, split_qualified_table


logger = logging.getLogger(__name__)


class StagingLoader:
    def __init__(self):
        connection = get_connection("dwh_postgres")
        self.engine = create_engine(get_sqlalchemy_uri(connection))

    def load(
        self,
        df: pd.DataFrame,
        staging_table: str,
        batch_id: str,
        load_mode: str = "append",
        upsert_key: list | None = None,
    ) -> int:
        if load_mode not in {"truncate", "append", "upsert"}:
            raise ValueError(f"load_mode không hợp lệ: {load_mode!r}")

        dataframe = self._prepare_dataframe(df, batch_id)
        schema, table = split_qualified_table(staging_table)
        self._ensure_schema(schema)

        if load_mode == "truncate":
            self._replace_atomically(dataframe, schema, table)
        elif load_mode == "upsert":
            if not upsert_key:
                raise ValueError("upsert_key bắt buộc khi load_mode='upsert'")
            self._upsert(dataframe, schema, table, upsert_key)
        else:
            dataframe.to_sql(
                table,
                self.engine,
                schema=schema,
                if_exists="append",
                index=False,
                chunksize=5_000,
            )
        return len(dataframe)

    @staticmethod
    def _prepare_dataframe(df: pd.DataFrame, batch_id: str) -> pd.DataFrame:
        dataframe = df.copy()
        new_columns = []
        seen = {}
        for index, column in enumerate(dataframe.columns):
            name = str(column).replace("\n", " ").strip() or f"col_{index}"
            if name in seen:
                seen[name] += 1
                name = f"{name}_{seen[name]}"
            else:
                seen[name] = 0
            new_columns.append(name)
        dataframe.columns = new_columns

        for column in dataframe.columns:
            if dataframe[column].map(
                lambda value: isinstance(value, (dict, list))
            ).any():
                dataframe[column] = dataframe[column].map(
                    lambda value: json.dumps(value, ensure_ascii=False)
                    if isinstance(value, (dict, list))
                    else value
                )

        dataframe["_batch_id"] = batch_id
        dataframe["_loaded_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
        return dataframe

    def _ensure_schema(self, schema: str) -> None:
        quoted_schema = quote_dataframe_column(schema)
        with self.engine.begin() as connection:
            connection.exec_driver_sql(
                f"CREATE SCHEMA IF NOT EXISTS {quoted_schema}"
            )

    def _replace_atomically(
        self,
        dataframe: pd.DataFrame,
        schema: str,
        table: str,
    ) -> None:
        temp_table = self._temporary_table_name(table)
        quoted_schema = quote_dataframe_column(schema)
        quoted_table = quote_dataframe_column(table)
        quoted_temp = quote_dataframe_column(temp_table)
        columns = ", ".join(
            quote_dataframe_column(column) for column in dataframe.columns
        )

        dataframe.to_sql(
            temp_table,
            self.engine,
            schema=schema,
            if_exists="fail",
            index=False,
            chunksize=5_000,
        )
        try:
            with self.engine.begin() as connection:
                target_exists = connection.execute(
                    text("SELECT to_regclass(:qualified_name) IS NOT NULL"),
                    {"qualified_name": f"{schema}.{table}"},
                ).scalar_one()
                if not target_exists:
                    connection.exec_driver_sql(
                        f"ALTER TABLE {quoted_schema}.{quoted_temp} "
                        f"RENAME TO {quoted_table}"
                    )
                    return

                connection.exec_driver_sql(
                    f"TRUNCATE TABLE {quoted_schema}.{quoted_table}"
                )
                if columns:
                    connection.exec_driver_sql(
                        f"INSERT INTO {quoted_schema}.{quoted_table} ({columns}) "
                        f"SELECT {columns} FROM {quoted_schema}.{quoted_temp}"
                    )
                connection.exec_driver_sql(
                    f"DROP TABLE {quoted_schema}.{quoted_temp}"
                )
        finally:
            self._cleanup_table(schema, temp_table)

    def _upsert(
        self,
        dataframe: pd.DataFrame,
        schema: str,
        table: str,
        upsert_key: list,
    ) -> None:
        missing_keys = [key for key in upsert_key if key not in dataframe.columns]
        if missing_keys:
            raise ValueError(
                f"upsert_key không tồn tại trong dữ liệu: {', '.join(missing_keys)}"
            )

        temp_table = self._temporary_table_name(table)
        dataframe.to_sql(
            temp_table,
            self.engine,
            schema=schema,
            if_exists="fail",
            index=False,
        )
        quoted_schema = quote_dataframe_column(schema)
        quoted_table = quote_dataframe_column(table)
        quoted_temp = quote_dataframe_column(temp_table)
        quoted_keys = [quote_dataframe_column(key) for key in upsert_key]
        quoted_columns = [
            quote_dataframe_column(column) for column in dataframe.columns
        ]
        non_key_columns = [
            column for column in dataframe.columns if column not in upsert_key
        ]
        raw_index_name = f"uq_{table}_{'_'.join(upsert_key)}"
        if len(raw_index_name.encode("utf-8")) > 63:
            index_hash = hashlib.sha256(
                f"{schema}.{table}:{','.join(upsert_key)}".encode("utf-8")
            ).hexdigest()[:12]
            raw_index_name = f"uq_{table[:40]}_{index_hash}"
        index_name = quote_dataframe_column(raw_index_name)

        try:
            dataframe.iloc[0:0].to_sql(
                table,
                self.engine,
                schema=schema,
                if_exists="append",
                index=False,
            )
            with self.engine.begin() as connection:
                connection.exec_driver_sql(
                    f"CREATE UNIQUE INDEX IF NOT EXISTS {index_name} "
                    f"ON {quoted_schema}.{quoted_table} "
                    f"({', '.join(quoted_keys)})"
                )
                if non_key_columns:
                    update_clause = ", ".join(
                        f"{quote_dataframe_column(column)} = "
                        f"EXCLUDED.{quote_dataframe_column(column)}"
                        for column in non_key_columns
                    )
                    conflict_action = f"DO UPDATE SET {update_clause}"
                else:
                    conflict_action = "DO NOTHING"
                connection.exec_driver_sql(
                    f"INSERT INTO {quoted_schema}.{quoted_table} "
                    f"({', '.join(quoted_columns)}) "
                    f"SELECT {', '.join(quoted_columns)} "
                    f"FROM {quoted_schema}.{quoted_temp} "
                    f"ON CONFLICT ({', '.join(quoted_keys)}) {conflict_action}"
                )
                connection.exec_driver_sql(
                    f"DROP TABLE {quoted_schema}.{quoted_temp}"
                )
        finally:
            self._cleanup_table(schema, temp_table)

    def _cleanup_table(self, schema: str, table: str) -> None:
        try:
            self._drop_table_if_exists(schema, table)
        except Exception:
            logger.warning(
                "Không thể dọn bảng tạm %s.%s",
                schema,
                table,
                exc_info=True,
            )

    def _drop_table_if_exists(self, schema: str, table: str) -> None:
        quoted_schema = quote_dataframe_column(schema)
        quoted_table = quote_dataframe_column(table)
        with self.engine.begin() as connection:
            connection.exec_driver_sql(
                f"DROP TABLE IF EXISTS {quoted_schema}.{quoted_table}"
            )

    @staticmethod
    def _temporary_table_name(table: str) -> str:
        return f"_load_{table[:35]}_{uuid4().hex[:12]}"

    def ensure_table_exists(self, df: pd.DataFrame, staging_table: str):
        schema, table = split_qualified_table(staging_table)
        self._ensure_schema(schema)
        df.iloc[0:0].to_sql(
            table,
            self.engine,
            schema=schema,
            if_exists="append",
            index=False,
        )
