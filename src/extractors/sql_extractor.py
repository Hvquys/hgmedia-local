"""Generic SQL extractor for PostgreSQL and SQL Server sources."""

import logging
from datetime import datetime
from typing import Optional

import pandas as pd
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine, text

from src.connections import get_sqlalchemy_uri
from src.extractors.base import BaseExtractor, ExtractResult
from src.sql_utils import (
    quote_identifier,
    validate_identifier,
)


logger = logging.getLogger(__name__)


class SQLExtractor(BaseExtractor):
    def __init__(self, source_config: dict, connection_config: dict):
        super().__init__(source_config)
        self.connection_config = connection_config
        self._engine = None

    @property
    def _dialect(self) -> str:
        return (
            "sqlserver"
            if self.connection_config.get("type") == "sqlserver"
            else "postgresql"
        )

    def _get_engine(self):
        if self._engine is None:
            kwargs = {"pool_pre_ping": True}
            if self._dialect == "sqlserver":
                kwargs["execution_options"] = {
                    "isolation_level": "AUTOCOMMIT",
                    "stream_results": True,
                }
                kwargs["connect_args"] = {"timeout": 120}
            self._engine = create_engine(
                get_sqlalchemy_uri(self.connection_config),
                **kwargs,
            )
        return self._engine

    def _source_relation(self) -> str:
        cfg = self.source_config
        custom_query = cfg.get("custom_query")
        if custom_query:
            normalized = custom_query.strip().rstrip(";").strip()
            if not normalized:
                raise ValueError("custom_query không được để trống")
            return f"({normalized}) AS source_data"

        schema = cfg.get("schema") or self.connection_config.get("default_schema")
        validate_identifier(schema, "schema")
        validate_identifier(cfg["source_table"], "source_table")
        return ".".join(
            (
                quote_identifier(schema, self._dialect),
                quote_identifier(cfg["source_table"], self._dialect),
            )
        )

    def _build_query(self, watermark_filter: Optional[str] = None):
        cfg = self.source_config
        predicates = []
        parameters = {}
        watermark_column = cfg.get("watermark_column")

        month_filter = cfg.get("month_filter")
        if month_filter:
            if not watermark_column:
                raise ValueError("month_filter yêu cầu watermark_column")
            month_start = datetime.strptime(month_filter, "%Y-%m")
            month_end = month_start + relativedelta(months=1)
            column = quote_identifier(watermark_column, self._dialect)
            predicates.extend(
                (f"{column} >= :month_start", f"{column} < :month_end")
            )
            parameters.update(
                {"month_start": month_start, "month_end": month_end}
            )

        if watermark_filter is not None:
            if not watermark_column:
                raise ValueError("watermark_filter yêu cầu watermark_column")
            column = quote_identifier(watermark_column, self._dialect)
            predicates.append(f"{column} > :watermark_value")
            parameters["watermark_value"] = watermark_filter

        sql = f"SELECT * FROM {self._source_relation()}"
        if predicates:
            sql += " WHERE " + " AND ".join(predicates)
        return text(sql), parameters

    def extract(self, watermark_filter: Optional[str] = None) -> ExtractResult:
        cfg = self.source_config
        engine = self._get_engine()
        query, parameters = self._build_query(watermark_filter)
        chunksize = int(cfg.get("chunksize", 50_000))

        if cfg.get("stream_to_staging"):
            return self._extract_streamed(
                query,
                parameters,
                engine,
                chunksize,
                watermark_filter,
            )

        chunks = []
        total = 0
        for chunk_number, chunk in enumerate(
            pd.read_sql(
                query,
                engine,
                params=parameters,
                chunksize=chunksize,
            ),
            1,
        ):
            chunks.append(chunk)
            total += len(chunk)
            logger.info(
                "[%s] chunk %s: +%s (tổng %s)",
                cfg["source_id"],
                chunk_number,
                len(chunk),
                total,
            )

        dataframe = (
            pd.concat(chunks, ignore_index=True)
            if chunks
            else pd.DataFrame()
        )
        dataframe["_source_id"] = cfg["source_id"]
        dataframe["_source_connection"] = cfg["connection"]
        watermark_value = self._max_watermark(dataframe)
        return ExtractResult(
            dataframe=dataframe,
            row_count=len(dataframe),
            checksum=None,
            watermark_value=watermark_value,
            source_meta={
                "query": str(query),
                "connection": cfg["connection"],
            },
        )

    def _extract_streamed(
        self,
        query,
        parameters: dict,
        source_engine,
        chunksize: int,
        watermark_filter: Optional[str],
    ) -> ExtractResult:
        from src.loaders.staging_loader import StagingLoader

        cfg = self.source_config
        loader = StagingLoader()
        total = 0
        watermark_value = None
        first_chunk = True
        for chunk_number, chunk in enumerate(
            pd.read_sql(
                query,
                source_engine,
                params=parameters,
                chunksize=chunksize,
            ),
            1,
        ):
            chunk["_source_id"] = cfg["source_id"]
            chunk["_source_connection"] = cfg["connection"]
            mode = "truncate" if first_chunk and not watermark_filter else "append"
            loader.load(
                chunk,
                staging_table=cfg["target_staging_table"],
                batch_id="streamed",
                load_mode=mode,
                upsert_key=cfg.get("upsert_key"),
            )
            first_chunk = False
            total += len(chunk)
            chunk_watermark = self._max_watermark(chunk)
            if chunk_watermark is not None:
                watermark_value = max(
                    value
                    for value in (watermark_value, chunk_watermark)
                    if value is not None
                )
            logger.info(
                "[%s] streamed chunk %s: +%s (tổng %s)",
                cfg["source_id"],
                chunk_number,
                len(chunk),
                total,
            )

        return ExtractResult(
            dataframe=pd.DataFrame(),
            row_count=total,
            checksum=None,
            watermark_value=watermark_value,
            source_meta={"query": str(query), "streamed": True},
        )

    def _max_watermark(self, dataframe: pd.DataFrame) -> Optional[str]:
        column = self.source_config.get("watermark_column")
        if column and column in dataframe.columns and not dataframe.empty:
            return str(dataframe[column].max())
        return None

    def has_changed(self, last_checksum_or_watermark: Optional[str]) -> bool:
        cfg = self.source_config
        watermark_column = cfg.get("watermark_column")
        if not cfg.get("incremental") or not watermark_column:
            return True

        column = quote_identifier(watermark_column, self._dialect)
        sql = f"SELECT COUNT(*) AS cnt FROM {self._source_relation()}"
        parameters = {}
        if last_checksum_or_watermark is not None:
            sql += f" WHERE {column} > :watermark_value"
            parameters["watermark_value"] = last_checksum_or_watermark

        with self._get_engine().connect() as connection:
            count = connection.execute(text(sql), parameters).scalar_one()
        return count > 0
