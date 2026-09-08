import hashlib
from pathlib import Path
from typing import Optional

import pandas as pd

from src.extractors.base import BaseExtractor, ExtractResult


class CsvExtractor(BaseExtractor):
    def __init__(self, source_config, connection_config=None):
        super().__init__(source_config)

    def extract(self, watermark_filter: Optional[str] = None) -> ExtractResult:
        cfg = self.source_config
        chunksize = cfg.get("chunksize", 50000)
        stream = cfg.get("stream_to_staging")
        if stream:
            from sqlalchemy import create_engine
            from src.connections import get_connection, get_sqlalchemy_uri
            dwh = create_engine(get_sqlalchemy_uri(get_connection("dwh_postgres")))
            schema, table = cfg["target_staging_table"].split(".")
            total, page = 0, 0
            for ck in pd.read_csv(cfg["file_path"], dtype=str, sep=cfg.get("sep", ","),
                                  chunksize=20000, on_bad_lines="skip",
                                  engine="python", quotechar='"'):
                page += 1
                ck.columns = [str(c).strip() for c in ck.columns]
                ck["_source_id"] = cfg["source_id"]
                ck.to_sql(table, dwh, schema=schema, index=False,
                          if_exists=("replace" if page == 1 else "append"))
                total += len(ck)
                print(f"[stream_distro] page {page}: tổng {total}", flush=True)
            return ExtractResult(dataframe=pd.DataFrame(), row_count=total,
                                 checksum=None, watermark_value=None,
                                 source_meta={"file": cfg["file_path"], "streamed": True})
        df = pd.read_csv(cfg["file_path"], dtype=str, sep=cfg.get("sep", ","),
                         on_bad_lines="skip", engine="python", quotechar='"')
        df.columns = [str(c).strip() for c in df.columns]

        watermark_column = cfg.get("watermark_column")
        watermark_value = None

        if watermark_column:
            if watermark_column not in df.columns:
                raise ValueError(
                    f"Không tìm thấy watermark_column '{watermark_column}' "
                    f"trong CSV {cfg['file_path']}"
                )

            parsed_watermark = pd.to_datetime(
                df[watermark_column],
                errors="coerce",
            )

            if parsed_watermark.notna().any():
                watermark_value = str(parsed_watermark.max())

            if cfg.get("incremental") and watermark_filter:
                last_watermark = pd.to_datetime(
                    watermark_filter,
                    errors="raise",
                )
                df = df.loc[parsed_watermark > last_watermark].copy()

        df["_source_id"] = cfg["source_id"]

        return ExtractResult(
            dataframe=df,
            row_count=len(df),
            checksum=self._compute_checksum(),
            watermark_value=watermark_value,
            source_meta={"file": cfg["file_path"]},
        )

    def has_changed(self, last_checksum_or_watermark: Optional[str]) -> bool:
        if last_checksum_or_watermark is None:
            return True

        return self._compute_checksum() != last_checksum_or_watermark

    def _compute_checksum(self) -> str:
        file_path = Path(self.source_config["file_path"])
        digest = hashlib.sha256()

        with file_path.open("rb") as file:
            for chunk in iter(lambda: file.read(1024 * 1024), b""):
                digest.update(chunk)

        return digest.hexdigest()
