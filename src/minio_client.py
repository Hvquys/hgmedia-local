"""
minio_client.py
Wrapper mỏng quanh thư viện minio để upload/download raw data (parquet) vào lớp bronze.
"""
import io
from datetime import datetime

import pandas as pd
from minio import Minio

from src.connections import get_connection


class MinIOClient:

    def __init__(self):
        cfg = get_connection("minio")
        self.client = Minio(
            cfg["endpoint"],
            access_key=cfg["access_key"],
            secret_key=cfg["secret_key"],
            secure=cfg["secure"],
        )
        self.bucket = cfg["bucket_raw"]
        self._ensure_bucket()

    def _ensure_bucket(self):
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)

    def build_path(self, source_config: dict, batch_id: str) -> str:
        st = source_config["source_type"]
        if st == "sql":
            conn = source_config["connection"]
            schema = source_config.get("schema", "default")
            table = source_config["source_table"]
            raw_key = f"{conn}/{schema}.{table}"
        elif st == "google_sheet":
            key = source_config.get("spreadsheet_id") or source_config.get("folder_id")
            ws = source_config.get("worksheet_name") or "all"
            raw_key = f"google_sheet/{key}/{ws}"
        else:
            raw_key = f"{st}/{source_config['source_id']}"
        return f"raw/{raw_key}/{batch_id}/data.parquet"

    def upload_dataframe(self, df: pd.DataFrame, source_config: dict, batch_id: str) -> str:
        path = self.build_path(source_config, batch_id)
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        self.client.put_object(
            self.bucket, path, buffer, length=buffer.getbuffer().nbytes,
            content_type="application/octet-stream",
        )
        return f"s3://{self.bucket}/{path}"

    def download_dataframe(self, minio_path: str) -> pd.DataFrame:
        """minio_path dạng s3://bucket/path/to/data.parquet"""
        path = minio_path.replace(f"s3://{self.bucket}/", "")
        response = self.client.get_object(self.bucket, path)
        try:
            buffer = io.BytesIO(response.read())
            return pd.read_parquet(buffer)
        finally:
            response.close()
            response.release_conn()

    @staticmethod
    def make_batch_id(source_id: str) -> str:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        return f"{source_id}_{ts}"
