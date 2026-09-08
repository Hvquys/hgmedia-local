"""
tasks/load_task.py
Task 2 (độc lập với Task 1): nhận batch_id + minio_path (qua XCom hoặc CLI),
download từ MinIO, load vào schema staging. KHÔNG cần file/kết nối nguồn gốc còn sống.
"""
import logging

from src.loaders.staging_loader import StagingLoader
from src.minio_client import MinIOClient
from src.source_registry import SourceRegistry

logger = logging.getLogger(__name__)


def run_load(
    source_config: dict,
    batch_id: str,
    minio_path: str,
    load_mode: str | None = None,
):
    source_id = source_config["source_id"]
    staging_table = source_config["target_staging_table"]
    effective_load_mode = load_mode or source_config.get("load_mode", "append")
    upsert_key = source_config.get("upsert_key")

    minio = MinIOClient()
    loader = StagingLoader()
    registry = SourceRegistry()

    try:
        df = minio.download_dataframe(minio_path)
        row_count = loader.load(
            df, staging_table=staging_table, batch_id=batch_id,
            load_mode=effective_load_mode, upsert_key=upsert_key,
        )
        registry.mark_loaded(batch_id)
        logger.info(f"[{source_id}] Load xong {row_count} dòng vào {staging_table}")
        return row_count
    except Exception as e:
        registry.mark_failed(batch_id, str(e))
        logger.error(f"[{source_id}] Load thất bại: {e}")
        raise
