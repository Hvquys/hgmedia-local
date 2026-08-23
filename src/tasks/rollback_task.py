"""
tasks/rollback_task.py
Rollback 1 source_id về batch 'loaded' gần nhất <= target_date.
Không cần file/connection gốc vì dữ liệu đã có sẵn trên MinIO từ lúc extract.
"""
import logging

from src.loaders.staging_loader import StagingLoader
from src.minio_client import MinIOClient
from src.source_registry import SourceRegistry

logger = logging.getLogger(__name__)


def run_rollback(source_config: dict, target_date: str):
    source_id = source_config["source_id"]
    staging_table = source_config["target_staging_table"]

    registry = SourceRegistry()
    minio = MinIOClient()
    loader = StagingLoader()

    batch = registry.find_batch_for_rollback(source_id, target_date)
    if not batch:
        logger.warning(f"[{source_id}] Không tìm thấy batch nào <= {target_date} để rollback.")
        return

    df = minio.download_dataframe(batch["minio_path"])
    loader.load(df, staging_table=staging_table, batch_id=batch["batch_id"], load_mode="truncate")

    logger.info(
        f"[{source_id}] Rollback xong về batch {batch['batch_id']} "
        f"(extracted_at={batch['extracted_at']}, {len(df)} dòng)"
    )
