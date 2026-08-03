"""
config_loader.py
Đọc các file YAML config (google_sheet_sources.yaml, db_sources.yaml) thành list dict.
"""
import yaml
from pathlib import Path


def load_sources(yaml_path: str, key: str) -> list[dict]:
    path = Path(yaml_path)
    if not path.exists():
        raise FileNotFoundError(f"Không tìm thấy config: {yaml_path}")
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data.get(key, [])


def load_all_sources(config_dir: str = "config") -> list[dict]:
    """Gộp tất cả nguồn (Google Sheet + DB) thành 1 list, mỗi item có source_id duy nhất."""
    all_sources = []
    all_sources += load_sources(f"{config_dir}/google_sheet_sources.yaml", "google_sheet_sources")
    all_sources += load_sources(f"{config_dir}/db_sources.yaml", "db_sources")
    all_sources += load_sources(f"{config_dir}/elastic_sources.yaml", "elastic_sources")
    all_sources += load_sources(f"{config_dir}/csv_sources.yaml", "csv_sources")
    all_sources += load_sources(f"{config_dir}/api_sources.yaml", "api_sources")
    return all_sources


def get_source_by_id(source_id: str, config_dir: str = "config") -> dict:
    for src in load_all_sources(config_dir):
        if src["source_id"] == source_id:
            return src
    raise ValueError(f"Không tìm thấy source_id '{source_id}' trong config")
