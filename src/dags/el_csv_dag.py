"""
dags/el_csv_dag.py
DAG cho nhóm nguồn CSV và FX rate với giao diện chọn bảng trong Airflow UI.
Trigger thủ công: chọn bảng muốn chạy.
Lịch tự động: chạy TẤT CẢ lúc 5h sáng hàng ngày.
"""
import sys
import os
from datetime import datetime, timedelta

PROJECT_ROOT = os.environ.get(
    "DWH_PROJECT_ROOT",
    "/mnt/d/HG_Project/etl_pipeline/dwh-pipeline-mapping/dwh-pipeline"
)

from airflow.sdk import dag, task
from airflow.models.param import Param

ALL_SOURCES = ["sale", "stream_distro", "usd_rate"]

SOURCE_DESCRIPTIONS = {
    "sale":         "CSV - Dữ liệu doanh thu (Discover session)",
    "stream_distro":"CSV - Lượt stream distro (fact_view_stream)",
    "usd_rate":     "FX  - Tỷ giá USD/VND hàng ngày",
}

default_args = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="el_csv_pipeline",
    schedule="0 5 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["el", "csv"],
    params={
        "selected_tables": Param(
            default=ALL_SOURCES,
            type="array",
            title="Chọn bảng cần chạy",
            description=(
                "Tick vào bảng muốn extract. Mặc định chạy tất cả.\n\n"
                + "\n".join(f"  {k}: {v}" for k, v in SOURCE_DESCRIPTIONS.items())
            ),
            examples=ALL_SOURCES,
            items={"type": "string", "enum": ALL_SOURCES},
        ),
    },
)
def el_csv_pipeline():

    @task
    def get_sources(**context):
        import sys, os
        sys.path.insert(0, PROJECT_ROOT)
        os.chdir(PROJECT_ROOT)
        from src.config_loader import load_sources

        selected = context["params"].get("selected_tables", ALL_SOURCES)
        all_sources = load_sources("config/csv_sources.yaml", "csv_sources")

        filtered = [s for s in all_sources if s["source_id"] in selected]
        print(f"✅ Sẽ chạy {len(filtered)}/{len(all_sources)} bảng: {[s['source_id'] for s in filtered]}")
        return filtered

    @task
    def extract_and_load(source_config: dict):
        import sys, os
        sys.path.insert(0, PROJECT_ROOT)
        os.chdir(PROJECT_ROOT)
        from src.tasks.extract_task import run_extract
        from src.tasks.load_task import run_load

        source_id = source_config["source_id"]
        print(f"▶ Bắt đầu extract: {source_id}")

        extract_result = run_extract(source_config)
        if extract_result is None:
            print(f"⏭ [{source_id}] skip")
            return f"[{source_id}] skip"

        if not source_config.get("stream_to_staging"):
            run_load(source_config, extract_result["batch_id"], extract_result["minio_path"])

        print(f"✅ [{source_id}] loaded ({extract_result['row_count']} rows)")
        return f"[{source_id}] loaded ({extract_result['row_count']} rows)"

    sources = get_sources()
    extract_and_load.expand(source_config=sources)


el_csv_pipeline()