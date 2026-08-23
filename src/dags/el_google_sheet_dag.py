"""
dags/el_google_sheet_dag.py
DAG cho nhóm nguồn Google Sheet với giao diện chọn bảng trong Airflow UI.
Trigger thủ công: chọn bảng muốn chạy → chỉ chạy bảng đó.
Lịch tự động: chạy TẤT CẢ bảng lúc 6h sáng hàng ngày.
"""
import sys
import os
from datetime import datetime, timedelta

PROJECT_ROOT = os.environ.get(
    "DWH_PROJECT_ROOT",
    "/mnt/d/HG_Project/etl_pipeline/dwh-pipeline-mapping/dwh-pipeline"
)
sys.path.insert(0, PROJECT_ROOT)

from airflow.sdk import dag, task
from airflow.models.param import Param

# Danh sách tất cả bảng Google Sheet
ALL_SOURCES = [
    "partners",
    "purchased_resource",
    "purchase_cost",
    "resource_before_odoo",
    "resource_performance",
    "distro_infomation"
]

default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="el_google_sheet_pipeline",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["el", "google_sheet"],
    params={
        "selected_tables": Param(
            default=ALL_SOURCES,
            type="array",
            title="Chọn bảng cần chạy",
            description="Tick vào bảng muốn extract. Mặc định chạy tất cả.",
            examples=ALL_SOURCES,
            items={"type": "string", "enum": ALL_SOURCES},
        ),
    },
)
def el_google_sheet_pipeline():

    @task
    def get_sources(**context):
        import sys, os
        sys.path.insert(0, PROJECT_ROOT)
        os.chdir(PROJECT_ROOT)
        from src.config_loader import load_sources

        selected = context["params"].get("selected_tables", ALL_SOURCES)
        all_sources = load_sources("config/google_sheet_sources.yaml", "google_sheet_sources")

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
            print(f"⏭ [{source_id}] skip - không có thay đổi")
            return f"[{source_id}] skip"

        if extract_result.get("streamed"):
            row_count = extract_result["row_count"]
        else:
            row_count = run_load(
                source_config,
                extract_result["batch_id"],
                extract_result["minio_path"],
            )

        print(f"✅ [{source_id}] loaded ({row_count} rows)")
        return f"[{source_id}] loaded ({row_count} rows)"

    sources = get_sources()
    extract_and_load.expand(source_config=sources)


el_google_sheet_pipeline()