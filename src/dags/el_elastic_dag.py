"""
dags/el_elastic_dag.py
DAG cho nhóm nguồn Elasticsearch với giao diện chọn bảng trong Airflow UI.
Trigger thủ công: chọn index muốn chạy + tuỳ chọn date range.
Lịch tự động: chạy TẤT CẢ mỗi 6 tiếng.
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

ALL_SOURCES = ["channel_video_info", "channel_video_metric"]

SOURCE_DESCRIPTIONS = {
    "channel_video_info":   "ES index: channel-video-info (thông tin video kênh)",
    "channel_video_metric": "ES index: channel-video-metric-* (metrics video theo ngày)",
}

default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="el_elastic_pipeline",
    schedule="0 */6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["el", "elasticsearch"],
    max_active_tasks=4,
    params={
        "selected_tables": Param(
            default="all",
            type="string",
            title="Chọn index Elasticsearch cần chạy",
            description=(
                "'all' = chạy tất cả\n\n"
                + "\n".join(f"  {k}: {v}" for k, v in SOURCE_DESCRIPTIONS.items())
            ),
            enum=["all"] + ALL_SOURCES,
        ),
        "date_from": Param(
            default="",
            type=["string", "null"],   # ← cho phép null/empty
            title="Từ ngày (tuỳ chọn)",
            description=(
                "Override date_from trong config. Định dạng: YYYY-MM-DD.\n"
                "Để trống → dùng date_from trong config (mặc định 2025-06-01)."
            ),
            minLength=0,               # ← cho phép chuỗi rỗng
        ),
    },
    
)
def el_elastic_pipeline():

    @task
    def get_sources(**context):
        import sys, os
        sys.path.insert(0, PROJECT_ROOT)
        os.chdir(PROJECT_ROOT)
        from src.config_loader import load_sources

        selected = context["params"].get("selected_tables", "all")
        date_from = context["params"].get("date_from", "")  # Bug 2: lấy date_from từ params

        all_sources = load_sources("config/elastic_sources.yaml", "elastic_sources")  # Bug 1: khai báo trước

        if selected == "all":
            filtered = all_sources
        else:
            filtered = [s for s in all_sources if s["source_id"] == selected]  # Bug 3: == thay vì in

        # Override date_from nếu user nhập
        if date_from:
            for s in filtered:
                s["date_from"] = date_from
            print(f"📅 Override date_from = {date_from}")

        print(f"✅ Sẽ chạy {len(filtered)}/{len(all_sources)} index: {[s['source_id'] for s in filtered]}")
        return filtered

    @task
    def extract_and_load(source_config: dict):
        import sys, os
        sys.path.insert(0, PROJECT_ROOT)
        os.chdir(PROJECT_ROOT)
        from src.tasks.extract_task import run_extract
        from src.tasks.load_task import run_load

        source_id = source_config["source_id"]
        print(f"▶ Bắt đầu extract: {source_id} (from {source_config.get('date_from')})")

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


el_elastic_pipeline()