"""
dags/el_database_dag.py
DAG cho nhóm nguồn Database với giao diện chọn bảng trong Airflow UI.
Hỗ trợ lọc theo connection (odoo_pg, hg_stock, channel_*, editing_management...).
Trigger thủ công: chọn connection + bảng muốn chạy.
Lịch tự động: chạy TẤT CẢ bảng mỗi 4 tiếng.
"""
import sys
import os
from datetime import datetime, timedelta

PROJECT_ROOT = os.environ.get(
    "DWH_PROJECT_ROOT",
    "/opt/airflow/project"
)
sys.path.insert(0, PROJECT_ROOT)

from airflow.sdk import Param, dag, task

from src.config_loader import load_sources
from src.tasks.extract_task import run_extract
from src.tasks.load_task import run_load

# Danh sách tất cả bảng theo connection
ALL_SOURCES_BY_CONNECTION = {
    "odoo_pg": [
        "hr_employee", "purchase_order", "purchase_order_line", "res_partner",
        "res_users", "sale_order", "sale_order_line", "x_acceptance_cert",
        "x_music_plan", "x_music_plan_detail",
        "x_music_plan_detail_price", "x_music_song", "x_product_genre",
        "x_product_subgenre", "x_project", "res_company", "hr_department", "hr_job",
    ],
    "hg_stock": [
        "distribution_media_history", "groups", "resource_file_info",
        "resource_file_action", "resource_folders", "roles",
        "user_departments", "users", "tracking_video_publish_infos",
        "resource_storage_history", "resource_files",
    ],
    "channel_organization": [
        "company", "department", "departmentlevel", "position", "user",
        "userdepartmentposition",
    ],
    "channel_relationship": [
        "channeldepartment", "channel_project", "channel_deal",
        "channel_user", "channel_company",
    ],
    "channel_channel": ["channel"],
    "channel_project": ["project"],
    "channel_network": ["network", "cms"],
    "editing_management": [
        "editings", "user_editing", "resource", "resource_editings",
    ],
    "record_survey": [
        "review", "review_user", "recording_sound",
        "recording_sound_version", "user_comment_version", "version_needs_comment",
    ],
}

ALL_SOURCES = [s for sources in ALL_SOURCES_BY_CONNECTION.values() for s in sources]
ALL_CONNECTIONS = list(ALL_SOURCES_BY_CONNECTION.keys())

default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="el_database_pipeline",
    schedule="0 */4 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["el", "database"],
    max_active_tasks=8,
    params={
        "selected_connections": Param(
            default=ALL_CONNECTIONS,
            type="array",
            title="Lọc theo Connection (nguồn DB)",
            description="Chọn connection muốn chạy. Bỏ trống = chạy tất cả.",
            examples=ALL_CONNECTIONS,
            items={"type": "string", "enum": ALL_CONNECTIONS},
        ),
        "selected_tables": Param(
            default=[],
            type="array",
            title="Chọn bảng cụ thể (tuỳ chọn)",
            description=(
                "Nếu để trống → chạy tất cả bảng của connection đã chọn.\n"
                "Nếu điền tên bảng → chỉ chạy bảng đó (phải thuộc connection đã chọn).\n\n"
                "Danh sách bảng theo connection:\n"
                + "\n".join(
                    f"  {conn}: {', '.join(tables)}"
                    for conn, tables in ALL_SOURCES_BY_CONNECTION.items()
                )
            ),
        ),
    },
)
def el_database_pipeline():

    @task
    def get_sources(**context):
        import sys, os
        sys.path.insert(0, PROJECT_ROOT)
        os.chdir(PROJECT_ROOT)
        from src.config_loader import load_sources

        selected_connections = context["params"].get("selected_connections", ALL_CONNECTIONS)
        selected_tables = context["params"].get("selected_tables", [])

        all_sources = load_sources("config/db_sources.yaml", "db_sources")

        # Lọc theo connection
        filtered = [s for s in all_sources if s.get("connection") in selected_connections]

        # Nếu có chọn bảng cụ thể → lọc thêm
        if selected_tables:
            filtered = [s for s in filtered if s["source_id"] in selected_tables]

        print(f"✅ Sẽ chạy {len(filtered)}/{len(all_sources)} bảng:")
        for s in filtered:
            print(f"   [{s.get('connection')}] {s['source_id']} → {s['target_staging_table']}")
        return filtered

    @task
    def extract_and_load(source_config: dict):
        import sys, os
        sys.path.insert(0, PROJECT_ROOT)
        os.chdir(PROJECT_ROOT)
        from src.tasks.extract_task import run_extract
        from src.tasks.load_task import run_load

        source_id = source_config["source_id"]
        connection = source_config.get("connection", "unknown")
        print(f"▶ [{connection}] Bắt đầu extract: {source_id}")

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
                load_mode=extract_result.get("load_mode_override"),
            )

        print(f"✅ [{source_id}] loaded ({row_count} rows)")
        return f"[{source_id}] loaded ({row_count} rows)"

    sources = get_sources()
    extract_and_load.expand(source_config=sources)


el_database_pipeline()
