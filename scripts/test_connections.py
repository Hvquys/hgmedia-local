"""
scripts/test_connections.py
Test nhanh kết nối tới từng nguồn, KHÔNG extract/load gì cả — chỉ xác nhận connect được.
Chạy: python scripts/test_connections.py
"""
import sys
sys.path.insert(0, ".")  # để import được src.* khi chạy từ thư mục gốc project

from src.connections import get_connection, get_sqlalchemy_uri


def test_postgres_or_sqlserver(name: str):
    from sqlalchemy import create_engine, text
    cfg = get_connection(name)
    try:
        engine = create_engine(get_sqlalchemy_uri(cfg))
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print(f"[OK]   {name} ({cfg['type']}) — kết nối thành công")
    except Exception as e:
        print(f"[FAIL] {name} ({cfg['type']}) — {e}")


def test_minio():
    from minio import Minio
    cfg = get_connection("minio")
    try:
        client = Minio(
            cfg["endpoint"], access_key=cfg["access_key"],
            secret_key=cfg["secret_key"], secure=cfg["secure"],
        )
        list(client.list_buckets())
        print(f"[OK]   minio — kết nối thành công")
    except Exception as e:
        print(f"[FAIL] minio — {e}")


def test_google_sheet():
    from google.oauth2.service_account import Credentials
    cfg = get_connection("google_sheet")
    try:
        Credentials.from_service_account_file(
            cfg["credentials_json_path"],
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
        )
        print(f"[OK]   google_sheet — đọc được credential file (chưa test gọi API thật)")
    except Exception as e:
        print(f"[FAIL] google_sheet — {e}")

def test_google_sheet_sources():
    import yaml
    from src.extractors.google_sheet_extractor import GoogleSheetExtractor
    gcfg = get_connection("google_sheet")
    srcs = yaml.safe_load(open("config/google_sheet_sources.yaml", encoding="utf-8"))["google_sheet_sources"]
    for s in srcs:
        try:
            ex = GoogleSheetExtractor(s, gcfg)
            rows = ex._read_raw_values()
            print(f"[OK]   {s['source_id']} — '{s['worksheet_name']}': {len(rows)} dòng, {len(rows[0]) if rows else 0} cột")
        except Exception as e:
            print(f"[FAIL] {s['source_id']} ({s['spreadsheet_id']}) — {e}")
            
if __name__ == "__main__":
    print("=== Test kết nối DB ===")
    for conn_name in [
        "dwh_postgres", "odoo_pg", "hg_stock", "editing_management",
        "channel_accountant", "channel_channel", "channel_network",
        "channel_organization", "channel_project", "channel_relationship",
    ]:
        test_postgres_or_sqlserver(conn_name)

    print("\n=== Test kết nối hạ tầng ===")
    test_minio()
    test_google_sheet()
    print("\n=== Test đọc Google Sheet sources ===")
    test_google_sheet_sources()
