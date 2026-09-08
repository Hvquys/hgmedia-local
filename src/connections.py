"""Khai báo connection; thông tin nhạy cảm chỉ được đọc từ environment."""
import os

from dotenv import load_dotenv

load_dotenv()  # tự đọc file .env ở thư mục gốc project nếu có


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


CONNECTIONS = {
    "record_survey": {
        "type": "sqlserver",
        "host": _env("RS_HOST"),
        "port": _env("RS_PORT", "1433"),
        "database": "record-survey",
        "user": _env("RS_USER"),
        "password": _env("RS_PASSWORD"),
    },
    "elastic": {
        "type": "elastic",
        "host": _env("ELASTIC_HOST", "http://hgmedia.app:9200"),
        "user": _env("ELASTIC_USER", ""),
        "password": _env("ELASTIC_PASSWORD", ""),
    },
    # ---- Hạ tầng pipeline ----
    "minio": {
        "endpoint": _env("MINIO_ENDPOINT", "localhost:9000"),
        "access_key": _env("MINIO_ACCESS_KEY"),
        "secret_key": _env("MINIO_SECRET_KEY"),
        "secure": False,
        "bucket_raw": "raw-bronze",
    },
    "dwh_postgres": {
        "type": "postgresql",
        "host": _env("DWH_PG_HOST", "localhost"),
        "port": int(_env("DWH_PG_PORT", "5432")),
        "database": _env("DWH_PG_DB", "data_warehouse"),
        "user": _env("DWH_PG_USER"),
        "password": _env("DWH_PG_PASSWORD"),
    },

    # ---- Nguồn Database (đọc từ DB có sẵn) ----
    "odoo_pg": {
        "type": "postgresql",
        "host": _env("ODOO_PG_HOST"),
        "port": int(_env("ODOO_PG_PORT", "5432")),
        "database": _env("ODOO_PG_DB", "odoo"),
        "user": _env("ODOO_PG_USER"),
        "password": _env("ODOO_PG_PASSWORD"),
        "default_schema": "public",
    },
    "hg_stock": {
        "type": "postgresql",
        "host": _env("HGSTOCK_HOST"),
        "port": int(_env("HGSTOCK_PORT", "5432")),
        "database": _env("HGSTOCK_DB", "hgstock"),
        "user": _env("HGSTOCK_USER"),
        "password": _env("HGSTOCK_PASSWORD"),
        "default_schema": "dbo",
    },
    "editing_management": {
        "type": "sqlserver",
        "host": _env("EDITING_HOST"),
        "port": int(_env("EDITING_PORT", "1433")),
        "database": _env("EDITING_DB", "editing-management"),
        "user": _env("EDITING_USER"),
        "password": _env("EDITING_PASSWORD"),
        "default_schema": "dbo",
    },

    # ---- Nguồn Google Sheet ----
    "google_sheet": {
        "type": "google_sheet",
        # service account JSON: để file credential ở path này (tải từ Google Cloud Console)
        "credentials_json_path": _env("GSHEET_CREDENTIALS_PATH", "config/gsheet_service_account.json"),
    },
}
# ---- Channel Service (SQL Server) — 1 login, 6 database tách riêng ----
_CHANNEL_DBS = {
    "channel_accountant":   _env("CHANNEL_ACCOUNTANT_DB",   "channel.service.accountant"),
    "channel_channel":      _env("CHANNEL_CHANNEL_DB",      "channel.service.channel"),
    "channel_network":      _env("CHANNEL_NETWORK_DB",      "channel.service.network"),
    "channel_organization": _env("CHANNEL_ORGANIZATION_DB", "channel.service.organization"),
    "channel_project":      _env("CHANNEL_PROJECT_DB",      "channel.service.project"),
    "channel_relationship": _env("CHANNEL_RELATIONSHIP_DB", "channel.service.relationship"),
}
for _name, _db in _CHANNEL_DBS.items():
    CONNECTIONS[_name] = {
        "type": "sqlserver",
        "host": _env("CHANNEL_HOST"),
        "port": int(_env("CHANNEL_PORT", "1433")),
        "database": _db,
        "user": _env("CHANNEL_USER"),
        "password": _env("CHANNEL_PASSWORD"),
        "default_schema": "dbo",
    }


def get_connection(name: str) -> dict:
    if name not in CONNECTIONS:
        raise ValueError(f"Connection '{name}' chưa được khai báo trong connections.py")
    connection = CONNECTIONS[name]
    if name == "google_sheet":
        required_fields = ("credentials_json_path",)
    elif name == "elastic":
        required_fields = ("host",)
    elif name == "minio":
        required_fields = ("endpoint", "access_key", "secret_key")
    else:
        required_fields = ("host", "database", "user", "password")

    missing = [field for field in required_fields if not connection.get(field)]
    if missing:
        raise ValueError(
            f"Connection '{name}' thiếu cấu hình: {', '.join(missing)}. "
            "Hãy bổ sung biến tương ứng trong .env."
        )
    return connection

from urllib.parse import quote_plus

def get_sqlalchemy_uri(conn: dict) -> str:
    """Build SQLAlchemy URI cho postgresql/sqlserver."""
    user = quote_plus(str(conn["user"]))
    pwd = quote_plus(str(conn["password"]))
    if conn["type"] == "postgresql":
        return (
            f"postgresql+psycopg2://{user}:{pwd}"
            f"@{conn['host']}:{conn['port']}/{conn['database']}"
        )
    if conn["type"] == "sqlserver":
        return (
            f"mssql+pyodbc://{user}:{pwd}"
            f"@{conn['host']}:{conn['port']}/{quote_plus(str(conn['database']))}"
            f"?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
        )
    raise ValueError(f"Không hỗ trợ build URI cho type: {conn['type']}")
