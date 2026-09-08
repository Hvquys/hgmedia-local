   # DWH Pipeline — Google Sheet + Database → MinIO → Postgres (staging) → dbt (silver/gold)

Pipeline EL (Extract-Load) bằng Python cho Google Sheet, database, CSV/FX,
Elasticsearch và API, kết hợp dbt để transform. Nhánh `phase6_sales` là vertical
slice đã được kiểm thử đầy đủ; xem `docs/PROJECT_REVIEW.md` để biết phạm vi còn
phải hoàn thiện trước production.

## Kiến trúc

```
[Google Sheet]  [Odoo PG + 3 SQL Server]
       │                  │
       ▼                  ▼
   Task 1: EXTRACT (Python) — đọc thô, KHÔNG transform
       │
       ▼
     MinIO (bronze/<source_type>/<source_id>/<batch_id>/data.parquet)
       │           SourceRegistry (meta._source_registry) ghi batch_id, watermark/checksum
       ▼
   Task 2: LOAD (Python) — load thô vào Postgres schema "staging"
       │
       ▼
   dbt models — business rule (join, tính cột phái sinh) bằng SQL
       │
       ▼
   Postgres schema "silver" (Dim/Fact chuẩn DWH) → "gold" (mart/báo cáo)
```

Task 1 và Task 2 độc lập nhau (đúng kiểu Airflow): Task 2 chỉ cần `batch_id + minio_path`,
không cần connection nguồn gốc còn sống.

## Cài đặt dev local

```powershell
Copy-Item .env.example .env
# Sửa toàn bộ giá trị change_me trong .env trước khi chạy.
docker compose config --quiet
pip install -r requirements.txt
docker compose up -d
```

PostgreSQL chỉ mở tại `127.0.0.1:5432`, MinIO API/Console tại
`127.0.0.1:9000/9001` và Airflow tại `127.0.0.1:8080`. Tên đăng nhập và mật
khẩu được đọc từ `.env`; repository không chứa giá trị thật.

## Dùng CLI

```bash
# Chạy 1 source cụ thể
python main.py run --id dim_partners

# Bỏ qua check has_changed, luôn extract lại
python main.py run --id fact_distribution --force

# Full Load: extract lại và replace toàn bộ bảng Staging
python main.py run --id phase6_sales --full

# Chạy Incremental; nếu checksum không đổi, pipeline tự skip
python main.py run --id phase6_sales

# Retry batch lỗi từ đúng Parquet đã lưu trên MinIO
python main.py retry --id phase6_sales --batch-id phase6_sales_YYYYMMDD_HHMMSS_microseconds

# Chạy tất cả nguồn theo loại
python main.py run --type google_sheet
python main.py run --type sql

# Rollback 1 hoặc nhiều source về batch cũ theo ngày
python main.py rollback --date 2026-06-01 --ids fact_distribution
python main.py rollback --date 2026-06-01    # rollback tất cả source trong config

# Xem lịch sử extract/load
python main.py history --id dim_partners
```

## Chạy dbt

```bash
cd dwh_dbt
dbt deps                 # cài dbt_utils
dbt run --profiles-dir .
dbt test --profiles-dir .
```

## Chạy qua Airflow

Airflow 3 chạy trong Linux containers trên Docker Desktop. Cấu hình local dùng
`LocalExecutor`, một PostgreSQL metadata riêng và mount trực tiếp `src/dags` vào
thư mục DAG của Airflow.

### `.env` và tài khoản Airflow

Docker Compose tự đọc file `.env` ở cùng thư mục để thay `${TEN_BIEN}` trong
`docker-compose.yml`. Các biến Airflow không còn giá trị mặc định trong Compose;
nếu thiếu, `docker compose config --quiet` sẽ dừng và báo đúng tên biến cần bổ
sung.

- `AIRFLOW_DB_USER`, `AIRFLOW_DB_PASSWORD`, `AIRFLOW_DB_NAME`: tài khoản của
  PostgreSQL metadata nội bộ.
- `AIRFLOW_ADMIN_USERNAME`, `AIRFLOW_ADMIN_PASSWORD`: tài khoản đăng nhập giao
  diện Airflow.
- `AIRFLOW_JWT_SECRET`: khóa ký token API; cần là chuỗi ngẫu nhiên dài.
- `AIRFLOW_UID`: UID Linux của tiến trình trong container; trên Windows có thể
  giữ `50000`.

`_AIRFLOW_WWW_USER_CREATE` chỉ tạo tài khoản ở lần khởi tạo đầu. Nếu đổi
`AIRFLOW_ADMIN_PASSWORD` sau khi volume metadata đã tồn tại, đồng bộ lại bằng:

```powershell
$airflowUser = ((Get-Content .env | Where-Object { $_ -match '^AIRFLOW_ADMIN_USERNAME=' }) -split '=', 2)[1]
$airflowPassword = ((Get-Content .env | Where-Object { $_ -match '^AIRFLOW_ADMIN_PASSWORD=' }) -split '=', 2)[1]
docker compose exec airflow-apiserver airflow users reset-password --username $airflowUser --password $airflowPassword
```

```powershell
# Khởi tạo metadata DB và tài khoản admin
docker compose build airflow-init
docker compose up airflow-init

# Khởi động các thành phần Airflow
docker compose up -d airflow-apiserver airflow-scheduler airflow-dag-processor airflow-triggerer
docker compose ps

# Kiểm tra import và dependency của DAG Phase 11
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler airflow tasks list el_csv_pipeline

# Trigger bài kiểm thử Phase 11 (đọc JSON trong container để tránh lỗi escape PowerShell)
docker compose exec airflow-scheduler airflow dags unpause -y el_csv_pipeline
docker compose exec airflow-scheduler bash /opt/airflow/project/scripts/trigger_phase11_airflow.sh
docker compose exec airflow-scheduler airflow dags list-runs el_csv_pipeline
docker compose exec airflow-scheduler bash /opt/airflow/project/scripts/validate_phase11_airflow.sh
```

Mở `http://localhost:8080`, đăng nhập bằng `AIRFLOW_ADMIN_USERNAME` và
`AIRFLOW_ADMIN_PASSWORD` trong `.env`, sau đó unpause `el_csv_pipeline` và trigger
với `selected_tables=["phase6_sales"]`, `force=true`. DAG có chuỗi task
`get_sources -> extract -> load`; kết quả extract chứa `batch_id` và `minio_path`
được truyền sang task load qua XCom. Task có 1 lần retry, timeout 30 phút và toàn
DAG có timeout 1 giờ.

- `el_csv_pipeline` — 5h sáng mỗi ngày; mặc định chạy mẫu local `phase6_sales`.
- `el_google_sheet_pipeline` — 6h sáng mỗi ngày, Dynamic Task Mapping qua `config/google_sheet_sources.yaml`.
- `el_database_pipeline` — mỗi 4 tiếng, Dynamic Task Mapping qua `config/db_sources.yaml`.
- `el_elastic_pipeline` — mỗi 6 tiếng, Dynamic Task Mapping qua `config/elastic_sources.yaml`.
- `dbt_transform_pipeline` — chờ các EL DAG rồi chạy `dbt run` và `dbt test`.

### Phase 12: chạy vertical slice end-to-end

`phase12_sales_e2e` chạy một batch `phase6_sales` xuyên suốt bằng Airflow:

```text
get_source → extract → failure_gate → load → data_quality → dbt_build → reconcile
```

Trigger lần chạy bình thường và kiểm tra metadata:

```powershell
docker compose exec airflow-scheduler bash /opt/airflow/project/scripts/trigger_phase12_airflow.sh success
docker compose exec airflow-scheduler python /opt/airflow/project/scripts/verify_phase12_airflow_metadata.py
```

Kiểm tra recovery: `failure_gate` cố ý lỗi ở lần đầu, sau 10 giây Airflow retry
và tiếp tục cùng batch từ XCom:

```powershell
docker compose exec airflow-scheduler bash /opt/airflow/project/scripts/trigger_phase12_airflow.sh recovery
docker compose exec airflow-scheduler python /opt/airflow/project/scripts/verify_phase12_airflow_metadata.py --expect-retry
```

Verifier yêu cầu cả 7 task thành công, batch có trạng thái `loaded`, DQ đạt 9/9,
reconciliation đạt 15/15 và metadata DQ liên kết đúng Airflow DAG run.

## Thêm 1 nguồn mới

1. Thêm entry vào `config/google_sheet_sources.yaml` (nếu Google Sheet) hoặc
   `config/db_sources.yaml` (nếu Database) — đủ `source_id`, `target_staging_table`, `load_mode`.
2. Nếu là DB mới (connection mới): khai báo thêm trong `src/connections.py`.
3. `python main.py run --id <source_id>` để test ngay, không cần sửa code.
4. Viết model dbt tương ứng trong `dwh_dbt/models/silver/dim/` hoặc `.../fact/`,
   `ref()` tới các Dim cần thiết — dbt tự lo thứ tự build.
5. Khai báo bảng staging mới vào `dwh_dbt/models/staging/_sources.yml`.

## Khác biệt so với pipeline Excel cũ (MISA)

| | Cũ (Excel-only) | Mới |
|---|---|---|
| Loại nguồn | Chỉ Excel | Google Sheet + nhiều DB (Postgres/SQL Server) cùng lúc |
| Phát hiện thay đổi | MD5 file | MD5 (Google Sheet) hoặc watermark column (DB, incremental) |
| Business logic | `transformers/*.py` (pandas, 1 file/bảng) | dbt models (SQL), `ref()` tự suy dependency |
| Thứ tự load | `PRIORITY_FILE_IDS` (list phẳng, hard-code) | dbt DAG tự động qua `ref()` |
| SCD / lịch sử Dim | Chưa hỗ trợ | dbt snapshot (xem `dwh_dbt/snapshots/`, cần bổ sung khi có yêu cầu) |
| Data quality | Không có | dbt tests (`not_null`, `unique`, `relationships`) |

## Thứ tự build Dim/Fact (9 layer theo Data Dictionary)

Các model trong `dwh_dbt/models/` được nối bằng `ref()` để dbt xác định thứ tự
phụ thuộc. Hiện còn 3 trường TODO trong `dim_distributed_employee` cần mapping
nghiệp vụ nguồn thật. Các model khác vẫn cần được đối chiếu với Data Dictionary
và dữ liệu nguồn trước khi coi là sẵn sàng production.

Build thử riêng 1 nhánh (model đó + mọi thứ nó phụ thuộc):
```bash
dbt run --select +fact_distribution
```
Build toàn bộ theo đúng thứ tự layer:
```bash
dbt run
```
Xem sơ đồ DAG trực quan sau khi đã có dữ liệu/docs:
```bash
dbt docs generate && dbt docs serve
```


## Test kết nối trước khi chạy pipeline thật

1. Copy `.env.example` thành `.env`, điền thông tin thật (host/port/db/user/password cho từng nguồn).
2. Cài thêm dependency nếu chưa có: `pip install python-dotenv`.
3. Với Google Sheet: tải file JSON service account từ Google Cloud Console, đặt vào
   `config/gsheet_service_account.json` (hoặc đường dẫn khác, sửa `GSHEET_CREDENTIALS_PATH` trong `.env`),
   rồi **share quyền View** Google Sheet đó cho email của service account (dạng `...@...iam.gserviceaccount.com`).
4. Chạy:
   ```bash
   python scripts/test_connections.py
   ```
   Script chỉ test connect (SELECT 1 / list bucket / đọc credential file), không extract/load dữ liệu thật.
   Kết quả in `[OK]` hoặc `[FAIL] <lý do>` cho từng nguồn — sửa theo đúng lỗi báo ra.

## TODO khi triển khai thật

- Điền connection thật vào `.env`; `src/connections.py` chỉ ánh xạ tên biến và không chứa mật khẩu.
- Điền `spreadsheet_id` thật + tạo Google Service Account, đặt credential JSON tại
  `config/gsheet_service_account.json`, share quyền view cho service account email lên từng Sheet.
- Xác nhận watermark column cho các bảng DB hiện đang để `watermark_column: null` (full reload).
- Bổ sung ~30 entry còn lại trong `config/db_sources.yaml` (mẫu hiện chỉ có vài bảng tiêu biểu)
  và ~25 dbt model còn lại theo 8 layer phụ thuộc đã phân tích từ Data Dictionary.
- Quyết định SCD Type 1 hay Type 2 cho từng Dim, viết `dwh_dbt/snapshots/*.sql` nếu cần Type 2.
- Kiểm thử Elasticsearch bằng dữ liệu thật và bổ sung DQ/reconciliation trước khi unpause DAG.

## Thiết lập Retention Policy Minio 7 ngày
Bước 1 — Cài MinIO Client (`mc`)
Mục đích: cài command-line client để quản lý MinIO và lifecycle policy từ WSL.

```bash
curl -fsSL https://dl.min.io/client/mc/release/linux-amd64/mc -o /tmp/mc
chmod +x /tmp/mc
sudo install /tmp/mc /usr/local/bin/mc

mc --version
```

**Bước 2 — Lấy password MinIO runtime**

```bash
sudo grep MINIO_ROOT_PASSWORD /etc/systemd/system/minio.service
```

**Bước 3 — Tạo alias mc đến MinIO**

```bash
read -s MINIO_PASSWORD
```

Nhập giá trị MINIO_ROOT_PASSWORD ở bước 2 rồi nhấn Enter. Password sẽ không hiển thị trên terminal.

**Bước 4 — Kết nối mc đến MinIO hiện tại**

```bash
mc alias set dwh-minio http://localhost:9000 hgmedia "$MINIO_PASSWORD"
```

**Bước 5 — Set retention 7 ngày cho toàn bộ raw-bronze**

```bash
mc ilm rule add dwh-minio/raw-bronze --expire-days 7
```

**Bước 6 — Xác nhận thành công**

```bash
mc ilm rule ls dwh-minio/raw-bronze
```

Bạn cần thấy rule có DAYS TO EXPIRE là 7
