# HGMEDIA local runbook

Tài liệu này là hướng dẫn chuẩn cho môi trường local đã hoàn thành đến Phase 13.
Phạm vi kiểm chứng là vertical slice `phase6_sales`; các nguồn nghiệp vụ khác chỉ
được chạy khi đã có dữ liệu và credential tương ứng.

## 1. Phiên bản đã kiểm chứng

| Thành phần | Phiên bản |
|---|---|
| Windows + PowerShell | Windows 11 / PowerShell 5.1 hoặc mới hơn |
| Git | 2.55.0.windows.3 |
| Python | 3.11.9 (64-bit) |
| Docker Engine | 29.7.2 |
| Docker Compose | v5.5.0 |
| Airflow image | 3.3.1, Python 3.11 |
| PostgreSQL DWH | 15 |
| PostgreSQL Airflow metadata | 16 |
| dbt-postgres | 1.11.0 |
| Great Expectations | 1.20.0 |

Các phiên bản Git và Docker mới hơn có thể dùng được nhưng chưa nằm trong bằng
chứng POC này.

## 2. Cài đặt lần đầu

Mở PowerShell tại thư mục repository:

```powershell
Set-Location D:\Projects\hgmedia
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
$env:PYTHONUTF8 = "1"
python -m pip install --upgrade pip setuptools wheel
python -m pip install -r requirements.txt
python -m pip install dbt-postgres==1.11.0
Copy-Item .env.example .env
```

Mở `.env` và thay các giá trị `change_me` cần cho môi trường local. Với POC
`phase6_sales`, các nhóm cần thiết là:

- `DWH_PG_*` và `POSTGRES_*`: dùng cùng database, username và password khi tạo
  volume DWH lần đầu.
- `MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY`: dùng cùng giá trị với
  `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD` vì POC chưa tạo MinIO user riêng.
- `AIRFLOW_DB_*`: tài khoản PostgreSQL metadata nội bộ của Airflow.
- `AIRFLOW_ADMIN_*`: tài khoản đăng nhập giao diện Airflow.
- `AIRFLOW_JWT_SECRET`: một chuỗi ngẫu nhiên dài.

Các biến Odoo, SQL Server, Google Sheet và Elasticsearch chưa cần điền để chạy
`phase6_sales`. Không commit `.env` hoặc file service-account JSON.

Kiểm tra cấu hình trước khi tạo container:

```powershell
docker compose config --quiet
python -m pip check
```

Không có output từ `docker compose config --quiet` nghĩa là cấu hình hợp lệ.

## 3. Khởi động và dừng hệ thống

Khởi động lần đầu hoặc sau khi Dockerfile thay đổi:

```powershell
docker compose up -d --build
Start-Sleep -Seconds 45
docker compose ps -a
```

Trạng thái mong đợi:

- PostgreSQL DWH, PostgreSQL Airflow và các service Airflow là `healthy`.
- MinIO là `Up`.
- `airflow_init` là `Exited (0)`; đây là kết quả thành công của service khởi tạo
  một lần.

Kiểm tra Airflow:

```powershell
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler airflow dags list
```

Mở các giao diện:

- Airflow: <http://localhost:8080>
- MinIO Console: <http://localhost:9001>
- PostgreSQL DWH: `localhost:5432`, database và tài khoản lấy từ `.env`

Dừng tạm thời, vẫn giữ container và dữ liệu:

```powershell
docker compose stop
```

Khởi động lại:

```powershell
docker compose start
```

Xóa container và network nhưng giữ named volume:

```powershell
docker compose down
```

`docker compose down -v` xóa cả dữ liệu PostgreSQL, MinIO và Airflow; chỉ dùng
khi chủ động muốn tạo lại môi trường trắng.

## 4. Chạy pipeline thủ công

Kích hoạt virtual environment rồi chạy full load:

```powershell
Set-Location D:\Projects\hgmedia
.\.venv\Scripts\Activate.ps1
$env:PYTHONUTF8 = "1"
python main.py run --id phase6_sales --full
```

Nạp biến trong `.env` vào PowerShell để dbt đọc được cấu hình:

```powershell
Get-Content .env |
  Where-Object { $_ -match '^[A-Z][A-Z0-9_]*=' } |
  ForEach-Object {
    $name, $value = $_.Split('=', 2)
    Set-Item -Path "Env:$name" -Value $value
  }

Push-Location dwh_dbt
dbt deps
dbt run --profiles-dir . --threads 1 --select fact_phase6_sales mart_phase6_daily_sales
Pop-Location

python scripts/run_data_quality.py --id phase6_sales
python scripts/reconcile_phase6_sales.py
```

Kết quả đạt yêu cầu khi dbt tạo thành công hai model, Data Quality đạt 9/9 và
reconciliation đạt 15/15.

Các lệnh vận hành bổ sung:

```powershell
# Incremental; nguồn không đổi sẽ được skip
python main.py run --id phase6_sales

# Xem lịch sử batch
python main.py history --id phase6_sales

# Chạy lại một batch lỗi từ Bronze
python main.py retry --id phase6_sales --batch-id <batch_id>

# Kiểm tra transaction và rollback của staging loader
python scripts/validate_pipeline_safety.py
```

## 5. Chạy bằng Airflow

### Smoke test Phase 11

```powershell
docker compose exec airflow-scheduler bash /opt/airflow/project/scripts/trigger_phase11_airflow.sh
docker compose exec airflow-scheduler airflow dags list-runs el_csv_pipeline
docker compose exec airflow-scheduler bash /opt/airflow/project/scripts/validate_phase11_airflow.sh
```

### End-to-end Phase 12

Trigger lần chạy bình thường:

```powershell
docker compose exec airflow-scheduler bash /opt/airflow/project/scripts/trigger_phase12_airflow.sh success
```

Đợi DAG hoàn thành, sau đó kiểm tra:

```powershell
docker compose exec airflow-scheduler airflow dags list-runs phase12_sales_e2e
docker compose exec airflow-scheduler python /opt/airflow/project/scripts/verify_phase12_airflow_metadata.py
```

Kiểm tra retry/recovery:

```powershell
docker compose exec airflow-scheduler bash /opt/airflow/project/scripts/trigger_phase12_airflow.sh recovery
docker compose exec airflow-scheduler python /opt/airflow/project/scripts/verify_phase12_airflow_metadata.py --expect-retry
```

Run recovery cần có `failure_gate` ở `try_number=2`; các task khác thành công và
verifier kết thúc bằng `Final status = PASSED`.

## 6. Xử lý lỗi thường gặp

### Compose báo thiếu biến môi trường

Chạy `docker compose config --quiet`, đọc tên biến trong thông báo và bổ sung
vào `.env`. Không thêm password trực tiếp vào `docker-compose.yml`.

### Port 5432, 8080, 9000 hoặc 9001 đang được sử dụng

```powershell
Get-NetTCPConnection -State Listen -LocalPort 5432,8080,9000,9001 |
  Select-Object LocalAddress,LocalPort,OwningProcess
```

Dừng ứng dụng đang chiếm port rồi chạy lại Compose.

### Container không healthy

```powershell
docker compose ps -a
docker compose logs --tail 150 postgres minio airflow-postgres airflow-apiserver airflow-scheduler airflow-dag-processor
```

Sửa lỗi đầu tiên xuất hiện trong log rồi chạy `docker compose up -d` lại.

### Airflow không thấy DAG

```powershell
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose logs --tail 150 airflow-dag-processor
```

Airflow 3 không hỗ trợ `airflow tasks list <dag_id> --tree`; dùng
`airflow tasks list <dag_id>`.

### PowerShell làm hỏng JSON của `airflow dags trigger`

Dùng `trigger_phase11_airflow.sh` hoặc `trigger_phase12_airflow.sh`. Hai script
đọc JSON ngay trong container nên không phụ thuộc cách PowerShell escape dấu
nháy.

### `No paused DAGs were found`

Đây không phải lỗi; DAG đã ở trạng thái unpaused và script vẫn tiếp tục trigger.

### Đổi password Airflow nhưng không đăng nhập được

Đổi `.env` không cập nhật user đã tồn tại trong Airflow metadata DB. Đồng bộ lại:

```powershell
$airflowUser = ((Get-Content .env | Where-Object { $_ -match '^AIRFLOW_ADMIN_USERNAME=' }) -split '=', 2)[1]
$airflowPassword = ((Get-Content .env | Where-Object { $_ -match '^AIRFLOW_ADMIN_PASSWORD=' }) -split '=', 2)[1]
docker compose exec airflow-apiserver airflow users reset-password --username $airflowUser --password $airflowPassword
```

### dbt trên Windows báo lỗi tạo process hoặc pipe

Chạy dbt trong container Airflow với một thread:

```powershell
docker compose exec airflow-scheduler bash -lc 'cd /opt/airflow/project/dwh_dbt && dbt run --profiles-dir . --threads 1 --select fact_phase6_sales mart_phase6_daily_sales'
```

### `stream_distro` báo thiếu file

Nguồn này cần `data/fact_view_stream.csv` và không thuộc vertical slice đã kiểm
chứng. Không chọn `stream_distro` khi file chưa được cung cấp.

### Python báo `UnicodeEncodeError` khi in trợ giúp tiếng Việt

Terminal đang dùng encoding cũ của Windows. Đặt UTF-8 cho phiên PowerShell hiện
tại rồi chạy lại lệnh:

```powershell
$env:PYTHONUTF8 = "1"
python main.py --help
```

## 7. Kết quả và giới hạn POC

Kết quả được xác nhận ngày 2026-09-08:

- `phase6_sales`: 5 dòng, tổng `amount` là 9.600.000.
- Bronze, Staging, Silver và Gold đối soát 15/15.
- Great Expectations đạt 9/9.
- Unit tests đạt 21/21.
- Kiểm tra atomic staging load đạt 6/6, bao gồm rollback khi replacement lỗi.
- Phase 12 chạy đủ 7 task và hoàn thành `success`.

POC local được xem là hoàn tất với `phase6_sales`. Các nguồn database nghiệp vụ,
Google Sheet, Elasticsearch, `stream_distro` và các mapping còn placeholder chỉ
được hoàn thiện khi thực sự đưa nguồn đó vào phạm vi. Chúng không chặn việc dựng
và vận hành POC local.

## 8. Kiểm tra trước khi bàn giao

```powershell
docker compose config --quiet
python -m pip check
python -m unittest discover -s tests -v
docker compose ps -a
docker compose exec airflow-scheduler airflow dags list-import-errors
```

Sau đó chạy một Phase 12 success và verifier theo mục 5. Nếu tất cả đạt, người
nhận bàn giao có thể dựng lại và vận hành vertical slice local.
