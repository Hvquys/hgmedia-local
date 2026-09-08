# Các thay đổi (bám Data Dictionary)

## 1. Silver models (43 file dim + fact) — sinh lại theo mapping
Trước: tất cả là `select * from staging` + PK ảo `<table>_id`.
Sau: mỗi model select **đúng cột trong Data Dictionary**, có:
- **Surrogate key** `<model>_sk = generate_surrogate_key([natural_key])` làm khóa duy nhất (`unique_key` cho incremental, test `unique + not_null`).
- **Ép kiểu đúng DATA_TYPE**: `DATE→date`, `*Datetime→timestamp`, `NUMBER(18,2)→numeric(18,2)`, `NUMBER(20)→numeric(38,0)`, `VARCHAR2/NVARCHAR*→text`.
- **Chuẩn hóa null**: `nullif(trim(...),'')` cho mọi cột.
- Cột kỹ thuật `_batch_id`, `_loaded_at as last_updated_at`.

### Quy ước khóa
- Dim tra cứu chỉ có tên (`dim_partners, dim_artist, dim_platform, dim_net, dim_ar`): `*_id` là **surrogate sinh từ tên** (nguồn báo cáo chỉ có tên).
- Bảng khóa kép (`dim_isrc, bridge_bt_vid, fact_artist_assignment, fact_evaluation_assignment, fact_label_operation, fact_youtube_operation, fact_view_stream_distro, fact_revenue_distro, fact_revenue_by_resources`): surrogate từ tổ hợp khóa tự nhiên.

### Incremental
- Fact **có cột ngày** → `incremental` (delete+insert theo `_sk`) + filter watermark `where <date> > max(<date>)`.
- Fact **không có cột ngày** (vd `bridge_bt_vid`, `fact_artist_assignment`) → `table` (tránh incremental gãy vì thiếu watermark).

## 2. `_silver_models.yml` — test & mô tả theo dictionary
- `not_null` cho cột `NULLABLE = N`.
- `relationships` (FK) cho các cột `*_id` trỏ về đúng dim (repository_id→dim_repository, project_id→dim_project, ...).
- `description` lấy nguyên văn từ dictionary.

## 3. `_sources.yml` — dọn sạch, khớp config
- Chuẩn hóa quy ước **1 bảng staging / source_id**, tên `staging.<source_id>`.
  Hiện còn 3 mapping TODO trong `dim_distributed_employee` cần xác nhận nghiệp vụ.

## 4. `config/db_sources.yaml` — đồng bộ tên bảng staging
- Bỏ prefix hệ nguồn: `staging.odoo_dim_project → staging.dim_project`, `staging.channel_dim_company → staging.dim_company`, ... để khớp `source()` trong dbt.

## 5. `staging_loader.py` — sửa bug lần chạy đầu
- `load_mode=truncate` trước đây `TRUNCATE` bảng chưa tồn tại → lỗi. Đổi sang `CREATE SCHEMA IF NOT EXISTS` + `to_sql(if_exists='replace')`.

## Cần bạn xử lý tiếp
- **Tên cột raw ở staging**: models giả định `tên raw == tên target`. Với nguồn SQL (Odoo `x_project`, SQL Server...) tên cột raw khác → sửa **vế trái** trong SELECT của model tương ứng (mỗi file có ghi chú ở đầu).
- **Bổ sung config EL** cho ~35 nguồn còn lại trong `config/db_sources.yaml` & `google_sheet_sources.yaml` (hiện mới có entry mẫu).
- Chạy: `dbt deps` (cần mạng tới hub.getdbt.com), rồi `dbt build`.
