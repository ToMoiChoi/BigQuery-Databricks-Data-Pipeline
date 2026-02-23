# 📊 BigQuery → Databricks Data Pipeline



Pipeline Python tự động trích xuất dữ liệu từ **Google BigQuery** và upload lên **Databricks** (Delta Table).



## 📐 Kiến trúc hệ thống

```
┌──────────────┐         ┌──────────────────┐         ┌──────────────────┐
│              │  query  │                  │  INSERT  │                  │
│   Google     │───────→ │  Python Pipeline │────────→ │   Databricks     │
│   BigQuery   │  REST   │  (pandas + SQL)  │  SQL     │   Delta Table    │
│              │  API    │                  │  Conn.   │                  │
└──────────────┘         └──────────────────┘         └──────────────────┘
   Dataset:                  Modules:                    Catalog:
   datalize                  - bigquery_extract.py       datalize.view.*
   (25 tables)               - databricks_upload.py
                             - config.py
```

## 📁 Cấu trúc Project

```
├── config.py               # Load & validate cấu hình từ .env
├── bigquery_extract.py     # Module trích xuất dữ liệu từ BigQuery
├── databricks_upload.py    # Module upload dữ liệu lên Databricks
├── main.py                 # Pipeline CLI (chạy từng table)
├── run_all.py              # Pipeline batch (chạy TẤT CẢ tables)
├── requirements.txt        # Python dependencies
├── .env                    # Biến môi trường (credentials) - KHÔNG commit!
├── .env.example            # Template biến môi trường
├── gcp-key.json            # Google Cloud Service Account key - KHÔNG commit!
└── README.md               # File này
```

## 🚀 Cài đặt

### 1. Tạo virtual environment
```bash
python -m venv .venv
.venv\Scripts\Activate.ps1   # Windows PowerShell
# hoặc
source .venv/bin/activate     # Linux/Mac
```

### 2. Cài đặt dependencies
```bash
pip install -r requirements.txt
```

### 3. Cấu hình credentials

Copy `.env.example` → `.env` và điền thông tin:

```bash
cp .env.example .env
```

| Biến | Mô tả | Ví dụ |
|------|--------|-------|
| `BIGQUERY_PROJECT_ID` | GCP Project ID | `tensile-cogency-408304` |
| `BIGQUERY_CREDENTIALS_PATH` | Đường dẫn tới Service Account JSON key | `gcp-key.json` |
| `BIGQUERY_DATASET` | Dataset trên BigQuery | `datalize` |
| `DATABRICKS_HOST` | URL workspace Databricks (phải có `https://`) | `https://dbc-xxx.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | Personal Access Token | `dapi0eb9d5c...` |
| `DATABRICKS_HTTP_PATH` | HTTP Path của SQL Warehouse | `/sql/1.0/warehouses/xxx` |
| `DATABRICKS_CATALOG` | Catalog trên Databricks | `datalize` |
| `DATABRICKS_SCHEMA` | Schema trên Databricks | `view` |

> ⚠️ **Lưu ý**: KHÔNG commit `.env` và `gcp-key.json` lên Git. Thêm vào `.gitignore`.

## 📖 Hướng dẫn sử dụng

### Chạy toàn bộ dataset (run_all.py)

Lấy **TẤT CẢ** tables từ BigQuery dataset và upload lên Databricks:

```bash
python run_all.py
```

**Output mẫu:**
```
2026-02-23 11:00:00 | INFO | PIPELINE: BigQuery → Databricks (ALL TABLES)
2026-02-23 11:00:01 | INFO | Found 25 tables: ['Attendance', 'Department1', ...]
2026-02-23 11:00:02 | INFO | 📦 [1/25] Processing table: Attendance
2026-02-23 11:00:03 | INFO |    ✅ Extracted 3039 rows, 19 columns
2026-02-23 11:00:10 | INFO |    ✅ Uploaded successfully!
...
2026-02-23 11:05:00 | INFO | 🏁 PIPELINE COMPLETED in 300.0s
2026-02-23 11:05:00 | INFO |    ✅ Success: 25/25 tables
```

### Chạy từng table (main.py)

```bash
# Upload 1 bảng lên DBFS dạng Parquet
python main.py --table Attendance --method dbfs

# Upload 1 bảng vào Delta Table qua SQL INSERT
python main.py --table Attendance --method sql_insert --target attendance_table

# Chạy custom SQL query
python main.py --query "SELECT * FROM datalize.Attendance WHERE employee = 'Nguyen Van A'" \
               --method sql_insert --target filtered_data

# Giới hạn số dòng
python main.py --table Attendance --method sql_insert --limit 100 --target attendance_sample

# Liệt kê tất cả tables trong BigQuery dataset
python main.py --list-tables
```

## 🔧 Chi tiết các Module

### `config.py` — Quản lý cấu hình

| Class | Mô tả |
|-------|--------|
| `BigQueryConfig` | Load & validate BigQuery credentials từ `.env` |
| `DatabricksConfig` | Load & validate Databricks credentials từ `.env` |

Cả 2 class đều có method `validate()` kiểm tra đầy đủ config trước khi kết nối.

---

### `bigquery_extract.py` — Trích xuất dữ liệu

| Method | Mô tả | Return |
|--------|--------|--------|
| `extract_by_query(query)` | Chạy SQL query tùy chỉnh | `pandas.DataFrame` |
| `extract_table(table_name, limit)` | Lấy toàn bộ bảng | `pandas.DataFrame` |
| `list_tables(dataset)` | Liệt kê bảng trong dataset | `List[str]` |
| `get_table_schema(table_name)` | Lấy schema (tên cột, kiểu dữ liệu) | `List[dict]` |

**Cách sử dụng:**
```python
from bigquery_extract import BigQueryExtractor

extractor = BigQueryExtractor()

# Lấy toàn bộ bảng
df = extractor.extract_table("Attendance")

# Chạy query tùy chỉnh
df = extractor.extract_by_query("SELECT * FROM datalize.Attendance LIMIT 100")

# Liệt kê bảng
tables = extractor.list_tables()  # → ['Attendance', 'Department1', ...]
```

---

### `databricks_upload.py` — Upload dữ liệu

| Method | Mô tả | Khi nào dùng |
|--------|--------|--------------|
| `upload_to_dbfs(df, path, format)` | Upload file Parquet/CSV lên DBFS | Lưu trữ file, dataset lớn |
| `upload_to_delta_table(df, table, mode)` | Tạo Delta Table từ staged Parquet | Cần DBFS access |
| `write_with_sql_connector(df, table, mode)` | INSERT từng batch qua SQL | **Recommend** — không cần DBFS |

> 💡 **Khuyến nghị**: Dùng `write_with_sql_connector()` vì không yêu cầu quyền DBFS.

**Cách sử dụng:**
```python
from databricks_upload import DatabricksUploader

uploader = DatabricksUploader()

# Upload qua SQL INSERT (khuyến nghị)
uploader.write_with_sql_connector(df, "my_table", mode="overwrite")

# Upload file lên DBFS (cần quyền DBFS)
uploader.upload_to_dbfs(df, "/FileStore/data/my_table.parquet")
```

**Tính năng:**
- ✅ Tự động sanitize tên cột (loại bỏ ký tự đặc biệt cho Delta Lake)
- ✅ Xử lý datetime, boolean, NULL values đúng cách
- ✅ Batch INSERT (1000 rows/batch) — tránh quá tải SQL
- ✅ Streaming upload cho file lớn (> 1MB) trên DBFS
- ✅ Hỗ trợ mode `overwrite` và `append`

---

### `run_all.py` — Pipeline batch

Tự động:
1. Kết nối BigQuery → liệt kê tất cả tables
2. Extract từng table → `pandas.DataFrame`
3. Sanitize tên cột → loại bỏ ký tự đặc biệt
4. Upload lên Databricks qua SQL INSERT
5. Log kết quả tổng hợp (success/error count)

---

### `main.py` — Pipeline CLI

Hỗ trợ arguments:

| Argument | Mô tả | Default |
|----------|--------|---------|
| `--table, -t` | Tên bảng BigQuery | — |
| `--query, -q` | SQL query tùy chỉnh | — |
| `--method, -m` | Phương thức upload: `dbfs`, `delta`, `sql_insert` | `dbfs` |
| `--target` | Tên bảng đích trên Databricks | = source table |
| `--mode` | `overwrite` hoặc `append` | `overwrite` |
| `--format` | `parquet` hoặc `csv` (cho DBFS) | `parquet` |
| `--limit, -l` | Giới hạn số dòng | — |
| `--list-tables` | Liệt kê tất cả tables | — |

## 📋 Dữ liệu BigQuery Dataset `datalize`

Pipeline đã phát hiện **25 tables** trong dataset:

| # | Table Name | Mô tả |
|---|-----------|--------|
| 1 | `Attendance` | Dữ liệu chấm công |
| 2 | `Department1` | Thông tin phòng ban |
| 3 | `Employee_infor` | Thông tin nhân viên |
| 4 | `Employee_infor1` | Thông tin nhân viên (bản 2) |
| 5 | `Group` | Nhóm |
| 6 | `Group_v2` | Nhóm (phiên bản 2) |
| 7 | `Holiday` | Ngày nghỉ lễ |
| 8 | `Shift_ok` | Ca làm việc |
| 9 | `attendance_results_chancekim` | Kết quả chấm công |
| 10 | `dahahi_devicesList` | Danh sách thiết bị |
| 11 | `dahahi_employeesList` | Danh sách nhân viên |
| 12 | `etl_control` | ETL control metadata |
| 13 | `hubspot_companies` | Dữ liệu công ty từ HubSpot |
| 14 | `hubspot_contacts` | Dữ liệu liên hệ từ HubSpot |
| 15 | `lark_studentsInfo` | Thông tin học sinh từ Lark |
| 16 | `lark_thongtingiaovien` | Thông tin giáo viên từ Lark |
| 17 | `lark_thongtinhocvien` | Thông tin học viên từ Lark |
| 18 | `lark_thongtinlophoc` | Thông tin lớp học từ Lark |
| 19 | `larktask` | Tasks từ Lark |
| 20 | `larktasktest` | Tasks test từ Lark |
| 21 | `shopify_orders` | Đơn hàng Shopify |
| 22 | `shopify_orders_raw` | Đơn hàng Shopify (raw) |
| 23 | `test` | Bảng test |
| 24 | `test2` | Bảng test 2 |
| 25 | `vw_shopify_orders_latest` | View đơn hàng Shopify mới nhất |

## 🔒 Bảo mật

Thêm vào `.gitignore`:
```
.env
gcp-key.json
.venv/
__pycache__/
*.pyc
```

## ⚠️ Troubleshooting

| Lỗi | Nguyên nhân | Giải pháp |
|-----|-------------|-----------|
| `403 Forbidden (DBFS)` | Token không có quyền DBFS | Dùng `sql_insert` method thay vì `dbfs`/`delta` |
| `PARSE_SYNTAX_ERROR` | Giá trị datetime không được quote | Đã fix — update `databricks_upload.py` mới nhất |
| `DELTA_INVALID_CHARACTERS` | Tên cột chứa ký tự đặc biệt | Đã fix — tự động sanitize trong `run_all.py` |
| `PAT token error` | Token hết hạn | Tạo mới token trên Databricks → Settings → Developer |
| `BigQuery Storage warning` | Thiếu module storage | `pip install google-cloud-bigquery-storage` |

## 📝 Dependencies

```
google-cloud-bigquery          # BigQuery client
google-cloud-bigquery-storage  # BigQuery Storage API (tăng tốc)
pandas                         # DataFrame processing
pyarrow                        # Parquet support
db-dtypes                      # BigQuery data types
databricks-sql-connector       # Databricks SQL Connector
databricks-sdk                 # Databricks SDK
python-dotenv                  # Load .env file
requests                       # HTTP requests (DBFS API)
```
#   B i g Q u e r y - D a t a b r i c k s - D a t a - P i p e l i n e 
 
 

