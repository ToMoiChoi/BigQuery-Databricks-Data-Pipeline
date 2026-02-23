# BigQuery to Databricks Data Pipeline

Pipeline Python tu dong trich xuat du lieu tu **Google BigQuery** va upload len **Databricks** (Delta Table).

## Kien truc he thong

```
+----------------+         +------------------+         +------------------+
|                |  query  |                  |  INSERT  |                  |
|   Google       |-------> |  Python Pipeline |--------> |   Databricks     |
|   BigQuery     |  REST   |  (pandas + SQL)  |  SQL     |   Delta Table    |
|                |  API    |                  |  Conn.   |                  |
+----------------+         +------------------+         +------------------+
   Dataset:                  Modules:                    Catalog:
   datalize                  - bigquery_extract.py       datalize.view.*
   (25 tables)               - databricks_upload.py
                             - config.py
```

## Cau truc Project

```
config.py               # Load & validate cau hinh tu .env
bigquery_extract.py     # Module trich xuat du lieu tu BigQuery
databricks_upload.py    # Module upload du lieu len Databricks
main.py                 # Pipeline CLI (chay tung table)
run_all.py              # Pipeline batch (chay TAT CA tables)
requirements.txt        # Python dependencies
.env                    # Bien moi truong (credentials) - KHONG commit!
.env.example            # Template bien moi truong
gcp-key.json            # Google Cloud Service Account key - KHONG commit!
README.md               # File nay
```

## Cai dat

### 1. Tao virtual environment

```bash
python -m venv .venv
.venv\Scripts\Activate.ps1   # Windows PowerShell
# hoac
source .venv/bin/activate     # Linux/Mac
```

### 2. Cai dat dependencies

```bash
pip install -r requirements.txt
```

### 3. Cau hinh credentials

Copy `.env.example` thanh `.env` va dien thong tin:

```bash
cp .env.example .env
```

| Bien | Mo ta | Vi du |
|------|-------|-------|
| `BIGQUERY_PROJECT_ID` | GCP Project ID | `tensile-cogency-408304` |
| `BIGQUERY_CREDENTIALS_PATH` | Duong dan toi Service Account JSON key | `gcp-key.json` |
| `BIGQUERY_DATASET` | Dataset tren BigQuery | `datalize` |
| `DATABRICKS_HOST` | URL workspace Databricks (phai co `https://`) | `https://dbc-xxx.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | Personal Access Token | `dapi0eb9d5c...` |
| `DATABRICKS_HTTP_PATH` | HTTP Path cua SQL Warehouse | `/sql/1.0/warehouses/xxx` |
| `DATABRICKS_CATALOG` | Catalog tren Databricks | `datalize` |
| `DATABRICKS_SCHEMA` | Schema tren Databricks | `view` |

> **Luu y**: KHONG commit `.env` va `gcp-key.json` len Git. Them vao `.gitignore`.

## Huong dan su dung

### Chay toan bo dataset (run_all.py)

Lay **TAT CA** tables tu BigQuery dataset va upload len Databricks:

```bash
python run_all.py
```

**Output mau:**

```
2026-02-23 11:00:00 | INFO | PIPELINE: BigQuery -> Databricks (ALL TABLES)
2026-02-23 11:00:01 | INFO | Found 25 tables: ['Attendance', 'Department1', ...]
2026-02-23 11:00:02 | INFO | [1/25] Processing table: Attendance
2026-02-23 11:00:03 | INFO |    Extracted 3039 rows, 19 columns
2026-02-23 11:00:10 | INFO |    Uploaded successfully!
...
2026-02-23 11:05:00 | INFO | PIPELINE COMPLETED in 300.0s
2026-02-23 11:05:00 | INFO |    Success: 25/25 tables
```

### Chay tung table (main.py)

```bash
# Upload 1 bang len DBFS dang Parquet
python main.py --table Attendance --method dbfs

# Upload 1 bang vao Delta Table qua SQL INSERT
python main.py --table Attendance --method sql_insert --target attendance_table

# Chay custom SQL query
python main.py --query "SELECT * FROM datalize.Attendance WHERE employee = 'Nguyen Van A'" \
               --method sql_insert --target filtered_data

# Gioi han so dong
python main.py --table Attendance --method sql_insert --limit 100 --target attendance_sample

# Liet ke tat ca tables trong BigQuery dataset
python main.py --list-tables
```

## Chi tiet cac Module

### `config.py` - Quan ly cau hinh

| Class | Mo ta |
|-------|-------|
| `BigQueryConfig` | Load & validate BigQuery credentials tu `.env` |
| `DatabricksConfig` | Load & validate Databricks credentials tu `.env` |

Ca 2 class deu co method `validate()` kiem tra day du config truoc khi ket noi.

---

### `bigquery_extract.py` - Trich xuat du lieu

| Method | Mo ta | Return |
|--------|-------|--------|
| `extract_by_query(query)` | Chay SQL query tuy chinh | `pandas.DataFrame` |
| `extract_table(table_name, limit)` | Lay toan bo bang | `pandas.DataFrame` |
| `list_tables(dataset)` | Liet ke bang trong dataset | `List[str]` |
| `get_table_schema(table_name)` | Lay schema (ten cot, kieu du lieu) | `List[dict]` |

**Cach su dung:**

```python
from bigquery_extract import BigQueryExtractor

extractor = BigQueryExtractor()

# Lay toan bo bang
df = extractor.extract_table("Attendance")

# Chay query tuy chinh
df = extractor.extract_by_query("SELECT * FROM datalize.Attendance LIMIT 100")

# Liet ke bang
tables = extractor.list_tables()  # -> ['Attendance', 'Department1', ...]
```

---

### `databricks_upload.py` - Upload du lieu

| Method | Mo ta | Khi nao dung |
|--------|-------|--------------|
| `upload_to_dbfs(df, path, format)` | Upload file Parquet/CSV len DBFS | Luu tru file, dataset lon |
| `upload_to_delta_table(df, table, mode)` | Tao Delta Table tu staged Parquet | Can DBFS access |
| `write_with_sql_connector(df, table, mode)` | INSERT tung batch qua SQL | **Recommend** - khong can DBFS |

> **Khuyen nghi**: Dung `write_with_sql_connector()` vi khong yeu cau quyen DBFS.

**Cach su dung:**

```python
from databricks_upload import DatabricksUploader

uploader = DatabricksUploader()

# Upload qua SQL INSERT (khuyen nghi)
uploader.write_with_sql_connector(df, "my_table", mode="overwrite")

# Upload file len DBFS (can quyen DBFS)
uploader.upload_to_dbfs(df, "/FileStore/data/my_table.parquet")
```

**Tinh nang:**

- Tu dong sanitize ten cot (loai bo ky tu dac biet cho Delta Lake)
- Xu ly datetime, boolean, NULL values dung cach
- Batch INSERT (1000 rows/batch) - tranh qua tai SQL
- Streaming upload cho file lon (> 1MB) tren DBFS
- Ho tro mode `overwrite` va `append`

---

### `run_all.py` - Pipeline batch

Tu dong:

1. Ket noi BigQuery -> liet ke tat ca tables
2. Extract tung table -> `pandas.DataFrame`
3. Sanitize ten cot -> loai bo ky tu dac biet
4. Upload len Databricks qua SQL INSERT
5. Log ket qua tong hop (success/error count)

---

### `main.py` - Pipeline CLI

Ho tro arguments:

| Argument | Mo ta | Default |
|----------|-------|---------|
| `--table, -t` | Ten bang BigQuery | - |
| `--query, -q` | SQL query tuy chinh | - |
| `--method, -m` | Phuong thuc upload: `dbfs`, `delta`, `sql_insert` | `dbfs` |
| `--target` | Ten bang dich tren Databricks | = source table |
| `--mode` | `overwrite` hoac `append` | `overwrite` |
| `--format` | `parquet` hoac `csv` (cho DBFS) | `parquet` |
| `--limit, -l` | Gioi han so dong | - |
| `--list-tables` | Liet ke tat ca tables | - |

## Du lieu BigQuery Dataset `datalize`

Pipeline da phat hien **25 tables** trong dataset:

| # | Table Name | Mo ta |
|---|-----------|-------|
| 1 | `Attendance` | Du lieu cham cong |
| 2 | `Department1` | Thong tin phong ban |
| 3 | `Employee_infor` | Thong tin nhan vien |
| 4 | `Employee_infor1` | Thong tin nhan vien (ban 2) |
| 5 | `Group` | Nhom |
| 6 | `Group_v2` | Nhom (phien ban 2) |
| 7 | `Holiday` | Ngay nghi le |
| 8 | `Shift_ok` | Ca lam viec |
| 9 | `attendance_results_chancekim` | Ket qua cham cong |
| 10 | `dahahi_devicesList` | Danh sach thiet bi |
| 11 | `dahahi_employeesList` | Danh sach nhan vien |
| 12 | `etl_control` | ETL control metadata |
| 13 | `hubspot_companies` | Du lieu cong ty tu HubSpot |
| 14 | `hubspot_contacts` | Du lieu lien he tu HubSpot |
| 15 | `lark_studentsInfo` | Thong tin hoc sinh tu Lark |
| 16 | `lark_thongtingiaovien` | Thong tin giao vien tu Lark |
| 17 | `lark_thongtinhocvien` | Thong tin hoc vien tu Lark |
| 18 | `lark_thongtinlophoc` | Thong tin lop hoc tu Lark |
| 19 | `larktask` | Tasks tu Lark |
| 20 | `larktasktest` | Tasks test tu Lark |
| 21 | `shopify_orders` | Don hang Shopify |
| 22 | `shopify_orders_raw` | Don hang Shopify (raw) |
| 23 | `test` | Bang test |
| 24 | `test2` | Bang test 2 |
| 25 | `vw_shopify_orders_latest` | View don hang Shopify moi nhat |

## Bao mat

Them vao `.gitignore`:

```
.env
gcp-key.json
.venv/
__pycache__/
*.pyc
```

## Troubleshooting

| Loi | Nguyen nhan | Giai phap |
|-----|-------------|-----------|
| `403 Forbidden (DBFS)` | Token khong co quyen DBFS | Dung `sql_insert` method thay vi `dbfs`/`delta` |
| `PARSE_SYNTAX_ERROR` | Gia tri datetime khong duoc quote | Da fix - update `databricks_upload.py` moi nhat |
| `DELTA_INVALID_CHARACTERS` | Ten cot chua ky tu dac biet | Da fix - tu dong sanitize trong `run_all.py` |
| `PAT token error` | Token het han | Tao moi token tren Databricks -> Settings -> Developer |
| `BigQuery Storage warning` | Thieu module storage | `pip install google-cloud-bigquery-storage` |

## Dependencies

```
google-cloud-bigquery          # BigQuery client
google-cloud-bigquery-storage  # BigQuery Storage API (tang toc)
pandas                         # DataFrame processing
pyarrow                        # Parquet support
db-dtypes                      # BigQuery data types
databricks-sql-connector       # Databricks SQL Connector
databricks-sdk                 # Databricks SDK
python-dotenv                  # Load .env file
requests                       # HTTP requests (DBFS API)
```
