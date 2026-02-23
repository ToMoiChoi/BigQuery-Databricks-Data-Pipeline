# BigQuery to Databricks Data Pipeline

A Python pipeline that automatically extracts data from **Google BigQuery** and uploads it to **Databricks** Delta Tables.

## Architecture

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

## Project Structure

```
config.py               # Load & validate config from .env
bigquery_extract.py     # BigQuery data extraction module
databricks_upload.py    # Databricks upload module
main.py                 # CLI pipeline (single table)
run_all.py              # Batch pipeline (ALL tables)
requirements.txt        # Python dependencies
.env                    # Environment variables (credentials) - DO NOT commit!
.env.example            # Environment variable template
gcp-key.json            # Google Cloud Service Account key - DO NOT commit!
README.md               # This file
```

## Setup

### 1. Create virtual environment

```bash
python -m venv .venv
.venv\Scripts\Activate.ps1   # Windows PowerShell
# or
source .venv/bin/activate     # Linux/Mac
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure credentials

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

| Variable | Description | Example |
|----------|-------------|---------|
| `BIGQUERY_PROJECT_ID` | GCP Project ID | `tensile-cogency-408304` |
| `BIGQUERY_CREDENTIALS_PATH` | Path to Service Account JSON key | `gcp-key.json` |
| `BIGQUERY_DATASET` | BigQuery dataset name | `datalize` |
| `DATABRICKS_HOST` | Databricks workspace URL (must include `https://`) | `https://dbc-xxx.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | Personal Access Token | `dapi0eb9d5c...` |
| `DATABRICKS_HTTP_PATH` | SQL Warehouse HTTP Path | `/sql/1.0/warehouses/xxx` |
| `DATABRICKS_CATALOG` | Databricks catalog | `datalize` |
| `DATABRICKS_SCHEMA` | Databricks schema | `view` |

> **Warning**: DO NOT commit `.env` and `gcp-key.json` to Git. Add them to `.gitignore`.

## Usage

### Run all tables (run_all.py)

Extract **ALL** tables from the BigQuery dataset and upload to Databricks:

```bash
python run_all.py
```

**Sample output:**

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

### Run single table (main.py)

```bash
# Upload a table to DBFS as Parquet
python main.py --table Attendance --method dbfs

# Upload a table to Delta Table via SQL INSERT
python main.py --table Attendance --method sql_insert --target attendance_table

# Run a custom SQL query
python main.py --query "SELECT * FROM datalize.Attendance WHERE employee = 'Nguyen Van A'" \
               --method sql_insert --target filtered_data

# Limit number of rows
python main.py --table Attendance --method sql_insert --limit 100 --target attendance_sample

# List all tables in the BigQuery dataset
python main.py --list-tables
```

## Module Details

### `config.py` - Configuration Management

| Class | Description |
|-------|-------------|
| `BigQueryConfig` | Load & validate BigQuery credentials from `.env` |
| `DatabricksConfig` | Load & validate Databricks credentials from `.env` |

Both classes have a `validate()` method that checks all required config before connecting.

---

### `bigquery_extract.py` - Data Extraction

| Method | Description | Return |
|--------|-------------|--------|
| `extract_by_query(query)` | Run a custom SQL query | `pandas.DataFrame` |
| `extract_table(table_name, limit)` | Extract an entire table | `pandas.DataFrame` |
| `list_tables(dataset)` | List tables in a dataset | `List[str]` |
| `get_table_schema(table_name)` | Get schema (column names, types) | `List[dict]` |

**Example usage:**

```python
from bigquery_extract import BigQueryExtractor

extractor = BigQueryExtractor()

# Extract full table
df = extractor.extract_table("Attendance")

# Run custom query
df = extractor.extract_by_query("SELECT * FROM datalize.Attendance LIMIT 100")

# List tables
tables = extractor.list_tables()  # -> ['Attendance', 'Department1', ...]
```

---

### `databricks_upload.py` - Data Upload

| Method | Description | When to use |
|--------|-------------|-------------|
| `upload_to_dbfs(df, path, format)` | Upload Parquet/CSV file to DBFS | File storage, large datasets |
| `upload_to_delta_table(df, table, mode)` | Create Delta Table from staged Parquet | Requires DBFS access |
| `write_with_sql_connector(df, table, mode)` | INSERT data in batches via SQL | **Recommended** - no DBFS needed |

> **Recommendation**: Use `write_with_sql_connector()` as it does not require DBFS permissions.

**Example usage:**

```python
from databricks_upload import DatabricksUploader

uploader = DatabricksUploader()

# Upload via SQL INSERT (recommended)
uploader.write_with_sql_connector(df, "my_table", mode="overwrite")

# Upload file to DBFS (requires DBFS permissions)
uploader.upload_to_dbfs(df, "/FileStore/data/my_table.parquet")
```

**Features:**

- Automatic column name sanitization (removes special characters for Delta Lake)
- Proper handling of datetime, boolean, NULL, array/list values
- Batch INSERT (1000 rows/batch) to avoid SQL overload
- Streaming upload for large files (> 1MB) on DBFS
- Supports `overwrite` and `append` modes

---

### `run_all.py` - Batch Pipeline

Automatically:

1. Connect to BigQuery and list all tables
2. Extract each table into a `pandas.DataFrame`
3. Sanitize column names (remove special characters)
4. Upload to Databricks via SQL INSERT
5. Log summary results (success/error count)

---

### `main.py` - CLI Pipeline

Supported arguments:

| Argument | Description | Default |
|----------|-------------|---------|
| `--table, -t` | BigQuery table name | - |
| `--query, -q` | Custom SQL query | - |
| `--method, -m` | Upload method: `dbfs`, `delta`, `sql_insert` | `dbfs` |
| `--target` | Target table name on Databricks | = source table |
| `--mode` | `overwrite` or `append` | `overwrite` |
| `--format` | `parquet` or `csv` (for DBFS) | `parquet` |
| `--limit, -l` | Limit number of rows | - |
| `--list-tables` | List all tables | - |

## BigQuery Dataset `datalize`

The pipeline detected **25 tables** in the dataset:

| # | Table Name | Description |
|---|-----------|-------------|
| 1 | `Attendance` | Attendance records |
| 2 | `Department1` | Department information |
| 3 | `Employee_infor` | Employee information |
| 4 | `Employee_infor1` | Employee information (v2) |
| 5 | `Group` | Groups |
| 6 | `Group_v2` | Groups (v2) |
| 7 | `Holiday` | Holidays |
| 8 | `Shift_ok` | Work shifts |
| 9 | `attendance_results_chancekim` | Attendance results |
| 10 | `dahahi_devicesList` | Device list |
| 11 | `dahahi_employeesList` | Employee list |
| 12 | `etl_control` | ETL control metadata |
| 13 | `hubspot_companies` | HubSpot companies data |
| 14 | `hubspot_contacts` | HubSpot contacts data |
| 15 | `lark_studentsInfo` | Student info from Lark |
| 16 | `lark_thongtingiaovien` | Teacher info from Lark |
| 17 | `lark_thongtinhocvien` | Student info from Lark |
| 18 | `lark_thongtinlophoc` | Class info from Lark |
| 19 | `larktask` | Lark tasks |
| 20 | `larktasktest` | Lark test tasks |
| 21 | `shopify_orders` | Shopify orders |
| 22 | `shopify_orders_raw` | Shopify orders (raw) |
| 23 | `test` | Test table |
| 24 | `test2` | Test table 2 |
| 25 | `vw_shopify_orders_latest` | Latest Shopify orders view |

## Security

Add to `.gitignore`:

```
.env
gcp-key.json
.venv/
__pycache__/
*.pyc
```

## Troubleshooting

| Error | Cause | Solution |
|-------|-------|----------|
| `403 Forbidden (DBFS)` | Token lacks DBFS permissions | Use `sql_insert` method instead of `dbfs`/`delta` |
| `PARSE_SYNTAX_ERROR` | Datetime values not properly quoted | Fixed - update to latest `databricks_upload.py` |
| `DELTA_INVALID_CHARACTERS` | Column names contain special characters | Fixed - auto-sanitized in `run_all.py` |
| `PAT token error` | Token expired | Generate new token: Databricks -> Settings -> Developer |
| `BigQuery Storage warning` | Missing storage module | `pip install google-cloud-bigquery-storage` |

## Dependencies

```
google-cloud-bigquery          # BigQuery client
google-cloud-bigquery-storage  # BigQuery Storage API (faster reads)
pandas                         # DataFrame processing
pyarrow                        # Parquet support
db-dtypes                      # BigQuery data types
databricks-sql-connector       # Databricks SQL Connector
databricks-sdk                 # Databricks SDK
python-dotenv                  # Load .env file
requests                       # HTTP requests (DBFS API)
```
