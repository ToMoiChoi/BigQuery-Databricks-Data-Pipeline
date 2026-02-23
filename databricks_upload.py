"""
Databricks Upload Module.

Upload dữ liệu lên Databricks qua 2 phương thức:
1. DBFS - Upload file Parquet lên Databricks File System
2. Delta Table - Ghi trực tiếp vào table bằng SQL Connector
"""

import io
import os
import logging
import base64
import tempfile
import requests
import pandas as pd
from databricks import sql as databricks_sql
from config import DatabricksConfig

logger = logging.getLogger(__name__)


class DatabricksUploader:
    """Upload data to Databricks via DBFS or Delta Table."""

    def __init__(self):
        """Initialize Databricks connection settings."""
        DatabricksConfig.validate()

        self.host = DatabricksConfig.HOST.rstrip("/")
        self.token = DatabricksConfig.TOKEN
        self.http_path = DatabricksConfig.HTTP_PATH
        self.catalog = DatabricksConfig.CATALOG
        self.schema = DatabricksConfig.SCHEMA

        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

        logger.info(f"Databricks uploader initialized for: {self.host}")

    # =========================================================================
    # METHOD 1: Upload to DBFS (Databricks File System)
    # =========================================================================

    def upload_to_dbfs(self, df: pd.DataFrame, dbfs_path: str, file_format: str = "parquet"):
        """
        Upload DataFrame as a file to DBFS.

        Args:
            df: pandas DataFrame to upload.
            dbfs_path: Destination path on DBFS (e.g., /FileStore/data/my_table.parquet).
            file_format: File format - 'parquet' or 'csv'.
        """
        logger.info(f"Uploading {len(df)} rows to DBFS: {dbfs_path}")

        # Convert DataFrame to bytes
        if file_format == "parquet":
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False, engine="pyarrow")
            file_bytes = buffer.getvalue()
        elif file_format == "csv":
            csv_str = df.to_csv(index=False)
            file_bytes = csv_str.encode("utf-8")
        else:
            raise ValueError(f"Unsupported format: {file_format}. Use 'parquet' or 'csv'.")

        # Upload using DBFS API (handles files > 1MB with streaming)
        file_size = len(file_bytes)
        logger.info(f"File size: {file_size / 1024 / 1024:.2f} MB")

        if file_size <= 1 * 1024 * 1024:  # < 1MB: direct upload
            self._dbfs_put(dbfs_path, file_bytes)
        else:  # >= 1MB: streaming upload
            self._dbfs_stream_upload(dbfs_path, file_bytes)

        logger.info(f"✅ Successfully uploaded to DBFS: {dbfs_path}")

    def _dbfs_put(self, dbfs_path: str, file_bytes: bytes):
        """Direct upload for small files (< 1MB)."""
        encoded = base64.standard_b64encode(file_bytes).decode("utf-8")

        response = requests.post(
            f"{self.host}/api/2.0/dbfs/put",
            headers=self.headers,
            json={
                "path": dbfs_path,
                "contents": encoded,
                "overwrite": True,
            },
        )
        response.raise_for_status()

    def _dbfs_stream_upload(self, dbfs_path: str, file_bytes: bytes, chunk_size: int = 1024 * 1024):
        """Streaming upload for large files (>= 1MB)."""
        # Step 1: Create upload handle
        response = requests.post(
            f"{self.host}/api/2.0/dbfs/create",
            headers=self.headers,
            json={"path": dbfs_path, "overwrite": True},
        )
        response.raise_for_status()
        handle = response.json()["handle"]

        try:
            # Step 2: Add blocks
            offset = 0
            while offset < len(file_bytes):
                chunk = file_bytes[offset : offset + chunk_size]
                encoded_chunk = base64.standard_b64encode(chunk).decode("utf-8")

                response = requests.post(
                    f"{self.host}/api/2.0/dbfs/add-block",
                    headers=self.headers,
                    json={"handle": handle, "data": encoded_chunk},
                )
                response.raise_for_status()

                offset += chunk_size
                logger.info(f"  Uploaded {min(offset, len(file_bytes))} / {len(file_bytes)} bytes")

            # Step 3: Close handle
            response = requests.post(
                f"{self.host}/api/2.0/dbfs/close",
                headers=self.headers,
                json={"handle": handle},
            )
            response.raise_for_status()

        except Exception:
            # If error, try to close handle to clean up
            requests.post(
                f"{self.host}/api/2.0/dbfs/close",
                headers=self.headers,
                json={"handle": handle},
            )
            raise

    # =========================================================================
    # METHOD 2: Write directly to Delta Table via SQL Connector
    # =========================================================================

    def upload_to_delta_table(self, df: pd.DataFrame, table_name: str, mode: str = "overwrite"):
        """
        Write DataFrame directly to a Databricks Delta Table.

        Args:
            df: pandas DataFrame to upload.
            table_name: Target table name (will be created in configured catalog.schema).
            mode: Write mode - 'overwrite' or 'append'.
        """
        full_table_name = f"{self.catalog}.{self.schema}.{table_name}"
        logger.info(f"Writing {len(df)} rows to Delta Table: {full_table_name} (mode={mode})")

        # Save DataFrame as temp parquet, then use SQL to load
        with tempfile.TemporaryDirectory() as tmpdir:
            parquet_path = os.path.join(tmpdir, f"{table_name}.parquet")
            df.to_parquet(parquet_path, index=False, engine="pyarrow")

            # Upload parquet to DBFS staging area
            staging_path = f"/FileStore/staging/{table_name}.parquet"
            with open(parquet_path, "rb") as f:
                file_bytes = f.read()

            logger.info("Uploading staging file to DBFS...")
            if len(file_bytes) <= 1 * 1024 * 1024:
                self._dbfs_put(staging_path, file_bytes)
            else:
                self._dbfs_stream_upload(staging_path, file_bytes)

        # Create table from staged file using SQL
        with databricks_sql.connect(
            server_hostname=self.host.replace("https://", ""),
            http_path=self.http_path,
            access_token=self.token,
        ) as connection:
            with connection.cursor() as cursor:
                if mode == "overwrite":
                    # Drop and recreate from parquet
                    cursor.execute(f"DROP TABLE IF EXISTS {full_table_name}")
                    cursor.execute(
                        f"""
                        CREATE TABLE {full_table_name}
                        USING PARQUET
                        LOCATION 'dbfs:{staging_path}'
                        """
                    )
                    logger.info(f"✅ Table {full_table_name} created (overwrite mode)")

                elif mode == "append":
                    # Check if table exists, if not create it
                    try:
                        cursor.execute(f"DESCRIBE TABLE {full_table_name}")
                        # Table exists, insert from staging
                        cursor.execute(
                            f"""
                            INSERT INTO {full_table_name}
                            SELECT * FROM parquet.`dbfs:{staging_path}`
                            """
                        )
                        logger.info(f"✅ Appended data to {full_table_name}")
                    except Exception:
                        # Table doesn't exist, create it
                        cursor.execute(
                            f"""
                            CREATE TABLE {full_table_name}
                            USING PARQUET
                            LOCATION 'dbfs:{staging_path}'
                            """
                        )
                        logger.info(f"✅ Table {full_table_name} created (new)")
                else:
                    raise ValueError(f"Invalid mode: {mode}. Use 'overwrite' or 'append'.")

    def write_with_sql_connector(self, df: pd.DataFrame, table_name: str, mode: str = "overwrite"):
        """
        Write DataFrame to Delta Table using INSERT statements (for small datasets).

        Args:
            df: pandas DataFrame to upload (best for < 10,000 rows).
            table_name: Target table name.
            mode: 'overwrite' or 'append'.
        """
        full_table_name = f"{self.catalog}.{self.schema}.{table_name}"
        logger.info(f"Writing {len(df)} rows via SQL INSERT to: {full_table_name}")

        with databricks_sql.connect(
            server_hostname=self.host.replace("https://", ""),
            http_path=self.http_path,
            access_token=self.token,
        ) as connection:
            with connection.cursor() as cursor:
                # Build CREATE TABLE statement from DataFrame schema
                col_defs = []
                for col_name, dtype in df.dtypes.items():
                    sql_type = self._pandas_to_sql_type(dtype)
                    col_defs.append(f"`{col_name}` {sql_type}")

                create_sql = f"CREATE TABLE IF NOT EXISTS {full_table_name} ({', '.join(col_defs)})"

                if mode == "overwrite":
                    cursor.execute(f"DROP TABLE IF EXISTS {full_table_name}")

                cursor.execute(create_sql)

                # Insert data in batches
                if len(df) == 0:
                    logger.info("DataFrame is empty, no data to insert.")
                    return

                batch_size = 1000
                for start in range(0, len(df), batch_size):
                    batch = df.iloc[start : start + batch_size]
                    values_list = []

                    for _, row in batch.iterrows():
                        values = []
                        for val in row:
                            # Check if value is a scalar NA
                            try:
                                if val is None:
                                    values.append("NULL")
                                    continue
                                # For arrays/lists, skip isna check
                                if isinstance(val, (list, dict)):
                                    import json
                                    escaped = json.dumps(val, default=str).replace("'", "''")
                                    values.append(f"'{escaped}'")
                                    continue
                                # Check for numpy arrays
                                import numpy as np
                                if isinstance(val, np.ndarray):
                                    import json
                                    escaped = json.dumps(val.tolist(), default=str).replace("'", "''")
                                    values.append(f"'{escaped}'")
                                    continue
                                # Now safe to check isna for scalars
                                if pd.isna(val):
                                    values.append("NULL")
                                elif isinstance(val, bool):
                                    values.append("TRUE" if val else "FALSE")
                                elif isinstance(val, (int, float)):
                                    values.append(str(val))
                                elif isinstance(val, str):
                                    escaped = val.replace("'", "''")
                                    values.append(f"'{escaped}'")
                                else:
                                    escaped = str(val).replace("'", "''")
                                    values.append(f"'{escaped}'")
                            except (ValueError, TypeError):
                                escaped = str(val).replace("'", "''")
                                values.append(f"'{escaped}'")
                        values_list.append(f"({', '.join(values)})")

                    cols = ", ".join([f"`{c}`" for c in df.columns])
                    insert_sql = (
                        f"INSERT INTO {full_table_name} ({cols}) VALUES {', '.join(values_list)}"
                    )
                    cursor.execute(insert_sql)

                    logger.info(f"  Inserted batch {start // batch_size + 1} ({len(batch)} rows)")

        logger.info(f"✅ Successfully wrote {len(df)} rows to {full_table_name}")

    @staticmethod
    def _pandas_to_sql_type(dtype) -> str:
        """Convert pandas dtype to Databricks SQL type."""
        dtype_str = str(dtype)
        if "int" in dtype_str:
            return "BIGINT"
        elif "float" in dtype_str:
            return "DOUBLE"
        elif "bool" in dtype_str:
            return "BOOLEAN"
        elif "datetime" in dtype_str:
            return "TIMESTAMP"
        else:
            return "STRING"
