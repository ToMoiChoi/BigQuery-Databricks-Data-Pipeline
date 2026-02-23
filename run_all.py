"""
Script lấy TẤT CẢ tables từ BigQuery dataset và upload lên Databricks.

Usage:
    python run_all.py
"""

import logging
import sys
from datetime import datetime
from bigquery_extract import BigQueryExtractor
from databricks_upload import DatabricksUploader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("run_all")


def main():
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("PIPELINE: BigQuery → Databricks (ALL TABLES)")
    logger.info("=" * 60)

    # ── Step 1: Connect to BigQuery & list all tables ───────────
    logger.info("📥 Connecting to BigQuery...")
    extractor = BigQueryExtractor()

    logger.info(f"📋 Listing tables in dataset: {extractor.dataset}")
    tables = extractor.list_tables()

    if not tables:
        logger.warning("⚠️  No tables found in dataset!")
        sys.exit(0)

    logger.info(f"Found {len(tables)} tables: {tables}")

    # ── Step 2: Connect to Databricks ───────────────────────────
    logger.info("📤 Connecting to Databricks...")
    uploader = DatabricksUploader()

    # ── Step 3: Extract & Upload each table ─────────────────────
    success_count = 0
    error_count = 0
    errors = []

    for i, table_name in enumerate(tables, 1):
        logger.info("-" * 50)
        logger.info(f"📦 [{i}/{len(tables)}] Processing table: {table_name}")

        try:
            # Extract from BigQuery
            logger.info(f"   Extracting from BigQuery...")
            df = extractor.extract_table(table_name)
            logger.info(f"   ✅ Extracted {len(df)} rows, {len(df.columns)} columns")

            if len(df) == 0:
                logger.info(f"   ⏭️  Table is empty, skipping upload.")
                success_count += 1
                continue

            # Sanitize column names for Delta Lake
            import re
            df.columns = [re.sub(r'[^a-zA-Z0-9_]', '_', col).strip('_') for col in df.columns]
            # Remove duplicate column names
            seen = {}
            new_cols = []
            for col in df.columns:
                if col in seen:
                    seen[col] += 1
                    new_cols.append(f"{col}_{seen[col]}")
                else:
                    seen[col] = 0
                    new_cols.append(col)
            df.columns = new_cols

            # Upload to Databricks via SQL Connector
            logger.info(f"   Uploading to Databricks table: {table_name}...")
            uploader.write_with_sql_connector(df, table_name, mode="overwrite")
            logger.info(f"   ✅ Uploaded successfully!")
            success_count += 1

        except Exception as e:
            logger.error(f"   ❌ Error processing {table_name}: {e}")
            errors.append((table_name, str(e)))
            error_count += 1

    # ── Summary ─────────────────────────────────────────────────
    elapsed = datetime.now() - start_time
    logger.info("=" * 60)
    logger.info(f"🏁 PIPELINE COMPLETED in {elapsed.total_seconds():.1f}s")
    logger.info(f"   ✅ Success: {success_count}/{len(tables)} tables")
    logger.info(f"   ❌ Errors:  {error_count}/{len(tables)} tables")

    if errors:
        logger.info("\n   Failed tables:")
        for tbl, err in errors:
            logger.info(f"     - {tbl}: {err}")

    logger.info("=" * 60)


if __name__ == "__main__":
    main()
