"""
Main Pipeline: BigQuery → Databricks

Usage:
    # Extract a full table and upload to DBFS as parquet:
    python main.py --table my_table --method dbfs

    # Extract with custom SQL and upload to Delta Table:
    python main.py --query "SELECT * FROM dataset.table WHERE date > '2024-01-01'" --method delta --target my_target_table

    # Extract table and upload via SQL INSERT (small datasets):
    python main.py --table my_table --method sql_insert --target my_target_table

    # List all tables in the configured dataset:
    python main.py --list-tables
"""

import argparse
import logging
import sys
from datetime import datetime

from bigquery_extract import BigQueryExtractor
from databricks_upload import DatabricksUploader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("pipeline")


def run_pipeline(args):
    """Execute the BigQuery → Databricks pipeline."""
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("PIPELINE START: BigQuery → Databricks")
    logger.info("=" * 60)

    # ── Step 1: Extract data from BigQuery ──────────────────────
    logger.info("📥 Step 1: Extracting data from BigQuery...")
    extractor = BigQueryExtractor()

    if args.query:
        df = extractor.extract_by_query(args.query)
    elif args.table:
        df = extractor.extract_table(args.table, limit=args.limit)
    else:
        logger.error("Please provide --query or --table argument.")
        sys.exit(1)

    logger.info(f"   Extracted {len(df)} rows, {len(df.columns)} columns")
    logger.info(f"   Columns: {list(df.columns)}")
    logger.info(f"   Memory usage: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")

    # ── Step 2: Upload data to Databricks ───────────────────────
    logger.info("📤 Step 2: Uploading data to Databricks...")
    uploader = DatabricksUploader()

    method = args.method
    target = args.target or args.table or "bigquery_data"
    write_mode = args.mode

    if method == "dbfs":
        # Upload as Parquet file to DBFS
        file_format = args.format or "parquet"
        dbfs_path = f"/FileStore/bigquery_data/{target}.{file_format}"
        uploader.upload_to_dbfs(df, dbfs_path, file_format=file_format)
        logger.info(f"   File uploaded to: dbfs:{dbfs_path}")

    elif method == "delta":
        # Upload to Delta Table via staging parquet
        uploader.upload_to_delta_table(df, target, mode=write_mode)

    elif method == "sql_insert":
        # Upload via SQL INSERT statements (for small datasets)
        if len(df) > 50000:
            logger.warning(
                f"⚠️  DataFrame has {len(df)} rows. "
                f"sql_insert is slow for large datasets. Consider using 'delta' method."
            )
        uploader.write_with_sql_connector(df, target, mode=write_mode)

    else:
        logger.error(f"Unknown method: {method}. Use 'dbfs', 'delta', or 'sql_insert'.")
        sys.exit(1)

    # ── Done ────────────────────────────────────────────────────
    elapsed = datetime.now() - start_time
    logger.info("=" * 60)
    logger.info(f"✅ PIPELINE COMPLETED in {elapsed.total_seconds():.1f}s")
    logger.info(f"   Rows transferred: {len(df)}")
    logger.info(f"   Method: {method}")
    logger.info("=" * 60)


def list_tables():
    """List all tables in the configured BigQuery dataset."""
    extractor = BigQueryExtractor()
    tables = extractor.list_tables()

    print("\n📋 Tables in BigQuery dataset:")
    print("-" * 40)
    for i, table in enumerate(tables, 1):
        print(f"  {i}. {table}")
    print(f"\nTotal: {len(tables)} tables")


def main():
    parser = argparse.ArgumentParser(
        description="BigQuery → Databricks Data Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --table users --method dbfs
  python main.py --table orders --method delta --target orders_table --mode append
  python main.py --query "SELECT * FROM dataset.table LIMIT 100" --method sql_insert --target test_table
  python main.py --list-tables
        """,
    )

    # Data source arguments
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--query", "-q", help="Custom SQL query to execute on BigQuery")
    source.add_argument("--table", "-t", help="BigQuery table name to extract")

    # Upload arguments
    parser.add_argument(
        "--method", "-m",
        choices=["dbfs", "delta", "sql_insert"],
        default="dbfs",
        help="Upload method (default: dbfs)",
    )
    parser.add_argument(
        "--target",
        help="Target table name or file name on Databricks (default: same as source table)",
    )
    parser.add_argument(
        "--mode",
        choices=["overwrite", "append"],
        default="overwrite",
        help="Write mode for delta/sql_insert (default: overwrite)",
    )
    parser.add_argument(
        "--format",
        choices=["parquet", "csv"],
        default="parquet",
        help="File format for DBFS upload (default: parquet)",
    )
    parser.add_argument("--limit", "-l", type=int, help="Limit number of rows to extract")

    # Utility arguments
    parser.add_argument(
        "--list-tables",
        action="store_true",
        help="List all tables in the configured BigQuery dataset",
    )

    args = parser.parse_args()

    if args.list_tables:
        list_tables()
    elif args.query or args.table:
        run_pipeline(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
