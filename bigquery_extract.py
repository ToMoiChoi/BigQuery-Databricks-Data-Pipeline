"""
BigQuery Data Extraction Module.

Kết nối Google BigQuery và trích xuất dữ liệu thành pandas DataFrame.
"""

import logging
from google.cloud import bigquery
from google.oauth2 import service_account
from config import BigQueryConfig

logger = logging.getLogger(__name__)


class BigQueryExtractor:
    """Extract data from Google BigQuery."""

    def __init__(self):
        """Initialize BigQuery client with credentials."""
        BigQueryConfig.validate()

        credentials = service_account.Credentials.from_service_account_file(
            BigQueryConfig.CREDENTIALS_PATH,
            scopes=["https://www.googleapis.com/auth/bigquery"],
        )

        self.client = bigquery.Client(
            project=BigQueryConfig.PROJECT_ID,
            credentials=credentials,
        )
        self.project_id = BigQueryConfig.PROJECT_ID
        self.dataset = BigQueryConfig.DATASET

        logger.info(f"BigQuery client initialized for project: {self.project_id}")

    def extract_by_query(self, query: str):
        """
        Execute a SQL query and return results as pandas DataFrame.

        Args:
            query: SQL query string to execute.

        Returns:
            pandas.DataFrame with query results.
        """
        logger.info(f"Executing query:\n{query}")

        try:
            query_job = self.client.query(query)
            df = query_job.to_dataframe()
            logger.info(f"Query completed. Rows returned: {len(df)}")
            return df

        except Exception as e:
            logger.error(f"BigQuery query failed: {e}")
            raise

    def extract_table(self, table_name: str, limit: int = None):
        """
        Extract full table data as pandas DataFrame.

        Args:
            table_name: Table name (format: dataset.table or just table).
            limit: Optional row limit.

        Returns:
            pandas.DataFrame with table data.
        """
        # Build fully qualified table name
        if "." not in table_name:
            full_table = f"`{self.project_id}.{self.dataset}.{table_name}`"
        else:
            full_table = f"`{self.project_id}.{table_name}`"

        query = f"SELECT * FROM {full_table}"
        if limit:
            query += f" LIMIT {limit}"

        return self.extract_by_query(query)

    def list_tables(self, dataset: str = None):
        """
        List all tables in a dataset.

        Args:
            dataset: Dataset name. Defaults to configured dataset.

        Returns:
            List of table names.
        """
        dataset = dataset or self.dataset
        if not dataset:
            raise ValueError("No dataset specified. Set BIGQUERY_DATASET in .env")

        dataset_ref = f"{self.project_id}.{dataset}"
        tables = list(self.client.list_tables(dataset_ref))
        table_names = [table.table_id for table in tables]

        logger.info(f"Found {len(table_names)} tables in {dataset_ref}")
        return table_names

    def get_table_schema(self, table_name: str, dataset: str = None):
        """
        Get schema information for a table.

        Args:
            table_name: Table name.
            dataset: Dataset name. Defaults to configured dataset.

        Returns:
            List of dicts with column name, type, and mode.
        """
        dataset = dataset or self.dataset
        table_ref = f"{self.project_id}.{dataset}.{table_name}"
        table = self.client.get_table(table_ref)

        schema = [
            {
                "name": field.name,
                "type": field.field_type,
                "mode": field.mode,
            }
            for field in table.schema
        ]

        logger.info(f"Schema for {table_ref}: {len(schema)} columns")
        return schema
