"""
Configuration module - Load settings from .env file.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class BigQueryConfig:
    """Google BigQuery connection configuration."""

    PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
    CREDENTIALS_PATH = os.getenv("BIGQUERY_CREDENTIALS_PATH")
    DATASET = os.getenv("BIGQUERY_DATASET")

    @classmethod
    def validate(cls):
        """Validate that all required BigQuery config values are set."""
        missing = []
        if not cls.PROJECT_ID:
            missing.append("BIGQUERY_PROJECT_ID")
        if not cls.CREDENTIALS_PATH:
            missing.append("BIGQUERY_CREDENTIALS_PATH")
        if missing:
            raise ValueError(
                f"Missing BigQuery config: {', '.join(missing)}. "
                f"Please check your .env file."
            )
        if not os.path.exists(cls.CREDENTIALS_PATH):
            raise FileNotFoundError(
                f"BigQuery credentials file not found: {cls.CREDENTIALS_PATH}"
            )


class DatabricksConfig:
    """Databricks connection configuration."""

    HOST = os.getenv("DATABRICKS_HOST")
    TOKEN = os.getenv("DATABRICKS_TOKEN")
    HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
    CATALOG = os.getenv("DATABRICKS_CATALOG", "hive_metastore")
    SCHEMA = os.getenv("DATABRICKS_SCHEMA", "default")

    @classmethod
    def validate(cls):
        """Validate that all required Databricks config values are set."""
        missing = []
        if not cls.HOST:
            missing.append("DATABRICKS_HOST")
        if not cls.TOKEN:
            missing.append("DATABRICKS_TOKEN")
        if not cls.HTTP_PATH:
            missing.append("DATABRICKS_HTTP_PATH")
        if missing:
            raise ValueError(
                f"Missing Databricks config: {', '.join(missing)}. "
                f"Please check your .env file."
            )
