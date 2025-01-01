import enum
import json
import logging
import os

import duckdb
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DriftAnalysisTables(enum.Enum):
    DB_DATA_DRIFT = "db_data_drift"
    MISSING_TABLE = "missing_table"
    MISSING_TABLE_COLUMN = "missing_table_column"
    MISMATCHED_COLUMN_TYPE = "mismatched_column_type"


class DriftAnalysis:
    """Manages database drift analysis between two database states.

    This class provides functionality to track and analyze differences between
    two database states, including schema changes, missing tables, missing columns,
    and data drift.

    It uses DuckDB as the backend storage for tracking these differences.

    Attributes:
        db_file_name (str): Name of the DuckDB database file used for storing analysis results.
        db_conn (duckdb.DuckDBPyConnection): Connection to the DuckDB database.
    """

    db_file_name: str = 'drift_analysis.db'
    db_conn = None

    def __init__(self):
        if os.path.exists(self.db_file_name):
            os.remove(self.db_file_name)

        if not DriftAnalysis.db_conn:
            DriftAnalysis.db_conn = duckdb.connect(self.db_file_name)
            self._add_tables()

    def _add_tables(self):
        self.db_conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {DriftAnalysisTables.DB_DATA_DRIFT.value} (
                id VARCHAR,
                observed_in VARCHAR,
                table_name VARCHAR,
                content VARCHAR
            );
        """)

        self.db_conn.execute(f"""
            CREATE TABLE {DriftAnalysisTables.MISSING_TABLE.value} (
                table_name VARCHAR,
                observed_in VARCHAR,
                missing_in VARCHAR
            )
        """)

        self.db_conn.execute(f"""
            CREATE TABLE {DriftAnalysisTables.MISSING_TABLE_COLUMN.value} (
                table_name VARCHAR,
                column_name VARCHAR,
                observed_in VARCHAR,
                missing_in VARCHAR
            )
        """)

        self.db_conn.execute(f"""
            CREATE TABLE {DriftAnalysisTables.MISMATCHED_COLUMN_TYPE.value} (
                table_name VARCHAR,
                column_name VARCHAR,
                db_1 VARCHAR,
                db_1_column_type VARCHAR,
                db_2 VARCHAR,
                db_2_column_type VARCHAR
            )
        """)

    def add_data_drift(
            self,
            diff_df: pd.DataFrame,
            table: str,
            observed_in: str,
    ):
        """Records data drift between databases for a specific table.

        Args:
            diff_df (pd.DataFrame): DataFrame containing the differences between databases.
                Must include a 'row_hash' column for unique identification.
            table (str): Name of the table where the drift was observed.
            observed_in (str): Identifier for the database where the drift was observed.

        The method converts the differences into JSON format and stores them in the
        db_data_drift table for further analysis and reporting.
        """
        table_name = DriftAnalysisTables.DB_DATA_DRIFT.value
        drift_df = pd.DataFrame({
            'id': diff_df['row_hash'],
            'observed_in': observed_in,
            'table_name': table,
            'content': diff_df.apply(lambda row: json.dumps({k: v for k, v in row.items()}, indent=2), axis=1)
        })
        self.db_conn.execute(f"""
            INSERT INTO {table_name}
            SELECT *
            FROM drift_df
        """)
