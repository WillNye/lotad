from dataclasses import dataclass, asdict
import enum
import os
from typing import Optional

import duckdb

from lotad.config import Config


@dataclass
class TableDataDiff:
    table_name: str
    tmp_path: str


@dataclass
class MissingTableDrift:
    table_name: str
    observed_in: str
    missing_in: str


@dataclass
class TableSchemaDrift:
    table_name: str
    column_name: str
    db1: str
    db2: str
    db1_column_type: Optional[str] = None
    db2_column_type: Optional[str] = None

    def dict(self) -> dict:
        return asdict(self)


class DriftAnalysisTables(enum.Enum):
    DB_DATA_DRIFT_SUMMARY = "lotad_db_data_drift_summary"
    MISSING_TABLE = "lotad_missing_table_drift"
    TABLE_SCHEMA_DRIFT = "lotad_table_schema_drift"


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

    def __init__(self, config: Config):
        self.config = config

        if os.path.exists(self.db_file_name):
            os.remove(self.db_file_name)

        if not DriftAnalysis.db_conn:
            DriftAnalysis.db_conn = duckdb.connect(self.db_file_name)
            self._add_tables()

    def _add_tables(self):
        self.db_conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {DriftAnalysisTables.DB_DATA_DRIFT_SUMMARY.value} (
                table_name VARCHAR,
                db1 VARCHAR,
                rows_only_in_db1 INTEGER,
                db2 VARCHAR,
                rows_only_in_db2 INTEGER,
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
            CREATE TABLE {DriftAnalysisTables.TABLE_SCHEMA_DRIFT.value} (
                table_name VARCHAR,
                column_name VARCHAR,
                db1 VARCHAR,
                db1_column_type VARCHAR,
                db2 VARCHAR,
                db2_column_type VARCHAR
            )
        """)

    def add_schema_drift(
        self,
        results: list[TableSchemaDrift]
    ):
        values: list[tuple] = []
        for result in results:
            values.append(
                (
                    f'"{result.table_name}"',
                    f'"{result.column_name}"',
                    f'"{result.db1}"',
                    f'"{result.db1_column_type}"',
                    f'"{result.db2}"',
                    f'"{result.db2_column_type}"',
                )
            )
        value_str = ',\n'.join([str(v) for v in values])
        self.db_conn.execute(
            f"INSERT INTO {DriftAnalysisTables.TABLE_SCHEMA_DRIFT.value}"
            f"VALUES ({value_str});"
        )

    def add_missing_table_drift(
        self,
        results: list[MissingTableDrift]
    ):
        values: list[tuple] = []
        for result in results:
            values.append(
                (
                    f'"{result.table_name}"',
                    f'"{result.observed_in}"',
                    f'"{result.missing_in}"',
                )
            )
        value_str = ',\n'.join([str(v) for v in values])
        self.db_conn.execute(
            f"INSERT INTO {DriftAnalysisTables.MISSING_TABLE.value}"
            f"VALUES ({value_str});"
        )

    def add_data_drift(
        self,
        results: list[TableDataDiff],
    ):
        """Records data drift between databases for a specific table.

        Args:
            results (list[TableDataDiff])

        The method converts the differences into JSON format and stores them in the
        db_data_drift table for further analysis and reporting.
        """
        for result in results:
            table_name = result.table_name
            tmp_path = result.tmp_path
            self.db_conn.execute(f"""
                ATTACH '{tmp_path}' AS tmp_{table_name}_db (READ_ONLY);
            
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT * FROM tmp_{table_name}_db.{table_name};
            """)

            db1 = self.config.db1_connection_string
            db2 = self.config.db2_connection_string
            self.db_conn.execute(
                f"""
                WITH _db1_row_summary AS (
                    SELECT '{table_name}' as table_name,
                        '{db1}' as db_path,
                        COUNT(*) as rows_only_in_db
                    FROM {table_name}
                    WHERE observed_in = '{db1}'
                ),
                _db2_row_summary AS (
                    SELECT '{table_name}' as table_name,
                        '{db2}' as db_path,
                        COUNT(*) as rows_only_in_db
                    FROM {table_name}
                    WHERE observed_in = '{db2}'
                )
                INSERT INTO {DriftAnalysisTables.DB_DATA_DRIFT_SUMMARY.value}
                SELECT '{table_name}' as table_name,
                    _db1.db_path AS db1,
                    _db1.rows_only_in_db AS rows_only_in_db1,
                    _db2.db_path AS db2,
                    _db2.rows_only_in_db AS rows_only_in_db2,
                FROM _db1_row_summary _db1
                JOIN _db2_row_summary _db2 ON _db1.table_name = _db2.table_name
                """
            )
