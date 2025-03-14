from dataclasses import dataclass, asdict
import enum
import os
from typing import Optional

import click
from jinja2 import Template

from lotad.config import Config
from lotad.connection import LotadConnectionInterface, DatabaseDetails, DatabaseType


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

    This class provides functionality to track, analyze, and write differences between two database states.
    This includes schema changes, missing tables, missing columns, and data drift.
    """

    db_interface: LotadConnectionInterface = None
    db_conn = None
    reports_dir = os.path.join(os.path.dirname(__file__), 'reports')

    def __init__(self, config: Config):
        self.config = config
        output_path = os.path.expanduser(config.output_path)

        if os.path.exists(output_path):
            os.remove(output_path)

        self.db_interface = LotadConnectionInterface.create(
            DatabaseDetails(database_type=DatabaseType.DUCKDB, path=output_path)
        )
        self.db_conn = self.db_interface.get_connection(read_only=False)
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
            # Normalize column type to handle enums
            db1_column_type = str(result.db1_column_type).replace('\'', '')
            db2_column_type = str(result.db2_column_type).replace('\'', '')
            values.append(
                (
                    f'"{result.table_name}"',
                    f'"{result.column_name}"',
                    f'"{result.db1}"',
                    f'"{db1_column_type}"',
                    f'"{result.db2}"',
                    f'"{db2_column_type}"',
                )
            )
        value_str = ',\n'.join([str(v) for v in values])
        self.db_conn.execute(
            f"INSERT INTO {DriftAnalysisTables.TABLE_SCHEMA_DRIFT.value}\n"
            f"VALUES {value_str};"
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
            f"INSERT INTO {DriftAnalysisTables.MISSING_TABLE.value}\n"
            f"VALUES {value_str};"
        )

    def add_data_drift(
        self,
        results: list[TableDataDiff],
    ):
        """Records data drift between databases for a specific table.

        Also, updates the DB_DATA_DRIFT_SUMMARY table.

        Args:
            results (list[TableDataDiff])

        The method converts the differences into JSON format and stores them in the
        db_data_drift table for further analysis and reporting.
        """
        for result in results:
            table_name = result.table_name
            tmp_path = result.tmp_path
            query = self.db_interface.get_query_template(
                'data_analysis_set_data_drift_for_table.sql'
            )
            self.db_conn.execute(
                query.render(
                    table_name=table_name,
                    tmp_path=tmp_path,
                )
            )

            query = self.db_interface.get_query_template('drift_analysis_extend_data_drift_summary')
            self.db_conn.execute(
                query.render(
                    table_name=table_name,
                    db1=self.config.db1.db_id,
                    db2=self.config.db2.db_id,
                    data_drift_summary_table=DriftAnalysisTables.DB_DATA_DRIFT_SUMMARY.value,

                )
            )

    def get_missing_table_drift(self):
        return self.db_interface.parse_db_response(
            self.db_conn.execute(
                f"SELECT * FROM {DriftAnalysisTables.MISSING_TABLE.value} ORDER BY table_name;"
            )
        )

    def get_table_schema_drift(self):
        return self.db_interface.parse_db_response(
            self.db_conn.execute(
                f"SELECT * FROM {DriftAnalysisTables.TABLE_SCHEMA_DRIFT.value} ORDER BY table_name, column_name;"
            )
        )

    def get_data_drift_summary(self):
        return self.db_interface.parse_db_response(
            self.db_conn.execute(
                f"SELECT * FROM {DriftAnalysisTables.DB_DATA_DRIFT_SUMMARY.value} ORDER BY table_name;"
            )
        )

    def output_summary(self):
        with open(os.path.join(self.reports_dir, 'db_comparison_report.j2')) as f:
            comparison_report = Template(f.read())

        output = comparison_report.render(
            table_drift=self.get_missing_table_drift(),
            table_schema_drift=self.get_table_schema_drift(),
            data_drift=self.get_data_drift_summary(),
        )
        click.echo(output)
