import logging
import os
from datetime import datetime
from typing import Optional

import duckdb

from lotad.data_analysis import DriftAnalysis
from lotad.utils import get_row_hash

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
CPU_COUNT = os.cpu_count()


class DatabaseComparator:
    """Compares two DuckDB databases to identify schema and data differences.

    This class provides functionality to perform comprehensive comparisons between
    two DuckDB databases, analyzing differences in table structures, schemas, and
    data content. It supports detailed analysis of table presence, column
    definitions, data types, and row-level differences.

    The comparator integrates with a DriftAnalysis system to track and store
    identified differences for further analysis and reporting.

    Attributes:
        db1_path (str): File path to the first DuckDB database.
        db1 (duckdb.DuckDBPyConnection): Connection to the first database.
        db2_path (str): File path to the second DuckDB database.
        db2 (duckdb.DuckDBPyConnection): Connection to the second database.
        drift_analysis (DriftAnalysis): Instance for tracking and storing comparison results.
    """

    def __init__(self, db1_path: str, db2_path: str):
        """Initialize connections to both DuckDB databases."""
        self.db1_path = str(db1_path)
        self.db1 = duckdb.connect(self.db1_path, read_only=True)
        self.db1.create_function("get_row_hash", get_row_hash)

        self.db2_path = str(db2_path)
        self.db2 = duckdb.connect(self.db2_path, read_only=True)
        self.db2.create_function("get_row_hash", get_row_hash)

        self.drift_analysis = DriftAnalysis()

    def get_tables(self, connection: duckdb.DuckDBPyConnection) -> list[str]:
        """Get list of all tables in a database."""
        return connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()

    def get_schema(self, connection: duckdb.DuckDBPyConnection, table: str) -> dict:
        """Get schema information for a table."""
        columns = connection.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{table}'
            AND table_schema = 'main'
            AND data_type NOT LIKE 'TIMESTAMP%'
            AND data_type NOT LIKE 'DATE'
            ORDER BY ordinal_position
        """).fetchall()
        return {col[0]: col[1] for col in columns}

    def compare_table_schemas(self, table: str) -> dict:
        """Detects schemas differences between the two db files.

        Args:
            table (str): Name of the table to compare.

        Returns:
            dict: Dictionary containing schema differences, including:
                - missing_in_db2: Columns present in db1 but not in db2
                - missing_in_db1: Columns present in db2 but not in db1
                - type_mismatches: Columns with different data types
        """
        schema1 = self.get_schema(self.db1, table)
        schema2 = self.get_schema(self.db2, table)

        return {
            'missing_in_db2': set(schema1.keys()) - set(schema2.keys()),
            'missing_in_db1': set(schema2.keys()) - set(schema1.keys()),
            'type_mismatches': {
                col: (schema1[col], schema2[col])
                for col in set(schema1.keys()) & set(schema2.keys())
                if schema1[col] != schema2[col]
            }
        }

    def compare_table_data(self, table: str) -> dict:
        """Performs a detailed comparison of table data between the two db files.

        Analyzes row-level differences between the two databases using hash-based
        comparison. Records differences in the drift analysis system for future
        reference.

        Args:
            table (str): Name of the table to compare.

        Returns:
            dict: Dictionary containing comparison results:
                - row_count_diff: Difference in number of rows between databases
                - only_in_db1: DataFrame of rows unique to first database
                - only_in_db2: DataFrame of rows unique to second database
        """

        start_time = datetime.now()
        logger.debug(f"Getting table data for {table} on df1")
        df1 = self.db1.execute(
            f"SELECT get_row_hash(ROW_TO_JSON(t)::VARCHAR) AS row_hash, * FROM {table} t"
        ).df()
        logger.debug(f"Finished table data for {table} on df1 in {datetime.now() - start_time}")
        logger.debug(f"Getting table data for {table} on df2")
        df2 = self.db2.execute(
            f"SELECT get_row_hash(ROW_TO_JSON(t)::VARCHAR) AS row_hash, * FROM {table} t"
        ).df()

        # Compare row counts
        row_diff = len(df1) - len(df2)

        # Find differences using hash comparison
        only_in_db1 = df1[~df1['row_hash'].isin(df2['row_hash'])]
        only_in_db2 = df2[~df2['row_hash'].isin(df1['row_hash'])]

        self.drift_analysis.add_data_drift(
            only_in_db1,
            table,
            self.db1_path
        )

        self.drift_analysis.add_data_drift(
            only_in_db2,
            table,
            self.db2_path
        )

        return {
            'row_count_diff': row_diff,
            'only_in_db1': only_in_db1,
            'only_in_db2': only_in_db2
        }

    def compare_all(
        self,
        ignore_tables: Optional[list[str]] = None,
        tables: Optional[list[str]] = None,

    ) -> dict:
        """Performs a comprehensive comparison of all tables between the 2 dbs.

        Args:
            ignore_tables (Optional[list[str]]): List of table names to exclude from comparison.
            tables (Optional[list[str]]): List of specific tables to compare. If provided,
                                        only these tables will be compared.

        Returns:
            dict: Dictionary containing comparison results:
                - missing_tables: Tables present in one database but not the other
                - common_tables: Detailed comparison results for tables present in both databases

        The method compares both schema and data differences for all relevant tables,
        respecting the ignore_tables and tables parameters for filtering.
        """

        tables1 = set(table[0] for table in self.get_tables(self.db1))
        tables2 = set(table[0] for table in self.get_tables(self.db2))

        results = {
            'missing_tables': {
                'in_db2': tables1 - tables2,
                'in_db1': tables2 - tables1
            },
            'common_tables': {}
        }

        for table in sorted(tables1 & tables2):
            table_name = table.lower()
            if ignore_tables and table_name in ignore_tables:
                continue

            if tables and table.lower() not in tables:
                continue
            logger.info(f"Comparing table {table}")
            self.compare_table_data(table)

            # logger.info(f"Comparing table: {table}")
            # results['common_tables'][table] = {
            #     'schema_differences': self.compare_table_schemas(table),
            #     'data_differences': self.compare_table_data(table)
            # }

        return results

    def close(self):
        self.db1.close()
        self.db2.close()


def generate_comparison_report(results: dict, output_path: str = "comparison_report.txt"):
    """Generates a text summary of the database comparison results.

    Args:
        results (dict): Comparison results from DatabaseComparator.
        output_path (str, optional): File path for the output report.
                                   Defaults to "comparison_report.txt".

    The report includes information about missing tables, schema differences,
    and data differences between the databases in a structured, readable format.
    """
    with open(output_path, 'w') as f:
        f.write("Database Comparison Report\n")
        f.write("=========================\n\n")

        # Report missing tables
        if results['missing_tables']['in_db2']:
            f.write("Tables missing in second database:\n")
            for table in results['missing_tables']['in_db2']:
                f.write(f"- {table}\n")
            f.write("\n")

        if results['missing_tables']['in_db1']:
            f.write("Tables missing in first database:\n")
            for table in results['missing_tables']['in_db1']:
                f.write(f"- {table}\n")
            f.write("\n")

        # Report differences in common tables
        f.write("Common Tables Analysis:\n")
        for table, differences in results['common_tables'].items():
            f.write(f"\nTable: {table}\n")
            f.write("-" * (len(table) + 7) + "\n")

            schema_diff = differences['schema_differences']
            if any(schema_diff.values()):
                f.write("Schema differences:\n")
                if schema_diff['missing_in_db2']:
                    f.write(f"  Columns missing in DB2: {', '.join(schema_diff['missing_in_db2'])}\n")
                if schema_diff['missing_in_db1']:
                    f.write(f"  Columns missing in DB1: {', '.join(schema_diff['missing_in_db1'])}\n")
                if schema_diff['type_mismatches']:
                    f.write("  Type mismatches:\n")
                    for col, (type1, type2) in schema_diff['type_mismatches'].items():
                        f.write(f"    {col}: DB1={type1}, DB2={type2}\n")
                f.write("\n")

            data_diff = differences['data_differences']
            f.write(f"Data differences:\n")
            f.write(f"  Row count difference: {data_diff['row_count_diff']}\n")
            f.write(f"  Rows only in DB1: {len(data_diff['only_in_db1'])}\n")
            f.write(f"  Rows only in DB2: {len(data_diff['only_in_db2'])}\n")
            f.write("\n")
