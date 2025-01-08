import multiprocessing
import re
from typing import Union

import duckdb

from lotad.config import Config, TableRules, TableRuleType, CPU_COUNT
from lotad.connection import LotadConnectionInterface
from lotad.data_analysis import DriftAnalysis, MissingTableDrift, TableDataDiff, TableSchemaDrift
from lotad.logger import logger


class DatabaseComparator:
    """Compares two databases to identify schema and data differences.

    This class provides functionality to perform comprehensive comparisons between
    two databases, analyzing differences in table structures, schemas, and
    data content. It supports detailed analysis of table presence, column
    definitions, data types, and row-level differences.

    The comparator integrates with a DriftAnalysis system to track and store
    identified differences for further analysis and reporting.

    Attributes:
        db1_path (str): Connection string to the first database.
        db2_path (str): Connection string to the second database.
        drift_analysis (DriftAnalysis): Instance for tracking and storing comparison results.
    """

    def __init__(self, config: Config):
        self.config = config
        self.db1_path = str(config.db1_connection_string)
        self.db2_path = str(config.db2_connection_string)
        self.db_interface: LotadConnectionInterface = LotadConnectionInterface.create_connection()

        self.drift_analysis = DriftAnalysis(self.config)

    def generate_table_schema_drift(self, table_name: str) -> list[TableSchemaDrift]:
        """Detects schemas differences between the two db files.

        Args:
            table_name (str): Name of the table_name to compare.

        Returns:
            dict: Dictionary containing schema differences, including:
                - missing_in_db2: Columns present in db1 but not in db2
                - missing_in_db1: Columns present in db2 but not in db1
                - type_mismatches: Columns with different data types
        """
        db1 = self.db_interface.get_connection(self.db1_path, read_only=True)
        db2 = self.db_interface.get_connection(self.db2_path, read_only=True)
        schema1 = self.db_interface.get_schema(db1, table_name)
        schema2 = self.db_interface.get_schema(db2, table_name)
        response = []

        db1.close()
        db2.close()

        for column in set(schema1.keys()) - set(schema2.keys()):
            response.append(
                TableSchemaDrift(
                    table_name=table_name,
                    db1=self.db1_path,
                    db2=self.db2_path,
                    column_name=column,
                    db1_column_type=schema1[column],
                    db2_column_type=None,
                )
            )

        for column in set(schema2.keys()) - set(schema1.keys()):
            response.append(
                TableSchemaDrift(
                    table_name=table_name,
                    db1=self.db1_path,
                    db2=self.db2_path,
                    column_name=column,
                    db1_column_type=None,
                    db2_column_type=schema2[column],
                )
            )

        for col in set(schema1.keys()) & set(schema2.keys()):
            if schema1[col] != schema2[col]:
                response.append(
                    TableSchemaDrift(
                        table_name=table_name,
                        db1=self.db1_path,
                        db2=self.db2_path,
                        column_name=col,
                        db1_column_type=schema1[col],
                        db2_column_type=schema2[col],
                    )
                )

        return response

    @staticmethod
    def generate_missing_table_drift(
        db1: str,
        db1_tables: set[str],
        db2: str,
        db2_tables: set[str],
    ) -> list[MissingTableDrift]:
        response = []
        for table_name in db1_tables:
            if table_name not in db2_tables:
                response.append(
                    MissingTableDrift(
                        table_name=table_name,
                        observed_in=db1,
                        missing_in=db2
                    )
                )

        for table_name in db2_tables:
            if table_name not in db1_tables:
                response.append(
                    MissingTableDrift(
                        table_name=table_name,
                        observed_in=db2,
                        missing_in=db1
                    )
                )

        return response

    def compare_all(self):
        """Performs a comprehensive comparison of all tables between the 2 dbs.

        Returns:
            dict: Dictionary containing comparison results:
                - missing_tables: Tables present in one database but not the other
                - common_tables: Detailed comparison results for tables present in both databases

        The method compares both schema and data differences for all relevant tables,
        respecting the ignore_tables and tables parameters for filtering.
        """

        db1 = self.db_interface.get_connection(self.db1_path, read_only=True)
        db2 = self.db_interface.get_connection(self.db2_path, read_only=True)

        tables1 = set(table[0] for table in self.db_interface.get_tables(db1))
        tables2 = set(table[0] for table in self.db_interface.get_tables(db2))
        all_tables = sorted(tables1 & tables2)

        all_schema_drift = []
        for table in all_tables:
            if schema_drift := self.generate_table_schema_drift(table):
                all_schema_drift.extend(schema_drift)
        if all_schema_drift:
            self.drift_analysis.add_schema_drift(all_schema_drift)

        if missing_table_drift := self.generate_missing_table_drift(
            self.db1_path,
            tables1,
            self.db1_path,
            tables2,
        ):
            self.drift_analysis.add_missing_table_drift(missing_table_drift)

        db1.close()
        db2.close()

        table_data = []
        ignore_tables = self.config.ignore_tables
        target_tables = self.config.target_tables

        with multiprocessing.Pool(CPU_COUNT) as pool:
            results = []

            for table_name in all_tables:
                if ignore_tables and any(re.match(it, table_name, re.IGNORECASE) for it in ignore_tables):
                    logger.info(f"{table_name} is an ignored table, skipping.")
                    continue

                if target_tables and any(re.match(tt, table_name, re.IGNORECASE) for tt in target_tables):
                    logger.info(f"{table_name} is not a target table, skipping.")
                    continue

                result = pool.apply_async(compare_table_data, (self.config, table_name))
                results.append(result)

            # Get the results
            for result in results:
                try:
                    tmp_table = result.get()
                    if tmp_table:
                        table_data.append(
                            tmp_table
                        )
                except duckdb.duckdb.CatalogException:
                    continue

        self.drift_analysis.add_data_drift(table_data)


def generate_schema_columns(
    db_schema: dict,
    alt_db_schema: dict,
    table_rules: Union[TableRules, None]
):
    db_columns = []
    for col, col_type in db_schema.items():
        if table_rules:
            col_rule = table_rules.get_rule(col)
            if col_rule and col_rule.rule_type == TableRuleType.IGNORE_COLUMN:
                continue
        if col in alt_db_schema and alt_db_schema[col] != col_type:
            col = f'"{col}"::VARCHAR'
        else:
            col = f'"{col}"'
        db_columns.append(col)

    for col in alt_db_schema.keys():
        if col not in db_schema:
            db_columns.append(f'NULL AS "{col}"')

    return db_columns


def get_gen_table_query(
    table: str,
    config: Config,
    db1_schema: dict,
    db2_schema: dict,
):
    table_rules = config.get_table_rules(table)
    db1_path = config.db1_connection_string
    db2_path = config.db2_connection_string
    db1_columns = ",\n".join(
        generate_schema_columns(db1_schema, db2_schema, table_rules)
    )
    db2_columns = ",\n".join(
        generate_schema_columns(db2_schema, db1_schema, table_rules)
    )

    return f"""
CREATE OR REPLACE TABLE {table}_t1 AS
SELECT '{db1_path}' AS observed_in, 
    get_row_hash(ROW_TO_JSON(t1)::VARCHAR) as hashed_row,
    {db1_columns} 
FROM db1.{table} t1;

CREATE OR REPLACE TABLE {table}_t2 AS 
SELECT '{db2_path}' AS observed_in, 
    get_row_hash(ROW_TO_JSON(t2)::VARCHAR) as hashed_row,
    {db2_columns} 
FROM db2.{table} t2;

CREATE OR REPLACE TABLE {table} AS
WITH _T1_ONLY_ROWS AS (
    SELECT _t1.*
    FROM {table}_t1 _t1
    ANTI JOIN {table}_t2 _t2 
    ON _t1.hashed_row = _t2.hashed_row 
),
_T2_ONLY_ROWS AS (
    SELECT _t2.*
    FROM {table}_t2 _t2
    ANTI JOIN {table}_t1 _t1
    ON _t2.hashed_row = _t1.hashed_row 
)
SELECT * FROM _T1_ONLY_ROWS
UNION 
SELECT * FROM _T2_ONLY_ROWS;"""


def compare_table_data(config: Config, table_name: str) -> Union[TableDataDiff, None]:
    logger.info(f"Comparing table", table=table_name)

    tmp_path = f"/tmp/lotad_config_{table_name}.db"
    db_interface: LotadConnectionInterface = LotadConnectionInterface.create_connection()
    tmp_db = db_interface.get_connection(tmp_path, read_only=False)

    db1_path = config.db1_connection_string
    db1 = db_interface.get_connection(db1_path, read_only=True)

    db2_path = config.db2_connection_string
    db2 = db_interface.get_connection(db2_path, read_only=True)

    tmp_db.execute(
        f"""ATTACH '{db1_path}' AS db1 (READ_ONLY);
        ATTACH '{db2_path}' AS db2 (READ_ONLY);""".lstrip()
    )

    query = get_gen_table_query(
        table_name,
        config,
        db_interface.get_schema(db1, table_name),
        db_interface.get_schema(db2, table_name)
    )
    db1.close()
    db2.close()

    try:
        tmp_db.execute(query)
        logger.info(f"Successfully processed table", table=table_name)
        contains_records = tmp_db.execute(
            f"SELECT * FROM {table_name} LIMIT 1;"
        ).fetchone()
        if contains_records:
            return TableDataDiff(
                table_name=table_name,
                tmp_path=tmp_path,
            )
        logger.info("No changes discovered", table=table_name)
    except duckdb.duckdb.CatalogException:
        logger.warning(f"Failed to process table", table=table_name)
        return


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
