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
    """

    def __init__(self, config: Config):
        self.config = config
        self.db1_path = config.db1.connection_str
        self.db2_path = config.db2.connection_str

        self.drift_analysis = DriftAnalysis(self.config)

    def generate_table_schema_drift(self, table_name: str) -> list[TableSchemaDrift]:
        """Detects schemas differences between the two db files.

        Args:
            table_name (str): Name of the table_name to compare.

        Returns:
            list[TableSchemaDrift]
        """
        db1 = self.config.db1.get_connection(read_only=True)
        db2 = self.config.db2.get_connection(read_only=True)
        schema1 = self.config.db1.get_schema(db1, table_name, self.config.ignore_dates)
        schema2 = self.config.db2.get_schema(db2, table_name, self.config.ignore_dates)
        response = []

        db1.close()
        db2.close()

        # Columns not found in schema2
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

        # Columns not found in schema1
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

        # Column type mismatches
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
        """Detects tables found in one db file but not the other."""
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

        This includes:
          * Data drift check
          * Table Schema drift check
          * Missing table drift check

        The method compares both schema and data differences for all relevant tables,
        respecting the ignore_tables and tables parameters for filtering.
        """

        db1 = self.config.db1.get_connection(read_only=True)
        db2 = self.config.db2.get_connection(read_only=True)

        tables1 = set(table[0] for table in self.config.db1.get_tables(db1))
        tables2 = set(table[0] for table in self.config.db2.get_tables(db2))
        all_tables = sorted(tables1 & tables2)

        # Calculate schema drift for all tables and write to drift analysis db
        all_schema_drift = []
        for table in all_tables:
            if schema_drift := self.generate_table_schema_drift(table):
                all_schema_drift.extend(schema_drift)
        if all_schema_drift:
            self.drift_analysis.add_schema_drift(all_schema_drift)

        # Calculate missing tables and write to drift analysis db
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

        # Run data drift check in proc pool
        with multiprocessing.Pool(CPU_COUNT) as pool:
            results = []

            for table_name in all_tables:
                if ignore_tables and any(re.match(it, table_name, re.IGNORECASE) for it in ignore_tables):
                    logger.info(f"{table_name} is an ignored table, skipping.")
                    continue

                if target_tables and not any(re.match(tt, table_name, re.IGNORECASE) for tt in target_tables):
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
                except duckdb.CatalogException:
                    continue

        self.drift_analysis.add_data_drift(table_data)
        self.drift_analysis.output_summary()


def generate_schema_columns(
    db_schema: dict,
    alt_db_schema: dict,
    table_rules: Union[TableRules, None]
) -> list[str]:
    """Returns a normalized list of columns to use when querying the table.

    Handles casting to string if there's a type mismatch between the 2 dbs.
    Escapes the column names to handle things like "this.name"
    Setting Null for columns that exist in the alt_db_schema but not the db_schema.

    The goal is to create columns that are the merged result of the 2 schemas
    """
    db_columns = []
    # Ensure only the columns we want
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

    # Set a null column for columns only in alt_db_schema
    for col in alt_db_schema.keys():
        if col not in db_schema:
            db_columns.append(f'NULL AS "{col}"')

    return db_columns


def compare_table_data(config: Config, table_name: str) -> Union[TableDataDiff, None]:
    """Runs the data diff check for a given table between the two dbs."""
    logger.info(f"Comparing table", table=table_name)

    tmp_path = f"/tmp/lotad_config_{table_name}.db"
    tmp_db_interface: LotadConnectionInterface = LotadConnectionInterface.create(tmp_path)
    tmp_db = tmp_db_interface.get_connection(read_only=False)

    db1_path = config.db1.connection_str
    db1 = config.db1.get_connection(read_only=True)

    db2_path = config.db2.connection_str
    db2 = config.db2.get_connection(read_only=True)

    # Attach the dbs to the tmp db so they can be used in queries
    tmp_db.execute(
        f"ATTACH '{db1_path}' AS db1 (READ_ONLY);\n"
        f"ATTACH '{db2_path}' AS db2 (READ_ONLY);".lstrip()
    )

    # Pull necessary context to generate the query then close the db conns no longer being used
    table_rules = config.get_table_rules(table_name)
    db1_schema = config.db1.get_schema(db1, table_name, config.ignore_dates)
    db2_schema = config.db2.get_schema(db2, table_name, config.ignore_dates)
    db1.close()
    db2.close()

    # Generate the query
    query_template = tmp_db_interface.get_query_template('db_compare_create_tmp_table_merge')
    db1_columns = generate_schema_columns(db1_schema, db2_schema, table_rules)
    db2_columns = generate_schema_columns(db2_schema, db1_schema, table_rules)
    if not db1_columns or not db2_columns:
        logger.warning("No columns found", table=table_name)
        return

    query = query_template.render(
        table_name=table_name,
        db1_path=db1_path,
        db1_columns=db1_columns,
        db2_path=db2_path,
        db2_columns=db2_columns,
    )

    try:
        tmp_db.execute(query)
        logger.info("Successfully processed table", table=table_name)

        contains_records = tmp_db.execute(
            f"SELECT * FROM {table_name} LIMIT 1;"
        ).fetchone()
        tmp_db.close()
        if contains_records:
            return TableDataDiff(
                table_name=table_name,
                tmp_path=tmp_path,
            )
        logger.debug("No changes discovered", table=table_name)
    except duckdb.CatalogException:
        logger.warning(f"Failed to process table", table=table_name)
        tmp_db.close()
        return
