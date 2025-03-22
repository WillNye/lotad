import multiprocessing
import os
import re
import tempfile
from typing import Optional, Union

import duckdb
from sql_metadata import Parser as SQLParser

from lotad.config import Config, TableConfig, TableRuleType, CPU_COUNT, DatabaseDetails
from lotad.connection import LotadConnectionInterface, DatabaseType
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
        self.db1_id = config.db1.db_id
        self.db2_id = config.db2.db_id

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
                    db1=self.db1_id,
                    db2=self.db2_id,
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
                    db1=self.db1_id,
                    db2=self.db2_id,
                    column_name=column,
                    db1_column_type=None,
                    db2_column_type=schema2[column],
                )
            )

        # Column type mismatches
        for col in set(schema1.keys()) & set(schema2.keys()):
            if schema1[col] != schema2[col]:
                db_1_generic_type = self.config.db1.get_generic_column_type(
                    schema1[col],
                    self.config.db2.db_type
                )
                db2_generic_type = self.config.db2.get_generic_column_type(
                    schema2[col],
                    self.config.db1.db_type
                )

                if (
                    db_1_generic_type
                    != db2_generic_type
                ):
                    if schema1[col] == db_1_generic_type:
                        db1_column_type = schema1[col]
                    else:
                        db1_column_type = f"{schema1[col]} ({db_1_generic_type})"

                    if schema2[col] == db2_generic_type:
                        db2_column_type = schema2[col]
                    else:
                        db2_column_type = f"{schema2[col]} ({db2_generic_type})"

                    response.append(
                        TableSchemaDrift(
                            table_name=table_name,
                            db1=self.db1_id,
                            db2=self.db2_id,
                            column_name=col,
                            db1_column_type=db1_column_type,
                            db2_column_type=db2_column_type,
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

        tables1 = set(table[0] for table in self.config.db1.list_tables(db1))
        tables2 = set(table[0] for table in self.config.db2.list_tables(db2))
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
            self.db1_id,
            tables1,
            self.db2_id,
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


def get_table_query(
    table_name: str, 
    columns: list[str], 
    db_name: str, 
    db_interface: LotadConnectionInterface,
    table_config: Optional[TableConfig], 
) -> str:
    """Generates a SQL query to select data from a table.

    Used as part of db_compare_create_tmp_table_merge query.

    Args:
        table_name: Name of the table to query
        columns: List of columns to select
        db_name: Name of the database (db1 or db2) 
        db_interface:
        table_config:
    """
    if table_config and table_config.query:
        # If custom query exists in config, 
        # use it but prefix tables with db name because 
        # the query is ran as a duckdb attached db
        query = table_config.query
        tables = SQLParser(query).tables
        for table in tables:
            # Replace table names with fully qualified db.table, but only when they are
            # standalone words (bounded by spaces, newlines, or SQL-specific characters)
            query = re.sub(
                rf'(?<=[\s\n(,])({re.escape(table)})(?=[\s\n),;])',
                rf'{db_name}.\1',
                query
            )
        return query

    # Use default template query that selects all columns
    query_template = db_interface.get_query_template('default_select_from_table')
    query = query_template.render(
        table_name=table_name,
        columns=columns,
        db_name=db_name,
    )
    return query


def generate_schema_columns(
    db_schema: dict,
    alt_db_schema: dict,
    table_configs: Union[TableConfig, None]
) -> list[str]:
    """Returns a normalized list of columns to use when querying the table.

    Handles casting to string if there's a type mismatch between the 2 dbs.
    Escapes the column names to handle things like "this.name"
    Setting Null for columns that exist in the alt_db_schema but not the db_schema.

    The goal is to create columns that are the merged result of the 2 schemas
    """
    db_columns = dict()
    # Ensure only the columns we want
    for col, col_type in db_schema.items():
        if table_configs:
            col_rule = table_configs.get_rule(col)
            if col_rule and col_rule.rule_type == TableRuleType.IGNORE_COLUMN:
                continue

        if col not in alt_db_schema:
            continue
        elif (
            any(col_type.startswith(c_type) for c_type in ["STRUCT", "MAP", "LIST", "ARRAY"])
            or col_type.endswith("[]")
        ):
            col_val = f'to_json("{col}") AS "{col}"'
        elif alt_db_schema[col] != col_type:
            col_val = f'"{col}"::VARCHAR AS "{col}"'
        else:
            col_val = f'"{col}"'
        db_columns[col] = col_val

    col_names = sorted(db_columns.keys())
    return [db_columns[c] for c in col_names]


def compare_table_data(config: Config, table_name: str) -> Union[TableDataDiff, None]:
    """Runs the data diff check for a given table between the two dbs."""
    logger.info(f"Comparing table", table=table_name)
    tmp_path = os.path.join(tempfile.mkdtemp(), f"lotad_{table_name}.db")
    tmp_db_interface: LotadConnectionInterface = LotadConnectionInterface.create(
        DatabaseDetails(database_type=DatabaseType.DUCKDB, path=tmp_path)
    )
    tmp_db = tmp_db_interface.get_connection(read_only=False)

    db1_id = config.db1.db_id
    db1 = config.db1.get_connection(read_only=True)

    db2_id = config.db2.db_id
    db2 = config.db2.get_connection(read_only=True)

    # Attach the dbs to the tmp db so they can be used in queries
    tmp_db.execute(
        f"ATTACH '{config.db1.connection_string}' AS db1 {config.db1.attach_db_params(True)};\n"
        f"ATTACH '{config.db2.connection_string}' AS db2 {config.db2.attach_db_params(True)};".lstrip()
    )

    # Pull necessary context to generate the query then close the db conns no longer being used
    table_config = config.get_table_config(table_name)
    db1_schema = config.db1.get_schema(db1, table_name, config.ignore_dates)
    db2_schema = config.db2.get_schema(db2, table_name, config.ignore_dates)
    db1.close()
    db2.close()

    # Generate the query
    query_template = tmp_db_interface.get_query_template('db_compare_create_tmp_table_merge')
    db1_columns = generate_schema_columns(db1_schema, db2_schema, table_config)
    db2_columns = generate_schema_columns(db2_schema, db1_schema, table_config)
    if not db1_columns or not db2_columns:
        logger.warning("No columns found", table=table_name)
        return
    
    db1_query = get_table_query(table_name, db1_columns, 'db1', tmp_db_interface, table_config)
    db2_query = get_table_query(table_name, db2_columns, 'db2', tmp_db_interface, table_config)

    query = query_template.render(
        table_name=table_name,
        db1_path=db1_id,
        db2_path=db2_id,
        db1_query=db1_query,
        db2_query=db2_query,
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
    except Exception as err:
        logger.error(
            "Failed to process table",
            error=str(err),
            table=table_name,
            query=query
        )
        raise
