import multiprocessing
from dataclasses import dataclass

import duckdb

from lotad.config import CPU_COUNT, Config, TableRuleType
from lotad.connection import LotadConnectionInterface
from lotad.logger import logger


@dataclass
class IgnoreColumnSuggestions:
    table_name: str
    columns: list[str]


class ConfigBuilder:

    def __init__(self, config: Config):
        self.config = config
        self.db1_path = str(config.db1_connection_string)
        self.db2_path = str(config.db2_connection_string)

        self.db_interface: LotadConnectionInterface = LotadConnectionInterface.create_connection()

    def get_table_ignore_columns(self, table_name: str) -> IgnoreColumnSuggestions:
        logger.info('Collecting ignorable columns for %s', table_name)

        response = IgnoreColumnSuggestions(
            table_name=table_name,
            columns=[]
        )
        tmp_path = f"/tmp/lotad_config_{table_name}.db"
        tmp_db = self.db_interface.get_connection(tmp_path, read_only=False)

        db1 = self.db_interface.get_connection(self.db1_path, read_only=True)
        db1_schema = self.db_interface.get_schema(db1, table_name)
        db1.close()

        db2 = self.db_interface.get_connection(self.db2_path, read_only=True)
        db2_schema = self.db_interface.get_schema(db2, table_name)
        db1.close()

        shared_columns = {
            f'"{col}"': col_type
            for col, col_type in db1_schema.items()
            if col in db2_schema and col_type == db2_schema[col]
        }
        shared_columns_str = ", ".join(list(shared_columns.keys()))

        tmp_db.execute(
            f"""ATTACH '{self.db1_path}' AS db1 (READ_ONLY);
            ATTACH '{self.db2_path}' AS db2 (READ_ONLY);""".lstrip()
        )

        query = f"""CREATE OR REPLACE TABLE {table_name} AS
WITH db_results AS (
    SELECT '{self.db1_path}' AS db_path, 
        {shared_columns_str} 
    FROM db1.{table_name}
    UNION
    SELECT '{self.db2_path}' AS db_path, 
        {shared_columns_str} 
    FROM db2.{table_name}
)
SELECT * FROM db_results;"""
        tmp_db.execute(query)

        for col in shared_columns.keys():
            row_count = tmp_db.execute(
                f"""
                WITH t1 AS (
                    SELECT DISTINCT {col}
                    FROM {table_name}
                    WHERE db_path = '{self.db1_path}' 
                    LIMIT 10000
                )
                
                SELECT COUNT(t2.{col}) 
                FROM {table_name} t2 
                JOIN t1 ON t1.{col} = t2.{col} AND t2.db_path = '{self.db2_path}' 
                """
            ).fetchone()[0]
            if not row_count:
                response.columns.append(col)

        tmp_db.close()

        logger.info('Finished collecting ignorable columns for %s', table_name)

        return response

    def generate_ignored_columns(self):
        db1 = self.db_interface.get_connection(self.db1_path, read_only=True)
        db2 = self.db_interface.get_connection(self.db2_path, read_only=True)
        db1_tables = self.db_interface.get_tables(db1)
        db2_tables = self.db_interface.get_tables(db2)
        shared_tables = [table for table in db1_tables if table in db2_tables]

        existing_ignore_rules = set()
        if self.config.table_rules:
            for table_rules in self.config.table_rules:
                for table_rule in table_rules.rules:
                    if table_rule.rule_type == TableRuleType.IGNORE_COLUMN:
                        existing_ignore_rules.add(f"{table_rules.table_name}-{table_rule.rule_value}")

        with multiprocessing.Pool(CPU_COUNT) as pool:
            results = []
            for table in shared_tables:
                result = pool.apply_async(self.get_table_ignore_columns, table)
                results.append(result)

            # Get the results
            for result in results:
                try:
                    table_result: IgnoreColumnSuggestions = result.get()
                    if table_result.columns:
                        table = table_result.table_name
                        for column in table_result.columns:
                            column = column.replace('"', '')
                            rule_str = f"{table}-{column}"
                            if rule_str not in existing_ignore_rules:
                                self.config.add_table_rule(
                                    table,
                                    TableRuleType.IGNORE_COLUMN,
                                    column
                                )

                except duckdb.duckdb.CatalogException:
                    continue

        self.config.write()
