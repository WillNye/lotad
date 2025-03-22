import multiprocessing
import os
import re
import sys
from dataclasses import dataclass
from typing import Optional

import click
import duckdb
# Both inquirer and questionary are used 
# because neither has all the features we need
# inquirer has a text editor option not in questionary
# questionary is used for pretty much everything else
import inquirer
import questionary

from lotad.config import CPU_COUNT, Config, TableRule, TableRuleType
from lotad.connection import LotadConnectionInterface, DatabaseType, DatabaseDetails
from lotad.logger import logger


@dataclass
class IgnoreColumnSuggestions:
    table_name: str
    columns: list[str]


class ConfigWizard:

    def __init__(self, config: Config):
        self.config = config

    def get_table_ignore_columns(self, table_name: str) -> IgnoreColumnSuggestions:
        """Returns all columns with no matching values between the 2 dbs

        :param table_name:
        :return: IgnoreColumnSuggestions
        """

        logger.info('Collecting ignorable columns for %s', table_name)

        response = IgnoreColumnSuggestions(
            table_name=table_name,
            columns=[]
        )
        tmp_db_interface: LotadConnectionInterface = LotadConnectionInterface.create(
            DatabaseDetails(
                database_type=DatabaseType.DUCKDB,
                path=f"/tmp/lotad_config_{table_name}.db"
            )
        )
        tmp_db = tmp_db_interface.get_connection(read_only=False)

        db1 = self.config.db1.get_connection(read_only=True)
        db1_schema = self.config.db1.get_schema(db1, table_name, self.config.ignore_dates)
        db1.close()

        db2 = self.config.db2.get_connection(read_only=True)
        db2_schema = self.config.db2.get_schema(db2, table_name, self.config.ignore_dates)
        db1.close()

        shared_columns = {
            f'"{col}"': col_type
            for col, col_type in db1_schema.items()
            if col in db2_schema and col_type == db2_schema[col]
        }

        config = self.config
        tmp_db.execute(
            f"ATTACH '{config.db1.connection_string}' AS db1 {config.db1.attach_db_params(True)};\n"
            f"ATTACH '{config.db2.connection_string}' AS db2 {config.db2.attach_db_params(True)};".lstrip()
        )

        query = tmp_db_interface.get_query_template('config_builder_ignore_columns_create_table')
        tmp_db.execute(
            query.render(
                table_name=table_name,
                db1_path=config.db1_details.db_id,
                db2_path=config.db2_details.db_id,
                shared_columns=list(shared_columns.keys())
            )
        )

        for col in shared_columns.keys():
            query = tmp_db_interface.get_query_template('config_builder_ignore_columns_get_column_val_intersect')
            row_count = tmp_db.execute(
                query.render(
                    table_name=table_name,
                    col=col
                )
            ).fetchone()[0]
            if not row_count:
                response.columns.append(col)

        tmp_db.close()

        logger.info('Finished collecting ignorable columns for %s', table_name)

        return response

    def generate_ignored_columns(self):
        """Updates the config to include the columns with no matching values between the 2 dbs for all tables.

        The primary use case for this is scenarios like a UUID for the PK
        """

        db1 = self.config.db1.get_connection(read_only=True)
        db2 = self.config.db2.get_connection(read_only=True)
        db1_tables = self.config.db1.list_tables(db1)
        db2_tables = self.config.db2.list_tables(db2)
        shared_tables = [
            table
            for table in db1_tables
            if table in db2_tables
            and not any(re.match(it, table[0], re.IGNORECASE) for it in self.config.ignore_tables)
        ]

        existing_ignore_rules = set()
        if self.config.table_configs:
            for table_configs in self.config.table_configs:
                for table_rule in table_configs.rules:
                    if table_rule.rule_type == TableRuleType.IGNORE_COLUMN:
                        existing_ignore_rules.add(f"{table_configs.table_name}-{table_rule.rule_value}")

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
                                self.config.update_table_config(
                                    table,
                                    TableRule(TableRuleType.IGNORE_COLUMN, column)
                                )

                except duckdb.CatalogException:
                    continue

    def update_ignore_dates(self):
        """Config wizard prompt to update the ignore_dates attr in the config
        """
        config = self.config
        click.echo(
            "If set to true all date columns will be ignored when performing the diff. "
            "Useful to set true for databases that work in a pipeline"
            " that always alters multiple date values on every run."
        )
        user_selection = questionary.select(
            "Ignore all date columns?",
            choices=["yes", "no"],
            default="yes" if config.ignore_dates else "no"
        )
        config.ignore_dates = bool(user_selection == "yes")
        config.write()
        click.echo("Config updated successfully.\n")

    def update_ignore_tables(self):
        """Config wizard prompt to update the ignore_tables attr in the config
        """
        config = self.config
        click.echo(
            "A diff will be performed on all tables EXCEPT these. "
            "Supports regex. NOT case sensitive."
        )
        q = [
            inquirer.Editor(
                "user_selection",
                message="Provide a comma separated list of tables to ignore.",
                default=', '.join(config.ignore_tables) if config.ignore_tables else ''
            ),
        ]
        answers = inquirer.prompt(q)
        config.ignore_tables = [
            table
            for table in answers["user_selection"].replace(" ", "").replace("\n", "").split(",")
            if table
        ]
        config.write()
        click.echo("Config updated successfully.\n")

    def update_output_path(self):
        """Config wizard prompt to update the output_path attr in the config
        """
        config = self.config
        click.echo(
            "A diff will be performed on all tables EXCEPT these. "
            "Supports regex. NOT case sensitive."
        )
        user_selection = questionary.text(
            message="Path where the DuckDB diff file will be written.",
            default=config.output_path
        ).ask()
        config.output_path = user_selection.replace(" ", "").replace("\n", "")
        config.write()
        click.echo("Config updated successfully.\n")

    def update_target_tables(self):
        """Config wizard prompt to update the target_tables attr in the config
        """
        config = self.config
        click.echo(
            "A diff will only be provided on these tables. "
            "Supports regex. NOT case sensitive."
        )
        q = [
            inquirer.Editor(
                "user_selection",
                message="Provide a comma separated list of target tables.",
                default=', '.join(config.target_tables) if config.target_tables else ''
            ),
        ]
        answers = inquirer.prompt(q)
        config.target_tables = [
            table
            for table in answers["user_selection"].replace(" ", "").replace("\n", "").split(",")
            if table
        ]
        config.write()
        click.echo("Config updated successfully.\n")

    def update_custom_query(self):
        """Config wizard prompt to update or add a custom query for a table"""
        db1 = self.config.db1.get_connection(read_only=True)
        db2 = self.config.db2.get_connection(read_only=True)
        db1_tables = self.config.db1.list_tables(db1)
        db2_tables = set(self.config.db2.list_tables(db2))
        shared_tables = [
            table[0] for table in db1_tables
            if table in db2_tables
        ]
        
        if not shared_tables:
            click.echo("No shared tables found between the databases.\n")
            return

        table_name = questionary.autocomplete(
            message="Select a table to add/update custom query for:",
            choices=shared_tables,
            style=questionary.Style([
                ('qmark', 'fg:cyan bold'),
                ('question', 'bold'),
                ('answer', 'fg:green bold'),
            ])
        ).ask()
        if not table_name:
            return

        table_config = self.config.get_table_config(table_name)
        default_query = None if not table_config else table_config._query
            
        while True:
            q = [
                inquirer.Editor(
                    "query",
                    message="Enter the custom query:",
                    default=default_query
                ),
            ]                
            answers = inquirer.prompt(q)

            try:
                self.config.update_table_config(
                    table_name,
                    query=answers["query"]
                )
                break
            except (TypeError, KeyboardInterrupt):
                return
            except Exception as e:
                click.echo(f"Unable to set custom query due to: {e}")

        self.config.write()
        click.echo("Config updated successfully.\n")

    def _get_existing_query(self, table_name: str) -> str:
        """Helper method to get existing custom query for a table if it exists"""
        if self.config.table_configs:
            for table_config in self.config.table_configs:
                if table_config.table_name == table_name:
                    for rule in table_config.rules:
                        if rule.rule_type == TableRuleType.CUSTOM_QUERY:
                            return rule.rule_value
        return f"SELECT * FROM {table_name}"

    def run_generate_ignored_columns(self):
        """Config wizard prompt to trigger generate_ignored_columns
        """
        config = self.config
        click.echo(
            "This will create or append the columns to ignore for all tables.\n"
            "Works by finding all columns with no matching values.\n"
            "Useful for no deterministic columns like a uuid primary key.\n"
            "Will NOT remove any ignore column rules already in the config."
        )
        user_selection = questionary.select(
            message="Proceed?",
            choices=["yes", "no"],
        ).ask()

        if user_selection == "yes":
            self.generate_ignored_columns()
            config.write()
            click.echo("Config updated successfully.\n")
        else:
            click.echo("Ignored columns were not generated. Going back.")

    @staticmethod
    def set_database_details():

        db_type = questionary.select(
            message="What type of database are you connecting to?",
            choices=[db_type.value for db_type in DatabaseType],
        ).ask()
        if db_type == DatabaseType.DUCKDB.value:
            return DatabaseDetails(
                database_type=DatabaseType.DUCKDB,
                path=questionary.text(
                    message="What is the DuckDB path?",
                ).ask(),
            )
        elif db_type == DatabaseType.POSTGRESQL.value:
            host = questionary.text(
                message="What is the host? Example: 127.0.0.1",
            ).ask()
            port = questionary.text(
                message="What is the port? Example: 5432 (default)",
                default="5432",
            ).ask()
            database = questionary.text(
                message="What is the database name?",
            ).ask()
            user = questionary.text(
                message="What is the PostgreSQL user?",
            ).ask()
            password = None
            passfile = None

            if questionary.confirm("Does the user have a password?").ask():
                password = questionary.password(
                    message="What is the user password?",
                ).ask()
            elif questionary.confirm("Does the user have a passfile?").ask():
                passfile = questionary.text(
                    message="What is the passfile path?",
                ).ask()

            return DatabaseDetails(
                database_type=DatabaseType.POSTGRESQL,
                path=host,
                port=port,
                user=user,
                password=password,
                passfile=passfile,
                database=database,
            )
        elif db_type == DatabaseType.SQLITE.value:
            return DatabaseDetails(
                database_type=DatabaseType.SQLITE,
                path=questionary.text(
                    message="What is the SQLite file path?",
                ).ask(),
            )
        else:
            raise ValueError("Invalid database type")

    @staticmethod
    def exit():
        sys.exit(0)

    @classmethod
    def cli_start(cls, config_path: Optional[str] = None):
        choice_map = {
            "Generate ignored columns for tables.": "run_generate_ignored_columns",
            "Set the list of ignored tables.": "update_ignore_tables",
            "Set the list of target tables.": "update_target_tables",
            "Set the path where the DuckDB diff file will be written.": "update_output_path",
            "Set ignore date behavior for config.": "update_ignore_dates",
            "Set a custom query for a table.": "update_custom_query",
        }
        if not config_path:
            config_path = questionary.text(
                message="What is the path of the config file, including the file name?",
            ).ask()
            if not config_path:
                sys.exit(0)

        if os.path.exists(config_path):
            config = Config.load(config_path)
        else:
            click.echo(
                "It doesn't look like this config exists yet. "
                "Let me get a bit more information."
            )

            config = Config(
                path=config_path,
                db1_details=cls.set_database_details(),
                db2_details=cls.set_database_details(),
                ignore_dates=questionary.confirm(message="Should all date columns be ignored?").ask()
            )
            config.write()

        # Adding here to ensure it is the last option in the list
        choice_map["Done."] = "exit"
        config_builder = cls(config)
        while True:
            try:
                user_selection = questionary.select(
                    message="What would you like to do next?",
                    choices=list(choice_map.keys()),
                ).ask()
                if user_selection == "Done.":
                    sys.exit(0)

                # Run the action that corresponds to the user selected option
                getattr(config_builder, choice_map[user_selection])()

            except (KeyError, KeyboardInterrupt, TypeError):
                sys.exit(0)
