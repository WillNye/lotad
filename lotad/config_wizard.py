import multiprocessing
import os
import sys
from dataclasses import dataclass

import click
import duckdb
import inquirer

from lotad.config import CPU_COUNT, Config, TableRuleType
from lotad.connection import LotadConnectionInterface
from lotad.logger import logger


@dataclass
class IgnoreColumnSuggestions:
    table_name: str
    columns: list[str]


class ConfigWizard:

    def __init__(self, config: Config):
        self.config = config
        self.db1_path = config.db1_connection_string
        self.db2_path = config.db2_connection_string

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
        tmp_path = f"/tmp/lotad_config_{table_name}.db"
        tmp_db_interface: LotadConnectionInterface = LotadConnectionInterface.create(tmp_path)
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

        tmp_db.execute(
            f"ATTACH '{self.db1_path}' AS db1 (READ_ONLY);\n"
            f"ATTACH '{self.db2_path}' AS db2 (READ_ONLY);".lstrip()
        )

        query = tmp_db_interface.get_query_template('config_builder_ignore_columns_create_table')
        tmp_db.execute(
            query.render(
                table_name=table_name,
                db1_path=self.db1_path,
                db2_path=self.db2_path,
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
        db1_tables = self.config.db1.get_tables(db1)
        db2_tables = self.config.db2.get_tables(db2)
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
        q = [
            inquirer.List(
                "user_selection",
                message="Ignore all date columns?",
                choices=["yes", "no"],
                default="yes" if config.ignore_dates else "no"
            ),
        ]
        answers = inquirer.prompt(q)
        config.ignore_dates = bool(answers["user_selection"] == "yes")
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
        q = [
            inquirer.Text(
                "user_selection",
                message="Path where the DuckDB diff file will be written.",
                default=config.output_path
            ),
        ]
        answers = inquirer.prompt(q)
        config.output_path = answers["user_selection"].replace(" ", "").replace("\n", "")
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
        q = [
            inquirer.List(
                "user_selection",
                message="Proceed?",
                choices=["yes", "no"],
            ),
        ]
        answers = inquirer.prompt(q)

        if answers["user_selection"] == "yes":
            self.generate_ignored_columns()
            config.write()
            click.echo("Config updated successfully.\n")
        else:
            click.echo("Ignored columns were not generated. Going back.")

    @staticmethod
    def exit():
        sys.exit(0)

    @classmethod
    def cli_start(cls, config_path: str):
        choice_map = {
            "Generate ignored columns for tables.": "run_generate_ignored_columns",
            "Set the list of ignored tables.": "update_ignore_tables",
            "Set the list of target tables.": "update_target_tables",
            "Set the path where the DuckDB diff file will be written.": "update_output_path",
            "Set ignore date behavior for config.": "update_ignore_dates",
        }
        if os.path.exists(config_path):
            config = Config.load(config_path)
        else:
            click.echo(
                "It doesn't look like this config exists yet. "
                "Let me get a bit more information."
            )
            questions = [
                inquirer.Text(
                    'db1_connection_string',
                    message="What is the connection string to the first target databases?"
                ),
                inquirer.Text(
                    'db2_connection_string',
                    message="What is the connection string to the second target database?"
                ),
                inquirer.Confirm("ignore_dates", message="Should all date columns be ignored?")
            ]
            answers = inquirer.prompt(questions)
            config = Config(path=config_path, **answers.items())
            config.write()

        # Adding here to ensure it is the last option in the list
        choice_map["Done."] = "exit"
        config_builder = cls(config)
        while True:
            questions = [
                inquirer.List(
                    "user_selection",
                    message="What would you like to do next?",
                    choices=list(choice_map.keys()),
                ),
            ]
            try:
                answers = inquirer.prompt(questions)
                user_selection = answers["user_selection"]
                if user_selection == "Done.":
                    sys.exit(0)

                # Run the action that corresponds to the user selected option
                getattr(config_builder, choice_map[user_selection])()

            except (KeyboardInterrupt, TypeError):
                sys.exit(0)
