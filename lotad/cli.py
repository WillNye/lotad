from datetime import datetime

import click

from lotad.config import Config
from lotad.config_wizard import ConfigWizard
from lotad.db_compare import DatabaseComparator
from lotad.logger import logger


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    '--config',
    help='The path of the config file that will be updated/created.',
)
def setup(config):
    ConfigWizard.cli_start(config)


@cli.command()
@click.option(
    '--config',
    help='The path of the config file that will be used.',
    required=False
)
@click.option(
    '--db1_connection_string',
    help='One of the two databases that will be diffed against. '
         'Only provide if a config is not defined.',
    required=False
)
@click.option(
    '--db2_connection_string',
    help='The second of the two databases that will be diffed against. '
         'Only provide if a config is not defined.',
    required=False
)
@click.option(
    '--ignore_dates',
    help="Ignore date columns when performing the diff check. "
         "Useful when dates are updated on every run creating noise. "
         "If a config is defined, this will overwrite the value set in the config.",
    required=False,
)
@click.option(
    '--target_tables',
    help='A diff will only be provided on these tables.'
         'Supports many. Supports regex. NOT case sensitive.'
         'If a config is defined, this will overwrite the value set in the config.',
    required=False,
    multiple=True
)
@click.option(
    '--ignore_tables',
    help='A diff will be performed on all tables EXCEPT these. '
         'Supports many. Supports regex. NOT case sensitive.'
         'If a config is defined, this will overwrite the value set in the config.',
    required=False,
    multiple=True
)
def run(
    config: str = None,
    db1_connection_string: str = None,
    db2_connection_string: str = None,
    ignore_dates: bool = None,
    target_tables: list[str] = None,
    ignore_tables: list[str] = None
):
    if config is None and (db1_connection_string is None or db2_connection_string is None):
        click.echo(
            "Either a config or both db1_connection_string and db2_connection_string must be provided."
        )
        click.Abort()

    if not config:
        run_config = Config(
            path="/tmp/lotad_config.yaml",
            db1_connection_string=db1_connection_string,
            db2_connection_string=db2_connection_string,
            ignore_dates=bool(ignore_dates),
        )
    else:
        run_config = Config.load(config)
        if ignore_dates is not None:
            run_config.ignore_dates = ignore_dates

    if target_tables:
        run_config.target_tables = target_tables
    if ignore_tables:
        run_config.ignore_tables = ignore_tables

    start_time = datetime.now()
    try:
        comparator = DatabaseComparator(run_config)
        comparator.compare_all()
    except Exception as e:
        logger.error(f"Error during comparison: {str(e)}")
        click.Abort()
    else:
        logger.info(
            f"Diff check completed successfully in {datetime.now() - start_time} seconds.\n"
            f"DuckDB file saved to {run_config.output_path}"
        )


if __name__ == '__main__':
    cli()

