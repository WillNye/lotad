import duckdb
import pytest

from lotad.config import Config
from lotad.data_analysis import DriftAnalysisTables
from lotad.db_compare import DatabaseComparator
from test import SampleTable
from test.utils import run_query


@pytest.mark.parametrize("config", ["duckdb_config", "postgres_config", "sqlite_config"], indirect=True)
def test_missing_column(config: Config):
    db_conn = config.db1.get_connection(read_only=False)
    test_table = SampleTable.EMPLOYEE.value
    db_conn.execute(
        f"ALTER TABLE {test_table} DROP COLUMN previous_positions;"
    )
    db_conn.close()

    comparator = DatabaseComparator(config)
    comparator.compare_all()

    drift_analysis_conn = duckdb.connect(config.output_path)

    drift_results = run_query(
        drift_analysis_conn,
        f"SELECT * FROM {DriftAnalysisTables.TABLE_SCHEMA_DRIFT.value}"
    )
    assert len(drift_results) == 1
    assert drift_results[0]["table_name"] == f'"{test_table}"'


@pytest.mark.parametrize("config", ["duckdb_config", "postgres_config", "sqlite_config"], indirect=True)
def test_mismatched_column_type(config: Config):
    db_conn = config.db1.get_connection(read_only=False)
    test_table = SampleTable.EMPLOYEE.value
    db_conn.execute(
        f"ALTER TABLE {test_table} ALTER COLUMN id TYPE VARCHAR;"
    )
    db_conn.close()

    comparator = DatabaseComparator(config)
    comparator.compare_all()

    drift_analysis_conn = duckdb.connect(config.output_path)

    drift_results = run_query(
        drift_analysis_conn,
        f"SELECT * FROM {DriftAnalysisTables.TABLE_SCHEMA_DRIFT.value}"
    )

    assert drift_results == [
        {
            "table_name": f'"{test_table}"',
            "column_name": '"id"',
            "db1": f'"{config.db1_details.db_id}"',
            "db1_column_type": '"VARCHAR"',
            "db2": f'"{config.db2_details.db_id}"',
            "db2_column_type": '"BIGINT"'
        }
    ]




