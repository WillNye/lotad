import duckdb

from lotad.config import Config
from lotad.data_analysis import DriftAnalysisTables
from lotad.db_compare import DatabaseComparator
from test import SampleTable
from test.utils import run_query


def test_missing_table(config: Config):
    db_conn = config.db2.get_connection(read_only=False)
    test_table = SampleTable.EMPLOYEE.value
    db_conn.execute(
        f"DROP TABLE {test_table};"
    )
    db_conn.close()

    comparator = DatabaseComparator(config)
    comparator.compare_all()

    drift_analysis_conn = duckdb.connect(config.output_path)

    drift_results = run_query(
        drift_analysis_conn,
        f"SELECT * FROM {DriftAnalysisTables.MISSING_TABLE.value}"
    )
    assert drift_results == [
        {
            "table_name": f'"{test_table}"',
            "observed_in": f'"{config.db1_connection_string}"',
            "missing_in": f'"{config.db2_connection_string}"',
        }
    ]


