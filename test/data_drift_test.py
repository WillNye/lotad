import copy
import json
from typing import Any

import duckdb
import pytest

from lotad.config import Config, TableRule, TableRuleType
from lotad.data_analysis import DriftAnalysisTables
from lotad.db_compare import DatabaseComparator
from test import SampleTable
from test.conftest import FAKER
from test.utils import run_query, get_random_row_from_table, normalize_results


def normalize_and_order_drift_table_results(cfg: Config, results: list[dict]) -> list[dict]:
    """Accepts a list of 2 table records and orders them such that the db1 connection is always listed first
    Additionally, removes lotad metadata so the results can be used for comparison
      and casts everything to string to supress issues when performing diff check
       caused by type casting to normalize values across different dbs.

    :param cfg:
    :param results:
    :return:
    """
    assert len(results) == 2
    for result in results:
        result.pop('hashed_row', None)
        for k, v in result.items():
            result[k] = str(v)

    response = [results[0]]
    if results[1]["observed_in"] == cfg.db1_details.db_id:
        response.insert(0, results[1])
    else:
        response.append(results[1])
    return response


@pytest.mark.parametrize("config", ["duckdb_config", "postgres_config", "sqlite_config"], indirect=True)
def test_no_changes(config: Config):
    comparator = DatabaseComparator(config)
    comparator.compare_all()

    drift_analysis_conn = duckdb.connect(config.output_path)
    drift_results = run_query(
        drift_analysis_conn,
        f"SELECT * FROM {DriftAnalysisTables.DB_DATA_DRIFT_SUMMARY.value}"
    )

    assert len(drift_results) == 0


@pytest.mark.parametrize("config", ["duckdb_config", "postgres_config", "sqlite_config"], indirect=True)
def test_missing_column_has_no_effect_on_data_drift(config: Config):
    # A missing column shouldn't impact the data drift summary
    # because only columns that exist in both dbs are checked
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
        f"SELECT * FROM {DriftAnalysisTables.DB_DATA_DRIFT_SUMMARY.value}"
    )
    assert len(drift_results) == 0


@pytest.mark.parametrize("config", ["duckdb_config", "postgres_config", "sqlite_config"], indirect=True)
def test_no_delta_on_mismatched_column_type(config: Config):
    # Lotad should normalize type inconsistencies when performing the data drift check
    # Column type delta info is represented in a dedicated table
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
        f"SELECT * FROM {DriftAnalysisTables.DB_DATA_DRIFT_SUMMARY.value}"
    )
    assert len(drift_results) == 0


@pytest.mark.parametrize("config", ["duckdb_config", "postgres_config", "sqlite_config"], indirect=True)
def test_missing_row_delta(config: Config):
    db_conn = config.db1.get_connection(read_only=False)
    test_table = SampleTable.USER.value
    random_row = get_random_row_from_table(db_conn, test_table)
    db_conn.execute(
        f"DELETE FROM {test_table} WHERE id = {random_row["id"]};"
    )
    db_conn.close()

    expected_drift_row = random_row
    expected_drift_row["observed_in"] = config.db2_details.db_id

    comparator = DatabaseComparator(config)
    comparator.compare_all()

    drift_analysis_conn = duckdb.connect(config.output_path)

    drift_summary_results = run_query(
        drift_analysis_conn,
        f"SELECT * FROM {DriftAnalysisTables.DB_DATA_DRIFT_SUMMARY.value}"
    )
    assert drift_summary_results == [
        {
            "table_name": test_table,
            "db1": config.db1_details.db_id,
            "rows_only_in_db1": 0,
            "db2": config.db2_details.db_id,
            "rows_only_in_db2": 1,
        }
    ]

    table_drift_results = run_query(
        drift_analysis_conn,
        f"SELECT * FROM {test_table}"
    )
    assert len(table_drift_results) == 1
    table_drift_result = table_drift_results[0]
    del table_drift_result["hashed_row"]

    assert int(table_drift_result["id"]) == int(expected_drift_row["id"])


def run_value_delta(
    test_config: Config,
    test_table: str,
    updated_column: str,
    updated_value: Any,
    updated_db: str,
    change_expected: bool = True,
):
    # This felt cleaner than using parametrize because of all the small config modifiers.
    # There are workarounds to include them as parameters
    # But that felt less readable and more difficult to maintain

    if updated_db == test_config.db1_details.db_id:
        random_row_observed_in = test_config.db2_details.db_id
        db_conn = test_config.db1.get_connection(read_only=False)
    else:
        raise ValueError("Only db1 is supported")

    random_row = get_random_row_from_table(db_conn, test_table)

    db_conn.execute(
        f"UPDATE {test_table} SET {updated_column} = ? WHERE id = {random_row["id"]};",
        [updated_value]
    )
    db_conn.close()

    random_row["observed_in"] = random_row_observed_in

    expected_drift_row = copy.deepcopy(random_row)
    expected_drift_row["observed_in"] = updated_db
    expected_drift_row[updated_column] = updated_value

    run_compare(
        test_config,
        test_table,
        random_row,
        expected_drift_row,
        change_expected
    )


def run_compare(
    test_config: Config,
    test_table: str,
    random_row: dict,
    expected_drift_row: dict,
    change_expected: bool = True,
):
    assert "observed_in" in random_row
    assert "observed_in" in expected_drift_row

    comparator = DatabaseComparator(test_config)
    comparator.compare_all()

    drift_analysis_conn = duckdb.connect(test_config.output_path)

    drift_summary_results = run_query(
        drift_analysis_conn,
        f"SELECT * FROM {DriftAnalysisTables.DB_DATA_DRIFT_SUMMARY.value}"
    )

    if change_expected:
        assert drift_summary_results == [
            {
                "table_name": test_table,
                "db1": test_config.db1_details.db_id,
                "rows_only_in_db1": 1,
                "db2": test_config.db2_details.db_id,
                "rows_only_in_db2": 1,
            }
        ]
        table_drift_results = normalize_results(
            run_query(
                drift_analysis_conn,
                f"SELECT to_json(t) as row FROM {test_table} t;"
            )
        )

        assert (
                normalize_and_order_drift_table_results(
                    test_config, table_drift_results
                )
                ==
                normalize_and_order_drift_table_results(
                    test_config,
                    [expected_drift_row, random_row]
                )
        )
    else:
        all_tables = run_query(
            drift_analysis_conn,
            "SHOW TABLES;"
        )
        assert not any(table["name"] == test_table for table in all_tables)
        assert len(drift_summary_results) == 0


@pytest.mark.parametrize("config", ["duckdb_config", "postgres_config", "sqlite_config"], indirect=True)
def test_no_delta_on_ignored_column(config: Config):
    test_table = SampleTable.EMPLOYEE.value
    ignored_column = "blood_group"
    config.update_table_config(
        test_table,
        table_rule=TableRule(TableRuleType.IGNORE_COLUMN, ignored_column)
    )

    run_value_delta(
        config,
        test_table,
        ignored_column,
        "HIPA",
        config.db1_details.db_id,
        change_expected=False
    )


@pytest.mark.parametrize("config", ["duckdb_config", "postgres_config", "sqlite_config"], indirect=True)
def test_int_value_delta(config: Config):
    run_value_delta(
        config,
        SampleTable.USER.value,
        "id",
        -1,
        config.db1_details.db_id
    )


@pytest.mark.parametrize("config", ["duckdb_config", "postgres_config", "sqlite_config"], indirect=True)
def test_date_value_delta(config: Config):
    run_value_delta(
        config,
        SampleTable.USER.value,
        "birthdate",
        FAKER.date_of_birth(),
        config.db1_details.db_id
    )


@pytest.mark.parametrize("config", ["duckdb_config", "postgres_config", "sqlite_config"], indirect=True)
def test_date_value_delta_with_ignore_date_set(config: Config):
    config.ignore_dates = True

    run_value_delta(
        config,
        SampleTable.USER.value,
        "birthdate",
        FAKER.date_of_birth(),
        config.db1_details.db_id,
        False
    )


@pytest.mark.parametrize("config", ["duckdb_config", "postgres_config", "sqlite_config"], indirect=True)
def test_str_value_delta(config: Config):
    run_value_delta(
        config,
        SampleTable.EMPLOYEE.value,
        "blood_group",
        "HIPA",
        config.db1_details.db_id
    )


@pytest.mark.parametrize("config", ["duckdb_config", "postgres_config", "sqlite_config"], indirect=True)
def test_json_value_delta(config: Config):
    db_conn = config.db1.get_connection(read_only=False)
    random_row_observed_in = config.db2_details.db_id
    updated_db = config.db1_details.db_id
    test_table = SampleTable.COMPANY.value
    updated_column = "owner"

    random_row = get_random_row_from_table(db_conn, test_table)
    updated_value = copy.deepcopy(random_row[updated_column])
    updated_value["job"] = "CEO"

    db_conn.execute(
        f"UPDATE {test_table} SET {updated_column} = ? WHERE id = {random_row["id"]};",
        [updated_value]
    )
    db_conn.close()

    random_row["observed_in"] = random_row_observed_in

    expected_drift_row = copy.deepcopy(random_row)
    expected_drift_row["observed_in"] = updated_db
    expected_drift_row[updated_column] = updated_value

    run_compare(
        config,
        test_table,
        random_row,
        expected_drift_row,
        True
    )


@pytest.mark.parametrize("config", ["duckdb_config", "postgres_config", "sqlite_config"], indirect=True)
def test_json_in_array_str_value_delta(config: Config):
    db_conn = config.db1.get_connection(read_only=False)
    random_row_observed_in = config.db2_details.db_id
    updated_db = config.db1_details.db_id
    test_table = SampleTable.EMPLOYEE.value
    updated_column = "previous_positions"

    random_row = get_random_row_from_table(db_conn, test_table)
    updated_value_list = json.loads(random_row[updated_column])
    updated_value_list[0]["position"] = "Assistant to the regional manager"
    updated_value = json.dumps(updated_value_list)

    db_conn.execute(
        f"UPDATE {test_table} SET {updated_column} = ? WHERE id = {random_row["id"]};",
        [updated_value]
    )
    db_conn.close()

    random_row["observed_in"] = random_row_observed_in

    expected_drift_row = copy.deepcopy(random_row)
    expected_drift_row["observed_in"] = updated_db
    expected_drift_row[updated_column] = updated_value

    run_compare(
        config,
        test_table,
        random_row,
        expected_drift_row,
        True
    )


@pytest.mark.parametrize("config", ["duckdb_config", "postgres_config", "sqlite_config"], indirect=True)
def test_json_key_sorting(config: Config):
    db_conn = config.db1.get_connection(read_only=False)
    random_row_observed_in = config.db2_details.db_id
    updated_db = config.db1_details.db_id
    test_table = SampleTable.COMPANY.value
    updated_column = "owner"

    random_row = get_random_row_from_table(db_conn, test_table)
    updated_value = copy.deepcopy(random_row[updated_column])
    updated_value = dict(sorted(updated_value.items(), reverse=True))

    db_conn.execute(
        f"UPDATE {test_table} SET {updated_column} = ? WHERE id = {random_row["id"]};",
        [updated_value]
    )
    db_conn.close()

    random_row["observed_in"] = random_row_observed_in

    expected_drift_row = copy.deepcopy(random_row)
    expected_drift_row["observed_in"] = updated_db
    expected_drift_row[updated_column] = updated_value

    run_compare(
        config,
        test_table,
        random_row,
        expected_drift_row,
        False
    )


@pytest.mark.parametrize("config", ["duckdb_config", "postgres_config", "sqlite_config"], indirect=True)
def test_array_of_json_str_sorting(config: Config):
    db_conn = config.db1.get_connection(read_only=False)
    random_row_observed_in = config.db2_details.db_id
    updated_db = config.db1_details.db_id
    test_table = SampleTable.EMPLOYEE.value
    updated_column = "previous_positions"

    random_row = get_random_row_from_table(db_conn, test_table)
    updated_value_list = json.loads(random_row[updated_column])
    updated_value_list = [val for val in updated_value_list[::-1]]
    updated_value = json.dumps(updated_value_list)

    db_conn.execute(
        f"UPDATE {test_table} SET {updated_column} = ? WHERE id = {random_row["id"]};",
        [updated_value]
    )
    db_conn.close()

    random_row["observed_in"] = random_row_observed_in

    expected_drift_row = copy.deepcopy(random_row)
    expected_drift_row["observed_in"] = updated_db
    expected_drift_row[updated_column] = updated_value

    run_compare(
        config,
        test_table,
        random_row,
        expected_drift_row,
        False
    )








