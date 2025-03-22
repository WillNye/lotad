from typing import Any

import duckdb
import pytest

from lotad.config import Config
from lotad.connection import LotadConnectionInterface
from lotad.db_compare import DatabaseComparator, get_table_query
from test import SampleTable
from test.utils import run_query, get_random_row_from_table


def _update_random_row(db_config: LotadConnectionInterface, table: str, column: str, value: Any):
    db_conn = db_config.get_connection(read_only=False)
    random_row = get_random_row_from_table(db_conn, table)

    db_conn.execute(
        f"UPDATE {table} SET {column} = ? WHERE id = {random_row['id']};",
        [value]
    )

    db_conn.close()


@pytest.mark.parametrize("config", ["duckdb_config", "postgres_config", "sqlite_config"], indirect=True)
def test_basic_select_query(config: Config):
    # This is more of a basic end-to-end test for the custom query feature
    # More focused custom query tests are below
    # Configure a custom query for the USER table
    test_table = SampleTable.USER.value
    custom_query = f"SELECT id, name FROM {test_table}"
    
    config.update_table_config(
        test_table,
        query=custom_query
    )

    _update_random_row(config.db1, test_table, "name", "HelloThere")

    # Run comparison
    comparator = DatabaseComparator(config)
    comparator.compare_all()

    # Verify results
    drift_analysis_conn = duckdb.connect(config.output_path)

    # Verify only specified columns were compared
    table_schema = run_query(
        drift_analysis_conn,
        f"DESCRIBE {test_table}"
    )
    column_names = sorted([col['column_name'] for col in table_schema])
    assert column_names == sorted(['id', 'name', 'hashed_row', 'observed_in'])


@pytest.mark.parametrize("config", ["duckdb_config", "postgres_config", "sqlite_config"], indirect=True)
def test_join_query(config: Config):
    # Configure a custom query joining USER and EMPLOYEE tables
    user_table = SampleTable.USER.value
    employee_table = SampleTable.EMPLOYEE.value
    
    custom_query = f"""
        SELECT u.id, u.name, u.last_name, e.position, e.blood_group
        FROM {user_table} u
        JOIN {employee_table} e ON u.id = e.user_id
    """    
    config.update_table_config(
        user_table,
        query=custom_query
    )

    table_query = get_table_query(
        user_table,
        ["id", "name", "last_name", "position", "blood_group"],
        "db1",
        config.db1,
        config.get_table_config(user_table)
    )
    expected_query = f"""
    SELECT u.id, u.name, u.last_name, e.position, e.blood_group
    FROM db1.user u
    JOIN db1.employee e ON u.id = e.user_id;""".replace("    ", "").lstrip("\n")

    assert table_query == expected_query


@pytest.mark.parametrize("config", ["duckdb_config", "postgres_config", "sqlite_config"], indirect=True)
def test_subquery(config: Config):
    # Configure a custom query with a subquery for the COMPANY table
    company_table = SampleTable.COMPANY.value
    employee_table = SampleTable.EMPLOYEE.value
    
    custom_query = f"""
        SELECT
            c.id,
            c.name,
            (SELECT COUNT(*) FROM {employee_table} e WHERE e.company_id = c.id) as employee_count
        FROM {company_table} c
    """
    config.update_table_config(
        company_table,
        query=custom_query
    )

    table_query = get_table_query(
        company_table,
        ["id"],  # Doesn't matter, we're not checking this
        "db2",
        config.db2,
        config.get_table_config(company_table)
    )
    expected_query = f"""
    SELECT
    c.id,
    c.name,
    (SELECT COUNT(*) FROM db2.employee e WHERE e.company_id = c.id) as employee_count
    FROM db2.company c;""".replace("    ", "").lstrip("\n")

    assert table_query == expected_query


@pytest.mark.parametrize("config", ["duckdb_config", "postgres_config", "sqlite_config"], indirect=True)
def test_query_with_where(config: Config):
    # Configure a custom query for the USER table
    test_table = SampleTable.USER.value
    custom_query = f"SELECT id, name, last_name FROM {test_table} WHERE user_id != -1"
        
    config.update_table_config(
        test_table,
        query=custom_query
    )
    table_query = get_table_query(
        test_table,
        ["id"],  # Doesn't matter, we're not checking this
        "db1",
        config.db1,
        config.get_table_config(test_table)
    )
    expected_query = "SELECT id, name, last_name FROM db1.user WHERE user_id != -1;"
    expected_query = expected_query.replace("    ", "").lstrip("\n")
    assert table_query == expected_query
