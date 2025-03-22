import json
import os
import shutil
import tempfile
from datetime import date, datetime
from decimal import Decimal

import duckdb
import pytest
from faker import Faker

from lotad.config import Config
from lotad.connection import DatabaseDetails, DatabaseType
from test import SampleTable

Faker.seed(0)
FAKER = Faker()


def json_encoder(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, datetime) or isinstance(obj, date):
        return obj.isoformat()
    raise TypeError(
        f"Object of type {type(obj).__name__} is not JSON serializable"
    )


def get_job_details():
    return {
        "position": FAKER.job(),
        "salary": FAKER.pricetag(),
        "company": FAKER.company(),
        "location": f"{FAKER.city()} {FAKER.state()} {FAKER.country()}",
        "start_date": FAKER.date(),
    }


def get_employee_table_data() -> tuple[str, list[dict]]:
    rows = [FAKER.profile() for _ in range(1000)]
    for row_id, row in enumerate(rows):
        row["id"] = row_id + 1
        row["previous_positions"] = json.dumps([get_job_details() for _ in range(5)])

    return SampleTable.EMPLOYEE.value, rows


def get_company_table_data():
    rows = [
        {
            "id": company_id,
            "name": FAKER.company(),
            "location": f"{FAKER.city()} {FAKER.state()} {FAKER.country()}",
            "owner": FAKER.profile(),
            "job_openings": [get_job_details() for _ in range(10)]
        }
        for company_id in range(1, 1000)
    ]

    return SampleTable.COMPANY.value, rows


def get_user_table_data():
    rows = [FAKER.profile() for _ in range(1000)]
    for row_id, row in enumerate(rows):
        row["id"] = row_id + 1

    return SampleTable.USER.value, rows


def populate_database(db_conn: duckdb.DuckDBPyConnection, tmpdir: str):
    for table, rows in [
        get_employee_table_data(),
        get_company_table_data(),
        get_user_table_data(),
    ]:
        json_path = os.path.join(tmpdir, f"{table}.json")
        with open(json_path, "w") as f:
            json.dump(rows, f, indent=2, default=json_encoder)

        db_conn.execute(
            f"CREATE TABLE {table} AS SELECT * FROM '{json_path}';"
        )


@pytest.fixture(scope="session")
def create_clean_duckdb_database() -> str:
    tmp_dir = tempfile.mkdtemp()

    # Create and populate a database
    db_path = os.path.join(tmp_dir, f"db1.db")
    db_conn = duckdb.connect(db_path)
    populate_database(db_conn, tmp_dir)
    db_conn.close()

    return db_path


@pytest.fixture(scope="session")
def create_clean_postgres_database(create_clean_duckdb_database: str) -> dict:
    """
    Creates a clean PostgreSQL database populated with data from DuckDB.
    Returns a tuple of (host, port) for connecting to the PostgreSQL database.
    """
    # PostgreSQL connection details
    pg_host = "localhost"
    pg_port = "5433"
    pg_user = "postgres"
    pg_db = "test"

    # Connect to the DuckDB database
    duckdb_conn = duckdb.connect(create_clean_duckdb_database)

    # Attach PostgreSQL database
    duckdb_conn.execute(f"""
        ATTACH 'dbname={pg_db} host={pg_host} port={pg_port} user={pg_user}' AS pg (TYPE postgres);
    """)

    # For each table, create and copy data to PostgreSQL
    for table in SampleTable:
        # Create the table in PostgreSQL and copy data from DuckDB

        schema_info = duckdb_conn.execute(f"DESCRIBE {table.value}").fetchall()
        # Create table in PostgreSQL with appropriate column types
        columns = []
        for col_info in schema_info:
            col_name = col_info[0]
            col_type = col_info[1]
            
            # Convert complex types to JSON strings for PostgreSQL compatibility
            if 'STRUCT' in col_type or 'LIST' in col_type:
                columns.append(f"{col_name} JSON")
            else:
                # Map DuckDB types to PostgreSQL types
                pg_type = col_type
                if col_type == 'VARCHAR':
                    pg_type = 'TEXT'
                elif col_type == 'UBIGINT':
                    pg_type = 'BIGINT'
                
                columns.append(f"{col_name} {pg_type}")
        
        # Create the table in PostgreSQL
        column_defs = ", ".join(columns)
        duckdb_conn.execute(f"DROP TABLE IF EXISTS pg.{table.value}")
        duckdb_conn.execute(f"CREATE TABLE pg.{table.value} ({column_defs})")
        
        # Convert complex types to JSON strings and insert data
        duckdb_conn.execute(f"""
            INSERT INTO pg.{table.value}
            SELECT * FROM {table.value};
        """)

    duckdb_conn.close()

    return {
        "path": pg_host,
        "port": pg_port,
        "database": pg_db,
        "user": pg_user,
    }


@pytest.fixture(scope="session")
def create_clean_sqlite_database(create_clean_duckdb_database: str) -> dict:
    """
    Creates a clean SQLite database populated with data from DuckDB.
    Returns connection details for the SQLite database.
    """
    # Create a temporary directory for the SQLite database
    tmp_dir = tempfile.mkdtemp()
    sqlite_db_path = os.path.join(tmp_dir, "test.db")
    
    # Connect to the DuckDB database
    duckdb_conn = duckdb.connect(create_clean_duckdb_database)
    
    # Attach SQLite database
    duckdb_conn.execute(f"""
        ATTACH '{sqlite_db_path}' AS db (TYPE sqlite);
    """)
    
    # For each table, create and copy data to SQLite
    for table in SampleTable:
        select_cols = []

        # Get the schema information for the table
        schema_info = duckdb_conn.execute(f"DESCRIBE {table.value}").fetchall()
        
        # Create table in SQLite with appropriate column types
        columns = []
        for col_info in schema_info:
            col_name = col_info[0]
            col_type = col_info[1]

            if (
                any(col_type.startswith(c_type) for c_type in ["STRUCT", "MAP", "LIST", "ARRAY"])
                or col_type.endswith("[]")
            ):
                select_cols.append(
                    f'TO_JSON("{col_name}")::VARCHAR AS "{col_name}"'
                )
            else:
                select_cols.append(f'"{col_name}"')

            # Convert complex types to JSON strings for SQLite compatibility
            if 'STRUCT' in col_type or 'LIST' in col_type:
                columns.append(f"{col_name} TEXT")
            else:
                # Map DuckDB types to SQLite types
                sqlite_type = "TEXT"
                if col_type in ('INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT'):
                    sqlite_type = 'INTEGER'
                elif col_type in ('DOUBLE', 'FLOAT', 'DECIMAL'):
                    sqlite_type = 'REAL'
                
                columns.append(f"{col_name} {sqlite_type}")
        
        # Create the table in SQLite
        column_defs = ", ".join(columns)
        duckdb_conn.execute(f"DROP TABLE IF EXISTS db.{table.value}")
        duckdb_conn.execute(f"CREATE TABLE db.{table.value} ({column_defs})")

        select_str = f"SELECT {', '.join(select_cols)}"

        # Convert complex types to JSON strings and insert data
        duckdb_conn.execute(f"""
            INSERT INTO db.{table.value}
            {select_str} FROM {table.value};
        """)
    
    duckdb_conn.close()
    
    return {
        "path": sqlite_db_path,
    }


@pytest.fixture(scope="function")
def duckdb_config(create_clean_duckdb_database: str) -> Config:
    clean_db_path = create_clean_duckdb_database
    tmp_dir = tempfile.mkdtemp()

    # Create a copy of the clean db files
    # so the data is only generated once
    db1_path = os.path.join(tmp_dir, f"db1.db")
    shutil.copy(clean_db_path, db1_path)

    db2_path = os.path.join(tmp_dir, f"db2.db")
    shutil.copy(clean_db_path, db2_path)

    return Config(
        path=os.path.join(tmp_dir, "lotad_config.yaml"),
        output_path=os.path.join(tmp_dir, "drift_analysis.db"),
        db1_details=DatabaseDetails(database_type=DatabaseType.DUCKDB, path=db1_path),
        db2_details=DatabaseDetails(database_type=DatabaseType.DUCKDB, path=db2_path),
        ignore_dates=False,
    )


@pytest.fixture(scope="function")
def postgres_config(
    create_clean_duckdb_database: str,
    create_clean_postgres_database: dict,
):
    clean_db_path = create_clean_duckdb_database
    tmp_dir = tempfile.mkdtemp()

    # Create a copy of the clean db files
    # so the data is only generated once
    db1_path = os.path.join(tmp_dir, f"db1.db")
    shutil.copy(clean_db_path, db1_path)

    return Config(
        path=os.path.join(tmp_dir, "lotad_config.yaml"),
        output_path=os.path.join(tmp_dir, "drift_analysis.db"),
        db1_details=DatabaseDetails(database_type=DatabaseType.DUCKDB, path=db1_path),
        db2_details=DatabaseDetails(database_type=DatabaseType.POSTGRESQL, **create_clean_postgres_database),
        ignore_dates=False,
    )


@pytest.fixture(scope="function")
def sqlite_config(
    create_clean_duckdb_database: str,
    create_clean_sqlite_database: dict,
):
    clean_db_path = create_clean_duckdb_database
    tmp_dir = tempfile.mkdtemp()

    # Create a copy of the clean db files
    # so the data is only generated once
    db1_path = os.path.join(tmp_dir, f"db1.db")
    shutil.copy(clean_db_path, db1_path)

    return Config(
        path=os.path.join(tmp_dir, "lotad_config.yaml"),
        output_path=os.path.join(tmp_dir, "drift_analysis.db"),
        db1_details=DatabaseDetails(database_type=DatabaseType.DUCKDB, path=db1_path),
        db2_details=DatabaseDetails(database_type=DatabaseType.SQLITE, **create_clean_sqlite_database),
        ignore_dates=False,
    )


@pytest.fixture(scope="function")
def config(request):
    """
    This fixture returns the appropriate config based on the parameter.
    It acts as a bridge between the parameterized test and the actual config fixtures.
    """
    if request.param == "duckdb_config":
        return request.getfixturevalue("duckdb_config")
    elif request.param == "postgres_config":
        return request.getfixturevalue("postgres_config")
    elif request.param == "sqlite_config":
        return request.getfixturevalue("sqlite_config")
    else:
        raise ValueError(f"Unknown config type: {request.param}")
