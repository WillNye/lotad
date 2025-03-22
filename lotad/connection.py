import functools
import json
import os
import tempfile
import urllib.parse
from dataclasses import dataclass, asdict
from enum import Enum
from hashlib import md5
from typing import Optional

import duckdb
from jinja2 import Template

from lotad.utils import get_row_hash
from lotad.logger import logger

_DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')


def run_duckdb_query(
    db_conn: duckdb.DuckDBPyConnection,
    query: str
) -> list[dict]:
    q = db_conn.execute(query)

    rows = q.fetchall()
    assert q.description

    column_names = [desc[0] for desc in q.description]
    return [
        dict(zip(column_names, row))
        for row in rows
    ]


@functools.cache
def get_data_type_map(source_db_type: str, target_db_type: str) -> dict[str, str]:
    with open(
        os.path.join(
            _DATA_DIR,
            f"{source_db_type}_to_{target_db_type}_data_types.json"
        )
    ) as f:
        return json.load(f)


class DatabaseType(Enum):
    DUCKDB = 'duckdb'
    POSTGRESQL = 'postgres'
    SQLITE = 'sqlite'


@dataclass
class DatabaseDetails:
    database_type: DatabaseType
    path: str
    port: Optional[int] = None
    database: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    passfile: Optional[str] = None

    def __post_init__(self):
        if isinstance(self.database_type, str):
            self.database_type = DatabaseType(self.database_type)

        if self.database_type == DatabaseType.POSTGRESQL:
            if not self.user:
                raise ValueError(f"User not specified for {self.db_id}")
            if not self.database:
                raise ValueError(f"Database not specified for {self.db_id}")
            if not self.port:
                self.port = 5432

    @property
    def db_id(self):
        return self.database or self.path

    def dict(self):
        response = asdict(self)
        response['database_type'] = self.database_type.value
        del response["password"]
        return {k: v for k, v in response.items() if v}


class LotadConnectionInterface:
    _db_type: DatabaseType
    _table_schema: str

    def __init__(self, db_details: DatabaseDetails):
        # There isn't a db connection attr because this needs to work in a proc pool
        #   And connection objects like the DuckDBPyConnection class cannot be pickled

        self.db_details: DatabaseDetails = db_details
        self._queries_dir = os.path.join(os.path.dirname(__file__), 'queries')

        if db_details.database_type == DatabaseType.DUCKDB:
            self._duckdb_path = db_details.path
        else:
            tmp_dir = tempfile.mkdtemp()
            self._duckdb_path = os.path.join(tmp_dir, f"{md5(self.db_id.encode("utf-8")).hexdigest()}.db")

    @property
    def db_id(self):
        return self.db_details.db_id

    @property
    def db_type(self):
        return self._db_type

    @property
    def connection_string(self) -> str:
        raise NotImplementedError

    def get_connection(self, read_only: bool = True) -> duckdb.DuckDBPyConnection:
        """
        Why funnel requests for all DBs through duckdb?
        * Reduce complexity in app code
        * Reduce package dependencies.
        * Makes queries db agnostic (at least as much as possible)
        :param read_only:
        :return: duckdb.DuckDBPyConnection
        """
        if not os.path.exists(self._duckdb_path):
            tmp_db_conn = duckdb.connect(self._duckdb_path, read_only=False)
            tmp_db_conn.execute(
                f"ATTACH '{self.connection_string}' AS db {self.attach_db_params(read_only)}"
            )
            tmp_db_conn.close()

        db_conn = duckdb.connect(self._duckdb_path, read_only=read_only)
        try:
            db_conn.create_function("get_row_hash", get_row_hash)
            db_conn.execute("SET enable_progress_bar = false;")
            db_conn.execute(
                f"ATTACH '{self.connection_string}' AS db {self.attach_db_params(read_only)}"
            )
        except duckdb.CatalogException:
            logger.debug("Scalar Function get_row_hash already exists")
        return db_conn

    def attach_db_params(self, read_only: bool = True) -> str:
        if read_only:
            return f"(TYPE {self._db_type.value}, READ_ONLY)"
        else:
            return f"(TYPE {self._db_type.value})"

    def get_schema(self, db_conn: duckdb.DuckDBPyConnection, table_name: str, ignore_dates: bool) -> dict:
        """Get schema information for a table."""
        query = self.get_query_template('get_schema')
        query = query.render(table_name=table_name, table_schema=self._table_schema, ignore_dates=ignore_dates)
        columns = run_duckdb_query(db_conn, query)
        return {col["column_name"]: col["data_type"] for col in columns}

    def list_tables(self, db_conn: duckdb.DuckDBPyConnection) -> list[str]:
        """Get list of all tables in a database."""
        return sorted(
            db_conn.execute(
                self.get_query_template('list_tables').render(),
                parameters={"table_schema": self._table_schema},
            ).fetchall()
        )

    def select_from_table_query(
        self,
        table_name: str,
        columns: list[str],
        db_name: str,
    ):
        query_template = self.get_query_template('default_select_from_table')
        return query_template.render(
            table_name=table_name,
            columns=columns,
            db_name=db_name,
        )

    @staticmethod
    def parse_db_response(db_response) -> list[dict]:
        rows = db_response.fetchall()
        assert db_response.description
        column_names = [desc[0] for desc in db_response.description]
        return [dict(zip(column_names, row)) for row in rows]

    def get_generic_column_type(
        self,
        column_type: str,
        comparison_db_type: DatabaseType,
    ) -> str:
        """Databases can have different types or names to represent the same column.
        For those situations, the DB connection interface can provide a custom value
        that will be compatible across all databases.

        This is a fallback mechanism for cross DB diffs,
          using the DB's proper type is always preferred

        There is yet to be a hard standard but the general rules are:
        List or Type[] -> ARRAY
        Struct, Map, or any other DB specific object type -> JSON
        Unresolved numeric type -> TBD
        Unresolved character type -> VARCHAR

        :param column_type:
        :param comparison_db_type:
        :return:
        """
        if comparison_db_type == self._db_type:
            return column_type

        return get_data_type_map(
            self._db_type.value, comparison_db_type.value
        ).get(column_type, column_type)

    @classmethod
    def create(cls, db_details: DatabaseDetails) -> "LotadConnectionInterface":
        if db_details.database_type == DatabaseType.DUCKDB:
            return DuckDbConnectionInterface(db_details)
        elif db_details.database_type == DatabaseType.POSTGRESQL:
            return PostgresConnectionInterface(db_details)
        elif db_details.database_type == DatabaseType.SQLITE:
            return SqliteConnectionInterface(db_details)
        else:
            raise NotImplementedError

    @functools.cache
    def get_query_template(self, query_name: str) -> Template:
        # This is reliant on all supported dbs containing the same sql file in its queries sub dir
        if not query_name.endswith('.sql'):
            query_name += '.sql'

        with open(
            os.path.join(self._queries_dir, self._db_type.value, query_name)
        ) as f:
            return Template(f.read())


class DuckDbConnectionInterface(LotadConnectionInterface):
    _db_type: DatabaseType = DatabaseType.DUCKDB
    _table_schema: str = "main"

    @property
    def connection_string(self) -> str:
        return self.db_details.path

    def get_connection(self, read_only: bool = True):
        db_conn = duckdb.connect(self.db_details.path, read_only=read_only)
        try:
            db_conn.create_function("get_row_hash", get_row_hash)
            db_conn.execute("SET enable_progress_bar = false;")
        except duckdb.CatalogException:
            logger.debug("Scalar Function get_row_hash already exists")
        return db_conn

    def attach_db_params(self, read_only: bool = True) -> str:
        if read_only:
            return f"(READ_ONLY)"
        else:
            return ""

    def get_generic_column_type(
        self,
        column_type: str,
        comparison_db_type: DatabaseType,
    ) -> str:
        if comparison_db_type == self._db_type:
            return column_type
        elif comparison_db_type == DatabaseType.POSTGRESQL:
            if column_type.startswith("STRUCT"):
                return "JSON"
            elif column_type.startswith("LIST") or column_type.endswith("[]"):
                return "ARRAY"
        elif comparison_db_type == DatabaseType.SQLITE:
            if column_type.startswith("STRUCT"):
                return "VARCHAR"
            elif column_type.startswith("LIST") or column_type.endswith("[]"):
                return "VARCHAR"

        return get_data_type_map(
            self._db_type.value, comparison_db_type.value
        ).get(column_type, column_type)


class PostgresConnectionInterface(LotadConnectionInterface):
    _db_type: DatabaseType = DatabaseType.POSTGRESQL
    _table_schema = 'public'

    @property
    def connection_string(self) -> str:
        details = self.db_details
        conn_str = f"dbname={details.database} host={details.path} port={details.port} user={details.user}"
        if password := details.password:
            encoded_password = urllib.parse.quote_plus(password)
            conn_str += f" password={encoded_password}"
        elif passfile := details.passfile:
            conn_str += f" passfile={passfile}"

        return conn_str


class SqliteConnectionInterface(LotadConnectionInterface):
    _db_type: DatabaseType = DatabaseType.SQLITE
    _table_schema = None

    @property
    def connection_string(self) -> str:
        return self.db_details.path

    def get_generic_column_type(
        self,
        column_type: str,
        comparison_db_type: DatabaseType,
    ) -> str:
        return column_type

    def list_tables(self, db_conn: duckdb.DuckDBPyConnection) -> list[str]:
        """Get list of all tables in a database."""
        return sorted(
            db_conn.execute(
                self.get_query_template('list_tables').render()
            ).fetchall()
        )

    def get_schema(self, db_conn: duckdb.DuckDBPyConnection, table_name: str, ignore_dates: bool) -> dict:
        """Get schema information for a table."""
        query = self.get_query_template('get_schema')
        query = query.render(table_name=table_name, ignore_dates=ignore_dates)
        columns = run_duckdb_query(db_conn, query)
        return {col["column_name"]: col["data_type"] for col in columns}
