import functools
import os

import duckdb
from jinja2 import Template

from lotad.logger import logger
from lotad.utils import get_row_hash


class LotadConnectionInterface:
    connection_str: str
    _queries_sub_dir: str

    def __init__(self, connection_string: str):
        # There isn't a db connection attr because this needs to work in a proc pool
        #   And connection objects like the DuckDBPyConnection class cannot be pickled

        self.connection_str = os.path.expanduser(connection_string)
        self._queries_dir = os.path.join(os.path.dirname(__file__), 'queries')

    def get_connection(self, read_only: bool = True):
        # Create with self.connection_str
        raise NotImplementedError

    def get_schema(self, db_conn, table_name: str, ignore_dates: bool) -> dict:
        raise NotImplementedError

    @staticmethod
    def parse_db_response(db_response) -> list[dict]:
        raise NotImplementedError

    @staticmethod
    def get_tables(db_conn) -> list:
        raise NotImplementedError

    @classmethod
    def create(cls, connection_str: str) -> "LotadConnectionInterface":
        # Only duckdb is supported right now
        return DuckDbConnectionInterface(connection_str)

    @functools.cache
    def get_query_template(self, query_name: str) -> Template:
        # This is reliant on all supported dbs containing the same sql file in its queries sub dir
        if not query_name.endswith('.sql'):
            query_name += '.sql'

        with open(
            os.path.join(self._queries_dir, self._queries_sub_dir, query_name)
        ) as f:
            return Template(f.read())


class DuckDbConnectionInterface(LotadConnectionInterface):
    _queries_sub_dir: str = 'duckdb'

    def get_connection(self, read_only: bool = True):
        db_conn = duckdb.connect(self.connection_str, read_only=read_only)
        try:
            db_conn.create_function("get_row_hash", get_row_hash)
            db_conn.execute("SET enable_progress_bar = false;")
        except duckdb.CatalogException:
            logger.debug("Scalar Function get_row_hash already exists")
        return db_conn

    def get_schema(self, db_conn: duckdb.DuckDBPyConnection, table_name: str, ignore_dates: bool) -> dict:
        """Get schema information for a table."""
        query = self.get_query_template('get_schema')
        query = query.render(table_name=table_name, ignore_dates=ignore_dates)
        columns = db_conn.execute(
            query
        ).fetchall()
        return {col[0]: col[1] for col in columns}

    @staticmethod
    def get_tables(db_conn: duckdb.DuckDBPyConnection) -> list[str]:
        """Get list of all tables in a database."""
        return sorted(
            db_conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        )

    @staticmethod
    def parse_db_response(db_response) -> list[dict]:
        rows = db_response.fetchall()
        assert db_response.description
        column_names = [desc[0] for desc in db_response.description]
        return [dict(zip(column_names, row)) for row in rows]
