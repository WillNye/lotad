import duckdb

from lotad.logger import logger
from lotad.utils import get_row_hash


class LotadConnectionInterface:

    @staticmethod
    def get_connection(db_conn_str: str, read_only: bool = True):
        raise NotImplementedError

    @staticmethod
    def get_schema(db_conn, table_name: str):
        raise NotImplementedError

    @staticmethod
    def get_tables(db_conn):
        raise NotImplementedError

    @classmethod
    def create_connection(cls):
        return DuckDbConnectionInterface()


class DuckDbConnectionInterface(LotadConnectionInterface):

    @staticmethod
    def get_connection(db_conn_str: str, read_only: bool = True):
        db_conn = duckdb.connect(db_conn_str, read_only=read_only)
        try:
            db_conn.create_function("get_row_hash", get_row_hash)
            db_conn.execute("SET enable_progress_bar = false;")
        except duckdb.duckdb.CatalogException:
            logger.debug("Scalar Function get_row_hash already exists")
        return db_conn

    @staticmethod
    def get_schema(db_conn: duckdb.DuckDBPyConnection, table_name: str):
        """Get schema information for a table."""
        columns = db_conn.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            AND table_schema = 'main'
            AND data_type NOT LIKE 'TIMESTAMP%'
            AND data_type NOT LIKE 'DATE'
            ORDER BY ordinal_position
        """).fetchall()
        return {col[0]: col[1] for col in columns}

    @staticmethod
    def get_tables(db_conn: duckdb.DuckDBPyConnection) -> list[str]:
        """Get list of all tables in a database."""
        return sorted(
            db_conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        )

