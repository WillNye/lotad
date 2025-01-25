from random import randint

import duckdb


def run_query(
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


def get_random_row_from_table(
    db_conn: duckdb.DuckDBPyConnection,
    table: str
) -> dict:
    count_result = run_query(
        db_conn, f"SELECT COUNT(*) AS ROW_COUNT FROM {table}"
    )
    row_count = count_result[0]['ROW_COUNT']
    results = run_query(
        db_conn,
        f"SELECT * FROM {table} WHERE id = {randint(1, row_count)}"
    )
    return results[0]

