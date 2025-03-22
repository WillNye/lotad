import json
from random import randint

import duckdb


def run_query(
    db_conn: duckdb.DuckDBPyConnection,
    query: str,
    **query_parameters,
) -> list[dict]:
    q = db_conn.execute(query, query_parameters or None)

    rows = q.fetchall()
    assert q.description

    column_names = [desc[0] for desc in q.description]
    return [
        dict(zip(column_names, row))
        for row in rows
    ]


def normalize_results(results: list[dict]) -> list[dict]:
    response = []
    for r in results:
        row = dict()
        for k, v in json.loads(r["row"]).items():
            if isinstance(v, dict):
                row[k] = v
            elif isinstance(v, str):
                try:
                    row[k] = json.dumps(json.loads(v))
                except Exception:
                    row[k] = v
            else:
                row[k] = str(v)
        response.append(row)
    return response


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
        f"SELECT to_json(t) as row FROM {table} t WHERE id = {randint(1, row_count)}"
    )
    return normalize_results(results)[0]

