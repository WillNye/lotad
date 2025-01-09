WITH _db1_row_summary AS (
    SELECT '{{ table_name }}' as table_name,
        '{{ db1 }}' as db_path,
        COUNT(*) as rows_only_in_db
    FROM {{ table_name }}
    WHERE observed_in = '{{ db1 }}'
),
_db2_row_summary AS (
    SELECT '{{ table_name }}' as table_name,
        '{{ db2 }}' as db_path,
        COUNT(*) as rows_only_in_db
    FROM {{ table_name }}
    WHERE observed_in = '{{ db2 }}'
)
INSERT INTO {{ data_drift_summary_table }}
SELECT '{{ table_name }}' as table_name,
    _db1.db_path AS db1,
    _db1.rows_only_in_db AS rows_only_in_db1,
    _db2.db_path AS db2,
    _db2.rows_only_in_db AS rows_only_in_db2,
FROM _db1_row_summary _db1
JOIN _db2_row_summary _db2 ON _db1.table_name = _db2.table_name;
