-- Write the raw db1 table contents to a temp table
CREATE OR REPLACE TEMP TABLE {{ table_name }}_base_t1 AS
{{ db1_query }}

-- Create a new table that adds observed_in and hashed_row data
CREATE OR REPLACE TABLE {{ table_name }}_t1 AS
SELECT '{{ db1_path }}' AS observed_in,
    *,
    get_row_hash(TO_JSON(t)::VARCHAR) as hashed_row
FROM {{ table_name }}_base_t1 t;

-- Drop the temp table
DROP TABLE {{ table_name }}_base_t1;

-- Do the same for the db2 table
CREATE OR REPLACE TABLE {{ table_name }}_base_t2 AS
{{ db2_query }}

CREATE OR REPLACE TABLE {{ table_name }}_t2 AS
SELECT '{{ db2_path }}' AS observed_in,
    *,
    get_row_hash(TO_JSON(t)::VARCHAR) as hashed_row
FROM {{ table_name }}_base_t2 t;

DROP TABLE {{ table_name }}_base_t2;

-- Write the delta between to the dbs to the same name as the table
CREATE OR REPLACE TABLE {{ table_name }} AS
WITH _T1_ONLY_ROWS AS (
    -- Get all rows observed in db1 but not db2
    SELECT _t1.*
    FROM {{ table_name }}_t1 _t1
    ANTI JOIN {{ table_name }}_t2 _t2
    ON _t1.hashed_row = _t2.hashed_row
),
_T2_ONLY_ROWS AS (
   -- Get all rows observed in db2 but not db1
    SELECT _t2.*
    FROM {{ table_name }}_t2 _t2
    ANTI JOIN {{ table_name }}_t1 _t1
    ON _t2.hashed_row = _t1.hashed_row
)
SELECT * FROM _T1_ONLY_ROWS
UNION
SELECT * FROM _T2_ONLY_ROWS;