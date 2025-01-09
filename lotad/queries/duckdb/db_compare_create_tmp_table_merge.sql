CREATE OR REPLACE TABLE {{ table_name }}_t1 AS
SELECT '{{ db1_path }}' AS observed_in,
    {%- for column in db1_columns %}
    {{ column }},
    {%- endfor %}
    get_row_hash(TO_JSON((
        {%- for column in db1_columns %}
        {% if not column.startswith("NULL") %}{{ column }}{% if not loop.last %},{% endif %}{% endif %}
        {%- endfor %}
   ))::VARCHAR) as hashed_row
FROM db1.{{ table_name }};

CREATE OR REPLACE TABLE {{ table_name }}_t2 AS
SELECT '{{ db2_path }}' AS observed_in,
    {%- for column in db2_columns %}
    {{ column }},
    {%- endfor %}
    get_row_hash(TO_JSON((
        {%- for column in db2_columns %}
        {{ column }}{% if not loop.last %},{% endif %}
        {%- endfor %}
   ))::VARCHAR) as hashed_row
FROM db2.{{ table_name }};

CREATE OR REPLACE TABLE {{ table_name }} AS
WITH _T1_ONLY_ROWS AS (
    SELECT _t1.*
    FROM {{ table_name }}_t1 _t1
    ANTI JOIN {{ table_name }}_t2 _t2
    ON _t1.hashed_row = _t2.hashed_row
),
_T2_ONLY_ROWS AS (
    SELECT _t2.*
    FROM {{ table_name }}_t2 _t2
    ANTI JOIN {{ table_name }}_t1 _t1
    ON _t2.hashed_row = _t1.hashed_row
)
SELECT * FROM _T1_ONLY_ROWS
UNION
SELECT * FROM _T2_ONLY_ROWS;