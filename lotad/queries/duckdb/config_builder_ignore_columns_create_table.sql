CREATE OR REPLACE TABLE {{ table_name }}_1 AS
SELECT '{{ db1_path }}' AS db_path,
    {%- for shared_column in shared_columns %}
    {{ shared_column }},
    {%- endfor %}
FROM db1.{{ table_name }};

CREATE OR REPLACE TABLE {{ table_name }}_2 AS
SELECT '{{ db2_path }}' AS db_path,
    {%- for shared_column in shared_columns %}
    {{ shared_column }},
    {%- endfor %}
FROM db2.{{ table_name }};
