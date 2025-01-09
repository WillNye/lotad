WITH t1 AS (
    SELECT DISTINCT {{ col }}
    FROM {{ table_name }}_1
    LIMIT 10000
)
SELECT COUNT(t2.{{ col }}) 
FROM {{ table_name }}_2 t2
JOIN t1 ON t1.{{ col }} = t2.{{ col }};
