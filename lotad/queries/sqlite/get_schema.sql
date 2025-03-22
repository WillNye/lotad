SELECT 
    column_name,
    UPPER(data_type) AS data_type
FROM information_schema.columns
WHERE table_name = '{{ table_name }}' AND table_catalog = 'db'
{%- if ignore_dates %}
AND LOWER(data_type) NOT LIKE '%date%' 
  AND LOWER(data_type) NOT LIKE '%time%'
  AND LOWER(data_type) NOT LIKE '%timestamp%'
{%- endif %}
ORDER BY ordinal_position
;