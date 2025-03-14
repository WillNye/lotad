SELECT column_name, UPPER(data_type) AS data_type
FROM db.information_schema.columns
WHERE table_name = '{{ table_name }}'
  AND table_schema = '{{ table_schema }}'
{%- if ignore_dates %}
  AND data_type NOT LIKE 'TIMESTAMP%'
  AND data_type NOT LIKE 'DATE'
{%- endif %}
ORDER BY ordinal_position