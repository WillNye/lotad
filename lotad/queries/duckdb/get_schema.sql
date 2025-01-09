SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = '{{ table_name }}'
  AND table_schema = 'main'
{%- if ignore_dates %}
  AND data_type NOT LIKE 'TIMESTAMP%'
  AND data_type NOT LIKE 'DATE'
{%- endif %}
ORDER BY ordinal_position