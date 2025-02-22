SELECT {%- for column in columns %}
    {{ column }},
    {%- endfor %}
FROM {{ db_name }}.{{ table_name }};