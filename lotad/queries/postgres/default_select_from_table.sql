SELECT {%- for column in columns %}
    {{ column }},
    {%- endfor %}
FROM db.{{ db_name }}.{{ table_name }};