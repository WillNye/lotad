ATTACH '{{ tmp_path }}' AS tmp_{{ table_name }}_db (READ_ONLY);

CREATE OR REPLACE TABLE {{ table_name }} AS
SELECT * FROM tmp_{{ table_name }}_db.{{ table_name }};
