-- dbt macro: generate_surrogate_key
-- Creates a surrogate key from multiple columns
{% macro generate_surrogate_key(column_list) %}
    {% set col_str = ",".join(column_list) %}
    md5(concat({{ col_str }}))
{% endmacro %}
