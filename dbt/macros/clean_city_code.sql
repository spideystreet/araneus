{% macro clean_city_code(dept_col, city_col) %}
    lpad({{ dept_col }}::text, 2, '0') || lpad({{ city_col }}::text, 3, '0')
{% endmacro %}
