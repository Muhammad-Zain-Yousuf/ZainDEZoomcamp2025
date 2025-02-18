{#
    This macro returns the year_quarter (e.g., 2019-Q2) frpm datetime 
#}
{% macro get_year_quarter_from_date(date_column) %}
    CONCAT(safe_cast((EXTRACT(YEAR FROM {{date_column}})) as STRING), '-Q', safe_cast((EXTRACT(QUARTER FROM {{date_column}})) as STRING))
{%- endmacro %}