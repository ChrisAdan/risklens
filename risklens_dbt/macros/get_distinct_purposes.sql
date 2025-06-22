{% macro get_distinct_purposes() %}
    {%- set sql %}
        select distinct purpose from {{ source('raw_loans', 'raw_loans_accepted') }} where purpose is not null
    {%- endset %}

    {%- set results = run_query(sql) %}
    {%- if execute %}
        {{ return(results.columns[0].values()) }}
    {%- else %}
        {{ return([]) }}
    {%- endif %}
{% endmacro %}