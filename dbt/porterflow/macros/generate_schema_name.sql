{#- Use the custom schema name as-is (staging, intermediate, marts, ...) instead of dbt's
    default "<target_schema>_<custom>" so the warehouse layout reads like the architecture. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}{{ target.schema }}{%- else -%}{{ custom_schema_name | trim }}{%- endif -%}
{%- endmacro %}
