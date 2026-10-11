{#- Business date processed by this run (IST). Airflow passes --vars '{run_date: YYYY-MM-DD}'. -#}
{% macro run_date() -%}
    {%- set value = var('run_date') -%}
    {%- if value is none -%}{%- set value = modules.datetime.date.today().isoformat() -%}{%- endif -%}
    cast('{{ value }}' as date)
{%- endmacro %}

{#- Location of a lake dataset; PORTERFLOW_DATA_DIR is set by the pipeline. -#}
{% macro lake_path(relative) -%}
    {{ env_var('PORTERFLOW_DATA_DIR', '../../data') }}/lake/{{ relative }}
{%- endmacro %}

{#- UTC (naive) -> IST wall clock. Explicit offset: never depends on the machine timezone. -#}
{% macro to_ist(ts) -%}
    ({{ ts }} + interval 330 minute)
{%- endmacro %}

{#- Great-circle distance in km between two coordinates. -#}
{% macro haversine_km(lat1, lng1, lat2, lng2) -%}
    (2 * 6371.0 * asin(sqrt(
        pow(sin(radians({{ lat2 }} - {{ lat1 }}) / 2), 2)
        + cos(radians({{ lat1 }})) * cos(radians({{ lat2 }})) * pow(sin(radians({{ lng2 }} - {{ lng1 }}) / 2), 2)
    )))
{%- endmacro %}

{#- Lower bound for incremental models: newest loaded row in the target minus a safety overlap. -#}
{% macro incremental_watermark(column) -%}
    (select coalesce(max({{ column }}), timestamp '1970-01-01') from {{ this }})
        - interval {{ var('late_lookback_hours') }} hour
{%- endmacro %}

{#- First business date the pipeline collected in full. Late events can bring in a few
    trips from earlier days; KPI marts start here so partial days never skew a chart. -#}
{% macro history_start() -%}
    cast('{{ env_var("PORTERFLOW_START_DATE", "2026-09-01") }}' as date)
{%- endmacro %}
