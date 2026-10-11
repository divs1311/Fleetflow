{% test unique_combination_of_columns(model, columns) %}
-- Grain test without the dbt_utils package: the given columns identify exactly one row.
select {{ columns | join(', ') }}, count(*) as n
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}
