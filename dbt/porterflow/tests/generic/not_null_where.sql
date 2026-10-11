{% test not_null_where(model, column_name, condition) %}
-- Conditional completeness, e.g. every completed trip must have a fare.
select *
from {{ model }}
where ({{ condition }}) and {{ column_name }} is null
{% endtest %}
