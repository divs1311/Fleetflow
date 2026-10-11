{% test null_share_below(model, column_name, max_share) %}
-- Tolerated incompleteness (e.g. GPS gaps): fails only when the NULL share exceeds the threshold.
select null_share
from (
    select count(*) filter (where {{ column_name }} is null) * 1.0 / nullif(count(*), 0) as null_share
    from {{ model }}
)
where null_share > {{ max_share }}
{% endtest %}
